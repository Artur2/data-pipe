use crate::configuration::configuration::DataPipeConfiguration;
use crate::data::client_subscription_info::ClientSubscriptionInfo;
use crate::data::clients_manager::ClientsManager;
use crate::data::error::{DataPipeError, DataPipeResult};
use crate::data::kafka::default_context::{DefaultConsumer, DefaultContext};
use crate::data::messaging::data_pipe_message::DataPipeMessage;
use crate::data::messaging::data_pipe_message_type::DataPipeMessageType;
use crate::metrics::statistics::Statistics;
use futures_util::StreamExt;
use log::{info, warn};
use parking_lot::RwLock;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio_util::sync::CancellationToken;

pub struct KafkaService {
    clients: Arc<RwLock<ClientsManager>>,
    configuration: Arc<DataPipeConfiguration>,
    statistics: Arc<Statistics>,
}

type ProducerResult = Result<FutureProducer, DataPipeError>;

impl KafkaService {
    pub fn new(
        clients: Arc<RwLock<ClientsManager>>,
        configuration: Arc<DataPipeConfiguration>,
        statistics: Arc<Statistics>,
    ) -> Arc<KafkaService> {
        Arc::new(KafkaService {
            clients,
            configuration,
            statistics,
        })
    }

    pub fn initialize(self: Arc<Self>, receiver: Receiver<DataPipeMessage>) -> DataPipeResult<()> {
        _ = self.init_message_processing(receiver);
        Ok(())
    }

    fn init_message_processing(
        self: Arc<Self>,
        mut receiver: Receiver<DataPipeMessage>,
    ) -> DataPipeResult<()> {
        tokio::spawn(async move {
            let producer_result = Self::create_producer(
                &self.configuration.bootstrap_servers,
                &self.configuration.kafka_message_send_timeout,
            );

            if producer_result.is_err() {
                warn!(
                    "Kafka producer creation error: {:?}",
                    producer_result.err().unwrap()
                );
                return;
            }

            let producer = producer_result.unwrap();
            let cloned_stats = self.statistics.clone();

            while let Ok(message) = receiver.recv().await {
                let cloned_stats = cloned_stats.clone();
                if !message.is_management() {
                    cloned_stats.increment_request_per_second_kafka_in();
                    let headers = Self::create_headers(&message);
                    if let Err(_) = &headers {
                        warn!("Kafka message received a message with no headers");
                        return;
                    }

                    let topic = message.topic.unwrap();
                    let cloned_producer = producer.clone();
                    tokio::spawn(async move {
                        let result = cloned_producer
                            .send(
                                FutureRecord::to(&topic)
                                    .payload(&message.data)
                                    .key(&message.message_identifier)
                                    .headers(headers.unwrap()),
                                Duration::from_secs(1),
                            )
                            .await;

                        if result.is_err() {
                            warn!("Kafka message produced error: {:?}", result.err().unwrap());
                        }
                    });
                } else {
                    let cloned_self = self.clone();
                    let result = cloned_self.manage_subscription(message);
                    if result.is_err() {
                        warn!("Management process error: {:?}", result);
                    }
                }
            }

            warn!("Message producing stopped");
        });

        Ok(())
    }

    fn manage_subscription(self: Arc<Self>, message: DataPipeMessage) -> DataPipeResult<()> {
        if message.is_subscribe() {
            self.create_subscription(message)?
        } else if message.is_unsubscribe() {
            self.unsubscribe(message)?;
        }

        Ok(())
    }

    fn create_subscription(self: Arc<Self>, message: DataPipeMessage) -> DataPipeResult<()> {
        let subscription_info = message.deserialize_subscription_data()?;
        for subscription in subscription_info {
            let cloned_self = self.clone();
            let client_identifier = message.client_identifier.clone();
            tokio::task::spawn(async move {
                info!("Creating subscription for {}", &client_identifier);
                let cancellation_token = cloned_self.create_client_subscription_info_with_token(
                    &client_identifier,
                    &subscription.topic,
                    &subscription.group,
                );

                let consumer_result = Self::create_consumer(
                    &subscription.group,
                    &cloned_self.configuration.bootstrap_servers,
                    &cloned_self.configuration.kafka_session_timeout,
                );

                if consumer_result.is_err() {
                    warn!("Kafka client creation error");
                    return;
                }

                let consumer: DefaultConsumer = consumer_result.unwrap();
                _ = consumer.subscribe(&[&subscription.topic]);
                let mut stream = consumer.stream();

                while let Some(message) = tokio::select! {
                    stream_result = stream.next() => {
                            match stream_result {
                                Some(Ok(msg)) => Some(msg),
                                _ => None,
                            }
                        }
                    _ = cancellation_token.cancelled() => None,
                } {
                    let message_data_raw = message.payload();
                    if message_data_raw.is_none() {
                        warn!("Kafka message payload is empty");
                        continue;
                    }

                    {
                        let message_data = message_data_raw.unwrap();
                        let clients = cloned_self.clients.read();
                        let found_client = clients.get(&client_identifier);

                        if let Some(client) = found_client {
                            let message_id = uuid::Uuid::new_v4().to_string();
                            let data_pipe_message = DataPipeMessage::new(
                                client_identifier.to_owned(),
                                message_id,
                                DataPipeMessageType::Default,
                                message_data.to_vec(),
                                Some(message.topic().to_owned()),
                                vec![],
                            );
                            let send_result = client.sender.send(data_pipe_message);
                            if send_result.is_err() {
                                warn!("Kafka client send error");
                            }
                        } else {
                            warn!("Kafka client not found");
                        }
                    }

                    let commit_message_result =
                        consumer.commit_message(&message, CommitMode::Async);

                    if commit_message_result.is_err() {
                        warn!("Kafka consumer commit error: {:?}", commit_message_result);
                    }
                }

                warn!(
                    "Kafka consumer closed, unsubscribing of client {0} and topic {1}, group {2}",
                    client_identifier, subscription.topic, subscription.group
                );
                consumer.unsubscribe();
                warn!(
                    "Kafka consumer closed, unsubscribed of client {0} and topic {1}, group {2}",
                    client_identifier, subscription.topic, subscription.group
                );
            });
        }

        Ok(())
    }

    fn unsubscribe(self: Arc<Self>, message: DataPipeMessage) -> DataPipeResult<()> {
        let unsubscribe_data = message.deserialize_subscription_data();

        for subscription in unsubscribe_data? {
            {
                let clients = self.clients.read();
                let cancellation_holder = clients
                    .get_client_cancellation_holder(&subscription.topic, &subscription.group);
                if cancellation_holder.is_some() {
                    let token = cancellation_holder.unwrap().token.clone();
                    token.cancel();
                }
            }

            let mut clients = self.clients.write();
            _ = clients.remove_subscription(
                &message.client_identifier,
                &subscription.topic,
                &subscription.group,
            );
        }

        Ok(())
    }

    fn create_headers(message: &DataPipeMessage) -> DataPipeResult<OwnedHeaders> {
        let mut headers = OwnedHeaders::new();
        for header in &message.headers {
            headers = headers.insert(Header {
                key: &header.key,
                value: Some(&header.value),
            });
        }

        Ok(headers)
    }

    fn create_producer(bootstrap_servers: &str, message_send_timeout: &str) -> ProducerResult {
        ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", message_send_timeout)
            .set("batch.size", "65536")
            .set("linger.ms", "50")
            .set("queue.buffering.max.kbytes", "524288")
            .set("queue.buffering.max.messages", "100000")
            .create()
            .map_err(|e| DataPipeError::ProducerCreationError(e.to_string()))
    }

    fn create_consumer(
        group: &str,
        bootstrap_servers: &str,
        timeout: &str,
    ) -> DataPipeResult<DefaultConsumer> {
        let context = DefaultContext;
        let consumer_result = ClientConfig::new()
            .set("group.id", group)
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", timeout)
            .set("enable.auto.commit", "false")
            .create_with_context(context);

        consumer_result.map_err(|e| DataPipeError::ConsumerCreationError(e.to_string()))
    }

    fn create_client_subscription_info_with_token(
        &self,
        client_identifier: &str,
        topic: &str,
        group: &str,
    ) -> CancellationToken {
        let mut clients = self.clients.write();
        let _ = clients.add_subscription(
            client_identifier,
            ClientSubscriptionInfo::new(topic.to_owned(), group.to_owned()),
        );

        let holder = clients.get_client_cancellation_holder(topic, group);
        if let Some(holder) = holder {
            holder.token.clone()
        } else {
            panic!("Cancellation token not exist")
        }
    }
}
