use crate::configuration::Configuration;
use crate::data::clients_manager::ClientsManager;
use crate::data::error::{DataPipeError, DataPipeResult};
use crate::data::kafka::default_context::{CustomConsumer, DefaultContext};
use crate::data::messaging::data_pipe_message::DataPipeMessage;
use crate::data::messaging::data_pipe_message_type::DataPipeMessageType;
use log::warn;
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
    configuration: Arc<Configuration>,
}

type ProducerResult = Result<FutureProducer, DataPipeError>;

impl KafkaService {
    pub fn new(
        clients: Arc<RwLock<ClientsManager>>,
        configuration: Arc<Configuration>,
    ) -> Arc<KafkaService> {
        Arc::new(KafkaService {
            clients,
            configuration,
        })
    }

    pub fn initialize(self: Arc<Self>, receiver: Receiver<DataPipeMessage>) -> DataPipeResult<()> {
        self.subscribe(receiver)?;
        Ok(())
    }

    fn subscribe(self: Arc<Self>, mut receiver: Receiver<DataPipeMessage>) -> DataPipeResult<()> {
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
            loop {
                while let Ok(message) = receiver.try_recv() {
                    if !message.is_management() {
                        let headers = Self::create_headers(&message);
                        if let Err(_) = &headers {
                            warn!("Kafka message received a message with no headers");
                            return;
                        }

                        let topic = message.topic.unwrap();

                        let send_result = producer
                            .send(
                                FutureRecord::to(&topic)
                                    .payload(&message.data)
                                    .key(&message.message_identifier)
                                    .headers(headers.unwrap()),
                                Duration::from_secs(0),
                            )
                            .await;

                        if send_result.is_err() {
                            warn!("Kafka producer send error: {:?}", send_result);
                        }
                    } else {
                        let cloned_self = self.clone();
                        _ = cloned_self.manage_subscription(message);
                    }
                }
            }
        });

        Ok(())
    }

    fn manage_subscription(self: Arc<Self>, message: DataPipeMessage) -> DataPipeResult<()> {
        if message.is_subscribe() {
            let subscription_info = message.deserialize_subscription_data()?;
            for subscription in subscription_info {
                let cloned_self = self.clone();
                let client_identifier = message.client_identifier.clone();

                tokio::spawn(async move {
                    let consumer_result = Self::create_consumer(
                        &subscription.group,
                        &cloned_self.configuration.bootstrap_servers,
                        &cloned_self.configuration.kafka_session_timeout,
                    );

                    if consumer_result.is_err() {
                        warn!("Kafka client creation error");
                        return;
                    }

                    let consumer: CustomConsumer = consumer_result.unwrap();
                    _ = consumer.subscribe(&[&subscription.topic]);

                    let cancellation_token = cloned_self
                        .clone_cancellation_token(&subscription.topic, &subscription.group);

                    while let Some(message) = tokio::select! {
                        _ = cancellation_token.cancelled() => None,
                        result = consumer.recv() => result.ok(),
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

                            if let Some(client_option) = found_client {
                                let message_id = uuid::Uuid::new_v4().to_string();
                                let data_pipe_message = DataPipeMessage::new(
                                    client_identifier.to_owned(),
                                    message_id,
                                    DataPipeMessageType::Default,
                                    message_data.to_vec(),
                                    Some(message.topic().to_owned()),
                                    vec![],
                                );
                                let send_result = client_option.sender.send(data_pipe_message);
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

                    warn!("Kafka consumer closed, unsubscribing");
                    consumer.unsubscribe();
                    warn!("Kafka consumer closed, unsubscribed");
                });
            }
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
            .create()
            .map_err(|e| DataPipeError::ProducerCreationError(e.to_string()))
    }

    fn create_consumer(
        group: &str,
        bootstrap_servers: &str,
        timeout: &str,
    ) -> DataPipeResult<CustomConsumer> {
        let context = DefaultContext;
        let consumer_result = ClientConfig::new()
            .set("group.id", group)
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", timeout)
            .set("enable.auto.commit", "false")
            .create_with_context(context);

        consumer_result.map_err(|_| DataPipeError::Unknown)
    }

    fn clone_cancellation_token(&self, topic: &str, group: &str) -> CancellationToken {
        let clients = self.clients.read();
        let cancellation_holder = clients.get_client_cancellation_holder(topic, group);

        cancellation_holder.unwrap().token.clone()
    }
}
