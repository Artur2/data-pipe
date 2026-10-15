use crate::configuration::configuration::DataPipeConfiguration;
use crate::data::clients_manager::ClientsManager;
use crate::data::error::{DataPipeError, DataPipeResult};
use crate::data::kafka::default_context::{DefaultConsumer, DefaultContext};
use crate::data::messaging::client_subscription_info::ClientSubscriptionInfo;
use crate::data::messaging::data_pipe_message::DataPipeMessage;
use crate::data::messaging::data_pipe_message_type::DataPipeMessageType;
use crate::metrics::statistics::Statistics;
use bytes::Bytes;
use futures_util::StreamExt;
use log::{info, warn};
use parking_lot::RwLock;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio_util::sync::CancellationToken;
use xxhash_rust::xxh32;

pub struct KafkaService {
    clients: Arc<RwLock<ClientsManager>>,
    configuration: Arc<DataPipeConfiguration>,
    statistics: Arc<Statistics>,
    consumers: RwLock<HashMap<i32, Arc<DefaultConsumer>>>,
}

type ProducerResult = Result<FutureProducer, DataPipeError>;

impl KafkaService {
    pub fn new(
        clients: Arc<RwLock<ClientsManager>>,
        configuration: Arc<DataPipeConfiguration>,
        statistics: Arc<Statistics>,
    ) -> Arc<KafkaService> {
        let consumers = RwLock::new(HashMap::new());
        Arc::new(KafkaService {
            clients,
            configuration,
            statistics,
            consumers,
        })
    }

    pub fn initialize(
        self: Arc<Self>,
        receiver: Receiver<Arc<DataPipeMessage>>,
    ) -> DataPipeResult<()> {
        _ = self.init_message_processing(receiver);
        Ok(())
    }

    fn init_message_processing(
        self: Arc<Self>,
        mut receiver: Receiver<Arc<DataPipeMessage>>,
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
                let cloned_self = self.clone();
                if !message.is_management() && !message.is_ack() {
                    if cloned_self.configuration.harvest_statistics {
                        let cloned_stats = cloned_stats.clone();
                        cloned_stats.increment_request_per_second_kafka_in();
                    }

                    let headers = Self::create_headers(&message);
                    if let Err(_) = &headers {
                        warn!("Kafka message received a message with no headers");
                        return;
                    }

                    let message_reference = message.clone();
                    let topic = message_reference.topic.clone().unwrap();
                    let cloned_producer = producer.clone();
                    tokio::spawn(async move {
                        let result = cloned_producer
                            .send(
                                FutureRecord::to(&topic)
                                    .payload(message.data.as_ref())
                                    .key(&message.message_identifier)
                                    .headers(headers.unwrap()),
                                Duration::from_secs(1),
                            )
                            .await;

                        if result.is_err() {
                            warn!("Kafka message produced error: {:?}", result.err().unwrap());
                        } else if cloned_self.configuration.ack_mode {
                            let clients = cloned_self.clients.read();
                            let client = clients.get(&message.client_identifier);
                            if let Some(client) = client {
                                let ack_message =
                                    Arc::new(message_reference.clone_for_ack().unwrap());
                                _ = client.sender.send(ack_message);
                            }
                        }
                    });
                } else if message.is_management() {
                    let cloned_self = self.clone();
                    let result = cloned_self.manage_subscription(message);
                    if result.is_err() {
                        warn!("Management process error: {:?}", result);
                    }
                } else if message.is_ack() {
                    cloned_self.process_ack_message(message);
                }
            }

            warn!("Message producing stopped");
        });

        Ok(())
    }

    fn manage_subscription(self: Arc<Self>, message: Arc<DataPipeMessage>) -> DataPipeResult<()> {
        if message.is_subscribe() {
            self.create_subscription(message)?
        } else if message.is_unsubscribe() {
            self.unsubscribe(message)?;
        }

        Ok(())
    }

    fn create_subscription(self: Arc<Self>, message: Arc<DataPipeMessage>) -> DataPipeResult<()> {
        let subscription_info = message.deserialize_subscription_data()?;
        for subscription in subscription_info {
            let cloned_self = self.clone();
            let client_identifier = message.client_identifier.clone();
            tokio::task::spawn(async move {
                info!("Creating subscription for {}", &client_identifier);
                let cancellation_token = cloned_self.create_client_tracking_info_with_token(
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
                let consumer_clone = Arc::new(consumer);
                let consumer_holder = consumer_clone.clone();
                _ = consumer_clone.subscribe(&[&subscription.topic]);
                let mut stream = consumer_clone.stream();

                {
                    let mut consumers = cloned_self.consumers.write();
                    let hash = Self::create_hash(
                        &client_identifier,
                        &subscription.topic,
                        &subscription.group,
                    );
                    _ = consumers.insert(hash, consumer_holder);
                }

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

                    let message_id = uuid::Uuid::new_v4().to_string();
                    let message_id_cloned = message_id.clone();
                    {
                        let message_data = message_data_raw.unwrap();
                        let clients = cloned_self.clients.read();
                        let found_client = clients.get(&client_identifier);

                        if let Some(client) = found_client {
                            let data_pipe_message = Arc::new(DataPipeMessage::new(
                                client_identifier.to_owned(),
                                message_id,
                                DataPipeMessageType::Default,
                                Bytes::copy_from_slice(message_data),
                                Some(message.topic().to_owned()),
                                vec![],
                                Some(subscription.group.to_owned()),
                            ));
                            let send_result = client.sender.send(data_pipe_message);
                            if send_result.is_err() {
                                warn!("Kafka client send error");
                            }
                        } else {
                            warn!("Kafka client not found");
                        }
                    }

                    if !cloned_self.configuration.ack_mode {
                        let commit_message_result =
                            consumer_clone.commit_message(&message, CommitMode::Async);

                        if commit_message_result.is_err() {
                            warn!("Kafka consumer commit error: {:?}", commit_message_result);
                        }
                    } else {
                        let mut clients = cloned_self.clients.write();
                        let result = clients.add_in_flight_offset(
                            &client_identifier,
                            &subscription.topic.to_owned(),
                            &subscription.group.to_owned(),
                            message.offset(),
                            &message_id_cloned,
                            message.partition() as u16,
                        );

                        if result.is_err() {
                            warn!(
                                "Kafka consumer error, cannot add inflight message: {:?}",
                                result
                            );
                        }
                    }
                }

                warn!(
                    "Kafka consumer closed, unsubscribing of client {0} and topic {1}, group {2}",
                    client_identifier, subscription.topic, subscription.group
                );
                consumer_clone.unsubscribe();
                {
                    let mut consumers = cloned_self.consumers.write();
                    let hash = Self::create_hash(
                        &client_identifier,
                        &subscription.topic,
                        &subscription.group,
                    );

                    _ = consumers.remove(&hash);
                }
                warn!(
                    "Kafka consumer closed, unsubscribed of client {0} and topic {1}, group {2}",
                    client_identifier, subscription.topic, subscription.group
                );
            });
        }

        Ok(())
    }

    fn unsubscribe(self: Arc<Self>, message: Arc<DataPipeMessage>) -> DataPipeResult<()> {
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
            _ = clients.remove_tracking_info(
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

    fn create_client_tracking_info_with_token(
        &self,
        client_identifier: &str,
        topic: &str,
        group: &str,
    ) -> CancellationToken {
        let mut clients = self.clients.write();
        let _ = clients.add_tracking_info(
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

    fn create_hash(client_identifier: &str, topic: &str, group: &str) -> i32 {
        let mut hasher = xxh32::Xxh32::new(0);
        hasher.update(client_identifier.as_bytes());
        hasher.update(topic.as_bytes());
        hasher.update(group.as_bytes());

        hasher.digest() as i32
    }

    fn process_ack_message(&self, message: Arc<DataPipeMessage>) {
        let mut clients = self.clients.write();

        if message.topic.is_none() || message.group.is_none() {
            warn!("Kafka message received an empty topic or group");
            return;
        }

        let topic = message.topic.as_ref().unwrap();
        let group = message.group.as_ref().unwrap();

        let compute_result = clients.compute_offset_to_commit(
            &message.client_identifier,
            &topic,
            &group,
            &message.message_identifier,
        );

        if compute_result.is_ok() {
            let (offset, partition) = compute_result.unwrap();
            let hash = Self::create_hash(&message.client_identifier, &topic, &group);

            let consumers = self.consumers.write();
            let consumer = consumers.get(&hash);
            if let Some(consumer) = consumer {
                let mut tpl = TopicPartitionList::new();
                _ = tpl.add_partition_offset(&topic, partition as i32, Offset::Offset(offset));
                consumer.commit(&tpl, CommitMode::Async).unwrap();
            }
        }
    }
}
