use std::sync::Arc;
use crate::data::client_subscription_info::ClientSubscriptionInfo;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use crate::data::messaging::data_pipe_message::DataPipeMessage;

/// Основная структура, по которой будет роутинг сообщений\
/// Когда сообщение приходит из Kafka, мы смотрим клиентов и отправляем сообщения по найденому
#[allow(dead_code)]
pub struct Client {
    pub identifier: String,
    pub sender: Sender<Arc<DataPipeMessage>>,
    pub subscription_infos: Vec<ClientSubscriptionInfo>,
}

impl Client {
    pub fn new(identifier: String, sender_capacity: usize) -> Client {
        let (sender, _) = broadcast::channel(sender_capacity);
        Client {
            identifier,
            sender,
            subscription_infos: vec![],
        }
    }
}
