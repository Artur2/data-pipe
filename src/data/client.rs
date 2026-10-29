use crate::data::client_tracking_info::ClientTrackingInfo;
use crate::data::messaging::data_pipe_message::DataPipeMessage;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;

/// Основная структура, по которой будет роутинг сообщений\
/// Когда сообщение приходит из Kafka, мы смотрим клиентов и отправляем сообщения по найденому
pub struct Client {
    pub identifier: String,
    pub sender: Sender<Arc<DataPipeMessage>>,
    pub tracking_infos: Vec<ClientTrackingInfo>,
}

impl Client {
    pub fn new(identifier: String, sender_capacity: usize) -> Client {
        let (sender, _) = broadcast::channel(sender_capacity);
        Client {
            identifier,
            sender,
            tracking_infos: vec![],
        }
    }
}
