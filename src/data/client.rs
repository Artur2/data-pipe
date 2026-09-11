use crate::data::client_subscription_info::ClientSubscriptionInfo;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use crate::data::messaging::data_pipe_message::DataPipeMessage;

/// Основная структура, по которой будет роутинг сообщений\
/// Когда сообщение приходит из Kafka, мы смотрим клиентов и отправляем сообщения по найденому
pub struct Client {
    pub identifier: String,
    pub sender: Sender<DataPipeMessage>,
    pub subscription_infos: Vec<ClientSubscriptionInfo>,
}

impl Client {
    pub fn new(identifier: String) -> Client {
        let (sender, _) = broadcast::channel(1024);
        Client {
            identifier,
            sender,
            subscription_infos: vec![],
        }
    }
}
