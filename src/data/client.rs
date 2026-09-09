use crate::data::client_subscription_info::ClientSubscriptionInfo;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;

/// Основная структура, по которой будет роутинг сообщений\
/// Когда сообщение приходит из Kafka, мы смотрим клиентов и отправляем сообщения по найденому
pub struct Client {
    pub identifier: String,
    pub sender: Sender<String>,
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
