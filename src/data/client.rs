use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;

/// Основная структура, по которой будет роутинг сообщений\
/// Когда сообщение приходит из Kafka, мы смотрим клиентов и отправляем сообщения по найденому
pub struct Client {
    pub identifier: String,
    pub sender: Sender<String>,
}

impl Client {
    pub fn new(identifier: String) -> Client {
        let (sender, _) = broadcast::channel(1024);
        Client { identifier, sender }
    }
}
