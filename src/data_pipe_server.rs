use crate::configuration::Configuration;
use crate::data::clients_manager::ClientsManager;
use crate::data::error::DataPipeResult;
use crate::services::kafka_service::KafkaService;
use crate::services::web_socket_service::WebSocketService;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct DataPipeServer {
    configuration: Arc<Configuration>,
    clients: Arc<RwLock<ClientsManager>>,
    web_socket_service: Arc<WebSocketService>,
    kafka_service: Arc<KafkaService>,
}

impl DataPipeServer {
    pub fn new(configuration: Configuration) -> DataPipeServer {
        let configuration = Arc::new(configuration);
        let clients = Arc::new(RwLock::new(ClientsManager::new()));
        let web_socket_service = WebSocketService::new(clients.clone(), configuration.clone());
        let kafka_service = KafkaService::new(clients.clone(), configuration.clone());
        DataPipeServer {
            web_socket_service,
            clients,
            configuration,
            kafka_service,
        }
    }

    pub async fn initialize(&self) -> DataPipeResult<()> {
        let ws_service = self.web_socket_service.clone();
        ws_service.initialize().await?;

        let ws_service_clone = self.web_socket_service.clone();
        let receiver = ws_service_clone.get_receiver()?;
        let kafka_service_clone = self.kafka_service.clone();
        kafka_service_clone.initialize(receiver)?;

        Ok(())
    }
}
