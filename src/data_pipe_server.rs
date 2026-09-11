use crate::data::client::Client;
use crate::data::error::DataPipeResult;
use crate::services::echo_service::EchoService;
use crate::services::ws_socket_service::WsSocketService;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct DataPipeServer {
    clients: Arc<RwLock<HashMap<String, Client>>>,
    ws_socket_service: Arc<WsSocketService>,
    echo_service: EchoService,
}

impl DataPipeServer {
    pub fn new() -> DataPipeServer {
        let clients = Arc::new(RwLock::new(HashMap::new()));
        let ws_socket_service = WsSocketService::new(clients.clone());
        let echo_service = EchoService::new(clients.clone());
        DataPipeServer {
            ws_socket_service,
            clients,
            echo_service,
        }
    }

    pub async fn initialize(&self) -> DataPipeResult<()> {
        let ws_service = self.ws_socket_service.clone();
        ws_service.initialize().await?;

        let ws_service_clone = self.ws_socket_service.clone();
        let receiver = ws_service_clone.get_receiver()?;
        self.echo_service.initialize(receiver).await;

        Ok(())
    }
}
