use crate::data::client::Client;
use crate::data::error::DataPipeResult;
use crate::services::ws_socket_service::WsSocketService;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct DataPipeServer {
    ws_socket_service: Arc<WsSocketService>,
    clients: Arc<RwLock<HashMap<String, Client>>>,
}

impl DataPipeServer {
    pub fn new() -> DataPipeServer {
        let clients = Arc::new(RwLock::new(HashMap::new()));
        let ws_socket_service = WsSocketService::new(clients.clone());

        DataPipeServer {
            ws_socket_service,
            clients,
        }
    }

    pub async fn initialize(&self) -> DataPipeResult<()> {
        let ws_service = self.ws_socket_service.clone();
        ws_service.initialize().await?;
        Ok(())
    }
}
