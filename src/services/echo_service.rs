use crate::data::clients_manager::ClientsManager;
use crate::data::messaging::data_pipe_message::DataPipeMessage;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::broadcast::Receiver;

pub struct EchoService {
    clients: Arc<RwLock<ClientsManager>>,
}

impl EchoService {
    pub fn new(clients: Arc<RwLock<ClientsManager>>) -> EchoService {
        EchoService { clients }
    }

    pub async fn initialize(&self, mut receiver: Receiver<DataPipeMessage>) {
        tokio::spawn(async move {
            while let Ok(message) = receiver.recv().await {
                println!("{}", message);
            }
        });
    }
}
