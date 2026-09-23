use crate::common::utils::create_random_message;
use crate::data::clients_manager::ClientsManager;
use crate::data::messaging::data_pipe_message::DataPipeMessage;
use log::{info, warn};
use rand::random_range;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::broadcast::Receiver;

pub struct EchoService {
    clients: Arc<RwLock<ClientsManager>>,
}

impl EchoService {
    pub fn new(clients: Arc<RwLock<ClientsManager>>) -> Arc<EchoService> {
        Arc::new(EchoService { clients })
    }

    pub async fn initialize(self: Arc<Self>, mut receiver: Receiver<DataPipeMessage>) {
        tokio::spawn(async move {
            while let Ok(message) = receiver.recv().await {
                info!("{}", message);
            }
        });

        tokio::spawn(async move {
            loop {
                self.write_random_message_to_client().await;
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }

    async fn write_random_message_to_client(&self) {
        let clients_read = self.clients.read().await;
        if !clients_read.is_empty() {
            let len = clients_read.len();
            let random_index = if len > 1 { random_range(0..len - 1) } else { 0 };
            let random_client_option = clients_read.get_by_index(random_index);
            match random_client_option {
                None => {
                    warn!("No random client found")
                }
                Some(client) => {
                    let message = create_random_message(client.identifier.clone());
                    let _ = client.sender.send(message);
                }
            };
        }
    }
}
