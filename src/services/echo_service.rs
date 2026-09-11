use crate::data::client::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::broadcast::Receiver;
use crate::data::messaging::data_pipe_message::DataPipeMessage;

pub struct EchoService {
    clients: Arc<RwLock<HashMap<String, Client>>>,
}

impl EchoService {
    pub fn new(clients: Arc<RwLock<HashMap<String, Client>>>) -> EchoService {
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
