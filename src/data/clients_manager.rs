use std::collections::HashMap;
use crate::data::client::Client;

pub struct ClientsManager {
    pub clients: HashMap<String, Client>,
}

impl ClientsManager {
    pub fn new() -> Self {
        ClientsManager {
            clients: HashMap::new(),
        }
    }

    pub fn add(&mut self, identifier: &str, client: Client) {
        self.clients.insert(identifier.to_string(), client);
    }

    pub fn contains(&self, identifier: &str) -> bool {
        self.clients.contains_key(identifier)
    }

    pub fn get(&self, identifier: &str) -> Option<&Client> {
        self.clients.get(identifier)
    }

    pub fn remove(&mut self, identifier: &str) -> Option<Client> {
        self.clients.remove(identifier)
    }
}