use crate::data::client::Client;
use std::collections::HashMap;

pub struct ClientsManager {
    clients: HashMap<String, Client>,
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

    pub fn len(&self) -> usize {
        self.clients.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub fn get_by_index(&self, index: usize) -> Option<&Client> {
        if let Some((_, value)) = self.clients.iter().nth(index) {
            Some(value)
        } else {
            None
        }
    }
}
