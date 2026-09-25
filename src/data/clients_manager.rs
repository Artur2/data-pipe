use crate::data::client::Client;
use crate::data::client_subscription_info::ClientSubscriptionInfo;
use crate::data::error::{DataPipeError, DataPipeResult};
use std::collections::HashMap;
use xxhash_rust::xxh32;

pub struct ClientsManager {
    clients: HashMap<String, Client>,
    /// Key is hash of topic, group. Value is client identifier
    lookup: HashMap<u32, String>,
}

#[allow(dead_code)]
impl ClientsManager {
    pub fn new() -> Self {
        ClientsManager {
            clients: HashMap::new(),
            lookup: HashMap::new(),
        }
    }

    pub fn add(&mut self, identifier: &str, client: Client) -> DataPipeResult<()> {
        if self.clients.contains_key(identifier) {
            return Err(DataPipeError::ClientAlreadyExist);
        }

        self.clients.insert(identifier.to_string(), client);
        Ok(())
    }

    pub fn add_subscription(
        &mut self,
        identifier: &str,
        client_subscription: ClientSubscriptionInfo,
    ) -> DataPipeResult<()> {
        let key = self.compute_hash(&client_subscription.topic, &client_subscription.group);
        if self.lookup.contains_key(&key) {
            return Err(DataPipeError::SubscriptionAlreadyExist(
                client_subscription.topic,
                client_subscription.group,
            ));
        }

        let client = self.clients.get_mut(identifier).unwrap();
        self.lookup.insert(key, identifier.to_owned());

        client.subscription_infos.push(client_subscription);
        Ok(())
    }

    pub fn remove_subscription(
        &mut self,
        identifier: &str,
        topic: &str,
        group: &str,
    ) -> DataPipeResult<()> {
        let key = self.compute_hash(topic, group);
        if !self.lookup.contains_key(&key) {
            return Ok(());
        }

        self.lookup.remove(&key);
        let client = self.clients.get_mut(identifier).unwrap();
        client
            .subscription_infos
            .retain(|sub| &sub.topic != topic && &sub.group != group);

        Ok(())
    }

    pub fn contains_subscription(&self, topic: &str, group: &str) -> bool {
        let key = self.compute_hash(topic, group);
        self.lookup.contains_key(&key)
    }

    pub fn contains(&self, identifier: &str) -> bool {
        self.clients.contains_key(identifier)
    }

    pub fn get(&self, identifier: &str) -> Option<&Client> {
        self.clients.get(identifier)
    }

    pub fn remove(&mut self, identifier: &str) -> Option<Client> {
        self.lookup.retain(|_, v| v != identifier);
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

    fn compute_hash(&self, topic: &str, group: &str) -> u32 {
        let mut hasher = xxh32::Xxh32::new(0);
        hasher.update(topic.as_bytes());
        hasher.update(group.as_bytes());

        hasher.digest()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn add_client_with_subscription_should_not_fail() {
        let mut client = ClientsManager::new();
        let identifier = "test";
        _ = client.add(identifier, Client::new(identifier.to_owned(), 200));

        _ = client.add_subscription(
            &identifier.to_owned(),
            ClientSubscriptionInfo::new("topic".to_owned(), "group".to_owned()),
        );
    }

    #[test]
    pub fn add_client_with_same_id_should_return_error() {
        let mut client = ClientsManager::new();
        let identifier = "test";
        _ = client.add(identifier, Client::new(identifier.to_owned(), 200));
        let result = client.add(identifier, Client::new(identifier.to_owned(), 200));
        assert!(result.is_err());
    }

    #[test]
    pub fn add_subscription_with_same_topic_group_should_return_error() {
        let mut client = ClientsManager::new();
        let identifier = "test";
        let topic = "topic";
        let group = "group";
        _ = client.add(identifier, Client::new(identifier.to_owned(), 200));

        _ = client.add_subscription(
            &identifier,
            ClientSubscriptionInfo::new(topic.to_owned(), group.to_owned()),
        );
        let result = client.add_subscription(
            &identifier,
            ClientSubscriptionInfo::new(topic.to_owned(), group.to_owned()),
        );
        assert!(result.is_err());
    }
}
