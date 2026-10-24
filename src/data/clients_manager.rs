use crate::common::utils::create_hash;
use crate::data::client::Client;
use crate::data::client_cancellation_holder::ClientCancellationHolder;
use crate::data::client_tracking_info::{ClientTrackingData, ClientTrackingInfo};
use crate::data::error::{DataPipeError, DataPipeResult};
use crate::data::messaging::client_subscription_info::ClientSubscriptionInfo;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

pub struct ClientsManager {
    clients: HashMap<String, Client>,
    /// Key is hash of topic, group. Value is client cancellation holder
    lookup: HashMap<i32, ClientCancellationHolder>,
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

    pub fn add_tracking_info(
        &mut self,
        identifier: &str,
        client_subscription: ClientSubscriptionInfo,
    ) -> DataPipeResult<()> {
        let key = create_hash(&[&client_subscription.topic, &client_subscription.group]);
        if self.lookup.contains_key(&key) {
            return Err(DataPipeError::SubscriptionAlreadyExist(
                client_subscription.topic,
                client_subscription.group,
            ));
        }

        let client = self.clients.get_mut(identifier).unwrap();
        self.lookup.insert(
            key,
            ClientCancellationHolder::new(identifier.to_owned(), CancellationToken::new()),
        );

        client.tracking_infos.push(ClientTrackingInfo::new(
            client_subscription.group,
            client_subscription.topic,
        ));
        Ok(())
    }

    pub fn remove_tracking_info(
        &mut self,
        identifier: &str,
        topic: &str,
        group: &str,
    ) -> DataPipeResult<()> {
        let key = create_hash(&[topic, group]);
        if !self.lookup.contains_key(&key) {
            return Ok(());
        }

        self.lookup.remove(&key);
        let client = self.clients.get_mut(identifier).unwrap();
        client
            .tracking_infos
            .retain(|sub| sub.topic != topic && sub.group != group);

        Ok(())
    }

    pub fn contains_tracking_info(&self, topic: &str, group: &str) -> bool {
        let hash = create_hash(&[topic, group]);
        self.lookup.contains_key(&hash)
    }

    pub fn get_client_cancellation_holder(
        &self,
        topic: &str,
        group: &str,
    ) -> Option<&ClientCancellationHolder> {
        let hash = create_hash(&[topic, group]);
        self.lookup.get(&hash)
    }

    pub fn contains(&self, identifier: &str) -> bool {
        self.clients.contains_key(identifier)
    }

    pub fn get(&self, identifier: &str) -> Option<&Client> {
        self.clients.get(identifier)
    }

    pub fn remove(&mut self, identifier: &str) -> Option<Client> {
        self.lookup.retain(|_, v| v.identifier != identifier);
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

    pub fn add_in_flight_offset(
        &mut self,
        identifier: &str,
        topic: &str,
        group: &str,
        offset: i64,
        message_identifier: &str,
        partition: u16,
    ) -> DataPipeResult<()> {
        let client = self.clients.get_mut(identifier).unwrap();
        let tracking_info_raw = client
            .tracking_infos
            .iter_mut()
            .find(|x| x.topic == topic && x.group == group);
        if tracking_info_raw.is_none() {
            return Err(DataPipeError::ClientNotFound);
        }

        let tracking_data =
            ClientTrackingData::new(message_identifier.to_owned(), offset, false, partition);
        let tracking_info = tracking_info_raw.unwrap();
        tracking_info.processing_acks.push_back(tracking_data);

        Ok(())
    }

    /// Returns offset and partition
    pub fn compute_offset_to_commit(
        &mut self,
        identifier: &str,
        topic: &str,
        group: &str,
        message_identifier: &str,
    ) -> DataPipeResult<(i64, u16)> {
        let mut committable_offset = None;
        let mut partition_id = None;
        let client = self.clients.get_mut(identifier).unwrap();
        let tracking_info_raw = client
            .tracking_infos
            .iter_mut()
            .find(|x| x.topic == topic && x.group == group);
        if tracking_info_raw.is_none() {
            return Err(DataPipeError::ClientNotFound);
        }

        let tracking_info = tracking_info_raw.unwrap();

        for processing_ack in tracking_info.processing_acks.iter_mut() {
            if processing_ack.message_id == message_identifier {
                processing_ack.acked = true;
                break;
            }
        }

        while let Some(info) = tracking_info.processing_acks.front() {
            if info.acked {
                // Take the value while popping the element out of the VecDeque
                if let Some(popped_info) = tracking_info.processing_acks.pop_front() {
                    committable_offset = Some(popped_info.offset);
                    partition_id = Some(popped_info.partition);
                }
            } else {
                break;
            }
        }

        if let (Some(committed), Some(partition_id)) = (committable_offset, partition_id) {
            Ok((committed, partition_id))
        } else {
            Err(DataPipeError::OffsetNotFound)
        }
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

        _ = client.add_tracking_info(
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

        _ = client.add_tracking_info(
            &identifier,
            ClientSubscriptionInfo::new(topic.to_owned(), group.to_owned()),
        );
        let result = client.add_tracking_info(
            &identifier,
            ClientSubscriptionInfo::new(topic.to_owned(), group.to_owned()),
        );
        assert!(result.is_err());
    }

    #[test]
    pub fn remove_tracking_info_should_not_fail() {
        let mut client = ClientsManager::new();

        let identifier = "test";
        let topic = "topic";
        let group = "group";
        _ = client.add(identifier, Client::new(identifier.to_owned(), 200));

        let result = client.add_tracking_info(
            &identifier,
            ClientSubscriptionInfo::new(topic.to_owned(), group.to_owned()),
        );

        assert!(result.is_ok());

        let mut remove_result = client.remove_tracking_info(&identifier, &topic, &group);

        assert!(remove_result.is_ok());

        remove_result = client.remove_tracking_info(&identifier, &topic, &group);

        assert!(remove_result.is_ok());
    }
}
