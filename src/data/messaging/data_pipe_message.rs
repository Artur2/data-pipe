use crate::data::client_subscription_info::ClientSubscriptionInfo;
use crate::data::error::{DataPipeError, DataPipeResult};
use crate::data::messaging::data_pipe_message_header::DataPipeMessageHeader;
use crate::data::messaging::data_pipe_message_type::DataPipeMessageType;
use rmp_serde::Serializer;
use serde::Serialize;
use std::fmt::Formatter;

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataPipeMessage {
    pub client_identifier: String,
    pub message_identifier: String,
    pub headers: Vec<DataPipeMessageHeader>,
    pub message_type: DataPipeMessageType,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub topic: Option<String>,
}

impl DataPipeMessage {
    pub fn new(
        client_identifier: String,
        message_identifier: String,
        message_type: DataPipeMessageType,
        data: Vec<u8>,
        topic: Option<String>,
        headers: Vec<DataPipeMessageHeader>,
    ) -> Self {
        DataPipeMessage {
            client_identifier,
            message_identifier,
            message_type,
            data,
            topic,
            headers,
        }
    }

    pub fn is_subscribe(&self) -> bool {
        self.message_type == DataPipeMessageType::Subscribe
    }

    pub fn is_unsubscribe(&self) -> bool {
        self.message_type == DataPipeMessageType::Unsubscribe
    }

    pub fn with_subscription(
        client_identifier: String,
        message_identifier: String,
        subscription_infos: &[ClientSubscriptionInfo],
        topic: Option<String>,
        headers: Vec<DataPipeMessageHeader>,
    ) -> DataPipeResult<Self> {
        let mut buffer = vec![];
        subscription_infos
            .serialize(&mut Serializer::new(&mut buffer))
            .map_err(|_| DataPipeError::SerializationError)?;

        Ok(DataPipeMessage {
            client_identifier,
            message_identifier,
            message_type: DataPipeMessageType::Subscribe,
            data: buffer,
            topic,
            headers,
        })
    }

    pub fn with_unsubscribe(
        client_identifier: String,
        message_identifier: String,
        subscription_infos: &[ClientSubscriptionInfo],
        topic: Option<String>,
        headers: Vec<DataPipeMessageHeader>,
    ) -> DataPipeResult<Self> {
        let mut buffer = vec![];
        subscription_infos
            .serialize(&mut Serializer::new(&mut buffer))
            .map_err(|_| DataPipeError::SerializationError)?;

        Ok(DataPipeMessage {
            client_identifier,
            message_identifier,
            message_type: DataPipeMessageType::Unsubscribe,
            data: buffer,
            topic,
            headers,
        })
    }

    pub fn deserialize_subscription_data(&self) -> DataPipeResult<Vec<ClientSubscriptionInfo>> {
        if !self.is_unsubscribe() && !self.is_subscribe() {
            return Err(DataPipeError::NotSuitableCall(
                "Not allowed to get sub/unsub info".to_owned(),
            ));
        }

        let result = rmp_serde::from_slice::<Vec<ClientSubscriptionInfo>>(&self.data);
        Ok(result.map_err(|_| DataPipeError::SerializationError)?)
    }
}

impl std::fmt::Display for DataPipeMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let topic = self.topic.clone().unwrap_or_else(|| "-".to_string());
        write!(
            f,
            "Message from {}, type {}, topic {}",
            self.client_identifier, self.message_type, topic
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn should_serialize_message_for_subscription() {
        let topic = "test_topic";
        let group = "test_group";
        let client_identifier = "identifier";
        let message_identifier = "message_identifier";
        let subscription_infos = vec![ClientSubscriptionInfo {
            topic: topic.to_owned(),
            group: group.to_owned(),
        }];

        let message = DataPipeMessage::with_subscription(
            client_identifier.to_owned(),
            message_identifier.to_owned(),
            &subscription_infos,
            None,
            vec![],
        );

        assert!(message.is_ok());

        let message_unwrapped = message.unwrap();
        assert!(message_unwrapped.data.len() > 0);
        assert!(message_unwrapped.is_subscribe());
    }

    #[test]
    pub fn should_serialize_message_for_unsubscribe() {
        let topic = "test_topic";
        let group = "test_group";
        let client_identifier = "identifier";
        let message_identifier = "message_identifier";
        let subscription_infos = vec![ClientSubscriptionInfo {
            topic: topic.to_owned(),
            group: group.to_owned(),
        }];

        let message = DataPipeMessage::with_unsubscribe(
            client_identifier.to_owned(),
            message_identifier.to_owned(),
            &subscription_infos,
            None,
            vec![],
        );

        assert!(message.is_ok());

        let message_unwrapped = message.unwrap();
        assert!(message_unwrapped.data.len() > 0);
        assert!(message_unwrapped.is_unsubscribe() == true);
    }

    #[test]
    pub fn should_deserialize_correctly() {
        let topic = "test_topic";
        let group = "test_group";
        let client_identifier = "identifier";
        let message_identifier = "message_identifier";
        let subscription_infos = vec![ClientSubscriptionInfo {
            topic: topic.to_owned(),
            group: group.to_owned(),
        }];

        let message = DataPipeMessage::with_unsubscribe(
            client_identifier.to_owned(),
            message_identifier.to_owned(),
            &subscription_infos,
            None,
            vec![],
        );

        assert!(message.is_ok());
        let message_unwrapped = message.unwrap();
        assert!(message_unwrapped.data.len() > 0);

        let infos_result = message_unwrapped.deserialize_subscription_data();
        assert!(infos_result.is_ok());
        let infos = infos_result.unwrap();

        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].topic, topic);
        assert_eq!(infos[0].group, group);
    }

    #[test]
    pub fn should_return_error_when_trying_get_sub_info_for_different_type() {
        let client_identifier = "identifier";
        let message_identifier = "message_identifier";

        let message = DataPipeMessage::new(
            client_identifier.to_owned(),
            message_identifier.to_owned(),
            DataPipeMessageType::Default,
            vec![],
            None,
            vec![],
        );

        let result = message.deserialize_subscription_data();
        assert!(result.is_err());
    }
}
