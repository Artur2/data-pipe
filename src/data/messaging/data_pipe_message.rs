use crate::data::messaging::data_pipe_message_type::DataPipeMessageType;
use std::fmt::{Formatter};

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataPipeMessage {
    pub client_identifier: String,
    pub message_type: DataPipeMessageType,
    pub data: Vec<u8>,
    pub topic: Option<String>,
}

impl DataPipeMessage {
    pub fn new(client_identifier: String, message_type: DataPipeMessageType, data: Vec<u8>, topic: Option<String>) -> Self {
        DataPipeMessage {
            client_identifier,
            message_type,
            data,
            topic
        }
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
