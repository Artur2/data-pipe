use crate::data::messaging::data_pipe_message_type::DataPipeMessageType;
use std::fmt::{Formatter};

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataPipeMessage {
    pub client_identifier: String,
    pub message_type: DataPipeMessageType,
    pub data: Vec<u8>,
    pub topic: Option<String>,
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
