use std::fmt::Formatter;
use crate::data::messaging::data_pipe_message_type::DataPipeMessageType;

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataPipeMessage {
    pub client_identifier: String,
    pub message_type: DataPipeMessageType,
    pub data: Vec<u8>,
    pub topic: Option<String>,
}

impl std::fmt::Display for DataPipeMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Message from {}", self.client_identifier)
    }
}