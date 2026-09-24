use crate::data::error::DataPipeError::ClientIdentificationError;
use crate::data::error::DataPipeResult;
use crate::data::messaging::data_pipe_message::DataPipeMessage;
use crate::data::messaging::data_pipe_message_type::DataPipeMessageType;
use std::sync::LazyLock;

pub fn get_parameter_from_query(query: &str, name_of_parameter: &str) -> DataPipeResult<String> {
    use regex::Regex;
    static REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[?&]([^=#]+)=([^&#]*)").unwrap());

    for caps in REGEX.captures_iter(query) {
        if let Some(cap) = caps.get(1) {
            if cap.as_str() == name_of_parameter {
                if let Some(id) = caps.get(2) {
                    if id.is_empty() {
                        return Err(ClientIdentificationError(
                            "Parameter client id is empty".to_string(),
                        ));
                    }

                    return Ok(id.as_str().to_owned());
                }
            }
        }
    }

    let error = format!("Parameter '{}' not found", name_of_parameter);
    Err(ClientIdentificationError(error))
}

pub fn create_random_message(client_identifier: String) -> DataPipeMessage {
    let mut data = [0u8; 500];
    rand::fill(&mut data);
    
    let uuid_raw = uuid::Uuid::new_v4();

    DataPipeMessage::new(
        client_identifier,
        uuid_raw.to_string(),
        DataPipeMessageType::Default,
        data.to_vec(),
        None,
        vec![],
    )
}
