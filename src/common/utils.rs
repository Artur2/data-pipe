use crate::data::error::DataPipeError::ClientIdentificationError;
use crate::data::error::DataPipeResult;

pub fn get_parameter_from_query(query: &str, name_of_parameter: &str) -> DataPipeResult<String> {
    use regex::Regex;
    let regex = Regex::new(r"[?&]([^=#]+)=([^&#]*)").unwrap();

    for caps in regex.captures_iter(query) {
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
