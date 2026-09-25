use std::fmt::Formatter;

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub enum DataPipeMessageType {
    #[default]
    Default,
    Subscribe,
    Unsubscribe,
}

impl std::fmt::Display for DataPipeMessageType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataPipeMessageType::Default => {
                write!(f, "{0}", DataPipeMessageType::Default.to_string())
            }
            DataPipeMessageType::Subscribe => {
                write!(f, "{0}", DataPipeMessageType::Subscribe.to_string())
            }
            DataPipeMessageType::Unsubscribe => {
                write!(f, "{0}", DataPipeMessageType::Unsubscribe.to_string())
            }
        }
    }
}
