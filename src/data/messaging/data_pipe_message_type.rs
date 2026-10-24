use std::fmt::Formatter;

#[derive(Default, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum DataPipeMessageType {
    #[default]
    Default,
    Subscribe,
    Unsubscribe,
    Ack,
}

impl std::fmt::Display for DataPipeMessageType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataPipeMessageType::Default => {
                write!(f, "Default")
            }
            DataPipeMessageType::Subscribe => {
                write!(f, "Subscribe")
            }
            DataPipeMessageType::Unsubscribe => {
                write!(f, "Unsubscribe")
            }
            DataPipeMessageType::Ack => {
                write!(f, "Ack")
            }
        }
    }
}
