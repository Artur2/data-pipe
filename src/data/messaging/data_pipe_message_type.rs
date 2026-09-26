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
                write!(f, "{0}", "Default")
            }
            DataPipeMessageType::Subscribe => {
                write!(f, "{0}", "Subscribe")
            }
            DataPipeMessageType::Unsubscribe => {
                write!(f, "{0}", "Unsubscribe")
            }
        }
    }
}
