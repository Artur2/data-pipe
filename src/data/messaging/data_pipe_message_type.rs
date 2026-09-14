use std::fmt::Formatter;

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub enum DataPipeMessageType {
    #[default]
    Default,
    Subscription,
}

impl std::fmt::Display for DataPipeMessageType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataPipeMessageType::Default => {
                write!(f, "Default")
            }
            DataPipeMessageType::Subscription => {
                write!(f, "Subscription")
            }
        }
    }
}
