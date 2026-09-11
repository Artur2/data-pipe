
#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub enum DataPipeMessageType {
    #[default]
    Default,
    Subscription
}