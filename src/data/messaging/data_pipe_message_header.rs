#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataPipeMessageHeader {
    pub key: String,
    pub value: String,
}
