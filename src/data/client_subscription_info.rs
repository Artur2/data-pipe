#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct ClientSubscriptionInfo {
    pub group: String,
    pub topic: String,
}

impl ClientSubscriptionInfo {
    pub fn new(group: String, topic: String) -> ClientSubscriptionInfo {
        ClientSubscriptionInfo { group, topic }
    }
}
