use std::collections::VecDeque;

#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct ClientSubscriptionInfo {
    pub group: String,
    pub topic: String,
    #[serde(skip)]
    pub pending_acks: VecDeque<(String, i64)>,
}

impl ClientSubscriptionInfo {
    pub fn new(group: String, topic: String) -> ClientSubscriptionInfo {
        ClientSubscriptionInfo {
            group,
            topic,
            pending_acks: VecDeque::new(),
        }
    }
}
