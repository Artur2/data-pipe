use std::collections::VecDeque;
pub struct ClientTrackingInfo {
    pub group: String,
    pub topic: String,
    pub processed_acks: VecDeque<(String, i64)>,
}

impl ClientTrackingInfo {
    pub fn new(group: String, topic: String) -> Self {
        Self {
            group,
            topic,
            processed_acks: VecDeque::new(),
        }
    }
}