use std::collections::VecDeque;
pub struct ClientTrackingInfo {
    pub group: String,
    pub topic: String,

    /// First is message identifier, second is offset, third is acked
    pub processing_acks: VecDeque<ClientTrackingData>,
}

impl ClientTrackingInfo {
    pub fn new(group: String, topic: String) -> Self {
        Self {
            group,
            topic,
            processing_acks: VecDeque::new(),
        }
    }
}

pub struct ClientTrackingData {
    pub message_id: String,
    pub offset: i64,
    pub acked: bool,
    pub partition: u16,
}

impl ClientTrackingData {
    pub fn new(message_id: String, offset: i64, acked: bool, partition: u16) -> Self {
        Self {
            message_id,
            offset,
            acked,
            partition,
        }
    }
}
