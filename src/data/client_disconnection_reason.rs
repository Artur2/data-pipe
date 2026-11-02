
pub enum ClientDisconnectionReason {
    Unknown,
    Normal,
    DuplicateClient(String)
}