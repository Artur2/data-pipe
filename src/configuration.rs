pub struct Configuration {
    /// Кол-во сообщений в буфере отправки в web socket
    pub ws_inbound_channel_capacity: usize,
    /// Кол-во сообщений в буфере отправки из web socket в kafka
    pub ws_outbound_channel_capacity: usize,
}

impl Default for Configuration {
    fn default() -> Configuration {
        Configuration {
            ws_inbound_channel_capacity: 1024,
            ws_outbound_channel_capacity: 1024,
        }
    }
}