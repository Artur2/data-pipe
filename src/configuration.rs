pub struct Configuration {
    /// Кол-во сообщений в буфере отправки в web socket
    pub ws_inbound_channel_capacity: usize,
    /// Кол-во сообщений в буфере отправки из web socket в kafka
    pub ws_outbound_channel_capacity: usize,
    /// Адрес для биндинга ws
    pub ws_host: String,
    /// Порт для биндинга ws
    pub ws_port: u16,
}

impl Configuration {
    pub fn get_ws_binding_address(&self) -> String {
        format!("{}:{}", self.ws_host, self.ws_port)
    }
}

impl Default for Configuration {
    fn default() -> Configuration {
        Configuration {
            ws_inbound_channel_capacity: 1024,
            ws_outbound_channel_capacity: 1024,
            ws_host: "127.0.0.1".to_owned(),
            ws_port: 7878,
        }
    }
}
