pub struct DataPipeConfiguration {
    /// Кол-во сообщений в буфере отправки в web socket
    pub ws_inbound_channel_capacity: usize,
    /// Кол-во сообщений в буфере отправки из web socket в Kafka
    pub ws_outbound_channel_capacity: usize,
    /// Адрес для биндинга ws
    pub ws_host: String,
    /// Порт для биндинга ws
    pub ws_port: u16,
    /// Разрешенное кол-во сообщений в секунду
    pub ws_socket_incoming_message_rate_limit: u32,
    /// Сервера bootstrap для Kafka
    pub bootstrap_servers: String,
    /// Таймаут при отправке сообщения в Кафку
    pub kafka_message_send_timeout: String,
    /// Таймаут сессии Кафки
    pub kafka_session_timeout: String,
    /// Собирать ли статистику
    pub harvest_statistics: bool,
}

impl DataPipeConfiguration {
    
    pub fn new() -> Self {
        DataPipeConfiguration::default()
    }
    
    pub fn get_ws_binding_address(&self) -> String {
        format!("{}:{}", self.ws_host, self.ws_port)
    }
}

impl Default for DataPipeConfiguration {
    fn default() -> DataPipeConfiguration {
        DataPipeConfiguration {
            ws_inbound_channel_capacity: 10_024,
            ws_outbound_channel_capacity: 10_024,
            ws_host: "127.0.0.1".to_owned(),
            ws_port: 7878,
            ws_socket_incoming_message_rate_limit: 100,
            bootstrap_servers: "localhost:9092".to_owned(),
            kafka_message_send_timeout: "5000".to_owned(),
            kafka_session_timeout: "6000".to_owned(),
            harvest_statistics: false,
        }
    }
}
