use crate::configuration::configuration::DataPipeConfiguration;
use crate::configuration::configuration_factory::ConfigurationFactory;
use crate::data::error::{DataPipeError, DataPipeResult};
use config::Config;

pub struct TomlConfigurationFactory {
    path: String,
}

impl TomlConfigurationFactory {
    pub fn new(path: String) -> TomlConfigurationFactory {
        TomlConfigurationFactory { path }
    }
}

impl ConfigurationFactory for TomlConfigurationFactory {
    fn create(&self) -> DataPipeResult<DataPipeConfiguration> {
        let build_result = Config::builder()
            .add_source(config::File::with_name(&self.path))
            .build();

        if build_result.is_err() {
            return Err(DataPipeError::ConfigurationError(
                format!("Cant parse or locate file, passed value - {0}", self.path).to_owned(),
            ));
        }

        let build = build_result.unwrap();
        let mut config = DataPipeConfiguration::new();

        config.ws_host = build
            .get_string("ws_host")
            .map_err(|_| DataPipeError::ConfigurationError("Cant parse ws_host".to_owned()))?;
        config.ws_port = build
            .get_int("ws_port")
            .map_err(|_| DataPipeError::ConfigurationError("Cant parse ws_port".to_owned()))?
            as u16;
        config.ws_inbound_channel_capacity =
            build.get_int("ws_inbound_channel_capacity").map_err(|_| {
                DataPipeError::ConfigurationError(
                    "Cant parse ws_inbound_channel_capacity".to_owned(),
                )
            })? as usize;
        config.ws_outbound_channel_capacity =
            build.get_int("ws_outbound_channel_capacity").map_err(|_| {
                DataPipeError::ConfigurationError(
                    "Cant parse ws_outbound_channel_capacity".to_owned(),
                )
            })? as usize;
        config.bootstrap_servers = build.get_string("bootstrap_servers").map_err(|_| {
            DataPipeError::ConfigurationError("Cant parse bootstrap_servers".to_owned())
        })?;
        config.ws_socket_incoming_message_rate_limit = build
            .get_int("ws_socket_incoming_message_rate_limit")
            .map_err(|_| {
                DataPipeError::ConfigurationError(
                    "Cant parse ws_socket_incoming_message_rate_limit".to_owned(),
                )
            })? as u32;
        config.kafka_message_send_timeout = build
            .get_string("kafka_message_send_timeout")
            .map_err(|_| {
                DataPipeError::ConfigurationError(
                    "Cant parse kafka_message_send_timeout".to_owned(),
                )
            })?;
        config.kafka_session_timeout = build.get_string("kafka_session_timeout").map_err(|_| {
            DataPipeError::ConfigurationError("Cant parse kafka_session_timeout".to_owned())
        })?;
        config.harvest_statistics = build.get_bool("harvest_statistics").map_err(|_| {
            DataPipeError::ConfigurationError("Cant parse harvest_statistics".to_owned())
        })?;

        Ok(config)
    }
}
