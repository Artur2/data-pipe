use crate::configuration::configuration::DataPipeConfiguration;
use crate::configuration::configuration_factory::ConfigurationFactory;

pub struct TomlConfigurationFactory;

impl TomlConfigurationFactory {
    pub fn new() -> TomlConfigurationFactory {
        TomlConfigurationFactory {}
    }
}

impl ConfigurationFactory for TomlConfigurationFactory {
    fn create(&self) -> DataPipeConfiguration {
        todo!()
    }
}