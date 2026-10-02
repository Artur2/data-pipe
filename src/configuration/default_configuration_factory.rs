use crate::configuration::configuration::DataPipeConfiguration;
use crate::configuration::configuration_factory::ConfigurationFactory;
use crate::data::error::DataPipeResult;

pub struct DefaultConfigurationFactory;

impl DefaultConfigurationFactory {
    pub fn new() -> Self {
        DefaultConfigurationFactory
    }
}

impl ConfigurationFactory for DefaultConfigurationFactory {
    fn create(&self) -> DataPipeResult<DataPipeConfiguration> {
        Ok(DataPipeConfiguration::default())
    }
}