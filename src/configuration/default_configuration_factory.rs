use crate::configuration::configuration::DataPipeConfiguration;
use crate::configuration::configuration_factory::ConfigurationFactory;

pub struct DefaultConfigurationFactory;

impl DefaultConfigurationFactory {
    pub fn new() -> Self {
        DefaultConfigurationFactory
    }
}

impl ConfigurationFactory for DefaultConfigurationFactory {
    fn create(&self) -> DataPipeConfiguration {
        DataPipeConfiguration::default()
    }
}