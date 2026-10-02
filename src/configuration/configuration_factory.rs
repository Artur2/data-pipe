use crate::configuration::configuration::DataPipeConfiguration;

pub trait ConfigurationFactory {
    fn create(&self) -> DataPipeConfiguration;
}
