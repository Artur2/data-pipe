use crate::configuration::configuration::DataPipeConfiguration;
use crate::data::error::DataPipeResult;

pub trait ConfigurationFactory {
    fn create(&self) -> DataPipeResult<DataPipeConfiguration>;
}
