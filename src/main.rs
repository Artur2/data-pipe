use crate::configuration::configuration_factory::ConfigurationFactory;
use crate::configuration::default_configuration_factory::DefaultConfigurationFactory;
use crate::configuration::toml_configuration_factory::TomlConfigurationFactory;
use crate::data::error::{DataPipeError, DataPipeResult};
use crate::data_pipe_server::DataPipeServer;
use log::{LevelFilter, error};
use mimalloc::MiMalloc;
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod common;
mod configuration;
mod data;
mod data_pipe_server;
mod metrics;
mod services;

#[tokio::main]
async fn main() -> DataPipeResult<()> {
    TermLogger::init(
        LevelFilter::Info,   // Log level
        Config::default(),   // Config options
        TerminalMode::Mixed, // Terminal mode
        ColorChoice::Auto,   // Automatically use colors if supported
    )
    .unwrap();

    let config_factory = create_configuration_factory(None)?;
    let config = config_factory.create()?;

    let data_pipe_server = DataPipeServer::new(config);
    data_pipe_server.initialize().await?;

    loop {
        let mut input = String::default();

        std::io::stdin()
            .read_line(&mut input)
            .map_err(|_| DataPipeError::Unknown)?;

        if input.trim() == "c" {
            break;
        }
    }

    Ok(())
}

fn create_configuration_factory(
    path_to_configuration_file: Option<&str>,
) -> DataPipeResult<Box<dyn ConfigurationFactory>> {
    #[allow(unused_variables)]
    if let Some(path_to_configuration_file) = path_to_configuration_file {
        let toml_configuration_factory =
            TomlConfigurationFactory::new(path_to_configuration_file.to_owned());
        Ok(Box::new(toml_configuration_factory))
    } else {
        Ok(Box::new(DefaultConfigurationFactory::new()))
    }
}
