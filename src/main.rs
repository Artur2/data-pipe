use crate::configuration::configuration_factory::ConfigurationFactory;
use crate::configuration::default_configuration_factory::DefaultConfigurationFactory;
use crate::configuration::toml_configuration_factory::TomlConfigurationFactory;
use crate::data::error::{DataPipeError, DataPipeResult};
use crate::data_pipe_server::DataPipeServer;
use clap::Parser;
use log::LevelFilter;
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

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(
        short,
        long,
        help = "File must be toml, example: src/config, without extension"
    )]
    pub config_path: Option<String>,
}

#[tokio::main]
async fn main() -> DataPipeResult<()> {
    TermLogger::init(
        LevelFilter::Info,   // Log level
        Config::default(),   // Config options
        TerminalMode::Mixed, // Terminal mode
        ColorChoice::Auto,   // Automatically use colors if supported
    )
    .unwrap();

    let args = Args::parse();

    let config_factory = create_configuration_factory(args.config_path)?;
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
    path_to_configuration_file: Option<String>,
) -> DataPipeResult<Box<dyn ConfigurationFactory>> {
    if let Some(path_to_configuration_file) = path_to_configuration_file {
        let toml_configuration_factory =
            TomlConfigurationFactory::new(path_to_configuration_file.to_owned());
        Ok(Box::new(toml_configuration_factory))
    } else {
        Ok(Box::new(DefaultConfigurationFactory::new()))
    }
}
