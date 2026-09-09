use crate::data::error::DataPipeResult;
use crate::data_pipe_server::DataPipeServer;
use log::LevelFilter;
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};
pub mod common;
pub mod data;
pub mod data_pipe_server;
pub mod services;

#[tokio::main]
async fn main() -> DataPipeResult<()> {
    TermLogger::init(
        LevelFilter::Info,   // Log level
        Config::default(),   // Config options
        TerminalMode::Mixed, // Terminal mode
        ColorChoice::Auto,   // Automatically use colors if supported
    )
    .unwrap();

    let data_pipe_server = DataPipeServer::new();
    data_pipe_server.initialize().await?;

    Ok(())
}
