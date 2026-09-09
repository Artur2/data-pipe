use crate::data::error::DataPipeResult;
use crate::data_pipe_server::DataPipeServer;
pub mod data;
pub mod data_pipe_server;
pub mod services;
pub mod common;

#[tokio::main]
async fn main() -> DataPipeResult<()> {
    let data_pipe_server = DataPipeServer::new();
    Ok(())
}
