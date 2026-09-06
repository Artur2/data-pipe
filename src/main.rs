extern crate alloc;

use crate::error::DataPipeResult;
use crate::ws_socket_transport::WsSocketTransport;
use tokio::task;

pub mod client;
pub mod error;
pub mod utils;
pub mod ws_socket_transport;

#[tokio::main]
async fn main() -> DataPipeResult<()> {
    let web_socket_transport = WsSocketTransport::new();
    let clone = web_socket_transport.clone();
    web_socket_transport.initialize().await?;

    let subscriber = clone.get_receiver()?;

    loop {
        let mut stdin = String::default();
        std::io::stdin().read_line(&mut stdin).unwrap_or_default();

        if stdin == "c" {
            break;
        }
    }

    Ok(())
}
