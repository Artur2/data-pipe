use crate::ws_socket_transport::WsSocketTransport;

pub mod client;
pub mod utils;
pub mod ws_socket_transport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Add receiver and sender
    let web_socket_transport = WsSocketTransport::new();
    web_socket_transport.initialize().await?;
    loop {
        let mut stdin = String::default();
        std::io::stdin().read_line(&mut stdin).unwrap_or_default();

        if stdin == "c" {
            break;
        }
    }

    Ok(())
}
