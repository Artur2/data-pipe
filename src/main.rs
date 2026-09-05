use crate::ws_socket_transport::WsSocketTransport;

pub mod client;
pub mod utils;
pub mod ws_socket_transport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Add receiver and sender
    let (sender, receiver) = tokio::sync::mpsc::channel::<String>(1024);
    let web_socket_transport = WsSocketTransport::new(receiver, sender);
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
