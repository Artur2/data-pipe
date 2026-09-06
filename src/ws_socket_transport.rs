use crate::client::Client;
use crate::error::{DataPipeError, DataPipeResult};
use crate::utils;
use log::{info, warn};
use sockudo_ws::{Config, Http1, Message, SplitReader, SplitWriter, Stream, WebSocketServer};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::sync::broadcast::{Receiver, Sender};

#[allow(dead_code)]
pub struct WsSocketTransport {
    clients: Arc<RwLock<HashMap<String, Client>>>,
    sender_receiver_out: (Sender<String>, Receiver<String>),
    sender_receiver_in: (Sender<String>, Receiver<String>),
}

impl WsSocketTransport {
    pub fn new() -> Arc<Self> {
        let (sender_out, receiver_out) = tokio::sync::broadcast::channel::<String>(1000);
        let (sender_in, receiver_in) = tokio::sync::broadcast::channel::<String>(1000); // TODO: Заменить на клиентский send/recv

        let mut receiver_out_clone = sender_out.subscribe();
        let mut receiver_in_clone = sender_in.subscribe();

        tokio::task::spawn(async move {
            while let Ok(message) = receiver_out_clone.recv().await {
                println!("{}", message);
            }
        });

        tokio::task::spawn(async move {
            while let Ok(message) = receiver_in_clone.recv().await {
                println!("{}", message);
            }
        });

        Arc::new(Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            sender_receiver_out: (sender_out, receiver_out),
            sender_receiver_in: (sender_in, receiver_in),
        })
    }

    pub async fn initialize(self: Arc<Self>) -> DataPipeResult<()> {
        tokio::spawn(self.bind_and_handle())
            .await
            .map_err(|e| DataPipeError::TaskError(e.to_string()))?
    }

    pub fn get_receiver(self: Arc<Self>) -> DataPipeResult<Receiver<String>> {
        Ok(self.sender_receiver_out.0.subscribe())
    }

    pub fn get_sender(self: Arc<Self>) -> DataPipeResult<Sender<String>> {
        Ok(self.sender_receiver_in.0.clone())
    }

    async fn bind_and_handle(self: Arc<Self>) -> DataPipeResult<()> {
        let listener = TcpListener::bind("127.0.0.1:7878")
            .await
            .map_err(|_| DataPipeError::CantBind)?;

        self.handle_connection(listener).await?;

        Ok(())
    }

    /// Пишет в sender из WS
    async fn web_socket_reader(
        self: Arc<Self>,
        sender: Sender<String>,
        mut reader: SplitReader<Stream<Http1>>,
        client_id: &str,
    ) -> DataPipeResult<()> {
        while let Some(Ok(msg)) = reader.next().await {
            match msg {
                Message::Text(text) => {
                    let text_as_string =
                        String::from_utf8(text.to_vec()).map_err(|_| DataPipeError::Unknown)?;
                    info!("Received message {}", &text_as_string);
                    sender
                        .send(text_as_string)
                        .map_err(|_| DataPipeError::ReceiveMessageFailed)?;
                }
                Message::Binary(_) => {
                    info!("Received binary message");
                }
                Message::Ping(_) => {
                    info!("Ping");
                }
                Message::Pong(_) => {
                    info!("Pong");
                }
                Message::Close(_) => {
                    info!("Removing client {} on close", client_id);
                    let mut clients = self.clients.write().await;
                    if clients.contains_key(client_id) {
                        clients.remove(client_id);
                    }
                }
            }
        }

        Ok(())
    }

    /// Пишет из receiver в ws
    async fn web_socket_writer(
        self: Arc<Self>,
        mut rx: Receiver<String>,
        mut ws_writer: SplitWriter<Stream<Http1>>,
        client_id: &str,
    ) -> DataPipeResult<()> {
        // receive message from outside and write it to ws
        while let Ok(msg) = rx.recv().await {
            ws_writer
                .send(Message::from(msg))
                .await
                .map_err(|_| DataPipeError::SendMessageFailed)?;
        }

        Ok(())
    }

    async fn handle_connection(self: Arc<Self>, listener: TcpListener) -> DataPipeResult<()> {
        let server = WebSocketServer::<Http1>::new(Config::default());
        server
            .serve(listener, move |ws, req| {
                let (reader, writer) = ws.split();
                let client_id = utils::get_parameter_from_query(&req.path, "clientId");
                let this = Arc::clone(&self);
                async move {
                    let mut clients_guard = this.clients.write().await;
                    if clients_guard.contains_key(&client_id) {
                        warn!("Client with same id already exists");
                        return;
                    } else {
                        let client = Client::new(client_id.clone());
                        (clients_guard).insert(client_id.clone(), client);

                        let receiver_self = Arc::clone(&this);
                        let writer_self = Arc::clone(&this);

                        let client_id_reader = client_id.clone();
                        let client_id_writer = client_id.clone();

                        tokio::spawn(async move {
                            let receiver = receiver_self.sender_receiver_in.0.subscribe();
                            receiver_self
                                .web_socket_writer(receiver, writer, &client_id_writer)
                                .await
                        });
                        tokio::spawn(async move {
                            let sender = writer_self.sender_receiver_out.0.clone();
                            writer_self
                                .web_socket_reader(sender, reader, &client_id_reader)
                                .await
                        });
                    }
                }
            })
            .await
            .map_err(|e| DataPipeError::TaskError(e.to_string()))?;

        Ok(())
    }
}
