use crate::common::utils;
use crate::data::client::Client;
use crate::data::error::{DataPipeError, DataPipeResult};
use log::{info, warn};
use sockudo_ws::{Config, Http1, Message, SplitReader, SplitWriter, Stream, WebSocketServer};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::sync::broadcast::{Receiver, Sender};

#[allow(dead_code)]
pub struct WsSocketService {
    clients: Arc<RwLock<HashMap<String, Client>>>,
    sender_out: Sender<String>,
}

impl WsSocketService {
    pub fn new(clients: Arc<RwLock<HashMap<String, Client>>>) -> Arc<Self> {
        let (sender_out, _) = tokio::sync::broadcast::channel::<String>(1000);

        Arc::new(Self {
            clients,
            sender_out,
        })
    }

    pub async fn initialize(self: Arc<Self>) -> DataPipeResult<()> {
        tokio::spawn(self.bind_and_handle())
            .await
            .map_err(|e| DataPipeError::TaskError(e.to_string()))?
    }

    pub fn get_receiver(self: Arc<Self>) -> DataPipeResult<Receiver<String>> {
        Ok(self.sender_out.subscribe())
    }

    async fn get_receiver_by_client_identifier(
        self: Arc<Self>,
        identifier: &String,
    ) -> DataPipeResult<Receiver<String>> {
        let clients = self.clients.read().await;
        if !clients.contains_key(identifier) {
            return Err(DataPipeError::Unknown);
        }

        let value = clients.get(identifier);
        Ok(value.unwrap().sender.subscribe())
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
                    // TODO: Add message type, not as raw string
                    let text_as_string =
                        String::from_utf8(text.to_vec()).map_err(|_| DataPipeError::Unknown)?;

                    if sender.receiver_count() > 0 {
                        sender
                            .send(text_as_string)
                            .map_err(|_| DataPipeError::ReceiveMessageFailed)?;
                    }
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
        info!("Start listening");
        let server = WebSocketServer::<Http1>::new(Config::default());
        server
            .serve(listener, move |ws, req| {
                let client_id = utils::get_parameter_from_query(&req.path, "clientId");
                let this = Arc::clone(&self);
                info!("Connecting client with id {}", client_id);

                async move {
                    {
                        let clients_guard = this.clients.write().await;
                        if clients_guard.contains_key(&client_id) {
                            warn!("Client with same id already exists, closing connection");
                            return;
                        }
                    }

                    {
                        let mut clients = this.clients.write().await;
                        if clients.contains_key(&client_id) {
                            warn!("Client with same id already added, closing connection");
                            return;
                        }

                        let client = Client::new(client_id.clone());
                        clients.insert(client_id.clone(), client);
                    }

                    let receiver_self = Arc::clone(&this);
                    let writer_self = Arc::clone(&this);

                    let client_id_reader = client_id.clone();
                    let client_id_writer = client_id.clone();

                    let (reader, writer) = ws.split();
                    tokio::task::spawn(async move {
                        let self_reference = writer_self.clone();
                        let client_receiver = self_reference
                            .get_receiver_by_client_identifier(&client_id_writer)
                            .await;

                        if client_receiver.is_err() {
                            panic!("Client is not registered");
                        }

                        writer_self
                            .web_socket_writer(client_receiver.unwrap(), writer, &client_id_writer)
                            .await
                    });
                    tokio::task::spawn(async move {
                        let sender = receiver_self.sender_out.clone();
                        receiver_self
                            .web_socket_reader(sender, reader, &client_id_reader)
                            .await
                    });
                }
            })
            .await
            .map_err(|e| DataPipeError::TaskError(e.to_string()))?;

        info!("Listener exited");

        Ok(())
    }
}
