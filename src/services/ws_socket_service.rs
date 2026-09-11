use crate::common::utils;
use crate::data::client::Client;
use crate::data::error::{DataPipeError, DataPipeResult};
use crate::data::messaging::data_pipe_message::DataPipeMessage;
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
    sender_out: Sender<DataPipeMessage>,
}

impl WsSocketService {
    pub fn new(clients: Arc<RwLock<HashMap<String, Client>>>) -> Arc<Self> {
        let (sender_out, _) = tokio::sync::broadcast::channel::<DataPipeMessage>(1000);

        Arc::new(Self {
            clients,
            sender_out,
        })
    }

    pub async fn initialize(self: Arc<Self>) -> DataPipeResult<()> {
        tokio::spawn(self.bind_and_handle());

        Ok(())
    }

    pub fn get_receiver(self: Arc<Self>) -> DataPipeResult<Receiver<DataPipeMessage>> {
        Ok(self.sender_out.subscribe())
    }

    async fn get_receiver_by_client_identifier(
        self: Arc<Self>,
        identifier: &String,
    ) -> DataPipeResult<Receiver<DataPipeMessage>> {
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
        sender: Sender<DataPipeMessage>,
        mut reader: SplitReader<Stream<Http1>>,
        client_id: &str,
    ) -> DataPipeResult<()> {
        while let Some(Ok(msg)) = reader.next().await {
            match msg {
                Message::Text(text) => {
                    let text_as_string =
                        String::from_utf8(text.to_vec()).map_err(|_| DataPipeError::Unknown);

                    if text_as_string.is_err() {
                        warn!("Can't parse text from websocket message");
                        continue;
                    }

                    if sender.receiver_count() > 0 {
                        let message =
                            serde_json::from_str::<DataPipeMessage>(&text_as_string.unwrap());
                        if message.is_err() {
                            warn!("Can't parse message from websocket message");
                            continue;
                        }

                        let send_result = sender.send(message.unwrap());
                        if send_result.is_err() {
                            warn!("Can't send message to websocket");
                            continue;
                        }
                    } else {
                        warn!("Can't send message to receivers");
                        continue;
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
        mut rx: Receiver<DataPipeMessage>,
        mut ws_writer: SplitWriter<Stream<Http1>>,
        client_id: &str,
    ) -> DataPipeResult<()> {
        // receive message from outside and write it to ws
        while let Ok(msg) = rx.recv().await {
            let message = serde_json::to_string(&msg).unwrap();

            ws_writer
                .send(Message::from(message))
                .await
                .map_err(|_| DataPipeError::SendMessageFailed)?;
        }

        Ok(())
    }

    async fn register_client(self: Arc<Self>, client_id: &str) -> DataPipeResult<()> {
        {
            let clients_guard = self.clients.write().await;
            if clients_guard.contains_key(client_id) {
                warn!("Client with same id already exists, closing connection");
                return Err(DataPipeError::ClientAlreadyExist);
            }
        }

        {
            let mut clients = self.clients.write().await;
            if clients.contains_key(client_id) {
                warn!("Client with same id already added, closing connection");
                return Err(DataPipeError::ClientAlreadyExist);
            }

            let client = Client::new(client_id.to_string().clone());
            clients.insert(client_id.to_string().clone(), client);
        }

        Ok(())
    }

    async fn handle_connection(self: Arc<Self>, listener: TcpListener) -> DataPipeResult<()> {
        info!("Start listening");
        let server = WebSocketServer::<Http1>::new(Config::default());
        server
            .serve(listener, move |ws, req| {
                let this = Arc::clone(&self);

                async move {
                    let client_id_result = utils::get_parameter_from_query(&req.path, "clientId");

                    if client_id_result.is_err() {
                        warn!("Cant parse client id, {}", client_id_result.err().unwrap());
                        return;
                    }

                    let client_id = client_id_result.unwrap();

                    info!("Connecting client with id {}", client_id);

                    let receiver_self = Arc::clone(&this);
                    let writer_self = Arc::clone(&this);
                    let client_registration_self = Arc::clone(&this);

                    let result = client_registration_self.register_client(&client_id).await;
                    if result.is_err() {
                        warn!(
                            "Can't register client with id {}, error: {}",
                            client_id,
                            result.err().unwrap()
                        );
                        return;
                    }

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
