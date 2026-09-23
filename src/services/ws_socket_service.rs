use crate::common::utils;
use crate::configuration::Configuration;
use crate::data::client::Client;
use crate::data::clients_manager::ClientsManager;
use crate::data::error::{DataPipeError, DataPipeResult};
use crate::data::messaging::data_pipe_message::DataPipeMessage;
use log::{info, warn};
use sockudo_ws::{Config, Http1, Message, SplitReader, SplitWriter, Stream, WebSocketServer};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::sync::broadcast::{Receiver, Sender};

#[allow(dead_code)]
pub struct WsSocketService {
    configuration: Arc<Configuration>,
    clients: Arc<RwLock<ClientsManager>>,
    sender_out: Sender<DataPipeMessage>,
}

impl WsSocketService {
    pub fn new(
        clients: Arc<RwLock<ClientsManager>>,
        configuration: Arc<Configuration>,
    ) -> Arc<Self> {
        let (sender_out, _) = tokio::sync::broadcast::channel::<DataPipeMessage>(
            configuration.ws_outbound_channel_capacity,
        );

        Arc::new(Self {
            clients,
            sender_out,
            configuration,
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
        if !clients.contains(identifier) {
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

                    match self
                        .clone()
                        .try_receive_message(sender.clone(), text_as_string?)
                        .await
                    {
                        Err(e) => {
                            warn!("Can't receive message, reason {}", e);
                            continue;
                        }
                        _ => {}
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
                    if clients.contains(client_id) {
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

    async fn try_receive_message(
        self: Arc<Self>,
        sender: Sender<DataPipeMessage>,
        message: String,
    ) -> DataPipeResult<()> {
        if sender.receiver_count() > 0 {
            let message_parse_result = serde_json::from_str::<DataPipeMessage>(&message);
            if message_parse_result.is_err() {
                return Err(DataPipeError::CantParseMessage(message));
            }

            let send_result = sender.send(message_parse_result.unwrap());
            if send_result.is_err() {
                return Err(DataPipeError::SendMessageFailed);
            }
        } else {
            return Err(DataPipeError::ReceiveMessageFailed);
        }

        Ok(())
    }

    async fn register_client(self: Arc<Self>, client_id: &str) -> DataPipeResult<()> {
        {
            let clients_guard = self.clients.read().await;
            if clients_guard.contains(client_id) {
                warn!("Client with same id already exists, closing connection");
                return Err(DataPipeError::ClientAlreadyExist);
            }
        }

        let mut clients = self.clients.write().await;
        let client = Client::new(
            client_id.to_string().clone(),
            self.configuration.ws_inbound_channel_capacity,
        );
        clients.add(client_id, client);

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
