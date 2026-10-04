use crate::common::utils;
use crate::configuration::configuration::DataPipeConfiguration;
use crate::data::client::Client;
use crate::data::clients_manager::ClientsManager;
use crate::data::error::{DataPipeError, DataPipeResult};
use crate::data::messaging::data_pipe_message::DataPipeMessage;
use crate::metrics::statistics::Statistics;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use log::{info, warn};
use parking_lot::RwLock;
use sockudo_ws::error::CloseReason;
use sockudo_ws::{Config, Http1, Message, SplitReader, SplitWriter, Stream, WebSocketServer};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
pub struct WebSocketService {
    configuration: Arc<DataPipeConfiguration>,
    clients: Arc<RwLock<ClientsManager>>,
    sender_out: Sender<DataPipeMessage>,
    statistics: Arc<Statistics>,
    rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

impl WebSocketService {
    pub fn new(
        clients: Arc<RwLock<ClientsManager>>,
        configuration: Arc<DataPipeConfiguration>,
        statistics: Arc<Statistics>,
    ) -> Arc<Self> {
        let (sender_out, _) = tokio::sync::broadcast::channel::<DataPipeMessage>(
            configuration.ws_outbound_channel_capacity,
        );

        let quota = Quota::per_second(
            NonZeroU32::new(configuration.ws_incoming_message_rate_limit).unwrap(),
        );
        let rate_limiter = Arc::new(RateLimiter::direct(quota));

        Arc::new(Self {
            clients,
            sender_out,
            configuration,
            statistics,
            rate_limiter,
        })
    }

    pub async fn initialize(self: Arc<Self>) -> DataPipeResult<()> {
        let cloned_self = self.clone();
        tokio::spawn(self.bind());
        cloned_self.start_statistics_loop();

        Ok(())
    }

    pub fn get_receiver(self: Arc<Self>) -> DataPipeResult<Receiver<DataPipeMessage>> {
        Ok(self.sender_out.subscribe())
    }

    fn get_receiver_by_client_identifier(
        self: Arc<Self>,
        identifier: &String,
    ) -> DataPipeResult<Receiver<DataPipeMessage>> {
        let clients = self.clients.read();
        if !clients.contains(identifier) {
            return Err(DataPipeError::ClientNotFound);
        }

        let value = clients.get(identifier);
        Ok(value.unwrap().sender.subscribe())
    }

    async fn bind(self: Arc<Self>) -> DataPipeResult<()> {
        let listener = TcpListener::bind(self.configuration.get_ws_binding_address())
            .await
            .map_err(|_| DataPipeError::CantBind)?;

        self.serve_connection(listener).await?;

        Ok(())
    }

    /// Пишет в sender из WS
    async fn web_socket_reader(
        self: Arc<Self>,
        sender: Sender<DataPipeMessage>,
        mut reader: SplitReader<Stream<Http1>>,
        client_id: &str,
        cancellation_token: CancellationToken,
    ) -> DataPipeResult<()> {
        while let Some(Ok(msg)) = reader.next().await {
            let cloned_self = self.clone();
            let cloned_rate_limiter = self.rate_limiter.clone();
            match msg {
                Message::Text(text) => {
                    cloned_self.write_per_message_statistics();
                    cloned_rate_limiter.until_ready().await;

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
                Message::Close(_) => cancellation_token.cancel(),
                _ => warn!("Not supported message request"),
            }
        }

        Ok(())
    }

    /// Пишет из receiver в ws
    async fn web_socket_writer(
        self: Arc<Self>,
        mut rx: Receiver<DataPipeMessage>,
        mut ws_writer: SplitWriter<Stream<Http1>>,
        cancellation_token: CancellationToken,
    ) -> DataPipeResult<SplitWriter<Stream<Http1>>> {
        // receive message from outside and write it to ws

        loop {
            let message = tokio::select! {
            message = rx.recv() => { message }
            _ = cancellation_token.cancelled() => break };

            if let Ok(msg) = message {
                let message = serde_json::to_string(&msg).unwrap();

                ws_writer
                    .send(Message::from(message))
                    .await
                    .map_err(|_| DataPipeError::SendMessageFailed)?;
            } else {
                break;
            }
        }

        Ok(ws_writer)
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

    fn register_client(self: Arc<Self>, client_id: &str) -> DataPipeResult<()> {
        {
            let clients = self.clients.read();
            if clients.contains(client_id) {
                warn!("Client with same id already exists, closing connection");
                return Err(DataPipeError::ClientAlreadyExist);
            }
        }

        let mut clients = self.clients.write();
        let client = Client::new(
            client_id.to_string().clone(),
            self.configuration.ws_inbound_channel_capacity,
        );

        clients.add(client_id, client)
    }

    async fn serve_connection(self: Arc<Self>, listener: TcpListener) -> DataPipeResult<()> {
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
                    let cancellation_token = CancellationToken::new();
                    let reader_cancellation_token = cancellation_token.clone();
                    let writer_cancellation_token = cancellation_token.clone();

                    let result = client_registration_self.register_client(&client_id);
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
                    let writer_result = tokio::task::spawn(async move {
                        let self_reference = writer_self.clone();
                        let client_receiver =
                            self_reference.get_receiver_by_client_identifier(&client_id_writer);

                        if client_receiver.is_err() {
                            warn!("Client is not registered");
                            return Ok(writer);
                        }

                        writer_self
                            .web_socket_writer(
                                client_receiver.unwrap(),
                                writer,
                                writer_cancellation_token,
                            )
                            .await
                    });

                    tokio::task::spawn(async move {
                        let sender = receiver_self.sender_out.clone();
                        _ = receiver_self
                            .web_socket_reader(
                                sender,
                                reader,
                                &client_id_reader,
                                reader_cancellation_token,
                            )
                            .await;
                    });

                    cancellation_token.cancelled().await; // Awaiting close sent by ws client

                    let writer_task_result = writer_result.await.unwrap();
                    if let Ok(writer) = writer_task_result {
                        this.close_connection_gracefully(writer, &client_id).await;
                    }
                }
            })
            .await
            .map_err(|e| DataPipeError::TaskError(e.to_string()))?;

        info!("Listener exited");

        Ok(())
    }

    fn remove_client(self: Arc<Self>, client_id: &str) {
        {
            let clients = self.clients.read();
            if !clients.contains(client_id) {
                return;
            }
        }

        info!("Cancelling client subscriptions {} on close", client_id);
        let mut clients = self.clients.write();
        let client = clients.get(client_id).unwrap();
        for subscription_info in &client.subscription_infos {
            let client_holder = clients
                .get_client_cancellation_holder(&subscription_info.topic, &subscription_info.group);
            let token = &client_holder.unwrap().token;
            token.cancel();
        }

        clients.remove(client_id);
    }

    fn start_statistics_loop(self: Arc<Self>) {
        if self.configuration.harvest_statistics {
            let cloned_statistics = self.statistics.clone();
            let cloned_sender_out = self.sender_out.clone();
            tokio::spawn(async move {
                loop {
                    let len = cloned_sender_out.len();
                    let statistics = cloned_statistics.clone();
                    statistics.set_out_broadcast_messages_count(len as u32);
                    sleep(Duration::from_secs(1)).await;
                }
            });
        }
    }

    fn write_per_message_statistics(self: Arc<Self>) {
        if self.configuration.harvest_statistics {
            let statistics_for_requests = self.statistics.clone();
            statistics_for_requests.increment_requests_per_second_ws_out();
        }
    }

    async fn close_connection_gracefully(
        self: Arc<Self>,
        mut writer: SplitWriter<Stream<Http1>>,
        client_id: &str,
    ) {
        const DISCONNECTION_NORMALLY_CODE: u16 = 1000;
        let close_reason =
            CloseReason::new(DISCONNECTION_NORMALLY_CODE, "Connection closed by client");
        let _ = writer.send(Message::Close(Some(close_reason))).await;
        let _ = writer.flush().await;
        self.remove_client(client_id);
    }
}
