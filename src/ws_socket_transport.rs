use crate::client::Client;
use crate::utils;
use log::{error, info, warn};
use sockudo_ws::{Config, Http1, Message, SplitReader, SplitWriter, Stream, WebSocketServer};
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender};

#[allow(dead_code)]
pub struct WsSocketTransport {
    /// Для отправки сообщений во внешние системы (out_msg)
    sender: Sender<String>,

    /// Для отправки сообщения внешними системами в ws (in_msg)
    receiver: Arc<Mutex<Receiver<String>>>,

    clients: Arc<Mutex<HashMap<String, Client>>>,
}

impl WsSocketTransport {
    pub fn new(outside_receiver: Receiver<String>, outside_sender: Sender<String>) -> Arc<Self> {
        Arc::new(Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            sender: outside_sender,
            receiver: Arc::new(Mutex::new(outside_receiver)),
        })
    }

    pub async fn initialize(self: Arc<Self>) -> Result<(), Box<dyn Error>> {
        tokio::spawn(async {
            let listener = TcpListener::bind("127.0.0.1:7878")
                .await
                .expect("Cant bind listener");

            self.handle_connection(listener)
                .await
                .expect("Cant handle connection");
        })
        .await?;

        Ok(())
    }

    /// Пишет в sender из WS
    async fn ws_writer(
        self: Arc<Self>,
        sender: Sender<String>,
        mut reader: SplitReader<Stream<Http1>>,
        client_id: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        while let Some(Ok(msg)) = reader.next().await {
            match msg {
                Message::Text(text) => {
                    let text_as_string = String::from_utf8(text.to_vec())?;
                    info!("Received message {}", &text_as_string);
                    sender.send(text_as_string).await?;
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
                    let clients = self.clients.lock();
                    match clients {
                        Ok(mut c) => {
                            c.remove(&client_id);
                        }
                        Err(e) => {
                            error!("{}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Пишет из receiver в ws
    async fn ws_receiver(
        self: Arc<Self>,
        mut rx: Receiver<String>,
        mut ws_writer: SplitWriter<Stream<Http1>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // receive message from outside and write it to ws
        while let Some(msg) = rx.recv().await {
            ws_writer.send(Message::from(msg)).await?;
        }

        Ok(())
    }

    async fn handle_connection(
        self: Arc<Self>,
        listener: TcpListener,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Concrete Errors
        let server = WebSocketServer::<Http1>::new(Config::default());
        server
            .serve(listener, move |ws, req| {
                let (reader, writer) = ws.split();
                let client_id = utils::get_parameter_from_query(&req.path, "clientId");
                let this = Arc::clone(&self);
                async move {
                    // outside_sender используется для отправки сообщения в ws
                    // outside_reader используется для отправки сообщения "наружу"
                    // TODO: Добавить коллбэк для outside_sender, который будет торчать наружу и на него будут подписываться уже отправители
                    let (outside_sender, ws_reader) = tokio::sync::mpsc::channel::<String>(1024);
                    let (ws_writer, mut outside_reader) =
                        tokio::sync::mpsc::channel::<String>(1024);
                    {
                        let mut clients_guard = this.clients.lock().unwrap();
                        if (*clients_guard).contains_key(&client_id) {
                            warn!("Client with same id already exists");
                            return; // Do not handle connection
                        } else {
                            // (*clients_guard).insert(
                            //     client_id.clone(),
                            //     Client::new(client_id.clone(), (outside_sender, outside_reader)),
                            // );
                        }
                    }

                    let receiver_self = Arc::clone(&this);
                    let writer_self = Arc::clone(&this);
                    tokio::spawn(async {
                        receiver_self
                            .ws_receiver(ws_reader, writer)
                            .await
                            .expect("Cant receive message");
                    });
                    tokio::spawn(async {
                        writer_self
                            .ws_writer(ws_writer, reader, client_id)
                            .await
                            .expect("Cant send message to client")
                    });

                    // Считываем сообщения из ws и отправляем в kafka
                    while let Some(message) = outside_reader.recv().await {
                        let _ = this.sender.send(message).await;
                    }
                }
            })
            .await?;

        Ok(())
    }
}
