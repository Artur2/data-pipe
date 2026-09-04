use tokio::sync::mpsc::{Receiver, Sender};

pub struct Client {
    pub identifier: String,
    pub sender_receiver: (Sender<String>, Receiver<String>),
}

impl Client {
    pub fn new(identifier: String, sender_receiver: (Sender<String>, Receiver<String>)) -> Client {
        Client {
            identifier,
            sender_receiver,
        }
    }
}
