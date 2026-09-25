use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataPipeError {
    #[error("Unknown error")]
    Unknown,
    #[error("Client already exist")]
    ClientAlreadyExist,
    #[error("Client not found")]
    ClientNotFound,
    #[error("Receive message failed")]
    ReceiveMessageFailed,
    #[error("Send message failed")]
    SendMessageFailed,
    #[error("Cant bind to specified port/host")]
    CantBind,
    #[error("Cant connect to specified port/host")]
    CantConnect,
    #[error("Task execution error: {0}")]
    TaskError(String),
    #[error("Cant identify client: {0}")]
    ClientIdentificationError(String),
    #[error("Cant parse message, message: {0}")]
    CantParseMessage(String),
    #[error("Subscription for topic {0} and group {1} already exists")]
    SubscriptionAlreadyExist(String, String)
}

pub type DataPipeResult<T> = Result<T, DataPipeError>;
