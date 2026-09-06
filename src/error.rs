use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataPipeError {
    #[error("Unknown error")]
    Unknown,
    #[error("Client already exist")]
    ClientAlreadyExist,
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
}

pub type DataPipeResult<T> = Result<T, DataPipeError>;