use tokio_util::sync::CancellationToken;

pub struct ClientCancellationHolder {
    pub identifier: String,
    pub token: CancellationToken
}

impl ClientCancellationHolder {
    pub fn new(identifier: String, token: CancellationToken) -> Self {
        Self { identifier, token }
    }
}