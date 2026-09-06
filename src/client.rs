pub struct Client {
    pub identifier: String,
}

impl Client {
    pub fn new(identifier: String) -> Client {
        Client {
            identifier,
        }
    }
}
