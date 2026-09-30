use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use log::info;
use tokio::time::interval;

pub struct Statistics {
    requests: AtomicU32,
    previous_requests: AtomicU32,
}

impl Statistics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            requests: AtomicU32::new(0),
            previous_requests: AtomicU32::new(0),
        })
    }

    pub async fn init(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(1));
            loop {
                tick.tick().await;

                let previous_value = self.requests.swap(0, Ordering::AcqRel);
                self.previous_requests.swap(previous_value, Ordering::AcqRel);
            }
        });
    }

    pub fn increment(self: Arc<Self>) {
        self.requests.fetch_add(1, Ordering::Release);
    }

    pub fn print_requests_per_second(self: Arc<Self>) {
        info!(
            "Current throughput of incoming messages in web_socket_service: {} requests/sec",
            self.previous_requests.load(Ordering::Acquire)
        );
    }
}
