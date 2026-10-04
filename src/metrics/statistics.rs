use log::info;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::interval;

pub struct Statistics {
    kafka_requests: AtomicU32,
    kafka_previous_requests: AtomicU32,
    web_socket_requests: AtomicU32,
    web_socket_previous_requests: AtomicU32,
    out_broadcast_messages_count: AtomicU32,
}

impl Statistics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            kafka_requests: AtomicU32::new(0),
            kafka_previous_requests: AtomicU32::new(0),
            web_socket_requests: AtomicU32::new(0),
            web_socket_previous_requests: AtomicU32::new(0),
            out_broadcast_messages_count: AtomicU32::new(0),
        })
    }

    pub fn init(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(1));
            loop {
                tick.tick().await;

                let previous_value = self.web_socket_requests.swap(0, Ordering::AcqRel);
                self.web_socket_previous_requests
                    .swap(previous_value, Ordering::AcqRel);

                let kafka_previous_value = self.kafka_requests.swap(0, Ordering::AcqRel);
                self.kafka_previous_requests
                    .swap(kafka_previous_value, Ordering::AcqRel);
            }
        });
    }

    pub fn increment_request_per_second_kafka_in(self: Arc<Self>) {
        self.kafka_requests.fetch_add(1, Ordering::Release);
    }

    pub fn increment_requests_per_second_ws_out(self: Arc<Self>) {
        self.web_socket_requests.fetch_add(1, Ordering::Release);
    }

    pub fn set_out_broadcast_messages_count(self: Arc<Self>, count: u32) {
        self.out_broadcast_messages_count
            .store(count, Ordering::Release);
    }

    pub fn print_statistics(self: Arc<Self>) {
        info!(
            "STATISTICS:
Current throughput of outcoming messages to kafka: {} requests/sec
Current throughput of incoming messages in web_socket_service: {} requests/sec\n\
Current count of outbound messages: {}\n",
            self.kafka_previous_requests.load(Ordering::Acquire),
            self.web_socket_previous_requests.load(Ordering::Acquire),
            self.out_broadcast_messages_count.load(Ordering::Acquire)
        );
    }
}
