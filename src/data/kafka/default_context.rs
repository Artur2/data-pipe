use log::info;
use rdkafka::{ClientContext, TopicPartitionList};
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;

pub struct DefaultContext;

impl ClientContext for DefaultContext {}

impl ConsumerContext for DefaultContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

pub type DefaultConsumer = StreamConsumer<DefaultContext>;