use crate::configuration::configuration::DataPipeConfiguration;
use crate::data::clients_manager::ClientsManager;
use crate::data::error::DataPipeResult;
use crate::metrics::statistics::Statistics;
use crate::services::kafka_service::KafkaService;
use crate::services::web_socket_service::WebSocketService;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

pub struct DataPipeServer {
    statistics: Arc<Statistics>,
    configuration: Arc<DataPipeConfiguration>,
    web_socket_service: Arc<WebSocketService>,
    kafka_service: Arc<KafkaService>,
}

impl DataPipeServer {
    pub fn new(configuration: DataPipeConfiguration) -> DataPipeServer {
        let statistics = Statistics::new();
        let configuration = Arc::new(configuration);
        let clients = Arc::new(RwLock::new(ClientsManager::new()));
        let web_socket_service =
            WebSocketService::new(clients.clone(), configuration.clone(), statistics.clone());
        let kafka_service =
            KafkaService::new(clients.clone(), configuration.clone(), statistics.clone());

        DataPipeServer {
            web_socket_service,
            configuration,
            kafka_service,
            statistics,
        }
    }

    pub async fn initialize(&self) -> DataPipeResult<()> {
        let ws_service = self.web_socket_service.clone();
        ws_service.initialize().await?;

        let ws_service_clone = self.web_socket_service.clone();
        let receiver = ws_service_clone.get_receiver()?;
        let kafka_service_clone = self.kafka_service.clone();
        kafka_service_clone.initialize(receiver)?;

        if self.configuration.harvest_statistics {
            let initialize_statistics = self.statistics.clone();
            initialize_statistics.init();

            let printing_statistics = self.statistics.clone();
            tokio::spawn(async move {
                loop {
                    let looping_stats = printing_statistics.clone();
                    looping_stats.print_statistics();
                    sleep(Duration::from_secs(1)).await;
                }
            });
        }

        Ok(())
    }
}
