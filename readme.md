### Websocket frontend for Kafka ###

Implementation of web-socket pub/sub for Kafka in **Rust**. Just for fun and educational purpose.\
Using **sockudo-ws**, **librdkafka** wrapper and of course - **tokio runtime**.\
Stress testing implemented using **C#** and plain **System.Net.WebSockets**

### TODOS ###

 - [ ] Telemetry
 - [ ] Statistics
 - [ ] Throttling
 - [ ] Think about transactional processing(in-flight commiting offsets?)
 - [ ] Configuration

### License ###

Licensed under MIT License.