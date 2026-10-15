### Websocket frontend for Kafka ###

Implementation of web-socket pub/sub for Kafka in **Rust**. Just for fun and educational purpose.\
Using **sockudo-ws**, **librdkafka** wrapper and of course - **tokio runtime**.\
Stress testing implemented using **C#** and plain **System.Net.WebSockets**

### TODOS ###

 - [ ] Telemetry
 - [x] Statistics
 - [x] Throttling
 - [x] Ack (on in-flight low watermark processing)
 - [x] Configuration

### License ###

Licensed under MIT License.