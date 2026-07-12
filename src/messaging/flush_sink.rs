use crate::configuration::CoreConfig;
use crate::messaging::avro::{AvroSchema, SchemaRegistry, TryFromAvro};
use crate::messaging::dlq_handler::DLQHandler;
use crate::messaging::sensor_data_message::{SensorDataMessage, UnwoundSensorDataMessage};
use crate::persistance::reading_collector::ReadingCollector;
use apache_avro::Schema;
use futures_util::StreamExt;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::sync::Arc;

struct FlushSink {
    consumer: StreamConsumer,
    dlq_handler: Arc<DLQHandler>,
    sensor_data_schema: Schema,
    collector: ReadingCollector,
}

impl FlushSink {
    pub fn new(config: &CoreConfig, dlq_handler: DLQHandler, collector: ReadingCollector) -> Self {
        let sensor_data_schema = SchemaRegistry::load_and_parse_from_env(AvroSchema::SensorData)
            .expect("Failed to load sensor data schema");
        let kafka_config = &config.kafka;
        let consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", &kafka_config.brokers.join(","))
            .set("enable.auto.commit", "false") // TODO
            .set("session.timeout.ms", "6000")
            .create()
            .expect("Failed to create Flush sensor data consumer");
        consumer
            .subscribe(&[&kafka_config.sensor_data_flush_topic])
            .expect("Failed to subscribe to Flush sensor data topic");
        Self {
            consumer,
            dlq_handler: Arc::new(dlq_handler),
            sensor_data_schema,
            collector,
        }
    }

    pub async fn process_stream(self: &Self) {
        let mut message_stream = self.consumer.stream();
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(borrowed_message) => {
                    match SensorDataMessage::try_from_avro(
                        &self.sensor_data_schema,
                        borrowed_message.payload(),
                    ) {
                        Ok(deserialized_message) => self
                            .collector
                            .accept(UnwoundSensorDataMessage::from(deserialized_message)),
                        Err(deserialization_error) => {
                            let dlq_ref = self.dlq_handler.clone();
                            let owned_message = borrowed_message.detach();
                            let error_str = deserialization_error.to_string();
                            tokio::spawn(async move {
                                dlq_ref.redirect_to_dlq(owned_message, &error_str).await
                            });
                        }
                    }
                }
                Err(kafka_error) => {
                    eprintln!("Kafka error occurred while polling: {}", kafka_error);
                }
            }
        }
    }
}

impl Drop for FlushSink {
    fn drop(&mut self) {
        self.consumer.unsubscribe();
    }
}
