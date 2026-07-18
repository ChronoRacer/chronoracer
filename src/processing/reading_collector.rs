use crate::messaging::partial_message::{PartialMessage, PartialMessageWithError};
use crate::processing::avro::{AvroSchema, SchemaRegistry, TryFromAvro};
use crate::processing::sensor_data_message::{SensorDataMessage, UnwoundSensorDataMessage};
use std::sync::Arc;

pub struct ReadingCollector {
    flush_rx: tokio::sync::mpsc::Receiver<PartialMessage>,
    dlq_tx: Arc<tokio::sync::mpsc::Sender<PartialMessageWithError>>,
    processed_data_tx: Arc<tokio::sync::mpsc::Sender<UnwoundSensorDataMessage>>,
    sensor_data_schema: Arc<apache_avro::Schema>,
}

impl ReadingCollector {
    pub fn new(
        flush_rx: tokio::sync::mpsc::Receiver<PartialMessage>,
        dlq_tx: tokio::sync::mpsc::Sender<PartialMessageWithError>,
        processed_data_tx: tokio::sync::mpsc::Sender<UnwoundSensorDataMessage>,
    ) -> Self {
        let sensor_data_schema = SchemaRegistry::load_and_parse_from_env(AvroSchema::SensorData)
            .expect("Failed to load sensor data schema");
        Self {
            flush_rx,
            dlq_tx: Arc::from(dlq_tx),
            processed_data_tx: Arc::from(processed_data_tx),
            sensor_data_schema: Arc::from(sensor_data_schema),
        }
    }

    pub async fn ingest(&mut self) {
        while let Some(partial_message) = self.flush_rx.recv().await {
            let schema = Arc::clone(&self.sensor_data_schema);
            let success_tx = Arc::clone(&self.processed_data_tx);
            let failure_tx = Arc::clone(&self.dlq_tx);
            rayon::spawn(move || {
                match SensorDataMessage::try_from_avro(&schema, partial_message.payload.as_deref())
                {
                    Ok(deserialized_message) => {
                        let unwound = UnwoundSensorDataMessage::from(deserialized_message);
                        if success_tx.blocking_send(unwound).is_err() {
                            eprintln!("Failed to send to downstream");
                        }
                    }
                    Err(deserialization_error) => {
                        if failure_tx
                            .blocking_send(PartialMessageWithError::new(
                                partial_message,
                                &deserialization_error,
                            ))
                            .is_err()
                        {
                            eprintln!("Failed to send to dlq");
                        }
                    }
                }
            })
        }
    }
}
