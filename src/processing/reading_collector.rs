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
        sensor_data_schema: apache_avro::Schema,
    ) -> Self {
        Self {
            flush_rx,
            dlq_tx: Arc::from(dlq_tx),
            processed_data_tx: Arc::from(processed_data_tx),
            sensor_data_schema: Arc::new(sensor_data_schema),
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

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    fn load_avro_schema() -> apache_avro::Schema {
        AvroSchema::point_env_var_to_workspace();
        let loaded = SchemaRegistry::load_and_parse_from_env(AvroSchema::SensorData);
        loaded.unwrap()
    }

    fn write_valid_avro(
        schema: &apache_avro::Schema,
        sensor_id: String,
        first_reading_value: f64,
    ) -> Vec<u8> {
        let mut buffer = Vec::new();
        let mut writer = apache_avro::Writer::new(schema, &mut buffer);
        // Matches SensorDataMessage
        let record = Value::Record(vec![
            ("sensor_id".to_string(), Value::String(sensor_id)),
            ("timestamp".to_string(), Value::TimestampNanos(1000000000)),
            (
                "readings".to_string(),
                Value::Array(vec![Value::Record(vec![
                    ("value".to_string(), Value::Double(first_reading_value)),
                    ("delta".to_string(), Value::Int(234)),
                ])]),
            ),
        ]);
        writer.append(record).expect("Failed to append record");
        writer.flush().expect("Failed to flush writer");
        drop(writer);
        buffer
    }

    #[tokio::test]
    async fn test_ingest_successful_processing() {
        let schema = load_avro_schema();

        let (flush_tx, flush_rx) = mpsc::channel::<PartialMessage>(10);
        let (dlq_tx, mut dlq_rx) = mpsc::channel::<PartialMessageWithError>(10);
        let (processed_tx, mut processed_rx) = mpsc::channel::<UnwoundSensorDataMessage>(10);

        let mut collector = ReadingCollector::new(flush_rx, dlq_tx, processed_tx, schema.clone());

        let ingest_handle = tokio::spawn(async move {
            collector.ingest().await;
        });

        let sensor_id = "test123";
        let first_reading_value = 1.0;
        let message = PartialMessage {
            key: None,
            offset: 0,
            partition: 0,
            payload: Some(Box::from(write_valid_avro(
                &schema,
                sensor_id.to_string(),
                first_reading_value,
            ))),
        };
        flush_tx
            .send(message)
            .await
            .expect("Failed to send message");

        let result = timeout(Duration::from_secs(2), processed_rx.recv()).await;
        assert!(dlq_rx.try_recv().is_err());

        assert!(
            result.is_ok(),
            "Timed out waiting for Rayon to process payload"
        );
        let processed_msg = result.unwrap();
        assert!(
            processed_msg.is_some(),
            "Expected processed message, got None"
        );
        let content = processed_msg.unwrap();
        assert_eq!(content.sensor_id, sensor_id);
        assert!(!content.readings.is_empty());
        assert_eq!(content.readings[0].value, first_reading_value);

        drop(flush_tx);
        let _ = ingest_handle.await;
    }

    #[tokio::test]
    async fn test_ingest_routes_to_dlq_on_deserialization_error() {
        let schema = load_avro_schema();

        let (flush_tx, flush_rx) = mpsc::channel::<PartialMessage>(10);
        let (dlq_tx, mut dlq_rx) = mpsc::channel::<PartialMessageWithError>(10);
        let (processed_tx, mut processed_rx) = mpsc::channel::<UnwoundSensorDataMessage>(10);

        let mut collector = ReadingCollector::new(flush_rx, dlq_tx, processed_tx, schema);

        let ingest_handle = tokio::spawn(async move {
            collector.ingest().await;
        });

        let corrupt_message = PartialMessage {
            key: None,
            offset: 10,
            partition: 1,
            payload: Some(vec![0xFF, 0xFF, 0xFF].into()),
        };
        flush_tx
            .send(corrupt_message)
            .await
            .expect("Failed to send bad message");

        let result = timeout(Duration::from_secs(2), dlq_rx.recv()).await;
        assert!(processed_rx.try_recv().is_err());

        assert!(result.is_ok(), "Timed out waiting for DLQ failure output");
        let dlq_msg = result.unwrap();
        assert!(dlq_msg.is_some(), "Expected PartialMessageWithError on DLQ");

        drop(flush_tx);
        let _ = ingest_handle.await;
    }
}
