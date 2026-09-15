use crate::messaging::dlq_handler::InvalidMessageTx;
use crate::messaging::message_sink::BatchSinkRx;
use crate::messaging::partial_message::PartialMessageWithError;
use crate::persistence::persistence_entrypoint::SensorDataBatch;
use crate::processing::avro::TryFromAvro;
use crate::processing::sensor_data_message::{SensorDataMessage, UnwoundSensorDataMessage};
use apache_avro::reader::datum::GenericDatumReader;
use ouroboros::self_referencing;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

#[self_referencing]
struct SensorDataReader {
    schema: Arc<apache_avro::Schema>,
    #[borrows(schema)]
    #[covariant]
    reader: GenericDatumReader<'this>,
}

pub struct ReadingCollector {
    flush_rx: BatchSinkRx,
    dlq_tx: Arc<InvalidMessageTx>,
    processed_data_tx: Arc<tokio::sync::mpsc::Sender<SensorDataBatch>>,
    sensor_data_reader: Arc<SensorDataReader>,
    rayon_pool: Arc<rayon::ThreadPool>,
    processing_slots: Arc<Semaphore>,
}

impl ReadingCollector {
    #[must_use]
    pub fn new(
        flush_rx: BatchSinkRx,
        dlq_tx: Arc<InvalidMessageTx>,
        processed_data_tx: tokio::sync::mpsc::Sender<SensorDataBatch>,
        sensor_data_schema: apache_avro::Schema,
        rayon_pool: Arc<rayon::ThreadPool>,
    ) -> Self {
        let sensor_data_reader = SensorDataReaderBuilder {
            schema: Arc::new(sensor_data_schema),
            reader_builder: |schema| {
                GenericDatumReader::builder(schema.as_ref())
                    .build()
                    .expect("sensor data reader should build")
            },
        }
            .build();

        Self {
            flush_rx,
            dlq_tx,
            processed_data_tx: Arc::from(processed_data_tx),
            sensor_data_reader: Arc::new(sensor_data_reader),
            processing_slots: Arc::new(Semaphore::new(rayon_pool.current_num_threads())),
            rayon_pool,
        }
    }

    pub async fn ingest(&mut self) {
        info!("Starting Reading collector ingestion loop");
        while let Some(partial_messages) = self.flush_rx.recv().await {
            let Ok(processing_slot) = Arc::clone(&self.processing_slots).acquire_owned().await
            else {
                error!("Reading collector processing slots closed");
                break;
            };
            let reader = Arc::clone(&self.sensor_data_reader);
            let success_tx = Arc::clone(&self.processed_data_tx);
            let failure_tx = Arc::clone(&self.dlq_tx);
            self.rayon_pool.spawn(move || {
                let _processing_slot = processing_slot;
                // TODO: Maybe this might be improved so no semaphore is needed. Do not spawn without bound or it will overflow and crash
                let processed: Vec<_> = partial_messages
                    .into_iter()
                    .filter_map(|partial_message| {
                        let result = reader.with_reader(|reader| {
                            SensorDataMessage::try_from_avro(
                                reader,
                                partial_message.payload.as_deref(),
                            )
                        });
                        match result {
                            Ok(msg) => Some(UnwoundSensorDataMessage::from(msg)),
                            Err(err) => {
                                let failed_msg =
                                    PartialMessageWithError::new(partial_message, &err);
                                // This should be okay: the channel has high capacity and invalid messages should be rare
                                if let Err(e) = failure_tx.blocking_send(failed_msg) {
                                    error!("Failed to send to dlq: {e}");
                                }
                                None
                            }
                        }
                    })
                    .collect();

                if !processed.is_empty()
                    && let Err(e) = success_tx.blocking_send(processed)
                {
                    error!("Failed to send processed batch downstream: {e}");
                }
            });
        }
        warn!("Reading collector ingestion loop terminated");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::partial_message::PartialMessage;
    use crate::persistence::persistence_entrypoint::SensorDataBatch;
    use crate::processing::avro::{AvroSchema, SchemaRegistry};
    use apache_avro::types::Value;
    use apache_avro::writer::datum::GenericDatumWriter;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    fn build_pool() -> rayon::ThreadPool {
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("Failed to build Rayon pool")
    }

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
        let record = Value::Record(vec![
            ("sensor_id".to_string(), Value::String(sensor_id)),
            (
                "timestamp".to_string(),
                Value::TimestampNanos(1_000_000_000),
            ),
            (
                "readings".to_string(),
                Value::Array(vec![Value::Record(vec![
                    ("delta".to_string(), Value::Int(234)),
                    ("value".to_string(), Value::Double(first_reading_value)),
                ])]),
            ),
        ]);
        let writer = GenericDatumWriter::builder(schema)
            .build()
            .expect("sensor data reader should build");
        writer.write_value_to_vec(record).expect("Failed to serialize to raw datum")
    }

    #[tokio::test]
    async fn test_ingest_successful_processing() {
        let schema = load_avro_schema();

        let (flush_tx, flush_rx) = mpsc::channel::<Vec<PartialMessage>>(10);
        let (dlq_tx, mut dlq_rx) = mpsc::channel::<PartialMessageWithError>(10);
        let (processed_tx, mut processed_rx) = mpsc::channel::<SensorDataBatch>(10);

        let mut collector = ReadingCollector::new(
            flush_rx,
            Arc::from(dlq_tx),
            processed_tx,
            schema.clone(),
            build_pool().into(),
        );

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
            .send(vec![message])
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
        let processed_batch = processed_msg.unwrap();
        assert_eq!(processed_batch.len(), 1);
        let content = &processed_batch[0];
        assert_eq!(content.sensor_id, sensor_id);
        assert!(!content.readings.is_empty());
        assert_eq!(content.readings[0].value, first_reading_value);

        drop(flush_tx);
        let _ = ingest_handle.await;
    }

    #[tokio::test]
    async fn test_ingest_routes_to_dlq_on_deserialization_error() {
        let schema = load_avro_schema();

        let (flush_tx, flush_rx) = mpsc::channel::<Vec<PartialMessage>>(10);
        let (dlq_tx, mut dlq_rx) = mpsc::channel::<PartialMessageWithError>(10);
        let (processed_tx, mut processed_rx) = mpsc::channel::<SensorDataBatch>(10);

        let mut collector = ReadingCollector::new(
            flush_rx,
            Arc::from(dlq_tx),
            processed_tx,
            schema,
            build_pool().into(),
        );

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
            .send(vec![corrupt_message])
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
