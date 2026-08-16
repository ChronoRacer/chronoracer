use crate::configuration::CoreConfig;
use crate::messaging::dlq_handler::DLQHandler;
use crate::messaging::heartbeat_handler::HeartbeatHandler;
use crate::messaging::kafka_exporter::KafkaExporter;
use crate::messaging::management::ensure_all_topics_exists;
use crate::messaging::message_sink::{MessageSink, SinkRx};
use crate::messaging::partial_message::{PartialMessage, PartialMessageWithError};
use rdkafka::producer::FutureProducer;
use std::sync::Arc;

pub struct MessagingLayerEntrypoint<'a> {
    pub sensor_data_sink: MessageSink,
    pub sensor_sink_rx: SinkRx,
    pub dlq_handler: DLQHandler<'a>,
    pub dlq_tx: tokio::sync::mpsc::Sender<PartialMessageWithError>,
    pub control_sink: MessageSink,
    pub control_sink_rx: SinkRx,
    pub heartbeat_handler: HeartbeatHandler,
    pub kafka_exporter: KafkaExporter<'a>,
}

impl<'a> MessagingLayerEntrypoint<'a> {
    #[must_use]
    pub async fn init(config: &'static CoreConfig) -> MessagingLayerEntrypoint<'a> {
        let kafka_exporter = KafkaExporter::new(&config.race.race_id, &config.gcp, &config.kafka).expect("Failed to create Kafka exported");

        ensure_all_topics_exists(&config.kafka)
            .await
            .expect("Failed to ensure topics exists");

        let (sensor_sink_tx, sensor_sink_rx) = tokio::sync::mpsc::channel::<PartialMessage>(
            config.processing.message_channel_buffer_size,
        );
        let sensor_data_sink = MessageSink::new(
            &config.kafka,
            vec![&config.kafka.sensor_data_topic.name],
            sensor_sink_tx,
        );
        let (control_sink_tx, control_sink_rx) = tokio::sync::mpsc::channel::<PartialMessage>(
            config.processing.message_channel_buffer_size,
        );
        let control_sink = MessageSink::new(
            &config.kafka,
            vec![&config.kafka.control_event_topic.name],
            control_sink_tx,
        );
        let (dlq_tx, dlq_rx) = tokio::sync::mpsc::channel::<PartialMessageWithError>(
            config.processing.dlq_channel_buffer_size,
        );

        let producer: Arc<FutureProducer> = Arc::new(
            rdkafka::ClientConfig::new()
                .set("bootstrap.servers", config.kafka.brokers.join(","))
                .create()
                .expect("Failed to create DLQ handler producer"),
        );
        let dlq_handler = DLQHandler::with_producer(config, dlq_rx, Arc::clone(&producer));
        let heartbeat_handler = HeartbeatHandler::new(Arc::clone(&producer), config);

        Self {
            sensor_data_sink,
            sensor_sink_rx,
            dlq_handler,
            dlq_tx,
            control_sink,
            control_sink_rx,
            heartbeat_handler,
            kafka_exporter
        }
    }
}
