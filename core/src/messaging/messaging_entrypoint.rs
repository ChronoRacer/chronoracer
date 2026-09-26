use crate::configuration::CoreConfig;
use crate::messaging::dlq_handler::DLQHandler;
use crate::messaging::heartbeat_handler::HeartbeatHandler;
use crate::messaging::kafka_exporter::KafkaExporter;
use crate::messaging::management::ensure_all_topics_exists;
use crate::messaging::message_sink::{BatchMessageSink, BatchSinkRx, InstantMessageSink, SinkRx};
use crate::messaging::partial_message::{PartialMessage, PartialMessageWithError};
use anyhow::{Context, Result, bail};
use rdkafka::producer::FutureProducer;
use std::sync::Arc;

pub struct MessagingLayerEntrypoint<'a> {
    pub sensor_data_sink: BatchMessageSink,
    pub sensor_sink_rx: BatchSinkRx,
    pub dlq_handler: DLQHandler<'a>,
    pub dlq_tx: tokio::sync::mpsc::Sender<PartialMessageWithError>,
    pub control_sink: InstantMessageSink,
    pub control_sink_rx: SinkRx,
    pub heartbeat_handler: HeartbeatHandler,
    pub kafka_exporter: KafkaExporter<'a>,
}

impl<'a> MessagingLayerEntrypoint<'a> {
    pub async fn init(config: &'static CoreConfig) -> Result<MessagingLayerEntrypoint<'a>> {
        ensure_kafka_compilation_features()?;

        let kafka_exporter = KafkaExporter::new(&config.race.race_id, &config.gcp, &config.kafka)
            .context("Failed to create Kafka exporter")?;

        ensure_all_topics_exists(&config.kafka)
            .await
            .context("Failed to ensure Kafka topics exist")?;

        let (sensor_sink_tx, sensor_sink_rx) = tokio::sync::mpsc::channel::<Vec<PartialMessage>>(
            config.processing.message_channel_buffer_size,
        );
        let sensor_data_sink = BatchMessageSink::new(
            &config.kafka,
            vec![&config.kafka.sensor_data_topic.name],
            sensor_sink_tx,
            config.processing.message_batch_size,
            config.processing.message_batch_flush_interval_ms,
        )?;
        let (control_sink_tx, control_sink_rx) = tokio::sync::mpsc::channel::<PartialMessage>(
            config.processing.message_channel_buffer_size,
        );
        let control_sink = InstantMessageSink::new(
            &config.kafka,
            vec![&config.kafka.control_event_topic.name],
            control_sink_tx,
        )?;
        let (dlq_tx, dlq_rx) = tokio::sync::mpsc::channel::<PartialMessageWithError>(
            config.processing.dlq_channel_buffer_size,
        );

        let producer: Arc<FutureProducer> = Arc::new(
            rdkafka::ClientConfig::new()
                .set("bootstrap.servers", config.kafka.brokers.join(","))
                .set("compression.type", "snappy")
                .create()
                .context("Failed to create DLQ producer")?,
        );
        let dlq_handler = DLQHandler::with_producer(config, dlq_rx, Arc::clone(&producer));
        let heartbeat_handler = HeartbeatHandler::new(Arc::clone(&producer), config);

        Ok(Self {
            sensor_data_sink,
            sensor_sink_rx,
            dlq_handler,
            dlq_tx,
            control_sink,
            control_sink_rx,
            heartbeat_handler,
            kafka_exporter,
        })
    }
}

fn ensure_kafka_compilation_features() -> Result<()> {
    let rdkafka_config = rdkafka::ClientConfig::new()
        .create_native_config()
        .context("Failed to create librdkafka config")?;
    // builtin.features is a string like "gzip,snappy,sasl,regex,lz4,sasl_gssapi,sasl_plain,plugins"
    let features = rdkafka_config
        .get("builtin.features")
        .context("Failed to read librdkafka builtin features")?;
    for required_feature in ["snappy", "lz4"] {
        if !features
            .split(',')
            .any(|feature| feature.trim() == required_feature)
        {
            bail!("rdkafka was built without {required_feature} support: {features}");
        }
    }
    Ok(())
}
