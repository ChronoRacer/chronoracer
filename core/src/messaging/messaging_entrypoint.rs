use crate::configuration::CoreConfig;
use crate::messaging::dlq_handler::{
    DLQHandler, InvalidMessageTx, InvalidSettler, invalid_message_channel,
};
use crate::messaging::heartbeat_handler::HeartbeatHandler;
use crate::messaging::kafka_exporter::KafkaExporter;
use crate::messaging::management::ensure_all_topics_exists;
use crate::messaging::message_sink::{BatchMessageSink, BatchSinkRx, InstantMessageSink, SinkRx};
use crate::messaging::offset_ledger::OffsetLedger;
use crate::messaging::partial_message::PartialMessage;
use crate::pipeline_stats::PipelineStats;
use anyhow::{Context, Result, bail};
use rdkafka::producer::FutureProducer;
use std::sync::Arc;

pub struct MessagingLayerEntrypoint {
    pub sensor_data_sink: BatchMessageSink,
    pub sensor_offsets: OffsetLedger,
    pub sensor_sink_rx: BatchSinkRx,
    pub dlq_handler: DLQHandler,
    pub dlq_tx: InvalidMessageTx,
    pub control_sink: InstantMessageSink,
    pub control_offsets: OffsetLedger,
    pub control_sink_rx: SinkRx,
    pub heartbeat_handler: HeartbeatHandler,
    pub kafka_exporter: KafkaExporter,
}

impl MessagingLayerEntrypoint {
    pub async fn init(
        config: &'static CoreConfig,
        stats: Arc<PipelineStats>,
    ) -> Result<MessagingLayerEntrypoint> {
        ensure_kafka_compilation_features()?;

        let kafka_exporter = KafkaExporter::new(&config.race.race_id, &config.gcp, &config.kafka)
            .context("Failed to create Kafka exporter")?;

        ensure_all_topics_exists(&config.kafka)
            .await
            .context("Failed to ensure Kafka topics exist")?;

        let (sensor_sink_tx, sensor_sink_rx) = tokio::sync::mpsc::channel::<Vec<PartialMessage>>(
            config.processing.message_channel_buffer_size.get(),
        );
        let sensor_data_sink = BatchMessageSink::new(
            &config.kafka,
            vec![&config.kafka.sensor_data_topic.name],
            sensor_sink_tx,
            config.processing.message_batch_size.get(),
            config.processing.message_batch_flush_interval_ms.get(),
            stats,
        )?;
        let (control_sink_tx, control_sink_rx) = tokio::sync::mpsc::channel::<PartialMessage>(
            config.processing.message_channel_buffer_size.get(),
        );
        let control_sink = InstantMessageSink::new(
            &config.kafka,
            vec![&config.kafka.control_event_topic.name],
            control_sink_tx,
        )?;
        let sensor_offsets = sensor_data_sink.offsets();
        let control_offsets = control_sink.offsets();
        let (dlq_tx, dlq_rx) = invalid_message_channel(
            config.processing.dlq_channel_buffer_size.get(),
            config.processing.dlq_sample_every.get(),
            InvalidSettler::new(sensor_offsets.clone(), control_offsets.clone()),
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
            sensor_offsets,
            sensor_sink_rx,
            dlq_handler,
            dlq_tx,
            control_sink,
            control_offsets,
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
