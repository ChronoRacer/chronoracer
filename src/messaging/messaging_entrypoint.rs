use crate::configuration::CoreConfig;
use crate::messaging::dlq_handler::DLQHandler;
use crate::messaging::message_sink::MessageSink;
use crate::messaging::partial_message::{PartialMessage, PartialMessageWithError};

pub type PartialMessageRx = tokio::sync::mpsc::Receiver<PartialMessage>;
pub fn setup_message_sinks(config: &'_ CoreConfig) -> (MessageSink, PartialMessageRx, DLQHandler<'_>, tokio::sync::mpsc::Sender<PartialMessageWithError>) {
    let (sink_tx, sink_rx) =
        tokio::sync::mpsc::channel::<PartialMessage>(config.processing.message_channel_buffer_size);
    let sink = MessageSink::new(
        &config.kafka,
        &[&config.kafka.sensor_data_flush_topic],
        sink_tx,
    );
    let (dlq_tx, dlq_rx) = tokio::sync::mpsc::channel::<PartialMessageWithError>(
        config.processing.dlq_channel_buffer_size,
    );
    let dlq_handler = DLQHandler::new(config, dlq_rx);
    (sink, sink_rx, dlq_handler, dlq_tx)
}
