#![warn(clippy::pedantic)]

use crate::configuration::CoreConfig;
use crate::messaging::dlq_handler::DLQHandler;
use crate::messaging::flush_sink::FlushSink;
use crate::messaging::partial_message::{PartialMessage, PartialMessageWithError};
use crate::processing::reading_collector::ReadingCollector;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;

mod configuration;
pub mod messaging;
pub mod persistence;
pub mod processing;

fn main() {
    let config = CoreConfig::load_from_env()
        .expect("Failed to load configuration. Config file path not set.");
    let (sink_tx, sink_rx) =
        tokio::sync::mpsc::channel::<PartialMessage>(config.processing.message_channel_buffer_size);
    let sink = FlushSink::new(&config, sink_tx);
    let (dlq_tx, dlq_rx) = tokio::sync::mpsc::channel::<PartialMessageWithError>(
        config.processing.dlq_channel_buffer_size,
    );
    let (processed_data_tx, processed_data_rx) = tokio::sync::mpsc::channel::<UnwoundSensorDataMessage>(
        10 // TODO
    );
    let dlq_handler = DLQHandler::new(&config, dlq_rx);
    let collector = ReadingCollector::new(sink_rx, dlq_tx, processed_data_tx);
}
