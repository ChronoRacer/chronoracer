use crate::configuration::CoreConfig;
use crate::messaging::dlq_handler::DLQHandler;
use crate::messaging::message_sink::{MessageSink, SinkRx};
use crate::messaging::partial_message::{PartialMessage, PartialMessageWithError};

pub struct MessagingLayerEntrypoint<'a> {
    pub sensor_data_sink: MessageSink,
    pub sensor_sink_rx: SinkRx,
    pub dlq_handler: DLQHandler<'a>,
    pub dlq_tx: tokio::sync::mpsc::Sender<PartialMessageWithError>,
    pub control_sink: MessageSink,
    pub control_sink_rx: SinkRx,
}

impl<'a> MessagingLayerEntrypoint<'a> {
    pub fn init(config: &'a CoreConfig) -> MessagingLayerEntrypoint<'a> {
        let (sensor_sink_tx, sensor_sink_rx) = tokio::sync::mpsc::channel::<PartialMessage>(
            config.processing.message_channel_buffer_size,
        );
        let sensor_data_sink = MessageSink::new(
            &config.kafka,
            &[
                &config.kafka.sensor_data_flush_topic,
                &config.kafka.sensor_data_order_topic,
            ],
            sensor_sink_tx,
        );
        let (control_sink_tx, control_sink_rx) = tokio::sync::mpsc::channel::<PartialMessage>(
            config.processing.message_channel_buffer_size,
        );
        let control_sink = MessageSink::new(
            &config.kafka,
            &[&config.kafka.control_event_topic],
            control_sink_tx,
        );
        let (dlq_tx, dlq_rx) = tokio::sync::mpsc::channel::<PartialMessageWithError>(
            config.processing.dlq_channel_buffer_size,
        );
        let dlq_handler = DLQHandler::new(config, dlq_rx);
        Self {
            sensor_data_sink,
            sensor_sink_rx,
            dlq_handler,
            dlq_tx,
            control_sink,
            control_sink_rx,
        }
    }
}
