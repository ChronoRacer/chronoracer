use crate::messaging::control_message::CoreControlMessage;
use crate::messaging::dlq_handler::InvalidMessageTx;
use crate::messaging::message_sink::SinkRx;
use crate::messaging::partial_message::PartialMessageWithError;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use std::sync::Arc;

pub struct ControlFilter {
    control_rx: SinkRx,
    dlq_tx: Arc<InvalidMessageTx>,
    chdrv_handler: RetainReadTimeValue<CompactString>,
}

impl ControlFilter {
    pub fn new(
        control_rx: SinkRx,
        dlq_tx: Arc<InvalidMessageTx>,
        chdrv_handler: RetainReadTimeValue<CompactString>,
    ) -> Self {
        Self {
            control_rx,
            dlq_tx,
            chdrv_handler,
        }
    }

    pub async fn ingest(&mut self) {
        while let Some(partial_message) = self.control_rx.recv().await {
            match CoreControlMessage::try_from(partial_message) {
                Ok(message) => self.relay_control_event(message),
                Err(error) => {
                    let message = error.message;
                    if self
                        .dlq_tx
                        .send(PartialMessageWithError::new(message, &error.kind))
                        .await
                        .is_err()
                    {
                        eprintln!("Failed to send to dlq");
                    }
                }
            }
        }
    }

    fn relay_control_event(&mut self, control_event: CoreControlMessage) {
        match control_event {
            CoreControlMessage::CHDRV(body) => {
                self.chdrv_handler.update(body.new_driver_id)
            }
            CoreControlMessage::UNKNOWN(unknown_key) => {
                println!("Unknown control message: '{unknown_key}'. Dropping");
            }
        }
    }
}
