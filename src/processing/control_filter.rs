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
    chdrv_handler: &'static RetainReadTimeValue<CompactString>,
    rayon_pool: Arc<rayon::ThreadPool>,
}

impl ControlFilter {
    pub fn new(
        control_rx: SinkRx,
        dlq_tx: Arc<InvalidMessageTx>,
        chdrv_handler: &'static RetainReadTimeValue<CompactString>,
        rayon_pool: Arc<rayon::ThreadPool>,
    ) -> Self {
        Self {
            control_rx,
            dlq_tx,
            chdrv_handler,
            rayon_pool,
        }
    }

    pub async fn ingest(&mut self) {
        while let Some(partial_message) = self.control_rx.recv().await {
            let failure_tx = Arc::clone(&self.dlq_tx);
            let handler_ref = self.chdrv_handler;
            self.rayon_pool.spawn(
                move || match CoreControlMessage::try_from(partial_message) {
                    Ok(message) => ControlFilter::relay_control_event(message, handler_ref),
                    Err(error) => {
                        let message = error.message;
                        if failure_tx
                            .blocking_send(PartialMessageWithError::new(message, &error.kind))
                            .is_err()
                        {
                            eprintln!("Failed to send to dlq");
                        }
                    }
                },
            )
        }
    }

    fn relay_control_event(
        control_event: CoreControlMessage,
        chdrv_handler: &RetainReadTimeValue<CompactString>,
    ) {
        match control_event {
            CoreControlMessage::CHDRV(body) => chdrv_handler.update(body.new_driver_id),
            CoreControlMessage::UNKNOWN(unknown_key) => {
                println!("Unknown control message: '{unknown_key}'. Dropping");
            }
        }
    }
}
