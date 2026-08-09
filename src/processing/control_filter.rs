use crate::messaging::control_message::CoreControlMessage;
use crate::messaging::dlq_handler::InvalidMessageTx;
use crate::messaging::message_sink::SinkRx;
use crate::messaging::partial_message::PartialMessageWithError;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use std::sync::Arc;
use tracing::{error, info, warn};

pub struct ControlFilter {
    control_rx: SinkRx,
    dlq_tx: Arc<InvalidMessageTx>,
    chdrv_handler: &'static RetainReadTimeValue<CompactString>,
    rayon_pool: Arc<rayon::ThreadPool>,
}

impl ControlFilter {
    #[must_use]
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
        info!("Staring control ingestion loop");
        while let Some(partial_message) = self.control_rx.recv().await {
            let failure_tx = Arc::clone(&self.dlq_tx);
            let handler_ref = self.chdrv_handler;
            self.rayon_pool.spawn(
                move || match CoreControlMessage::try_from(partial_message) {
                    Ok(message) => ControlFilter::relay_control_event(message, handler_ref),
                    Err(error) => {
                        let message = error.message;
                        if let Err(e) = failure_tx
                            .blocking_send(PartialMessageWithError::new(message, &error.kind))
                        {
                            error!("Failed to send to dlq: {e}");
                        }
                    }
                },
            );
        }
        warn!("Control ingestion loop terminated");
    }

    fn relay_control_event(
        control_event: CoreControlMessage,
        chdrv_handler: &RetainReadTimeValue<CompactString>,
    ) {
        match control_event {
            CoreControlMessage::CHDRV(body) => {
                let new_driver_id = body.new_driver_id;
                info!("Relaying CHDRV control message: new_driver_id={new_driver_id}");
                chdrv_handler.update(new_driver_id);
            },
            CoreControlMessage::UNKNOWN(unknown_key) => {
                warn!("Unknown control message: '{unknown_key}'. Dropping");
            }
        }
    }
}
