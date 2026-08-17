use crate::messaging::control_message::CoreControlMessage;
use crate::messaging::dlq_handler::InvalidMessageTx;
use crate::messaging::message_sink::SinkRx;
use crate::messaging::partial_message::PartialMessageWithError;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tracing::{error, info, warn};

pub struct ControlFilter {
    control_rx: SinkRx,
    inner: Arc<ControlRelay>,
    rayon_pool: Arc<rayon::ThreadPool>,
}

struct ControlRelay {
    dlq_tx: Arc<InvalidMessageTx>,
    chdrv_handler: &'static RetainReadTimeValue<CompactString>,
    shutdown_handler_tx: Mutex<Option<oneshot::Sender<()>>>,
    closed: AtomicBool,
}

impl ControlFilter {
    #[must_use]
    pub fn new(
        control_rx: SinkRx,
        dlq_tx: Arc<InvalidMessageTx>,
        chdrv_handler: &'static RetainReadTimeValue<CompactString>,
        rayon_pool: Arc<rayon::ThreadPool>,
        shutdown_handler_tx: oneshot::Sender<()>,
    ) -> Self {
        let inner = ControlRelay {
            dlq_tx,
            chdrv_handler,
            shutdown_handler_tx: Mutex::new(Some(shutdown_handler_tx)),
            closed: AtomicBool::new(false),
        };
        Self {
            control_rx,
            rayon_pool,
            inner: Arc::new(inner),
        }
    }

    pub async fn ingest(&mut self) {
        info!("Starting control ingestion loop");
        while let Some(partial_message) = self.control_rx.recv().await {
            let inner = Arc::clone(&self.inner);

            self.rayon_pool.spawn(
                move || match CoreControlMessage::try_from(partial_message) {
                    Ok(message) => inner.relay_control_event(message),
                    Err(error) => {
                        let message = error.message;
                        if let Err(e) = inner
                            .dlq_tx
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
}

impl ControlRelay {
    fn relay_control_event(&self, control_event: CoreControlMessage) {
        info!("Received control event: {control_event}");
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            warn!("Control relay closed. Dropping");
            return;
        }

        match control_event {
            CoreControlMessage::CHDRV(body) => {
                let new_driver_id = body.new_driver_id;
                self.chdrv_handler.update(new_driver_id);
            }
            CoreControlMessage::SHUTDOWN => {
                self.closed
                    .store(true, std::sync::atomic::Ordering::Release);
                info!("Relay closed due to receiving shutdown event");
                if let Ok(mut lock) = self.shutdown_handler_tx.lock() {
                    if let Some(tx) = lock.take() {
                        let _ = tx.send(());
                    }
                }
            }
            _ => {}
        }
    }
}
