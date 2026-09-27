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
                        if let Err(error) = inner
                            .dlq_tx
                            .blocking_send(PartialMessageWithError::new(message, &error.kind))
                        {
                            error!(%error, "Failed to send to dlq");
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
                if let Ok(mut lock) = self.shutdown_handler_tx.lock()
                    && let Some(tx) = lock.take()
                {
                    let _ = tx.send(());
                }
            }
            CoreControlMessage::UNKNOWN(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::control_message::{ChangeDriverMessage, CoreControlMessage};

    fn relay(
        shutdown_handler_tx: oneshot::Sender<()>,
    ) -> (ControlRelay, &'static RetainReadTimeValue<CompactString>) {
        let driver_id = Box::leak(Box::new(RetainReadTimeValue::new(CompactString::from(
            "initial",
        ))));
        let (dlq_tx, _dlq_rx) = tokio::sync::mpsc::channel(1);
        let relay = ControlRelay {
            dlq_tx: Arc::new(dlq_tx),
            chdrv_handler: driver_id,
            shutdown_handler_tx: Mutex::new(Some(shutdown_handler_tx)),
            closed: AtomicBool::new(false),
        };
        (relay, driver_id)
    }

    #[test]
    fn change_driver_event_updates_runtime_driver_id() {
        let (shutdown_tx, _shutdown_rx) = oneshot::channel();
        let (relay, driver_id) = relay(shutdown_tx);

        relay.relay_control_event(CoreControlMessage::CHDRV(ChangeDriverMessage {
            new_driver_id: CompactString::from("driver-42"),
        }));

        assert_eq!(driver_id.get().as_str(), "driver-42");
    }

    #[tokio::test]
    async fn shutdown_event_notifies_once_and_closes_relay() {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let (relay, driver_id) = relay(shutdown_tx);

        relay.relay_control_event(CoreControlMessage::SHUTDOWN);
        assert!(shutdown_rx.try_recv().is_ok());

        relay.relay_control_event(CoreControlMessage::CHDRV(ChangeDriverMessage {
            new_driver_id: CompactString::from("ignored"),
        }));

        assert_eq!(driver_id.get().as_str(), "initial");
    }

    #[test]
    fn unknown_event_does_not_change_runtime_state() {
        let (shutdown_tx, _shutdown_rx) = oneshot::channel();
        let (relay, driver_id) = relay(shutdown_tx);

        relay.relay_control_event(CoreControlMessage::UNKNOWN("other".to_string()));

        assert_eq!(driver_id.get().as_str(), "initial");
    }
}
