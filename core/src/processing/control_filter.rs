use crate::messaging::control_message::CoreControlMessage;
use crate::messaging::dlq_handler::{InvalidMessageTx, InvalidOutcome};
use crate::messaging::message_sink::SinkRx;
use crate::messaging::offset_ledger::OffsetLedger;
use crate::messaging::partial_message::{PartialMessageWithError, SourceOffset};
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

pub struct ControlFilter {
    control_rx: SinkRx,
    dlq_tx: InvalidMessageTx,
    chdrv_handler: &'static RetainReadTimeValue<CompactString>,
    /// Carries the offset of the SHUTDOWN event.
    shutdown_handler_tx: Option<oneshot::Sender<SourceOffset>>,
    offsets: OffsetLedger,
}

impl ControlFilter {
    #[must_use]
    pub fn new(
        control_rx: SinkRx,
        dlq_tx: InvalidMessageTx,
        chdrv_handler: &'static RetainReadTimeValue<CompactString>,
        shutdown_handler_tx: oneshot::Sender<SourceOffset>,
        offsets: OffsetLedger,
    ) -> Self {
        Self {
            control_rx,
            dlq_tx,
            chdrv_handler,
            shutdown_handler_tx: Some(shutdown_handler_tx),
            offsets,
        }
    }

    /// Applies control events inline in arrival order and settles each one's offset once handled:
    /// applied, ignored, or handed to the DLQ. SHUTDOWN is the exception, see
    /// [`Self::apply_control_event`]. After SHUTDOWN is relayed, stops consuming so this task's
    /// DLQ sender is dropped and the DLQ channel can close.
    pub async fn ingest(&mut self) {
        info!("Starting control ingestion loop");
        while let Some(partial_message) = self.control_rx.recv().await {
            let at = partial_message.source_offset();
            match CoreControlMessage::try_from(partial_message) {
                Ok(message) => {
                    if self.apply_control_event(message, at) {
                        break;
                    }
                }
                Err(error) => {
                    let message = error.message;
                    // A queued record is settled by the DLQ handler once delivered.
                    if let Err(send_error) = self
                        .dlq_tx
                        .send(PartialMessageWithError::new(message, &error.kind))
                        .await
                    {
                        error!(%send_error, "Failed to send to dlq");
                        self.dlq_tx.settler().settle(at, InvalidOutcome::Dropped);
                    }
                }
            }
        }
        warn!("Control ingestion loop terminated");
    }

    /// Returns `true` when the filter should stop consuming (SHUTDOWN relayed).
    ///
    /// SHUTDOWN is not settled: its offset goes to the supervisor, which settles it after the
    /// export sequence. Nothing after it in the partition can be committed either, so a crash
    /// mid-export replays SHUTDOWN.
    fn apply_control_event(&mut self, control_event: CoreControlMessage, at: SourceOffset) -> bool {
        info!("Received control event: {control_event}");
        match control_event {
            CoreControlMessage::CHDRV(body) => {
                self.chdrv_handler.update(body.new_driver_id);
            }
            CoreControlMessage::SHUTDOWN => {
                if let Some(tx) = self.shutdown_handler_tx.take() {
                    let _ = tx.send(at);
                }
                return true;
            }
            CoreControlMessage::UNKNOWN(_) => {}
        }
        self.offsets.settle_one(at.partition, at.offset);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::control_message::{ChangeDriverMessage, CoreControlMessage};
    use crate::messaging::dlq_handler::{InvalidSettler, invalid_message_channel};
    use crate::messaging::offset_ledger::testing::{RecordingSink, recording_ledger};
    use crate::messaging::partial_message::{MessageOrigin, PartialMessage};
    use std::sync::Arc;

    struct Fixture {
        filter: ControlFilter,
        driver_id: &'static RetainReadTimeValue<CompactString>,
        control_tx: Option<tokio::sync::mpsc::Sender<PartialMessage>>,
        shutdown_rx: oneshot::Receiver<SourceOffset>,
        ledger: OffsetLedger,
        sink: Arc<RecordingSink>,
    }

    fn fixture(dlq_open: bool) -> Fixture {
        let driver_id = Box::leak(Box::new(RetainReadTimeValue::new(CompactString::from(
            "initial",
        ))));
        let (ledger, sink) = recording_ledger();
        let settler = InvalidSettler::new(recording_ledger().0, ledger.clone());
        let (dlq_tx, dlq_rx) = invalid_message_channel(1, settler);
        if !dlq_open {
            drop(dlq_rx);
        } else {
            // Keep the receiver alive for the test's duration.
            Box::leak(Box::new(dlq_rx));
        }
        let (control_tx, control_rx) = tokio::sync::mpsc::channel(8);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        Fixture {
            filter: ControlFilter::new(control_rx, dlq_tx, driver_id, shutdown_tx, ledger.clone()),
            driver_id,
            control_tx: Some(control_tx),
            shutdown_rx,
            ledger,
            sink,
        }
    }

    impl Fixture {
        async fn send(&self, message: PartialMessage) {
            self.control_tx
                .as_ref()
                .unwrap()
                .send(message)
                .await
                .unwrap();
        }

        fn close_input(&mut self) {
            self.control_tx = None;
        }
    }

    fn event(
        offset: i64,
        key: &[u8],
        payload: Option<&[u8]>,
        ledger: &OffsetLedger,
    ) -> PartialMessage {
        ledger.register(0, offset);
        PartialMessage {
            key: Some(Box::from(key)),
            offset,
            partition: 0,
            payload: payload.map(Box::from),
            origin: MessageOrigin::Control,
        }
    }

    fn at(offset: i64) -> SourceOffset {
        SourceOffset {
            origin: MessageOrigin::Control,
            partition: 0,
            offset,
        }
    }

    #[test]
    fn change_driver_event_updates_runtime_driver_id() {
        let mut fixture = fixture(true);
        fixture.ledger.register(0, 0);

        assert!(!fixture.filter.apply_control_event(
            CoreControlMessage::CHDRV(ChangeDriverMessage {
                new_driver_id: CompactString::from("driver-42"),
            }),
            at(0)
        ));

        assert_eq!(fixture.driver_id.get().as_str(), "driver-42");
        assert_eq!(
            fixture.sink.positions(),
            [(0, 1)],
            "handled events are settled"
        );
    }

    #[tokio::test]
    async fn shutdown_event_notifies_with_its_offset_and_is_not_settled() {
        let mut fixture = fixture(true);
        fixture.ledger.register(0, 0);

        assert!(
            fixture
                .filter
                .apply_control_event(CoreControlMessage::SHUTDOWN, at(0))
        );

        assert_eq!(fixture.shutdown_rx.try_recv().unwrap(), at(0));
        assert_eq!(fixture.driver_id.get().as_str(), "initial");
        assert!(fixture.filter.shutdown_handler_tx.is_none());
        assert!(fixture.sink.stored().is_empty());
        assert_eq!(fixture.ledger.lag()[0].pending, 1);
    }

    #[test]
    fn unknown_event_does_not_change_runtime_state_but_is_settled() {
        let mut fixture = fixture(true);
        fixture.ledger.register(0, 0);

        assert!(
            !fixture
                .filter
                .apply_control_event(CoreControlMessage::UNKNOWN("other".to_string()), at(0))
        );

        assert_eq!(fixture.driver_id.get().as_str(), "initial");
        assert_eq!(fixture.sink.positions(), [(0, 1)]);
    }

    #[tokio::test]
    async fn ingest_settles_events_in_order_and_holds_everything_from_shutdown_on() {
        let mut fixture = fixture(true);
        let messages = [
            event(0, b"CHDRV", Some(b"driver-1"), &fixture.ledger),
            event(1, b"OTHER", None, &fixture.ledger),
            event(2, b"SHUTDOWN", None, &fixture.ledger),
            event(3, b"CHDRV", Some(b"driver-2"), &fixture.ledger),
        ];
        for message in messages {
            fixture.send(message).await;
        }
        fixture.close_input();

        fixture.filter.ingest().await;

        assert_eq!(fixture.driver_id.get().as_str(), "driver-1");
        assert_eq!(fixture.shutdown_rx.try_recv().unwrap(), at(2));
        assert_eq!(fixture.sink.positions(), [(0, 2)]);
        let lag = fixture.ledger.lag();
        assert_eq!((lag[0].pending, lag[0].position), (2, 2));

        // The supervisor settles SHUTDOWN after the export: the position moves to the unhandled
        // event behind it, which the filter never consumed.
        fixture.ledger.settle_one(0, 2);
        assert_eq!(fixture.sink.positions(), [(0, 3)]);
    }

    #[tokio::test]
    async fn invalid_events_are_left_to_the_dlq_handler_to_settle() {
        let mut fixture = fixture(true);
        let invalid = event(0, b"CHDRV", None, &fixture.ledger);
        fixture.send(invalid).await;
        fixture.close_input();

        fixture.filter.ingest().await;

        assert!(fixture.sink.stored().is_empty());
        assert_eq!(fixture.ledger.lag()[0].pending, 1);
    }

    #[tokio::test]
    async fn invalid_events_are_settled_when_the_dlq_is_gone() {
        let mut fixture = fixture(false);
        let invalid = event(0, b"CHDRV", None, &fixture.ledger);
        fixture.send(invalid).await;
        fixture.close_input();

        fixture.filter.ingest().await;

        assert_eq!(fixture.sink.positions(), [(0, 1)]);
    }
}
