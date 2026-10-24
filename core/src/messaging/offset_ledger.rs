use crate::messaging::offset_tracker::{OffsetSet, OffsetTracker, PartitionLag};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Where settled positions go. The Kafka implementation stores the offset in the consumer, and
/// librdkafka commits stored offsets periodically.
pub trait OffsetSink: Send + Sync {
    /// Marks `next_offset` (the first offset that is not settled) as the position to commit.
    fn store(&self, partition: i32, next_offset: i64) -> Result<(), String>;
    /// Blocks until everything stored so far is committed.
    fn commit(&self) -> Result<(), String>;
}

impl<T: OffsetSink + ?Sized> OffsetSink for Arc<T> {
    fn store(&self, partition: i32, next_offset: i64) -> Result<(), String> {
        (**self).store(partition, next_offset)
    }

    fn commit(&self) -> Result<(), String> {
        (**self).commit()
    }
}

/// The tracker is shared with the consumer's rebalance callbacks, which only ever need the tracker.
pub type SharedTracker = Arc<Mutex<OffsetTracker>>;

pub fn lock_tracker(tracker: &Mutex<OffsetTracker>) -> MutexGuard<'_, OffsetTracker> {
    tracker.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One consumer's settlement state: a message's offset is registered when it is consumed and
/// settled once nothing will ever need the message again. Settling advances the contiguous prefix and
/// stores the new position with the consumer. Cloning shares the state.
#[derive(Clone)]
pub struct OffsetLedger {
    inner: Arc<LedgerInner>,
}

struct LedgerInner {
    name: &'static str,
    tracker: SharedTracker,
    sink: Box<dyn OffsetSink>,
}

impl OffsetLedger {
    #[must_use]
    pub fn new(name: &'static str, tracker: SharedTracker, sink: Box<dyn OffsetSink>) -> Self {
        Self {
            inner: Arc::new(LedgerInner {
                name,
                tracker,
                sink,
            }),
        }
    }

    /// Call in consumption order, before the message is handed on.
    pub fn register(&self, partition: i32, offset: i64) {
        lock_tracker(&self.inner.tracker).register(partition, offset);
    }

    /// Settles every message of a point batch. One lock and at most one store per partition run
    /// for the whole batch.
    pub fn settle(&self, set: &OffsetSet) {
        if set.is_empty() {
            return;
        }
        let mut tracker = lock_tracker(&self.inner.tracker);
        let mut advanced: Option<(i32, i64)> = None;
        for range in set.ranges() {
            let Some(position) = tracker.settle(range.partition, range.first, range.last) else {
                continue;
            };
            if let Some((partition, previous)) = advanced
                && partition != range.partition
            {
                self.store(partition, previous);
            }
            advanced = Some((range.partition, position));
        }
        if let Some((partition, position)) = advanced {
            self.store(partition, position);
        }
    }

    pub fn settle_one(&self, partition: i32, offset: i64) {
        let mut tracker = lock_tracker(&self.inner.tracker);
        if let Some(position) = tracker.settle(partition, offset, offset) {
            self.store(partition, position);
        }
    }

    /// Runs under the tracker lock, so a rebalance cannot drop a partition between computing its
    /// position and storing it, and positions of one partition are stored in order.
    fn store(&self, partition: i32, position: i64) {
        if let Err(error) = self.inner.sink.store(partition, position) {
            warn!(consumer = self.inner.name, partition, position, %error, "Failed to store offset");
        }
    }

    #[must_use]
    pub fn lag(&self) -> Vec<PartitionLag> {
        lock_tracker(&self.inner.tracker).lag()
    }

    /// Logs consumed-but-unsettled messages per partition, and warns about partitions whose
    /// position has not moved since `previous` (the last report) although messages are pending:
    /// the database is stuck, or a message never settles. Quiet when nothing is pending.
    pub fn log_lag(&self, previous: &mut Vec<PartitionLag>) {
        let lag = self.lag();
        let stalled = stalled_partitions(previous, &lag);
        if !stalled.is_empty() {
            warn!(
                consumer = self.inner.name,
                ?stalled,
                "Commit position has not advanced since the last report"
            );
        }
        if lag.iter().all(|entry| entry.pending == 0) {
            debug!(consumer = self.inner.name, "No unsettled offsets");
        } else {
            let partitions = lag
                .iter()
                .map(|entry| {
                    format!(
                        "p{}: pending={} position={}",
                        entry.partition, entry.pending, entry.position
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            info!(
                consumer = self.inner.name,
                "Unsettled offsets ({partitions})"
            );
        }
        *previous = lag;
    }

    /// Synchronously commits everything stored, bounded by `timeout`. Never fails the caller: what
    /// is not committed is replayed after a restart.
    ///
    /// On its own thread, not the blocking pool: that pool is one thread, and a commit to an
    /// unreachable broker outlives `timeout`.
    pub async fn commit_final(&self, timeout: Duration) -> bool {
        let unsettled: u64 = self.lag().iter().map(|entry| entry.pending).sum();
        let ledger = self.clone();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        if let Err(error) = std::thread::Builder::new()
            .name(format!("{}-commit", self.inner.name))
            .spawn(move || {
                let _ = done_tx.send(ledger.inner.sink.commit());
            })
        {
            warn!(consumer = self.inner.name, %error, "Final offset commit could not start");
            return false;
        }
        let committed = match tokio::time::timeout(timeout, done_rx).await {
            Ok(Ok(Ok(()))) => true,
            Ok(Ok(Err(error))) => {
                warn!(consumer = self.inner.name, %error, "Final offset commit failed; uncommitted messages are replayed after a restart");
                false
            }
            Ok(Err(_)) => {
                warn!(
                    consumer = self.inner.name,
                    "Final offset commit thread failed"
                );
                false
            }
            Err(_) => {
                warn!(
                    consumer = self.inner.name,
                    ?timeout,
                    "Final offset commit timed out; uncommitted messages are replayed after a restart"
                );
                false
            }
        };
        if committed {
            info!(
                consumer = self.inner.name,
                unsettled, "Final offset commit done"
            );
        }
        committed
    }
}

fn stalled_partitions(previous: &[PartitionLag], current: &[PartitionLag]) -> Vec<i32> {
    current
        .iter()
        .filter(|entry| {
            entry.pending > 0
                && previous.iter().any(|last| {
                    last.partition == entry.partition && last.position == entry.position
                })
        })
        .map(|entry| entry.partition)
        .collect()
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Records what would reach the consumer.
    #[derive(Default)]
    pub struct RecordingSink {
        pub stored: Mutex<Vec<(i32, i64)>>,
        pub commits: AtomicUsize,
        pub fail_commit: bool,
    }

    impl RecordingSink {
        pub fn stored(&self) -> Vec<(i32, i64)> {
            self.stored.lock().unwrap().clone()
        }

        /// The last stored position of each partition, in partition order.
        pub fn positions(&self) -> Vec<(i32, i64)> {
            let mut last = std::collections::BTreeMap::new();
            for (partition, position) in self.stored() {
                last.insert(partition, position);
            }
            last.into_iter().collect()
        }
    }

    impl OffsetSink for RecordingSink {
        fn store(&self, partition: i32, next_offset: i64) -> Result<(), String> {
            self.stored.lock().unwrap().push((partition, next_offset));
            Ok(())
        }

        fn commit(&self) -> Result<(), String> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            if self.fail_commit {
                Err("broker unavailable".into())
            } else {
                Ok(())
            }
        }
    }

    #[must_use]
    pub fn recording_ledger() -> (OffsetLedger, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        let ledger = OffsetLedger::new(
            "test",
            SharedTracker::default(),
            Box::new(Arc::clone(&sink)),
        );
        (ledger, sink)
    }

    /// A ledger nobody inspects, for components that only need something to settle into.
    #[must_use]
    pub fn detached_ledger() -> OffsetLedger {
        recording_ledger().0
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{RecordingSink, recording_ledger};
    use super::*;
    use std::sync::atomic::Ordering;

    fn set(offsets: &[(i32, i64)]) -> OffsetSet {
        let mut set = OffsetSet::default();
        for &(partition, offset) in offsets {
            set.push(partition, offset);
        }
        set
    }

    #[test]
    fn settling_a_batch_stores_each_partition_once_per_advance() {
        let (ledger, sink) = recording_ledger();
        for offset in 0..6 {
            ledger.register(0, offset);
            ledger.register(1, offset + 10);
        }

        ledger.settle(&set(&[(0, 0), (0, 1), (0, 2), (1, 10), (1, 11)]));

        assert_eq!(sink.stored(), [(0, 3), (1, 12)]);
    }

    #[test]
    fn a_batch_split_into_many_ranges_stores_each_partition_once() {
        let (ledger, sink) = recording_ledger();
        for offset in 0..10 {
            ledger.register(0, offset);
            ledger.register(1, offset);
        }
        // Odd offsets went the other path and are already settled: each even offset is its own range.
        ledger.settle(&set(&[
            (0, 1),
            (0, 3),
            (0, 5),
            (0, 7),
            (0, 9),
            (1, 1),
            (1, 3),
        ]));
        assert_eq!(sink.stored(), [] as [(i32, i64); 0]);

        ledger.settle(&set(&[
            (0, 0),
            (0, 2),
            (0, 4),
            (1, 0),
            (1, 2),
            (0, 6),
            (0, 8),
        ]));

        assert_eq!(sink.stored(), [(0, 6), (1, 4), (0, 10)]);
    }

    #[test]
    fn a_partition_is_stalled_when_its_position_holds_while_messages_are_pending() {
        let lag = |partition, pending, position| PartitionLag {
            partition,
            pending,
            position,
        };
        let previous = [lag(0, 3, 10), lag(1, 3, 20), lag(2, 0, 30)];

        let current = [lag(0, 9, 10), lag(1, 3, 21), lag(2, 0, 30), lag(3, 1, 0)];

        assert_eq!(stalled_partitions(&previous, &current), [0]);
        assert_eq!(stalled_partitions(&[], &current), [] as [i32; 0]);
    }

    #[test]
    fn nothing_is_stored_while_the_lowest_offset_is_unsettled() {
        let (ledger, sink) = recording_ledger();
        for offset in 0..4 {
            ledger.register(0, offset);
        }

        ledger.settle(&set(&[(0, 1), (0, 2), (0, 3)]));
        assert_eq!(sink.stored(), [] as [(i32, i64); 0]);

        ledger.settle_one(0, 0);
        assert_eq!(sink.stored(), [(0, 4)]);
    }

    #[test]
    fn settling_an_empty_set_or_unknown_partition_is_a_no_op() {
        let (ledger, sink) = recording_ledger();
        ledger.register(0, 0);

        ledger.settle(&OffsetSet::default());
        ledger.settle(&set(&[(9, 0)]));
        ledger.settle_one(9, 0);

        assert_eq!(sink.stored(), [] as [(i32, i64); 0]);
        assert_eq!(ledger.lag()[0].pending, 1);
    }

    #[test]
    fn revoked_partitions_are_not_stored_after_the_tracker_forgot_them() {
        let (ledger, sink) = recording_ledger();
        let tracker = Arc::clone(&ledger.inner.tracker);
        for offset in 0..3 {
            ledger.register(0, offset);
        }

        lock_tracker(&tracker).forget([0]);
        ledger.settle(&set(&[(0, 0), (0, 1), (0, 2)]));

        assert_eq!(sink.stored(), [] as [(i32, i64); 0]);
    }

    #[tokio::test]
    async fn final_commit_reports_success_and_failure_without_panicking() {
        let (ledger, sink) = recording_ledger();
        assert!(ledger.commit_final(Duration::from_secs(1)).await);
        assert_eq!(sink.commits.load(Ordering::SeqCst), 1);

        let failing = Arc::new(RecordingSink {
            fail_commit: true,
            ..RecordingSink::default()
        });
        let ledger = OffsetLedger::new("test", SharedTracker::default(), Box::new(failing));
        assert!(!ledger.commit_final(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn final_commit_is_bounded_by_its_timeout() {
        struct Stuck;
        impl OffsetSink for Stuck {
            fn store(&self, _: i32, _: i64) -> Result<(), String> {
                Ok(())
            }
            fn commit(&self) -> Result<(), String> {
                std::thread::sleep(Duration::from_millis(300));
                Ok(())
            }
        }
        let ledger = OffsetLedger::new("test", SharedTracker::default(), Box::new(Stuck));

        let started = std::time::Instant::now();
        assert!(!ledger.commit_final(Duration::from_millis(30)).await);
        assert!(started.elapsed() < Duration::from_millis(250));
    }
}
