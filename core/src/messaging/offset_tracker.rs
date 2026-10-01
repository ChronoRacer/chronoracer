use std::collections::{BTreeMap, HashMap};

/// Tracks, per partition, which consumed offsets are not settled yet, so the position that is safe to
/// commit is always the lowest unsettled offset (or one past the highest consumed offset when nothing
/// is pending). Offsets arrive in order per partition, settlement arrives in any order.
///
/// Pure bookkeeping: no I/O and no locking, see [`crate::messaging::offset_ledger::OffsetLedger`].
#[derive(Debug, Default)]
pub struct OffsetTracker {
    partitions: HashMap<i32, PartitionOffsets>,
}

#[derive(Debug)]
struct PartitionOffsets {
    /// Consumed and unsettled offsets as disjoint inclusive ranges, `start -> end`. Consumption is
    /// contiguous in practice, so this stays a handful of entries however many messages are in flight.
    pending: BTreeMap<i64, i64>,
    /// One past the highest consumed offset.
    next: i64,
    /// Highest position handed out for commit. Starts at the first consumed offset: there is nothing
    /// to commit below where this consumer began.
    stored: i64,
}

/// Point-in-time view of one partition, for logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionLag {
    pub partition: i32,
    /// Consumed offsets that are not settled yet.
    pub pending: u64,
    /// Position that is safe to commit right now.
    pub position: i64,
}

impl PartitionOffsets {
    fn new(first: i64) -> Self {
        Self {
            pending: BTreeMap::new(),
            next: first,
            stored: first,
        }
    }

    fn register(&mut self, offset: i64) {
        if offset >= self.next {
            match self.pending.last_entry() {
                Some(mut last) if *last.get() + 1 == offset => *last.get_mut() = offset,
                _ => {
                    self.pending.insert(offset, offset);
                }
            }
            self.next = offset + 1;
            return;
        }

        // Below the high mark: a redelivery. It becomes pending again (and may settle again).
        let mut start = offset;
        let mut end = offset;
        if let Some((&pred_start, &pred_end)) = self.pending.range(..=offset).next_back() {
            if pred_end >= offset {
                return;
            }
            if pred_end + 1 == offset {
                start = pred_start;
            }
        }
        if let Some(succ_end) = self.pending.remove(&(offset + 1)) {
            end = succ_end;
        }
        self.pending.insert(start, end);
    }

    fn position(&self) -> i64 {
        self.pending
            .first_key_value()
            .map_or(self.next, |(&start, _)| start)
    }

    fn settle(&mut self, first: i64, last: i64) -> Option<i64> {
        if first > last {
            return None;
        }
        // Ranges are disjoint and sorted: walk back from `last` while they reach `first`.
        while let Some((&start, &end)) = self.pending.range(..=last).next_back() {
            if end < first {
                break;
            }
            self.pending.remove(&start);
            if start < first {
                self.pending.insert(start, first - 1);
            }
            if end > last {
                self.pending.insert(last + 1, end);
            }
        }
        let position = self.position();
        (position > self.stored).then(|| {
            self.stored = position;
            position
        })
    }

    fn pending_count(&self) -> u64 {
        self.pending
            .iter()
            .map(|(&start, &end)| u64::try_from(end - start + 1).unwrap_or(0))
            .sum()
    }
}

impl OffsetTracker {
    /// Records a consumed offset. Call in consumption order, before the message can settle.
    pub fn register(&mut self, partition: i32, offset: i64) {
        self.partitions
            .entry(partition)
            .or_insert_with(|| PartitionOffsets::new(offset))
            .register(offset);
    }

    /// Settles `first..=last`. Offsets that are unknown, already settled, or on a partition this
    /// consumer no longer owns are ignored. Returns the new position to commit if it advanced.
    pub fn settle(&mut self, partition: i32, first: i64, last: i64) -> Option<i64> {
        self.partitions.get_mut(&partition)?.settle(first, last)
    }

    /// Drops all state of the partitions: on revoke (nothing may be committed for them any more)
    /// and on assign (consumption restarts at the committed offset).
    pub fn forget(&mut self, partitions: impl IntoIterator<Item = i32>) {
        for partition in partitions {
            self.partitions.remove(&partition);
        }
    }

    #[must_use]
    pub fn lag(&self) -> Vec<PartitionLag> {
        let mut lag: Vec<_> = self
            .partitions
            .iter()
            .map(|(&partition, state)| PartitionLag {
                partition,
                pending: state.pending_count(),
                position: state.position(),
            })
            .collect();
        lag.sort_by_key(|entry| entry.partition);
        lag
    }
}

/// How many recent ranges [`OffsetSet::push`] looks through for one to extend.
const COALESCE_LOOKBACK: usize = 8;

/// Inclusive offset range of one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetRange {
    pub partition: i32,
    pub first: i64,
    pub last: i64,
}

/// The source offsets of the messages in one point batch, as coalesced ranges. Messages of a
/// partition are mostly consecutive, so a batch of thousands of points carries a few ranges.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OffsetSet {
    ranges: Vec<OffsetRange>,
}

impl OffsetSet {
    pub fn push(&mut self, partition: i32, offset: i64) {
        for range in self.ranges.iter_mut().rev().take(COALESCE_LOOKBACK) {
            if range.partition != partition {
                continue;
            }
            if offset == range.last + 1 {
                range.last = offset;
                return;
            }
            if (range.first..=range.last).contains(&offset) {
                return;
            }
        }
        self.ranges.push(OffsetRange {
            partition,
            first: offset,
            last: offset,
        });
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    #[must_use]
    pub fn ranges(&self) -> &[OffsetRange] {
        &self.ranges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registered(partition: i32, offsets: std::ops::Range<i64>) -> OffsetTracker {
        let mut tracker = OffsetTracker::default();
        for offset in offsets {
            tracker.register(partition, offset);
        }
        tracker
    }

    #[test]
    fn in_order_settlement_advances_the_position_one_by_one() {
        let mut tracker = registered(0, 10..14);

        assert_eq!(tracker.settle(0, 10, 10), Some(11));
        assert_eq!(tracker.settle(0, 11, 11), Some(12));
        assert_eq!(tracker.settle(0, 12, 13), Some(14));
        assert_eq!(tracker.lag()[0].pending, 0);
    }

    #[test]
    fn out_of_order_settlement_waits_for_the_gap_to_close() {
        let mut tracker = registered(0, 0..6);

        assert_eq!(tracker.settle(0, 3, 4), None);
        assert_eq!(tracker.settle(0, 1, 2), None);
        assert_eq!(tracker.settle(0, 5, 5), None);
        assert_eq!(tracker.settle(0, 0, 0), Some(6));
        assert_eq!(tracker.lag()[0].pending, 0);
        assert_eq!(tracker.lag()[0].position, 6);
    }

    #[test]
    fn a_single_unsettled_offset_holds_the_position_back() {
        let mut tracker = registered(0, 0..100);

        assert_eq!(tracker.settle(0, 1, 99), None);
        assert_eq!(tracker.lag()[0].pending, 1);
        assert_eq!(tracker.settle(0, 0, 0), Some(100));
    }

    #[test]
    fn settling_inside_a_range_splits_it() {
        let mut tracker = registered(0, 0..10);

        assert_eq!(tracker.settle(0, 4, 5), None);
        assert_eq!(tracker.lag()[0].pending, 8);
        assert_eq!(tracker.settle(0, 0, 3), Some(6));
        assert_eq!(tracker.settle(0, 6, 9), Some(10));
    }

    #[test]
    fn offset_gaps_in_the_topic_do_not_block_the_position() {
        let mut tracker = OffsetTracker::default();
        tracker.register(0, 5);
        tracker.register(0, 6);
        // 7..=9 never reach the consumer (compaction, transaction markers).
        tracker.register(0, 10);

        assert_eq!(tracker.settle(0, 5, 6), Some(10));
        assert_eq!(tracker.settle(0, 10, 10), Some(11));
    }

    #[test]
    fn partitions_are_independent() {
        let mut tracker = OffsetTracker::default();
        for offset in 0..3 {
            tracker.register(0, offset);
            tracker.register(1, offset + 100);
        }

        assert_eq!(tracker.settle(1, 100, 102), Some(103));
        assert_eq!(tracker.settle(0, 1, 2), None);
        assert_eq!(tracker.settle(0, 0, 0), Some(3));
        let lag = tracker.lag();
        assert_eq!(
            lag,
            [
                PartitionLag { partition: 0, pending: 0, position: 3 },
                PartitionLag { partition: 1, pending: 0, position: 103 },
            ]
        );
    }

    #[test]
    fn settling_twice_is_harmless() {
        let mut tracker = registered(0, 0..4);

        assert_eq!(tracker.settle(0, 0, 1), Some(2));
        assert_eq!(tracker.settle(0, 0, 1), None);
        assert_eq!(tracker.settle(0, 1, 1), None);
        assert_eq!(tracker.settle(0, 2, 3), Some(4));
        assert_eq!(tracker.settle(0, 2, 3), None);
    }

    #[test]
    fn settling_unknown_offsets_or_partitions_changes_nothing() {
        let mut tracker = registered(0, 10..13);

        assert_eq!(tracker.settle(7, 10, 12), None, "unknown partition");
        assert_eq!(tracker.settle(0, 500, 600), None, "never consumed");
        assert_eq!(tracker.settle(0, 0, 9), None, "below the first consumed offset");
        assert_eq!(tracker.settle(0, 12, 11), None, "inverted range");
        assert_eq!(tracker.lag()[0].pending, 3);
        assert_eq!(tracker.lag()[0].position, 10);
    }

    #[test]
    fn a_range_straddling_settled_and_unsettled_offsets_only_settles_the_pending_ones() {
        let mut tracker = registered(0, 0..10);
        assert_eq!(tracker.settle(0, 0, 4), Some(5));

        assert_eq!(tracker.settle(0, 3, 7), Some(8));
        assert_eq!(tracker.lag()[0].pending, 2);
        assert_eq!(tracker.settle(0, 8, 20), Some(10));
    }

    #[test]
    fn position_never_goes_backwards() {
        let mut tracker = registered(0, 0..4);
        assert_eq!(tracker.settle(0, 0, 3), Some(4));

        // Redelivery of a settled offset (a seek back) makes it pending again.
        tracker.register(0, 2);
        assert_eq!(tracker.lag()[0].pending, 1);
        assert_eq!(tracker.lag()[0].position, 2);
        assert_eq!(tracker.settle(0, 2, 2), None, "4 was already handed out");

        tracker.register(0, 4);
        assert_eq!(tracker.settle(0, 4, 4), Some(5));
    }

    #[test]
    fn redelivered_offsets_merge_with_neighbouring_ranges() {
        let mut tracker = registered(0, 0..10);
        assert_eq!(tracker.settle(0, 3, 5), None);
        assert_eq!(tracker.lag()[0].pending, 7);

        tracker.register(0, 4);
        tracker.register(0, 3);
        tracker.register(0, 5);
        tracker.register(0, 5);
        assert_eq!(tracker.lag()[0].pending, 10);

        assert_eq!(tracker.settle(0, 0, 9), Some(10));
    }

    #[test]
    fn revoke_drops_state_and_late_settlement_is_ignored() {
        let mut tracker = registered(0, 0..5);
        tracker.register(1, 50);

        tracker.forget([0]);

        assert_eq!(tracker.settle(0, 0, 4), None);
        assert_eq!(tracker.lag().iter().map(|l| l.partition).collect::<Vec<_>>(), [1]);
        assert_eq!(tracker.settle(1, 50, 50), Some(51));
    }

    #[test]
    fn a_reassigned_partition_starts_fresh() {
        let mut tracker = registered(0, 100..110);
        assert_eq!(tracker.settle(0, 100, 104), Some(105));

        tracker.forget([0]);
        // Consumption resumes at the committed offset 105, so nothing below it is pending.
        for offset in 105..108 {
            tracker.register(0, offset);
        }

        assert_eq!(tracker.lag()[0], PartitionLag { partition: 0, pending: 3, position: 105 });
        // The old generation's in-flight message settles: it is durable, so it counts.
        assert_eq!(tracker.settle(0, 105, 105), Some(106));
        assert_eq!(tracker.settle(0, 106, 107), Some(108));
    }

    #[test]
    fn the_first_consumed_offset_is_the_baseline_not_a_commit() {
        let mut tracker = OffsetTracker::default();
        tracker.register(0, 40);
        tracker.register(0, 41);

        // Settling out of order must not hand out 40 again as a "new" position.
        assert_eq!(tracker.settle(0, 41, 41), None);
        assert_eq!(tracker.settle(0, 40, 40), Some(42));
    }

    #[test]
    fn many_messages_in_flight_stay_a_single_range() {
        let mut tracker = registered(0, 0..50_000);

        assert_eq!(tracker.partitions[&0].pending.len(), 1);
        assert_eq!(tracker.settle(0, 0, 24_999), Some(25_000));
        assert_eq!(tracker.partitions[&0].pending.len(), 1);
        assert_eq!(tracker.settle(0, 25_000, 49_999), Some(50_000));
        assert!(tracker.partitions[&0].pending.is_empty());
    }

    #[test]
    fn offset_set_coalesces_consecutive_offsets_per_partition() {
        let mut set = OffsetSet::default();
        for offset in 5..10 {
            set.push(0, offset);
            set.push(1, offset * 2);
        }
        set.push(0, 10);

        assert_eq!(
            set.ranges(),
            [
                OffsetRange { partition: 0, first: 5, last: 10 },
                OffsetRange { partition: 1, first: 10, last: 10 },
                OffsetRange { partition: 1, first: 12, last: 12 },
                OffsetRange { partition: 1, first: 14, last: 14 },
                OffsetRange { partition: 1, first: 16, last: 16 },
                OffsetRange { partition: 1, first: 18, last: 18 },
            ]
        );
    }

    #[test]
    fn offset_set_ignores_duplicates_and_keeps_gaps_as_separate_ranges() {
        let mut set = OffsetSet::default();
        assert!(set.is_empty());
        for offset in [3, 4, 4, 3, 9, 10] {
            set.push(2, offset);
        }

        assert_eq!(
            set.ranges(),
            [
                OffsetRange { partition: 2, first: 3, last: 4 },
                OffsetRange { partition: 2, first: 9, last: 10 },
            ]
        );
    }

    #[test]
    fn offset_set_ranges_settle_exactly_their_messages() {
        let mut tracker = registered(0, 0..10);
        let mut set = OffsetSet::default();
        for offset in [0, 1, 2, 5, 6] {
            set.push(0, offset);
        }

        let mut position = None;
        for range in set.ranges() {
            position = tracker.settle(range.partition, range.first, range.last).or(position);
        }

        assert_eq!(position, Some(3));
        assert_eq!(tracker.lag()[0].pending, 5);
    }
}
