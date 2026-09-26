use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

pub(crate) const MAX_POINTS_PER_BATCH: usize = 1024;

/// One `offer` is one slot. The point budget admits data; a full channel is only the fallback
/// when the telemetry task is not draining.
const HANDOFF_SLOTS: usize = 256;

/// Unsequenced points may fill the retained cap twice: the batch still in the channel, plus a
/// replacement batch that eviction will keep.
const INFLIGHT_BUFFERS: usize = 2;

struct Incoming {
    messages: Vec<Arc<UnwoundSensorDataMessage>>,
    driver_id: Arc<CompactString>,
    enqueued_at: Instant,
    generation: u64,
    /// Points reserved in `inflight` for `messages`.
    points: usize,
    /// Points from this batch that did not fit the point budget.
    missed: u64,
}

struct Queued {
    message: Arc<UnwoundSensorDataMessage>,
    driver_id: Arc<CompactString>,
    enqueued_at: Instant,
    /// Sequence of `readings[cursor]`.
    first_sequence: u64,
    /// First unread index in `message.readings`.
    cursor: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct GapSpan {
    pub(crate) first_sequence: u64,
    pub(crate) dropped: u64,
}

pub(crate) struct Chunk {
    pub(crate) message: Arc<UnwoundSensorDataMessage>,
    pub(crate) driver_id: Arc<CompactString>,
    pub(crate) first_sequence: u64,
    pub(crate) start: usize,
    pub(crate) len: usize,
}

pub(crate) enum Drain {
    Idle,
    Replaced,
    Ready {
        /// Oldest points dropped while they sat in the queue.
        gap: Option<GapSpan>,
        /// Points refused by the handoff, sequenced after everything still queued.
        tail: Option<GapSpan>,
        chunks: Vec<Chunk>,
    },
}

struct State {
    generation: u64,
    active: bool,
    /// Next sequence to assign. A miss advances this and stores no point slot.
    next_sequence: u64,
    /// Next sequence `try_drain` has not returned.
    ///
    /// Point slots cover some of `[next_unread, next_sequence)`. Every other sequence in that
    /// range is a gap. Eviction drops point slots and leaves `next_unread` where it is.
    next_unread: u64,
    points: usize,
    slots: VecDeque<Queued>,
}

/// Sync handoff from the parse threads into the serving queue.
///
/// `offer` never locks the serving queue and never waits. With no caller it returns after one
/// atomic load. Sequencing, age, and the point cap run in `pull_ingress`, on the telemetry task.
#[derive(Clone)]
pub struct PreviewHub {
    inner: Arc<Inner>,
}

struct Inner {
    driver: &'static RetainReadTimeValue<CompactString>,
    max_points: usize,
    /// Admit budget for batches not yet sequenced. Above this, `offer` records a tail gap.
    max_inflight: usize,
    max_age: Duration,
    connected: AtomicBool,
    /// Mirrors `State::generation` so `offer` can tag a batch without the queue lock.
    generation: AtomicU64,
    inflight: AtomicUsize,
    /// Drops that did not fit in a handoff slot. The success path of `offer` does not lock this.
    missed: Mutex<Missed>,
    notify: Arc<Notify>,
    tx: SyncSender<Incoming>,
    rx: Mutex<Receiver<Incoming>>,
    state: Mutex<State>,
    stream_id: String,
}

impl PreviewHub {
    #[must_use]
    pub fn new(
        driver: &'static RetainReadTimeValue<CompactString>,
        max_points: usize,
        max_age: Duration,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(HANDOFF_SLOTS);
        Self {
            inner: Arc::new(Inner {
                driver,
                max_points,
                max_inflight: inflight_budget(max_points),
                max_age,
                connected: AtomicBool::new(false),
                generation: AtomicU64::new(0),
                inflight: AtomicUsize::new(0),
                missed: Mutex::new(Missed {
                    generation: 0,
                    count: 0,
                }),
                notify: Arc::new(Notify::new()),
                tx,
                rx: Mutex::new(rx),
                state: Mutex::new(State {
                    generation: 0,
                    active: false,
                    next_sequence: 1,
                    next_unread: 1,
                    points: 0,
                    slots: VecDeque::new(),
                }),
                stream_id: new_stream_id(),
            }),
        }
    }

    #[must_use]
    pub fn stream_id(&self) -> &str {
        &self.inner.stream_id
    }

    #[must_use]
    pub fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.notify)
    }

    #[must_use]
    pub(crate) fn max_age(&self) -> Duration {
        self.inner.max_age
    }

    /// Points sequenced and not yet drained. Test and diagnostics only.
    #[must_use]
    pub fn queued_points(&self) -> usize {
        self.lock().points
    }

    /// Hand the batch to the telemetry task.
    ///
    /// One `try_send` for the whole batch. Returns as soon as that send accepts or refuses.
    /// Does not lock the serving queue. A point-budget miss travels with the batch. A full
    /// channel adds the same points to the miss cell. Either becomes a gap on the next
    /// `pull_ingress`.
    pub fn offer(&self, batch: &[Arc<UnwoundSensorDataMessage>]) {
        if !self.inner.connected.load(Ordering::Acquire) {
            return;
        }
        let generation = self.inner.generation.load(Ordering::Acquire);
        let driver_id = self.inner.driver.get();
        let enqueued_at = Instant::now();
        let mut messages = Vec::with_capacity(batch.len());
        let mut parked = 0usize;
        let mut missed = 0u64;
        for message in batch {
            let points = message.readings.len();
            if points == 0 {
                continue;
            }
            if self.try_reserve(points) {
                parked += points;
                messages.push(Arc::clone(message));
            } else {
                missed = missed.saturating_add(as_u64(points));
            }
        }
        if parked == 0 && missed == 0 {
            return;
        }
        let incoming = Incoming {
            messages,
            driver_id,
            enqueued_at,
            generation,
            points: parked,
            missed,
        };
        if let Err(TrySendError::Full(incoming) | TrySendError::Disconnected(incoming)) =
            self.inner.tx.try_send(incoming)
        {
            self.release_inflight(incoming.points);
            self.note_miss(
                generation,
                as_u64(incoming.points).saturating_add(incoming.missed),
            );
        }
        self.inner.notify.notify_waiters();
    }

    /// Start a `Watch`. Drops any previous queue and resets sequence to 1.
    pub(crate) fn begin_stream(&self) -> (u64, u64) {
        let generation = {
            let mut state = self.lock();
            let generation = state.generation.wrapping_add(1).max(1);
            state.generation = generation;
            state.active = true;
            state.next_sequence = 1;
            state.next_unread = 1;
            state.points = 0;
            state.slots.clear();
            self.inner.generation.store(generation, Ordering::Release);
            generation
        };
        self.inner.connected.store(true, Ordering::Release);
        self.pull_ingress();
        self.inner.notify.notify_waiters();
        (generation, 1)
    }

    pub(crate) fn end_stream(&self, generation: u64) {
        {
            let mut state = self.lock();
            if state.generation != generation {
                return;
            }
            state.active = false;
            state.points = 0;
            state.slots.clear();
            self.inner.connected.store(false, Ordering::Release);
        }
        self.inner.notify.notify_waiters();
    }

    /// Sequence handoff batches and drop oldest points that are past age or the point cap.
    ///
    /// Pump and `Watch` both call this. The state mutex serializes them. `offer` never calls it.
    /// Returns whether a waiter should be woken.
    pub(crate) fn pull_ingress(&self) -> bool {
        let incoming = {
            let rx = self.rx_lock();
            let mut incoming = Vec::new();
            while let Ok(item) = rx.try_recv() {
                incoming.push(item);
            }
            incoming
        };
        for item in &incoming {
            self.release_inflight(item.points);
        }

        let mut state = self.lock();
        let mut changed = false;
        if state.active {
            for item in incoming {
                changed |= sequence_batch(&mut state, item);
            }
            let missed = self.take_misses(state.generation);
            if missed > 0 {
                // Full-channel refusals have no slot. They follow every batch this pull sequenced.
                state.next_sequence = state.next_sequence.saturating_add(missed);
                changed = true;
            }
        } else {
            let _ = self.take_misses(state.generation);
        }
        let points_before = state.points;
        evict(&mut state, self.inner.max_points, self.inner.max_age);
        debug_assert_consistent(&state);
        changed || state.points != points_before
    }

    pub(crate) fn try_drain(&self, generation: u64, batch_limit: usize) -> Drain {
        self.pull_ingress();
        let mut state = self.lock();
        if state.generation != generation || !state.active {
            return Drain::Replaced;
        }
        pop_empty(&mut state);
        let unread_until = front_bound(&state);
        let gap = take_gap(&mut state.next_unread, unread_until);
        let mut chunks = Vec::new();
        let mut taken = 0usize;
        while taken < batch_limit {
            let Some(chunk) = take_contiguous(&mut state, batch_limit - taken) else {
                break;
            };
            taken += chunk.len;
            chunks.push(chunk);
        }
        let tail = if chunks.is_empty() {
            None
        } else {
            pop_empty(&mut state);
            let unread_until = front_bound(&state);
            take_gap(&mut state.next_unread, unread_until)
        };
        debug_assert_consistent(&state);
        if gap.is_none() && tail.is_none() && chunks.is_empty() {
            Drain::Idle
        } else {
            Drain::Ready { gap, tail, chunks }
        }
    }

    fn try_reserve(&self, points: usize) -> bool {
        let max = self.inner.max_inflight;
        let mut current = self.inner.inflight.load(Ordering::Acquire);
        loop {
            let Some(next) = admitted_inflight(current, points, max) else {
                return false;
            };
            match self.inner.inflight.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn release_inflight(&self, points: usize) {
        if points == 0 {
            return;
        }
        let mut current = self.inner.inflight.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(points);
            match self.inner.inflight.compare_exchange_weak(
                current,
                next,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    debug_assert!(current >= points, "inflight underflow");
                    return;
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn note_miss(&self, generation: u64, points: u64) {
        if points == 0 {
            return;
        }
        let mut missed = self.miss_lock();
        if missed.generation == generation {
            missed.count = missed.count.saturating_add(points);
        } else if missed.generation < generation {
            missed.generation = generation;
            missed.count = points;
        }
    }

    /// Take misses for `generation`. A newer generation's count stays. An older generation's
    /// count is dropped.
    fn take_misses(&self, generation: u64) -> u64 {
        let mut missed = self.miss_lock();
        if missed.count == 0 || missed.generation > generation {
            return 0;
        }
        let count = std::mem::take(&mut missed.count);
        if missed.generation == generation {
            count
        } else {
            0
        }
    }

    fn miss_lock(&self) -> MutexGuard<'_, Missed> {
        self.inner
            .missed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn rx_lock(&self) -> MutexGuard<'_, Receiver<Incoming>> {
        self.inner
            .rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

const fn inflight_budget(max_points: usize) -> usize {
    let scaled = max_points.saturating_mul(INFLIGHT_BUFFERS);
    if scaled == 0 { 1 } else { scaled }
}

/// Next inflight total, or `None` when this message does not fit.
///
/// An idle handoff admits one message even when it is larger than the budget. Evict then keeps
/// its newest points. A later message in the same batch still has to fit.
const fn admitted_inflight(current: usize, points: usize, max: usize) -> Option<usize> {
    let Some(next) = current.checked_add(points) else {
        return None;
    };
    if current > 0 && next > max {
        None
    } else {
        Some(next)
    }
}

fn sequence_batch(state: &mut State, item: Incoming) -> bool {
    if item.generation != state.generation {
        return false;
    }
    let mut changed = false;
    for message in item.messages {
        let points = message.readings.len();
        if points == 0 {
            continue;
        }
        let first_sequence = state.next_sequence;
        state.next_sequence = state.next_sequence.saturating_add(as_u64(points));
        state.points = state.points.saturating_add(points);
        state.slots.push_back(Queued {
            message,
            driver_id: Arc::clone(&item.driver_id),
            enqueued_at: item.enqueued_at,
            first_sequence,
            cursor: 0,
        });
        changed = true;
    }
    if item.missed > 0 {
        // Budget misses stay behind this batch, ahead of later channel items.
        state.next_sequence = state.next_sequence.saturating_add(item.missed);
        changed = true;
    }
    changed
}

/// Drop a prefix of the oldest points. `next_unread` stays put, so those sequences are the next
/// leading gap.
fn evict(state: &mut State, max_points: usize, max_age: Duration) {
    let now = Instant::now();
    loop {
        pop_empty(state);
        let Some(queued) = state.slots.front() else {
            break;
        };
        let remaining = queued.message.readings.len().saturating_sub(queued.cursor);
        let too_old = now.saturating_duration_since(queued.enqueued_at) >= max_age;
        let excess = state.points.saturating_sub(max_points);
        if !too_old && excess == 0 {
            break;
        }
        let drop_n = if too_old {
            remaining
        } else {
            excess.min(remaining)
        };
        if !drop_front_points(state, drop_n) {
            break;
        }
    }
}

fn drop_front_points(state: &mut State, n: usize) -> bool {
    if n == 0 {
        return false;
    }
    pop_empty(state);
    let Some(queued) = state.slots.front() else {
        return false;
    };
    let remaining = queued.message.readings.len().saturating_sub(queued.cursor);
    let take = n.min(remaining);
    if take == 0 {
        return false;
    }
    if take == remaining {
        state.slots.pop_front();
    } else {
        let Some(queued) = state.slots.front_mut() else {
            return false;
        };
        queued.cursor += take;
        queued.first_sequence = queued.first_sequence.saturating_add(as_u64(take));
    }
    debug_assert!(state.points >= take, "point cache underflow");
    state.points = state.points.saturating_sub(take);
    true
}

fn pop_empty(state: &mut State) {
    while state
        .slots
        .front()
        .is_some_and(|queued| queued.message.readings.len() <= queued.cursor)
    {
        state.slots.pop_front();
    }
}

fn front_bound(state: &State) -> u64 {
    state
        .slots
        .iter()
        .find(|queued| queued.message.readings.len() > queued.cursor)
        .map_or(state.next_sequence, |queued| queued.first_sequence)
}

const fn take_gap(next_unread: &mut u64, until: u64) -> Option<GapSpan> {
    if *next_unread >= until {
        return None;
    }
    let dropped = until.saturating_sub(*next_unread);
    if dropped == 0 {
        return None;
    }
    let gap = GapSpan {
        first_sequence: *next_unread,
        dropped,
    };
    *next_unread = until;
    Some(gap)
}

/// Take the next points only when they begin at `next_unread`. A hole stops the batch so it can
/// be returned behind these points.
fn take_contiguous(state: &mut State, n: usize) -> Option<Chunk> {
    if n == 0 {
        return None;
    }
    pop_empty(state);
    let front = state.slots.front()?;
    if front.first_sequence != state.next_unread {
        return None;
    }
    let chunk = take_front_points(state, n)?;
    state.next_unread = chunk.first_sequence.saturating_add(as_u64(chunk.len));
    Some(chunk)
}

/// Remove up to `n` points from the front point slot.
fn take_front_points(state: &mut State, n: usize) -> Option<Chunk> {
    if n == 0 {
        return None;
    }
    pop_empty(state);
    let queued = state.slots.front()?;
    let remaining = queued.message.readings.len().saturating_sub(queued.cursor);
    if remaining == 0 {
        return None;
    }
    let take = n.min(remaining);
    let message = Arc::clone(&queued.message);
    let driver_id = Arc::clone(&queued.driver_id);
    let first_sequence = queued.first_sequence;
    let start = queued.cursor;
    if take == remaining {
        state.slots.pop_front();
    } else {
        let queued = state.slots.front_mut()?;
        queued.cursor += take;
        queued.first_sequence = queued.first_sequence.saturating_add(as_u64(take));
    }
    debug_assert!(state.points >= take, "point cache underflow");
    state.points = state.points.saturating_sub(take);
    Some(Chunk {
        message,
        driver_id,
        first_sequence,
        start,
        len: take,
    })
}

struct Missed {
    generation: u64,
    count: u64,
}

fn as_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

fn new_stream_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{}-{nanos:x}", std::process::id())
}

#[cfg(not(debug_assertions))]
#[inline(always)]
fn debug_assert_consistent(_state: &State) {}

#[cfg(debug_assertions)]
#[inline]
fn debug_assert_consistent(state: &State) {
    if !cfg!(debug_assertions) {
        return;
    }
    debug_assert!(state.next_unread <= state.next_sequence);
    let mut covered_end = state.next_unread;
    let mut summed = 0usize;
    for queued in &state.slots {
        let remaining = queued.message.readings.len().saturating_sub(queued.cursor);
        if remaining == 0 {
            continue;
        }
        debug_assert!(
            queued.first_sequence >= covered_end,
            "point slot overlaps or precedes the unread cursor"
        );
        let end = queued.first_sequence.saturating_add(as_u64(remaining));
        debug_assert!(
            end <= state.next_sequence,
            "point slot past assigned sequences"
        );
        covered_end = end;
        summed = summed.saturating_add(remaining);
    }
    debug_assert_eq!(state.points, summed, "point cache drifted from slots");
}

#[cfg(test)]
mod tests;
