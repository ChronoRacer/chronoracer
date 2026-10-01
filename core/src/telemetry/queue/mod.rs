use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use crate::runtime_configuration::RetainReadTimeValue;
use arc_swap::ArcSwap;
use compact_str::CompactString;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

pub(crate) const MAX_POINTS_PER_BATCH: usize = 1024;

/// First sequence every `Watch` call assigns. Each call has its own sequence space.
pub(crate) const FIRST_SEQUENCE: u64 = 1;

/// One `offer` is one slot. The point budget admits data; a full channel is only the fallback
/// when the telemetry task is not draining.
const HANDOFF_SLOTS: usize = 256;

/// Unsequenced points may fill the retained cap twice: the batch still in the channel, plus a
/// replacement batch that eviction will keep.
const INFLIGHT_BUFFERS: usize = 2;

/// Points of one sensor that a batch could not hand off.
type Miss = (Arc<str>, u64);

/// Which sensors a subscriber receives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Filter {
    All,
    Sensors(HashSet<CompactString>),
}

impl Filter {
    /// Build a list filter. Duplicates collapse.
    pub(crate) fn sensors<I, S>(ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<CompactString>,
    {
        Self::Sensors(ids.into_iter().map(Into::into).collect())
    }

    fn wants(&self, sensor_id: &str) -> bool {
        match self {
            Self::All => true,
            Self::Sensors(set) => set.contains(sensor_id),
        }
    }

    /// Distinct selected sensors. Zero for `All`.
    pub(crate) fn selected_count(&self) -> usize {
        match self {
            Self::All => 0,
            Self::Sensors(set) => set.len(),
        }
    }
}

/// Union of every subscriber's filter. `offer` skips messages nobody wants.
enum Interest {
    Nobody,
    Watching(Filter),
}

impl Interest {
    fn union(subscribers: &[Arc<Subscriber>]) -> Self {
        if subscribers.is_empty() {
            return Self::Nobody;
        }
        let mut union = HashSet::new();
        for subscriber in subscribers {
            match &subscriber.filter {
                Filter::All => return Self::Watching(Filter::All),
                Filter::Sensors(set) => union.extend(set.iter().cloned()),
            }
        }
        Self::Watching(Filter::Sensors(union))
    }
}

struct Incoming {
    messages: Vec<Arc<UnwoundSensorDataMessage>>,
    driver_id: Arc<CompactString>,
    enqueued_at: Instant,
    /// Epoch read before the interest, so a subscriber joining later can tell whether this batch
    /// was already filtered against its sensors.
    epoch: u64,
    /// Points reserved in `inflight` for `messages`.
    points: usize,
    /// Points from this batch that did not fit the point budget, by sensor.
    missed: Vec<Miss>,
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

impl State {
    const fn new() -> Self {
        Self {
            next_sequence: FIRST_SEQUENCE,
            next_unread: FIRST_SEQUENCE,
            points: 0,
            slots: VecDeque::new(),
        }
    }
}

/// One `Watch` call: its filter, its own queue, its own sequence space.
///
/// Queued points are `Arc` references to messages shared with every other subscriber and with
/// persistence. A subscriber costs one slot per message, not a copy of the points.
pub(crate) struct Subscriber {
    client_id: String,
    filter: Filter,
    /// Epoch this subscriber joined at. Handoff batches and misses from earlier epochs were
    /// filtered without its sensors and are not its data.
    join_epoch: u64,
    replaced: AtomicBool,
    notify: Notify,
    state: Mutex<State>,
}

impl Subscriber {
    pub(crate) fn notify(&self) -> &Notify {
        &self.notify
    }

    pub(crate) fn client_id(&self) -> &str {
        &self.client_id
    }

    pub(crate) fn selected_sensor_count(&self) -> usize {
        self.filter.selected_count()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Mark replaced, drop the unsent queue, and wake the stream so it ends.
    fn retire(&self) {
        self.replaced.store(true, Ordering::Release);
        {
            let mut state = self.lock();
            state.points = 0;
            state.slots.clear();
        }
        self.notify.notify_waiters();
    }

    /// Sequence this subscriber's share of a pull. Returns whether its queue changed.
    fn ingest(
        &self,
        incoming: &[Incoming],
        misses: &[(u64, Arc<str>, u64)],
        max_points: usize,
        max_age: Duration,
    ) -> bool {
        let mut state = self.lock();
        let points_before = state.points;
        let mut changed = false;
        for item in incoming {
            if item.epoch >= self.join_epoch {
                changed |= sequence_batch(&mut state, item, &self.filter);
            }
        }
        // Full-channel refusals have no slot. They follow every batch this pull sequenced.
        let refused = misses
            .iter()
            .filter(|(epoch, sensor, _)| *epoch >= self.join_epoch && self.filter.wants(sensor))
            .fold(0u64, |total, (_, _, count)| total.saturating_add(*count));
        if refused > 0 {
            state.next_sequence = state.next_sequence.saturating_add(refused);
            changed = true;
        }
        evict(&mut state, max_points, max_age);
        debug_assert_consistent(&state);
        changed || state.points != points_before
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscribeError {
    /// Every subscriber slot is held by another `client_id`.
    Full,
}

/// Sync handoff from the parse threads into per-subscriber serving queues.
///
/// `offer` never locks a serving queue and never waits. With no subscriber it returns after
/// reading the interest. Sequencing, filtering, age, and the point cap run in `pull_ingress`, on
/// the telemetry task.
#[derive(Clone)]
pub struct PreviewHub {
    inner: Arc<Inner>,
}

struct Inner {
    driver: &'static RetainReadTimeValue<CompactString>,
    max_points: usize,
    /// Admit budget for batches not yet sequenced. Above this, `offer` records a miss.
    max_inflight: usize,
    max_age: Duration,
    max_subscribers: usize,
    /// Bumped when a subscriber joins. Stored after `interest`, read before it.
    epoch: AtomicU64,
    /// Union of subscriber filters, read by `offer` without a lock.
    interest: ArcSwap<Interest>,
    inflight: AtomicUsize,
    /// Drops that did not fit in a handoff slot, by `(epoch, sensor)`. The success path of
    /// `offer` does not lock this.
    missed: Mutex<HashMap<(u64, Arc<str>), u64>>,
    /// Wakes the pump when `offer` hands over a batch.
    notify: Arc<Notify>,
    tx: SyncSender<Incoming>,
    rx: Mutex<Receiver<Incoming>>,
    /// Lock order: `rx`, then `registry`, then a subscriber's state.
    registry: Mutex<Vec<Arc<Subscriber>>>,
    stream_id: String,
}

impl PreviewHub {
    #[must_use]
    pub fn new(
        driver: &'static RetainReadTimeValue<CompactString>,
        max_points: usize,
        max_age: Duration,
        max_subscribers: usize,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(HANDOFF_SLOTS);
        Self {
            inner: Arc::new(Inner {
                driver,
                max_points,
                max_inflight: inflight_budget(max_points),
                max_age,
                max_subscribers,
                epoch: AtomicU64::new(0),
                interest: ArcSwap::from_pointee(Interest::Nobody),
                inflight: AtomicUsize::new(0),
                missed: Mutex::new(HashMap::new()),
                notify: Arc::new(Notify::new()),
                tx,
                rx: Mutex::new(rx),
                registry: Mutex::new(Vec::new()),
                stream_id: new_stream_id(),
            }),
        }
    }

    #[must_use]
    pub fn stream_id(&self) -> &str {
        &self.inner.stream_id
    }

    /// Wakes the pump. Subscribers wait on their own notify.
    #[must_use]
    pub fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.notify)
    }

    #[must_use]
    pub(crate) fn max_age(&self) -> Duration {
        self.inner.max_age
    }

    /// Points sequenced and not yet drained, summed over subscribers. Test and diagnostics only.
    #[must_use]
    pub fn queued_points(&self) -> usize {
        self.registry_lock()
            .iter()
            .map(|subscriber| subscriber.lock().points)
            .sum()
    }

    /// Hand the batch to the telemetry task.
    ///
    /// One `try_send` for the whole batch. Returns as soon as that send accepts or refuses.
    /// Does not lock a serving queue. Messages no subscriber wants are skipped before they
    /// take budget. A point-budget miss travels with the batch. A full channel adds the same
    /// points to the miss cell. Either becomes a gap on the next `pull_ingress`.
    pub fn offer(&self, batch: &[Arc<UnwoundSensorDataMessage>]) {
        // Epoch first: a batch tagged with an epoch was filtered by an interest at least that new.
        let epoch = self.inner.epoch.load(Ordering::Acquire);
        let interest = self.inner.interest.load();
        let Interest::Watching(union) = &**interest else {
            return;
        };
        let driver_id = self.inner.driver.get();
        let enqueued_at = Instant::now();
        let mut messages = Vec::new();
        let mut parked = 0usize;
        let mut missed: Vec<Miss> = Vec::new();
        for message in batch {
            let points = message.readings.len();
            if points == 0 || !union.wants(&message.sensor_id) {
                continue;
            }
            if self.try_reserve(points) {
                if messages.is_empty() {
                    messages.reserve(batch.len());
                }
                parked += points;
                messages.push(Arc::clone(message));
            } else {
                missed.push((Arc::clone(&message.sensor_id), as_u64(points)));
            }
        }
        if messages.is_empty() && missed.is_empty() {
            return;
        }
        let incoming = Incoming {
            messages,
            driver_id,
            enqueued_at,
            epoch,
            points: parked,
            missed,
        };
        if let Err(TrySendError::Full(incoming) | TrySendError::Disconnected(incoming)) =
            self.inner.tx.try_send(incoming)
        {
            self.release_inflight(incoming.points);
            let refused = incoming
                .messages
                .iter()
                .map(|message| (Arc::clone(&message.sensor_id), as_u64(message.readings.len())))
                .chain(incoming.missed);
            self.note_misses(epoch, refused);
        }
        self.inner.notify.notify_waiters();
    }

    /// Start a `Watch`. A `client_id` already subscribed is replaced and its stream ends. A new
    /// `client_id` needs a free slot. The returned subscriber sees only batches offered after
    /// this call.
    pub(crate) fn subscribe(
        &self,
        client_id: &str,
        filter: Filter,
    ) -> Result<Arc<Subscriber>, SubscribeError> {
        let mut registry = self.registry_lock();
        if let Some(position) = registry
            .iter()
            .position(|subscriber| subscriber.client_id == client_id)
        {
            registry.remove(position).retire();
        } else if registry.len() >= self.inner.max_subscribers {
            return Err(SubscribeError::Full);
        }
        let join_epoch = self.inner.epoch.load(Ordering::Acquire).wrapping_add(1).max(1);
        let subscriber = Arc::new(Subscriber {
            client_id: client_id.to_owned(),
            filter,
            join_epoch,
            replaced: AtomicBool::new(false),
            notify: Notify::new(),
            state: Mutex::new(State::new()),
        });
        registry.push(Arc::clone(&subscriber));
        // Interest before epoch: an offer that reads the new epoch also reads this interest.
        self.inner
            .interest
            .store(Arc::new(Interest::union(&registry)));
        self.inner.epoch.store(join_epoch, Ordering::Release);
        drop(registry);
        self.inner.notify.notify_waiters();
        Ok(subscriber)
    }

    /// End a `Watch`. A subscriber that was already replaced is not in the registry, so a late
    /// call from the replaced stream leaves the replacement alone.
    pub(crate) fn unsubscribe(&self, subscriber: &Arc<Subscriber>) {
        {
            let mut registry = self.registry_lock();
            let Some(position) = registry
                .iter()
                .position(|candidate| Arc::ptr_eq(candidate, subscriber))
            else {
                return;
            };
            registry.remove(position);
            self.inner
                .interest
                .store(Arc::new(Interest::union(&registry)));
        }
        let mut state = subscriber.lock();
        state.points = 0;
        state.slots.clear();
    }

    /// Sequence handoff batches into each subscriber and drop its oldest points that are past
    /// age or the point cap.
    ///
    /// Pump and every `Watch` call this. One caller drains the handoff and distributes it, so
    /// subscribers see batches in offer order. A caller that finds the handoff busy returns
    /// `false`: the busy one notifies whoever changed. `offer` never calls it. Returns whether
    /// any subscriber changed.
    pub(crate) fn pull_ingress(&self) -> bool {
        let Some(rx) = self.try_rx_lock() else {
            return false;
        };
        let mut incoming = Vec::new();
        while let Ok(item) = rx.try_recv() {
            incoming.push(item);
        }
        for item in &incoming {
            self.release_inflight(item.points);
        }
        let misses = self.take_misses();
        let registry = self.registry_lock();
        let mut any = false;
        for subscriber in registry.iter() {
            if subscriber.ingest(&incoming, &misses, self.inner.max_points, self.inner.max_age) {
                subscriber.notify.notify_waiters();
                any = true;
            }
        }
        drop(registry);
        drop(rx);
        any
    }

    pub(crate) fn try_drain(&self, subscriber: &Subscriber, batch_limit: usize) -> Drain {
        self.pull_ingress();
        if subscriber.replaced.load(Ordering::Acquire) {
            return Drain::Replaced;
        }
        let mut state = subscriber.lock();
        evict(&mut state, self.inner.max_points, self.inner.max_age);
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
        // Underflow is a bug (a release without a matching reserve), not a runtime case.
        let previous = self.inner.inflight.fetch_sub(points, Ordering::Release);
        debug_assert!(previous >= points, "inflight underflow");
    }

    /// One lock per refused batch. The map holds one entry per `(epoch, sensor)` until the next
    /// pull takes it.
    fn note_misses(&self, epoch: u64, refused: impl Iterator<Item = Miss>) {
        let mut missed = self.miss_lock();
        for (sensor, points) in refused {
            if points == 0 {
                continue;
            }
            let count = missed.entry((epoch, sensor)).or_insert(0);
            *count = count.saturating_add(points);
        }
    }

    fn take_misses(&self) -> Vec<(u64, Arc<str>, u64)> {
        let mut missed = self.miss_lock();
        if missed.is_empty() {
            return Vec::new();
        }
        missed
            .drain()
            .map(|((epoch, sensor), count)| (epoch, sensor, count))
            .collect()
    }

    fn miss_lock(&self) -> MutexGuard<'_, HashMap<(u64, Arc<str>), u64>> {
        self.inner
            .missed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn registry_lock(&self) -> MutexGuard<'_, Vec<Arc<Subscriber>>> {
        self.inner
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn try_rx_lock(&self) -> Option<MutexGuard<'_, Receiver<Incoming>>> {
        match self.inner.rx.try_lock() {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
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

fn sequence_batch(state: &mut State, item: &Incoming, filter: &Filter) -> bool {
    let mut changed = false;
    for message in &item.messages {
        let points = message.readings.len();
        if points == 0 || !filter.wants(&message.sensor_id) {
            continue;
        }
        let first_sequence = state.next_sequence;
        state.next_sequence = state.next_sequence.saturating_add(as_u64(points));
        state.points = state.points.saturating_add(points);
        state.slots.push_back(Queued {
            message: Arc::clone(message),
            driver_id: Arc::clone(&item.driver_id),
            enqueued_at: item.enqueued_at,
            first_sequence,
            cursor: 0,
        });
        changed = true;
    }
    // Budget misses stay behind this batch, ahead of later channel items.
    let missed = item
        .missed
        .iter()
        .filter(|(sensor, _)| filter.wants(sensor))
        .fold(0u64, |total, (_, count)| total.saturating_add(*count));
    if missed > 0 {
        state.next_sequence = state.next_sequence.saturating_add(missed);
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
