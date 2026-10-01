use super::*;
use crate::processing::sensor_data_message::AbsoluteSensorReading;
use std::thread;
use std::time::Duration;

fn hub(max_points: usize, max_age: Duration) -> PreviewHub {
    let driver = Box::leak(Box::new(RetainReadTimeValue::new(CompactString::from(
        "driver",
    ))));
    PreviewHub::new(driver, max_points, max_age, 4)
}

fn join(preview: &PreviewHub) -> Arc<Subscriber> {
    preview.subscribe("client", Filter::All).unwrap()
}

fn join_as(preview: &PreviewHub, client_id: &str, sensors: &[&str]) -> Arc<Subscriber> {
    let filter = if sensors.is_empty() {
        Filter::All
    } else {
        Filter::sensors(sensors.iter().map(|sensor| (*sensor).to_owned()))
    };
    preview.subscribe(client_id, filter).unwrap()
}

/// Everything a subscriber can read now: `(first_sequence, sensor_id, len)` per chunk, plus gaps.
struct Drained {
    gap: Option<GapSpan>,
    tail: Option<GapSpan>,
    chunks: Vec<(u64, String, usize)>,
}

fn drained(preview: &PreviewHub, sub: &Subscriber) -> Drained {
    match preview.try_drain(sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready { gap, tail, chunks } => Drained {
            gap,
            tail,
            chunks: chunks
                .iter()
                .map(|chunk| {
                    (
                        chunk.first_sequence,
                        chunk.message.sensor_id.to_string(),
                        chunk.len,
                    )
                })
                .collect(),
        },
        Drain::Idle => Drained {
            gap: None,
            tail: None,
            chunks: Vec::new(),
        },
        Drain::Replaced => panic!("subscriber was replaced"),
    }
}

fn message(sensor_id: &str, points: u64) -> Arc<UnwoundSensorDataMessage> {
    let readings = (0..points)
        .map(|index| AbsoluteSensorReading {
            value: 0.0,
            timestamp: index,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Arc::new(UnwoundSensorDataMessage {
        sensor_id: Arc::from(sensor_id),
        readings,
        source: Default::default(),
    })
}

#[test]
fn offer_with_no_caller_does_not_queue() {
    let preview = hub(100, Duration::from_secs(10));
    preview.offer(&[message("speed", 3)]);
    assert_eq!(preview.queued_points(), 0);
    let sub = join(&preview);
    assert!(matches!(
        preview.try_drain(&sub, MAX_POINTS_PER_BATCH),
        Drain::Idle
    ));
}

#[test]
fn drain_keeps_offer_order() {
    let preview = hub(100, Duration::from_secs(10));
    let sub = join(&preview);
    preview.offer(&[message("a", 1), message("b", 2)]);
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: None,
            tail: None,
            chunks,
        } => {
            assert_eq!(chunks.len(), 2);
            assert_eq!(chunks[0].first_sequence, 1);
            assert_eq!(chunks[0].start, 0);
            assert_eq!(chunks[0].len, 1);
            assert_eq!(chunks[0].message.sensor_id.as_ref(), "a");
            assert_eq!(chunks[1].first_sequence, 2);
            assert_eq!(chunks[1].start, 0);
            assert_eq!(chunks[1].len, 2);
            assert_eq!(chunks[1].message.sensor_id.as_ref(), "b");
            assert_eq!(chunks[0].driver_id.as_str(), "driver");
        }
        other => panic!("expected both messages, got {}", drain_name(&other)),
    }
}

#[test]
fn point_cap_drops_oldest_points_and_keeps_a_partial_message() {
    let preview = hub(3, Duration::from_secs(10));
    let sub = join(&preview);
    preview.offer(&[message("old", 2), message("mid", 2), message("new", 2)]);
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: Some(gap),
            tail: None,
            chunks,
        } => {
            assert_eq!(gap.first_sequence, 1);
            assert_eq!(gap.dropped, 3);
            assert_eq!(chunks.len(), 2);
            assert_eq!(chunks[0].message.sensor_id.as_ref(), "mid");
            assert_eq!(chunks[0].start, 1);
            assert_eq!(chunks[0].len, 1);
            assert_eq!(chunks[0].first_sequence, 4);
            assert_eq!(chunks[1].message.sensor_id.as_ref(), "new");
            assert_eq!(chunks[1].start, 0);
            assert_eq!(chunks[1].len, 2);
            assert_eq!(chunks[1].first_sequence, 5);
        }
        other => panic!(
            "expected a gap and the newest points, got {}",
            drain_name(&other)
        ),
    }
    assert_eq!(preview.queued_points(), 0);
}

#[test]
fn point_cap_keeps_the_newest_tail_of_one_message() {
    let preview = hub(3, Duration::from_secs(10));
    let sub = join(&preview);
    preview.offer(&[message("wide", 5)]);
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: Some(gap),
            tail: None,
            chunks,
        } => {
            assert_eq!(gap.first_sequence, 1);
            assert_eq!(gap.dropped, 2);
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].start, 2);
            assert_eq!(chunks[0].len, 3);
            assert_eq!(chunks[0].first_sequence, 3);
        }
        other => panic!(
            "expected the newest tail after a gap, got {}",
            drain_name(&other)
        ),
    }
}

#[test]
fn drain_splits_a_message_at_the_batch_cap_and_leaves_the_tail_queued() {
    let preview = hub(10_000, Duration::from_secs(10));
    let sub = join(&preview);
    preview.offer(&[message("wide", 2_000)]);
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: None,
            tail: None,
            chunks,
        } => {
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].start, 0);
            assert_eq!(chunks[0].len, MAX_POINTS_PER_BATCH);
            assert_eq!(chunks[0].first_sequence, 1);
        }
        other => panic!("expected the first batch, got {}", drain_name(&other)),
    }
    assert_eq!(preview.queued_points(), 2_000 - MAX_POINTS_PER_BATCH);
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: None,
            tail: None,
            chunks,
        } => {
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].start, MAX_POINTS_PER_BATCH);
            assert_eq!(chunks[0].len, 2_000 - MAX_POINTS_PER_BATCH);
            assert_eq!(chunks[0].first_sequence, as_u64(MAX_POINTS_PER_BATCH) + 1);
        }
        other => panic!("expected the tail batch, got {}", drain_name(&other)),
    }
    assert_eq!(preview.queued_points(), 0);
}

#[test]
fn age_cap_on_drain_drops_stale_points_without_a_new_offer() {
    let preview = hub(100, Duration::from_millis(20));
    let sub = join(&preview);
    preview.offer(&[message("stale", 2)]);
    assert!(preview.pull_ingress());
    assert_eq!(preview.queued_points(), 2);
    thread::sleep(Duration::from_millis(40));
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: Some(gap),
            tail: None,
            chunks,
        } => {
            assert_eq!(gap.first_sequence, 1);
            assert_eq!(gap.dropped, 2);
            assert!(chunks.is_empty());
        }
        other => panic!("expected a gap and no points, got {}", drain_name(&other)),
    }
    assert_eq!(preview.queued_points(), 0);
}

#[test]
fn age_cap_drops_only_the_stale_prefix() {
    let preview = hub(100, Duration::from_millis(20));
    let sub = join(&preview);
    preview.offer(&[message("stale", 1)]);
    thread::sleep(Duration::from_millis(40));
    preview.offer(&[message("fresh", 1)]);
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: Some(gap),
            tail: None,
            chunks,
        } => {
            assert_eq!(gap.first_sequence, 1);
            assert_eq!(gap.dropped, 1);
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].first_sequence, 2);
            assert_eq!(chunks[0].message.sensor_id.as_ref(), "fresh");
        }
        other => panic!(
            "expected the fresh point after a gap, got {}",
            drain_name(&other)
        ),
    }
}

#[test]
fn a_new_stream_with_the_same_client_id_discards_the_previous_queue() {
    let preview = hub(100, Duration::from_secs(10));
    let first = join(&preview);
    preview.offer(&[message("gone", 4)]);
    assert!(preview.pull_ingress());
    let second = join(&preview);
    assert!(matches!(preview.try_drain(&first, 10), Drain::Replaced));
    assert!(matches!(preview.try_drain(&second, 10), Drain::Idle));
    assert_eq!(preview.queued_points(), 0);
}

#[test]
fn offer_over_the_handoff_budget_records_a_tail_gap_without_queueing_it() {
    let preview = hub(4, Duration::from_secs(30));
    let sub = join(&preview);
    preview.offer(&[message("first", 4)]);
    preview.offer(&[message("second", 4)]);
    preview.offer(&[message("tail", 4)]);
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: Some(gap),
            tail: Some(tail),
            chunks,
        } => {
            assert_eq!(gap.first_sequence, 1);
            assert_eq!(gap.dropped, 4);
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].message.sensor_id.as_ref(), "second");
            assert_eq!(chunks[0].first_sequence, 5);
            assert_eq!(chunks[0].len, 4);
            assert_eq!(tail.first_sequence, 9);
            assert_eq!(tail.dropped, 4);
        }
        other => panic!(
            "expected a prefix gap, the second batch, and a tail gap, got {}",
            drain_name(&other)
        ),
    }
}

#[test]
fn tail_gap_is_emitted_before_points_sequenced_after_it() {
    let preview = hub(4, Duration::from_secs(30));
    let sub = join(&preview);
    preview.offer(&[message("first", 4)]);
    preview.offer(&[message("second", 4)]);
    preview.offer(&[message("missed", 4)]);
    assert!(preview.pull_ingress());
    preview.offer(&[message("later", 2)]);
    let tail_end = match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            tail: Some(tail),
            chunks,
            ..
        } => {
            assert!(
                chunks
                    .iter()
                    .all(|chunk| chunk.message.sensor_id.as_ref() != "later")
            );
            let end = chunks.last().map_or(tail.first_sequence, |chunk| {
                chunk.first_sequence.saturating_add(as_u64(chunk.len))
            });
            assert_eq!(tail.first_sequence, end);
            tail.first_sequence.saturating_add(tail.dropped)
        }
        other => panic!(
            "expected the gap before later points, got {}",
            drain_name(&other)
        ),
    };
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: None,
            tail: None,
            chunks,
        } => {
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].first_sequence, tail_end);
            assert_eq!(chunks[0].len, 2);
            assert_eq!(chunks[0].message.sensor_id.as_ref(), "later");
        }
        other => panic!("expected the later points, got {}", drain_name(&other)),
    }
}

#[test]
fn one_offer_queues_a_batch_bigger_than_the_handoff_slot_count() {
    let preview = hub(10_000, Duration::from_secs(10));
    let sub = join(&preview);
    let count = HANDOFF_SLOTS + 44;
    let batch: Vec<_> = (0..count)
        .map(|index| message(&index.to_string(), 1))
        .collect();
    preview.offer(&batch);
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: None,
            tail: None,
            chunks,
        } => {
            assert_eq!(chunks.len(), count);
            assert_eq!(chunks[0].first_sequence, 1);
            assert_eq!(chunks.last().expect("batch").first_sequence, as_u64(count));
        }
        other => panic!(
            "expected every point in the batch, got {}",
            drain_name(&other)
        ),
    }
}

#[test]
fn second_miss_wave_keeps_gap_ranges_disjoint() {
    let preview = hub(12, Duration::from_secs(30));
    let sub = join(&preview);
    preview.offer(&[message("first", 4)]);
    preview.offer(&[message("missed", 24)]);
    assert!(preview.pull_ingress());
    preview.offer(&[message("later", 4)]);
    preview.offer(&[message("missed-again", 24)]);
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: None,
            tail: Some(tail),
            chunks,
        } => {
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].first_sequence, 1);
            assert_eq!(chunks[0].len, 4);
            assert_eq!(chunks[0].message.sensor_id.as_ref(), "first");
            assert_eq!(tail.first_sequence, 5);
            assert_eq!(tail.dropped, 24);
        }
        other => panic!("expected the first wave, got {}", drain_name(&other)),
    }
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: None,
            tail: Some(tail),
            chunks,
        } => {
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].first_sequence, 29);
            assert_eq!(chunks[0].len, 4);
            assert_eq!(chunks[0].message.sensor_id.as_ref(), "later");
            assert_eq!(tail.first_sequence, 33);
            assert_eq!(tail.dropped, 24);
        }
        other => panic!("expected the second wave, got {}", drain_name(&other)),
    }
    assert_eq!(preview.queued_points(), 0);
}

#[test]
fn concurrent_full_handoff_misses_are_all_counted() {
    let preview = hub(10_000, Duration::from_secs(30));
    let sub = join(&preview);
    for _ in 0..HANDOFF_SLOTS {
        preview.offer(&[message("pad", 1)]);
    }
    let mut handles = Vec::new();
    for _ in 0..8 {
        let preview = preview.clone();
        handles.push(thread::spawn(move || {
            preview.offer(&[message("drop", 2)]);
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: None,
            tail: Some(tail),
            chunks,
        } => {
            assert_eq!(chunks.len(), HANDOFF_SLOTS);
            assert!(chunks.iter().all(|chunk| chunk.len == 1));
            assert_eq!(tail.first_sequence, as_u64(HANDOFF_SLOTS) + 1);
            assert_eq!(tail.dropped, 16);
        }
        other => panic!(
            "expected the queued pads and every refused point, got {}",
            drain_name(&other)
        ),
    }
    assert_eq!(preview.queued_points(), 0);
}

#[test]
fn oversized_message_on_an_empty_handoff_keeps_its_newest_points() {
    let preview = hub(3, Duration::from_secs(10));
    let sub = join(&preview);
    preview.offer(&[message("wide", 10)]);
    match preview.try_drain(&sub, MAX_POINTS_PER_BATCH) {
        Drain::Ready {
            gap: Some(gap),
            tail: None,
            chunks,
        } => {
            assert_eq!(gap.first_sequence, 1);
            assert_eq!(gap.dropped, 7);
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].start, 7);
            assert_eq!(chunks[0].len, 3);
            assert_eq!(chunks[0].first_sequence, 8);
        }
        other => panic!(
            "expected the newest tail of the oversized message, got {}",
            drain_name(&other)
        ),
    }
}

#[test]
fn offer_returns_while_drain_holds_the_queue() {
    let preview = hub(10_000, Duration::from_secs(10));
    let sub = join(&preview);
    let stop = Arc::new(AtomicBool::new(false));
    let stop_drain = Arc::clone(&stop);
    let draining = preview.clone();
    let draining_sub = Arc::clone(&sub);
    let drainer = thread::spawn(move || {
        while !stop_drain.load(Ordering::Relaxed) {
            let _ = draining.try_drain(&draining_sub, 1);
        }
    });
    let started = Instant::now();
    for index in 0..2_000 {
        preview.offer(&[message(&index.to_string(), 4)]);
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "offer waited on drain for {:?}",
        started.elapsed()
    );
    stop.store(true, Ordering::Relaxed);
    drainer.join().unwrap();
}

fn inflight(preview: &PreviewHub) -> usize {
    preview.inner.inflight.load(Ordering::Acquire)
}

#[test]
fn subscribers_share_the_same_message_allocation() {
    let preview = hub(100, Duration::from_secs(10));
    let first = join_as(&preview, "first", &[]);
    let second = preview.subscribe("second", Filter::All).unwrap();
    let offered = message("speed", 3);
    preview.offer(std::slice::from_ref(&offered));
    let mut seen = Vec::new();
    for sub in [&first, &second] {
        let Drain::Ready { chunks, .. } = preview.try_drain(sub, MAX_POINTS_PER_BATCH) else {
            panic!("expected points");
        };
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].first_sequence, 1);
        seen.push(Arc::clone(&chunks[0].message));
    }
    assert!(Arc::ptr_eq(&seen[0], &offered));
    assert!(Arc::ptr_eq(&seen[1], &offered));
}

#[test]
fn a_slow_subscriber_gaps_alone() {
    let preview = hub(4, Duration::from_secs(30));
    let fast = join_as(&preview, "fast", &[]);
    let slow = preview.subscribe("slow", Filter::All).unwrap();
    preview.offer(&[message("a", 4)]);
    let read = drained(&preview, &fast);
    assert_eq!(read.chunks, vec![(1, "a".to_owned(), 4)]);
    preview.offer(&[message("b", 4)]);
    let read = drained(&preview, &fast);
    assert!(read.gap.is_none());
    assert_eq!(read.chunks, vec![(5, "b".to_owned(), 4)]);
    let read = drained(&preview, &slow);
    let gap = read.gap.expect("slow subscriber lost the first batch");
    assert_eq!((gap.first_sequence, gap.dropped), (1, 4));
    assert_eq!(read.chunks, vec![(5, "b".to_owned(), 4)]);
}

#[test]
fn filters_select_sensors_and_number_only_their_points() {
    let preview = hub(100, Duration::from_secs(10));
    let only_b = join_as(&preview, "only-b", &["b"]);
    let everything = preview.subscribe("all", Filter::All).unwrap();
    preview.offer(&[message("a", 1), message("b", 2), message("c", 1)]);
    let read = drained(&preview, &only_b);
    assert_eq!(read.chunks, vec![(1, "b".to_owned(), 2)]);
    let read = drained(&preview, &everything);
    assert_eq!(
        read.chunks,
        vec![
            (1, "a".to_owned(), 1),
            (2, "b".to_owned(), 2),
            (4, "c".to_owned(), 1)
        ]
    );
}

#[test]
fn messages_nobody_wants_take_no_handoff_budget() {
    let preview = hub(4, Duration::from_secs(30));
    let sub = join_as(&preview, "client", &["wanted"]);
    preview.offer(&[message("ignored", 1_000), message("ignored", 1_000)]);
    assert_eq!(inflight(&preview), 0);
    let read = drained(&preview, &sub);
    assert!(read.gap.is_none() && read.tail.is_none() && read.chunks.is_empty());
    preview.offer(&[message("wanted", 4)]);
    let read = drained(&preview, &sub);
    assert!(read.gap.is_none() && read.tail.is_none());
    assert_eq!(read.chunks, vec![(1, "wanted".to_owned(), 4)]);
}

#[test]
fn unsubscribing_narrows_the_union_and_then_stops_the_copy() {
    let preview = hub(100, Duration::from_secs(10));
    let a = join_as(&preview, "a", &["a"]);
    let b = join_as(&preview, "b", &["b"]);
    preview.unsubscribe(&b);
    preview.offer(&[message("b", 2)]);
    assert_eq!(inflight(&preview), 0);
    preview.unsubscribe(&a);
    preview.offer(&[message("a", 2)]);
    assert_eq!(inflight(&preview), 0);
    assert_eq!(preview.queued_points(), 0);
}

#[test]
fn budget_misses_are_split_by_each_subscribers_sensors() {
    let preview = hub(4, Duration::from_secs(30));
    let a = join_as(&preview, "a", &["a"]);
    let b = join_as(&preview, "b", &["b"]);
    // Budget is 8 points: the last two messages are refused.
    preview.offer(&[message("a", 4), message("b", 4), message("a", 4), message("b", 2)]);
    let read = drained(&preview, &a);
    assert_eq!(read.chunks, vec![(1, "a".to_owned(), 4)]);
    let tail = read.tail.expect("a lost its refused points");
    assert_eq!((tail.first_sequence, tail.dropped), (5, 4));
    let read = drained(&preview, &b);
    assert_eq!(read.chunks, vec![(1, "b".to_owned(), 4)]);
    let tail = read.tail.expect("b lost its refused points");
    assert_eq!((tail.first_sequence, tail.dropped), (5, 2));
}

#[test]
fn full_handoff_misses_are_split_by_each_subscribers_sensors() {
    let preview = hub(10_000, Duration::from_secs(30));
    let a = join_as(&preview, "a", &["a"]);
    let b = join_as(&preview, "b", &["b"]);
    for _ in 0..HANDOFF_SLOTS {
        preview.offer(&[message("a", 1)]);
    }
    for _ in 0..3 {
        preview.offer(&[message("a", 2)]);
    }
    for _ in 0..2 {
        preview.offer(&[message("b", 3)]);
    }
    let read = drained(&preview, &a);
    assert_eq!(read.chunks.len(), HANDOFF_SLOTS);
    let tail = read.tail.expect("a lost refused points");
    assert_eq!((tail.first_sequence, tail.dropped), (as_u64(HANDOFF_SLOTS) + 1, 6));
    let read = drained(&preview, &b);
    assert!(read.chunks.is_empty());
    let gap = read.gap.expect("b lost refused points");
    assert_eq!((gap.first_sequence, gap.dropped), (1, 6));
}

#[test]
fn a_joining_subscriber_does_not_see_earlier_batches_or_misses() {
    let preview = hub(4, Duration::from_secs(30));
    let early = join_as(&preview, "early", &[]);
    // Budget is 8 points: the third message is refused before `late` exists.
    preview.offer(&[message("m", 4), message("m", 4), message("m", 4)]);
    let late = preview.subscribe("late", Filter::All).unwrap();
    let read = drained(&preview, &late);
    assert!(read.gap.is_none() && read.tail.is_none() && read.chunks.is_empty());
    let read = drained(&preview, &early);
    let gap = read.gap.expect("early lost its oldest batch to the cap");
    assert_eq!((gap.first_sequence, gap.dropped), (1, 4));
    assert_eq!(read.chunks, vec![(5, "m".to_owned(), 4)]);
    let tail = read.tail.expect("early lost the refused batch");
    assert_eq!((tail.first_sequence, tail.dropped), (9, 4));
    preview.offer(&[message("n", 1)]);
    let read = drained(&preview, &late);
    assert_eq!(read.chunks, vec![(1, "n".to_owned(), 1)]);
    assert!(read.gap.is_none() && read.tail.is_none());
}

#[test]
fn same_client_id_replaces_and_distinct_ids_coexist() {
    let preview = hub(100, Duration::from_secs(10));
    let old = join_as(&preview, "telemetry", &[]);
    let other = join_as(&preview, "analysis", &[]);
    let new = preview.subscribe("telemetry", Filter::All).unwrap();
    assert!(matches!(preview.try_drain(&old, 10), Drain::Replaced));
    // The replaced stream ending late must not remove its replacement.
    preview.unsubscribe(&old);
    preview.offer(&[message("x", 1)]);
    assert_eq!(drained(&preview, &new).chunks.len(), 1);
    assert_eq!(drained(&preview, &other).chunks.len(), 1);
}

#[test]
fn subscribers_beyond_the_cap_are_refused_without_evicting_anyone() {
    let preview = hub(100, Duration::from_secs(10));
    let held: Vec<_> = (0..4)
        .map(|index| preview.subscribe(&format!("client-{index}"), Filter::All).unwrap())
        .collect();
    assert_eq!(
        preview.subscribe("extra", Filter::All).err(),
        Some(SubscribeError::Full)
    );
    // Replacing a held id still works at the cap.
    assert!(preview.subscribe("client-0", Filter::All).is_ok());
    preview.unsubscribe(&held[1]);
    assert!(preview.subscribe("extra", Filter::All).is_ok());
    preview.offer(&[message("x", 1)]);
    assert_eq!(drained(&preview, &held[2]).chunks.len(), 1);
}

fn drain_name(drain: &Drain) -> &'static str {
    match drain {
        Drain::Idle => "idle",
        Drain::Replaced => "replaced",
        Drain::Ready {
            gap: Some(_),
            tail: Some(_),
            ..
        } => "ready-with-gap-and-tail",
        Drain::Ready { gap: Some(_), .. } => "ready-with-gap",
        Drain::Ready { tail: Some(_), .. } => "ready-with-tail",
        Drain::Ready { .. } => "ready",
    }
}
