# Proposal - Core optimisations backlog

Status: **remaining backlog, not scheduled.** This file owns every Core optimisation idea that was judged and not built
(or built and worth recording). Add new ideas here. Micro-optimization items need a benchmark showing the cost before
they are built.

## Done

| Change                                                                                                                                                                                                                                                                                                                                           | Where                                                                                                                                                                       |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SimplePoint` removed. The batcher writes line protocol straight into a pooled `String` (`LineWriter`), with the per-message prefix built once and values formatted with `ryu` and `itoa`. Removes two `Arc` clones and drops per point (the driver `Arc` was shared by all concurrent flush tasks) and the second in-memory copy of each batch. | [`sensor_data_batcher.rs`](../../core/src/persistence/sensor_data_batcher.rs), [`sensor_data_flusher.rs`](../../core/src/persistence/sensor_data_flusher.rs)                |
| DLQ sampling (one in `processing.dlq_sample_every`) and no Kafka key copy for sensor data messages.                                                                                                                                                                                                                                              | [`dlq_handler.rs`](../../core/src/messaging/dlq_handler.rs), [`partial_message.rs`](../../core/src/messaging/partial_message.rs); behaviour in [Core](../component-core.md) |
| Splitter reserves both destination vectors for the incoming batch length, avoiding growth while routing. Each path preserves message order; the splitter still runs only when fast sensors are configured.                                                                                                                                       | [`sensor_data_splitter.rs`](../../core/src/persistence/sensor_data_splitter.rs); measurements below                                                                         |

Side effect to verify against a real InfluxDB 3: floats are now written as `3.0` (was `3`) and large or tiny values in
exponent form (`1e21`, was 22 digits). Both are valid line protocol floats; the field type stays float.

### Allocation microbenchmark (Core v0.3.0)

TODO: refine this further, might change with InfluxDB client version

Copying a 4.6 MB line-protocol measured about 169 µs per attempt on system allocator.
Technically, the InfluxDB client supports an internal retry loop which avoids the copies. However, this changes the
retry mechanism and makes collecting metrics from retries harder if not impossible. Retries also have a finite count
inside the client, so we would have to nack (or just avoid commiting) offset of the message that failed the write so
it's redelivered.

Not sure if this is worth the gain, though it could remove most of the retry path and offload it to the library. Time
spent copying is not very important as this is on the persistence path, but the copied data buffer itself is (5MB is a lot),
especially that unless the database fails, we are doing it with each flush and then dropping the unused original right after.

---

## Wire format change: `bytes` ids in the telemetry stream

`point_batch` in [`telemetry/serve/mod.rs`](../../core/src/telemetry/serve/mod.rs) runs `to_owned()` on `sensor_id`
and `driver_id` for every point: two allocations, copies and frees per point, about 400k allocations per second at
200k pts/s.

Change (protocol version stays 1, the bytes on the wire are identical):

1. In [`telemetry_v1.proto`](../../core/proto/telemetry_v1.proto) change `Point.sensor_id` and `Point.driver_id` from
   `string` to `bytes`. Check `buf breaking` rules first: `string` to `bytes` is wire compatible but changes the JSON
   mapping.
2. `build.rs`: `.bytes(["."])`, then build one `bytes::Bytes` per chunk and per batch with `Bytes::from_owner`.
   Per point it becomes one refcount increment.
3. Clients, in the same release:
    - Python: regenerate `analysis/shared/proto/telemetry_v1_pb2*.py` and `load-test/gen/*`; decode `point.sensor_id`
      and
      `point.driver_id` in `analysis/shared/connector.py`.
    - TypeScript: `buf generate` in `telemetry/` (fields become `Uint8Array`); decode in `src/server/core/watch.ts` and
      `types.ts`, encode in `scripts/mock-core.mjs`.
    - Update [ADR - Core telemetry stream](../adr-telemetry-stream.md) if it states the field types.

Worth doing only if a `point_batch` benchmark shows allocation as a measurable cost.

A larger wire change, not recommended without a protocol version bump: group points by sensor in a batch (`sensor_id`
once per group) so the ids are not repeated per point at all. It also shrinks the HTTP/2 payload.

## Smaller ideas

| Idea                                                      | Reasoning                                                                                                                                                                                                            | Cost                    |
|-----------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| `write(&str)` copies the body (`to_vec`) on every attempt | influxdb3-client 0.3.0 `WriteInput` produces owned `Vec<u8>` batches. Passing `String` moves and consumes the buffer; the API cannot retain it for retries or return it to the pool. See the copy measurement above. | See measurements above  |
| Move formatting off the batcher task                      | Formatting now runs on one task per path (estimated ~1% of a core at 200k pts/s). If it ever dominates, format in rayon or per flush task again.                                                                     | Only if measured        |
| Lock-free driver register                                 | `RetainReadTimeValue` is already `ArcSwap`; the batcher reads it once per incoming batch. `load()` instead of `load_full()` saves one atomic per batch.                                                              | Not worth an API change |
| `PreviewHub` drain buffers                                | `try_drain` and `pending` allocate per wakeup, not per point.                                                                                                                                                        | Not worth it            |

## Rejected (checked, no gain)

| Idea                                         | Why not                                                                                           |
|----------------------------------------------|---------------------------------------------------------------------------------------------------|
| Decode Avro without copying the payload      | `BorrowedMessage` is tied to the consumer; decoding runs on rayon threads, so the copy is needed. |
| Avro decoder, offset tracker, pipeline stats | Already allocation-free or coalesced; nothing to gain.                                            |
