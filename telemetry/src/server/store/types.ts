/**
 * @fileoverview Server-side Telemetry Store Types
 *
 * Adapts shared protocol types for internal server storage by binding the generic `U64`
 * type parameter specifically to native `bigint`.
 *
 * This guarantees nanosecond timestamp precision in memory while abstracting away
 * the network-layer `string` serialization.
 */

import type { Buckets, CoreStatus, Gap, Latest, Snapshot } from '../../shared/protocol.js';

export type { Buckets, Latest, RingStats } from '../../shared/protocol.js';

export type GapEvent = Gap<bigint>;
export type StoreStatus = CoreStatus<bigint>;
export type StoreSnapshot = Snapshot<bigint>;

export type Delta = { buckets: Record<string, Buckets>; latest: Latest; gaps: GapEvent[] };
