// Types for the telemetry server store.

import type { CoreStatus, Gap, Snapshot } from '../../shared/protocol.js';
import type { Point } from '../core/types.js';

export type { RingStats } from '../../shared/protocol.js';

export type GapEvent = Gap<bigint>;
export type StoreStatus = CoreStatus<bigint>;
export type StoreSnapshot = Snapshot<bigint>;

export type Delta = { points: Point[]; gaps: GapEvent[] };
