import { createContext } from 'react';

/** Used only before the first snapshot, when there is nothing to animate yet. */
export const DEFAULT_TICK_MS = 100;

/**
 * The server's push interval, from the snapshot. Live widgets animate each update over one tick, so they move
 * continuously between pushes instead of stopping and starting on every push.
 */
export const FanoutTick = createContext(DEFAULT_TICK_MS);
