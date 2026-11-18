/**
 * @fileoverview In-Memory Operator → Driver State
 *
 * Keeps the most recent operator actions with their delivery status and the active set, both shared by every
 * operator, and collects what changed for the next hub tick. An action changes the active set only once Kafka
 * acknowledged it; the publisher sends one at a time, so acks arrive in topic order. Lost on restart, like the rest
 * of the preview state.
 */

import { randomUUID } from 'node:crypto';
import Denque from 'denque';
import {
  DRIVER_ALL,
  DRIVER_UNTIL_CLEARED,
  type DriverActive,
  type DriverCommand,
  type DriverMessage,
  type DriverSnapshot,
  type DriverType,
} from '../../shared/protocol.js';

export const MAX_DRIVER_MESSAGES = 100;

export type DriverChanges = {
  messages: DriverMessage[];
  /** The whole active set, only when it changed. */
  active?: DriverActive[];
};

export class DriverMessages {
  private readonly messages = new Denque<DriverMessage>();
  private changed = new Set<DriverMessage>();
  /** Insertion order is show order: a replaced type moves to the end. */
  private readonly active = new Map<DriverType, DriverActive>();
  private activeChanged = false;

  constructor(
    private readonly enabled: boolean,
    private readonly maxMessages = MAX_DRIVER_MESSAGES,
  ) {}

  /** Records an action as `sending` before it is produced. */
  add(command: DriverCommand, sentBy: string, nowMs = Date.now()): DriverMessage {
    const message: DriverMessage = { ...command, id: randomUUID(), sentBy, sentAtMs: nowMs, status: 'sending' };
    this.messages.push(message);
    if (this.messages.length > this.maxMessages) {
      this.messages.shift();
    }
    this.changed.add(message);
    return message;
  }

  /** Delivered without `error`, failed with it. A delivered action is applied to the active set. */
  settle(message: DriverMessage, error?: string, nowMs = Date.now()): void {
    message.status = error === undefined ? 'delivered' : 'failed';
    if (error !== undefined) {
      message.error = error;
    } else {
      this.apply(message, nowMs);
    }
    this.changed.add(message);
  }

  snapshot(nowMs = Date.now()): DriverSnapshot {
    this.expire(nowMs);
    return {
      driverEnabled: this.enabled,
      driverMessages: this.messages.toArray(),
      driverActive: [...this.active.values()],
    };
  }

  drain(nowMs = Date.now()): DriverChanges {
    this.expire(nowMs);
    const changes: DriverChanges = { messages: [...this.changed] };
    if (this.activeChanged) {
      changes.active = [...this.active.values()];
    }
    this.changed = new Set();
    this.activeChanged = false;
    return changes;
  }

  private apply(message: DriverMessage, nowMs: number): void {
    if (message.action === 'show') {
      const { type, text, durationS, sentBy } = message;
      this.active.delete(type);
      this.active.set(type, {
        type,
        ...(text === undefined ? {} : { text }),
        durationS,
        shownAtMs: nowMs,
        expiresAtMs: durationS === DRIVER_UNTIL_CLEARED ? null : nowMs + durationS * 1000,
        sentBy,
      });
      this.activeChanged = true;
      return;
    }
    if (message.type === DRIVER_ALL) {
      if (this.active.size > 0) {
        this.active.clear();
        this.activeChanged = true;
      }
      return;
    }
    if (this.active.delete(message.type)) {
      this.activeChanged = true;
    }
  }

  private expire(nowMs: number): void {
    for (const [type, { expiresAtMs }] of this.active) {
      if (expiresAtMs !== null && expiresAtMs <= nowMs) {
        this.active.delete(type);
        this.activeChanged = true;
      }
    }
  }
}
