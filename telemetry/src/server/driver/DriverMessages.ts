/**
 * @fileoverview In-Memory History of Operator → Driver Messages
 *
 * Keeps the most recent messages with their delivery status, shared by every operator, and collects the ones that
 * are new or changed status for the next hub tick. Lost on restart, like the rest of the preview state.
 */

import { randomUUID } from 'node:crypto';
import Denque from 'denque';
import type { DriverMessage, DriverMessageRequest, DriverSnapshot } from '../../shared/protocol.js';

export const MAX_DRIVER_MESSAGES = 100;

export class DriverMessages {
  private readonly messages = new Denque<DriverMessage>();
  private changed = new Set<DriverMessage>();

  constructor(
    private readonly enabled: boolean,
    private readonly maxMessages = MAX_DRIVER_MESSAGES,
  ) {}

  /** Records a message as `sending` before it is produced. */
  add(request: DriverMessageRequest, sentBy: string): DriverMessage {
    const message: DriverMessage = {
      id: randomUUID(),
      key: request.key,
      text: request.text,
      sentBy,
      sentAtMs: Date.now(),
      status: 'sending',
    };
    this.messages.push(message);
    if (this.messages.length > this.maxMessages) {
      this.messages.shift();
    }
    this.changed.add(message);
    return message;
  }

  /** Delivered without `error`, failed with it. */
  settle(message: DriverMessage, error?: string): void {
    message.status = error === undefined ? 'delivered' : 'failed';
    if (error !== undefined) {
      message.error = error;
    }
    this.changed.add(message);
  }

  /** Oldest first. */
  snapshot(): DriverSnapshot {
    return { driverEnabled: this.enabled, driverMessages: this.messages.toArray() };
  }

  drain(): DriverMessage[] {
    const changed = [...this.changed];
    this.changed = new Set();
    return changed;
  }
}
