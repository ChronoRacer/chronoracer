/**
 * @fileoverview Operator → Driver Message Routes (`/api/driver/*`)
 *
 * A signed-in browser posts a message. It is recorded as `sending`, answered with 202, and produced to Kafka in the
 * background; the outcome reaches every browser through the hub. Session and same-origin checks are applied by the
 * caller.
 */

import { Hono } from 'hono';
import { bodyLimit } from 'hono/body-limit';
import { readSession } from '../auth/index.js';
import type { AuthConfig } from '../env/index.js';
import { log, logError } from '../log.js';
import type { DriverMessages } from './DriverMessages.js';
import type { DriverPublisher } from './publisher.js';
import { driverMessageRequest } from './schema.js';

export const MAX_BODY_BYTES = 4 * 1024;

export function driverRoutes(messages: DriverMessages, publisher: DriverPublisher | undefined, auth: AuthConfig) {
  return new Hono()
    .use('*', bodyLimit({ maxSize: MAX_BODY_BYTES, onError: (c) => c.json({ error: 'body too large' }, 413) }))
    .post('/messages', async (c) => {
      if (!publisher) {
        return c.json({ error: 'driver messages disabled: KAFKA_PRIVATE_BOOTSTRAP not set' }, 503);
      }
      let raw: unknown;
      try {
        raw = await c.req.json();
      } catch {
        return c.json({ error: 'body is not JSON' }, 400);
      }
      const parsed = driverMessageRequest.safeParse(raw);
      if (!parsed.success) {
        const issues = parsed.error.issues.map(({ path, message }) => ({ path: path.join('.'), message }));
        return c.json({ error: 'invalid body', issues }, 400);
      }
      const session = await readSession(c, auth);
      const message = messages.add(parsed.data, session?.email ?? session?.sub ?? 'unknown');
      log('driver_message_sending', { id: message.id, key: message.key, sentBy: message.sentBy });
      publisher.publish(message.key, message.text).then(
        () => {
          messages.settle(message);
          log('driver_message_delivered', { id: message.id });
        },
        (err: unknown) => {
          messages.settle(message, err instanceof Error ? err.message : String(err));
          logError('driver_message', err);
        },
      );
      return c.json(message, 202);
    });
}
