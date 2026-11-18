/**
 * @fileoverview Kafka Producer for Driver Flags and Messages
 *
 * Produces to the `event` topic on Kafka's private listener (plaintext, network-authorized, like Core and Relay).
 * One long-lived producer; it connects on the first send and reconnects on later ones. Sends go out one at a time,
 * so a `clear` reaches the topic after the `show` it cancels. Wire contract:
 * docs/component-kafka.md (driver display records).
 */

import { ProduceAcks, Producer, stringSerializers } from '@platformatic/kafka';
import type { DriverCommand, DriverTarget } from '../../shared/protocol.js';
import type { DriverKafkaConfig } from '../env/index.js';

export type DriverPublisher = {
  publish(command: DriverCommand): Promise<void>;
  close(): Promise<void>;
};

export type DriverRecord = {
  key: DriverTarget;
  value: { action: 'show'; text?: string; duration_s: number } | { action: 'clear' };
};

/** Kafka key and JSON value of one operator action. */
export function driverRecord(command: DriverCommand): DriverRecord {
  if (command.action === 'clear') {
    return { key: command.type, value: { action: 'clear' } };
  }
  const { type, text, durationS } = command;
  return { key: type, value: { action: 'show', ...(text === undefined ? {} : { text }), duration_s: durationS } };
}

/** Runs `task`s one after another, whatever the previous one's outcome. */
export function serial(): <T>(task: () => Promise<T>) => Promise<T> {
  let tail: Promise<unknown> = Promise.resolve();
  return (task) => {
    const run = tail.then(task, task);
    tail = run.catch(() => undefined);
    return run;
  };
}

// Same guarantees as Relay's operator events: acks from all in-sync replicas, idempotent, a few bounded retries.
const KAFKA_TIMEOUT_MS = 5_000;

export function kafkaDriverPublisher({ bootstrap, topic }: NonNullable<DriverKafkaConfig>): DriverPublisher {
  const producer = new Producer({
    clientId: 'telemetry',
    bootstrapBrokers: [bootstrap],
    serializers: stringSerializers,
    idempotent: true,
    acks: ProduceAcks.ALL,
    retries: 3,
    retryDelay: 250,
    timeout: KAFKA_TIMEOUT_MS,
    connectTimeout: KAFKA_TIMEOUT_MS,
    requestTimeout: KAFKA_TIMEOUT_MS,
    autocreateTopics: false,
  });
  const inOrder = serial();
  return {
    publish: async (command) => {
      const { key, value } = driverRecord(command);
      await inOrder(() => producer.send({ messages: [{ topic, key, value: JSON.stringify(value) }] }));
    },
    close: () => producer.close(),
  };
}
