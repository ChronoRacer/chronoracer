/**
 * @fileoverview Kafka Producer for Driver Messages
 *
 * Produces to the `event` topic on Kafka's private listener (plaintext, network-authorized, like Core and Relay).
 * One long-lived producer; it connects on the first send and reconnects on later ones.
 */

import { ProduceAcks, Producer, stringSerializers } from '@platformatic/kafka';
import type { DriverKey } from '../../shared/protocol.js';
import type { DriverKafkaConfig } from '../env/index.js';

export type DriverPublisher = {
  publish(key: DriverKey, text: string): Promise<void>;
  close(): Promise<void>;
};

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
  return {
    publish: async (key, text) => {
      await producer.send({ messages: [{ topic, key, value: text }] });
    },
    close: () => producer.close(),
  };
}
