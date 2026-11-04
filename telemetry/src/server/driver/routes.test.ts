import { describe, expect, it, vi } from 'vitest';
import type { DriverMessage } from '../../shared/protocol.js';
import { DriverMessages } from './DriverMessages.js';
import type { DriverPublisher } from './publisher.js';
import { driverRoutes, MAX_BODY_BYTES } from './routes.js';

function setup(publish: DriverPublisher['publish'] | null = async () => {}) {
  const messages = new DriverMessages(publish !== null);
  const publisher = publish === null ? undefined : { publish: vi.fn(publish), close: async () => {} };
  const app = driverRoutes(messages, publisher, { enabled: false });
  const post = (body: unknown) =>
    app.request('/messages', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: typeof body === 'string' ? body : JSON.stringify(body),
    });
  return { messages, publisher, post };
}

describe('driver messages', () => {
  it('answers 202 with the message as sending, produces it, and records delivery', async () => {
    const { messages, publisher, post } = setup();
    const res = await post({ key: 'BOX', text: '  BOX BOX  ' });
    expect(res.status).toBe(202);
    const sent = (await res.json()) as DriverMessage;
    expect(sent).toMatchObject({ key: 'BOX', text: 'BOX BOX', sentBy: 'anonymous', status: 'sending' });
    expect(publisher!.publish).toHaveBeenCalledWith('BOX', 'BOX BOX');

    await vi.waitFor(() => expect(messages.snapshot().driverMessages[0].status).toBe('delivered'));
    expect(messages.drain().map((m) => [m.id, m.status])).toEqual([[sent.id, 'delivered']]);
    expect(messages.drain()).toEqual([]);
  });

  it('records a Kafka failure with its reason', async () => {
    const { messages, post } = setup(async () => {
      throw new Error('broker down');
    });
    expect((await post({ key: 'MSG', text: 'hello' })).status).toBe(202);
    await vi.waitFor(() =>
      expect(messages.snapshot().driverMessages[0]).toMatchObject({ status: 'failed', error: 'broker down' }),
    );
  });

  it('rejects unknown keys, empty or long text, control characters, non-JSON and large bodies', async () => {
    const { messages, publisher, post } = setup();
    for (const body of [
      { key: 'OTHER', text: 'x' },
      { key: 'MSG', text: '   ' },
      { key: 'MSG', text: 'x'.repeat(201) },
      { key: 'MSG', text: 'two\nlines' },
      'not json',
    ]) {
      expect((await post(body)).status).toBe(400);
    }
    expect((await post({ key: 'MSG', text: 'x'.repeat(MAX_BODY_BYTES) })).status).toBe(413);
    expect(publisher!.publish).not.toHaveBeenCalled();
    expect(messages.snapshot().driverMessages).toEqual([]);
  });

  it('answers 503 without Kafka configuration', async () => {
    const { messages, post } = setup(null);
    expect((await post({ key: 'MSG', text: 'hello' })).status).toBe(503);
    expect(messages.snapshot()).toEqual({ driverEnabled: false, driverMessages: [] });
  });

  it('keeps only the most recent messages', () => {
    const messages = new DriverMessages(true, 2);
    for (const text of ['a', 'b', 'c']) {
      messages.add({ key: 'MSG', text }, 'op');
    }
    expect(messages.snapshot().driverMessages.map((m) => m.text)).toEqual(['b', 'c']);
  });
});
