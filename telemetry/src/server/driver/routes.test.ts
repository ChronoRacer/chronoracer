import { describe, expect, it, vi } from 'vitest';
import type { DriverMessage } from '../../shared/protocol.js';
import { DriverMessages } from './DriverMessages.js';
import { driverRecord, serial, type DriverPublisher } from './publisher.js';
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

describe('driver routes', () => {
  it('answers 202 with the action as sending, produces it, and records delivery', async () => {
    const { messages, publisher, post } = setup();
    const res = await post({ action: 'show', type: 'BOX', text: '  BOX BOX  ' });
    expect(res.status).toBe(202);
    const sent = (await res.json()) as DriverMessage;
    expect(sent).toMatchObject({
      action: 'show',
      type: 'BOX',
      text: 'BOX BOX',
      durationS: 5,
      sentBy: 'anonymous',
      status: 'sending',
    });
    expect(publisher!.publish).toHaveBeenCalledWith({ action: 'show', type: 'BOX', text: 'BOX BOX', durationS: 5 });

    await vi.waitFor(() => expect(messages.snapshot().driverMessages[0].status).toBe('delivered'));
    expect(messages.drain().messages.map((m) => [m.id, m.status])).toEqual([[sent.id, 'delivered']]);
    expect(messages.drain().messages).toEqual([]);
  });

  it('accepts a flag without text, until cleared, and clears of a type or of all', async () => {
    const { publisher, post } = setup();
    for (const body of [
      { action: 'show', type: 'RED', durationS: -1 },
      { action: 'show', type: 'YELLOW', text: 'Wypadek w zakręcie 3', durationS: 3600 },
      { action: 'clear', type: 'YELLOW', text: 'ignored' },
      { action: 'clear', type: 'ALL' },
    ]) {
      expect((await post(body)).status).toBe(202);
    }
    expect(publisher!.publish.mock.calls.map(([command]) => command)).toEqual([
      { action: 'show', type: 'RED', durationS: -1 },
      { action: 'show', type: 'YELLOW', text: 'Wypadek w zakręcie 3', durationS: 3600 },
      { action: 'clear', type: 'YELLOW' },
      { action: 'clear', type: 'ALL' },
    ]);
  });

  it('records a Kafka failure with its reason and leaves the active set alone', async () => {
    const { messages, post } = setup(async () => {
      throw new Error('broker down');
    });
    expect((await post({ action: 'show', type: 'MSG', text: 'hello' })).status).toBe(202);
    await vi.waitFor(() =>
      expect(messages.snapshot().driverMessages[0]).toMatchObject({ status: 'failed', error: 'broker down' }),
    );
    expect(messages.snapshot().driverActive).toEqual([]);
  });

  it('rejects invalid actions, types, text, durations, non-JSON and large bodies', async () => {
    const { messages, publisher, post } = setup();
    for (const body of [
      { type: 'MSG', text: 'x' },
      { action: 'hide', type: 'MSG' },
      { action: 'show', type: 'FLAG', text: 'x' },
      { action: 'show', type: 'ALL' },
      { action: 'show', type: 'BOX' },
      { action: 'show', type: 'MSG', text: '   ' },
      { action: 'show', type: 'MSG', text: 'x'.repeat(201) },
      { action: 'show', type: 'MSG', text: 'two\nlines' },
      { action: 'show', type: 'RED', text: '' },
      { action: 'show', type: 'RED', durationS: 0 },
      { action: 'show', type: 'RED', durationS: -2 },
      { action: 'show', type: 'RED', durationS: 3601 },
      { action: 'show', type: 'RED', durationS: 2.5 },
      { action: 'clear', type: 'OTHER' },
      'not json',
    ]) {
      expect((await post(body)).status, JSON.stringify(body)).toBe(400);
    }
    expect((await post({ action: 'show', type: 'MSG', text: 'x'.repeat(MAX_BODY_BYTES) })).status).toBe(413);
    expect(publisher!.publish).not.toHaveBeenCalled();
    expect(messages.snapshot().driverMessages).toEqual([]);
  });

  it('answers 503 without Kafka configuration', async () => {
    const { messages, post } = setup(null);
    expect((await post({ action: 'show', type: 'MSG', text: 'hello' })).status).toBe(503);
    expect(messages.snapshot()).toEqual({ driverEnabled: false, driverMessages: [], driverActive: [] });
  });
});

describe('driver state', () => {
  const show = (type: 'RED' | 'YELLOW' | 'BOX', durationS: number, text?: string) =>
    ({ action: 'show', type, durationS, ...(text === undefined ? {} : { text }) }) as const;

  function delivered(messages: DriverMessages, ...commands: Parameters<DriverMessages['add']>[0][]) {
    return (atMs: number) => {
      for (const command of commands) {
        messages.settle(messages.add(command, 'op', atMs), undefined, atMs);
      }
    };
  }

  it('keeps only the most recent messages', () => {
    const messages = new DriverMessages(true, 2);
    for (const text of ['a', 'b', 'c']) {
      messages.add({ action: 'show', type: 'MSG', text, durationS: 5 }, 'op');
    }
    expect(messages.snapshot().driverMessages.map((m) => m.action === 'show' && m.text)).toEqual(['b', 'c']);
  });

  it('activates on delivery only, replaces a type and moves it last, and expires timed entries', () => {
    const messages = new DriverMessages(true);
    const pending = messages.add(show('RED', -1), 'op', 0);
    expect(messages.drain(0).active).toBeUndefined();

    messages.settle(pending, undefined, 0);
    delivered(messages, show('YELLOW', 10, 'old'), show('BOX', 5, 'BOX BOX'))(1_000);
    delivered(messages, show('YELLOW', 30, 'new'))(2_000);
    expect(messages.drain(2_000).active).toEqual([
      { type: 'RED', durationS: -1, shownAtMs: 0, expiresAtMs: null, sentBy: 'op' },
      { type: 'BOX', text: 'BOX BOX', durationS: 5, shownAtMs: 1_000, expiresAtMs: 6_000, sentBy: 'op' },
      { type: 'YELLOW', text: 'new', durationS: 30, shownAtMs: 2_000, expiresAtMs: 32_000, sentBy: 'op' },
    ]);
    expect(messages.drain(5_999).active).toBeUndefined();
    expect(messages.drain(6_000).active?.map((a) => a.type)).toEqual(['RED', 'YELLOW']);
    expect(messages.snapshot(1_000_000).driverActive.map((a) => a.type)).toEqual(['RED']);
  });

  it('clears one type, ignores a type that is not active, and clears all', () => {
    const messages = new DriverMessages(true);
    delivered(messages, show('RED', -1), show('BOX', -1, 'x'))(0);
    messages.drain(0);

    delivered(messages, { action: 'clear', type: 'YELLOW' })(1);
    expect(messages.drain(1).active).toBeUndefined();
    delivered(messages, { action: 'clear', type: 'RED' })(2);
    expect(messages.drain(2).active?.map((a) => a.type)).toEqual(['BOX']);
    delivered(messages, { action: 'clear', type: 'ALL' })(3);
    expect(messages.drain(3).active).toEqual([]);
    delivered(messages, { action: 'clear', type: 'ALL' })(4);
    expect(messages.drain(4).active).toBeUndefined();
  });
});

describe('driver wire record', () => {
  it('keys by type or ALL and writes the contract JSON', () => {
    expect(driverRecord({ action: 'show', type: 'YELLOW', text: 'Wypadek w zakręcie 3', durationS: -1 })).toEqual({
      key: 'YELLOW',
      value: { action: 'show', text: 'Wypadek w zakręcie 3', duration_s: -1 },
    });
    expect(driverRecord({ action: 'show', type: 'RED', durationS: 30 })).toEqual({
      key: 'RED',
      value: { action: 'show', duration_s: 30 },
    });
    expect(driverRecord({ action: 'clear', type: 'ALL' })).toEqual({ key: 'ALL', value: { action: 'clear' } });
  });

  it('sends one at a time in call order, also after a failure', async () => {
    const inOrder = serial();
    const log: string[] = [];
    const task = (name: string, ms: number, fail = false) => () =>
      new Promise<void>((resolve, reject) =>
        setTimeout(() => {
          log.push(name);
          fail ? reject(new Error(name)) : resolve();
        }, ms),
      );
    const results = await Promise.allSettled([
      inOrder(task('show', 20, true)),
      inOrder(task('clear', 0)),
      inOrder(task('show-again', 5)),
    ]);
    expect(log).toEqual(['show', 'clear', 'show-again']);
    expect(results.map((r) => r.status)).toEqual(['rejected', 'fulfilled', 'fulfilled']);
  });
});
