import { useRef, useState, type FormEvent } from 'react';
import {
  DRIVER_KEYS,
  DRIVER_TEXT_MAX,
  type DriverKey,
  type DriverMessage,
  type DriverMessageRequest,
} from '../../shared/protocol';
import { DRIVER_KEY_LABEL, DRIVER_PRESETS, sendDriverMessage } from '../lib/driver';
import { cx } from './instruments';

type Props = {
  enabled: boolean;
  messages: DriverMessage[];
  /** Compact strip in the chrome; the composer and history go in the drawer. */
  variant?: 'strip' | 'full';
  onExpand?: () => void;
};

const STATUS_LABEL = { sending: 'SENDING…', delivered: 'SENT', failed: 'FAILED' } as const;
const STATUS_TONE = { sending: 'warn', delivered: 'ok', failed: 'bad' } as const;
const STATUS_HINT = {
  sending: 'Waiting for Kafka ack',
  delivered: 'Kafka acknowledged write; not a driver read receipt',
  failed: 'Kafka rejected the message',
} as const;

const formatTime = (ms: number) => new Date(ms).toLocaleTimeString('en-GB');

export function DriverPanel({ enabled, messages, variant = 'full', onExpand }: Props) {
  const latest = messages.at(-1);

  if (variant === 'strip') {
    return (
      <button type="button" className="meta-strip" onClick={onExpand} aria-label="Expand driver radio">
        <span className="zone-title">DRIVER</span>
        {latest ? (
          <span className="meta-latest">
            <span className="alert-time">{formatTime(latest.sentAtMs)}</span>
            <span className="alert-message">{latest.text}</span>
            <StatusChip message={latest} />
          </span>
        ) : (
          <span className="hint">{enabled ? 'SEND MESSAGE' : 'DISABLED'}</span>
        )}
      </button>
    );
  }

  return (
    <section className="zone driver" aria-label="Driver radio">
      <Composer enabled={enabled} />
      {messages.length === 0 ? (
        <p className="empty-note">NO MESSAGES SENT YET</p>
      ) : (
        <ul className="driver-history" aria-label="Sent messages">
          {[...messages].reverse().map((m) => (
            <li key={m.id} className={cx('driver-item', `is-${m.status}`)}>
              <div className="alert-head">
                <span className="alert-time">{formatTime(m.sentAtMs)}</span>
                <span className="chip">{DRIVER_KEY_LABEL[m.key]}</span>
                <span className="alert-message">{m.text}</span>
              </div>
              <div className="driver-meta">
                <StatusChip message={m} />
                <span className="hint">{m.sentBy}</span>
                {m.error ? <span className="driver-error">{m.error}</span> : null}
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function StatusChip({ message: { status } }: { message: DriverMessage }) {
  return (
    <span className={cx('chip', `tone-${STATUS_TONE[status]}`)} title={STATUS_HINT[status]}>
      {STATUS_LABEL[status]}
    </span>
  );
}

function Composer({ enabled }: { enabled: boolean }) {
  const [key, setKey] = useState<DriverKey>('MSG');
  const [text, setText] = useState('');
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string>();
  const input = useRef<HTMLInputElement>(null);
  const canSend = enabled && !pending && text.trim().length > 0;

  const pick = (preset: DriverMessageRequest) => {
    setKey(preset.key);
    setText(preset.text);
    setError(undefined);
    input.current?.focus();
  };

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (!canSend) {
      return;
    }
    setPending(true);
    setError(undefined);
    try {
      await sendDriverMessage({ key, text: text.trim() });
      setText('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setPending(false);
      input.current?.focus();
    }
  };

  return (
    <form className="driver-compose" onSubmit={submit}>
      {enabled ? null : (
        <div className="driver-error" role="status">
          SEND DISABLED: SERVER HAS NO KAFKA CONFIG.
        </div>
      )}
      <div className="driver-keys" role="radiogroup" aria-label="Message kind">
        {DRIVER_KEYS.map((k) => (
          <button
            key={k}
            type="button"
            role="radio"
            aria-checked={key === k}
            className={cx('driver-key', key === k && 'on')}
            onClick={() => setKey(k)}
            disabled={!enabled}
          >
            {DRIVER_KEY_LABEL[k]}
          </button>
        ))}
      </div>
      <div className="driver-input">
        <input
          ref={input}
          value={text}
          onChange={(e) => setText(e.target.value)}
          maxLength={DRIVER_TEXT_MAX}
          placeholder="Message for driver"
          aria-label="Message text"
          disabled={!enabled}
          autoFocus
        />
        <span className="card-unit" aria-label="Character count">
          {text.length}/{DRIVER_TEXT_MAX}
        </span>
        <button type="submit" className="driver-send" disabled={!canSend}>
          {pending ? 'SENDING…' : 'SEND'}
        </button>
      </div>
      {error ? (
        <div className="driver-error" role="alert">
          NOT SENT: {error}
        </div>
      ) : null}
      <div className="driver-presets" aria-label="Preset messages">
        {DRIVER_PRESETS.map((p) => (
          <button
            key={`${p.key}:${p.text}`}
            type="button"
            className="driver-preset"
            onClick={() => pick(p)}
            disabled={!enabled}
          >
            <span className="card-unit">{DRIVER_KEY_LABEL[p.key]}</span>
            {p.text}
          </button>
        ))}
      </div>
    </form>
  );
}
