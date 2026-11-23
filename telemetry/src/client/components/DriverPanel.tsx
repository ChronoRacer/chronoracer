import { useEffect, useRef, useState, type FormEvent, type ReactNode } from 'react';
import {
  DRIVER_ALL,
  DRIVER_DURATION_DEFAULT_S,
  DRIVER_DURATION_MAX_S,
  DRIVER_FLAG_TYPES,
  DRIVER_TEXT_MAX,
  DRIVER_TYPES,
  isDriverFlag,
  type DriverActive,
  type DriverMessage,
  type DriverRequest,
  type DriverTarget,
  type DriverType,
} from '../../shared/protocol';
import { DRIVER_DURATIONS, DRIVER_LABEL, DRIVER_PRESETS, formatDuration, sendDriver } from '../lib/driver';
import { cx } from './instruments';

type Props = {
  enabled: boolean;
  messages: DriverMessage[];
  /** Oldest first. */
  active: DriverActive[];
};

const STATUS_LABEL = { sending: 'SENDING…', delivered: 'SENT', failed: 'FAILED' } as const;
const STATUS_TONE = { sending: 'warn', delivered: 'ok', failed: 'bad' } as const;
const STATUS_HINT = {
  sending: 'Waiting for Kafka ack',
  delivered: 'Kafka acknowledged write; not a driver read receipt',
  failed: 'Kafka rejected the message',
} as const;

const formatTime = (ms: number) => new Date(ms).toLocaleTimeString('en-GB');

/** Types with an action still waiting for its Kafka ack; their controls wait too. */
const sendingTypes = (messages: DriverMessage[]) =>
  new Set<DriverTarget>(messages.filter((m) => m.status === 'sending').map((m) => m.type));

/** One colour dot per type on the driver's display; shown on the DRIVER tab button. */
export function DriverDots({ active }: { active: DriverActive[] }) {
  if (active.length === 0) {
    return null;
  }
  return (
    <span className="driver-dots" aria-label={`${active.length} active on the driver's display`}>
      {active.map((a) => (
        <TypeSwatch key={a.type} type={a.type} />
      ))}
    </span>
  );
}

/** DRIVER tab: on display, time, flags | message, history. */
export function DriverPanel({ enabled, messages, active }: Props) {
  const [durationS, setDurationS] = useState<number | undefined>(DRIVER_DURATION_DEFAULT_S);
  const [error, setError] = useState<string>();
  const sending = sendingTypes(messages);

  const send = async (request: DriverRequest) => {
    setError(undefined);
    try {
      await sendDriver(request);
      return true;
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      return false;
    }
  };

  return (
    <section className="zone driver" aria-label="Driver radio">
      {enabled ? null : (
        <div className="driver-error" role="status">
          SEND DISABLED: SERVER HAS NO KAFKA CONFIG.
        </div>
      )}
      <ActiveList enabled={enabled} active={active} sending={sending} send={send} />
      <Box title="TIME ON DISPLAY" hint="applies to the next flag or message you send" className="driver-time">
        <Duration enabled={enabled} onChange={setDurationS} />
      </Box>
      <div className="driver-send-grid">
        <Box
          title="FLAGS"
          hint="one click shows the flag on the driver's display at once; click a lit flag to clear it"
          className="driver-flags-box"
        >
          <Flags enabled={enabled} active={active} sending={sending} durationS={durationS} send={send} />
        </Box>
        <Box
          title="MESSAGE"
          hint="write your own text, pick its type, then SHOW or Enter; a preset only fills the box"
          className="driver-message-box"
        >
          <Composer enabled={enabled} durationS={durationS} send={send} />
        </Box>
      </div>
      {error ? (
        <div className="driver-error" role="alert">
          NOT SENT: {error}
        </div>
      ) : null}
      <Box title="HISTORY" hint="every operator's actions, newest first">
        <History messages={messages} />
      </Box>
    </section>
  );
}

/** A titled DRIVER section; `hint` says in one line what its controls do. */
function Box({
  title,
  hint,
  aside,
  className,
  children,
}: {
  title: string;
  hint?: string;
  aside?: ReactNode;
  className?: string;
  children: ReactNode;
}) {
  return (
    <section className={cx('driver-box', className)} aria-label={title}>
      <header className="driver-box-head">
        <h3 className="zone-title">{title}</h3>
        {hint ? <span className="hint">{hint}</span> : null}
        {aside}
      </header>
      {children}
    </section>
  );
}

type Send = (request: DriverRequest) => Promise<boolean>;

function ActiveList({
  enabled,
  active,
  sending,
  send,
}: {
  enabled: boolean;
  active: DriverActive[];
  sending: Set<DriverTarget>;
  send: Send;
}) {
  const now = useNow(active.some((a) => a.expiresAtMs !== null));
  return (
    <Box
      title="ON DISPLAY"
      hint="operators' view; the car sends no receipt"
      className="driver-active"
      aside={
        <button
          type="button"
          className="driver-clear"
          onClick={() => void send({ action: 'clear', type: DRIVER_ALL })}
          disabled={!enabled || sending.has(DRIVER_ALL)}
        >
          CLEAR ALL
        </button>
      }
    >
      {active.length === 0 ? (
        <p className="empty-note">NOTHING ACTIVE</p>
      ) : (
        <ul className="driver-active-list">
          {active.map((a) => (
            <li key={a.type} className={cx('driver-active-item', `dt-${a.type.toLowerCase()}`)}>
              <TypeChip type={a.type} />
              <span className="alert-message">{a.text ?? '—'}</span>
              <span className="card-unit" title={`Shown ${formatTime(a.shownAtMs)} by ${a.sentBy}`}>
                {a.expiresAtMs === null
                  ? 'UNTIL CLEARED'
                  : `${Math.max(0, Math.ceil((a.expiresAtMs - now) / 1000))} S LEFT`}
              </span>
              <span className="hint">{a.sentBy}</span>
              <button
                type="button"
                className="driver-clear"
                onClick={() => void send({ action: 'clear', type: a.type })}
                disabled={!enabled || sending.has(a.type)}
              >
                CLEAR
              </button>
            </li>
          ))}
        </ul>
      )}
    </Box>
  );
}

/** One latching button per flag: shows it with the chosen time, or clears it while it is active. */
function Flags({
  enabled,
  active,
  sending,
  durationS,
  send,
}: {
  enabled: boolean;
  active: DriverActive[];
  sending: Set<DriverTarget>;
  durationS: number | undefined;
  send: Send;
}) {
  const on = new Set(active.map((a) => a.type));
  return (
    <div className="driver-flags" role="group" aria-label="Flags">
      {DRIVER_FLAG_TYPES.map((type) => {
        const pressed = on.has(type);
        return (
          <button
            key={type}
            type="button"
            aria-pressed={pressed}
            className={cx('driver-flag', `dt-${type.toLowerCase()}`, pressed && 'on')}
            onClick={() => void send(pressed ? { action: 'clear', type } : { action: 'show', type, durationS })}
            disabled={!enabled || (!pressed && durationS === undefined) || sending.has(type) || sending.has(DRIVER_ALL)}
          >
            <TypeSwatch type={type} />
            {DRIVER_LABEL[type]}
          </button>
        );
      })}
    </div>
  );
}

/** Time for the next send; `undefined` while a custom value is invalid. */
function Duration({ enabled, onChange }: { enabled: boolean; onChange: (durationS: number | undefined) => void }) {
  const [choice, setChoice] = useState<number | 'custom'>(DRIVER_DURATION_DEFAULT_S);
  const [custom, setCustom] = useState('60');
  const customValue = parseCustom(custom);

  useEffect(() => onChange(choice === 'custom' ? customValue : choice), [choice, customValue, onChange]);

  return (
    <div className="driver-keys" role="radiogroup" aria-label="Time on the driver's display">
      {DRIVER_DURATIONS.map(({ value, label }) => (
        <button
          key={value}
          type="button"
          role="radio"
          aria-checked={choice === value}
          className={cx('driver-key', choice === value && 'on')}
          onClick={() => setChoice(value)}
          disabled={!enabled}
        >
          {label}
        </button>
      ))}
      <button
        type="button"
        role="radio"
        aria-checked={choice === 'custom'}
        className={cx('driver-key', choice === 'custom' && 'on')}
        onClick={() => setChoice('custom')}
        disabled={!enabled}
      >
        CUSTOM
      </button>
      {choice === 'custom' ? (
        <label className="driver-custom">
          <input
            type="number"
            min={1}
            max={DRIVER_DURATION_MAX_S}
            step={1}
            value={custom}
            onChange={(e) => setCustom(e.target.value)}
            aria-label="Custom time in seconds"
            aria-invalid={customValue === undefined}
            disabled={!enabled}
          />
          <span className="card-unit">S (1–{DRIVER_DURATION_MAX_S})</span>
        </label>
      ) : null}
    </div>
  );
}

function parseCustom(raw: string): number | undefined {
  const value = Number(raw);
  return raw.trim() !== '' && Number.isInteger(value) && value >= 1 && value <= DRIVER_DURATION_MAX_S
    ? value
    : undefined;
}

function Composer({ enabled, durationS, send }: { enabled: boolean; durationS: number | undefined; send: Send }) {
  const [type, setType] = useState<DriverType>('MSG');
  const [text, setText] = useState('');
  const [pending, setPending] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  const trimmed = text.trim();
  const canSend = enabled && !pending && durationS !== undefined && (trimmed.length > 0 || isDriverFlag(type));

  const pick = (preset: { type: DriverType; text: string }) => {
    setType(preset.type);
    setText(preset.text);
    input.current?.focus();
  };

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (!canSend) {
      return;
    }
    setPending(true);
    const ok = await send({ action: 'show', type, ...(trimmed ? { text: trimmed } : {}), durationS });
    if (ok) {
      setText('');
    }
    setPending(false);
    input.current?.focus();
  };

  return (
    <form className="driver-form" onSubmit={submit}>
      <div className="driver-keys" role="radiogroup" aria-label="Message type">
        <span className="card-unit">TYPE</span>
        {DRIVER_TYPES.map((t) => (
          <button
            key={t}
            type="button"
            role="radio"
            aria-checked={type === t}
            className={cx('driver-key', type === t && 'on')}
            onClick={() => setType(t)}
            disabled={!enabled}
          >
            <TypeSwatch type={t} />
            {DRIVER_LABEL[t]}
          </button>
        ))}
      </div>
      <div className="driver-input">
        <input
          ref={input}
          value={text}
          onChange={(e) => setText(e.target.value)}
          maxLength={DRIVER_TEXT_MAX}
          placeholder={isDriverFlag(type) ? 'Optional text on the flag colour' : 'Message for driver'}
          aria-label="Message text"
          disabled={!enabled}
          autoFocus
        />
        <span className="card-unit" aria-label="Character count">
          {text.length}/{DRIVER_TEXT_MAX}
        </span>
        <button type="submit" className="driver-send" disabled={!canSend}>
          {pending ? 'SENDING…' : `SHOW ${DRIVER_LABEL[type]}`}
        </button>
      </div>
      <div className="driver-presets" role="group" aria-label="Preset messages">
        <span className="card-unit">PRESETS</span>
        {DRIVER_PRESETS.map((p) => (
          <button
            key={`${p.type}:${p.text}`}
            type="button"
            className="driver-preset"
            onClick={() => pick(p)}
            disabled={!enabled}
          >
            <TypeSwatch type={p.type} />
            <span className="card-unit">{DRIVER_LABEL[p.type]}</span>
            {p.text}
          </button>
        ))}
      </div>
    </form>
  );
}

function History({ messages }: { messages: DriverMessage[] }) {
  if (messages.length === 0) {
    return <p className="empty-note">NO MESSAGES SENT YET</p>;
  }
  return (
    <ul className="driver-history" aria-label="Sent actions">
      {[...messages].reverse().map((m) => (
        <li key={m.id} className={cx('driver-item', `is-${m.status}`)}>
          <div className="alert-head">
            <span className="alert-time">{formatTime(m.sentAtMs)}</span>
            <span className="chip">{m.action === 'show' ? 'SHOW' : 'CLEAR'}</span>
            <TypeChip type={m.type} />
            {m.action === 'show' ? (
              <>
                <span className="alert-message">{m.text ?? '—'}</span>
                <span className="card-unit">{formatDuration(m.durationS)}</span>
              </>
            ) : null}
          </div>
          <div className="driver-meta">
            <StatusChip message={m} />
            <span className="hint">{m.sentBy}</span>
            {m.error ? <span className="driver-error">{m.error}</span> : null}
          </div>
        </li>
      ))}
    </ul>
  );
}

function TypeSwatch({ type }: { type: DriverTarget }) {
  return <span className={cx('driver-swatch', `dt-${type.toLowerCase()}`)} title={DRIVER_LABEL[type]} aria-hidden />;
}

function TypeChip({ type }: { type: DriverTarget }) {
  return (
    <span className="chip driver-type">
      <TypeSwatch type={type} />
      {DRIVER_LABEL[type]}
    </span>
  );
}

function StatusChip({ message: { status } }: { message: DriverMessage }) {
  return (
    <span className={cx('chip', `tone-${STATUS_TONE[status]}`)} title={STATUS_HINT[status]}>
      {STATUS_LABEL[status]}
    </span>
  );
}

/** Wall clock, ticking twice a second only while `ticking`. */
function useNow(ticking: boolean): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!ticking) {
      return;
    }
    setNow(Date.now());
    const timer = setInterval(() => setNow(Date.now()), 500);
    return () => clearInterval(timer);
  }, [ticking]);
  return now;
}
