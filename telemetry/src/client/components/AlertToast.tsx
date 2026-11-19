import { useEffect, useRef, useState } from 'react';
import type { Alert } from '../../shared/protocol';
import { formatTimeUs } from '../lib/analysis-format';

const SHOW_MS = 6000;

type Props = { alerts: Alert[] };

type Toast = { key: string; alert: Alert };

/** Corner popup only when a new alert arrives — not a replay of the full history. */
export function AlertToast({ alerts }: Props) {
  const seen = useRef(new Set<string>());
  const primed = useRef(false);
  const [toast, setToast] = useState<Toast | null>(null);

  useEffect(() => {
    if (alerts.length === 0) {
      return;
    }
    if (!primed.current) {
      for (const a of alerts) {
        seen.current.add(alertKey(a));
      }
      primed.current = true;
      return;
    }
    const fresh = alerts.filter((a) => !seen.current.has(alertKey(a)));
    for (const a of alerts) {
      seen.current.add(alertKey(a));
    }
    const newest = fresh.at(-1);
    if (!newest) {
      return;
    }
    const key = alertKey(newest);
    setToast({ key, alert: newest });
    const clear = window.setTimeout(() => {
      setToast((cur) => (cur?.key === key ? null : cur));
    }, SHOW_MS);
    return () => window.clearTimeout(clear);
  }, [alerts]);

  if (!toast) {
    return null;
  }

  const { alert } = toast;
  return (
    <aside className="alert-toast" role="status" aria-live="assertive">
      <span className="alert-toast-label">ALERT</span>
      <span className="alert-toast-message">{alert.message}</span>
      <span className="alert-toast-meta">
        {formatTimeUs(alert.timestampUs)} · {alert.alertId}
      </span>
    </aside>
  );
}

function alertKey(a: Alert): string {
  return `${a.timestampUs}:${a.alertId}:${a.message}`;
}
