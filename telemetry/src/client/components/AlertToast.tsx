import { useEffect, useRef, useState } from 'react';
import type { Alert } from '../../shared/protocol';
import { formatTimeUs } from '../lib/analysis-format';

const SHOW_MS = 6000;

type Props = {
  alerts: Alert[];
  /** The alert list is on screen: new alerts count as seen and get no popup. */
  muted: boolean;
};

type Toast = { key: string; alert: Alert };

/** Corner popup only when a new alert arrives — not a replay of the full history, not while the alert list is shown. */
export function AlertToast({ alerts, muted }: Props) {
  const seen = useRef(new Set<string>());
  const primed = useRef(false);
  const [toast, setToast] = useState<Toast | null>(null);

  useEffect(() => {
    if (alerts.length === 0) {
      return;
    }
    const fresh = alerts.filter((a) => !seen.current.has(alertKey(a)));
    // The list keeps only the newest alerts; one that dropped out never comes back.
    seen.current = new Set(alerts.map(alertKey));
    if (!primed.current) {
      primed.current = true;
      return;
    }
    const newest = fresh.at(-1);
    if (newest && !muted) {
      setToast({ key: alertKey(newest), alert: newest });
    }
  }, [alerts, muted]);

  useEffect(() => {
    if (muted) {
      setToast(null);
    }
  }, [muted]);

  // Keyed on the toast itself, so a resync snapshot that replaces `alerts` cannot cancel the hide.
  useEffect(() => {
    if (!toast) {
      return;
    }
    const hide = window.setTimeout(() => setToast(null), SHOW_MS);
    return () => window.clearTimeout(hide);
  }, [toast]);

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
