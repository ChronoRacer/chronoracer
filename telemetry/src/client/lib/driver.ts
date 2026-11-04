import type { DriverKey, DriverMessage, DriverMessageRequest } from '../../shared/protocol';

export const DRIVER_KEY_LABEL: Record<DriverKey, string> = {
  MSG: 'Wiadomość',
  BOX: 'Boks',
  FLAG: 'Flaga',
  PACE: 'Tempo',
};

/** One-click texts; UI copy for this team, not part of the Kafka contract. */
export const DRIVER_PRESETS: readonly DriverMessageRequest[] = [
  { key: 'BOX', text: 'BOX BOX — zjedź w tym okrążeniu' },
  { key: 'BOX', text: 'Zostań na torze' },
  { key: 'PACE', text: 'Atakuj' },
  { key: 'PACE', text: 'Oszczędzaj paliwo' },
  { key: 'PACE', text: 'Zwolnij, chłodź silnik' },
  { key: 'FLAG', text: 'Żółta flaga — uważaj' },
  { key: 'FLAG', text: 'Safety car — zwolnij' },
  { key: 'FLAG', text: 'Czerwona flaga — zjedź do boksu' },
];

/** Resolves once the server accepted the message; its delivery arrives later over the socket. */
export async function sendDriverMessage(request: DriverMessageRequest): Promise<DriverMessage> {
  const res = await fetch('/api/driver/messages', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
  });
  const body = (await res.json().catch(() => undefined)) as (DriverMessage & { error?: string }) | undefined;
  if (res.status !== 202 || body === undefined) {
    throw new Error(body?.error ?? `HTTP ${res.status}`);
  }
  return body;
}

/** Adds new messages and replaces changed ones by `id`, keeping send order and the newest `max`. */
export function mergeDriverMessages(known: DriverMessage[], changed: DriverMessage[], max: number): DriverMessage[] {
  const byId = new Map(known.map((m) => [m.id, m]));
  for (const m of changed) {
    byId.set(m.id, m);
  }
  return [...byId.values()].sort((a, b) => a.sentAtMs - b.sentAtMs).slice(-max);
}
