import {
  DRIVER_DURATION_DEFAULT_S,
  DRIVER_UNTIL_CLEARED,
  type DriverMessage,
  type DriverRequest,
  type DriverTarget,
  type DriverType,
} from '../../shared/protocol';

export const DRIVER_LABEL: Record<DriverTarget, string> = {
  YELLOW: 'YELLOW',
  RED: 'RED',
  GREEN: 'GREEN',
  BLUE: 'BLUE',
  SAFETY: 'SAFETY CAR',
  CHECKERED: 'CHEQUERED',
  BOX: 'BOX',
  PACE: 'PACE',
  MSG: 'MESSAGE',
  ALL: 'ALL',
};

/** Time choices for the next send, in seconds; the first is the contract default. */
export const DRIVER_DURATIONS: readonly { value: number; label: string }[] = [
  { value: DRIVER_DURATION_DEFAULT_S, label: '5 S' },
  { value: 10, label: '10 S' },
  { value: 30, label: '30 S' },
  { value: DRIVER_UNTIL_CLEARED, label: 'UNTIL CLEARED' },
];

/** One-click texts; UI copy for this team, not part of the Kafka contract. Flags carry no flag name: the car shows the colour. */
export const DRIVER_PRESETS: readonly { type: DriverType; text: string }[] = [
  { type: 'BOX', text: 'BOX BOX — zjedź w tym okrążeniu' },
  { type: 'BOX', text: 'Zostań na torze' },
  { type: 'PACE', text: 'Atakuj' },
  { type: 'PACE', text: 'Oszczędzaj paliwo' },
  { type: 'PACE', text: 'Zwolnij, chłodź silnik' },
  { type: 'YELLOW', text: 'Uważaj' },
  { type: 'SAFETY', text: 'Zwolnij' },
  { type: 'RED', text: 'Zjedź do boksu' },
];

export const formatDuration = (durationS: number) =>
  durationS === DRIVER_UNTIL_CLEARED ? 'UNTIL CLEARED' : `${durationS} S`;

/** Resolves once the server accepted the action; its delivery arrives later over the socket. */
export async function sendDriver(request: DriverRequest): Promise<DriverMessage> {
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
