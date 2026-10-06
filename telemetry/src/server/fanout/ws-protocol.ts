// This file contains the functions to encode and decode the messages between the server and the clients.

import { z } from 'zod';
import type { ClientMessage, ServerMessage } from '../../shared/protocol.js';
import { toJson } from '../json.js';

export const encode = (message: ServerMessage<bigint>): string => toJson(message);

const clientMessage: z.ZodType<ClientMessage> = z.union([
  z.object({ type: z.literal('subscribe'), all: z.literal(true) }),
  z.object({ type: z.literal('subscribe'), sensors: z.array(z.string().min(1)).min(1) }),
]);

export function parseClientMessage(raw: string): ClientMessage | undefined {
  try {
    return clientMessage.parse(JSON.parse(raw));
  } catch {
    return undefined;
  }
}
