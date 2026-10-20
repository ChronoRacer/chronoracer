/**
 * @fileoverview Serialization & Runtime Validation for Client-Server Messages
 *
 * Handles outgoing JSON serialization (supporting BigInt via `toJson`) and strict
 * runtime validation of incoming client payloads using Zod schema parsing.
 */

import { z } from 'zod';
import type { ClientMessage, ServerMessage } from '../../shared/protocol.js';
import { toJson } from '../json.js';

export const encode = (message: ServerMessage<bigint>): string => toJson(message);

const clientMessage: z.ZodType<ClientMessage> = z.union([
  z.object({ type: z.literal('subscribe'), all: z.literal(true) }),
  z.object({ type: z.literal('subscribe'), sensors: z.array(z.string().min(1)).min(1) }),
]);

/**
 * Safely parses and validates raw WebSocket text payloads against the ClientMessage schema.
 * @returns Validated `ClientMessage` object, or `undefined` if parsing/validation fails.
 */
export function parseClientMessage(raw: string): ClientMessage | undefined {
  try {
    return clientMessage.parse(JSON.parse(raw));
  } catch {
    return undefined;
  }
}
