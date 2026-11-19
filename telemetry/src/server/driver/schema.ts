/**
 * @fileoverview Validation of operator → driver requests.
 */

import { z } from 'zod';
import {
  DRIVER_ALL,
  DRIVER_DURATION_DEFAULT_S,
  DRIVER_DURATION_MAX_S,
  DRIVER_TEXT_MAX,
  DRIVER_TYPES,
  DRIVER_UNTIL_CLEARED,
  isDriverFlag,
  type DriverCommand,
} from '../../shared/protocol.js';

// One line for the driver's display; length is in string units, not bytes.
const text = z
  .string()
  .trim()
  .min(1)
  .max(DRIVER_TEXT_MAX)
  .regex(/^\P{Cc}*$/u, 'no control characters');

const show = z
  .object({
    action: z.literal('show'),
    type: z.enum(DRIVER_TYPES),
    text: text.optional(),
    durationS: z
      .union([z.literal(DRIVER_UNTIL_CLEARED), z.int().min(1).max(DRIVER_DURATION_MAX_S)])
      .default(DRIVER_DURATION_DEFAULT_S),
  })
  .refine((s) => s.text !== undefined || isDriverFlag(s.type), {
    path: ['text'],
    message: 'required for BOX, PACE and MSG',
  });

const clear = z.object({
  action: z.literal('clear'),
  type: z.enum([...DRIVER_TYPES, DRIVER_ALL]),
});

export const driverRequest: z.ZodType<DriverCommand> = z.discriminatedUnion('action', [show, clear]);
