/**
 * @fileoverview Validation of operator → driver message requests.
 */

import { z } from 'zod';
import { DRIVER_KEYS, DRIVER_TEXT_MAX, type DriverMessageRequest } from '../../shared/protocol.js';

export const driverMessageRequest: z.ZodType<DriverMessageRequest> = z.object({
  key: z.enum(DRIVER_KEYS),
  // One line for the driver's display; length is in string units, not bytes.
  text: z
    .string()
    .trim()
    .min(1)
    .max(DRIVER_TEXT_MAX)
    .regex(/^\P{Cc}*$/u, 'no control characters'),
});
