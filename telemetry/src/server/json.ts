/**
 * Safely serializes data to JSON, natively converting BigInt values to strings
 * to prevent standard `TypeError: Do not know how to serialize a BigInt`.
 */

export const toJson = (value: unknown): string =>
  JSON.stringify(value, (_key: string, v: unknown) => (typeof v === 'bigint' ? v.toString() : v));
