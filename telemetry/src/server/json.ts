export const toJson = (value: unknown): string =>
  JSON.stringify(value, (_key: string, v: unknown) =>
    typeof v === "bigint" ? v.toString() : v,
  );
