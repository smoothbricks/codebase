/** A plain object: its keys are known to exist, its values are not known to be anything. */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
