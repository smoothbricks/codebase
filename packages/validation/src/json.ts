import { isRecord } from './guards.js';

export type JsonParseResult<T> = { ok: true; value: T } | { ok: false; error: string };

export type JsonGuard<T> = (value: unknown) => value is T;

/** Safe JSON.parse that returns a Result instead of throwing. */
export function safeJsonParse(raw: string): JsonParseResult<unknown> {
  try {
    return { ok: true, value: JSON.parse(raw) };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : 'JSON parse failed' };
  }
}

export function parseJsonValue<T>(raw: string, guard: JsonGuard<T>, expected: string): JsonParseResult<T> {
  const result = safeJsonParse(raw);
  if (!result.ok) return result;
  if (!guard(result.value)) {
    return { ok: false, error: `Expected ${expected}` };
  }
  return { ok: true, value: result.value };
}

/** Parse JSON and validate it's a record (object with string keys). */
export function parseJsonRecord(raw: string): JsonParseResult<Record<string, unknown>> {
  return parseJsonValue(raw, isRecord, 'JSON object');
}

/** Parse JSON and validate it's an array. */
export function parseJsonArray(raw: string): JsonParseResult<unknown[]> {
  return parseJsonValue(raw, Array.isArray, 'JSON array');
}
