import { isRecord } from './guards.js';

export type JsonParseResult<T> = { ok: true; value: T } | { ok: false; error: string };

/** Safe JSON.parse that returns a Result instead of throwing. */
export function safeJsonParse(raw: string): JsonParseResult<unknown> {
  try {
    return { ok: true, value: JSON.parse(raw) };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : 'JSON parse failed' };
  }
}

/** Parse JSON and validate it's a record (object with string keys). */
export function parseJsonRecord(raw: string): JsonParseResult<Record<string, unknown>> {
  const result = safeJsonParse(raw);
  if (!result.ok) return result;
  if (!isRecord(result.value)) {
    return { ok: false, error: `Expected JSON object, got ${typeof result.value}` };
  }
  return { ok: true, value: result.value };
}

/** Parse JSON and validate it's an array. */
export function parseJsonArray(raw: string): JsonParseResult<unknown[]> {
  const result = safeJsonParse(raw);
  if (!result.ok) return result;
  if (!Array.isArray(result.value)) {
    return { ok: false, error: `Expected JSON array, got ${typeof result.value}` };
  }
  return { ok: true, value: result.value };
}
