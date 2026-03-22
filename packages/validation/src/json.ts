import { isRecord } from './guards.js';

export type JsonParseResult<T> = { ok: true; value: T } | { ok: false; error: string };

export type JsonBoundaryParser<T> = (value: unknown) => T;
export type JsonBoundaryValidator<T> = (value: unknown) => JsonParseResult<T>;

/** Safe JSON.parse that returns a Result instead of throwing. */
export function safeJsonParse(raw: string): JsonParseResult<unknown> {
  try {
    return { ok: true, value: JSON.parse(raw) };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : 'JSON parse failed' };
  }
}

export function validateJsonValue<T>(raw: string, validate: JsonBoundaryValidator<T>): JsonParseResult<T> {
  const result = safeJsonParse(raw);
  if (!result.ok) return result;

  return validate(result.value);
}

export function parseJsonValue<T>(raw: string, parse: JsonBoundaryParser<T>, expected: string): JsonParseResult<T> {
  const result = safeJsonParse(raw);
  if (!result.ok) return result;

  try {
    return { ok: true, value: parse(result.value) };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : `Expected ${expected}`,
    };
  }
}

function validateJsonRecordValue(value: unknown): JsonParseResult<Record<string, unknown>> {
  if (!isRecord(value)) {
    return { ok: false, error: 'Expected JSON object' };
  }

  return { ok: true, value };
}

function validateJsonArrayValue(value: unknown): JsonParseResult<unknown[]> {
  if (!Array.isArray(value)) {
    return { ok: false, error: 'Expected JSON array' };
  }

  return { ok: true, value };
}

/** Parse JSON and validate it's a record (object with string keys). */
export function parseJsonRecord(raw: string): JsonParseResult<Record<string, unknown>> {
  return validateJsonValue(raw, validateJsonRecordValue);
}

/** Parse JSON and validate it's an array. */
export function parseJsonArray(raw: string): JsonParseResult<unknown[]> {
  return validateJsonValue(raw, validateJsonArrayValue);
}
