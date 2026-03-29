import typia from 'typia';

const validateUnknownJson = typia.json.createValidateParse<unknown>();
const validateJsonRecord = typia.json.createValidateParse<Record<string, unknown>>();
const validateJsonArray = typia.json.createValidateParse<unknown[]>();

function describeJsonParseFailure(error: unknown): string {
  return error instanceof Error ? error.message : 'JSON parse failed';
}

function normalizeJsonExpected(expected: string | undefined, fallback: string): string {
  if (expected === 'Record<string, unknown>') {
    return 'Expected JSON object';
  }
  if (expected === 'Array<unknown>') {
    return 'Expected JSON array';
  }
  return expected ?? fallback;
}

export type JsonParseResult<T> = { ok: true; value: T } | { ok: false; error: string };

export type JsonBoundaryParser<T> = (value: unknown) => T;
export type JsonBoundaryValidator<T> = (value: unknown) => JsonParseResult<T>;

export interface JsonBoundaryParseOptions<T> {
  readonly parse: JsonBoundaryParser<T>;
  readonly expected?: string;
}

/** Safe JSON.parse that returns a Result instead of throwing. */
export function safeJsonParse(raw: string): JsonParseResult<unknown> {
  try {
    const result = validateUnknownJson(raw);
    if (result.success) {
      return { ok: true, value: result.data };
    }

    const firstError = result.errors[0];
    return { ok: false, error: normalizeJsonExpected(firstError?.expected, 'Expected valid JSON value') };
  } catch (error) {
    return { ok: false, error: describeJsonParseFailure(error) };
  }
}

export function validateJsonValue<T>(raw: string, validate: JsonBoundaryValidator<T>): JsonParseResult<T> {
  const result = safeJsonParse(raw);
  if (!result.ok) return result;

  return validate(result.value);
}

export function parseJsonBoundaryValue<T>(value: unknown, options: JsonBoundaryParseOptions<T>): JsonParseResult<T> {
  try {
    return { ok: true, value: options.parse(value) };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : `Expected ${options.expected ?? 'JSON value'}`,
    };
  }
}

export function parseJsonBoundary<T>(raw: string, options?: JsonBoundaryParseOptions<T>): JsonParseResult<T | unknown> {
  const result = safeJsonParse(raw);
  if (!result.ok || !options) {
    return result;
  }

  return parseJsonBoundaryValue(result.value, options);
}

export function expectJsonBoundary<T>(
  raw: string,
  options?: JsonBoundaryParseOptions<T>,
  fallbackExpected = 'JSON value',
): T | unknown {
  const result = parseJsonBoundary(raw, options);
  if (result.ok) {
    return result.value;
  }

  throw new Error(`Failed to parse ${options?.expected ?? fallbackExpected}: ${result.error}`);
}

export function expectJsonRecord(raw: string, errorPrefix = 'Invalid JSON object'): Record<string, unknown> {
  try {
    const result = validateJsonRecord(raw);
    if (result.success) return result.data;
    const firstError = result.errors[0];
    const message = normalizeJsonExpected(firstError?.expected, 'Expected JSON object');
    throw new TypeError(message);
  } catch (error) {
    throw new Error(`${errorPrefix}: ${describeJsonParseFailure(error)}`);
  }
}

export function parseJsonRecordString(
  value: unknown,
  fieldName = 'JSON value',
): JsonParseResult<Record<string, unknown>> {
  if (typeof value !== 'string') {
    return { ok: false, error: `${fieldName} must be a JSON string, got ${typeof value}` };
  }
  try {
    const result = validateJsonRecord(value);
    if (result.success) {
      return { ok: true, value: result.data };
    }
    const firstError = result.errors[0];
    return {
      ok: false,
      error: `${fieldName} must be valid JSON: ${normalizeJsonExpected(firstError?.expected, 'Expected JSON object')}`,
    };
  } catch (error) {
    return { ok: false, error: `${fieldName} must be valid JSON: ${describeJsonParseFailure(error)}` };
  }
}

export function parseOptionalJsonRecordString(
  value: unknown,
  fieldName = 'JSON value',
): JsonParseResult<Record<string, unknown> | undefined> {
  if (value === undefined || value === null) {
    return { ok: true, value: undefined };
  }

  if (typeof value !== 'string') {
    return { ok: false, error: `${fieldName} must be a JSON string or null, got ${typeof value}` };
  }
  try {
    const result = validateJsonRecord(value);
    if (result.success) {
      return { ok: true, value: result.data };
    }
    const firstError = result.errors[0];
    return {
      ok: false,
      error: `${fieldName} must be valid JSON: ${normalizeJsonExpected(firstError?.expected, 'Expected JSON object')}`,
    };
  } catch (error) {
    return { ok: false, error: `${fieldName} must be valid JSON: ${describeJsonParseFailure(error)}` };
  }
}

export function parseJsonValue<T>(raw: string, parse: JsonBoundaryParser<T>, expected: string): JsonParseResult<T> {
  const result = safeJsonParse(raw);
  if (!result.ok) {
    return result;
  }

  return parseJsonBoundaryValue(result.value, { parse, expected });
}

/** Parse JSON and validate it's a record (object with string keys). */
export function parseJsonRecord(raw: string): JsonParseResult<Record<string, unknown>> {
  try {
    const result = validateJsonRecord(raw);
    if (result.success) {
      return { ok: true, value: result.data };
    }

    const firstError = result.errors[0];
    return { ok: false, error: normalizeJsonExpected(firstError?.expected, 'Expected JSON object') };
  } catch (error) {
    return { ok: false, error: describeJsonParseFailure(error) };
  }
}

/** Parse JSON and validate it's an array. */
export function parseJsonArray(raw: string): JsonParseResult<unknown[]> {
  try {
    const result = validateJsonArray(raw);
    if (result.success) {
      return { ok: true, value: result.data };
    }

    const firstError = result.errors[0];
    return { ok: false, error: normalizeJsonExpected(firstError?.expected, 'Expected JSON array') };
  } catch (error) {
    return { ok: false, error: describeJsonParseFailure(error) };
  }
}
