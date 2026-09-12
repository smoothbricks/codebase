/** Portable, canonical capture is an explicit cold boundary, never a state lookup strategy. */
export type CaptureValue =
  | null
  | boolean
  | number
  | string
  | readonly CaptureValue[]
  | { readonly [key: string]: CaptureValue };

export interface CaptureIssue {
  readonly code: 'size-limit' | 'non-portable' | 'schema' | 'decision' | 'incomplete';
  readonly boundary: string;
  readonly owner?: string;
  readonly declaration?: string;
  readonly schema?: string;
  readonly fromVersion?: number;
  readonly toVersion?: number;
  readonly limit?: number;
}

/** A refused capture/import is not a partially successful replay. No payloads enter this diagnostic. */
export class CaptureError extends Error {
  constructor(
    readonly issue: CaptureIssue,
    options?: ErrorOptions,
  ) {
    super(
      `StateBus ${issue.code}: ${issue.boundary}${issue.owner ? ` (${issue.owner}/${issue.declaration ?? ''})` : ''}`,
      options,
    );
    this.name = 'CaptureError';
    Object.freeze(issue);
  }
}

export function captureLimit(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(`${name} must be a positive safe integer.`);
  return value;
}

/** Number before string; numbers use numeric order, strings use UTF-16 code-unit order (not locale). */
export function compareResourceIds(left: string | number, right: string | number): number {
  if (typeof left === 'number') return typeof right === 'number' ? left - right : -1;
  if (typeof right === 'number') return 1;
  return left < right ? -1 : left === right ? 0 : 1;
}

/**
 * Canonical JSON serializer / owned-value builder. Codecs validate domain contracts; this walk implements
 * the transport format, including byte/depth limits, key ordering and rejection of executable objects.
 * It never invokes toJSON, getters or a prototype method. Optional undefined object fields are absent
 * on the wire; codecs must reconstruct anything whose absence would not be lossless. Nonfinite numbers,
 * sparse/undefined array members, cycles, class instances and other non-JSON values are refused.
 */
function inspectCapture(
  input: unknown,
  maxBytes = 8 * 1024 * 1024,
  copy = true,
): { readonly value: CaptureValue; readonly bytes: number } {
  captureLimit(maxBytes, 'maxBytes');
  let bytes = 0;
  const ancestors = new Set<object>();
  function add(size: number): void {
    bytes += size;
    if (bytes > maxBytes) throw new CaptureError({ code: 'size-limit', boundary: 'portable value', limit: maxBytes });
  }
  function stringSize(value: string): void {
    add(2);
    for (let index = 0; index < value.length; index++) {
      const code = value.charCodeAt(index);
      if (code === 34 || code === 92 || code === 8 || code === 9 || code === 10 || code === 12 || code === 13) add(2);
      else if (code < 32) add(6);
      else if (code < 128) add(1);
      else if (code < 2048) add(2);
      else if (code >= 0xd800 && code <= 0xdbff) {
        const next = value.charCodeAt(index + 1);
        if (next >= 0xdc00 && next <= 0xdfff) {
          add(4);
          index++;
        } else add(6);
      } else if (code >= 0xdc00 && code <= 0xdfff) add(6);
      else add(3);
    }
  }
  function visit(value: unknown, depth: number): CaptureValue {
    if (depth > 64) throw new CaptureError({ code: 'size-limit', boundary: 'capture nesting', limit: 64 });
    if (value === null) {
      add(4);
      return null;
    }
    if (typeof value === 'boolean') {
      add(value ? 4 : 5);
      return value;
    }
    if (typeof value === 'string') {
      stringSize(value);
      return value;
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
      add(String(value).length);
      return value === 0 ? 0 : value;
    }
    if (typeof value !== 'object' || value === null || ancestors.has(value))
      throw new CaptureError({ code: 'non-portable', boundary: 'codec output' });
    const array = Array.isArray(value);
    if (!array && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
      throw new CaptureError({ code: 'non-portable', boundary: 'codec output prototype' });
    if (Object.getOwnPropertySymbols(value).length > 0)
      throw new CaptureError({ code: 'non-portable', boundary: 'symbol capture keys' });
    ancestors.add(value);
    add(2);
    try {
      if (array) {
        const result: CaptureValue[] | undefined = copy ? [] : undefined;
        // Access descriptors so a surprising getter cannot execute at export time.
        for (let index = 0; index < value.length; index++) {
          const property = Object.getOwnPropertyDescriptor(value, index);
          if (!property || !('value' in property))
            throw new CaptureError({ code: 'non-portable', boundary: 'array member' });
          if (index > 0) add(1);
          const item: unknown = property.value;
          const child = visit(item, depth + 1);
          result?.push(child);
        }
        return result ? Object.freeze(result) : null;
      }
      const result: { [key: string]: CaptureValue } | undefined = copy ? {} : undefined;
      let count = 0;
      const keys = Object.keys(value);
      if (copy) keys.sort();
      for (const key of keys) {
        const property = Object.getOwnPropertyDescriptor(value, key);
        if (!property || !('value' in property))
          throw new CaptureError({ code: 'non-portable', boundary: 'object member' });
        const item: unknown = property.value;
        if (item === undefined) continue;
        if (count++ > 0) add(1);
        stringSize(key);
        add(1);
        const child = visit(item, depth + 1);
        if (result) Object.defineProperty(result, key, { value: child, enumerable: true });
      }
      return result ? Object.freeze(result) : null;
    } finally {
      ancestors.delete(value);
    }
  }
  const value = visit(input, 0);
  return Object.freeze({ value, bytes });
}

export function captureValue(
  input: unknown,
  maxBytes?: number,
): { readonly value: CaptureValue; readonly bytes: number } {
  return inspectCapture(input, maxBytes, true);
}
/** Exact canonical JSON UTF-8 bytes without allocating a second payload tree or JSON string. */
export function captureBytes(input: unknown, maxBytes?: number): number {
  return inspectCapture(input, maxBytes, false).bytes;
}

export function canonicalCapture(input: unknown, maxBytes?: number): string {
  return JSON.stringify(captureValue(input, maxBytes).value);
}
