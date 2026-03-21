/** Assert value is defined (not null/undefined). Throws with message on failure. */
export function assertDefined<T>(value: T | null | undefined, message?: string): asserts value is T {
  if (value == null) {
    throw new Error(message ?? `Expected value to be defined, got ${String(value)}`);
  }
}

/** Assert value is a Record. Throws with message on failure. */
export function assertRecord(value: unknown, message?: string): asserts value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(message ?? `Expected a record object, got ${typeof value}`);
  }
}

/** Exhaustiveness check for switch/if-else chains. */
export function assertNever(value: never, message?: string): never {
  throw new Error(message ?? `Unexpected value: ${String(value)}`);
}
