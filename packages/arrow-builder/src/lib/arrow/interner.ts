/**
 * StringInterner - Pre-encoded UTF-8 bytes keyed by JS string.
 *
 * Used to avoid repeated UTF-8 encoding for known strings (enums, module names).
 * The interner is a simple Map - no wrapper class needed.
 */

const encoder = new TextEncoder();

/** Global interner instance for pre-encoded UTF-8 */
const strings = new Map<string, Uint8Array>();

/**
 * Intern a string - returns pre-encoded UTF-8 bytes.
 * If already interned, returns cached bytes.
 * If not, encodes and caches.
 */
export function intern(str: string): Uint8Array {
  let utf8 = strings.get(str);
  if (!utf8) {
    utf8 = encoder.encode(str);
    strings.set(str, utf8);
  }
  return utf8;
}

/**
 * Get pre-encoded UTF-8 if already interned, undefined otherwise.
 */
export function getInterned(str: string): Uint8Array | undefined {
  return strings.get(str);
}

/**
 * Check if a string is interned.
 */
export function isInterned(str: string): boolean {
  return strings.has(str);
}

/**
 * Get the number of interned strings.
 */
export function internedCount(): number {
  return strings.size;
}

/**
 * Clear all interned strings (mainly for testing).
 */
export function clearInterned(): void {
  strings.clear();
}
