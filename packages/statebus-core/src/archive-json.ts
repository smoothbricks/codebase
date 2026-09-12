/** Cold JSON transport utilities, not a domain/schema validator. Domain codecs remain authoritative. */
export interface ArchiveLimits {
  readonly maxBytes?: number;
  readonly maxDepth?: number;
  readonly maxNodes?: number;
}
export class ArchiveFailure extends Error {
  constructor(readonly code: 'limit' | 'json' | 'schema' | 'libraries' | 'migration' | 'incomplete' | 'codec') {
    super(code);
  }
}
const encoder = new TextEncoder();
export function canonicalJson(value: unknown, limits: ArchiveLimits = {}): string {
  const maxBytes = limits.maxBytes ?? 12 * 1024 * 1024;
  const maxDepth = limits.maxDepth ?? 128;
  const maxNodes = limits.maxNodes ?? 1_000_000;
  for (const limit of [maxBytes, maxDepth, maxNodes])
    if (!Number.isSafeInteger(limit) || limit < 1) throw new RangeError('Archive limits must be positive integers.');
  const chunks: string[] = [];
  const ancestors = new Set<object>();
  let bytes = 0;
  let nodes = 0;
  function append(text: string): void {
    bytes += encoder.encode(text).byteLength;
    if (bytes > maxBytes) throw new ArchiveFailure('limit');
    chunks.push(text);
  }
  function visit(current: unknown, depth: number): void {
    if (++nodes > maxNodes || depth > maxDepth) throw new ArchiveFailure('limit');
    if (current === null || typeof current === 'boolean') {
      append(String(current));
      return;
    }
    if (typeof current === 'string') {
      // Refuse before escaping a string that cannot possibly fit.
      if (current.length + bytes + 2 > maxBytes) throw new ArchiveFailure('limit');
      append(JSON.stringify(current));
      return;
    }
    if (typeof current === 'number') {
      if (!Number.isFinite(current)) throw new ArchiveFailure('json');
      append(JSON.stringify(current));
      return;
    }
    if (typeof current !== 'object') throw new ArchiveFailure('json');
    if (ancestors.has(current)) throw new ArchiveFailure('json');
    const array = Array.isArray(current);
    const prototype: unknown = Object.getPrototypeOf(current);
    if (!array && prototype !== Object.prototype && prototype !== null) throw new ArchiveFailure('json');
    if (Object.getOwnPropertySymbols(current).length !== 0) throw new ArchiveFailure('json');
    ancestors.add(current);
    // Inspect descriptors so neither getters nor toJSON hooks execute at an export boundary.
    const properties = Object.getOwnPropertyDescriptors(current);
    const keys = array ? Array.from({ length: current.length }, (_, index) => String(index)) : Object.keys(current).sort();
    if (keys.length + nodes > maxNodes) throw new ArchiveFailure('limit');
    append(array ? '[' : '{');
    let first = true;
    for (const key of keys) {
      const property = properties[key];
      if (!property || !('value' in property)) throw new ArchiveFailure('json');
      const child: unknown = property.value;
      if (!array && child === undefined) continue;
      if (!first) append(',');
      first = false;
      if (!array) {
        append(JSON.stringify(key));
        append(':');
      }
      visit(child, depth + 1);
    }
    append(array ? ']' : '}');
    ancestors.delete(current);
  }
  visit(value, 0);
  return chunks.join('');
}
function freezeJson(value: unknown): void {
  if (value === null || typeof value !== 'object') return;
  for (const child of Object.values(value)) freezeJson(child);
  Object.freeze(value);
}
export function ownJson(value: unknown, limits?: ArchiveLimits): unknown {
  const owned: unknown = JSON.parse(canonicalJson(value, limits));
  freezeJson(owned);
  return owned;
}
export function compareText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}
