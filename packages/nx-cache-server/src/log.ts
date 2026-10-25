/**
 * One line per event, logfmt, on stderr. stdout is reserved for the machine-readable readiness
 * record consumed by the host supervisor. Every event names its key and outcome.
 * The supervisor stamps and collects these events; the server owns no permanent text log.
 *
 * Every value that came from a request goes through `logValue`, so a crafted cache key
 * cannot forge a field or split one event across two lines.
 */

const PROGRAM = 'nx-cache-server';

/** Values that need no quoting in logfmt: the cache-key charset plus the punctuation paths use. */
const BARE_VALUE = /^[\w.:@/+-]+$/;

export function logLine(fields: string): void {
  console.error(`${PROGRAM} ${fields}`);
}

/**
 * Render an untrusted string as one logfmt value. `JSON.stringify` is the escaping: it quotes,
 * escapes inner quotes, and turns newlines into `\n` so an event stays on one line.
 */
export function logValue(value: string): string {
  return BARE_VALUE.test(value) ? value : JSON.stringify(value);
}
