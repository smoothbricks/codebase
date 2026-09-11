import {
  NAVIGATION_CANCELLED,
  NAVIGATION_DISPATCHED,
  type NavigationDriver,
  type NavigationObservation,
  type NavigationOutcome,
} from './driver.js';

/** Real in-memory history for stories and shell tests, not a mock of StateBus hooks. */
export function createMemoryNavigation<Location>(options: {
  readonly entries: readonly Location[];
  readonly index?: number;
  readonly equal: (left: Location, right: Location) => boolean;
}): NavigationDriver<Location, Location> {
  const first = options.entries[0];
  if (first === undefined) throw new RangeError('Memory navigation needs at least one entry.');
  const entries = [...options.entries];
  let index = options.index ?? entries.length - 1;
  if (!Number.isInteger(index) || index < 0 || index >= entries.length) throw new RangeError('Invalid history index.');
  const listeners = new Set<(observation: NavigationObservation<Location>) => void>();
  function current(): Location {
    // Index bounds are owned exclusively by this driver.
    const value = entries[index];
    if (value === undefined) throw new Error('Memory history invariant failed.');
    return value;
  }
  return {
    current,
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    execute(request, { signal }): NavigationOutcome {
      if (signal.aborted) return NAVIGATION_CANCELLED;
      const intent = request.intent;
      switch (intent.kind) {
        case 'external':
          return {
            kind: 'failed',
            error: { code: 'unsupported', message: 'External navigation is disabled in memory history.' },
          };
        case 'push':
          entries.splice(index + 1, entries.length, intent.to);
          index += 1;
          break;
        case 'replace':
          entries[index] = intent.to;
          break;
        case 'navigate':
          if (options.equal(current(), intent.to)) break;
          entries.splice(index + 1, entries.length, intent.to);
          index += 1;
          break;
        case 'back':
        case 'forward':
        case 'go': {
          const delta = intent.kind === 'go' ? intent.delta : intent.kind === 'back' ? -1 : 1;
          if (!Number.isSafeInteger(delta) || delta === 0) {
            return {
              kind: 'failed',
              error: { code: 'invalid-target', message: 'History delta must be a nonzero integer.' },
            };
          }
          const next = index + delta;
          if (next < 0 || next >= entries.length || next === index) break;
          index = next;
          break;
        }
      }
      const observation: NavigationObservation<Location> = {
        location: current(),
        source: 'intent',
        requestId: request.requestId,
      };
      for (const listener of listeners) listener(observation);
      return NAVIGATION_DISPATCHED;
    },
  };
}
