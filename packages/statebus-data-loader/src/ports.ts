import type { StateBusReader, StateInterest, StateInterestChange } from '@smoothbricks/statebus-core';
import type { LoaderEvent, LoadState } from './model.js';

/** These functions bind to one application event capability, not an ambient global schema. */
export interface LoaderChannel<T, Failure> {
  publish(event: LoaderEvent<T, Failure>): void;
  /** Must notify after the complete dispatch wave has reduced, as StateBus does. */
  subscribe(listener: (event: LoaderEvent<T, Failure>) => void): () => void;
  read(interest: StateInterest): LoadState<T, Failure>;
}

export interface InterestSource {
  snapshot(): readonly StateInterestChange[];
  subscribe(listener: (changes: readonly StateInterestChange[]) => void): () => void;
}

export function statebusInterestSource(bus: Pick<StateBusReader, 'subscribe' | 'getStateInterests'>): InterestSource {
  return {
    snapshot: () => bus.getStateInterests(),
    subscribe: (listener) =>
      bus.subscribe('statebus', 'substateInterest', (event) => {
        // Aggregate property counts cannot identify a keyed resource. Never guess an ID from them.
        if (event.payload.changes) listener(event.payload.changes);
      }),
  };
}

export interface TimerPort {
  after(milliseconds: number, callback: () => void): () => void;
}

export const systemTimer: TimerPort = {
  after(milliseconds, callback) {
    const timer = setTimeout(callback, milliseconds);
    return () => clearTimeout(timer);
  },
};
