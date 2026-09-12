import type { StateInterestChange } from './interest.js';
import type { StateKeys } from './types.js';

export type { Computed } from '@tldraw/state';

declare const _byID_: unique symbol;

declare module '@smoothbricks/statebus-core' {
  export interface States {
    // User defined mapping of state-key to state-value type (primitive or object shape)
  }

  /**
   * ByID is a marker type that indicates that every Substate should be indexed by an id.
   */
  export type ByID<T> = T & { readonly [_byID_]: null };

  export interface Events {
    // Topics
    statebus: {
      // Event types
      substateInterest: {
        /** Aggregate demand per property, including keyed consumers. */
        subscribers: Partial<Record<StateKeys, number>>;
        /** Exact addresses emitted by the runtime. Keyed providers must use these, never aggregate counts. */
        changes?: readonly StateInterestChange<StateKeys>[];
      };
      error: Error;
      dispatchCompleted: { reducers: number; listeners: number };
    };
  }
}

export { StateBus } from './api.js';
export * from './composition.js';
export { type DispatchScheduler, ManualScheduler, microtaskScheduler } from './dispatch.js';
export * from './effects.js';
export type { StateInterest, StateInterestChange, StateInterestKey } from './interest.js';
export {
  mergeStateInterests,
  StateInterestBatch,
  StateInterestMap,
  StateInterestRegistry,
  stateInterestKey,
} from './interest.js';
export * from './journal.js';
export { ManualStateBus } from './manual.js';
export { MicrotaskStateBus } from './microtask.js';
export * from './recording.js';
export type {
  AnyEvent,
  AnyListener,
  Event,
  EventBus,
  EventPayload,
  EventReducer,
  EventReducers,
  EventTypes,
  InitialState,
  Listener,
  ReadonlyState,
  StateBusConfig,
  StateBusReader,
  StateBusWriter,
  StateByIDKey,
  StateKeys,
  StatePropKey,
  StateValue,
  Substate,
  SubstateRepository,
  Substates,
  SubstatesWriter,
  TopicReducer,
  TopicReducers,
  Topics,
  TopLevelReducer,
  WritableState,
} from './types.js';
export type { ViewFunction, ViewProps } from './view.js';
export { captureViewProps, computed, sameViewProps } from './view.js';
