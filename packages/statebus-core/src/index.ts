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
      substateInterest: { subscribers: Partial<Record<StateKeys, number>> };
      error: Error;
      dispatchCompleted: { reducers: number; listeners: number };
    };
  }
}

export { StateBus } from './api.js';
export { ManualStateBus } from './manual.js';
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
export { computed, sortedKeyValuePairs } from './view.js';
