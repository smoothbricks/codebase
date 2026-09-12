import type { StateInterestChange } from './interest.js';
import type { StateKeys } from './types.js';

export type { Computed } from '@tldraw/state';

declare const _byID_: unique symbol;

declare module '@smoothbricks/statebus-core' {
  export interface States {}
  /** ByID marks a state property whose values are indexed by resource ID. */
  export type ByID<T> = T & { readonly [_byID_]: null };
  export interface Events {
    statebus: {
      substateInterest: {
        subscribers: Partial<Record<StateKeys, number>>;
        changes?: readonly StateInterestChange<StateKeys>[];
      };
      error: Error;
      dispatchCompleted: { reducers: number; listeners: number };
    };
  }
}

export { StateBus } from './api.js';
export type { StateInterest, StateInterestChange, StateInterestKey } from './interest.js';
export {
  mergeStateInterests,
  StateInterestBatch,
  StateInterestMap,
  StateInterestRegistry,
  stateInterestKey,
} from './interest.js';
export { ManualStateBus } from './manual.js';
export { MicrotaskStateBus } from './microtask.js';
export type {
  AnyEvent, AnyListener, Event, EventBus, EventPayload, EventReducer, EventReducers,
  EventTypes, InitialState, Listener, ReadonlyState, StateBusConfig, StateBusReader,
  StateBusWriter, StateByIDKey, StateKeys, StatePropKey, StateValue, Substate,
  SubstateRepository, Substates, SubstatesWriter, TopicReducer, TopicReducers, Topics,
  TopLevelReducer, WritableState,
} from './types.js';
export type { ViewFunction, ViewProps } from './view.js';
export { captureViewProps, computed, sameViewProps } from './view.js';
export {
  composeLibraries, ComposedStateBus, defineLibrary, eventType, keyedState,
  LibraryScope, mountLibrary, onEvent, requiredBinding, scalarState, StateBusComposition,
} from './composition.js';
export type {
  BindingDeclaration, EventDeclaration, EventHandle, ExternalSubscription,
  HandleDescription, HandleMetadata, KeyedHandle, LibraryDefinition, LibraryEffect,
  LibraryMount, LibraryState, ProvidedBinding, RecordedEvent, RecordingOptions,
  ReducerBinding, ReducerState, RequiredBinding, ResourceId, ResourceInterest,
  RuntimeOptions, SavedState, SavedValue, ScalarHandle, StateBusCheckpoint,
  StateBusCodec, StateBusRecording, StateDeclaration, SupportClassification,
} from './composition.js';
export { plannedEffect } from './effect.js';
export type { CapturedOutcome, EffectExecutionContext, EffectOperation, PlannedEffect, PlannedEffectOptions } from './effect.js';
export { manualScheduler, microtaskScheduler } from './scheduler.js';
export type { DispatchScheduler } from './scheduler.js';
