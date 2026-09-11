import { atom } from '@tldraw/state';
import { SubscriberCountBatch } from './subscriber-counts.js';
import type {
  AnyEvent,
  AnyEventReducer,
  AnyTopicListenerMap,
  AnyTopicReducer,
  Atom,
  Event,
  EventTypes,
  InitialState,
  Substates as ISubstates,
  Listener,
  ReadonlyState,
  StateBusConfig,
  StateBusReader,
  StateKeys,
  Topics,
  TopLevelReducer,
  WritableState,
} from './types.js';

class Substates<T> implements ISubstates<T> {
  private readonly _byID = new Map<string | number, Atom<T | undefined>>();

  constructor(readonly defaultValue: (id: string | number) => T | undefined) {}

  get(id: string | number) {
    let substate = this._byID.get(id);
    if (!substate) {
      const value = this.defaultValue(id);
      substate = atom(`${id}`, value); // new Substate(value);
      this._byID.set(id, substate);
    }
    return substate;
  }

  remove(id: string | number): boolean {
    const existing = this._byID.get(id);
    if (existing) {
      // notify watchers that the substate is gone
      existing.set(undefined);
      return this._byID.delete(id);
    }
    return false;
  }

  [Symbol.iterator]() {
    // Return an iterator that filters undefined values
    const iter = this._byID.values();
    const filtered = {
      next: (): IteratorResult<T, undefined> => {
        let result = iter.next();
        while (!result.done) {
          const value = result.value?.get();
          if (value !== undefined) {
            return { value, done: false };
          }
          result = iter.next();
        }
        return { value: undefined, done: true };
      },
    };
    return filtered;
  }
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function isSubstateFactory(value: unknown): value is (id: string | number) => unknown {
  return typeof value === 'function';
}

function createStateEntry(key: string, value: unknown) {
  return isSubstateFactory(value) ? new Substates(value) : atom(`${key}`, value);
}

export abstract class StateBus implements StateBusReader {
  readonly isolates: Record<string, ReadonlyState> = {};
  readonly reduceEvent: TopLevelReducer;
  readonly state: WritableState;
  readonly substateInterestCount = new Map<string, number>();
  private initialState: InitialState;

  constructor(config: StateBusConfig) {
    const { initialState, reducers } = config;
    this.initialState = Object.freeze(initialState);

    if (typeof reducers === 'function') {
      this.reduceEvent = reducers;
    } else {
      // Build an object of per-topic reducer functions
      const topicMap: Record<string, AnyTopicReducer | undefined> = {};
      for (const [topicString, reducer] of Object.entries(reducers)) {
        if (typeof reducer === 'function') {
          topicMap[topicString] = reducer as AnyTopicReducer;
        } else {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Event type lookup is guarded by the already matched topic.
          const reducerMap = Object.freeze(reducer) as Record<string, AnyEventReducer>;
          // Create a function that dispatches on event type
          topicMap[topicString] = (state, event) => reducerMap[event.type]?.(state, event.payload);
        }
      }
      Object.freeze(topicMap);
      // Build a single reducer function that dispatches on topic and type
      this.reduceEvent = (state, event) => topicMap[event.topic]?.(state, event);
    }

    // Build the state object of Atoms and Substates
    const s = Object.fromEntries(
      Object.entries(this.initialState).map(([key, value]) => [key, createStateEntry(key, value)]),
    );
    this.state = Object.freeze(s) as WritableState;
  }

  // Keep backing capacity between waves; length = 0 can discard the backing store.
  // Slots are cleared after dispatch so retaining capacity never retains payloads.
  private queuedEventCount = 0;
  private eventQueue = new Array<AnyEvent | undefined>(32).fill(undefined);
  private dispatchingEventQueue = new Array<AnyEvent | undefined>(32).fill(undefined);
  private listeners: AnyTopicListenerMap = {};
  private dispatching = false;
  private readonly interestCounts = new SubscriberCountBatch<StateKeys>();

  dispatchEvents() {
    // A listener may request a flush, but must not interrupt the current wave.
    if (this.dispatching) return;
    this.dispatching = true;
    try {
      this.dispatchQueuedEvents();
    } finally {
      this.dispatching = false;
      // A failed reducer may have published work before throwing.
      if (this.queuedEventCount > 0) this.scheduleDispatch();
    }
  }

  private dispatchQueuedEvents() {
    while (this.queuedEventCount > 0) {
      const eventQueue = this.eventQueue;
      const eventCount = this.queuedEventCount;
      this.queuedEventCount = 0;
      // Reuse queue storage. Publications during this wave enter the other queue.
      this.eventQueue = this.dispatchingEventQueue;
      this.dispatchingEventQueue = eventQueue;
      try {
        this.dispatchWave(eventQueue, eventCount);
      } finally {
        // Never retain or replay a failed wave when this storage is reused.
        for (let index = 0; index < eventCount; index += 1) eventQueue[index] = undefined;
        this.interestCounts.clear();
      }
    }
  }

  private dispatchWave(eventQueue: readonly (AnyEvent | undefined)[], eventCount: number): void {
    let firstInterestEvent: Event<'statebus', 'substateInterest'> | undefined;
    let multipleInterests = false;
    // Reduce the complete wave before any listener observes it.
    for (let index = 0; index < eventCount; index += 1) {
      const event = eventQueue[index];
      if (event) this.reduceEvent(this.state, event);
    }
    for (let index = 0; index < eventCount; index += 1) {
      const event = eventQueue[index];
      if (!event) continue;
      if (event.topic === 'statebus' && event.type === 'substateInterest') {
        if (firstInterestEvent === undefined) {
          firstInterestEvent = event;
        } else {
          if (!multipleInterests) {
            this.interestCounts.append(firstInterestEvent.payload.subscribers);
            multipleInterests = true;
          }
          this.interestCounts.append(event.payload.subscribers);
        }
      } else {
        this.dispatchEvent(event);
      }
    }
    if (!firstInterestEvent) return;
    if (!multipleInterests) {
      this.dispatchEvent(firstInterestEvent);
      return;
    }
    const subscribers = this.interestCounts.take();
    if (subscribers) {
      this.dispatchEvent({ topic: 'statebus', type: 'substateInterest', payload: { subscribers } });
    }
  }

  dispatchEvent(event: AnyEvent) {
    const listeners = this.listeners[event.topic]?.[event.type];

    if (listeners) {
      for (const listener of listeners) {
        try {
          listener(event, this);
        } catch (error) {
          console.error('Error in listener while handling:', event, error);
          this.publish({
            topic: 'statebus',
            type: 'error',
            payload: toError(error),
          });
        }
      }
    }
  }

  protected abstract scheduleDispatch(): void;

  publish(event: AnyEvent) {
    const length = ++this.queuedEventCount;
    this.eventQueue[length - 1] = event;
    // The active dispatcher already drains every queued successor wave.
    if (!this.dispatching) this.scheduleDispatch();
    return length;
  }

  subscribe<Topic extends Topics, Type extends EventTypes<Topic>>(
    topic: Topic,
    type: Type,
    listener: Listener<Topic, Type>,
  ): () => void {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Listener storage is keyed by topic/type and only read back through the same keys.
    const topicListeners = (this.listeners[topic] ?? {}) as Record<Type, Set<Listener<Topic, Type>> | undefined>;
    this.listeners[topic] = topicListeners as AnyTopicListenerMap[Topic];

    const typeListeners = topicListeners[type] ?? new Set<Listener<Topic, Type>>();
    topicListeners[type] = typeListeners;
    typeListeners.add(listener);
    // Capture the actual set even when this is the first subscription.
    return () => {
      typeListeners.delete(listener);
    };
  }

  substateInterest<SK extends StateKeys>(keys: SK[]): () => void {
    if (keys.length === 0) return () => {};
    const subscribedKeys = [...keys];

    const subscribers: Record<SK, number> = {};
    for (const key of subscribedKeys) {
      const count = (this.substateInterestCount.get(key) ?? 0) + 1;
      subscribers[key] = count;
      this.substateInterestCount.set(key, count);
    }

    // Let data-providers know there is interest in these topics
    this.publish({ topic: 'statebus', type: 'substateInterest', payload: { subscribers } });

    let released = false;
    return () => {
      if (released) return;
      released = true;
      const subscribers: Record<SK, number> = {};
      for (const key of subscribedKeys) {
        const count = (this.substateInterestCount.get(key) ?? 1) - 1;
        subscribers[key] = count;
        if (count > 0) this.substateInterestCount.set(key, count);
        else this.substateInterestCount.delete(key);
      }
      this.publish({ topic: 'statebus', type: 'substateInterest', payload: { subscribers } });
    };
  }
}
