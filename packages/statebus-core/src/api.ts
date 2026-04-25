import { atom } from '@tldraw/state';
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

  private eventQueue: AnyEvent[] = [];
  private dispatchingEventQueue: AnyEvent[] = [];
  private listeners: AnyTopicListenerMap = {};

  dispatchEvents() {
    let substateInterestEvent: undefined | Event<'statebus', 'substateInterest'>;
    while (this.eventQueue.length > 0) {
      const eventQueue = this.eventQueue;
      // Set an empty queue in case publish is called while dispatching
      // Swap eventQueue and dispatchingEventQueue every dispatch to lower GC pressure
      this.eventQueue = this.dispatchingEventQueue;
      this.dispatchingEventQueue = eventQueue;

      // Reduce all events first, to make sure all state is up to date
      for (const event of eventQueue) {
        if (!event) continue;
        this.reduceEvent(this.state, event);
      }

      // Dispatch events to listeners, that may read updated state
      for (const event of eventQueue) {
        if (!event) {
          continue;
        }
        if (!(event.topic === 'statebus' && event.type === 'substateInterest')) {
          this.dispatchEvent(event);
        } else if (event.type === 'substateInterest') {
          if (substateInterestEvent) Object.assign(substateInterestEvent.payload, event.payload);
          else substateInterestEvent = event;
        }
      }
      // Finally dispatch the merged 'substateInterest' event
      if (substateInterestEvent) {
        this.dispatchEvent(substateInterestEvent);
      }
      // Clear the dispatching queue
      eventQueue.length = 0;
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
    const length = this.eventQueue.push(event);
    this.scheduleDispatch();
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

    const typeListeners = topicListeners[type];
    if (!typeListeners) {
      topicListeners[type] = new Set([listener]);
    } else {
      typeListeners.add(listener);
    }
    // Return a function to unsubscribe
    return () => typeListeners?.delete(listener);
  }

  substateInterest<SK extends StateKeys>(keys: SK[]): () => void {
    if (keys.length === 0) return () => {};

    const subscribers: Record<SK, number> = {};
    for (const key of keys) {
      const count = (this.substateInterestCount.get(key) ?? 0) + 1;
      subscribers[key] = count;
      this.substateInterestCount.set(key, count);
    }

    // Let data-providers know there is interest in these topics
    this.publish({ topic: 'statebus', type: 'substateInterest', payload: { subscribers } });

    return () => {
      const subscribers: Record<SK, number> = {};
      for (const key of keys) {
        const count = (this.substateInterestCount.get(key) ?? 1) - 1;
        if (count > 0) this.substateInterestCount.set(key, count);
        else this.substateInterestCount.delete(key);
      }
      this.publish({ topic: 'statebus', type: 'substateInterest', payload: { subscribers } });
    };
  }
}
