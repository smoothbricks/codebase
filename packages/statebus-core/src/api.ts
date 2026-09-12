import { atom } from '@tldraw/state';
import { DispatchQueue } from './dispatch.js';
import { type StateInterest, StateInterestBatch, type StateInterestChange, StateInterestRegistry } from './interest.js';
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

  constructor(
    readonly defaultValue: (id: string | number) => T | undefined,
    private readonly interested: (id: string | number) => boolean,
  ) {}

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
      if (!this.interested(id)) this._byID.delete(id);
      return true;
    }
    return false;
  }

  releaseRemoved(id: string | number): void {
    if (!this.interested(id) && this._byID.get(id)?.get() === undefined) this._byID.delete(id);
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

function createStateEntry(key: string, value: unknown, interested: (id: string | number) => boolean) {
  return isSubstateFactory(value) ? new Substates(value, interested) : atom(`${key}`, value);
}

export abstract class StateBus implements StateBusReader {
  readonly isolates: Record<string, ReadonlyState> = {};
  readonly reduceEvent: TopLevelReducer;
  readonly state: WritableState;
  private readonly exactInterest = new StateInterestRegistry<StateKeys>((changes) => {
    const subscribers: Record<string, number> = {};
    for (const { interest } of changes) {
      const count = this.substateInterestCount.get(interest.key) ?? 0;
      if (interest.key === '__proto__') {
        Object.defineProperty(subscribers, interest.key, {
          value: count,
          enumerable: true,
          writable: true,
          configurable: true,
        });
      } else subscribers[interest.key] = count;
    }
    this.publish({ topic: 'statebus', type: 'substateInterest', payload: { subscribers, changes } });
  });
  readonly substateInterestCount: ReadonlyMap<StateKeys, number> = this.exactInterest.propertyCounts;
  private readonly keyedStates = new Map<string, Substates<unknown>>();
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
      Object.entries(this.initialState).map(([key, value]) => {
        const entry = createStateEntry(key, value, (id) => this.exactInterest.countAt(key, id) > 0);
        if (entry instanceof Substates) this.keyedStates.set(key, entry);
        return [key, entry];
      }),
    );
    this.state = Object.freeze(s) as WritableState;
  }

  private listeners: AnyTopicListenerMap = {};
  private readonly interestCounts = new SubscriberCountBatch<StateKeys>();
  private readonly interestChanges = new StateInterestBatch<StateKeys>();
  private readonly queue = new DispatchQueue<AnyEvent>(
    (events, count) => {
      try {
        this.dispatchWave(events, count);
      } finally {
        this.interestCounts.clear();
        this.interestChanges.clear();
      }
    },
    () => this.scheduleDispatch(),
  );

  dispatchEvents() {
    this.queue.flush();
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
        if (firstInterestEvent === undefined) firstInterestEvent = event;
        else {
          if (!multipleInterests) {
            this.interestCounts.append(firstInterestEvent.payload.subscribers);
            multipleInterests = true;
          }
          this.interestCounts.append(event.payload.subscribers);
        }
        if (event.payload.changes) this.interestChanges.append(event.payload.changes);
      } else {
        this.dispatchEvent(event);
      }
    }
    if (!firstInterestEvent) return;
    const subscribers = multipleInterests ? this.interestCounts.take() : firstInterestEvent.payload.subscribers;
    const changes = this.interestChanges.take();
    if (subscribers) {
      this.dispatchEvent(
        !multipleInterests &&
          (changes === firstInterestEvent.payload.changes ||
            (changes.length === 0 && firstInterestEvent.payload.changes === undefined))
          ? firstInterestEvent
          : { topic: 'statebus', type: 'substateInterest', payload: { subscribers, changes } },
      );
      // Read live counts: a listener may already have reacquired an address in the next wave.
      for (let index = 0; index < changes.length; index += 1) {
        const change = changes[index];
        if (change.subscribers === 0 && change.interest.id !== undefined) {
          this.keyedStates.get(change.interest.key)?.releaseRemoved(change.interest.id);
        }
      }
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
    return this.queue.publish(event);
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

  getStateInterests(): readonly StateInterestChange<StateKeys>[] {
    return this.exactInterest.snapshot();
  }

  substateInterest<SK extends StateKeys>(keys: readonly (SK | StateInterest<SK>)[]): () => void {
    return this.exactInterest.acquire(keys);
  }
}
