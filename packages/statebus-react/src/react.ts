import type {
  AnyEvent,
  EventPayload,
  EventTypes,
  StateBus,
  StateByIDKey,
  StateInterest,
  StateKeys,
  StatePropKey,
  StateValue,
  Topics,
} from '@smoothbricks/statebus-core';
import {
  captureViewProps,
  computed,
  sameViewProps,
  type ViewFunction,
  type ViewProps,
} from '@smoothbricks/statebus-core';
import { isSignal, type Signal } from '@tldraw/state';
import { useValue } from '@tldraw/state-react';
import React, { useContext, useEffect, useMemo, useState } from 'react';

const StatebusReactContext = React.createContext<StateBus | null>(null);
export const StatebusProvider = StatebusReactContext.Provider;
export const useStateBus = () => {
  const bus = useContext(StatebusReactContext);
  if (!bus) {
    throw new Error('StateBus React hooks require a StatebusProvider.');
  }
  return bus;
};

const NO_INTEREST: readonly StateInterest<never>[] = Object.freeze([]);
type Interests<SK extends StateKeys> = readonly (SK | StateInterest<SK>)[];

function captureInterests<SK extends StateKeys>(input: Interests<SK>): readonly StateInterest<SK>[] {
  if (input.length === 0) return NO_INTEREST;
  return input.map((value) => (typeof value === 'string' ? { key: value } : { key: value.key, id: value.id }));
}

function sameInterests<SK extends StateKeys>(left: readonly StateInterest<SK>[], right: Interests<SK>): boolean {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index += 1) {
    const value = right[index];
    const key = typeof value === 'string' ? value : value.key;
    const id = typeof value === 'string' ? undefined : value.id;
    if (left[index].key !== key || left[index].id !== id) return false;
  }
  return true;
}

export function computedHook<SK extends StateKeys, Props extends ViewProps, R>(
  viewId: string,
  hook: ViewFunction<SK, Props, R>,
  latestDataInterest?: Interests<SK> | ((props: Props) => Interests<SK>),
): (props: Props) => R {
  // Static declarations are captured once. Dynamic declarations are pure functions of props.
  const staticInterest =
    typeof latestDataInterest === 'function' ? NO_INTEREST : captureInterests(latestDataInterest ?? NO_INTEREST);
  function bind(bus: StateBus, props: Props, previousInterests?: readonly StateInterest<SK>[]) {
    const captured = captureViewProps(props);
    const input = typeof latestDataInterest === 'function' ? latestDataInterest(captured) : staticInterest;
    const interests =
      previousInterests && sameInterests(previousInterests, input) ? previousInterests : captureInterests(input);
    return { bus, props: captured, interests, signal: computed(bus, viewId, hook, captured) };
  }
  return function useComputed(props: Props): R {
    const bus = useStateBus();
    const [binding, setBinding] = useState(() => bind(bus, props));
    let current = binding;
    if (binding.bus !== bus || !sameViewProps(binding.props, props)) {
      current = bind(bus, props, binding.interests);
      // Conditional render-state adjustment is isolated by React's concurrent renderer.
      // Unlike a mutable ref cache, an abandoned render cannot replace the committed view.
      setBinding(current);
    }
    const value = useValue(current.signal);
    const interests = current.interests;
    useEffect(() => bus.substateInterest(interests), [bus, interests]);
    return value;
  };
}

export function useSubstate<SK extends StateKeys>(key: StatePropKey<SK>): StateValue<SK>;
export function useSubstate<SK extends StateKeys>(key: StateByIDKey<SK>, id: string | number): StateValue<SK>;
export function useSubstate<SK extends StateKeys>(key: SK, id?: string | number): StateValue<SK> {
  const bus = useStateBus();
  const sub = bus.state[key];
  let signal: Signal<StateValue<SK>, unknown>;
  if (isSignal(sub)) {
    signal = sub as Signal<StateValue<SK>, unknown>;
  } else {
    if (id === undefined) throw new Error(`StateBus substate '${String(key)}' requires an id.`);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Library compiles without app-specific StateKeys augmentation.
    signal = sub.get(id) as Signal<StateValue<SK>, unknown>;
  }
  const value = useValue(signal);
  // Descriptor and lease storage are created only when this primitive address changes or mounts.
  useEffect(() => bus.substateInterest(id === undefined ? [key] : [{ key, id }]), [bus, key, id]);
  return value;
}

type EventTypePublisher<Topic extends Topics, Type extends EventTypes<Topic>> = (
  body: EventPayload<Topic, Type>,
) => void;
export type EventTopicPublisher<Topic extends Topics> = {
  readonly [Type in EventTypes<Topic>]: EventTypePublisher<Topic, Type>;
};
export type EventBusPublisher = {
  readonly [Topic in Exclude<Topics, 'statebus'>]: EventTopicPublisher<Topic>;
};

/** One binding per caller/runtime. Each event function is created on first use, not each property read. */
export function eventPublisher<Topic extends Topics>(bus: StateBus, topic: Topic): EventTopicPublisher<Topic>;
export function eventPublisher<Topic extends Topics>(bus: StateBus, topic: Topic) {
  const publishers = new Map<string, (payload: EventPayload<Topic, EventTypes<Topic>>) => void>();
  return new Proxy(
    {},
    {
      get: (target, prop) => {
        if (typeof prop === 'symbol') return Reflect.get(target, prop);
        let publish = publishers.get(prop);
        if (publish === undefined) {
          publish = (payload) => {
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Typed facade constrains the property to an event of the captured topic.
            const event = { topic, type: prop as EventTypes<Topic>, payload };
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Topic and event payload are paired by the typed facade.
            bus.publish(event as AnyEvent);
          };
          publishers.set(prop, publish);
        }
        return publish;
      },
    },
  );
}

function busPublisher(bus: StateBus): EventBusPublisher;
function busPublisher(bus: StateBus) {
  const topics = new Map<string, object>();
  return new Proxy(
    {},
    {
      get: (target, prop) => {
        if (typeof prop === 'symbol') return Reflect.get(target, prop);
        let publisher = topics.get(prop);
        if (publisher === undefined) {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- The typed root facade constrains topic names.
          publisher = eventPublisher(bus, prop as Topics);
          topics.set(prop, publisher);
        }
        return publisher;
      },
    },
  );
}

export function useBus(): EventBusPublisher;
export function useBus<Topic extends Exclude<Topics, 'statebus'>>(topic: Topic): EventTopicPublisher<Topic>;
export function useBus(topic?: Topics) {
  const bus = useStateBus();
  return useMemo(() => (topic === undefined ? busPublisher(bus) : eventPublisher(bus, topic)), [bus, topic]);
}
