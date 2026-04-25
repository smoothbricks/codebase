import type {
  AnyEvent,
  EventPayload,
  EventTypes,
  StateBus,
  StateByIDKey,
  StateKeys,
  StatePropKey,
  StateValue,
  Topics,
} from '@smoothbricks/statebus-core';
import { computed, sortedKeyValuePairs, type ViewFunction, type ViewProps } from '@smoothbricks/statebus-core';
import { isSignal, type Signal } from '@tldraw/state';
import { useValue } from '@tldraw/state-react';
import React, { useContext, useEffect, useMemo } from 'react';

const StatebusReactContext = React.createContext<StateBus | null>(null);
export const StatebusProvider = StatebusReactContext.Provider;
export const useStateBus = () => {
  const bus = useContext(StatebusReactContext);
  if (!bus) {
    throw new Error('StateBus React hooks require a StatebusProvider.');
  }
  return bus;
};

function useBusSignal<T, SK extends StateKeys>(
  bus: StateBus,
  _viewId: string,
  signal: Signal<T, unknown>,
  latestDataInterest: SK[],
): T {
  /*---
  // Awaiting answer why `useState` shouldn't work: https://github.com/tldraw/signia/issues/88
  const [state, setState] = useState(signal.get());
  
  useEffect(() => {
    const decrement = latestDataInterest ? bus.substateInterest(latestDataInterest) : undefined;
    const unreact = react(viewId, () => setState(signal.get()));
    return () => {
      unreact();
      decrement?.();
    };
  }, [bus, ...latestDataInterest]);
  //---*/

  const value = useValue(signal);
  // biome-ignore lint/correctness/useExhaustiveDependencies: latestDataInterest is used as spread
  useEffect(
    () => (latestDataInterest ? bus.substateInterest(latestDataInterest) : undefined),
    [bus, ...latestDataInterest],
  );

  return value;
}

export function computedHook<SK extends StateKeys, Props extends ViewProps, R>(
  viewId: string,
  hook: ViewFunction<SK, Props, R>,
  latestDataInterest?: SK[],
): (props: Props) => R {
  return (props) => {
    const bus = useStateBus();
    const deps = useMemo(
      () => (!props || typeof props !== 'object' ? [props, bus] : [bus, ...sortedKeyValuePairs(props)]),
      [props, bus],
    );
    // biome-ignore lint/correctness/useExhaustiveDependencies: dynamic deps array
    const signal = useMemo(() => computed(bus, viewId, hook, props), deps);
    return useBusSignal(bus, viewId, signal, latestDataInterest ?? []);
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
    if (id === undefined) {
      throw new Error(`StateBus substate '${String(key)}' requires an id.`);
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Library source compiles without app-specific StateKeys augmentation.
    signal = sub.get(id) as Signal<StateValue<SK>, unknown>;
  }

  return useBusSignal(bus, key, signal, [key]);
}

type EventTypePublisher<
  Topic extends Topics,
  Type extends EventTypes<Topic>,
  Body extends EventPayload<Topic, Type>,
> = (body: Body extends undefined ? undefined : Body) => void;

type EventTopicPublisher<Topic extends Topics> = {
  readonly [Type in EventTypes<Topic>]: EventTypePublisher<Topic, Type, EventPayload<Topic, Type>>;
};

type EventBusPublisher = {
  readonly [Topic in Exclude<Topics, 'statebus'>]: EventTopicPublisher<Topic>;
};

export function eventPublisher<Topic extends Topics>(bus: StateBus, topic: Topic) {
  return new Proxy(
    {},
    {
      get: (target, prop) => {
        if (typeof prop === 'symbol') return Reflect.get(target, prop);
        return (payload: EventPayload<Topic, EventTypes<Topic>>) => {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Proxy property names are the runtime event keys for this topic.
          const event = { topic, type: prop as EventTypes<Topic>, payload };
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Proxy property names are the runtime event keys for this topic.
          return bus.publish(event as AnyEvent);
        };
      },
    },
  );
}

export function useBus(): EventBusPublisher;
export function useBus<Topic extends Exclude<Topics, 'statebus'>>(topic: Topic): EventTopicPublisher<Topic>;
export function useBus(topic?: Topics) {
  const bus = useStateBus();
  return topic
    ? eventPublisher(bus, topic)
    : new Proxy(
        {},
        {
          get: (target, prop) => {
            if (typeof prop === 'symbol') return Reflect.get(target, prop);
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Proxy property names are the runtime topic keys for the bus.
            return eventPublisher(bus, prop as Topics);
          },
        },
      );
}
