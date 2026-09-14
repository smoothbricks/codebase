import {
  type BusAccess,
  type BusAccessReference,
  type BusApiLibraries,
  type BusApiSpecification,
  type BusApi as CoreBusApi,
  createBusApi as createCoreBusApi,
  type EventHandle,
  type KeyedHandle,
  type ReadableHandle,
  type StateBusInstance,
  type StateInterestHandle,
  type StateReader,
  type ViewProps,
} from '@smoothbricks/statebus-core';
import { createContext, createElement, type ReactNode, useContext, useMemo, useSyncExternalStore } from 'react';
import { RuntimeContext, useSelectionBinding } from './composition.js';

const ApiContext = createContext<BusAccessReference | null>(null);

export type BusApiProviderProps<Exports, Libraries extends BusApiLibraries> = {
  readonly children?: ReactNode;
} & (
  | { readonly bus: StateBusInstance; readonly scope?: never }
  | { readonly scope: BusAccess<Exports, Libraries>; readonly bus?: never }
);
export interface ReactBusApi<Exports, Libraries extends BusApiLibraries> extends CoreBusApi<Exports, Libraries> {
  readonly Provider: (props: BusApiProviderProps<Exports, Libraries>) => ReactNode;
  /** Stable library-bound access, not a subscription to an application snapshot. */
  useBus(): BusAccess<Exports, Libraries>;
  useStateValue<T>(select: (model: Exports) => ReadableHandle<T>): T;
  useKeyedState<T, ID extends string | number>(select: (model: Exports) => KeyedHandle<T, ID>, id: NoInfer<ID>): T;
  useEventPublisher<T>(select: (model: Exports) => EventHandle<T>): (payload: T) => void;
  createSelectionHook<Props extends ViewProps, Value>(
    name: string,
    select: (state: StateReader, model: Exports, props: Props) => Value,
    interests?: (model: Exports, props: Props) => readonly StateInterestHandle[],
  ): (props: Props) => Value;
}

/**
 * One factory for a reusable library or an application. Hooks are bound to this API identity;
 * a host Provider resolves them to the appropriate declarations in its single existing StateBus.
 * Defining this API creates neither live state nor a process-global bus.
 */
export function createBusApi<Exports, Libraries extends BusApiLibraries = Record<never, never>>(
  specification: BusApiSpecification<Exports, Libraries>,
): ReactBusApi<Exports, Libraries> {
  const api = createCoreBusApi(specification);
  function Provider({ bus, scope, children }: BusApiProviderProps<Exports, Libraries>): ReactNode {
    const parent = useContext(ApiContext);
    const instance = bus ?? scope?.instance;
    if (!instance) throw new Error('A bus API Provider requires a bus or a selected scope.');
    if (scope && parent && scope.instance !== parent.instance)
      throw new Error("A library scope cannot select another provider's StateBus.");
    const access = api.getBus(instance, scope);
    instance.assertReady();
    return createElement(
      RuntimeContext.Provider,
      { value: instance },
      createElement(ApiContext.Provider, { value: access }, children),
    );
  }
  function useBus(): BusAccess<Exports, Libraries> {
    const context = useContext(ApiContext);
    const instance = useContext(RuntimeContext);
    if (!context || !instance || context.instance !== instance)
      throw new Error('A matching bus API Provider is required.');
    return api.getBus(instance, context);
  }
  function useStateValue<T>(select: (model: Exports) => ReadableHandle<T>): T {
    const access = useBus();
    const handle = select(access.exports);
    const instance = access.instance;
    const binding = useMemo(() => instance.binding(handle), [instance, handle]);
    return useSyncExternalStore(binding.subscribe, binding.getSnapshot, binding.getSnapshot);
  }
  function useKeyedState<T, ID extends string | number>(
    select: (model: Exports) => KeyedHandle<T, ID>,
    id: NoInfer<ID>,
  ): T {
    const access = useBus();
    const handle = select(access.exports);
    const instance = access.instance;
    const binding = useMemo(() => instance.binding(handle.at(id)), [instance, handle, id]);
    return useSyncExternalStore(binding.subscribe, binding.getSnapshot, binding.getSnapshot);
  }
  function useEventPublisher<T>(select: (model: Exports) => EventHandle<T>): (payload: T) => void {
    const access = useBus();
    const handle = select(access.exports);
    const instance = access.instance;
    return useMemo(() => instance.publisher(handle), [instance, handle]);
  }
  function createSelectionHook<Props extends ViewProps, Value>(
    name: string,
    select: (state: StateReader, model: Exports, props: Props) => Value,
    interests: (model: Exports, props: Props) => readonly StateInterestHandle[] = () => [],
  ): (props: Props) => Value {
    return function useSelection(props: Props): Value {
      const access = useBus();
      const instance = access.instance;
      return useSelectionBinding(instance, access, props, (captured) =>
        instance.selection(
          name,
          (state) => select(state, access.exports, captured),
          interests(access.exports, captured),
        ),
      );
    };
  }
  return Object.freeze({
    ...api,
    Provider,
    useBus,
    useStateValue,
    useKeyedState,
    useEventPublisher,
    createSelectionHook,
  });
}
