import {
  type ComposedRuntime,
  captureViewProps,
  type EventHandle,
  type ExternalStore,
  type KeyedHandle,
  type LibraryDefinition,
  type MountedLibrary,
  type ReadableHandle,
  type StateBusComposition,
  type StateInterestHandle,
  type StateReader,
  sameViewProps,
  type ViewProps,
} from '@smoothbricks/statebus-core';
import {
  createContext,
  createElement,
  type ReactNode,
  useContext,
  useMemo,
  useState,
  useSyncExternalStore,
} from 'react';

// A context token is not a store. Every provider receives an explicitly owned runtime.
const RuntimeContext = createContext<ComposedRuntime | null>(null);
export function useComposedRuntime(): ComposedRuntime {
  const runtime = useContext(RuntimeContext);
  if (!runtime || runtime.disposed) throw new Error('A live composition-bound StateBus provider is required.');
  return runtime;
}
function useBinding<T>(binding: ExternalStore<T>): T {
  return useSyncExternalStore(binding.subscribe, binding.getSnapshot, binding.getSnapshot);
}
export function useStateValue<T>(handle: ReadableHandle<T>): T {
  const runtime = useComposedRuntime();
  return useBinding(useMemo(() => runtime.binding(handle), [runtime, handle]));
}
export function useKeyedState<T, ID extends string | number>(handle: KeyedHandle<T, ID>, id: NoInfer<ID>): T {
  const runtime = useComposedRuntime();
  return useBinding(useMemo(() => runtime.binding(handle.at(id)), [runtime, handle, id]));
}
export function useEventPublisher<T>(handle: EventHandle<T>): (payload: T) => void {
  const runtime = useComposedRuntime();
  return useMemo(() => runtime.publisher(handle), [runtime, handle]);
}

/** Bind once to an application composition, but never instantiate a global runtime. */
export function createStateBusReact(composition: StateBusComposition) {
  function Provider({ runtime, children }: { readonly runtime: ComposedRuntime; readonly children?: ReactNode }) {
    if (runtime.composition !== composition || runtime.disposed)
      throw new Error('Provider received a foreign or disposed runtime.');
    return createElement(RuntimeContext.Provider, { value: runtime }, children);
  }
  return Object.freeze({ Provider, useRuntime: useComposedRuntime, useStateValue, useKeyedState, useEventPublisher });
}

/** A reusable library connector is independent of the host application's types and provider factory. */
export function createLibraryReact<Exports>(definition: LibraryDefinition<Exports>) {
  const LibraryContext = createContext<MountedLibrary<Exports> | null>(null);
  function Provider({ mount, children }: { readonly mount: MountedLibrary<Exports>; readonly children?: ReactNode }) {
    const runtime = useComposedRuntime();
    if (mount.definition !== definition) throw new Error('Library provider received an incompatible mount.');
    runtime.assertOwner(mount.scope.ownerToken);
    return createElement(LibraryContext.Provider, { value: mount }, children);
  }
  function useLibraryMount(): MountedLibrary<Exports> {
    const runtime = useComposedRuntime();
    const mount = useContext(LibraryContext);
    if (!mount) throw new Error('A library-mount provider is required.');
    runtime.assertOwner(mount.scope.ownerToken);
    return mount;
  }
  function useLibrary(): Exports {
    return useLibraryMount().exports;
  }
  function createLibrarySelectionHook<Props extends ViewProps, Value>(
    name: string,
    select: (state: StateReader, library: Exports, props: Props) => Value,
    interests: (library: Exports, props: Props) => readonly StateInterestHandle[] = () => [],
  ): (props: Props) => Value {
    return function useLibrarySelection(props: Props): Value {
      const runtime = useComposedRuntime();
      const mount = useLibraryMount();
      return useSelectionBinding(runtime, mount, props, (captured) =>
        runtime.selection(name, (state) => select(state, mount.exports, captured), interests(mount.exports, captured)),
      );
    };
  }
  return Object.freeze({ Provider, useLibrary, createSelectionHook: createLibrarySelectionHook });
}

/** Preserve the committed binding until its runtime, mount/definition or primitive props actually change. */
function useSelectionBinding<Props extends ViewProps, Key, Value>(
  runtime: ComposedRuntime,
  key: Key,
  props: Props,
  create: (captured: Props) => ExternalStore<Value>,
): Value {
  function bind() {
    const captured = captureViewProps(props);
    return { runtime, key, props: captured, store: create(captured) };
  }
  const [binding, setBinding] = useState(bind);
  let current = binding;
  if (binding.runtime !== runtime || !Object.is(binding.key, key) || !sameViewProps(binding.props, props)) {
    current = bind();
    // Render-state adjustment, not a mutable ref: an abandoned render cannot retarget the committed store.
    setBinding(current);
  }
  return useBinding(current.store);
}

/** Props are captured only on change; unchanged bindings keep their computed signal and subscriptions. */
export function createSelectionHook<Props extends ViewProps, Value>(
  name: string,
  select: (state: StateReader, props: Props) => Value,
  interests: (props: Props) => readonly StateInterestHandle[] = () => [],
): (props: Props) => Value {
  const definition = Object.freeze({ name, select, interests });
  return function useSelection(props: Props): Value {
    const runtime = useComposedRuntime();
    return useSelectionBinding(runtime, definition, props, (captured) =>
      runtime.selection(name, (state) => select(state, captured), interests(captured)),
    );
  };
}
