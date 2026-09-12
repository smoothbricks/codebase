import {
  captureViewProps, sameViewProps,
  type ComposedStateBus, type EventHandle, type ExternalSubscription, type KeyedHandle,
  type LibraryDefinition, type LibraryMount, type LibraryScope, type LibraryState, type ResourceId,
  type ScalarHandle, type StateBusComposition, type StateInterest, type ViewProps,
} from '@smoothbricks/statebus-core';
import { createContext, createElement, useContext, useMemo, useState, useSyncExternalStore, type ReactNode } from 'react';

export interface StateBusSelection<Props extends ViewProps, Value> {
  readonly name: string;
  readonly read: (state: LibraryState, props: Props) => Value;
  readonly interests: (scope: LibraryScope, props: Props) => readonly StateInterest[];
  readonly isEqual?: (left: Value, right: Value) => boolean;
}
export function defineSelection<Props extends ViewProps, Value>(selection: StateBusSelection<Props, Value>): StateBusSelection<Props, Value> {
  return Object.freeze(selection);
}
function useSubscription<T>(subscription: ExternalSubscription<T>): T {
  return useSyncExternalStore(subscription.subscribe, subscription.getSnapshot, subscription.getSnapshot);
}

/** Define once next to a public library. A host supplies its scope, never an independent nested store. */
export function createLibraryReact<L extends LibraryDefinition>(library: L) {
  const Context = createContext<LibraryScope | null>(null);
  function useScope(): LibraryScope {
    const scope = useContext(Context);
    if (!scope) throw new Error('Library hooks require their library Provider.');
    return scope;
  }
  return {
    Provider({ scope, children }: { readonly scope: LibraryScope; readonly children?: ReactNode }) {
      if (scope.mount.library !== library) throw new Error('Incompatible library Provider scope.');
      scope.runtime.assertActive();
      return createElement(Context.Provider, { value: scope }, children);
    },
    useScope,
    useScalar<T>(handle: ScalarHandle<T>): T {
      const scope = useScope();
      return useSubscription(useMemo(() => scope.observe(handle), [scope, handle]));
    },
    useKeyed<ID extends ResourceId, T>(handle: KeyedHandle<ID, T>, id: NoInfer<ID>): T {
      const scope = useScope();
      return useSubscription(useMemo(() => scope.observeKeyed(handle, id), [scope, handle, id]));
    },
    usePublisher<E>(event: EventHandle<E>): (payload: E) => void {
      return useScope().publisher(event);
    },
    useSelection<Props extends ViewProps, Value>(selection: StateBusSelection<Props, Value>, props: Props): Value {
      const scope = useScope();
      const [captured, setCaptured] = useState(() => captureViewProps(props));
      if (!sameViewProps(captured, props)) setCaptured(captureViewProps(props));
      const subscription = useMemo(() => scope.select(selection.name, (state) => selection.read(state, captured), selection.interests(scope, captured), selection.isEqual), [scope, selection, captured]);
      return useSubscription(subscription);
    },
  };
}

/** A runtime is supplied by its application/test owner and explicitly disposed by that owner. */
export function createCompositionReact(composition: StateBusComposition) {
  const Context = createContext<ComposedStateBus | null>(null);
  function useRuntime(): ComposedStateBus {
    const runtime = useContext(Context);
    if (!runtime) throw new Error('Composition hooks require their composition Provider.');
    return runtime;
  }
  return {
    Provider({ runtime, children }: { readonly runtime: ComposedStateBus; readonly children?: ReactNode }) {
      if (runtime.composition !== composition) throw new Error('Incompatible composition Provider runtime.');
      runtime.assertActive();
      return createElement(Context.Provider, { value: runtime }, children);
    },
    useRuntime,
    useScope(mount: LibraryMount): LibraryScope { return useRuntime().scope(mount); },
  };
}
