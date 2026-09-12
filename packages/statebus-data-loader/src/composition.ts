import type { ComposedRuntime, EventHandle, KeyedHandle, LibraryScope } from '@smoothbricks/statebus-core';
import { type LoaderEvent, type LoadState, reduceLoadState } from './model.js';
import type { InterestSource, LoaderChannel } from './ports.js';

/** Register the existing pure loader reducer against one owned keyed declaration. */
export function reduceComposedLoader<T, Failure, ID extends string | number>(
  scope: LibraryScope,
  state: KeyedHandle<LoadState<T, Failure>, ID>,
  event: EventHandle<LoaderEvent<T, Failure>>,
): void {
  if (state.ownerToken !== scope.ownerToken || event.ownerToken !== scope.ownerToken)
    throw new Error('Loader state and events require the same owning library.');
  scope.reduce(event, (writer, payload) => {
    // Foreign addresses are refused, never interpreted as a local resource.
    if (payload.request.interest.key !== state.metadata.key || payload.request.interest.id === undefined) return;
    const id = state.resourceId(payload.request.interest);
    writer.setKeyed(state, id, reduceLoadState(writer.readKeyed(state, id), payload));
  });
}

export interface ComposedLoaderBinding<T, Failure, ID extends string | number = string | number> {
  readonly runtime: ComposedRuntime;
  readonly channel: LoaderChannel<T, Failure>;
  readonly interests: InterestSource;
  readonly matches: (interest: import('@smoothbricks/statebus-core').StateInterest) => boolean;
  readonly resourceId: (interest: import('@smoothbricks/statebus-core').StateInterest) => ID;
}

/** Bind the existing loader and QueryClient ports; this is not an independent store or executor. */
export function composedLoaderChannel<T, Failure, ID extends string | number>(
  runtime: ComposedRuntime,
  state: KeyedHandle<LoadState<T, Failure>, ID>,
  event: EventHandle<LoaderEvent<T, Failure>>,
): ComposedLoaderBinding<T, Failure, ID> {
  if (state.ownerToken !== event.ownerToken) throw new Error('Incompatible loader state/event owners.');
  runtime.assertOwner(state.ownerToken);
  runtime.assertOwner(event.ownerToken);
  const matches = (interest: import('@smoothbricks/statebus-core').StateInterest) =>
    interest.key === state.metadata.key && interest.id !== undefined;
  return Object.freeze({
    runtime,
    resourceId: (interest: import('@smoothbricks/statebus-core').StateInterest) => state.resourceId(interest),
    matches,
    interests: runtime.interestSource,
    channel: Object.freeze({
      publish: (payload: LoaderEvent<T, Failure>) => {
        if (!runtime.disposed) runtime.publish(event, payload);
      },
      subscribe: (listener: (payload: LoaderEvent<T, Failure>) => void) =>
        runtime.listen(event, (payload, admitted) => {
          if (admitted) listener(payload);
        }),
      read: (interest: import('@smoothbricks/statebus-core').StateInterest) =>
        runtime.readKeyed(state, state.resourceId(interest)),
    }),
  });
}
