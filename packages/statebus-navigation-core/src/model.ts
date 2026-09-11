/** Target is deliberately generic: URL strings, typed web routes or Expo's Href can be used. */
export type NavigationIntent<Target> =
  | { readonly kind: 'navigate'; readonly to: Target }
  | { readonly kind: 'push'; readonly to: Target }
  | { readonly kind: 'replace'; readonly to: Target }
  | { readonly kind: 'back' }
  | { readonly kind: 'forward' }
  | { readonly kind: 'go'; readonly delta: number }
  | { readonly kind: 'external'; readonly href: string; readonly mode: 'assign' | 'replace' | 'new-tab' };

export interface NavigationRequest<Target> {
  readonly requestId: string;
  readonly intent: NavigationIntent<Target>;
}

/** A location observation is a fact, not a prediction made when an intent is published. */
export interface NavigationLocation {
  readonly pathname: string;
  readonly search: string;
  readonly hash: string;
}

export interface NavigationFailure {
  readonly code: 'invalid-target' | 'unsafe-protocol' | 'cross-origin' | 'unsupported' | 'driver-failed';
  readonly message: string;
}

export type NavigationOperation<Target> =
  | { readonly kind: 'idle' }
  | { readonly kind: 'requested' | 'dispatched'; readonly request: NavigationRequest<Target> }
  | { readonly kind: 'blocked'; readonly request: NavigationRequest<Target>; readonly reason: string }
  | { readonly kind: 'failed'; readonly request: NavigationRequest<Target>; readonly error: NavigationFailure };

export interface NavigationState<Target, Location> {
  readonly location: Location;
  readonly operation: NavigationOperation<Target>;
  readonly guard?: string;
  readonly lastRequestId?: string;
}

export type NavigationEvent<Target, Location> =
  | { readonly type: 'navigationRequested'; readonly request: NavigationRequest<Target> }
  | { readonly type: 'navigationConfirmed' | 'navigationCancelled'; readonly requestId: string }
  | { readonly type: 'navigationGuardChanged'; readonly reason?: string }
  | { readonly type: 'navigationDispatched'; readonly requestId: string }
  | { readonly type: 'navigationFailed'; readonly requestId: string; readonly error: NavigationFailure }
  | {
      readonly type: 'locationObserved';
      readonly location: Location;
      readonly source: 'initial' | 'intent' | 'history' | 'external';
      readonly requestId?: string;
    };

export function initialNavigationState<Target, Location>(location: Location): NavigationState<Target, Location> {
  return { location, operation: { kind: 'idle' } };
}

export function admittedNavigation<Target, Location>(
  state: NavigationState<Target, Location>,
  requestId: string,
): NavigationRequest<Target> | undefined {
  return state.operation.kind === 'requested' && state.operation.request.requestId === requestId
    ? state.operation.request
    : undefined;
}

export function reduceNavigation<Target, Location>(
  state: NavigationState<Target, Location>,
  event: NavigationEvent<Target, Location>,
): NavigationState<Target, Location> {
  switch (event.type) {
    case 'navigationRequested':
      if (event.request.requestId === state.lastRequestId) return state;
      return {
        ...state,
        lastRequestId: event.request.requestId,
        operation: state.guard
          ? { kind: 'blocked', request: event.request, reason: state.guard }
          : { kind: 'requested', request: event.request },
      };
    case 'navigationGuardChanged':
      return { ...state, guard: event.reason };
    case 'navigationConfirmed':
      if (state.operation.kind !== 'blocked' || state.operation.request.requestId !== event.requestId) return state;
      return { ...state, operation: { kind: 'requested', request: state.operation.request } };
    case 'navigationCancelled':
      if (state.operation.kind !== 'blocked' || state.operation.request.requestId !== event.requestId) return state;
      return { ...state, operation: { kind: 'idle' } };
    case 'navigationDispatched': {
      const request = admittedNavigation(state, event.requestId);
      return request ? { ...state, operation: { kind: 'dispatched', request } } : state;
    }
    case 'navigationFailed': {
      const request = admittedNavigation(state, event.requestId);
      return request ? { ...state, operation: { kind: 'failed', request, error: event.error } } : state;
    }
    case 'locationObserved':
      // Facts always update location. An older correlated fact must not acknowledge a newer pending intent.
      // Browser Back/Forward observations without a correlation ID are authoritative, even while guarded.
      return {
        ...state,
        location: event.location,
        operation:
          event.source === 'initial' || (event.requestId !== undefined && event.requestId !== state.lastRequestId)
            ? state.operation
            : { kind: 'idle' },
      };
  }
}
