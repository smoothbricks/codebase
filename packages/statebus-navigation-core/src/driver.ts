import {
  admittedNavigation,
  type NavigationEvent,
  type NavigationFailure,
  type NavigationRequest,
  type NavigationRequestId,
  type NavigationState,
} from './model.js';

export interface NavigationObservation<Location> {
  readonly location: Location;
  readonly source: 'intent' | 'history' | 'external';
  readonly requestId?: NavigationRequestId;
}

export type NavigationOutcome =
  | { readonly kind: 'dispatched' }
  | { readonly kind: 'failed'; readonly error: NavigationFailure };
export const NAVIGATION_DISPATCHED: NavigationOutcome = Object.freeze({ kind: 'dispatched' });

/** The future Expo adapter implements this contract without importing browser or React DOM code. */
export interface NavigationDriver<Target, Location> {
  current(): Location;
  subscribe(listener: (observation: NavigationObservation<Location>) => void): () => void;
  execute(
    request: NavigationRequest<Target>,
    context: { readonly signal: AbortSignal },
  ): NavigationOutcome | Promise<NavigationOutcome>;
}

export interface NavigationChannel<Target, Location> {
  publish(event: NavigationEvent<Target, Location>): void;
  /** Like StateBus, listeners must run against batch-final reduced state. */
  subscribe(listener: (event: NavigationEvent<Target, Location>) => void): () => void;
  read(): NavigationState<Target, Location>;
}

/** Composition-owned wiring only. Admission, guards and transition decisions live in the reducer. */
export function connectNavigation<Target, Location>(options: {
  readonly channel: NavigationChannel<Target, Location>;
  readonly driver: NavigationDriver<Target, Location>;
}): () => void {
  let disposed = false;
  let lastStarted: NavigationRequestId | undefined;
  let activeScope: AbortController | undefined;
  const { channel, driver } = options;
  // Read before installing listeners so a faulty adapter cannot leave a half-installed connection.
  const initialLocation = driver.current();
  const stopLocation = driver.subscribe((observation) => {
    if (!disposed) channel.publish({ type: 'locationObserved', ...observation });
  });
  let stopRequests: () => void;
  try {
    stopRequests = channel.subscribe((event) => {
      if (disposed) return;
      const state = channel.read();
      const operation = state.operation;
      if (
        activeScope &&
        ((operation.kind !== 'requested' && operation.kind !== 'dispatched') ||
          operation.request.requestId !== lastStarted)
      ) {
        activeScope.abort();
        activeScope = undefined;
      }
      if (event.type !== 'navigationRequested' && event.type !== 'navigationConfirmed') return;
      const id = event.type === 'navigationRequested' ? event.request.requestId : event.requestId;
      const request = admittedNavigation(state, id);
      if (!request || lastStarted === id) return;
      lastStarted = id;
      activeScope?.abort();
      activeScope = new AbortController();
      execute(request, activeScope.signal);
    });
  } catch (cause) {
    stopLocation();
    throw cause;
  }
  try {
    channel.publish({ type: 'locationObserved', source: 'initial', location: initialLocation });
  } catch (cause) {
    stopRequests();
    stopLocation();
    throw cause;
  }

  function execute(request: NavigationRequest<Target>, signal: AbortSignal): void {
    const finish = (outcome: NavigationOutcome) => {
      if (disposed || signal.aborted || lastStarted !== request.requestId) return;
      channel.publish(
        outcome.kind === 'dispatched'
          ? { type: 'navigationDispatched', requestId: request.requestId }
          : { type: 'navigationFailed', requestId: request.requestId, error: outcome.error },
      );
    };
    const failed = () => finish(DRIVER_FAILED);
    let outcome: NavigationOutcome | Promise<NavigationOutcome>;
    try {
      outcome = driver.execute(request, { signal });
    } catch {
      failed();
      return;
    }
    // Synchronous history implementations need no promise/microtask just to acknowledge a write.
    if ('kind' in outcome) finish(outcome);
    else void outcome.then(finish, failed);
  }

  return () => {
    if (disposed) return;
    disposed = true;
    activeScope?.abort();
    activeScope = undefined;
    stopRequests();
    stopLocation();
  };
}

const DRIVER_FAILED: NavigationOutcome = Object.freeze({
  kind: 'failed',
  error: Object.freeze({ code: 'driver-failed', message: 'The navigation driver failed.' }),
});
export const NAVIGATION_CANCELLED: NavigationOutcome = Object.freeze({
  kind: 'failed',
  error: Object.freeze({ code: 'cancelled', message: 'Navigation was cancelled.' }),
});
