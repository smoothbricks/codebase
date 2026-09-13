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

export interface NavigationConnectionOptions<Target, Location> {
  readonly channel: NavigationChannel<Target, Location>;
  readonly driver: NavigationDriver<Target, Location>;
  /** Programmer/adapter failures, not normal navigation outcomes. */
  readonly onError?: (cause: unknown) => void;
  /** The composed runtime can track actual asynchronous driver completion. */
  readonly trackExecution?: (task: Promise<void>) => void;
  /** Structural admission port; pass the host runtime's work without a platform/core dependency. */
  readonly work?: { tryAcquire(): boolean; release(): void };
}

/** Composition-owned wiring only. Admission, guards and transition decisions live in the reducer. */
export function connectNavigation<Target, Location>(
  options: NavigationConnectionOptions<Target, Location>,
): () => void {
  let disposed = false;
  let lastStarted: NavigationRequestId | undefined;
  let activeScope: AbortController | undefined;
  const { channel, driver, work } = options;
  let stopLocation: (() => void) | undefined;
  let stopRequests: (() => void) | undefined;
  function report(cause: unknown): void {
    try {
      if (options.onError) options.onError(cause);
      else console.error('StateBus navigation boundary failure', cause);
    } catch {
      // A failed observer must not convert a handled async error into an unhandled rejection.
    }
  }
  function release(stop: (() => void) | undefined): void {
    try {
      stop?.();
    } catch (cause) {
      report(cause);
    }
  }
  function abort(): void {
    const scope = activeScope;
    activeScope = undefined;
    scope?.abort();
  }
  function dispose(): void {
    if (disposed) return;
    disposed = true;
    abort();
    release(stopRequests);
    release(stopLocation);
    stopRequests = undefined;
    stopLocation = undefined;
  }
  function execute(request: NavigationRequest<Target>, signal: AbortSignal): void {
    const finish = (outcome: NavigationOutcome) => {
      work?.release();
      if (disposed || signal.aborted || lastStarted !== request.requestId) return;
      try {
        channel.publish(
          outcome.kind === 'dispatched'
            ? { type: 'navigationDispatched', requestId: request.requestId }
            : { type: 'navigationFailed', requestId: request.requestId, error: outcome.error },
        );
      } catch (cause) {
        report(cause);
      }
    };
    const failed = () => finish(DRIVER_FAILED);
    let outcome: NavigationOutcome | Promise<NavigationOutcome>;
    try {
      outcome = driver.execute(request, { signal });
    } catch {
      failed();
      return;
    }
    // Synchronous history writes still need no Promise to acknowledge a write.
    if ('kind' in outcome) finish(outcome);
    else {
      const task = outcome.then(finish, failed);
      // Observe first, then expose completion to a caller hook that itself may throw.
      void task.catch(report);
      try {
        options.trackExecution?.(task);
      } catch (cause) {
        report(cause);
      }
    }
  }
  try {
    const initialLocation = driver.current();
    stopLocation = driver.subscribe((observation) => {
      if (!disposed) {
        try {
          channel.publish({ type: 'locationObserved', ...observation });
        } catch (cause) {
          report(cause);
        }
      }
    });
    stopRequests = channel.subscribe((event) => {
      if (disposed) return;
      const state = channel.read();
      const operation = state.operation;
      const observedRequest = lastStarted;
      if (
        activeScope &&
        ((operation.kind !== 'requested' && operation.kind !== 'dispatched') ||
          operation.request.requestId !== lastStarted)
      )
        abort();
      // Abort observers can synchronously submit newer navigation through a host port.
      if (disposed || lastStarted !== observedRequest) return;
      if (event.type !== 'navigationRequested' && event.type !== 'navigationConfirmed') return;
      const id = event.type === 'navigationRequested' ? event.request.requestId : event.requestId;
      const request = admittedNavigation(state, id);
      if (!request || lastStarted === id) return;
      lastStarted = id;
      abort();
      if (disposed || lastStarted !== id) return;
      // Reserve before creating the controller or invoking a native/custom driver. Aborting
      // a previous request did not release its slot: it may ignore cancellation and finish later.
      if (work && !work.tryAcquire()) {
        channel.publish({ type: 'navigationFailed', requestId: id, error: CAPACITY_EXHAUSTED });
        return;
      }
      activeScope = new AbortController();
      execute(request, activeScope.signal);
    });
    channel.publish({ type: 'locationObserved', source: 'initial', location: initialLocation });
  } catch (cause) {
    dispose();
    throw cause;
  }
  return dispose;
}

const CAPACITY_EXHAUSTED: NavigationFailure = Object.freeze({
  code: 'capacity',
  message: 'The runtime work capacity is exhausted.',
});
const DRIVER_FAILED: NavigationOutcome = Object.freeze({
  kind: 'failed',
  error: Object.freeze({ code: 'driver-failed', message: 'The navigation driver failed.' }),
});
export const NAVIGATION_CANCELLED: NavigationOutcome = Object.freeze({
  kind: 'failed',
  error: Object.freeze({ code: 'cancelled', message: 'Navigation was cancelled.' }),
});
