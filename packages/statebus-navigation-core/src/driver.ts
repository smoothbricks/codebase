import {
  admittedNavigation,
  type NavigationEvent,
  type NavigationFailure,
  type NavigationRequest,
  type NavigationState,
} from './model.js';

export interface NavigationObservation<Location> {
  readonly location: Location;
  readonly source: 'intent' | 'history' | 'external';
  readonly requestId?: string;
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
  let lastStarted: string | undefined;
  let activeScope: AbortController | undefined;
  const { channel, driver } = options;
  const stopLocation = driver.subscribe((observation) => {
    if (!disposed) channel.publish({ type: 'locationObserved', ...observation });
  });
  const stopRequests = channel.subscribe((event) => {
    if (disposed || (event.type !== 'navigationRequested' && event.type !== 'navigationConfirmed')) return;
    const id = event.type === 'navigationRequested' ? event.request.requestId : event.requestId;
    const request = admittedNavigation(channel.read(), id);
    if (!request || lastStarted === id) return;
    lastStarted = id;
    // A driver may be an async router adapter. The catch also consumes rejected navigation promises.
    activeScope?.abort();
    activeScope = new AbortController();
    void execute(request, activeScope.signal);
  });
  channel.publish({ type: 'locationObserved', source: 'initial', location: driver.current() });

  async function execute(request: NavigationRequest<Target>, signal: AbortSignal): Promise<void> {
    let outcome: NavigationOutcome;
    try {
      outcome = await driver.execute(request, { signal });
    } catch {
      outcome = { kind: 'failed', error: { code: 'driver-failed', message: 'The navigation driver failed.' } };
    }
    if (disposed || signal.aborted || lastStarted !== request.requestId) return;
    channel.publish(
      outcome.kind === 'dispatched'
        ? { type: 'navigationDispatched', requestId: request.requestId }
        : { type: 'navigationFailed', requestId: request.requestId, error: outcome.error },
    );
  }

  return () => {
    if (disposed) return;
    disposed = true;
    activeScope?.abort();
    stopRequests();
    stopLocation();
  };
}
