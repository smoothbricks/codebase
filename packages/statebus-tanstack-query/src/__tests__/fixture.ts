import { type ByID, ManualStateBus, type StateInterest } from '@smoothbricks/statebus-core';
import {
  initialLoadState,
  type LoaderChannel,
  type LoaderEvent,
  type LoadRequest,
  type LoadState,
  reduceLoadState,
  statebusInterestSource,
  type TimerPort,
} from '@smoothbricks/statebus-data-loader';
import { QueryClient, type QueryKey } from '@tanstack/query-core';
import { installTanStackQueryLoader, type LoaderQuery, type QueryExecutionContext } from '../index.js';

export type Items = readonly string[];
export interface Failure {
  readonly code: 'query-failed';
  readonly message: string;
}
type State = LoadState<Items, Failure>;
type Event = LoaderEvent<Items, Failure>;

// Test-only augmentation exercises today's production StateBus, not a mock of its hooks or dispatch.
declare module '@smoothbricks/statebus-core' {
  interface States {
    'loaderTest.resources': ByID<State>;
  }
  interface Events {
    loaderTest: { lifecycle: Event };
  }
}

export class Clock implements TimerPort {
  now = 0;
  private readonly tasks = new Map<() => void, number>();
  after(milliseconds: number, callback: () => void): () => void {
    this.tasks.set(callback, this.now + milliseconds);
    return () => {
      this.tasks.delete(callback);
    };
  }
  advance(milliseconds: number): void {
    this.now += milliseconds;
    for (const [callback, due] of [...this.tasks])
      if (due <= this.now) {
        this.tasks.delete(callback);
        callback();
      }
  }
  get pending(): number {
    return this.tasks.size;
  }
}

export function fixture(options: {
  execute: (context: QueryExecutionContext) => Promise<Items>;
  queryKey?: (request: LoadRequest) => QueryKey;
  retry?: LoaderQuery<Items>['retry'];
  staleTime?: LoaderQuery<Items>['staleTime'];
  autoInstall?: boolean;
  automaticDemand?: boolean;
}) {
  const timer = new Clock();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Number.POSITIVE_INFINITY } },
  });
  const bus = new ManualStateBus({
    initialState: { 'loaderTest.resources': () => initialLoadState<Items, Failure>() },
    reducers: {
      loaderTest: {
        lifecycle(state, event) {
          const { interest } = event.request;
          if (interest.key !== 'loaderTest.resources' || interest.id === undefined) return;
          state['loaderTest.resources']
            .get(interest.id)
            .update((previous) => reduceLoadState(previous ?? initialLoadState(), event));
        },
      },
    },
  });
  const events: Event[] = [];
  bus.subscribe('loaderTest', 'lifecycle', (event) => events.push(event.payload));
  const channel: LoaderChannel<Items, Failure> = {
    publish: (event) => {
      bus.publish({ topic: 'loaderTest', type: 'lifecycle', payload: event });
    },
    subscribe: (listener) => bus.subscribe('loaderTest', 'lifecycle', (event) => listener(event.payload)),
    read: (interest) => {
      if (interest.key !== 'loaderTest.resources' || interest.id === undefined)
        throw new Error('Invalid test resource address.');
      return bus.state['loaderTest.resources'].get(interest.id).get() ?? initialLoadState();
    },
  };
  let requestCount = 0;
  let disposeProvider: (() => void) | undefined;
  function install() {
    if (disposeProvider) throw new Error('Test provider already installed.');
    disposeProvider = installTanStackQueryLoader({
      channel,
      queryClient,
      interests: statebusInterestSource(bus),
      matches: (interest) => interest.key === 'loaderTest.resources' && interest.id !== undefined,
      query: (request) => ({
        queryKey: options.queryKey?.(request) ?? ['loader', typeof request.interest.id, request.interest.id],
        execute: options.execute,
        retry: options.retry ?? false,
        retryDelay: 0,
        staleTime: options.staleTime ?? 0,
      }),
      demand: options.automaticDemand === false ? () => false : undefined,
      failure: (cause) => ({ code: 'query-failed', message: cause instanceof Error ? cause.message : String(cause) }),
      requestId: () => `request-${++requestCount}`,
      now: () => timer.now,
      timer,
      graceMs: 250,
      progressIntervalMs: 100,
    });
  }
  if (options.autoInstall !== false) install();
  function address(id: string | number): StateInterest {
    return { key: 'loaderTest.resources', id };
  }
  return {
    bus,
    queryClient,
    channel,
    timer,
    events,
    install,
    interest: (id: string | number) => bus.substateInterest([{ key: 'loaderTest.resources', id }]),
    state: (id: string | number) => channel.read(address(id)),
    async tick() {
      bus.dispatchEvents();
      await Bun.sleep(0);
      bus.dispatchEvents();
    },
    dispose() {
      disposeProvider?.();
      queryClient.clear();
    },
  };
}
