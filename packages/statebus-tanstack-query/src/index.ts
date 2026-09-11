import { type StateInterest, type StateInterestChange, stateInterestKey } from '@smoothbricks/statebus-core';
import {
  type ByteSample,
  createByteProgressReporter,
  type InterestSource,
  isLoadAccepted,
  type LoaderChannel,
  type LoaderEvent,
  type LoadRequest,
  type LoadState,
  needsLoad,
  sameLoadRequest,
  systemTimer,
  type TimerPort,
} from '@smoothbricks/statebus-data-loader';
import { type FetchQueryOptions, hashKey, type QueryClient, type QueryKey, QueryObserver } from '@tanstack/query-core';

export interface QueryExecutionContext {
  readonly signal: AbortSignal;
  readonly request: LoadRequest;
  readonly attempt: number;
  /** Measurements must come from the actual transport, not a guessed percentage. */
  reportBytes(sample: ByteSample): void;
}

export type LoaderQuery<T> = Pick<
  FetchQueryOptions<T, Error, T, QueryKey>,
  'queryKey' | 'staleTime' | 'gcTime' | 'retry' | 'retryDelay' | 'networkMode'
> & {
  /** Supply the existing boundary Op here; this helper does not introduce another Op/DI system. */
  execute(context: QueryExecutionContext): Promise<T>;
};

export interface TanStackLoaderOptions<T, Failure> {
  readonly queryClient: QueryClient;
  readonly channel: LoaderChannel<T, Failure>;
  readonly interests: InterestSource;
  /** Use the resolved CMS/SaaS state namespace; unrelated interest is ignored. */
  readonly matches: (interest: StateInterest) => boolean;
  readonly query: (request: LoadRequest) => LoaderQuery<T>;
  readonly failure: (cause: unknown) => Failure;
  readonly requestId: () => string;
  readonly now: () => number;
  readonly fingerprint?: (interest: StateInterest) => string;
  readonly staleAfterMs?: number;
  readonly graceMs?: number;
  readonly progressIntervalMs?: number;
  readonly timer?: TimerPort;
  readonly demand?: (input: {
    readonly state: LoadState<T, Failure>;
    readonly subscribers: number;
    readonly previousSubscribers: number;
    readonly now: number;
    readonly staleAfterMs: number;
  }) => boolean;
}

interface QueryJob<T> {
  readonly request: LoadRequest;
  readonly address: string;
  readonly group: QueryGroup<T>;
  closed: boolean;
  stopObservation: () => void;
}

interface QueryGroup<T> {
  readonly jobs: Set<QueryJob<T>>;
  attempt: number;
}

/**
 * Interest -> request event -> reducer admission -> QueryClient -> result events.
 * Each instance belongs to one runtime. The caller owns and disposes the QueryClient.
 */
export function installTanStackQueryLoader<T, Failure>(options: TanStackLoaderOptions<T, Failure>): () => void {
  const timer = options.timer ?? systemTimer;
  const graceMs = options.graceMs ?? 250;
  const progressIntervalMs = options.progressIntervalMs ?? 100;
  const staleAfterMs = options.staleAfterMs ?? 30_000;
  if (!Number.isFinite(graceMs) || graceMs < 0) throw new RangeError('graceMs must be finite and nonnegative.');
  if (!Number.isFinite(progressIntervalMs) || progressIntervalMs <= 0)
    throw new RangeError('progressIntervalMs must be finite and positive.');
  if (Number.isNaN(staleAfterMs) || staleAfterMs < 0) throw new RangeError('staleAfterMs must be nonnegative.');
  const { channel, queryClient } = options;
  const demand = options.demand ?? needsLoad;
  const jobs = new Map<string, QueryJob<T>>();
  const groups = new Map<string, QueryGroup<T>>();
  const counts = new Map<string, number>();
  const pendingRelease = new Map<string, () => void>();
  let disposed = false;

  function current(job: QueryJob<T>): boolean {
    return (
      !disposed &&
      !job.closed &&
      jobs.get(job.address) === job &&
      isLoadAccepted(channel.read(job.request.interest), job.request)
    );
  }

  function clearRelease(address: string): void {
    pendingRelease.get(address)?.();
    pendingRelease.delete(address);
  }

  function close(job: QueryJob<T>, reason?: 'unobserved' | 'superseded'): void {
    if (job.closed) return;
    job.closed = true;
    job.group.jobs.delete(job);
    job.stopObservation();
    if (jobs.get(job.address) === job) {
      clearRelease(job.address);
      jobs.delete(job.address);
    }
    if (reason && !disposed) channel.publish({ type: 'loadCancelled', request: job.request, reason });
  }

  function releaseLater(address: string): void {
    if (pendingRelease.has(address) || !jobs.has(address)) return;
    pendingRelease.set(
      address,
      timer.after(graceMs, () => {
        pendingRelease.delete(address);
        if (disposed || (counts.get(address) ?? 0) !== 0) return;
        const job = jobs.get(address);
        if (job) close(job, 'unobserved');
      }),
    );
  }

  function onInterests(changes: readonly StateInterestChange[]): void {
    if (disposed) return;
    for (const { interest, subscribers } of changes) {
      if (!options.matches(interest)) continue;
      const address = stateInterestKey(interest);
      const previousSubscribers = counts.get(address) ?? 0;
      if (subscribers > 0) counts.set(address, subscribers);
      else counts.delete(address);
      if (subscribers <= 0) {
        releaseLater(address);
        continue;
      }
      clearRelease(address);
      const state = channel.read(interest);
      const now = options.now();
      // A disposed/cancelled provider may have left an accepted load with no executor.
      const orphanedLoad = previousSubscribers === 0 && state.kind === 'loading' && !jobs.has(address);
      if (!orphanedLoad && !demand({ state, subscribers, previousSubscribers, now, staleAfterMs })) continue;
      const request: LoadRequest = {
        interest,
        requestId: options.requestId(),
        fingerprint: options.fingerprint?.(interest) ?? address,
        at: now,
        reason: 'interest',
        policy: 'latest-wins',
      };
      channel.publish({ type: 'loadRequested', request });
    }
  }

  function emitGroup(group: QueryGroup<T>, build: (request: LoadRequest) => LoaderEvent<T, Failure>): void {
    for (const job of group.jobs) {
      if (current(job)) channel.publish(build(job.request));
    }
  }

  async function start(request: LoadRequest): Promise<void> {
    const address = stateInterestKey(request.interest);
    const previous = jobs.get(address);
    if (previous?.request.requestId === request.requestId) return;
    let job: QueryJob<T> | undefined;
    let queryHash: string | undefined;
    try {
      const definition = options.query(request);
      queryHash = hashKey(definition.queryKey);
      let group = groups.get(queryHash);
      if (!group) {
        group = { jobs: new Set(), attempt: 0 };
        groups.set(queryHash, group);
      }
      const executionGroup = group;
      const { execute, ...queryOptions } = definition;
      // A disabled observer pins only this query while an admitted load is in flight.
      // Removing the LAST observer cancels signal-consuming work; external observers remain safe.
      const observer = new QueryObserver<T, Error, T, T, QueryKey>(queryClient, {
        ...queryOptions,
        enabled: false,
      });
      const unsubscribe = observer.subscribe(() => {});
      job = {
        request,
        address,
        group,
        closed: false,
        stopObservation() {
          unsubscribe();
          const query = observer.getCurrentQuery();
          // A paused/offline query may not have consumed its AbortSignal yet. Releasing its final
          // observer must cancel that pending retryer as well, not wait indefinitely for reconnect.
          if (query.getObserversCount() === 0) void query.cancel({ revert: true });
        },
      };
      jobs.set(address, job);
      group.jobs.add(job);
      if (group.attempt > 0) channel.publish({ type: 'loadAttemptStarted', request, attempt: group.attempt });
      // Subscribe the replacement BEFORE releasing the old job, so a shared request is not transiently aborted.
      if (previous) close(previous, 'superseded');
      // Interest can disappear before the request event reaches its post-reducer listener.
      if ((counts.get(address) ?? 0) === 0) releaseLater(address);
      const value = await queryClient.fetchQuery({
        ...queryOptions,
        // StateBus decides when to ask; QueryClient may fulfill that request from its bounded execution cache.
        queryFn: async ({ signal }) => {
          const attempt = ++executionGroup.attempt;
          emitGroup(executionGroup, (accepted) => ({ type: 'loadAttemptStarted', request: accepted, attempt }));
          const progress = createByteProgressReporter({
            timer,
            intervalMs: progressIntervalMs,
            emit: (sample) =>
              emitGroup(executionGroup, (accepted) => ({
                type: 'loadProgressed',
                request: accepted,
                attempt,
                sample,
                at: options.now(),
              })),
          });
          const abort = () => progress.dispose();
          signal.addEventListener('abort', abort, { once: true });
          try {
            if (signal.aborted) throw signal.reason;
            return await execute({ signal, request, attempt, reportBytes: progress.report });
          } finally {
            // Publish the last measured bytes before success/failure, even when no interval has elapsed.
            if (!signal.aborted) progress.flush();
            progress.dispose();
            signal.removeEventListener('abort', abort);
          }
        },
      });
      if (current(job)) channel.publish({ type: 'loadSucceeded', request, value, at: options.now() });
    } catch (cause) {
      if (!job && previous) close(previous, 'superseded');
      if (!disposed && (!job || current(job)) && isLoadAccepted(channel.read(request.interest), request)) {
        channel.publish({ type: 'loadFailed', request, error: options.failure(cause) });
      }
    } finally {
      if (job) close(job);
      // Keep a shared group's byte fanout alive until its last admitted consumer settles.
      if (queryHash !== undefined && groups.get(queryHash)?.jobs.size === 0) groups.delete(queryHash);
    }
  }

  const stopRequests = channel.subscribe((event) => {
    if (disposed || !options.matches(event.request.interest)) return;
    if (event.type === 'loadCancelled') {
      const job = jobs.get(stateInterestKey(event.request.interest));
      const state = channel.read(event.request.interest);
      if (
        job &&
        state.kind === 'cancelled' &&
        sameLoadRequest(state.request, event.request) &&
        sameLoadRequest(job.request, event.request)
      )
        close(job);
    } else if (event.type === 'loadRequested' && isLoadAccepted(channel.read(event.request.interest), event.request)) {
      void start(event.request);
    }
  });
  const stopInterest = options.interests.subscribe(onInterests);
  // Late installation must see already-mounted screens, not wait for an unrelated remount.
  onInterests(options.interests.snapshot());
  return () => {
    if (disposed) return;
    disposed = true;
    stopInterest();
    stopRequests();
    for (const cancel of pendingRelease.values()) cancel();
    pendingRelease.clear();
    for (const job of jobs.values()) close(job);
    jobs.clear();
    groups.clear();
    counts.clear();
  };
}
