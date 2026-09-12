import type { StateInterest } from '@smoothbricks/statebus-core';
import type { LoadFingerprint, LoadRequestId } from './identifiers.js';

export interface Loaded<T> {
  readonly value: T;
  readonly receivedAt: number;
}

export interface LoadRequest {
  readonly interest: StateInterest;
  readonly requestId: LoadRequestId;
  readonly fingerprint: LoadFingerprint;
  readonly at: number;
  readonly reason: 'interest' | 'refresh' | 'retry';
  readonly policy: 'drop-duplicate' | 'latest-wins';
}

export interface ByteSample {
  readonly direction: 'upload' | 'download';
  readonly transferred: number;
  readonly total?: number;
}

export interface ByteCount {
  readonly transferred: number;
  readonly total?: number;
}

export interface LoadProgress {
  readonly attempt: number;
  readonly upload?: ByteCount;
  readonly download?: ByteCount;
}

export type LoadState<T, Failure> =
  | { readonly kind: 'not-requested' }
  | {
      readonly kind: 'loading';
      readonly request: LoadRequest;
      readonly previous?: Loaded<T>;
      readonly progress: LoadProgress;
    }
  | { readonly kind: 'ready'; readonly request: LoadRequest; readonly data: Loaded<T> }
  | { readonly kind: 'failed'; readonly request: LoadRequest; readonly error: Failure; readonly previous?: Loaded<T> }
  | { readonly kind: 'cancelled'; readonly request: LoadRequest; readonly previous?: Loaded<T> };

export type LoaderEvent<T, Failure> =
  | { readonly type: 'loadRequested'; readonly request: LoadRequest }
  | {
      readonly type: 'loadAttemptStarted';
      readonly request: LoadRequest;
      readonly attempt: number;
    }
  | {
      readonly type: 'loadProgressed';
      readonly request: LoadRequest;
      readonly attempt: number;
      readonly sample: ByteSample;
      readonly at: number;
    }
  | { readonly type: 'loadSucceeded'; readonly request: LoadRequest; readonly value: T; readonly at: number }
  | { readonly type: 'loadFailed'; readonly request: LoadRequest; readonly error: Failure }
  | {
      readonly type: 'loadCancelled';
      readonly request: LoadRequest;
      readonly reason: 'unobserved' | 'superseded' | 'cancelled';
    };

const NOT_REQUESTED = Object.freeze({ kind: 'not-requested' } as const);
export function initialLoadState<T, Failure>(): LoadState<T, Failure> {
  return NOT_REQUESTED;
}

export function previousData<T, Failure>(state: LoadState<T, Failure>): Loaded<T> | undefined {
  if (state.kind === 'not-requested') return undefined;
  return state.kind === 'ready' ? state.data : state.previous;
}

export function admitLoadRequest<T, Failure>(
  state: LoadState<T, Failure>,
  request: LoadRequest,
): 'accepted' | 'duplicate' | 'request-id-conflict' | 'different-resource' {
  if (state.kind !== 'not-requested' && !sameResource(state.request, request)) return 'different-resource';
  if (state.kind !== 'not-requested' && state.request.requestId === request.requestId) {
    return state.request.fingerprint === request.fingerprint ? 'duplicate' : 'request-id-conflict';
  }
  if (
    state.kind === 'loading' &&
    request.policy === 'drop-duplicate' &&
    state.request.fingerprint === request.fingerprint
  )
    return 'duplicate';
  return 'accepted';
}

function sameResource(left: LoadRequest, right: LoadRequest): boolean {
  return left.interest.key === right.interest.key && left.interest.id === right.interest.id;
}

export function sameLoadRequest(left: LoadRequest, right: LoadRequest): boolean {
  return sameResource(left, right) && left.requestId === right.requestId && left.fingerprint === right.fingerprint;
}

export function isLoadAccepted<T, Failure>(state: LoadState<T, Failure>, request: LoadRequest): boolean {
  return state.kind === 'loading' && sameLoadRequest(state.request, request);
}

/** Unknown or incomparable totals remain unknown, never a guessed percentage. */
export function comparableByteTotal(transferred: number, total: number | undefined): number | undefined {
  return total !== undefined && Number.isSafeInteger(total) && total >= transferred ? total : undefined;
}

/** Progress is monotone within one attempt/direction; a retry starts a new byte count. */
export function reduceByteProgress(progress: LoadProgress, attempt: number, sample: ByteSample): LoadProgress {
  if (!Number.isSafeInteger(attempt) || attempt < 1 || attempt < progress.attempt) return progress;
  if (!Number.isSafeInteger(sample.transferred) || sample.transferred < 0) return progress;
  const sameAttempt = attempt === progress.attempt;
  const previous = sameAttempt ? progress[sample.direction] : undefined;
  if (previous && sample.transferred < previous.transferred) return progress;
  const total = comparableByteTotal(sample.transferred, sample.total);
  if (previous && previous.transferred === sample.transferred && previous.total === total) return progress;
  const bytes: ByteCount = { transferred: sample.transferred, total };
  // Fixed field order and shape; no transient base object or dynamically shaped spread.
  return {
    attempt,
    upload: sample.direction === 'upload' ? bytes : sameAttempt ? progress.upload : undefined,
    download: sample.direction === 'download' ? bytes : sameAttempt ? progress.download : undefined,
  };
}

/** Reduce a single address only. A composed reducer selects the exact entry from event.request.interest. */
export function reduceLoadState<T, Failure>(
  state: LoadState<T, Failure>,
  event: LoaderEvent<T, Failure>,
): LoadState<T, Failure> {
  if (event.type === 'loadRequested') {
    if (admitLoadRequest(state, event.request) !== 'accepted') return state;
    return { kind: 'loading', request: event.request, previous: previousData(state), progress: { attempt: 0 } };
  }
  if (state.kind !== 'loading' || !isLoadAccepted(state, event.request)) return state;
  switch (event.type) {
    case 'loadAttemptStarted':
      if (!Number.isSafeInteger(event.attempt) || event.attempt <= state.progress.attempt) return state;
      return { ...state, progress: { attempt: event.attempt } };
    case 'loadProgressed': {
      const progress = reduceByteProgress(state.progress, event.attempt, event.sample);
      return progress === state.progress ? state : { ...state, progress };
    }
    case 'loadSucceeded':
      return { kind: 'ready', request: state.request, data: { value: event.value, receivedAt: event.at } };
    case 'loadFailed':
      return { kind: 'failed', request: state.request, error: event.error, previous: state.previous };
    case 'loadCancelled':
      return { kind: 'cancelled', request: state.request, previous: state.previous };
  }
}

/** All inputs, including time, are supplied by the provider; QueryClient is not a state authority. */
export function needsLoad<T, Failure>(input: {
  readonly state: LoadState<T, Failure>;
  readonly subscribers: number;
  readonly previousSubscribers: number;
  readonly now: number;
  readonly staleAfterMs: number;
}): boolean {
  if (input.subscribers <= 0 || input.previousSubscribers > 0 || input.state.kind === 'loading') return false;
  const value = previousData(input.state);
  return input.state.kind !== 'ready' || value === undefined || input.now - value.receivedAt >= input.staleAfterMs;
}
