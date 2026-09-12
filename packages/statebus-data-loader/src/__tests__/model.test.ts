import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { type LoadFingerprint, type LoadRequestId, loadFingerprint, loadRequestId } from '../identifiers.js';
import {
  admitLoadRequest,
  initialLoadState,
  isLoadAccepted,
  type LoaderEvent,
  type LoadProgress,
  type LoadRequest,
  type LoadState,
  needsLoad,
  previousData,
  reduceByteProgress,
  reduceLoadState,
} from '../model.js';

const request: LoadRequest = {
  interest: { key: 'example.documents', id: 'home' },
  requestId: loadRequestId('r1'),
  fingerprint: loadFingerprint('home'),
  reason: 'interest',
  policy: 'latest-wins',
  at: 1,
};
const identity = fc.string({ minLength: 1 });
const failure = Object.freeze({ code: 'offline' });
type Failure = typeof failure;

function loading(command = request): LoadState<number, Failure> {
  return reduceLoadState(initialLoadState<number, Failure>(), { type: 'loadRequested', request: command });
}

describe('data lifecycle properties', () => {
  it('never accepts an older request result or progress over a newer request', () => {
    fc.assert(
      fc.property(identity, identity, fc.integer(), (oldId, newId, value) => {
        fc.pre(oldId !== newId);
        const old = { ...request, requestId: loadRequestId(oldId) };
        const latest = { ...request, requestId: loadRequestId(newId) };
        const state = Object.freeze(reduceLoadState(loading(old), { type: 'loadRequested', request: latest }));
        const stale: readonly LoaderEvent<number, Failure>[] = [
          { type: 'loadSucceeded', request: old, value, at: 10 },
          { type: 'loadFailed', request: old, error: failure },
          { type: 'loadCancelled', request: old, reason: 'unobserved' },
          { type: 'loadAttemptStarted', request: old, attempt: 2 },
          {
            type: 'loadProgressed',
            request: old,
            attempt: 1,
            sample: { direction: 'download', transferred: 5 },
            at: 4,
          },
        ];
        for (const event of stale) expect(reduceLoadState(state, event)).toBe(state);
        expect(isLoadAccepted(state, latest)).toBe(true);
      }),
      { seed: 230923, numRuns: 500 },
    );
  });

  it('preserves the first equivalent admitted request and exposes request-ID conflicts', () => {
    fc.assert(
      fc.property(identity, identity, (nextId, nextFingerprint) => {
        fc.pre(nextId !== request.requestId && nextFingerprint !== request.fingerprint);
        const state = Object.freeze(loading());
        const duplicate: LoadRequest = { ...request, requestId: loadRequestId(nextId), policy: 'drop-duplicate' };
        expect(admitLoadRequest(state, duplicate)).toBe('duplicate');
        expect(reduceLoadState(state, { type: 'loadRequested', request: duplicate })).toBe(state);
        const conflicting = { ...request, fingerprint: loadFingerprint(nextFingerprint) };
        expect(admitLoadRequest(state, conflicting)).toBe('request-id-conflict');
        expect(isLoadAccepted(state, conflicting)).toBe(false);
        expect(reduceLoadState(state, { type: 'loadRequested', request: conflicting })).toBe(state);
      }),
      { seed: 230923, numRuns: 300 },
    );
  });

  it('is neutral to other resource namespaces and string/numeric IDs', () => {
    const state = loading({ ...request, interest: { key: 'cms.content', id: 7 } });
    for (const interest of [
      { key: 'host.content', id: 7 },
      { key: 'cms.content', id: '7' },
    ]) {
      const foreign = { ...request, interest };
      expect(admitLoadRequest(state, foreign)).toBe('different-resource');
      expect(reduceLoadState(state, { type: 'loadSucceeded', request: foreign, value: 3, at: 1 })).toBe(state);
    }
  });

  it('retains previous data across failed/cancelled refresh and can replay a complete event stream', () => {
    fc.assert(
      fc.property(
        fc.integer(),
        fc.array(fc.constantFrom('succeeded', 'failed', 'cancelled'), { maxLength: 40 }),
        (value, outcomes) => {
          const events: LoaderEvent<number, Failure>[] = [
            { type: 'loadRequested', request },
            { type: 'loadSucceeded', request, value, at: 2 },
          ];
          let current = events.reduce(reduceLoadState<number, Failure>, initialLoadState<number, Failure>());
          outcomes.forEach((outcome, index) => {
            const next = { ...request, requestId: loadRequestId(`refresh-${index}`), reason: 'refresh' as const };
            const start: LoaderEvent<number, Failure> = { type: 'loadRequested', request: next };
            const result: LoaderEvent<number, Failure> =
              outcome === 'succeeded'
                ? { type: 'loadSucceeded', request: next, value, at: index + 3 }
                : outcome === 'failed'
                  ? { type: 'loadFailed', request: next, error: failure }
                  : { type: 'loadCancelled', request: next, reason: 'unobserved' };
            const before = JSON.stringify(current);
            const started = reduceLoadState(current, start);
            expect(JSON.stringify(current)).toBe(before);
            expect(previousData(started)?.value).toBe(value);
            current = reduceLoadState(started, result);
            expect(previousData(current)?.value).toBe(value);
            events.push(start, result);
          });
          expect(events.reduce(reduceLoadState<number, Failure>, initialLoadState<number, Failure>())).toEqual(current);
        },
      ),
      { seed: 230923, numRuns: 300 },
    );
  });

  it('loads on first demand/staleness, not while already requested or merely mounting another consumer', () => {
    const ready = reduceLoadState(loading(), { type: 'loadSucceeded', request, value: 3, at: 10 });
    expect(needsLoad({ state: ready, subscribers: 1, previousSubscribers: 0, now: 20, staleAfterMs: 10 })).toBe(true);
    expect(needsLoad({ state: ready, subscribers: 1, previousSubscribers: 0, now: 19, staleAfterMs: 10 })).toBe(false);
    expect(needsLoad({ state: ready, subscribers: 2, previousSubscribers: 1, now: 100, staleAfterMs: 10 })).toBe(false);
    expect(needsLoad({ state: loading(), subscribers: 1, previousSubscribers: 0, now: 100, staleAfterMs: 10 })).toBe(
      false,
    );
    expect(
      needsLoad({ state: initialLoadState(), subscribers: 0, previousSubscribers: 1, now: 100, staleAfterMs: 10 }),
    ).toBe(false);
  });
});

describe('progress properties', () => {
  it('keeps byte counts monotone per attempt and direction without fabricating totals', () => {
    fc.assert(
      fc.property(fc.array(fc.nat({ max: 1_000_000 }), { maxLength: 100 }), (samples) => {
        let progress: LoadProgress = Object.freeze({ attempt: 1 });
        let greatest = 0;
        for (const transferred of samples) {
          greatest = Math.max(greatest, transferred);
          progress = Object.freeze(reduceByteProgress(progress, 1, { direction: 'download', transferred }));
          expect(progress.download?.transferred).toBe(greatest);
          expect(progress.download?.total).toBeUndefined();
          expect(progress.upload).toBeUndefined();
        }
      }),
      { seed: 230923, numRuns: 500 },
    );
  });

  it('resets progress for a new attempt and ignores old attempts or invalid measurements', () => {
    const old = reduceByteProgress({ attempt: 1 }, 1, { direction: 'download', transferred: 100, total: 200 });
    const retried = reduceByteProgress(old, 2, { direction: 'download', transferred: 5 });
    expect(retried).toEqual({ attempt: 2, download: { transferred: 5 } });
    expect(reduceByteProgress(retried, 1, { direction: 'download', transferred: 200 })).toBe(retried);
    for (const transferred of [-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(reduceByteProgress(retried, 2, { direction: 'download', transferred })).toBe(retried);
    }
    expect(reduceByteProgress(retried, 2, { direction: 'upload', transferred: 9, total: 1 })).toEqual({
      attempt: 2,
      download: { transferred: 5 },
      upload: { transferred: 9 },
    });
  });
});

it('preserves progress and load-state identity for repeated or ineffective byte samples', () => {
  fc.assert(
    fc.property(fc.nat({ max: 1000000 }), (bytes) => {
      const sample = Object.freeze({ direction: 'download' as const, transferred: bytes });
      const once = Object.freeze(reduceByteProgress({ attempt: 1 }, 1, sample));
      expect(reduceByteProgress(once, 1, sample)).toBe(once);
      expect(reduceByteProgress(once, 1, { ...sample, total: -1 })).toBe(once);
      const event: LoaderEvent<number, Failure> = { type: 'loadProgressed', request, attempt: 1, sample, at: 2 };
      const state = Object.freeze(reduceLoadState(loading(), event));
      expect(reduceLoadState(state, event)).toBe(state);
      expect(reduceLoadState(state, { ...event, at: 99 })).toBe(state);
    }),
    { seed: 230923, numRuns: 500 },
  );
});

it('keeps logical request IDs nominally distinct from fingerprints and ordinary strings', () => {
  const id = loadRequestId('request');
  const fingerprint = loadFingerprint('operation');
  // @ts-expect-error a fingerprint cannot serve as a logical request identity
  const wrongId: LoadRequestId = fingerprint;
  // @ts-expect-error a logical request identity is not the operation fingerprint
  const wrongFingerprint: LoadFingerprint = id;
  // @ts-expect-error plain strings must be bound at the composition boundary
  const unbound: LoadRequestId = 'request';
  expect(String(wrongId)).toBe('operation');
  expect(String(wrongFingerprint)).toBe('request');
  expect(unbound).toBe(id);
  expect(() => loadRequestId('')).toThrow(RangeError);
});
