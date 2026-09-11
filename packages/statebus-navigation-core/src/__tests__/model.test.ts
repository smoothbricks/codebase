import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import {
  admittedNavigation,
  initialNavigationState,
  type NavigationEvent,
  type NavigationRequest,
  reduceNavigation,
} from '../model.js';

const request: NavigationRequest<string> = { requestId: 'first', intent: { kind: 'push', to: '/settings/billing' } };

describe('navigation reducer properties', () => {
  it('does not predict a URL from an intent and only admits an unblocked request', () => {
    fc.assert(
      fc.property(fc.string(), fc.string(), (from, to) => {
        const initial = Object.freeze(initialNavigationState<string, string>(from));
        const target: NavigationRequest<string> = { requestId: 'go', intent: { kind: 'navigate', to } };
        const next = reduceNavigation(initial, { type: 'navigationRequested', request: target });
        expect(next.location).toBe(from);
        expect(admittedNavigation(next, 'go')).toEqual(target);
        expect(initial).toEqual({ location: from, operation: { kind: 'idle' } });
      }),
      { numRuns: 500 },
    );
  });

  it('requires the matching confirmation for guarded requests; repeated/stale events are neutral', () => {
    fc.assert(
      fc.property(fc.string({ minLength: 1 }), fc.string({ minLength: 1 }), (reason, other) => {
        fc.pre(other !== request.requestId);
        const guarded = reduceNavigation(initialNavigationState<string, string>('/editor'), {
          type: 'navigationGuardChanged',
          reason,
        });
        const blocked = Object.freeze(reduceNavigation(guarded, { type: 'navigationRequested', request }));
        expect(blocked.operation.kind).toBe('blocked');
        expect(admittedNavigation(blocked, request.requestId)).toBeUndefined();
        expect(reduceNavigation(blocked, { type: 'navigationRequested', request })).toBe(blocked);
        expect(reduceNavigation(blocked, { type: 'navigationConfirmed', requestId: other })).toBe(blocked);
        expect(reduceNavigation(blocked, { type: 'navigationCancelled', requestId: other })).toBe(blocked);
        const confirmed = reduceNavigation(blocked, { type: 'navigationConfirmed', requestId: request.requestId });
        expect(admittedNavigation(confirmed, request.requestId)).toEqual(request);
        const cancelled = reduceNavigation(blocked, { type: 'navigationCancelled', requestId: request.requestId });
        expect(cancelled.location).toBe('/editor');
        expect(cancelled.operation.kind).toBe('idle');
      }),
      { numRuns: 300 },
    );
  });

  it('refuses stale acknowledgements but accepts actual locations without acknowledging newer intent', () => {
    fc.assert(
      fc.property(fc.string(), (observed) => {
        const first = reduceNavigation(initialNavigationState<string, string>('/'), {
          type: 'navigationRequested',
          request,
        });
        const latest: NavigationRequest<string> = { requestId: 'latest', intent: { kind: 'replace', to: '/members' } };
        const pending = reduceNavigation(first, { type: 'navigationRequested', request: latest });
        expect(
          reduceNavigation(pending, {
            type: 'navigationFailed',
            requestId: 'first',
            error: { code: 'driver-failed', message: 'late' },
          }),
        ).toBe(pending);
        expect(reduceNavigation(pending, { type: 'navigationDispatched', requestId: 'first' })).toBe(pending);
        const fact = reduceNavigation(pending, {
          type: 'locationObserved',
          source: 'intent',
          requestId: 'first',
          location: observed,
        });
        expect(fact.location).toBe(observed);
        expect(admittedNavigation(fact, 'latest')).toEqual(latest);
        const history = reduceNavigation(fact, { type: 'locationObserved', source: 'history', location: '/back' });
        expect(history.location).toBe('/back');
        expect(history.operation.kind).toBe('idle');
      }),
      { numRuns: 300 },
    );
  });

  it('replays arbitrary intent/guard/fact streams deterministically without mutating the checkpoint', () => {
    fc.assert(
      fc.property(fc.array(fc.tuple(fc.nat({ max: 4 }), fc.string()), { maxLength: 100 }), (steps) => {
        const initial = Object.freeze(initialNavigationState<string, string>('/'));
        const events: NavigationEvent<string, string>[] = steps.map(([kind, text], index) => {
          switch (kind) {
            case 0:
              return {
                type: 'navigationRequested',
                request: { requestId: String(index), intent: { kind: 'push', to: text } },
              };
            case 1:
              return { type: 'navigationGuardChanged', reason: text || undefined };
            case 2:
              return { type: 'locationObserved', source: 'history', location: text };
            case 3:
              return { type: 'navigationConfirmed', requestId: String(index - 1) };
            default:
              return { type: 'navigationCancelled', requestId: String(index - 1) };
          }
        });
        const expected = events.reduce(reduceNavigation<string, string>, initial);
        const midpoint = Math.floor(events.length / 2);
        const checkpoint = Object.freeze(events.slice(0, midpoint).reduce(reduceNavigation<string, string>, initial));
        const before = JSON.stringify(checkpoint);
        expect(events.slice(midpoint).reduce(reduceNavigation<string, string>, checkpoint)).toEqual(expected);
        expect(JSON.stringify(checkpoint)).toBe(before);
        expect(initial.location).toBe('/');
      }),
      { numRuns: 500 },
    );
  });
});
