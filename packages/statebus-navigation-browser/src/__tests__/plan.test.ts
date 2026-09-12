import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { createBrowserNavigation, planBrowserNavigation } from '../index.js';

const BASE = 'https://app.example/editor/pages?locale=en#content';
describe('browser navigation policy', () => {
  it('resolves relative links, encoded paths, queries and hashes without changing input', () => {
    fc.assert(
      fc.property(fc.string(), fc.string(), (path, value) => {
        const to = `/pages/x-${encodeURIComponent(path)}?filter=${encodeURIComponent(value)}#preview`;
        const intent = Object.freeze({ kind: 'push', to } as const);
        const plan = planBrowserNavigation(intent, BASE);
        expect(plan.kind).toBe('write');
        if (plan.kind !== 'write') throw new Error('Expected internal URL write.');
        const url = new URL(plan.href);
        expect(url.origin).toBe('https://app.example');
        expect(decodeURIComponent(url.pathname)).toBe(`/pages/x-${path}`);
        expect(url.searchParams.get('filter')).toBe(value);
        expect(url.hash).toBe('#preview');
        expect(intent.to).toBe(to);
      }),
      { numRuns: 500 },
    );
    expect(planBrowserNavigation({ kind: 'replace', to: '?locale=fr' }, BASE)).toEqual({
      kind: 'write',
      mode: 'replace',
      href: 'https://app.example/editor/pages?locale=fr',
    });
    expect(planBrowserNavigation({ kind: 'navigate', to: BASE }, BASE).kind).toBe('unchanged');
  });
  it('requires explicit external intent and rejects unsafe protocols and embedded credentials', () => {
    for (const to of [
      'javascript:alert(1)',
      'JaVaScRiPt:alert(1)',
      'data:text/html,hi',
      'file:///etc/passwd',
      'vbscript:foo',
    ]) {
      expect(planBrowserNavigation({ kind: 'push', to }, BASE)).toMatchObject({
        kind: 'failed',
        error: { code: 'unsafe-protocol' },
      });
      expect(planBrowserNavigation({ kind: 'external', href: to, mode: 'assign' }, BASE)).toMatchObject({
        kind: 'failed',
        error: { code: 'unsafe-protocol' },
      });
    }
    expect(planBrowserNavigation({ kind: 'push', to: '//other.example/' }, BASE)).toMatchObject({
      kind: 'failed',
      error: { code: 'cross-origin' },
    });
    expect(
      planBrowserNavigation({ kind: 'external', href: 'https://other.example/', mode: 'new-tab' }, BASE),
    ).toMatchObject({ kind: 'external' });
    expect(
      planBrowserNavigation({ kind: 'external', href: 'https://user:secret@other.example/', mode: 'assign' }, BASE),
    ).toMatchObject({ kind: 'failed', error: { code: 'invalid-target' } });
    expect(planBrowserNavigation({ kind: 'push', to: 'https://[' }, BASE).kind).toBe('failed');
  });
  it('never turns invalid history deltas into reload through WebIDL integer wrapping', () => {
    for (const delta of [
      0,
      -0,
      0.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
      2 ** 32,
      -(2 ** 32),
      2 ** 31,
      -2147483649,
    ]) {
      expect(planBrowserNavigation({ kind: 'go', delta }, BASE).kind).toBe('failed');
    }
    fc.assert(
      fc.property(
        fc.integer({ min: -2147483648, max: 2147483647 }).filter((n) => n !== 0),
        (delta) => {
          expect(planBrowserNavigation({ kind: 'go', delta }, BASE)).toEqual({ kind: 'traverse', delta });
        },
      ),
    );
  });
  it('imports safely outside a browser; a Window is required only at installation', () => {
    expect(typeof createBrowserNavigation).toBe('function');
    expect(typeof globalThis.window).toBe('undefined');
  });
});
