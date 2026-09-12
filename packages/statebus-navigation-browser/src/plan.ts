import type { NavigationFailure, NavigationIntent } from '@smoothbricks/statebus-navigation-core';

export type BrowserNavigationPlan =
  | { readonly kind: 'unchanged' }
  | { readonly kind: 'write'; readonly mode: 'push' | 'replace'; readonly href: string }
  | { readonly kind: 'traverse'; readonly delta: number }
  | { readonly kind: 'external'; readonly mode: 'assign' | 'replace' | 'new-tab'; readonly href: string }
  | { readonly kind: 'failed'; readonly error: NavigationFailure };

const UNCHANGED: BrowserNavigationPlan = Object.freeze({ kind: 'unchanged' });
const INVALID: BrowserNavigationPlan = Object.freeze({
  kind: 'failed',
  error: Object.freeze({ code: 'invalid-target', message: 'The navigation target is invalid.' }),
});
const UNSAFE: BrowserNavigationPlan = Object.freeze({
  kind: 'failed',
  error: Object.freeze({ code: 'unsafe-protocol', message: 'Only HTTP and HTTPS navigation is allowed.' }),
});
const CROSS_ORIGIN: BrowserNavigationPlan = Object.freeze({
  kind: 'failed',
  error: Object.freeze({ code: 'cross-origin', message: 'Use an explicit external intent for another origin.' }),
});

/** Pure URL/intent policy. No global browser access or history writes. */
export function planBrowserNavigation(intent: NavigationIntent<string>, baseHref: string): BrowserNavigationPlan {
  switch (intent.kind) {
    case 'back':
      return { kind: 'traverse', delta: -1 };
    case 'forward':
      return { kind: 'traverse', delta: 1 };
    case 'go':
      return Number.isInteger(intent.delta) &&
        intent.delta !== 0 &&
        intent.delta >= -2147483648 &&
        intent.delta <= 2147483647
        ? { kind: 'traverse', delta: intent.delta }
        : INVALID;
    default: {
      let base: URL;
      let target: URL;
      try {
        base = new URL(baseHref);
        target = new URL(intent.kind === 'external' ? intent.href : intent.to, base);
      } catch {
        return INVALID;
      }
      if (target.protocol !== 'http:' && target.protocol !== 'https:') return UNSAFE;
      if (target.username !== '' || target.password !== '') return INVALID;
      if (intent.kind === 'external') return { kind: 'external', mode: intent.mode, href: target.href };
      if (target.origin !== base.origin) return CROSS_ORIGIN;
      if (intent.kind === 'navigate' && target.href === base.href) return UNCHANGED;
      return { kind: 'write', mode: intent.kind === 'replace' ? 'replace' : 'push', href: target.href };
    }
  }
}
