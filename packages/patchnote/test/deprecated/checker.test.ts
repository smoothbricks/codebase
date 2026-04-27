/**
 * Unit tests for deprecated package checker
 * Tests npm registry deprecation detection
 */

import { afterEach, describe, expect, mock, test } from 'bun:test';
import { checkDeprecations, getDeprecationStatus } from '../../src/deprecated/checker.js';
import type { Logger } from '../../src/logger.js';
import type { PackageUpdate } from '../../src/types.js';

/** Helper: create a minimal PackageUpdate fixture */
function makeUpdate(name: string, overrides?: Partial<PackageUpdate>): PackageUpdate {
  return {
    name,
    fromVersion: '1.0.0',
    toVersion: '2.0.0',
    updateType: 'major',
    ecosystem: 'npm',
    ...overrides,
  };
}

/** Helper: create a mock logger that captures messages */
function createMockLogger(): { logger: Logger; messages: string[] } {
  const messages: string[] = [];
  const logger: Logger = {
    debug: (msg: string) => messages.push(`debug: ${msg}`),
    info: (msg: string) => messages.push(`info: ${msg}`),
    warn: (msg: string) => messages.push(`warn: ${msg}`),
    error: (msg: string) => messages.push(`error: ${msg}`),
  };
  return { logger, messages };
}

/** Create a registry response with deprecated field */
function withDeprecated(message: string) {
  return { deprecated: message };
}

/** Create a registry response without deprecated field */
function withoutDeprecated() {
  return { name: 'some-pkg', version: '1.0.0' };
}

const originalFetch = globalThis.fetch;

describe('getDeprecationStatus', () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test('returns deprecation message when registry response has deprecated field', async () => {
    const message = 'request has been deprecated, see https://github.com/request/request/issues/3142';
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withDeprecated(message)), { status: 200 })),
    ) as typeof fetch;

    const result = await getDeprecationStatus('request', '2.88.2');
    expect(result).toBe(message);
  });

  test('returns null when no deprecated field present', async () => {
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withoutDeprecated()), { status: 200 })),
    ) as typeof fetch;

    const result = await getDeprecationStatus('express', '4.18.0');
    expect(result).toBeNull();
  });

  test('returns null on fetch error (fail open)', async () => {
    globalThis.fetch = mock(() => Promise.reject(new Error('Network error'))) as typeof fetch;

    const result = await getDeprecationStatus('some-package', '1.0.0');
    expect(result).toBeNull();
  });

  test('returns null on 404 response', async () => {
    globalThis.fetch = mock(() => Promise.resolve(new Response('Not Found', { status: 404 }))) as typeof fetch;

    const result = await getDeprecationStatus('nonexistent', '0.0.0');
    expect(result).toBeNull();
  });

  test('properly encodes scoped package names in URL', async () => {
    const fetchMock = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withoutDeprecated()), { status: 200 })),
    ) as typeof fetch;
    globalThis.fetch = fetchMock;

    await getDeprecationStatus('@scope/pkg', '1.0.0');

    const calledUrl = fetchMock.mock.calls[0]![0] as string;
    expect(calledUrl).toBe('https://registry.npmjs.org/%40scope%2Fpkg/1.0.0');
  });
});

describe('checkDeprecations', () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test('sets deprecatedMessage on PackageUpdate when toVersion is deprecated', async () => {
    const deprecationMsg = 'This package has been deprecated';
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withDeprecated(deprecationMsg)), { status: 200 })),
    ) as typeof fetch;

    const updates = [makeUpdate('old-pkg')];
    await checkDeprecations(updates);

    expect(updates[0]!.deprecatedMessage).toBe(deprecationMsg);
  });

  test('does NOT set deprecatedMessage when toVersion is not deprecated', async () => {
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withoutDeprecated()), { status: 200 })),
    ) as typeof fetch;

    const updates = [makeUpdate('safe-pkg')];
    await checkDeprecations(updates);

    expect(updates[0]!.deprecatedMessage).toBeUndefined();
  });

  test('sets replacementName and replacementVersion when Renovate mapping exists', async () => {
    const deprecationMsg = 'Use @babel/eslint-parser instead';
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withDeprecated(deprecationMsg)), { status: 200 })),
    ) as typeof fetch;

    const updates = [makeUpdate('babel-eslint')];
    await checkDeprecations(updates);

    expect(updates[0]!.deprecatedMessage).toBe(deprecationMsg);
    expect(updates[0]!.replacementName).toBe('@babel/eslint-parser');
    expect(updates[0]!.replacementVersion).toBe('7.11.0');
  });

  test('skips non-npm ecosystem packages', async () => {
    const fetchMock = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withDeprecated('deprecated')), { status: 200 })),
    ) as typeof fetch;
    globalThis.fetch = fetchMock;

    const updates = [
      makeUpdate('nodejs', { ecosystem: 'nix' }),
      makeUpdate('nixpkgs-fmt', { ecosystem: 'nixpkgs' }),
      makeUpdate('expo-modules-core', { ecosystem: 'expo' }),
    ];
    await checkDeprecations(updates);

    expect(fetchMock).not.toHaveBeenCalled();
    for (const u of updates) {
      expect(u.deprecatedMessage).toBeUndefined();
    }
  });

  test('processes packages in batches respecting maxConcurrent', async () => {
    let activeFetches = 0;
    let maxConcurrentSeen = 0;

    globalThis.fetch = mock(async () => {
      activeFetches++;
      if (activeFetches > maxConcurrentSeen) maxConcurrentSeen = activeFetches;
      await new Promise((resolve) => setTimeout(resolve, 10));
      activeFetches--;
      return new Response(JSON.stringify(withoutDeprecated()), { status: 200 });
    }) as typeof fetch;

    const updates = Array.from({ length: 8 }, (_, i) => makeUpdate(`pkg-${i}`));
    await checkDeprecations(updates, 3);

    expect(maxConcurrentSeen).toBeLessThanOrEqual(3);
  });

  test('logs warning for each deprecated package found', async () => {
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withDeprecated('deprecated!')), { status: 200 })),
    ) as typeof fetch;

    const { logger, messages } = createMockLogger();
    const updates = [makeUpdate('pkg-a'), makeUpdate('pkg-b')];
    await checkDeprecations(updates, 5, logger);

    const warnings = messages.filter((m) => m.startsWith('warn:'));
    expect(warnings).toHaveLength(2);
    expect(warnings[0]).toContain('pkg-a');
    expect(warnings[1]).toContain('pkg-b');
  });
});
