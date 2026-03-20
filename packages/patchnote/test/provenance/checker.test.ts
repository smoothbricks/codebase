/**
 * Unit tests for provenance checker
 * Tests npm provenance downgrade detection
 */

import { afterEach, describe, expect, mock, test } from 'bun:test';
import type { Logger } from '../../src/logger.js';
import { checkProvenanceDowngrades, getProvenanceStatus } from '../../src/provenance/checker.js';
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

/** Create a registry response with provenance */
function withProvenance() {
  return {
    dist: {
      attestations: {
        url: 'https://registry.npmjs.org/-/npm/v1/attestations/pkg@1.0.0',
        provenance: {
          predicateType: 'https://slsa.dev/provenance/v1',
        },
      },
    },
  };
}

/** Create a registry response without provenance */
function withoutProvenance() {
  return {
    dist: {},
  };
}

const originalFetch = globalThis.fetch;

describe('getProvenanceStatus', () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test('returns true when registry response has dist.attestations.provenance', async () => {
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withProvenance()), { status: 200 })),
    ) as typeof fetch;

    const result = await getProvenanceStatus('taze', '19.3.0');
    expect(result).toBe(true);
  });

  test('returns false when registry response has no attestations field', async () => {
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withoutProvenance()), { status: 200 })),
    ) as typeof fetch;

    const result = await getProvenanceStatus('lodash', '4.17.21');
    expect(result).toBe(false);
  });

  test('returns false on fetch error (fail open)', async () => {
    globalThis.fetch = mock(() => Promise.reject(new Error('Network error'))) as typeof fetch;

    const result = await getProvenanceStatus('some-package', '1.0.0');
    expect(result).toBe(false);
  });

  test('returns false on non-OK response status', async () => {
    globalThis.fetch = mock(() => Promise.resolve(new Response('Not Found', { status: 404 }))) as typeof fetch;

    const result = await getProvenanceStatus('nonexistent', '0.0.0');
    expect(result).toBe(false);
  });

  test('properly encodes scoped package names', async () => {
    const fetchMock = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withProvenance()), { status: 200 })),
    ) as typeof fetch;
    globalThis.fetch = fetchMock;

    await getProvenanceStatus('@scope/pkg', '1.0.0');

    const calledUrl = fetchMock.mock.calls[0]![0] as string;
    expect(calledUrl).toBe('https://registry.npmjs.org/%40scope%2Fpkg/1.0.0');
  });
});

describe('checkProvenanceDowngrades', () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test('sets provenanceDowngraded=true when fromVersion has provenance and toVersion does not', async () => {
    const callMap: Record<string, object> = {
      'https://registry.npmjs.org/taze/1.0.0': withProvenance(),
      'https://registry.npmjs.org/taze/2.0.0': withoutProvenance(),
    };
    globalThis.fetch = mock((url: string) =>
      Promise.resolve(new Response(JSON.stringify(callMap[url] ?? withoutProvenance()), { status: 200 })),
    ) as typeof fetch;

    const updates = [makeUpdate('taze')];
    await checkProvenanceDowngrades(updates);

    expect(updates[0]!.provenanceDowngraded).toBe(true);
  });

  test('does NOT set provenanceDowngraded when fromVersion has no provenance', async () => {
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withoutProvenance()), { status: 200 })),
    ) as typeof fetch;

    const updates = [makeUpdate('lodash')];
    await checkProvenanceDowngrades(updates);

    expect(updates[0]!.provenanceDowngraded).toBeUndefined();
  });

  test('does NOT set provenanceDowngraded when both versions have provenance', async () => {
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withProvenance()), { status: 200 })),
    ) as typeof fetch;

    const updates = [makeUpdate('taze')];
    await checkProvenanceDowngrades(updates);

    expect(updates[0]!.provenanceDowngraded).toBeUndefined();
  });

  test('does NOT set provenanceDowngraded when neither has provenance', async () => {
    globalThis.fetch = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withoutProvenance()), { status: 200 })),
    ) as typeof fetch;

    const updates = [makeUpdate('lodash')];
    await checkProvenanceDowngrades(updates);

    expect(updates[0]!.provenanceDowngraded).toBeUndefined();
  });

  test('skips non-npm ecosystem packages', async () => {
    const fetchMock = mock(() =>
      Promise.resolve(new Response(JSON.stringify(withProvenance()), { status: 200 })),
    ) as typeof fetch;
    globalThis.fetch = fetchMock;

    const updates = [
      makeUpdate('nodejs', { ecosystem: 'nix' }),
      makeUpdate('nixpkgs-fmt', { ecosystem: 'nixpkgs' }),
      makeUpdate('expo-modules-core', { ecosystem: 'expo' }),
    ];
    await checkProvenanceDowngrades(updates);

    expect(fetchMock).not.toHaveBeenCalled();
    for (const u of updates) {
      expect(u.provenanceDowngraded).toBeUndefined();
    }
  });

  test('short-circuits: only fetches toVersion if fromVersion has provenance', async () => {
    const fetchCalls: string[] = [];
    const callMap: Record<string, object> = {
      'https://registry.npmjs.org/no-prov/1.0.0': withoutProvenance(),
      'https://registry.npmjs.org/has-prov/1.0.0': withProvenance(),
      'https://registry.npmjs.org/has-prov/2.0.0': withProvenance(),
    };
    globalThis.fetch = mock((url: string) => {
      fetchCalls.push(url);
      return Promise.resolve(new Response(JSON.stringify(callMap[url] ?? withoutProvenance()), { status: 200 }));
    }) as typeof fetch;

    const updates = [makeUpdate('no-prov'), makeUpdate('has-prov')];
    await checkProvenanceDowngrades(updates);

    // no-prov: only 1 fetch (fromVersion), has-prov: 2 fetches (from + to)
    expect(fetchCalls).toHaveLength(3);
    expect(fetchCalls).toContain('https://registry.npmjs.org/no-prov/1.0.0');
    expect(fetchCalls).not.toContain('https://registry.npmjs.org/no-prov/2.0.0');
    expect(fetchCalls).toContain('https://registry.npmjs.org/has-prov/1.0.0');
    expect(fetchCalls).toContain('https://registry.npmjs.org/has-prov/2.0.0');
  });

  test('processes packages in batches respecting maxConcurrent', async () => {
    // Track concurrent fetch calls
    let activeFetches = 0;
    let maxConcurrentSeen = 0;

    globalThis.fetch = mock(async () => {
      activeFetches++;
      if (activeFetches > maxConcurrentSeen) maxConcurrentSeen = activeFetches;
      // Simulate network delay
      await new Promise((resolve) => setTimeout(resolve, 10));
      activeFetches--;
      return new Response(JSON.stringify(withoutProvenance()), { status: 200 });
    }) as typeof fetch;

    // Create 8 updates -- with maxConcurrent=3, should batch
    const updates = Array.from({ length: 8 }, (_, i) => makeUpdate(`pkg-${i}`));
    await checkProvenanceDowngrades(updates, 3);

    // Since fromVersion has no provenance, only 1 fetch per package (8 total)
    // With maxConcurrent=3, max concurrent should be <= 3
    expect(maxConcurrentSeen).toBeLessThanOrEqual(3);
  });

  test('logs warning for each downgrade detected', async () => {
    const callMap: Record<string, object> = {
      'https://registry.npmjs.org/pkg-a/1.0.0': withProvenance(),
      'https://registry.npmjs.org/pkg-a/2.0.0': withoutProvenance(),
      'https://registry.npmjs.org/pkg-b/1.0.0': withProvenance(),
      'https://registry.npmjs.org/pkg-b/2.0.0': withoutProvenance(),
    };
    globalThis.fetch = mock((url: string) =>
      Promise.resolve(new Response(JSON.stringify(callMap[url] ?? withoutProvenance()), { status: 200 })),
    ) as typeof fetch;

    const { logger, messages } = createMockLogger();
    const updates = [makeUpdate('pkg-a'), makeUpdate('pkg-b')];
    await checkProvenanceDowngrades(updates, 5, logger);

    const warnings = messages.filter((m) => m.startsWith('warn:'));
    expect(warnings).toHaveLength(2);
    expect(warnings[0]).toContain('pkg-a');
    expect(warnings[1]).toContain('pkg-b');
  });
});
