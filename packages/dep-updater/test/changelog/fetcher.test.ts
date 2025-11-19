import { beforeEach, describe, expect, mock, test } from 'bun:test';
import { fetchChangelog, fetchChangelogs, generateSimpleChangelog } from '../../src/changelog/fetcher.js';
import type { PackageUpdate } from '../../src/types.js';

describe('Changelog Fetcher', () => {
  let mockLogger: any;

  beforeEach(() => {
    mockLogger = {
      info: mock(() => {}),
      warn: mock(() => {}),
      error: mock(() => {}),
      debug: mock(() => {}),
    };
  });

  describe('generateSimpleChangelog', () => {
    test('groups updates by type', () => {
      const updates: PackageUpdate[] = [
        { name: 'react', fromVersion: '19.0.0', toVersion: '20.0.0', updateType: 'major', ecosystem: 'npm' },
        { name: 'vite', fromVersion: '7.2.0', toVersion: '7.3.0', updateType: 'minor', ecosystem: 'npm' },
        { name: 'typescript', fromVersion: '5.9.0', toVersion: '5.9.1', updateType: 'patch', ecosystem: 'npm' },
      ];

      const result = generateSimpleChangelog(updates);

      expect(result).toContain('### Major Updates');
      expect(result).toContain('react: 19.0.0 → 20.0.0');
      expect(result).toContain('### Minor Updates');
      expect(result).toContain('vite: 7.2.0 → 7.3.0');
      expect(result).toContain('### Patch Updates');
      expect(result).toContain('typescript: 5.9.0 → 5.9.1');
    });

    test('handles empty updates', () => {
      const updates: PackageUpdate[] = [];

      const result = generateSimpleChangelog(updates);

      expect(result).toBe('');
    });

    test('handles only major updates', () => {
      const updates: PackageUpdate[] = [
        { name: 'react', fromVersion: '19.0.0', toVersion: '20.0.0', updateType: 'major', ecosystem: 'npm' },
      ];

      const result = generateSimpleChangelog(updates);

      expect(result).toContain('### Major Updates');
      expect(result).not.toContain('### Minor Updates');
      expect(result).not.toContain('### Patch Updates');
    });

    test('handles unknown update types', () => {
      const updates: PackageUpdate[] = [
        { name: 'some-package', fromVersion: '1.0.0', toVersion: '2.0.0', updateType: 'unknown', ecosystem: 'npm' },
      ];

      const result = generateSimpleChangelog(updates);

      expect(result).toContain('### Unknown Updates');
      expect(result).toContain('some-package: 1.0.0 → 2.0.0');
    });

    test('handles mixed ecosystems', () => {
      const updates: PackageUpdate[] = [
        { name: 'react', fromVersion: '19.0.0', toVersion: '19.1.0', updateType: 'minor', ecosystem: 'npm' },
        { name: 'devenv', fromVersion: '1.0.0', toVersion: '1.1.0', updateType: 'minor', ecosystem: 'nix' },
      ];

      const result = generateSimpleChangelog(updates);

      expect(result).toContain('react: 19.0.0 → 19.1.0');
      expect(result).toContain('devenv: 1.0.0 → 1.1.0');
    });
  });

  describe('fetchChangelog', () => {
    test('returns null when fetch fails', async () => {
      // Mock fetch to fail
      const originalFetch = globalThis.fetch;
      globalThis.fetch = mock(() => Promise.resolve({ ok: false } as Response)) as unknown as typeof fetch;

      const update: PackageUpdate = {
        name: 'react',
        fromVersion: '19.0.0',
        toVersion: '19.1.0',
        updateType: 'minor',
        ecosystem: 'npm',
      };

      const result = await fetchChangelog(update, mockLogger);

      expect(result).toBeNull();

      globalThis.fetch = originalFetch;
    });

    test('logs info message when fetching', async () => {
      // Mock fetch to fail (so we don't make actual network requests)
      const originalFetch = globalThis.fetch;
      globalThis.fetch = mock(() => Promise.resolve({ ok: false } as Response)) as unknown as typeof fetch;

      const update: PackageUpdate = {
        name: 'react',
        fromVersion: '19.0.0',
        toVersion: '19.1.0',
        updateType: 'minor',
        ecosystem: 'npm',
      };

      await fetchChangelog(update, mockLogger);

      expect(mockLogger.info).toHaveBeenCalledWith('Fetching changelog for react...');

      globalThis.fetch = originalFetch;
    });
  });

  describe('fetchChangelogs', () => {
    test('processes updates in batches', async () => {
      // Mock fetch to fail (so we don't make actual network requests)
      const originalFetch = globalThis.fetch;
      globalThis.fetch = mock(() => Promise.resolve({ ok: false } as Response)) as unknown as typeof fetch;

      const updates: PackageUpdate[] = [
        { name: 'react', fromVersion: '19.0.0', toVersion: '19.1.0', updateType: 'minor', ecosystem: 'npm' },
        { name: 'vite', fromVersion: '7.2.0', toVersion: '7.3.0', updateType: 'minor', ecosystem: 'npm' },
        { name: 'typescript', fromVersion: '5.9.0', toVersion: '5.9.1', updateType: 'patch', ecosystem: 'npm' },
      ];

      const result = await fetchChangelogs(updates, 2, mockLogger);

      expect(result).toBeInstanceOf(Map);
      expect(result.size).toBe(0); // All fetches failed

      globalThis.fetch = originalFetch;
    });

    test('returns empty map for empty updates', async () => {
      const updates: PackageUpdate[] = [];

      const result = await fetchChangelogs(updates, 5, mockLogger);

      expect(result).toBeInstanceOf(Map);
      expect(result.size).toBe(0);
    });

    test('logs summary of fetched changelogs', async () => {
      // Mock fetch to fail
      const originalFetch = globalThis.fetch;
      globalThis.fetch = mock(() => Promise.resolve({ ok: false } as Response)) as unknown as typeof fetch;

      const updates: PackageUpdate[] = [
        { name: 'react', fromVersion: '19.0.0', toVersion: '19.1.0', updateType: 'minor', ecosystem: 'npm' },
      ];

      await fetchChangelogs(updates, 5, mockLogger);

      expect(mockLogger.info).toHaveBeenCalledWith('✓ Fetched 0 changelogs');

      globalThis.fetch = originalFetch;
    });

    test('respects maxConcurrent parameter', async () => {
      let concurrentRequests = 0;
      let maxConcurrent = 0;

      const originalFetch = globalThis.fetch;
      globalThis.fetch = mock(async () => {
        concurrentRequests++;
        maxConcurrent = Math.max(maxConcurrent, concurrentRequests);
        await new Promise((resolve) => setTimeout(resolve, 10));
        concurrentRequests--;
        return { ok: false } as Response;
      }) as unknown as typeof fetch;

      const updates: PackageUpdate[] = Array.from({ length: 10 }, (_, i) => ({
        name: `package-${i}`,
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        updateType: 'minor' as const,
        ecosystem: 'npm' as const,
      }));

      await fetchChangelogs(updates, 3, mockLogger);

      // With batch size 3, max concurrent should not exceed 3
      expect(maxConcurrent).toBeLessThanOrEqual(3);

      globalThis.fetch = originalFetch;
    });
  });

  describe('fetchChangelog - npm registry', () => {
    test('constructs GitHub release URL from npm registry data', async () => {
      const originalFetch = globalThis.fetch;
      const fetchUrls: string[] = [];

      globalThis.fetch = mock(async (url: string | Request) => {
        const urlString = typeof url === 'string' ? url : url.url;
        fetchUrls.push(urlString);

        if (urlString.includes('registry.npmjs.org')) {
          return {
            ok: true,
            json: async () => ({
              versions: {
                '19.1.0': {
                  repository: {
                    url: 'git+https://github.com/facebook/react.git',
                  },
                },
              },
            }),
          } as Response;
        }

        return { ok: false } as Response;
      }) as unknown as typeof fetch;

      const update: PackageUpdate = {
        name: 'react',
        fromVersion: '19.0.0',
        toVersion: '19.1.0',
        updateType: 'minor',
        ecosystem: 'npm',
      };

      await fetchChangelog(update, mockLogger);

      expect(fetchUrls[0]).toContain('registry.npmjs.org/react');

      globalThis.fetch = originalFetch;
    });

    test('handles npm registry errors gracefully', async () => {
      const originalFetch = globalThis.fetch;

      globalThis.fetch = mock(async () => {
        throw new Error('Network error');
      }) as unknown as typeof fetch;

      const update: PackageUpdate = {
        name: 'react',
        fromVersion: '19.0.0',
        toVersion: '19.1.0',
        updateType: 'minor',
        ecosystem: 'npm',
      };

      const result = await fetchChangelog(update, mockLogger);

      expect(result).toBeNull();

      globalThis.fetch = originalFetch;
    });
  });

  describe('fetchChangelog - GitHub releases', () => {
    test('returns null when GitHub API fails', async () => {
      const originalFetch = globalThis.fetch;

      globalThis.fetch = mock(async (url: string | Request) => {
        const urlString = typeof url === 'string' ? url : url.url;

        if (urlString.includes('registry.npmjs.org')) {
          return { ok: false } as Response;
        }

        if (urlString.includes('api.github.com')) {
          return { ok: false } as Response;
        }

        return { ok: false } as Response;
      }) as unknown as typeof fetch;

      const update: PackageUpdate = {
        name: 'react',
        fromVersion: '19.0.0',
        toVersion: '19.1.0',
        updateType: 'minor',
        ecosystem: 'npm',
      };

      const result = await fetchChangelog(update, mockLogger);

      expect(result).toBeNull();

      globalThis.fetch = originalFetch;
    });
  });

  describe('fetchChangelog - content fetching', () => {
    test('fetches content from changelog URL', async () => {
      const originalFetch = globalThis.fetch;
      let fetchCount = 0;

      globalThis.fetch = mock(async (url: string | Request) => {
        fetchCount++;
        const urlString = typeof url === 'string' ? url : url.url;

        if (urlString.includes('registry.npmjs.org')) {
          return {
            ok: true,
            json: async () => ({
              versions: {
                '19.1.0': {
                  repository: {
                    url: 'git+https://github.com/facebook/react.git',
                  },
                },
              },
            }),
          } as Response;
        }

        if (urlString.includes('github.com/facebook/react/releases/tag')) {
          return {
            ok: true,
            headers: {
              get: () => 'text/html',
            },
            text: async () => '<html>Release notes here</html>',
          } as any;
        }

        return { ok: false } as Response;
      }) as unknown as typeof fetch;

      const update: PackageUpdate = {
        name: 'react',
        fromVersion: '19.0.0',
        toVersion: '19.1.0',
        updateType: 'minor',
        ecosystem: 'npm',
      };

      const result = await fetchChangelog(update, mockLogger);

      expect(result).toBe('<html>Release notes here</html>');
      expect(fetchCount).toBeGreaterThanOrEqual(2); // npm registry + content fetch

      globalThis.fetch = originalFetch;
    });

    test('handles JSON content type', async () => {
      const originalFetch = globalThis.fetch;

      globalThis.fetch = mock(async (url: string | Request) => {
        const urlString = typeof url === 'string' ? url : url.url;

        if (urlString.includes('registry.npmjs.org')) {
          return {
            ok: true,
            json: async () => ({
              versions: {
                '19.1.0': {
                  repository: {
                    url: 'git+https://github.com/facebook/react.git',
                  },
                },
              },
            }),
          } as Response;
        }

        if (urlString.includes('github.com/facebook/react/releases/tag')) {
          return {
            ok: true,
            headers: {
              get: () => 'application/json',
            },
            json: async () => ({
              body: 'Release notes in JSON format',
              html_url: 'https://github.com/facebook/react/releases/tag/v19.1.0',
            }),
          } as any;
        }

        return { ok: false } as Response;
      }) as unknown as typeof fetch;

      const update: PackageUpdate = {
        name: 'react',
        fromVersion: '19.0.0',
        toVersion: '19.1.0',
        updateType: 'minor',
        ecosystem: 'npm',
      };

      const result = await fetchChangelog(update, mockLogger);

      expect(result).toBe('Release notes in JSON format');

      globalThis.fetch = originalFetch;
    });

    test('returns URL if content fetch fails', async () => {
      const originalFetch = globalThis.fetch;

      globalThis.fetch = mock(async (url: string | Request) => {
        const urlString = typeof url === 'string' ? url : url.url;

        if (urlString.includes('registry.npmjs.org')) {
          return {
            ok: true,
            json: async () => ({
              versions: {
                '19.1.0': {
                  repository: {
                    url: 'git+https://github.com/facebook/react.git',
                  },
                },
              },
            }),
          } as Response;
        }

        if (urlString.includes('github.com/facebook/react/releases/tag')) {
          return { ok: false } as Response; // Content fetch fails
        }

        return { ok: false } as Response;
      }) as unknown as typeof fetch;

      const update: PackageUpdate = {
        name: 'react',
        fromVersion: '19.0.0',
        toVersion: '19.1.0',
        updateType: 'minor',
        ecosystem: 'npm',
      };

      const result = await fetchChangelog(update, mockLogger);

      // Should return URL since content fetch failed
      expect(result).toContain('github.com/facebook/react/releases/tag/v19.1.0');

      globalThis.fetch = originalFetch;
    });
  });
});
