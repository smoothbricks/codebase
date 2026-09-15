import { describe, expect, it } from 'bun:test';
import { type ChromiumSetupDependencies, ensureChromium } from './index.js';

interface RunCall {
  readonly command: string;
  readonly args: string[];
  readonly cwd: string;
  readonly env?: Record<string, string>;
}

function setupDependencies(
  env: ChromiumSetupDependencies['env'],
  existingPaths: readonly string[],
  calls: RunCall[],
): ChromiumSetupDependencies {
  const existing = new Set(existingPaths);
  return {
    env,
    exists: (path) => existing.has(path),
    run: async (command, args, cwd, runEnv) => {
      calls.push({ command, args, cwd, ...(runEnv ? { env: runEnv } : {}) });
    },
  };
}

describe('ensureChromium', () => {
  it('uses preinstalled Chrome without invoking Playwright install on an ephemeral GitHub runner', async () => {
    const calls: RunCall[] = [];
    const executablePath = '/usr/bin/google-chrome';

    const result = await ensureChromium(
      '/workspace/package',
      setupDependencies({ GITHUB_ACTIONS: 'true' }, [executablePath], calls),
    );

    expect(result).toEqual({ mode: 'system', executablePath });
    expect(calls).toEqual([]);
  });

  it('uses the configured Nix browser instead of downloading an incompatible host binary', async () => {
    const calls: RunCall[] = [];
    const executablePath = '/nix/store/fixture-chromium/bin/chromium';
    const result = await ensureChromium(
      '/workspace/package',
      setupDependencies(
        { GITHUB_ACTIONS: 'true', PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH: executablePath },
        ['/var/cache/ci', executablePath],
        calls,
      ),
    );
    expect(result).toEqual({ mode: 'system', executablePath });
    expect(calls).toEqual([]);
  });

  it('honors a configured browser cache before the host XDG cache', async () => {
    const calls: RunCall[] = [];

    const result = await ensureChromium(
      '/workspace/package',
      setupDependencies(
        {
          GITHUB_ACTIONS: 'true',
          PLAYWRIGHT_BROWSERS_PATH: '/fixture/browser-cache',
          XDG_CACHE_HOME: '/var/cache/ci/xdg',
        },
        ['/var/cache/ci'],
        calls,
      ),
    );

    expect(result).toEqual({ mode: 'persistent-cache', browserCachePath: '/fixture/browser-cache' });
    expect(calls).toEqual([
      {
        command: 'playwright',
        args: ['install', 'chromium', '--only-shell'],
        cwd: '/workspace/package',
        env: { PLAYWRIGHT_BROWSERS_PATH: '/fixture/browser-cache' },
      },
    ]);
  });

  it('fails rather than downloading when an ephemeral GitHub runner has no system Chrome', async () => {
    const calls: RunCall[] = [];

    await expect(
      ensureChromium('/workspace/package', setupDependencies({ GITHUB_ACTIONS: 'true' }, [], calls)),
    ).rejects.toThrow('Refusing to download');
    expect(calls).toEqual([]);
  });

  it('honors an explicit developer cache outside GitHub Actions', async () => {
    const calls: RunCall[] = [];

    const result = await ensureChromium(
      '/workspace/package',
      setupDependencies({ PLAYWRIGHT_BROWSERS_PATH: '/tmp/browser-cache' }, [], calls),
    );

    expect(result).toEqual({ mode: 'developer-cache', browserCachePath: '/tmp/browser-cache' });
    expect(calls).toEqual([
      {
        command: 'playwright',
        args: ['install', 'chromium', '--only-shell'],
        cwd: '/workspace/package',
        env: { PLAYWRIGHT_BROWSERS_PATH: '/tmp/browser-cache' },
      },
    ]);
  });
});
