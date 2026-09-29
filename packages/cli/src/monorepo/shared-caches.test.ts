import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { printCommandOutput } from '../lib/run.js';

const script = join(managedAssetsRoot, 'raw/tooling/direnv/shared-caches.sh');

const CACHE_VARIABLES = ['TTSC_CACHE_DIR', 'TTSC_GO_CACHE_DIR', 'GOCACHE', 'GOMODCACHE', 'GOFLAGS'] as const;
type CacheVariable = (typeof CACHE_VARIABLES)[number];
type CacheEnvironment = Partial<Record<CacheVariable, string>>;

interface Scratch {
  readonly checkout: string;
  readonly cachesRoot: string;
  /** A second HOME, so a host shell and a sandboxed one can be told apart. */
  readonly homes: readonly [string, string];
}

function withScratch(run: (scratch: Scratch) => void): void {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-shared-caches-')));
  try {
    const checkout = join(dir, 'checkout');
    const homes: [string, string] = [join(dir, 'host-home'), join(dir, 'checkout', '.cowshed', 'home')];
    for (const path of [checkout, ...homes]) mkdirSync(path, { recursive: true });
    run({ checkout, cachesRoot: join(dir, 'caches'), homes });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

/**
 * Source the script the way devenv.smoo.nix's prologue does — from the
 * workspace root, naming the caches root — and report what it exported.
 */
function enterShell(
  scratch: Scratch,
  options: { home: string; inherited?: CacheEnvironment; cachesRoot?: string },
): CacheEnvironment {
  const printed = CACHE_VARIABLES.map((name) => `printf '%s=%s\\n' ${name} "\${${name}-<unset>}"`).join('; ');
  const result = spawnSync(
    'bash',
    ['-c', `. "$1" "$2" && ${printed}`, 'shell', script, options.cachesRoot ?? scratch.cachesRoot],
    {
      cwd: scratch.checkout,
      encoding: 'utf8',
      env: { PATH: process.env.PATH ?? '', HOME: options.home, ...options.inherited },
    },
  );
  if (result.status !== 0) printCommandOutput(result.stdout ?? '', result.stderr ?? '');
  expect(result.status).toBe(0);
  const exported: CacheEnvironment = {};
  for (const line of result.stdout.split('\n')) {
    const separator = line.indexOf('=');
    const name = CACHE_VARIABLES.find((candidate) => candidate === line.slice(0, separator));
    const value = line.slice(separator + 1);
    if (name !== undefined && value !== '<unset>') exported[name] = value;
  }
  return exported;
}

describe('shared-caches.sh', () => {
  it('points the host and a sandbox at the one shared cache path when the caches volume exists', () => {
    withScratch((scratch) => {
      mkdirSync(scratch.cachesRoot);
      const shared = {
        TTSC_CACHE_DIR: join(scratch.cachesRoot, 'ttsc'),
        TTSC_GO_CACHE_DIR: join(scratch.cachesRoot, 'ttsc', 'go-build'),
        GOCACHE: join(scratch.cachesRoot, 'go', 'build'),
        GOMODCACHE: join(scratch.cachesRoot, 'go', 'mod'),
        GOFLAGS: '-trimpath',
      };
      // HOME differs between the host and a sandbox; the cache path must not.
      for (const home of scratch.homes) {
        expect(enterShell(scratch, { home })).toEqual(shared);
      }
      for (const directory of [shared.TTSC_GO_CACHE_DIR, shared.GOCACHE, shared.GOMODCACHE]) {
        expect(existsSync(directory)).toBe(true);
      }
    });
  });

  it('keeps ttsc per checkout and leaves Go on its defaults when there is no caches volume', () => {
    withScratch((scratch) => {
      expect(enterShell(scratch, { home: scratch.homes[0] })).toEqual({
        TTSC_CACHE_DIR: join(scratch.checkout, '.cache', 'ttsc'),
        TTSC_GO_CACHE_DIR: join(scratch.checkout, '.cache', 'ttsc', 'go-build'),
        GOFLAGS: '-trimpath',
      });
      expect(existsSync(join(scratch.cachesRoot))).toBe(false);
    });
  });

  it('lets a value the caller already exported win over the shared path', () => {
    withScratch((scratch) => {
      mkdirSync(scratch.cachesRoot);
      const inherited = {
        TTSC_CACHE_DIR: join(scratch.checkout, 'ci-ttsc'),
        GOCACHE: join(scratch.checkout, 'ci-go-build'),
        GOMODCACHE: join(scratch.checkout, 'ci-go-mod'),
        GOFLAGS: '-mod=mod',
      };
      expect(enterShell(scratch, { home: scratch.homes[0], inherited })).toEqual({
        ...inherited,
        TTSC_GO_CACHE_DIR: join(inherited.TTSC_CACHE_DIR, 'go-build'),
      });
    });
  });
});
