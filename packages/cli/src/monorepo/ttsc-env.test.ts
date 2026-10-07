import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { printCommandOutput } from '../lib/run.js';

const script = join(managedAssetsRoot, 'raw/tooling/direnv/ttsc-env.sh');

interface TtscEnv {
  readonly TTSC_TSGO_BINARY: string;
  readonly TTSC_CACHE_DIR: string;
  readonly TTSC_GO_CACHE_DIR: string;
}

/**
 * Source the script from `checkout` with exactly `inherited` as the environment
 * (plus PATH), the way a git hook or devenv's enterShell does, and read back
 * what it exported.
 */
function enter(checkout: string, inherited: Record<string, string>): TtscEnv {
  const result = spawnSync(
    '/bin/sh',
    ['-c', `. "$0" && printf '%s\\n' "$TTSC_TSGO_BINARY" "$TTSC_CACHE_DIR" "$TTSC_GO_CACHE_DIR"`, script],
    { cwd: checkout, env: { PATH: process.env.PATH ?? '/usr/bin:/bin', ...inherited }, encoding: 'utf8' },
  );
  if (result.status !== 0) printCommandOutput(result.stdout ?? '', result.stderr ?? '');
  expect(result.status).toBe(0);
  const [binary, cache, goCache] = result.stdout.split('\n');
  return { TTSC_TSGO_BINARY: binary ?? '', TTSC_CACHE_DIR: cache ?? '', TTSC_GO_CACHE_DIR: goCache ?? '' };
}

function withCheckouts(run: (checkout: string, other: string, scratch: string) => void): void {
  const scratch = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-ttsc-env-')));
  try {
    const checkout = join(scratch, 'checkout');
    const other = join(scratch, 'other');
    for (const path of [checkout, other]) mkdirSync(path);
    run(checkout, other, scratch);
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

/** What another checkout's shell exports, as a hook run from that shell inherits it. */
function boundFor(other: string): Record<string, string> {
  return {
    NX_WORKSPACE_ROOT_PATH: other,
    TTSC_TSGO_BINARY: join(other, 'node_modules/@typescript/native/bin/tsc'),
    TTSC_CACHE_DIR: join(other, '.cache/ttsc'),
    TTSC_GO_CACHE_DIR: join(other, '.cache/ttsc/go-build'),
  };
}

describe('ttsc-env.sh', () => {
  it("names this checkout's compiler and caches when nothing is inherited", () => {
    withCheckouts((checkout) => {
      expect(enter(checkout, {})).toEqual({
        TTSC_TSGO_BINARY: join(checkout, 'node_modules/@typescript/native/bin/tsc'),
        TTSC_CACHE_DIR: join(checkout, '.cache/ttsc'),
        TTSC_GO_CACHE_DIR: join(checkout, '.cache/ttsc/go-build'),
      });
    });
  });

  it("drops another workspace's compiler and caches that a shell bound for it left behind", () => {
    withCheckouts((checkout, other) => {
      expect(enter(checkout, boundFor(other))).toEqual({
        TTSC_TSGO_BINARY: join(checkout, 'node_modules/@typescript/native/bin/tsc'),
        TTSC_CACHE_DIR: join(checkout, '.cache/ttsc'),
        TTSC_GO_CACHE_DIR: join(checkout, '.cache/ttsc/go-build'),
      });
    });
  });

  it('keeps a cache the host chose, with no workspace bound or with this one', () => {
    withCheckouts((checkout, _other, scratch) => {
      const shared = join(scratch, 'shared-ttsc');
      const expected = {
        TTSC_TSGO_BINARY: join(checkout, 'node_modules/@typescript/native/bin/tsc'),
        TTSC_CACHE_DIR: shared,
        TTSC_GO_CACHE_DIR: join(shared, 'go-build'),
      };
      expect(enter(checkout, { TTSC_CACHE_DIR: shared })).toEqual(expected);
      expect(enter(checkout, { NX_WORKSPACE_ROOT_PATH: checkout, TTSC_CACHE_DIR: shared })).toEqual(expected);
    });
  });
});
