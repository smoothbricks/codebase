import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { printCommandOutput } from '../lib/run.js';

const script = join(managedAssetsRoot, 'raw/tooling/direnv/ttsc-env.sh');
/** Where every checkout holds its managed copy, sourced relative to the checkout root. */
const checkoutScript = 'tooling/direnv/ttsc-env.sh';

interface TtscEnv {
  readonly TTSC_TSGO_BINARY: string;
  readonly TTSC_CACHE_DIR: string;
  readonly TTSC_GO_CACHE_DIR: string;
}

/**
 * Source the checkout's own copy from its root with exactly `inherited` as the
 * environment (plus PATH), the way a git hook or devenv's enterShell does, and
 * read back what it exported.
 */
function enter(checkout: string, inherited: Record<string, string>): TtscEnv {
  const result = spawnSync(
    '/bin/sh',
    ['-c', `. "$0" && printf '%s\\n' "$TTSC_TSGO_BINARY" "$TTSC_CACHE_DIR" "$TTSC_GO_CACHE_DIR"`, checkoutScript],
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
    for (const path of [checkout, other]) {
      mkdirSync(join(path, 'tooling/direnv'), { recursive: true });
      copyFileSync(script, join(path, checkoutScript));
    }
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
    TTSC_CACHE_DIR: join(other, '.cache/ttsc-plugins'),
    TTSC_GO_CACHE_DIR: join(other, '.cache/ttsc/go-build'),
  };
}

it('selects this checkout profile when the repository declares its native producer', () => {
  withCheckouts((checkout, other) => {
    mkdirSync(join(checkout, 'tooling/direnv'), { recursive: true });
    writeFileSync(join(checkout, 'tooling/direnv/typescript-go.nix'), '{ pkgs }: pkgs.typescript\n');
    mkdirSync(join(checkout, 'tooling/direnv/.devenv/profile/bin'), { recursive: true });
    symlinkSync(process.execPath, join(checkout, 'tooling/direnv/.devenv/profile/bin/tsgo'));
    expect(enter(checkout, boundFor(other))).toEqual({
      TTSC_TSGO_BINARY: join(checkout, 'tooling/direnv/.devenv/profile/bin/tsgo'),
      TTSC_CACHE_DIR: join(checkout, '.cache/ttsc-plugins'),
      TTSC_GO_CACHE_DIR: join(checkout, '.cache/ttsc/go-build'),
    });
  });
});

it('refuses a declared producer with no executable profile output instead of selecting the stock compiler', () => {
  withCheckouts((checkout) => {
    mkdirSync(join(checkout, 'tooling/direnv'), { recursive: true });
    writeFileSync(join(checkout, 'tooling/direnv/typescript-go.nix'), '{ pkgs }: pkgs.typescript\n');
    const result = spawnSync('/bin/sh', ['-c', '. "$0"', checkoutScript], {
      cwd: checkout,
      env: { PATH: process.env.PATH ?? '/usr/bin:/bin', TTSC_TSGO_BINARY: '/old/compiler' },
      encoding: 'utf8',
    });
    expect(result.status).toBe(1);
    expect(result.stderr).toContain('native TypeScript producer is declared');
    expect(result.stderr).toContain(join(checkout, 'tooling/direnv/.devenv/profile/bin/tsgo'));
  });
});
it('refuses a directory that is not a workspace root instead of caching beside it', () => {
  withCheckouts((checkout, _other, scratch) => {
    const home = join(scratch, 'home');
    mkdirSync(home);
    const result = spawnSync('/bin/sh', ['-c', '. "$0"', join(checkout, checkoutScript)], {
      cwd: home,
      env: { PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: home },
      encoding: 'utf8',
    });
    expect(result.status).toBe(1);
    expect(result.stderr).toContain(`not a workspace root (no ${checkoutScript}): ${home}`);
    expect(existsSync(join(home, '.cache'))).toBe(false);
  });
});

describe('ttsc-env.sh', () => {
  it("names this checkout's compiler and caches when nothing is inherited", () => {
    withCheckouts((checkout) => {
      expect(enter(checkout, {})).toEqual({
        TTSC_TSGO_BINARY: join(checkout, 'node_modules/@typescript/native/bin/tsc'),
        TTSC_CACHE_DIR: join(checkout, '.cache/ttsc-plugins'),
        TTSC_GO_CACHE_DIR: join(checkout, '.cache/ttsc/go-build'),
      });
    });
  });

  it("drops another workspace's compiler and caches that a shell bound for it left behind", () => {
    withCheckouts((checkout, other) => {
      expect(enter(checkout, boundFor(other))).toEqual({
        TTSC_TSGO_BINARY: join(checkout, 'node_modules/@typescript/native/bin/tsc'),
        TTSC_CACHE_DIR: join(checkout, '.cache/ttsc-plugins'),
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
        TTSC_GO_CACHE_DIR: join(checkout, '.cache/ttsc/go-build'),
      };
      expect(enter(checkout, { TTSC_CACHE_DIR: shared })).toEqual(expected);
      expect(enter(checkout, { NX_WORKSPACE_ROOT_PATH: checkout, TTSC_CACHE_DIR: shared })).toEqual(expected);
    });
  });
});

/**
 * A `go list` in the cwd under the environment ttsc builds for each of its go invocations (GoSourceInputs.goBuildEnv,
 * the module TTSC_GO_SOURCE_INPUTS names), printing that environment's GOFLAGS.
 */
const LIST_UNDER_TTSC_ENV = `const { GoSourceInputs } = require(process.env.TTSC_GO_SOURCE_INPUTS);
const env = GoSourceInputs.goBuildEnv('go', undefined, process.env);
const listed = require('node:child_process').spawnSync('go', ['list', '.'], { env, encoding: 'utf8' });
process.stderr.write(listed.stderr);
process.stdout.write(env.GOFLAGS);
process.exit(listed.status ?? 1);`;

describe("ttsc's go invocations", () => {
  it("list a plugin's packages under a caller's GOFLAGS -mod=mod, keeping its other flags", () => {
    // ttsc runs go with GOWORK=auto, so go works in workspace mode wherever a go.work lies (typia's native plugin ships
    // one), and go refuses -mod=mod there: a host shell exporting GOFLAGS=-mod=mod failed its entry rebuilding a tool,
    // 'selecting Go packages for plugin "typia" failed: go: -mod may only be set to readonly or vendor when in
    // workspace mode'.
    const root = mkdtempSync(join(tmpdir(), 'ttsc-goflags-'));
    try {
      writeFileSync(join(root, 'go.work'), 'go 1.21\n\nuse ./plugin\n');
      mkdirSync(join(root, 'plugin'));
      writeFileSync(join(root, 'plugin', 'go.mod'), 'module example.com/plugin\n\ngo 1.21\n');
      writeFileSync(join(root, 'plugin', 'plugin.go'), 'package plugin\n');
      const ttsc = dirname(createRequire(import.meta.url).resolve('ttsc/package.json'));
      const listed = spawnSync(process.execPath, ['-e', LIST_UNDER_TTSC_ENV], {
        cwd: join(root, 'plugin'),
        env: {
          ...process.env,
          GOFLAGS: '-mod=mod -trimpath',
          GOWORK: '',
          GOCACHE: join(root, 'go-build'),
          TTSC_GO_SOURCE_INPUTS: join(ttsc, 'lib/plugin/internal/source/GoSourceInputs.js'),
        },
        encoding: 'utf8',
      });
      expect(listed.stderr).toBe('');
      expect(listed.status).toBe(0);
      expect(listed.stdout).toBe('-trimpath');
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
