import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
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
  readonly CGO_ENABLED: string;
}

/** The HOME every checkout of one fixture enters with: one host, however many checkouts. */
function homeOf(checkout: string): string {
  return join(dirname(checkout), 'home');
}

/** This checkout's plugin store, and the Go build cache every checkout of the fixture's host shares. */
function ownCaches(checkout: string): Pick<TtscEnv, 'TTSC_CACHE_DIR' | 'TTSC_GO_CACHE_DIR'> {
  return {
    TTSC_CACHE_DIR: join(checkout, '.cache/ttsc-plugins'),
    TTSC_GO_CACHE_DIR: join(homeOf(checkout), '.cache/ttsc/go-build'),
  };
}

/**
 * Source the checkout's own copy from its root with exactly "inherited" as the
 * environment (plus PATH and the fixture's HOME), the way a git hook or devenv's
 * enterShell does, and read back what it exported.
 */
function enter(checkout: string, inherited: Record<string, string>): TtscEnv {
  const result = spawnSync(
    '/bin/sh',
    [
      '-c',
      `. "$0" && printf '%s\\n' "$TTSC_TSGO_BINARY" "$TTSC_CACHE_DIR" "$TTSC_GO_CACHE_DIR" "\${CGO_ENABLED-unset}"`,
      checkoutScript,
    ],
    {
      cwd: checkout,
      env: { PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: homeOf(checkout), ...inherited },
      encoding: 'utf8',
    },
  );
  if (result.status !== 0) printCommandOutput(result.stdout ?? '', result.stderr ?? '');
  expect(result.status).toBe(0);
  const [binary, cache, goCache, cgoEnabled] = result.stdout.split('\n');
  return {
    TTSC_TSGO_BINARY: binary ?? '',
    TTSC_CACHE_DIR: cache ?? '',
    TTSC_GO_CACHE_DIR: goCache ?? '',
    CGO_ENABLED: cgoEnabled ?? '',
  };
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

/** What a shell bound for another checkout exports when it names caches inside that checkout. */
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
      ...ownCaches(checkout),
      CGO_ENABLED: '0',
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
  //#region smoo/cowshed!n/repository-placed-caches
  it("names this checkout's compiler and plugin store and the host's Go build cache when nothing is inherited", () => {
    withCheckouts((checkout) => {
      expect(enter(checkout, {})).toEqual({
        TTSC_TSGO_BINARY: join(checkout, 'node_modules/@typescript/native/bin/tsc'),
        ...ownCaches(checkout),
        CGO_ENABLED: '0',
      });
    });
  });

  it('gives every checkout of a host one Go build cache, and each its own plugin store', () => {
    withCheckouts((checkout, other) => {
      const first = enter(checkout, {});
      const second = enter(other, {});
      expect(second.TTSC_GO_CACHE_DIR).toBe(first.TTSC_GO_CACHE_DIR);
      expect(second.TTSC_CACHE_DIR).not.toBe(first.TTSC_CACHE_DIR);
      expect(existsSync(first.TTSC_GO_CACHE_DIR)).toBe(true);
    });
  });
  //#endregion smoo/cowshed!n/repository-placed-caches

  it("drops another workspace's compiler and caches that a shell bound for it left behind", () => {
    withCheckouts((checkout, other) => {
      expect(enter(checkout, boundFor(other))).toEqual({
        TTSC_TSGO_BINARY: join(checkout, 'node_modules/@typescript/native/bin/tsc'),
        ...ownCaches(checkout),
        CGO_ENABLED: '0',
      });
    });
  });

  it("keeps a plugin store the host chose, with no workspace bound or with this one, beside the host's Go build cache", () => {
    withCheckouts((checkout, _other, scratch) => {
      const shared = join(scratch, 'shared-ttsc');
      const expected = {
        TTSC_TSGO_BINARY: join(checkout, 'node_modules/@typescript/native/bin/tsc'),
        TTSC_CACHE_DIR: shared,
        TTSC_GO_CACHE_DIR: ownCaches(checkout).TTSC_GO_CACHE_DIR,
        CGO_ENABLED: '0',
      };
      expect(enter(checkout, { TTSC_CACHE_DIR: shared })).toEqual(expected);
      expect(enter(checkout, { NX_WORKSPACE_ROOT_PATH: checkout, TTSC_CACHE_DIR: shared })).toEqual(expected);
    });
  });

  it('defaults CGO_ENABLED to 0 and keeps an inherited value', () => {
    withCheckouts((checkout, other) => {
      expect(enter(checkout, {}).CGO_ENABLED).toBe('0');
      expect(enter(checkout, { CGO_ENABLED: '1' }).CGO_ENABLED).toBe('1');
      expect(enter(checkout, { ...boundFor(other), CGO_ENABLED: '1' }).CGO_ENABLED).toBe('1');
    });
  });

  it('refuses to guess a Go build cache when no HOME names the host', () => {
    withCheckouts((checkout) => {
      const result = spawnSync('/bin/sh', ['-c', '. "$0"', checkoutScript], {
        cwd: checkout,
        env: { PATH: process.env.PATH ?? '/usr/bin:/bin' },
        encoding: 'utf8',
      });
      expect(result.status).toBe(1);
      expect(result.stderr).toContain('HOME is unset');
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

/**
 * ttsc's collectPluginSourceFiles over the plugin module argv[-1], from the ttsc module argv[-2], printed as JSON, or
 * its refusal on stderr with status 1.
 */
const COLLECT_PLUGIN_SOURCE_FILES = `const { collectPluginSourceFiles } = require(process.argv.at(-2));
try {
  process.stdout.write(JSON.stringify(collectPluginSourceFiles(process.argv.at(-1))));
} catch (error) {
  process.stderr.write(error.message);
  process.exit(1);
}`;

describe("ttsc's plugin source files", () => {
  it('pass over a link below a directory the go tool ignores, and refuse a link the build would read', () => {
    // cowshed links each package's .cache/lmao onto its build volume (a repository's .cowshed.toml [build] state), a package's Go
    // plugin module among them. ttsc refused that module's source identity for the link, so every load copied the
    // module and asked go for its packages again: 31 process spawns per TtscCompiler.prepare() instead of 10.
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'ttsc-source-links-')));
    try {
      const plugin = join(root, 'plugin');
      mkdirSync(join(plugin, '.cache'), { recursive: true });
      mkdirSync(join(root, 'volume'));
      writeFileSync(join(plugin, 'go.mod'), 'module example.com/plugin\n\ngo 1.21\n');
      writeFileSync(join(plugin, 'plugin.go'), 'package plugin\n');
      symlinkSync(join(root, 'volume'), join(plugin, '.cache', 'lmao'));
      const ttsc = dirname(createRequire(import.meta.url).resolve('ttsc/package.json'));
      const collect = () =>
        spawnSync(
          process.execPath,
          [
            '-e',
            COLLECT_PLUGIN_SOURCE_FILES,
            join(ttsc, 'lib/plugin/internal/source/collectPluginSourceFiles.js'),
            plugin,
          ],
          { encoding: 'utf8' },
        );
      const passed = collect();
      expect(passed.stderr).toBe('');
      expect(passed.status).toBe(0);
      expect(JSON.parse(passed.stdout)).toEqual([join(plugin, 'go.mod'), join(plugin, 'plugin.go')]);
      symlinkSync(join(root, 'volume'), join(plugin, 'linked'));
      const refused = collect();
      expect(refused.status).toBe(1);
      expect(refused.stderr).toContain(`contains a link at ${join(plugin, 'linked')}`);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

/**
 * Run ttsc's `hashPluginBuildEnvironment` over the cwd under the child's env,
 * recording every framed `[key, value]` chunk and the resulting SHA-256 digest.
 */
const HASH_PLUGIN_BUILD_ENV = `const crypto = require('node:crypto');
const fs = require('node:fs');
const { hashPluginBuildEnvironment } = require(process.argv.at(-1));
const hasher = crypto.createHash('sha256');
const entries = [];
const sink = {
  update(chunk) {
    hasher.update(chunk);
    entries.push(JSON.parse(chunk));
  },
};
hashPluginBuildEnvironment(sink, 'go', process.cwd(), process.env, {
  readFile: (location) => fs.readFileSync(location),
});
process.stdout.write(JSON.stringify({ digest: hasher.digest('hex'), entries }));`;

const COSTLY_CGO_KEYS = [
  'PKG_CONFIG',
  'CGO_CFLAGS',
  'CGO_CPPFLAGS',
  'CGO_CXXFLAGS',
  'CGO_FFLAGS',
  'CGO_LDFLAGS',
] as const;

describe("ttsc's plugin build environment hash", () => {
  function runHash(
    root: string,
    extraEnv: Record<string, string>,
  ): {
    readonly digest: string;
    readonly entries: readonly [string, unknown][];
    readonly clangSpawns: readonly string[];
  } {
    const bin = join(root, 'bin');
    mkdirSync(bin, { recursive: true });
    const clangLog = join(root, 'clang.log');
    writeFileSync(clangLog, '');
    const recordingClang = join(bin, 'clang');
    writeFileSync(recordingClang, '#!/bin/sh\nprintf "%s\\n" "$*" >> "$TTSC_TEST_CLANG_LOG"\nexit 0\n');
    chmodSync(recordingClang, 0o755);
    const fakePkgConfig = join(bin, 'pkg-config');
    writeFileSync(fakePkgConfig, '#!/bin/sh\nexit 0\n');
    chmodSync(fakePkgConfig, 0o755);

    const ttsc = dirname(createRequire(import.meta.url).resolve('ttsc/package.json'));
    const result = spawnSync(
      process.execPath,
      ['-e', HASH_PLUGIN_BUILD_ENV, join(ttsc, 'lib/plugin/internal/source/hashPluginBuildEnvironment.js')],
      {
        cwd: root,
        env: {
          ...process.env,
          PATH: `${bin}:${process.env.PATH ?? ''}`,
          CC: 'clang',
          GOCACHE: join(root, 'go-build'),
          GOENV: join(root, 'go.env'),
          GOWORK: 'off',
          PKG_CONFIG: fakePkgConfig,
          TTSC_TEST_CLANG_LOG: clangLog,
          ...extraEnv,
        },
        encoding: 'utf8',
      },
    );
    if (result.status !== 0) printCommandOutput(result.stdout ?? '', result.stderr ?? '');
    expect(result.status).toBe(0);
    const parsed: unknown = JSON.parse(result.stdout);
    if (
      typeof parsed !== 'object' ||
      parsed === null ||
      !('digest' in parsed) ||
      typeof parsed.digest !== 'string' ||
      !('entries' in parsed) ||
      !Array.isArray(parsed.entries)
    ) {
      throw new Error(`Unexpected hash output: ${result.stdout}`);
    }
    const entries: [string, unknown][] = [];
    for (const entry of parsed.entries) {
      if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== 'string') {
        throw new Error(`Unexpected hash entry: ${JSON.stringify(entry)}`);
      }
      entries.push([entry[0], entry[1]]);
    }
    const logLines = readFileSync(clangLog, 'utf8')
      .split('\n')
      .filter((line) => line.length > 0);
    return { digest: parsed.digest, entries, clangSpawns: logLines };
  }

  it('skips Go costly cgo keys and spawns clang zero times when CGO_ENABLED=0', () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'ttsc-cgo-off-')));
    try {
      const first = runHash(root, {
        CGO_ENABLED: '0',
        CGO_CFLAGS: '-DIGNORED_C_ONE',
        CGO_CPPFLAGS: '-DIGNORED_CPP',
        CGO_CXXFLAGS: '-DIGNORED_CXX',
        CGO_FFLAGS: '-DIGNORED_F',
        CGO_LDFLAGS: '-L/ignored',
      });
      expect(first.clangSpawns).toEqual([]);
      const keys = new Map(first.entries);
      expect(keys.get('CGO_ENABLED')).toBe('0');
      for (const key of COSTLY_CGO_KEYS) {
        expect(keys.has(key)).toBe(false);
      }

      // Changing any costly cgo flag must not move the digest when cgo is off.
      const second = runHash(root, {
        CGO_ENABLED: '0',
        CGO_CFLAGS: '-DIGNORED_C_TWO',
        CGO_CPPFLAGS: '-DIGNORED_CPP_TWO',
        CGO_CXXFLAGS: '-DIGNORED_CXX_TWO',
        CGO_FFLAGS: '-DIGNORED_F_TWO',
        CGO_LDFLAGS: '-L/ignored-two',
      });
      expect(second.clangSpawns).toEqual([]);
      expect(second.digest).toBe(first.digest);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it('reads and hashes all six costly cgo keys in order when CGO_ENABLED=1', () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'ttsc-cgo-on-')));
    try {
      const cgoValues = {
        CGO_ENABLED: '1',
        CGO_CFLAGS: '-DTEST_C',
        CGO_CPPFLAGS: '-DTEST_CPP',
        CGO_CXXFLAGS: '-DTEST_CXX',
        CGO_FFLAGS: '-DTEST_F',
        CGO_LDFLAGS: '-L/test',
      };
      const first = runHash(root, cgoValues);
      const keys = new Map(first.entries);
      expect(keys.get('CGO_ENABLED')).toBe('1');
      expect(keys.get('CGO_CFLAGS')).toBe('-DTEST_C');
      expect(keys.get('CGO_CPPFLAGS')).toBe('-DTEST_CPP');
      expect(keys.get('CGO_CXXFLAGS')).toBe('-DTEST_CXX');
      expect(keys.get('CGO_FFLAGS')).toBe('-DTEST_F');
      expect(keys.get('CGO_LDFLAGS')).toBe('-L/test');
      expect(String(keys.get('PKG_CONFIG'))).toContain(`${join(root, 'bin', 'pkg-config')}\0command:sha256:`);

      // Key ordering must match stock GO_BUILD_ENV_KEYS so the digest is unchanged when cgo is on.
      const entryNames = first.entries.map(([key]) => key);
      const positions = COSTLY_CGO_KEYS.map((key) => entryNames.indexOf(key));
      expect(positions.every((pos) => pos >= 0)).toBe(true);
      for (let i = 1; i < positions.length; i += 1) {
        const previous = positions[i - 1];
        const current = positions[i];
        expect(previous).toBeDefined();
        expect(current).toBeDefined();
        if (previous !== undefined && current !== undefined) {
          expect(current).toBeGreaterThan(previous);
        }
      }

      const second = runHash(root, { ...cgoValues, CGO_CFLAGS: '-DTEST_C_CHANGED' });
      expect(second.digest).not.toBe(first.digest);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
