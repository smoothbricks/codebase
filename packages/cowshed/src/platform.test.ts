/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { readFileSync } from 'node:fs';
import { hostStableCowshedBinary, NATIVE_TARGETS, platformDirectory } from './platform.js';

/**
 * `NATIVE_TARGETS` is the runtime owner of the platform → `dist` directory mapping.
 * `package.json` restates it twice: `napi.targets` rustc triples and
 * `--output-dir dist/bin/<directory>` in the per-arch `cli-*` commands, because nx
 * command strings cannot import the table. This file is the check that those strings
 * still name the table. Adding a triple to `napi.targets` without a row produces a
 * successful build followed by `Unsupported Cowshed native target` at load.
 */

interface Manifest {
  napi: { targets: readonly string[] };
  nx: { targets: Readonly<Record<string, { options?: { command?: string } }>> };
}

const manifest: Manifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));

describe('platform target table', () => {
  it('is the exact set package.json napi.targets lists', () => {
    expect([...manifest.napi.targets].sort()).toEqual(
      NATIVE_TARGETS.map((target) => target.triple)
        .slice()
        .sort(),
    );
  });

  it('resolves every table row to its directory', () => {
    for (const target of NATIVE_TARGETS) {
      expect(platformDirectory(target.platform, target.arch)).toBe(target.directory);
    }
  });

  it('builds one CLI binary per table row, into the directory the loader looks in', () => {
    const built: Record<string, string> = {};
    for (const target of Object.values(manifest.nx.targets)) {
      const command = target.options?.command;
      if (command === undefined || !command.includes('--bin cowshed')) {
        continue;
      }
      const triple = /--target (\S+)/.exec(command)?.[1];
      const outputDirectory = /--output-dir dist\/bin\/(\S+)/.exec(command)?.[1];
      if (triple === undefined || outputDirectory === undefined) {
        throw new Error(`a CLI build command names no target or no output directory: ${command}`);
      }
      built[triple] = outputDirectory;
    }

    expect(Object.keys(built).sort()).toEqual(
      NATIVE_TARGETS.map((target) => target.triple)
        .slice()
        .sort(),
    );
    for (const target of NATIVE_TARGETS) {
      expect(built[target.triple]).toBe(target.directory);
    }
  });

  it('refuses a host it does not ship for', () => {
    // Fail-closed is the contract: `loadNativeModule` and `resolveCliBackend` both branch on null,
    // and a wrong non-null answer would send them looking in a directory that never exists.
    expect(platformDirectory('darwin', 'ia32')).toBeNull();
    expect(platformDirectory('win32', 'x64')).toBeNull();
    expect(platformDirectory('freebsd', 'arm64')).toBeNull();
  });
});

describe('host-stable cowshed binary', () => {
  it('joins launchd HostStableExecutable segments under home', () => {
    expect(hostStableCowshedBinary('/Users/test')).toBe(
      '/Users/test/Library/Application Support/dev.cowshed/bin/cowshed',
    );
  });
});
