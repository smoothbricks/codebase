/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { readFileSync } from 'node:fs';
import { platformDirectory } from './platform.js';

/**
 * `platformDirectory` is the runtime owner of the platform → `dist` directory mapping, and
 * `package.json` restates the same mapping twice: once as `napi.targets` rustc triples and once as
 * `--output-dir dist/bin/<directory>` literals in the per-arch `cli-*` commands.
 *
 * Adding a target to `napi.targets` without teaching `platformDirectory` about it produces a
 * successful build followed by `Unsupported Cowshed native target` at load, and a mistyped
 * `--output-dir` produces a binary the trampoline never finds. Neither is visible at build time,
 * so the three statements are compared here instead.
 */

/** rustc triple → the `<platform>-<arch>` pair `platformDirectory` is asked about. */
const TRIPLE_HOSTS: Readonly<Record<string, { platform: NodeJS.Platform; arch: string }>> = {
  'aarch64-apple-darwin': { platform: 'darwin', arch: 'arm64' },
  'x86_64-apple-darwin': { platform: 'darwin', arch: 'x64' },
  'aarch64-unknown-linux-gnu': { platform: 'linux', arch: 'arm64' },
  'x86_64-unknown-linux-gnu': { platform: 'linux', arch: 'x64' },
};

interface Manifest {
  napi: { targets: readonly string[] };
  nx: { targets: Readonly<Record<string, { options?: { command?: string } }>> };
}

const manifest: Manifest = JSON.parse(
  readFileSync(new URL('../package.json', import.meta.url), 'utf8'),
);

describe('platform target table', () => {
  it('resolves every napi target to a directory platformDirectory returns', () => {
    for (const triple of manifest.napi.targets) {
      const host = TRIPLE_HOSTS[triple];
      // A triple with no host mapping is drift by itself: nothing can check where it lands.
      if (host === undefined) {
        throw new Error(`napi.targets lists ${triple}, which has no host mapping in this test`);
      }
      expect(platformDirectory(host.platform, host.arch)).not.toBeNull();
    }
  });

  it('builds one CLI binary per napi target, into the directory the loader looks in', () => {
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

    expect(Object.keys(built).sort()).toEqual([...manifest.napi.targets].sort());
    for (const [triple, outputDirectory] of Object.entries(built)) {
      const host = TRIPLE_HOSTS[triple];
      if (host === undefined) {
        throw new Error(`a CLI build command targets ${triple}, which has no host mapping`);
      }
      const expected = platformDirectory(host.platform, host.arch);
      if (expected === null) {
        throw new Error(`platformDirectory refuses ${host.platform}-${host.arch}`);
      }
      expect(outputDirectory).toBe(expected);
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
