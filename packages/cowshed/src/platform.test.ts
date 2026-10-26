/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { addonDirectory, cliDirectory, NATIVE_TARGETS } from './platform.js';

/**
 * `package.json`'s `napi.targets` is the single owner of the shipped triples: the Nx plugin
 * generates the `napi-*`/`cli-*` build targets from it, and `NATIVE_TARGETS` derives the
 * loader's platform to `dist` directory rows from the same list at load. No second statement
 * is left to compare against, so this file pins the derivation's output as a golden, the
 * row resolution, and the fail-closed refusal for hosts the package does not ship for.
 */

describe('platform target table', () => {
  it('derives the shipped rows from the manifest triples', () => {
    expect(NATIVE_TARGETS).toEqual([
      {
        platform: 'darwin',
        arch: 'arm64',
        addon: { triple: 'aarch64-apple-darwin', directory: 'darwin-arm64' },
        cli: { triple: 'aarch64-apple-darwin', directory: 'darwin-arm64' },
      },
      {
        platform: 'darwin',
        arch: 'x64',
        addon: { triple: 'x86_64-apple-darwin', directory: 'darwin-x64' },
        cli: { triple: 'x86_64-apple-darwin', directory: 'darwin-x64' },
      },
      {
        platform: 'linux',
        arch: 'arm64',
        addon: { triple: 'aarch64-unknown-linux-gnu', directory: 'linux-arm64-gnu' },
        cli: { triple: 'aarch64-unknown-linux-musl', directory: 'linux-arm64' },
      },
      {
        platform: 'linux',
        arch: 'x64',
        addon: { triple: 'x86_64-unknown-linux-gnu', directory: 'linux-x64-gnu' },
        cli: { triple: 'x86_64-unknown-linux-musl', directory: 'linux-x64' },
      },
    ]);
  });

  it('resolves every table row to its addon and CLI directories', () => {
    for (const target of NATIVE_TARGETS) {
      expect(addonDirectory(target.platform, target.arch)).toBe(target.addon.directory);
      expect(cliDirectory(target.platform, target.arch)).toBe(target.cli.directory);
    }
  });

  it('refuses a host it does not ship for', () => {
    // Fail-closed is the contract: `loadNativeModule` branches on null, and a wrong non-null
    // answer would send it looking in a directory that never exists.
    for (const directory of [addonDirectory, cliDirectory]) {
      expect(directory('darwin', 'ia32')).toBeNull();
      expect(directory('win32', 'x64')).toBeNull();
      expect(directory('freebsd', 'arm64')).toBeNull();
    }
  });
});
