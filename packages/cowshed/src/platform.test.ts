/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { readFileSync } from 'node:fs';
import { addonDirectory, cliDirectory, NATIVE_TARGETS } from './platform.js';

/**
 * `NATIVE_TARGETS` is the runtime owner of the platform → `dist` directory mapping, for the addon
 * (`dist/native/<directory>`) and the CLI (`dist/bin/<directory>`). `package.json` restates the addon
 * triples as `napi.targets`, and the CLI triples and directories in the per-arch `cli-*` targets,
 * because nx command strings cannot import the table. This file is the check that those strings
 * still name the table. Adding a triple to `napi.targets` without a row produces a successful build
 * followed by `Unsupported Cowshed native target` at load.
 */

interface Target {
  executor?: string;
  options?: { command?: string; target?: string; bin?: string; output?: string };
}

interface Manifest {
  napi: { targets: readonly string[] };
  nx: { targets: Readonly<Record<string, Target>> };
}

const manifest: Manifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));

const ZIGBUILD_EXECUTOR = '@smoothbricks/nx-plugin:cargo-zigbuild';
const ZIGBUILD_CLI_OUTPUT = /^\{projectRoot\}\/dist\/bin\/([^/]+)\/cowshed$/;

/**
 * The CLI a target builds, as the triple it compiles for and the `dist/bin` directory it writes
 * into, or `null` when the target builds no CLI.
 */
function cliBuildOf(target: Target): { triple: string; directory: string } | null {
  const options = target.options;
  if (target.executor === ZIGBUILD_EXECUTOR && options?.bin === 'cowshed') {
    const triple = options.target;
    const directory = ZIGBUILD_CLI_OUTPUT.exec(options.output ?? '')?.[1];
    if (triple === undefined || directory === undefined) {
      throw new Error(
        `a cargo-zigbuild CLI build needs a target and a {projectRoot}/dist/bin/<directory>/cowshed output: ${JSON.stringify(options)}`,
      );
    }
    return { triple, directory };
  }
  const command = options?.command;
  if (command === undefined || !command.includes('--bin cowshed')) {
    return null;
  }
  const triple = /--target (\S+)/.exec(command)?.[1];
  const directory = /--output-dir dist\/bin\/(\S+)/.exec(command)?.[1];
  if (triple === undefined || directory === undefined) {
    throw new Error(`a CLI build command names no target or no output directory: ${command}`);
  }
  return { triple, directory };
}

describe('platform target table', () => {
  it('is the exact set package.json napi.targets lists', () => {
    expect([...manifest.napi.targets].sort()).toEqual(NATIVE_TARGETS.map((target) => target.addon.triple).sort());
  });

  it('resolves every table row to its addon and CLI directories', () => {
    for (const target of NATIVE_TARGETS) {
      expect(addonDirectory(target.platform, target.arch)).toBe(target.addon.directory);
      expect(cliDirectory(target.platform, target.arch)).toBe(target.cli.directory);
    }
  });

  it('builds one CLI binary per table row, into the directory the loader looks in', () => {
    const built: Record<string, string> = {};
    for (const target of Object.values(manifest.nx.targets)) {
      const build = cliBuildOf(target);
      if (build !== null) {
        built[build.triple] = build.directory;
      }
    }

    expect(Object.keys(built).sort()).toEqual(NATIVE_TARGETS.map((target) => target.cli.triple).sort());
    for (const target of NATIVE_TARGETS) {
      expect(built[target.cli.triple]).toBe(target.cli.directory);
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
