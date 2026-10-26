/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { readFileSync } from 'node:fs';
import { addonDirectory, addonSearch, cliDirectory, NATIVE_TARGETS } from './platform.js';

/**
 * `NATIVE_TARGETS` is the runtime owner of the platform → `dist` directory mapping, for the addon
 * (`dist/native/<directory>`) and the CLI (`dist/bin/<directory>` shipped, `.cache/bin/<directory>`
 * developed). `package.json` restates the addon triples as `napi.targets`, and the CLI triples and
 * directories in the per-arch `cli-*` targets, because nx command strings cannot import the table.
 * This file is the check that those strings still name the table. Adding a triple to `napi.targets`
 * without a row produces a successful build followed by `Unsupported Cowshed native target` at load.
 */

interface TargetOptions {
  command?: string;
  target?: string;
  bin?: string;
  output?: string;
  outputDir?: string;
}

interface Target {
  executor?: string;
  outputs?: readonly string[];
  options?: TargetOptions;
  configurations?: Readonly<Record<string, TargetOptions>>;
}

interface Manifest {
  napi: { targets: readonly string[] };
  nx: { targets: Readonly<Record<string, Target>> };
}

const manifest: Manifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));

const ZIGBUILD_EXECUTOR = '@smoothbricks/nx-plugin:cargo-zigbuild';
const ZIGBUILD_CLI_OUTPUT = /^\{projectRoot\}\/((?:\.cache|dist)\/bin\/[^/]+)\/cowshed$/;
/** A cargo-zigbuild CLI declares the directory its `output` file sits in, so the tree is restored whole. */
const ZIGBUILD_CLI_OUTPUTS = ['{options.output}/../'];
/** A napi CLI declares the tree its `outputDir` names, which napi and the receipt both write into. */
const NAPI_CLI_OUTPUTS = ['{projectRoot}/{options.outputDir}/'];

/**
 * The CLI a target builds under one configuration (`undefined`: its default options), as the triple
 * it compiles for and the tree it writes, or `null` when the target builds no CLI. The declared
 * outputs must read the option that command or executor writes through, so the declaration and the
 * build cannot name different trees.
 */
function cliBuildOf(target: Target, configuration: string | undefined): { triple: string; tree: string } | null {
  const options = { ...target.options, ...(configuration === undefined ? {} : target.configurations?.[configuration]) };
  if (target.executor === ZIGBUILD_EXECUTOR && options.bin === 'cowshed') {
    const triple = options.target;
    const tree = ZIGBUILD_CLI_OUTPUT.exec(options.output ?? '')?.[1];
    if (triple === undefined || tree === undefined) {
      throw new Error(
        `a cargo-zigbuild CLI build needs a target and a {projectRoot}/(.cache|dist)/bin/<directory>/cowshed output: ${JSON.stringify(options)}`,
      );
    }
    if (JSON.stringify(target.outputs) !== JSON.stringify(ZIGBUILD_CLI_OUTPUTS)) {
      throw new Error(
        `a cargo-zigbuild CLI build declares ${JSON.stringify(target.outputs)}, not its output's directory`,
      );
    }
    return { triple, tree };
  }
  const command = options.command;
  if (command === undefined || !command.includes('--bin cowshed')) {
    return null;
  }
  const triple = /--target (\S+)/.exec(command)?.[1];
  const tree = options.outputDir;
  if (triple === undefined || tree === undefined) {
    throw new Error(`a CLI build names no target or no outputDir: ${JSON.stringify(options)}`);
  }
  // napi writes the binary, and `smoo-nx-build-inputs` its receipt, into the tree the declaration names.
  if (!command.includes('--output-dir {args.outputDir}') || !command.includes('--output {args.outputDir}')) {
    throw new Error(`a CLI build command must write its binary and its receipt to {args.outputDir}: ${command}`);
  }
  if (JSON.stringify(target.outputs) !== JSON.stringify(NAPI_CLI_OUTPUTS)) {
    throw new Error(`a napi CLI build declares ${JSON.stringify(target.outputs)}, not its outputDir`);
  }
  return { triple, tree };
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

  it('builds one CLI binary per table row: the development build into .cache/bin, the shipped one into dist/bin', () => {
    const development: Record<string, string> = {};
    const production: Record<string, string> = {};
    for (const target of Object.values(manifest.nx.targets)) {
      const dev = cliBuildOf(target, undefined);
      const shipped = cliBuildOf(target, 'production');
      if (dev !== null) {
        development[dev.triple] = dev.tree;
      }
      if (shipped !== null) {
        production[shipped.triple] = shipped.tree;
      }
    }

    expect(Object.keys(development).sort()).toEqual(NATIVE_TARGETS.map((target) => target.cli.triple).sort());
    expect(Object.keys(production).sort()).toEqual(NATIVE_TARGETS.map((target) => target.cli.triple).sort());
    for (const target of NATIVE_TARGETS) {
      expect(development[target.cli.triple]).toBe(`.cache/bin/${target.cli.directory}`);
      expect(production[target.cli.triple]).toBe(`dist/bin/${target.cli.directory}`);
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

describe('addon search', () => {
  const addon = { directory: 'darwin-arm64', fileName: 'cowshed.darwin-arm64.node' };
  /** The search the loader asks from a module location, with the files `existing` present. */
  const search = (
    modulePath: string,
    existing: readonly string[],
    request: { override?: string; debug?: boolean } = {},
  ) =>
    addonSearch({
      ...addon,
      modulePath,
      exists: (path) => existing.includes(path),
      override: request.override,
      debug: request.debug ?? false,
    });
  const checkout = '/work/packages/cowshed';
  const sourceEntry = `${checkout}/crates/cowshed-napi/src/lib.rs`;
  const devTree = [
    `${checkout}/.cache/native-dev/host/cowshed.darwin-arm64.node`,
    `${checkout}/.cache/native-dev/darwin-arm64/cowshed.darwin-arm64.node`,
  ];

  it('loads only the dev build in a checkout, from source or compiled, and names the target that writes it', () => {
    for (const modulePath of [`${checkout}/src/native.ts`, `${checkout}/dist/ts/native.js`]) {
      expect(search(modulePath, [sourceEntry])).toEqual({ paths: devTree, producer: 'cowshed:napi-native' });
    }
  });

  it('loads only the shipped tree in a package that holds no source', () => {
    const installed = '/app/node_modules/@smoothbricks/cowshed';
    expect(search(`${installed}/dist/ts/native.js`, [])).toEqual({
      paths: [
        `${installed}/dist/native/host/cowshed.darwin-arm64.node`,
        `${installed}/dist/native/darwin-arm64/cowshed.darwin-arm64.node`,
      ],
      producer: null,
    });
  });

  it('puts the napi-rs override first, and replaces the tree with the debug addon where the napi-test targets ask', () => {
    const compiled = `${checkout}/dist/ts/native.js`;
    expect(search(compiled, [sourceEntry], { override: '/elsewhere.node' }).paths).toEqual([
      '/elsewhere.node',
      ...devTree,
    ]);
    expect(search(compiled, [sourceEntry], { debug: true })).toEqual({
      paths: [`${checkout}/.cache/native-debug/cowshed.darwin-arm64.node`],
      producer: 'cowshed:napi-debug',
    });
  });
});
