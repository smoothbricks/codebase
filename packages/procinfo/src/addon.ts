/// <reference types="node" />

import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { basename, dirname, join } from 'node:path';

/**
 * Each host procinfo ships an addon for, by `<platform>-<arch>`, to the directory napi names its family in. The glibc
 * Linux families carry `-gnu`. package.json restates the triples as `napi.targets`; native.test.ts checks that.
 */
export const ADDON_DIRECTORIES: ReadonlyMap<string, string> = new Map([
  ['darwin-arm64', 'darwin-arm64'],
  ['darwin-x64', 'darwin-x64'],
  ['linux-arm64', 'linux-arm64-gnu'],
  ['linux-x64', 'linux-x64-gnu'],
]);

/** The addon's file name in a family's directory, as `napi build` writes it. */
export function addonFileName(directory: string): string {
  return `procinfo.${directory}.node`;
}

/** The package root of a module at `src/<file>` (run from source) or `dist/ts/<file>` (built). */
function packageRoot(modulePath: string): string {
  const directory = dirname(modulePath);
  return basename(directory) === 'src' ? dirname(directory) : dirname(dirname(directory));
}

/**
 * Where the addon is, for the package at `root`: a checkout (it holds the addon crate's source) loads the development
 * build `procinfo:napi-native` writes under `.cache/native-dev`, an installed package the `dist/native` it ships, and
 * the napi-test target (`NAPI_DEBUG_ADDON=1`) the debug build `procinfo:napi-debug` copies to `.cache/native-debug`.
 * napi-rs's own `NAPI_RS_NATIVE_LIBRARY_PATH` comes first. A tree never falls back to another.
 */
export function addonPaths(search: {
  readonly root: string;
  readonly directory: string;
  readonly exists: (path: string) => boolean;
  readonly override: string | undefined;
  readonly debug: boolean;
}): { readonly paths: readonly string[]; readonly producer: string | null } {
  const fileName = addonFileName(search.directory);
  const override = search.override ? [search.override] : [];
  if (search.debug) {
    return {
      paths: [...override, join(search.root, '.cache/native-debug', fileName)],
      producer: 'procinfo:napi-debug',
    };
  }
  const checkout = search.exists(join(search.root, 'crates/procinfo-napi/src/lib.rs'));
  const tree = checkout ? '.cache/native-dev' : 'dist/native';
  return {
    paths: [...override, join(search.root, tree, search.directory, fileName)],
    producer: checkout ? 'procinfo:napi-native' : null,
  };
}

/**
 * The host's addon, required from the first of {@link addonPaths} for the module at `modulePath` whose loaded module
 * `accept` takes. A host procinfo ships no addon for, or no path that loads, throws naming every path tried and, in a
 * checkout, the Nx target that writes it.
 */
export function loadAddon<T>(modulePath: string, accept: (module: unknown) => T): T {
  const directory = ADDON_DIRECTORIES.get(`${process.platform}-${process.arch}`);
  if (directory === undefined) {
    throw new Error(`procinfo ships no addon for ${process.platform}-${process.arch}`);
  }
  const { paths, producer } = addonPaths({
    root: packageRoot(modulePath),
    directory,
    exists: existsSync,
    override: process.env.NAPI_RS_NATIVE_LIBRARY_PATH,
    debug: process.env.NAPI_DEBUG_ADDON === '1',
  });
  const require = createRequire(modulePath);
  let lastError: unknown;
  for (const path of paths) {
    try {
      return accept(require(path));
    } catch (error) {
      lastError = error;
    }
  }
  const remedy = producer === null ? '' : ` Run "nx run ${producer}" for this platform.`;
  throw new Error(`Could not load ${addonFileName(directory)} from ${paths.join(', ')}.${remedy}`, {
    cause: lastError,
  });
}
