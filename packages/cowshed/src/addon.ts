/// <reference types="node" />

import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { addonDirectory, addonFileName, addonSearch } from './platform.js';

/**
 * The host's addon, required from the first path {@link addonSearch} names for the module at
 * `modulePath` whose loaded module `accept` takes. The module's own location decides the package
 * root, and so the tree it loads from. Every entry that loads the addon searches this one way: the
 * root entry accepts a module carrying every operation it binds, `process-lineage` one carrying its
 * own export. A host the package ships no addon for, or no path that loads, throws naming every
 * path tried and the Nx target that writes them in a checkout.
 */
export function loadAddon<T>(modulePath: string, accept: (module: unknown) => T): T {
  const directory = addonDirectory(process.platform, process.arch);
  if (directory === null) {
    throw new Error(`Unsupported Cowshed native target: ${process.platform}-${process.arch}`);
  }
  const fileName = addonFileName(directory);
  const { paths, producer } = addonSearch({
    modulePath,
    exists: existsSync,
    directory,
    fileName,
    // napi-rs loaders honour NAPI_RS_NATIVE_LIBRARY_PATH first; this is that same hook, not a
    // bespoke cowshed name. The override stays — only the spelling is the ecosystem one.
    override: process.env.NAPI_RS_NATIVE_LIBRARY_PATH,
    // NAPI_DEBUG_ADDON is set only by the napi-test targets: the test suite loads the dev-profile
    // addon of its own graph from .cache/native-debug (never packaged) instead of the dev build.
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
  throw new Error(`Could not load ${fileName} from ${paths.join(', ')}.${remedy}`, { cause: lastError });
}
