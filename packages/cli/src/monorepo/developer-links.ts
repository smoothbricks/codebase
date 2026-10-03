/**
 * The developer links an install smoo runs keeps, as the CLI sees them.
 *
 * `managed/raw/tooling/direnv/developer-links.ts` holds the one
 * implementation. A managed repository's shell entry runs the copy smoo
 * writes into its `tooling/direnv/`; `smoo monorepo update` runs THIS
 * package's copy of the same file, so the shell and the command keep the same
 * links. It is resolved from the installed plugin, like the secret resolver,
 * so source and published layouts agree.
 */

import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  type KeepDeveloperLinksOptions,
  type KeepDeveloperLinks as ManagedKeepDeveloperLinks,
  managedAssetsRoot,
} from '@smoothbricks/nx-plugin/managed-assets';

const MODULE_PATH = join(managedAssetsRoot, 'raw/tooling/direnv/developer-links.ts');

/** Presence only: what a loaded function does is the managed module's contract. */
function isKeepDeveloperLinks(value: unknown): value is ManagedKeepDeveloperLinks {
  return typeof value === 'function';
}

/**
 * Runs `install` in `root` with every link a developer pointed outside the checkout put back afterwards, except
 * those of the packages `options.relink` names when `install` resolves.
 */
export async function keepDeveloperLinks<T>(
  root: string,
  install: () => Promise<T>,
  options?: KeepDeveloperLinksOptions,
): Promise<T> {
  // Dynamic by necessity: the target is a managed raw script outside this
  // package's compiled rootDir, so no static import can name it.
  const imported: unknown = await import(pathToFileURL(MODULE_PATH).href);
  const keep = typeof imported === 'object' && imported !== null ? Reflect.get(imported, 'keepDeveloperLinks') : null;
  if (!isKeepDeveloperLinks(keep)) {
    throw new Error(`${MODULE_PATH} does not export keepDeveloperLinks`);
  }
  return keep(root, install, options);
}
