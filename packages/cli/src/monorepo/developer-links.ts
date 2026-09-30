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
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';

const MODULE_PATH = join(managedAssetsRoot, 'raw/tooling/direnv/developer-links.ts');

type KeepDeveloperLinks = (root: string, install: () => Promise<void>) => Promise<unknown>;

/** Presence only: what a loaded function does is the managed module's contract. */
function isKeepDeveloperLinks(value: unknown): value is KeepDeveloperLinks {
  return typeof value === 'function';
}

/** Runs `install` in `root` with every link a developer pointed outside the checkout put back afterwards. */
export async function keepDeveloperLinks(root: string, install: () => Promise<void>): Promise<void> {
  // Dynamic by necessity: the target is a managed raw script outside this
  // package's compiled rootDir, so no static import can name it.
  const imported: unknown = await import(pathToFileURL(MODULE_PATH).href);
  const keep = typeof imported === 'object' && imported !== null ? Reflect.get(imported, 'keepDeveloperLinks') : null;
  if (!isKeepDeveloperLinks(keep)) {
    throw new Error(`${MODULE_PATH} does not export keepDeveloperLinks`);
  }
  await keep(root, install);
}
