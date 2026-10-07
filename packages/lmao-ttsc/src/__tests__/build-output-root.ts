import { mkdtemp, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * The package's own `node_modules`, which every emitted bundle resolves its
 * externals from.
 */
const packageNodeModules = fileURLToPath(new URL('../../node_modules', import.meta.url));

/**
 * Create a private `Bun.build` output directory named after the calling test.
 *
 * Two constraints fix where and what it is.
 *
 * It must be invisible to `@ttsc/unplugin`'s generation proof. The plugin walks
 * the whole project root before and after a transform and rejects the
 * generation when any walked directory's membership moved
 * (`project/directory-membership-changed`), which a sibling build writing into
 * the project reliably triggers: these tests compile plugin-ON and plugin-OFF
 * concurrently, and Bun runs several test files at once. The walk is not
 * limited to the tsconfig `include` globs and offers no opt-out key, so the
 * directory lives outside the project, under the temp directory.
 *
 * Its bundles must still resolve the package's externals. They keep
 * `@smoothbricks/lmao` external and are executed as files, and Bun resolves a
 * bare specifier by walking the parent directories of the importing file's
 * real path. A `node_modules` link to the package's own puts the answer on that
 * walk wherever the directory physically lives. Placement inside the package
 * did not: `node_modules/.cache` is a link to a cowshed build volume, so the
 * real path of a bundle written there had no `node_modules/@smoothbricks` above
 * it and every external failed to resolve.
 */
export async function makeBuildOutputRoot(testName: string): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), `lmao-ttsc-${testName}-`));
  await symlink(packageNodeModules, join(root, 'node_modules'), 'dir');
  return root;
}
