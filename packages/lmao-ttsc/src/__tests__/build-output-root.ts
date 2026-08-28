import { mkdir, mkdtemp } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Parent of every `Bun.build` output directory the integration tests create.
 *
 * Two constraints fix this location, and only a directory satisfying both works.
 *
 * It must be invisible to `@ttsc/unplugin`'s generation proof. The plugin walks
 * the whole project root before and after a transform and rejects the
 * generation when any walked directory's membership moved
 * (`project/directory-membership-changed`), which a sibling build writing into
 * the project reliably triggers: these tests compile plugin-ON and plugin-OFF
 * concurrently, and Bun runs several test files at once. The walk is not
 * limited to the tsconfig `include` globs and offers no opt-out key, but it
 * skips directory entries named `node_modules`, so nothing under this path is
 * ever walked, hashed, or watched.
 *
 * It must also stay inside the package. The emitted bundles keep
 * `@smoothbricks/lmao` external and are executed as files, and Bun resolves a
 * bare specifier by walking parent directories of the importing file; from
 * `os.tmpdir()` that walk finds no `node_modules` and the externals fail.
 */
const buildOutputRoot = fileURLToPath(
  new URL('../../node_modules/.cache/lmao-ttsc-integration-builds/', import.meta.url),
);

/**
 * Create a private output directory named after the calling test.
 *
 * `node_modules` exists before any test runs — the test files cannot resolve
 * their own imports otherwise — so creating the cache directories underneath it
 * never mutates a directory the project walk observes.
 */
export async function makeBuildOutputRoot(testName: string): Promise<string> {
  await mkdir(buildOutputRoot, { recursive: true });
  return mkdtemp(join(buildOutputRoot, `${testName}-`));
}
