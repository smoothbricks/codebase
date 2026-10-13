import { appendFileSync, existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { ManagedFileContext } from '@smoothbricks/nx-plugin/managed-files/files';
import { generateManagedFiles, generateManagedFilesForContext } from '@smoothbricks/nx-plugin/managed-files/generator';
import type { FileResult } from '@smoothbricks/nx-plugin/managed-files/tree';
import { FsTree } from 'nx/src/generators/tree.js';
import typia from 'typia';
import { loadNxProjects } from '../nx/index.js';
import { finishManagedFiles } from './managed-fs.js';

export * from '@smoothbricks/nx-plugin/managed-files/context';
export {
  type ManagedFileContext,
  managedFileTargetsForContext,
  managedFileTargetsForTest,
  renderManagedWorkflowForTest,
} from '@smoothbricks/nx-plugin/managed-files/files';
export {
  INLINE_LOCAL_BEGIN,
  INLINE_LOCAL_END,
  LOCAL_SECTION_MARKER,
} from '@smoothbricks/nx-plugin/managed-files/managed-content';
export type { FileResult } from '@smoothbricks/nx-plugin/managed-files/tree';

export async function applyManagedFiles(root: string, mode: 'update' | 'check' | 'diff'): Promise<FileResult[]> {
  const projects = await loadNxProjects(root);
  const tree = new FsTree(root, false);
  return finishManagedFiles(root, tree, await generateManagedFiles(tree, projects), mode);
}

/**
 * Apply against an already-resolved context. The context is the only part of
 * this flow that needs a real Nx graph, so taking it as an argument lets a
 * fixture repository exercise the whole write-and-compare path.
 */
export async function applyManagedFilesForContext(
  root: string,
  mode: 'update' | 'check' | 'diff',
  context: ManagedFileContext,
): Promise<FileResult[]> {
  const tree = new FsTree(root, false);
  return finishManagedFiles(root, tree, await generateManagedFilesForContext(tree, context), mode);
}

export function printResults(results: FileResult[]): void {
  for (const result of results) {
    console.log(`${result.action.padEnd(15)} ${result.target}${result.reason ? ` — ${result.reason}` : ''}`);
  }
}

export async function validateManagedFiles(root: string): Promise<number> {
  const results = await applyManagedFiles(root, 'check');
  printResults(results);
  const failures = results.filter((result) => result.action === 'drifted').length;
  if (failures > 0) {
    console.error('Managed monorepo files are out of date. Run: smoo monorepo update');
  }
  return failures;
}

/** The devenv module is inert until the repo-owned devenv.nix imports it. */
export const DEVENV_MODULE_IMPORT = './devenv.smoo.nix';

/**
 * Report, never rewrite: the import belongs to a repo-owned Nix file whose
 * shape this tool has no business editing. A missing import would otherwise be
 * silent — the managed module simply would not apply.
 */
export function validateDevenvModuleImport(root: string): number {
  const target = join(root, 'tooling/direnv/devenv.nix');
  if (!existsSync(target)) {
    return 0;
  }
  if (readFileSync(target, 'utf8').includes(DEVENV_MODULE_IMPORT)) {
    return 0;
  }
  console.error(
    `tooling/direnv/devenv.nix does not import ${DEVENV_MODULE_IMPORT}: add "imports = [${DEVENV_MODULE_IMPORT}];" so the managed shell contract applies`,
  );
  return 1;
}

/** The only part of devenv.lock this check reads: the node install-devenv installs the CLI from. */
interface DevenvLock {
  nodes: { devenv?: { locked?: { rev?: string }; original?: { ref?: string; rev?: string } } };
}

/** devenv.lock is repo-owned text, so it is validated rather than cast. */
const parseDevenvLock = typia.json.createIsParse<DevenvLock>();

/**
 * What a devenv input declared at `latest` or a `v*` tag leaves in the lock's
 * `original.ref`. A `git+https` URL can name a tag only as `refs/tags/<tag>`.
 */
const DEVENV_RELEASE_REF = /^(?:refs\/tags\/)?(?:latest|v(\d+(?:\.\d+)*))$/;

/**
 * devenv.cachix.org has pinned the aarch64-darwin, aarch64-linux and
 * x86_64-linux builds of every release from this one on for good, and the
 * x86_64-darwin builds up to v2.1.2, since from v2.2 on devenv builds nothing
 * for x86_64-darwin. Some older tags, such as v2.0.3, have no pin, and their
 * builds are gone.
 */
const OLDEST_PINNED_DEVENV_RELEASE = '2.0.4';

/** Whether a lock's `original.ref` names `latest`, which is always the newest release, or a pinned release. */
function isPinnedDevenvRelease(ref: string): boolean {
  const release = DEVENV_RELEASE_REF.exec(ref);
  if (release === null) {
    return false;
  }
  const version = release[1];
  // Numeric collation orders dotted versions: 2.0 < 2.0.4 < 2.1 < 2.10.
  return version === undefined || version.localeCompare(OLDEST_PINNED_DEVENV_RELEASE, 'en', { numeric: true }) >= 0;
}

/**
 * Report, never rewrite: re-locking is `devenv update`'s job. On a hosted
 * runner install-devenv installs the CLI at the commit this node locks.
 * devenv.cachix.org keeps the aarch64-darwin, aarch64-linux and x86_64-linux
 * builds of every release from v2.0.4 on for good, but drops a build of
 * devenv's main after about six weeks, and from then on every cold runner
 * compiles devenv from source. An input nobody declared locks main as it was
 * that minute. The check reads the tag offline: asking the cache would pass on
 * the day of the re-lock and fail six weeks later.
 */
export function validateDevenvReleaseLock(root: string): number {
  const target = join(root, 'tooling/direnv/devenv.lock');
  if (!existsSync(target)) {
    return 0;
  }
  let lock: DevenvLock | null;
  try {
    lock = parseDevenvLock(readFileSync(target, 'utf8'));
  } catch {
    lock = null;
  }
  if (lock === null) {
    console.error('tooling/direnv/devenv.lock is not a devenv lock: it must be JSON with a nodes object');
    return 1;
  }
  const node = lock.nodes.devenv;
  const ref = node?.original?.ref;
  // A rev declared beside the tag (a git+https URL takes both) is what Nix locks, whichever commit it names.
  const revBesideRef = ref !== undefined && node?.original?.rev !== undefined;
  if (node === undefined || (ref !== undefined && !revBesideRef && isPinnedDevenvRelease(ref))) {
    return 0;
  }
  const commit = node.locked?.rev?.slice(0, 7) ?? 'an unknown commit';
  let lockedAt = ref === undefined ? commit : `${ref} (${commit})`;
  if (revBesideRef) {
    lockedAt = `${commit} (a rev declared beside ${ref})`;
  }
  const oldest = `v${OLDEST_PINNED_DEVENV_RELEASE}`;
  console.error(
    `tooling/direnv/devenv.lock locks devenv at ${lockedAt}, not at latest or a release from ${oldest} on.\n` +
      '  On a GitHub-hosted runner install-devenv installs the devenv CLI at that commit. devenv.cachix.org keeps\n' +
      `  the aarch64-darwin, aarch64-linux and x86_64-linux builds of every release from ${oldest} on for good.\n` +
      "  Some older tags, such as v2.0.3, have no pin and their builds are gone, and a build of devenv's main is\n" +
      '  dropped after about six weeks. Once the build is gone, every cold runner compiles devenv from source.\n' +
      '  Declare the input at a release in tooling/direnv/devenv.yaml:\n' +
      '    inputs:\n' +
      '      devenv:\n' +
      '        url: github:cachix/devenv/latest?dir=src/modules   # or a tag, such as v2.3.1\n' +
      '  and re-lock that input alone: run `devenv update devenv` in tooling/direnv.',
  );
  return 1;
}

/**
 * Non-blocking drift report: prints the per-file table and surfaces drift as
 * GitHub Actions warning annotations (plain stderr elsewhere) without failing
 * the run. Managed-file drift is derived state with its own remediation flow
 * (the persistent managed-files PR); only `monorepo check` without --warn and
 * PR-scoped gates treat it as an error.
 *
 * Under GitHub Actions the drifted-file count is published as the step output
 * `drifted`, so downstream steps gate declaratively instead of parsing logs.
 */
export async function warnOnManagedFileDrift(root: string): Promise<void> {
  const results = await applyManagedFiles(root, 'check');
  printResults(results);
  const drifted = results.filter((result) => result.action === 'drifted');
  if (process.env.GITHUB_OUTPUT) {
    appendFileSync(process.env.GITHUB_OUTPUT, `drifted=${drifted.length}\n`);
  }
  if (drifted.length === 0) {
    return;
  }
  if (process.env.GITHUB_ACTIONS === 'true') {
    for (const result of drifted) {
      console.log(
        `::warning title=Managed file drift::${result.target} drifted from the @smoothbricks/cli template; run 'smoo monorepo update'`,
      );
    }
  }
  console.error(`${drifted.length} managed monorepo file(s) drifted (non-blocking). Run: smoo monorepo update`);
}
