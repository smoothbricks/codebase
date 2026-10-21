import { appendFileSync, existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { ManagedFileContext } from '@smoothbricks/nx-plugin/managed-files/files';
import { generateManagedFiles, generateManagedFilesForContext } from '@smoothbricks/nx-plugin/managed-files/generator';
import type { FileResult } from '@smoothbricks/nx-plugin/managed-files/tree';
import { FsTree } from 'nx/src/generators/tree.js';
import typia from 'typia';
import { runResult } from '../lib/run.js';
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
  nodes: { devenv?: { locked?: LockedDevenvInput } };
}

/**
 * The input as Nix locked it: a `github` input names its remote by owner and repo (and a host for GitHub Enterprise),
 * a `git` input by url. `rev` is the commit install-devenv installs.
 */
interface LockedDevenvInput {
  type?: string;
  owner?: string;
  repo?: string;
  host?: string;
  url?: string;
  rev?: string;
}

/** devenv.lock is repo-owned text, so it is validated rather than cast. */
const parseDevenvLock = typia.json.createIsParse<DevenvLock>();

/** A full commit hash: the only `rev` that may reach git's argv. */
const COMMIT_HASH = /^[0-9a-f]{40}$/;

/**
 * A release tag in `git ls-remote` output. An annotated tag's own line names the tag object; the commit it tags is on
 * the `^{}` line after it.
 */
const DEVENV_RELEASE_TAG = /^refs\/tags\/(v(\d+(?:\.\d+)*))(?:\^\{\})?$/;

/**
 * devenv.cachix.org has pinned the aarch64-darwin, aarch64-linux and
 * x86_64-linux builds of every release from this one on for good, and the
 * x86_64-darwin builds up to v2.1.2, since from v2.2 on devenv builds nothing
 * for x86_64-darwin. Some older tags, such as v2.0.3, have no pin, and their
 * builds are gone.
 */
const OLDEST_PINNED_DEVENV_RELEASE = '2.0.4';
const OLDEST_PINNED_DEVENV_TAG = `v${OLDEST_PINNED_DEVENV_RELEASE}`;

type DevenvRemote = { kind: 'remote'; url: string } | { kind: 'none'; reason: string };

/** The remote whose tags say whether the locked commit is a release. */
function devenvRemote(locked: LockedDevenvInput | undefined): DevenvRemote {
  if (locked?.type === undefined) {
    return { kind: 'none', reason: 'no locked input' };
  }
  switch (locked.type) {
    case 'github':
      return locked.owner === undefined || locked.repo === undefined
        ? { kind: 'none', reason: 'a github input without an owner and a repo' }
        : { kind: 'remote', url: `https://${locked.host ?? 'github.com'}/${locked.owner}/${locked.repo}` };
    case 'git':
      return locked.url === undefined
        ? { kind: 'none', reason: 'a git input without a url' }
        : { kind: 'remote', url: locked.url };
    default:
      return { kind: 'none', reason: `a ${locked.type} input, which names no git remote` };
  }
}

/** A release tag the remote lists at the locked commit, with its version for the comparison. */
interface ReleaseTag {
  tag: string;
  version: string;
}

type ReleaseTags = { kind: 'listed'; tags: ReleaseTag[] } | { kind: 'unreadable'; detail: string };

/** The release tags `url` has at `rev`, as the remote lists them now. */
async function releaseTagsAt(root: string, url: string, rev: string): Promise<ReleaseTags> {
  // No terminal prompt: a remote that wants credentials fails here instead of waiting on a hook's stdin.
  const listed = await runResult('git', ['ls-remote', '--tags', '--', url, 'refs/tags/v*'], root, {
    GIT_TERMINAL_PROMPT: '0',
  });
  if (listed.exitCode !== 0) {
    return { kind: 'unreadable', detail: listed.stderr.trim() || `git ls-remote exited ${listed.exitCode}` };
  }
  const tags: ReleaseTag[] = [];
  for (const line of listed.stdout.split('\n')) {
    const [hash, ref] = line.split('\t');
    const release = ref === undefined ? null : DEVENV_RELEASE_TAG.exec(ref);
    if (hash === rev && release?.[1] !== undefined && release[2] !== undefined) {
      tags.push({ tag: release[1], version: release[2] });
    }
  }
  return { kind: 'listed', tags };
}

const DEVENV_LOCK_PATH = 'tooling/direnv/devenv.lock';

const RELOCK_DEVENV_AT_A_RELEASE =
  '  Declare the input at a release in tooling/direnv/devenv.yaml:\n' +
  '    inputs:\n' +
  '      devenv:\n' +
  '        url: github:cachix/devenv/latest?dir=src/modules   # or a tag, such as v2.3.1\n' +
  '  and re-lock that input alone: run `devenv update devenv` in tooling/direnv.';

/**
 * Report, never rewrite: re-locking is `devenv update`'s job. On a hosted
 * runner install-devenv installs the CLI at the commit this node locks.
 * devenv.cachix.org keeps the aarch64-darwin, aarch64-linux and x86_64-linux
 * builds of every release tag from v2.0.4 on for good, and only those: it
 * drops the build of any other commit after about six weeks, an ancestor of a
 * release included, and from then on every cold runner compiles devenv from
 * source. So the lock passes when it names a remote and a commit that one of
 * that remote's release tags from v2.0.4 on names, however devenv.yaml
 * declared the input. The tags are asked of the remote with `git ls-remote` on
 * every run, and nothing is kept: a remote that cannot be listed, offline
 * included, fails the check rather than passes it. A pushed tag is permanent,
 * which is why asking the remote holds where asking the cache would pass on
 * the day of the re-lock and fail six weeks later.
 */
export async function validateDevenvReleaseLock(root: string): Promise<number> {
  const target = join(root, DEVENV_LOCK_PATH);
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
    console.error(`${DEVENV_LOCK_PATH} is not a devenv lock: it must be JSON with a nodes object`);
    return 1;
  }
  const node = lock.nodes.devenv;
  if (node === undefined) {
    return 0;
  }
  const remote = devenvRemote(node.locked);
  const rev = node.locked?.rev;
  if (remote.kind === 'none' || rev === undefined || !COMMIT_HASH.test(rev)) {
    const missing =
      remote.kind === 'none' ? `no remote (${remote.reason})` : `no commit hash (rev: ${rev ?? 'absent'})`;
    console.error(
      `${DEVENV_LOCK_PATH} locks devenv with ${missing}, so nothing can tell whether it is a release.\n` +
        RELOCK_DEVENV_AT_A_RELEASE,
    );
    return 1;
  }
  const listed = await releaseTagsAt(root, remote.url, rev);
  if (listed.kind === 'unreadable') {
    console.error(
      `${DEVENV_LOCK_PATH} locks devenv at ${rev}, and the check cannot list the release tags of ${remote.url} to tell whether a release names it:\n` +
        `  ${listed.detail}\n` +
        '  The check asks the remote on every run and keeps no copy of its tags, so it fails rather than passes\n' +
        '  offline. Run it again with the remote reachable.',
    );
    return 1;
  }
  // Numeric collation orders dotted versions: 2.0 < 2.0.4 < 2.1 < 2.10.
  if (
    listed.tags.some(({ version }) => version.localeCompare(OLDEST_PINNED_DEVENV_RELEASE, 'en', { numeric: true }) >= 0)
  ) {
    return 0;
  }
  const named =
    listed.tags.length === 0
      ? 'which no release tag names'
      : `which only ${listed.tags.map(({ tag }) => tag).join(' and ')} name${listed.tags.length === 1 ? 's' : ''}`;
  console.error(
    `${DEVENV_LOCK_PATH} locks devenv at ${rev} from ${remote.url}, ${named}, not a release from ${OLDEST_PINNED_DEVENV_TAG} on.\n` +
      '  On a GitHub-hosted runner install-devenv installs the devenv CLI at that commit. devenv.cachix.org keeps\n' +
      `  the aarch64-darwin, aarch64-linux and x86_64-linux builds of every release tag from ${OLDEST_PINNED_DEVENV_TAG} on\n` +
      '  for good, and only those. Some older tags, such as v2.0.3, have no pin and their builds are gone, and the\n' +
      '  cache drops the build of any other commit, an ancestor of a release included, after about six weeks. Once\n' +
      '  the build is gone, every cold runner compiles devenv from source.\n' +
      RELOCK_DEVENV_AT_A_RELEASE,
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
