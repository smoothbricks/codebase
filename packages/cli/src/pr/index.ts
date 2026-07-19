import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import typia from 'typia';
import { formatMarkerHits, scanRefChangedForMarkers } from '../lib/conflict-markers.js';
import { readJson } from '../lib/json.js';
import { run, runResult } from '../lib/run.js';

/** 0 = clean/done, 2 = action required (conflicts to resolve), 1 = usage/error. */
export type PrResolveExit = 0 | 1 | 2;

export interface PrResolveOptions {
  /** Git remote hosting the PR branch. Auto-inferred from the PR repo when omitted. */
  remote?: string;
  /** Abort an in-progress resolution: restore the original branch, drop state. */
  abort?: boolean;
}

export interface PrResolveShell {
  runResult(
    command: string,
    args: string[],
    cwd: string,
  ): Promise<{ exitCode: number; stdout: string; stderr: string }>;
  run(command: string, args: string[], cwd: string): Promise<void>;
}

const defaultShell: PrResolveShell = {
  runResult: (command, args, cwd) => runResult(command, args, cwd),
  run: (command, args, cwd) => run(command, args, cwd),
};

interface PrMeta {
  number: number;
  url: string;
  headBranch: string;
  baseBranch: string;
  crossRepo: boolean;
  nameWithOwner: string;
}

interface PrResolveState {
  pr: number;
  url: string;
  headBranch: string;
  baseBranch: string;
  remote: string;
  crossRepo: boolean;
  /** Branch to return to, or '' when the original HEAD was detached. */
  originalBranch: string;
  originalSha: string;
  startedAt: string;
}

interface GhPrViewJson {
  number: number;
  url: string;
  headRefName: string;
  baseRefName: string;
  isCrossRepository?: boolean;
}

interface PrResolveStateJson {
  pr: number;
  url?: string;
  headBranch: string;
  baseBranch: string;
  remote: string;
  crossRepo?: boolean;
  originalBranch?: string;
  originalSha?: string;
  startedAt?: string;
}

const parseGhPrViewJson = typia.json.createIsParse<GhPrViewJson>();
const isPrResolveStateJson = typia.createIs<PrResolveStateJson>();

/**
 * Agent-first conflict resolution for a GitHub PR.
 *
 * First run (pointed at a PR): reports the conflict markers and, if any, checks
 * out the PR branch and instructs the next step. Second run (on that branch,
 * after the human/agent has resolved + committed): verifies no markers remain,
 * pushes, and returns to the original branch. Idempotent via a state file under
 * the git dir.
 */
export async function resolvePrConflicts(
  root: string,
  prArg: string | undefined,
  options: PrResolveOptions,
  shell: PrResolveShell = defaultShell,
): Promise<PrResolveExit> {
  const statePath = join(await absoluteGitDir(shell, root), 'smoo', 'pr-resolve.json');
  const state = readState(statePath);

  if (options.abort === true) {
    return abortResolve(shell, root, statePath, state);
  }
  if (state) {
    return finishResolve(shell, root, statePath, state, prArg);
  }
  return startResolve(shell, root, statePath, prArg, options.remote);
}

async function startResolve(
  shell: PrResolveShell,
  root: string,
  statePath: string,
  prArg: string | undefined,
  remoteOption: string | undefined,
): Promise<PrResolveExit> {
  if (!prArg) {
    console.error('Point smoo at a PR: smoo pr resolve <number|url|branch> [--remote <name>]');
    return 1;
  }
  if (!(await isWorkingTreeClean(shell, root))) {
    console.error('Working tree is dirty. Commit or stash your changes before resolving a PR.');
    return 1;
  }

  const meta = await prMeta(shell, root, prArg);
  const remote = remoteOption ?? (await inferRemote(shell, root, meta.nameWithOwner));
  await fetchBranches(shell, root, remote, [meta.baseBranch, meta.headBranch]);

  const baseRef = `${remote}/${meta.baseBranch}`;
  const headRef = `${remote}/${meta.headBranch}`;
  const hits = await scanRefChangedForMarkers(shell, root, baseRef, headRef);
  if (hits.length === 0) {
    console.log(
      `✅ PR #${meta.number} (${meta.headBranch} → ${meta.baseBranch}) has no conflict markers — nothing to resolve.`,
    );
    console.log('Next: it is safe to Rebase-and-merge.');
    return 0;
  }

  const originalBranch = await currentBranch(shell, root);
  const originalSha = (await mustRun(shell, root, ['rev-parse', 'HEAD'])).trim();
  await checkoutPrBranch(shell, root, remote, meta);

  writeState(statePath, {
    pr: meta.number,
    url: meta.url,
    headBranch: meta.headBranch,
    baseBranch: meta.baseBranch,
    remote,
    crossRepo: meta.crossRepo,
    originalBranch,
    originalSha,
    startedAt: new Date().toISOString(),
  });

  const fileCount = hits.length;
  console.log(`⚠️  PR #${meta.number} has conflict markers in ${fileCount} file${fileCount === 1 ? '' : 's'}:`);
  console.log(formatMarkerHits(hits));
  console.log('');
  console.log(`Checked out '${meta.headBranch}' (was on '${originalBranch || originalSha.slice(0, 8)}').`);
  console.log('Next:');
  console.log('  1. Resolve each <<<<<<< / ======= / >>>>>>> block (keep the right content, delete the markers).');
  console.log('  2. Commit the resolution:  git add -A && git commit');
  console.log('  3. Run the SAME command again to verify, push, and return:  smoo pr resolve');
  return 2;
}

async function finishResolve(
  shell: PrResolveShell,
  root: string,
  statePath: string,
  state: PrResolveState,
  prArg: string | undefined,
): Promise<PrResolveExit> {
  if (prArg && !prArgMatchesState(prArg, state)) {
    console.error(
      `A resolution for PR #${state.pr} (${state.headBranch}) is already in progress.\n` +
        'Finish it with `smoo pr resolve` (no argument), or discard it with `smoo pr resolve --abort`.',
    );
    return 1;
  }

  const branch = await currentBranch(shell, root);
  if (branch !== state.headBranch) {
    console.error(
      `Expected to be on '${state.headBranch}' to finish resolving PR #${state.pr}, but HEAD is '${branch || 'detached'}'.\n` +
        `Check out the branch (git checkout ${state.headBranch}) or discard with \`smoo pr resolve --abort\`.`,
    );
    return 1;
  }
  if (!(await isWorkingTreeClean(shell, root))) {
    console.error('Your resolution is not committed yet.');
    console.error('Next: git add -A && git commit   — then run `smoo pr resolve` again.');
    return 2;
  }

  await fetchBranches(shell, root, state.remote, [state.baseBranch]);
  const hits = await scanRefChangedForMarkers(shell, root, `${state.remote}/${state.baseBranch}`, 'HEAD');
  if (hits.length > 0) {
    console.error(`Conflict markers still present in ${hits.length} file(s):`);
    console.error(formatMarkerHits(hits));
    console.error('Next: resolve the remaining markers, commit, then run `smoo pr resolve` again.');
    return 2;
  }

  await pushResolvedBranch(shell, root, state);
  await restoreOriginalBranch(shell, root, state);
  clearState(statePath);

  const back = state.originalBranch || state.originalSha.slice(0, 8);
  console.log(
    `✅ Resolved PR #${state.pr}: pushed '${state.headBranch}' to '${state.remote}' and returned to '${back}'.`,
  );
  console.log(`Next: PR #${state.pr} is clean now — mark it Ready / Rebase-and-merge.`);
  return 0;
}

async function abortResolve(
  shell: PrResolveShell,
  root: string,
  statePath: string,
  state: PrResolveState | null,
): Promise<PrResolveExit> {
  if (!state) {
    console.log('No conflict resolution in progress.');
    return 0;
  }
  await restoreOriginalBranch(shell, root, state);
  clearState(statePath);
  const back = state.originalBranch || state.originalSha.slice(0, 8);
  console.log(`Aborted resolution of PR #${state.pr}; returned to '${back}'. The PR branch is unchanged.`);
  return 0;
}

async function prMeta(shell: PrResolveShell, root: string, prArg: string): Promise<PrMeta> {
  const fields = 'number,url,headRefName,baseRefName,isCrossRepository';
  const result = await shell.runResult('gh', ['pr', 'view', prArg, '--json', fields], root);
  if (result.exitCode !== 0) {
    throw new Error(`Could not resolve PR '${prArg}' via gh: ${result.stderr.trim() || `exit ${result.exitCode}`}`);
  }
  const raw = parseGhPrViewJson(result.stdout);
  if (!raw) {
    throw new Error(`gh pr view returned unexpected JSON for '${prArg}'.`);
  }
  const match = /github\.com\/([^/]+\/[^/]+)\/pull\//.exec(raw.url);
  return {
    number: raw.number,
    url: raw.url,
    headBranch: raw.headRefName,
    baseBranch: raw.baseRefName,
    crossRepo: raw.isCrossRepository === true,
    nameWithOwner: match ? match[1] : '',
  };
}

/** Pick the git remote whose URL points at `nameWithOwner`; fall back to origin. */
async function inferRemote(shell: PrResolveShell, root: string, nameWithOwner: string): Promise<string> {
  if (nameWithOwner.length === 0) {
    return 'origin';
  }
  const result = await shell.runResult('git', ['remote', '-v'], root);
  const needle = nameWithOwner.toLowerCase();
  for (const line of result.stdout.split('\n')) {
    const [name, url] = line.split(/\s+/);
    if (name && url?.toLowerCase().includes(needle)) {
      return name;
    }
  }
  return 'origin';
}

async function checkoutPrBranch(shell: PrResolveShell, root: string, remote: string, meta: PrMeta): Promise<void> {
  if (meta.crossRepo) {
    // Fork PR: let gh set up the fork remote + tracking branch correctly.
    await shell.run('gh', ['pr', 'checkout', String(meta.number)], root);
    return;
  }
  await shell.run('git', ['checkout', '-B', meta.headBranch, `${remote}/${meta.headBranch}`], root);
}

async function pushResolvedBranch(shell: PrResolveShell, root: string, state: PrResolveState): Promise<void> {
  const refspec = `HEAD:refs/heads/${state.headBranch}`;
  const ff = await shell.runResult('git', ['push', state.remote, refspec], root);
  if (ff.exitCode === 0) {
    return;
  }
  // The resolution may have been rebased/amended onto a moved head; the branch is
  // the review branch we own, so force-with-lease is the safe way to update it.
  console.log('Fast-forward push rejected; retrying with --force-with-lease (review branch).');
  const forced = await shell.runResult('git', ['push', '--force-with-lease', state.remote, refspec], root);
  if (forced.exitCode !== 0) {
    throw new Error(
      `Failed to push '${state.headBranch}' to '${state.remote}': ${forced.stderr.trim() || ff.stderr.trim()}`,
    );
  }
}

async function restoreOriginalBranch(shell: PrResolveShell, root: string, state: PrResolveState): Promise<void> {
  const target = state.originalBranch.length > 0 ? state.originalBranch : state.originalSha;
  await shell.run('git', ['checkout', target], root);
}

async function fetchBranches(shell: PrResolveShell, root: string, remote: string, branches: string[]): Promise<void> {
  const refspecs = branches.map((branch) => `+refs/heads/${branch}:refs/remotes/${remote}/${branch}`);
  await shell.run('git', ['fetch', remote, ...refspecs], root);
}

async function currentBranch(shell: PrResolveShell, root: string): Promise<string> {
  const result = await shell.runResult('git', ['symbolic-ref', '--quiet', '--short', 'HEAD'], root);
  return result.exitCode === 0 ? result.stdout.trim() : '';
}

async function isWorkingTreeClean(shell: PrResolveShell, root: string): Promise<boolean> {
  const result = await shell.runResult('git', ['status', '--porcelain'], root);
  return result.exitCode === 0 && result.stdout.trim().length === 0;
}

async function absoluteGitDir(shell: PrResolveShell, root: string): Promise<string> {
  const result = await shell.runResult('git', ['rev-parse', '--absolute-git-dir'], root);
  if (result.exitCode !== 0) {
    throw new Error(`Not a git repository at ${root}: ${result.stderr.trim()}`);
  }
  return result.stdout.trim();
}

async function mustRun(shell: PrResolveShell, root: string, args: string[]): Promise<string> {
  const result = await shell.runResult('git', args, root);
  if (result.exitCode !== 0) {
    throw new Error(`git ${args.join(' ')} failed: ${result.stderr.trim() || `exit ${result.exitCode}`}`);
  }
  return result.stdout;
}

function prArgMatchesState(prArg: string, state: PrResolveState): boolean {
  if (prArg === state.headBranch || prArg === state.url) {
    return true;
  }
  const numeric = prArg.startsWith('#') ? prArg.slice(1) : prArg;
  return numeric === String(state.pr);
}

function readState(statePath: string): PrResolveState | null {
  let raw: unknown;
  try {
    raw = readJson(statePath);
  } catch {
    return null;
  }
  if (!isPrResolveStateJson(raw)) {
    return null;
  }
  return {
    pr: raw.pr,
    url: raw.url ?? '',
    headBranch: raw.headBranch,
    baseBranch: raw.baseBranch,
    remote: raw.remote,
    crossRepo: raw.crossRepo === true,
    originalBranch: raw.originalBranch ?? '',
    originalSha: raw.originalSha ?? '',
    startedAt: raw.startedAt ?? '',
  };
}

function writeState(statePath: string, state: PrResolveState): void {
  mkdirSync(dirname(statePath), { recursive: true });
  writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`);
}

function clearState(statePath: string): void {
  rmSync(statePath, { force: true });
}
