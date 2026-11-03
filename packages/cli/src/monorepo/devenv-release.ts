import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { FileResult } from '@smoothbricks/nx-plugin/managed-files/tree';
import typia from 'typia';
import { runResult } from '../lib/run.js';

export const DEVENV_LOCK_PATH = 'tooling/direnv/devenv.lock';

/** The verdict beside devenv.lock, committed with it: which release tag names the commit the lock pins. */
export const DEVENV_RELEASE_RECORD_PATH = 'tooling/direnv/devenv-release.json';

const RECORD_COMMAND = 'smoo devenv record-release';

/** The only part of devenv.lock this module reads: the node install-devenv installs the CLI from. */
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

/**
 * The verdict for one locked commit: `tag`, a release tag of `remote` from v2.0.4 on, names `rev`. Written only after
 * the remote said so, and valid only for exactly that remote and commit.
 */
export interface DevenvReleaseRecord {
  remote: string;
  rev: string;
  tag: string;
}

/** The record is repo-owned text too. */
const parseDevenvReleaseRecord = typia.json.createIsParse<DevenvReleaseRecord>();

/** A full commit hash: the only `rev` that may reach git's argv. */
const COMMIT_HASH = /^[0-9a-f]{40}$/;

/**
 * A release tag in `git ls-remote` output. An annotated tag's own line names the tag object; the commit it tags is on
 * the `^{}` line after it.
 */
const DEVENV_RELEASE_TAG = /^refs\/tags\/(v(\d+(?:\.\d+)*))(?:\^\{\})?$/;

/** A release tag as the record names it. */
const RECORDED_TAG = /^v(\d+(?:\.\d+)*)$/;

/**
 * devenv.cachix.org has pinned the aarch64-darwin, aarch64-linux and
 * x86_64-linux builds of every release from this one on for good, and the
 * x86_64-darwin builds up to v2.1.2, since from v2.2 on devenv builds nothing
 * for x86_64-darwin. Some older tags, such as v2.0.3, have no pin, and their
 * builds are gone.
 */
const OLDEST_PINNED_DEVENV_RELEASE = '2.0.4';
const OLDEST_PINNED_DEVENV_TAG = `v${OLDEST_PINNED_DEVENV_RELEASE}`;

/** Numeric collation orders dotted versions: 2.0 < 2.0.4 < 2.1 < 2.10. */
const compareVersions = (a: string, b: string): number => a.localeCompare(b, 'en', { numeric: true });

const isPinnedRelease = (version: string): boolean => compareVersions(version, OLDEST_PINNED_DEVENV_RELEASE) >= 0;

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

const RELOCK_DEVENV_AT_A_RELEASE =
  '  Declare the input at a release in tooling/direnv/devenv.yaml:\n' +
  '    inputs:\n' +
  '      devenv:\n' +
  '        url: github:cachix/devenv/latest?dir=src/modules   # or a tag, such as v2.3.1\n' +
  '  Then re-lock that input alone (`devenv update devenv` in tooling/direnv) and record the verdict:\n' +
  `    ${RECORD_COMMAND}`;

const WHY_A_RELEASE =
  '  On a GitHub-hosted runner install-devenv installs the devenv CLI at that commit. devenv.cachix.org keeps\n' +
  `  the aarch64-darwin, aarch64-linux and x86_64-linux builds of every release tag from ${OLDEST_PINNED_DEVENV_TAG} on\n` +
  '  for good, and only those. Some older tags, such as v2.0.3, have no pin and their builds are gone, and the\n' +
  '  cache drops the build of any other commit, an ancestor of a release included, after about six weeks. Once\n' +
  '  the build is gone, every cold runner compiles devenv from source.\n';

const RECORD_THE_VERDICT =
  '  The check asks no remote: it only verifies the verdict recorded for the locked commit. Record it once, with the\n' +
  '  remote reachable, and commit the file beside devenv.lock:\n' +
  `    ${RECORD_COMMAND}\n` +
  '  (`smoo devenv update`, which bumps the lock, and `smoo monorepo update` record it too.)';

/** What devenv.lock pins: the remote whose tags answer, and the commit they are asked about. */
interface LockedDevenv {
  remote: string;
  rev: string;
}

type LockedDevenvRead =
  /** No lock, or none with a devenv node: nothing to hold to a release. */
  { kind: 'absent' } | { kind: 'locked'; locked: LockedDevenv } | { kind: 'refused'; message: string };

function readLockedDevenv(root: string): LockedDevenvRead {
  const target = join(root, DEVENV_LOCK_PATH);
  if (!existsSync(target)) {
    return { kind: 'absent' };
  }
  let lock: DevenvLock | null;
  try {
    lock = parseDevenvLock(readFileSync(target, 'utf8'));
  } catch {
    lock = null;
  }
  if (lock === null) {
    return {
      kind: 'refused',
      message: `${DEVENV_LOCK_PATH} is not a devenv lock: it must be JSON with a nodes object`,
    };
  }
  const node = lock.nodes.devenv;
  if (node === undefined) {
    return { kind: 'absent' };
  }
  const remote = devenvRemote(node.locked);
  const rev = node.locked?.rev;
  if (remote.kind === 'none' || rev === undefined || !COMMIT_HASH.test(rev)) {
    const missing =
      remote.kind === 'none' ? `no remote (${remote.reason})` : `no commit hash (rev: ${rev ?? 'absent'})`;
    return {
      kind: 'refused',
      message:
        `${DEVENV_LOCK_PATH} locks devenv with ${missing}, so nothing can tell whether it is a release.\n` +
        RELOCK_DEVENV_AT_A_RELEASE,
    };
  }
  return { kind: 'locked', locked: { remote: remote.url, rev } };
}

type RecordRead = { kind: 'missing' } | { kind: 'unreadable' } | { kind: 'read'; record: DevenvReleaseRecord };

function readRecord(root: string): RecordRead {
  const target = join(root, DEVENV_RELEASE_RECORD_PATH);
  if (!existsSync(target)) {
    return { kind: 'missing' };
  }
  try {
    const record = parseDevenvReleaseRecord(readFileSync(target, 'utf8'));
    return record === null ? { kind: 'unreadable' } : { kind: 'read', record };
  } catch {
    return { kind: 'unreadable' };
  }
}

type RecordJudgment = { kind: 'holds'; record: DevenvReleaseRecord } | { kind: 'fails'; message: string };

/**
 * Whether the recorded verdict is the locked commit's, from the two files alone. The floor is applied here, to the
 * tag the record names, so a record written under an older floor, or by hand, is held to this one.
 */
function judgeRecord(root: string, locked: LockedDevenv): RecordJudgment {
  const fails = (message: string): RecordJudgment => ({ kind: 'fails', message });
  const read = readRecord(root);
  switch (read.kind) {
    case 'missing':
      return fails(
        `${DEVENV_LOCK_PATH} locks devenv at ${locked.rev} from ${locked.remote}, and ${DEVENV_RELEASE_RECORD_PATH} holds no verdict for it.\n` +
          RECORD_THE_VERDICT,
      );
    case 'unreadable':
      return fails(
        `${DEVENV_RELEASE_RECORD_PATH} is not a devenv release record: it must be JSON with remote, rev and tag strings.\n` +
          RECORD_THE_VERDICT,
      );
    case 'read': {
      const { record } = read;
      if (record.rev !== locked.rev || record.remote !== locked.remote) {
        return fails(
          `${DEVENV_RELEASE_RECORD_PATH} records ${record.tag} for ${record.rev} from ${record.remote}, but ${DEVENV_LOCK_PATH} locks devenv at ${locked.rev} from ${locked.remote}.\n` +
            RECORD_THE_VERDICT,
        );
      }
      const version = RECORDED_TAG.exec(record.tag)?.[1];
      if (version === undefined || !isPinnedRelease(version)) {
        return fails(
          `${DEVENV_RELEASE_RECORD_PATH} records ${record.tag} for ${record.rev} from ${record.remote}, not a release from ${OLDEST_PINNED_DEVENV_TAG} on.\n` +
            WHY_A_RELEASE +
            RELOCK_DEVENV_AT_A_RELEASE,
        );
      }
      return { kind: 'holds', record };
    }
  }
}

/**
 * Report, never rewrite: re-locking is `devenv update`'s job and recording is `smoo devenv record-release`'s. On a hosted
 * runner install-devenv installs the CLI at the commit devenv.lock's devenv node locks. devenv.cachix.org keeps the
 * aarch64-darwin, aarch64-linux and x86_64-linux builds of every release tag from v2.0.4 on for good, and only
 * those: it drops the build of any other commit after about six weeks, an ancestor of a release included, and from
 * then on every cold runner compiles devenv from source. So the lock passes when it names a remote and a commit that
 * one of that remote's release tags from v2.0.4 on names, however devenv.yaml declared the input.
 *
 * That is a fact about the commit, not about this run, and a pushed tag is permanent, so it is asked of the remote
 * once per locked commit (`recordDevenvRelease`) and written to DEVENV_RELEASE_RECORD_PATH. This only verifies the
 * record against the lock: no remote is asked, so a sandbox or a runner with no route to GitHub passes or fails on
 * the repository's own bytes. A bump without its record fails here, naming the command that records it.
 */
export function validateDevenvReleaseLock(root: string): number {
  const read = readLockedDevenv(root);
  if (read.kind === 'absent') {
    return 0;
  }
  if (read.kind === 'refused') {
    console.error(read.message);
    return 1;
  }
  const judgment = judgeRecord(root, read.locked);
  if (judgment.kind === 'holds') {
    return 0;
  }
  console.error(judgment.message);
  return 1;
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

export type DevenvReleaseRecording =
  /** No devenv node in the lock: nothing to record. */
  | { kind: 'no-devenv' }
  /** The record already holds this commit's verdict: no remote was asked. */
  | { kind: 'current'; record: DevenvReleaseRecord }
  | { kind: 'recorded'; created: boolean; record: DevenvReleaseRecord }
  /** No verdict was written, and the record is as it was. */
  | { kind: 'refused'; message: string };

/**
 * Computes the verdict for the commit devenv.lock locks and writes it to DEVENV_RELEASE_RECORD_PATH: the one step
 * that asks a remote, so the one that needs the network. A record that already holds the locked commit's verdict
 * asks nothing, which makes it once per locked commit. A remote that cannot be listed, or a commit no release tag
 * from v2.0.4 on names, is refused with the record left as it was. The lowest such tag is the one recorded: the
 * release that first shipped the commit.
 */
export async function recordDevenvRelease(root: string): Promise<DevenvReleaseRecording> {
  const read = readLockedDevenv(root);
  if (read.kind === 'absent') {
    return { kind: 'no-devenv' };
  }
  if (read.kind === 'refused') {
    return { kind: 'refused', message: read.message };
  }
  const { remote, rev } = read.locked;
  const judgment = judgeRecord(root, read.locked);
  if (judgment.kind === 'holds') {
    return { kind: 'current', record: judgment.record };
  }
  const listed = await releaseTagsAt(root, remote, rev);
  if (listed.kind === 'unreadable') {
    return {
      kind: 'refused',
      message:
        `${DEVENV_LOCK_PATH} locks devenv at ${rev}, and smoo cannot list the release tags of ${remote} to tell whether a release names it:\n` +
        `  ${listed.detail}\n` +
        `  Recording the verdict asks the remote once per locked commit. Run ${RECORD_COMMAND} again with the remote reachable.`,
    };
  }
  const pinned = listed.tags
    .filter(({ version }) => isPinnedRelease(version))
    .sort((a, b) => compareVersions(a.version, b.version))[0];
  if (pinned === undefined) {
    const named =
      listed.tags.length === 0
        ? 'which no release tag names'
        : `which only ${listed.tags.map(({ tag }) => tag).join(' and ')} name${listed.tags.length === 1 ? 's' : ''}`;
    return {
      kind: 'refused',
      message:
        `${DEVENV_LOCK_PATH} locks devenv at ${rev} from ${remote}, ${named}, not a release from ${OLDEST_PINNED_DEVENV_TAG} on.\n` +
        WHY_A_RELEASE +
        RELOCK_DEVENV_AT_A_RELEASE,
    };
  }
  const record: DevenvReleaseRecord = { remote, rev, tag: pinned.tag };
  const target = join(root, DEVENV_RELEASE_RECORD_PATH);
  const created = !existsSync(target);
  writeFileSync(target, `${JSON.stringify(record, null, 2)}\n`);
  return { kind: 'recorded', created, record };
}

/** The row `smoo monorepo update` and `smoo devenv` print for an outcome; null when there was nothing to record. */
export function recordingResult(recording: DevenvReleaseRecording): FileResult | null {
  const target = DEVENV_RELEASE_RECORD_PATH;
  switch (recording.kind) {
    case 'no-devenv':
      return null;
    case 'refused':
      return { target, action: 'drifted', reason: recording.message };
    case 'recorded':
      return {
        target,
        action: recording.created ? 'created' : 'updated',
        reason: `${recording.record.tag} names ${recording.record.rev}`,
      };
    case 'current':
      return { target, action: 'unchanged', reason: `${recording.record.tag} names ${recording.record.rev}` };
  }
}
