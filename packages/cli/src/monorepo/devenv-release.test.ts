import { describe, expect, it, spyOn } from 'bun:test';
import { existsSync } from 'node:fs';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { mergeEnv } from '../lib/run.js';
import {
  DEVENV_LOCK_PATH,
  DEVENV_RELEASE_RECORD_PATH,
  type DevenvReleaseRecord,
  recordDevenvRelease,
  recordingResult,
  validateDevenvReleaseLock,
} from './devenv-release.js';

const GITHUB_DEVENV = 'https://github.com/cachix/devenv';
const RECORD_COMMAND = 'smoo devenv record-release';
/** A remote nothing answers for: listing its tags fails. */
const NO_REMOTE = pathToFileURL(join(tmpdir(), 'smoo-no-such-devenv-remote')).href;

const gitEnv = mergeEnv(undefined, ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR']);
const git = (cwd: string, ...args: string[]): string => {
  const identity = ['-c', 'user.name=smoo', '-c', 'user.email=smoo@example.invalid'];
  const unsigned = ['-c', 'commit.gpgSign=false', '-c', 'tag.gpgSign=false'];
  const result = Bun.spawnSync(['git', ...identity, ...unsigned, ...args], { cwd, env: gitEnv });
  if (result.exitCode !== 0) {
    throw new Error(`git ${args.join(' ')} failed: ${result.stderr.toString()}`);
  }
  return result.stdout.toString().trim();
};

interface DevenvRemote {
  readonly root: string;
  readonly url: string;
  readonly v203: string;
  readonly v204: string;
  readonly between: string;
  readonly v210: string;
  readonly head: string;
}

/**
 * A stand-in for cachix/devenv's repository: v2.0.3, older than every release whose builds stay cached; v2.0.4,
 * annotated, so ls-remote lists its commit only as refs/tags/v2.0.4^{}; an untagged commit after it; v2.10, which
 * only a numeric comparison puts after v2.0.4, and which v2.11 names as well; and an untagged head.
 */
const withDevenvRemote = async (run: (remote: DevenvRemote) => Promise<void>): Promise<void> => {
  const root = await mkdtemp(join(tmpdir(), 'smoo-devenv-remote-'));
  try {
    const repo = join(root, 'devenv');
    await mkdir(repo);
    git(repo, 'init', '--quiet');
    const commit = (message: string): string => {
      git(repo, 'commit', '--quiet', '--allow-empty', '-m', message);
      return git(repo, 'rev-parse', 'HEAD');
    };
    const v203 = commit('2.0.3');
    git(repo, 'tag', 'v2.0.3');
    const v204 = commit('2.0.4');
    git(repo, 'tag', '--annotate', 'v2.0.4', '-m', 'v2.0.4');
    const between = commit('between releases');
    const v210 = commit('2.10');
    git(repo, 'tag', 'v2.10');
    git(repo, 'tag', 'v2.11');
    const head = commit('main');
    await run({ root, url: pathToFileURL(repo).href, v203, v204, between, v210, head });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
};

/** git's own URL rewriting sends the derived GitHub URL to `url`, so no test reaches the network. */
const withGithubAt = async <T>(url: string, run: () => Promise<T>): Promise<T> => {
  const rewrite: Record<string, string> = {
    GIT_CONFIG_COUNT: '1',
    GIT_CONFIG_KEY_0: `url.${url}.insteadOf`,
    GIT_CONFIG_VALUE_0: GITHUB_DEVENV,
  };
  const saved = Object.fromEntries(Object.keys(rewrite).map((key) => [key, process.env[key]]));
  Object.assign(process.env, rewrite);
  try {
    return await run();
  } finally {
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
};

/**
 * Runs `run` with a `git` ahead of the real one on PATH that logs its arguments and fails: the calls it saw are the
 * questions a remote would have been asked.
 */
const withFailingGit = async <T>(run: () => Promise<T>): Promise<{ result: T; calls: string[] }> => {
  const dir = await mkdtemp(join(tmpdir(), 'smoo-failing-git-'));
  const log = join(dir, 'calls');
  const shim = join(dir, 'git');
  await writeFile(log, '');
  await writeFile(shim, `#!/bin/sh\necho "$*" >> '${log}'\necho 'git: network is unreachable' >&2\nexit 97\n`);
  await chmod(shim, 0o755);
  const saved = process.env.PATH;
  process.env.PATH = `${dir}:${saved ?? ''}`;
  try {
    const result = await run();
    return { result, calls: (await readFile(log, 'utf8')).split('\n').filter((line) => line !== '') };
  } finally {
    if (saved === undefined) delete process.env.PATH;
    else process.env.PATH = saved;
    await rm(dir, { recursive: true, force: true });
  }
};

// The devenv node as `devenv update` writes it. `original` is what devenv.yaml declared; the check reads `locked`.
const lockWithDevenv = (locked: Record<string, string>): string =>
  `${JSON.stringify(
    {
      nodes: {
        devenv: {
          locked: { dir: 'src/modules', ...locked },
          original: { dir: 'src/modules', owner: 'cachix', repo: 'devenv', type: 'github' },
        },
        root: { inputs: { devenv: 'devenv' } },
      },
      root: 'root',
      version: 7,
    },
    null,
    2,
  )}\n`;

const recordJson = (record: DevenvReleaseRecord): string => `${JSON.stringify(record, null, 2)}\n`;

/** A repository holding the lock, and the verdict record as the text it holds, when given. */
const withRepo = async <T>(
  files: { lock?: string; record?: string },
  run: (root: string) => Promise<T>,
): Promise<T> => {
  const root = await mkdtemp(join(tmpdir(), 'smoo-devenv-release-'));
  try {
    await mkdir(join(root, 'tooling/direnv'), { recursive: true });
    if (files.lock !== undefined) await writeFile(join(root, DEVENV_LOCK_PATH), files.lock);
    if (files.record !== undefined) await writeFile(join(root, DEVENV_RELEASE_RECORD_PATH), files.record);
    return await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
};

/** The check's failure count and everything it printed, with every `git` call failing and counted. */
const checkOffline = async (files: {
  lock?: string;
  record?: string;
}): Promise<{ failures: number; reason: string; gitCalls: string[] }> => {
  const errors = spyOn(console, 'error').mockImplementation(() => {});
  try {
    return await withRepo(files, async (root) => {
      const { result, calls } = await withFailingGit(async () => validateDevenvReleaseLock(root));
      return { failures: result, reason: errors.mock.calls.map((call) => String(call[0])).join('\n'), gitCalls: calls };
    });
  } finally {
    errors.mockRestore();
  }
};

describe('devenv release check', () => {
  const rev = 'b'.repeat(40);
  const bumped = 'c'.repeat(40);
  const gitLock = (at: string): string => lockWithDevenv({ type: 'git', url: NO_REMOTE, rev: at });
  const githubLock = (at: string): string =>
    lockWithDevenv({ type: 'github', owner: 'cachix', repo: 'devenv', rev: at });

  it('passes a recorded verdict for the locked commit with no network, asking no remote', async () => {
    // The remotes cannot be listed and `git` itself fails: only the record can pass these.
    const cases = [
      [gitLock(rev), NO_REMOTE],
      [githubLock(rev), GITHUB_DEVENV],
    ] as const;
    for (const [lock, remote] of cases) {
      const record = recordJson({ remote, rev, tag: 'v2.4.0' });
      expect(await checkOffline({ lock, record })).toEqual({ failures: 0, reason: '', gitCalls: [] });
    }
  });

  it('fails a bump whose verdict is not recorded, offline, naming the command that records it', async () => {
    // The lock moved on; the record still holds the previous commit's verdict.
    const stale = await checkOffline({
      lock: gitLock(bumped),
      record: recordJson({ remote: NO_REMOTE, rev, tag: 'v2.3.1' }),
    });
    expect(stale.failures).toBe(1);
    expect(stale.reason).toContain(
      `${DEVENV_RELEASE_RECORD_PATH} records v2.3.1 for ${rev} from ${NO_REMOTE}, but ${DEVENV_LOCK_PATH} locks devenv at ${bumped} from ${NO_REMOTE}`,
    );
    expect(stale.reason).toContain(RECORD_COMMAND);
    expect(stale.gitCalls).toEqual([]);

    // The same commit from another remote is another verdict: whose tags named it is part of the answer.
    const other = await checkOffline({
      lock: githubLock(rev),
      record: recordJson({ remote: 'https://github.com/someone/devenv', rev, tag: 'v2.4.0' }),
    });
    expect(other.failures).toBe(1);
    expect(other.reason).toContain(`records v2.4.0 for ${rev} from https://github.com/someone/devenv, but`);
    expect(other.reason).toContain(`locks devenv at ${rev} from ${GITHUB_DEVENV}`);
    expect(other.gitCalls).toEqual([]);

    const none = await checkOffline({ lock: githubLock(rev) });
    expect(none.failures).toBe(1);
    expect(none.reason).toContain(`${DEVENV_RELEASE_RECORD_PATH} holds no verdict for it`);
    expect(none.reason).toContain(RECORD_COMMAND);
    expect(none.gitCalls).toEqual([]);
  });

  it('fails a record it cannot read, or one that names no release from v2.0.4 on', async () => {
    for (const record of ['{"rev":', '{}\n', '[]\n', '{"remote":1,"rev":2,"tag":3}\n']) {
      const unreadable = await checkOffline({ lock: gitLock(rev), record });
      expect(unreadable.failures).toBe(1);
      expect(unreadable.reason).toContain(
        `${DEVENV_RELEASE_RECORD_PATH} is not a devenv release record: it must be JSON with remote, rev and tag strings`,
      );
      expect(unreadable.gitCalls).toEqual([]);
    }
    // The floor is the check's, so it holds a record written under an older floor, or by hand, as well.
    for (const tag of ['v2.0.3', 'v1.9', 'main', '2.4.0']) {
      const old = await checkOffline({ lock: gitLock(rev), record: recordJson({ remote: NO_REMOTE, rev, tag }) });
      expect(old.failures).toBe(1);
      expect(old.reason).toContain(
        `${DEVENV_RELEASE_RECORD_PATH} records ${tag} for ${rev} from ${NO_REMOTE}, not a release from v2.0.4 on`,
      );
      expect(old.gitCalls).toEqual([]);
    }
    // As text, v2.10 would sort before v2.0.4.
    const ten = await checkOffline({
      lock: gitLock(rev),
      record: recordJson({ remote: NO_REMOTE, rev, tag: 'v2.10' }),
    });
    expect(ten.failures).toBe(0);
  });

  it('refuses a lock that names no remote or no commit', async () => {
    const lockedRev = 'f'.repeat(40);
    const cases = [
      [
        { path: '/nix/store/devenv', rev: lockedRev, type: 'path' },
        'with no remote (a path input, which names no git remote)',
      ],
      [
        { repo: 'devenv', rev: lockedRev, type: 'github' },
        'with no remote (a github input without an owner and a repo)',
      ],
      [{ rev: lockedRev, type: 'git' }, 'with no remote (a git input without a url)'],
      [{ rev: 'main', type: 'git', url: GITHUB_DEVENV }, 'with no commit hash'],
      [{ type: 'git', url: GITHUB_DEVENV }, 'with no commit hash'],
    ] as const;
    for (const [locked, why] of cases) {
      const { failures, reason, gitCalls } = await checkOffline({ lock: lockWithDevenv(locked) });
      expect(failures).toBe(1);
      expect(reason).toContain(`${DEVENV_LOCK_PATH} locks devenv ${why}`);
      expect(gitCalls).toEqual([]);
    }
  });

  it('has nothing to check without a lock or a devenv node, and reports a lock it cannot read', async () => {
    expect((await checkOffline({})).failures).toBe(0);
    expect(
      (await checkOffline({ lock: '{"nodes":{"root":{"inputs":{}}},"root":"root","version":7}\n' })).failures,
    ).toBe(0);
    // A record beside a lock with no devenv node is nothing to verify either.
    expect((await checkOffline({ record: '{"rev":' })).failures).toBe(0);
    expect((await checkOffline({ lock: '{"nodes":' })).failures).toBe(1);
  });
});

describe('devenv release record', () => {
  const recordOf = (root: string): Promise<string> => readFile(join(root, DEVENV_RELEASE_RECORD_PATH), 'utf8');

  it('records the lowest release tag the remote lists at the locked commit, and the check then passes offline', async () => {
    await withDevenvRemote(async ({ url, v204, v210 }) => {
      // An annotated tag, matched through the commit it peels to.
      await withRepo({ lock: lockWithDevenv({ type: 'git', url, rev: v204 }) }, async (root) => {
        const record = { remote: url, rev: v204, tag: 'v2.0.4' };
        expect(await recordDevenvRelease(root)).toEqual({ kind: 'recorded', created: true, record });
        expect(await recordOf(root)).toBe(recordJson(record));
        expect((await withFailingGit(async () => validateDevenvReleaseLock(root))).result).toBe(0);
      });
      // As text, v2.10 would sort before v2.0.4; v2.11 names the same commit and is not the lowest.
      await withRepo({ lock: lockWithDevenv({ type: 'git', url, rev: v210 }) }, async (root) => {
        const record = { remote: url, rev: v210, tag: 'v2.10' };
        expect(await recordDevenvRelease(root)).toEqual({ kind: 'recorded', created: true, record });
        expect(await recordOf(root)).toBe(recordJson(record));
      });
      // A github input names its remote by owner and repo.
      const locked = lockWithDevenv({ owner: 'cachix', repo: 'devenv', rev: v204, type: 'github' });
      await withGithubAt(url, () =>
        withRepo({ lock: locked }, async (root) => {
          const record = { remote: GITHUB_DEVENV, rev: v204, tag: 'v2.0.4' };
          expect(await recordDevenvRelease(root)).toEqual({ kind: 'recorded', created: true, record });
        }),
      );
    });
  });

  it('asks the remote once per locked commit: a current record asks nothing, a bump asks again', async () => {
    await withDevenvRemote(async ({ url, v204, v210 }) => {
      await withRepo({ lock: lockWithDevenv({ type: 'git', url, rev: v204 }) }, async (root) => {
        await recordDevenvRelease(root);

        const current = await withFailingGit(() => recordDevenvRelease(root));
        expect(current.result).toEqual({ kind: 'current', record: { remote: url, rev: v204, tag: 'v2.0.4' } });
        expect(current.calls).toEqual([]);

        await writeFile(join(root, DEVENV_LOCK_PATH), lockWithDevenv({ type: 'git', url, rev: v210 }));

        // With the remote unreachable the bump asks once, is refused, and the old verdict stays for the check to judge.
        const unreachable = await withFailingGit(() => recordDevenvRelease(root));
        expect(unreachable.calls).toHaveLength(1);
        expect(unreachable.result.kind === 'refused' ? unreachable.result.message : '').toContain(
          'git: network is unreachable',
        );
        expect(await recordOf(root)).toBe(recordJson({ remote: url, rev: v204, tag: 'v2.0.4' }));

        const record = { remote: url, rev: v210, tag: 'v2.10' };
        expect(await recordDevenvRelease(root)).toEqual({ kind: 'recorded', created: false, record });
        expect(await recordOf(root)).toBe(recordJson(record));
      });
    });
  });

  it('refuses a commit no release from v2.0.4 on names, and leaves the record as it was', async () => {
    await withDevenvRemote(async ({ root: remoteRoot, url, v203, v204, between, head }) => {
      // `between` is part of v2.10's history, which is not enough: the cache keeps the tag's build, not its history's.
      for (const rev of [between, head, 'f'.repeat(40)]) {
        await withRepo({ lock: lockWithDevenv({ type: 'git', url, rev }) }, async (root) => {
          const recording = await recordDevenvRelease(root);
          expect(recording.kind).toBe('refused');
          const message = recording.kind === 'refused' ? recording.message : '';
          expect(message).toContain(`locks devenv at ${rev} from ${url}, which no release tag names,`);
          expect(message).toContain('every cold runner compiles devenv from source');
          expect(message).toContain(RECORD_COMMAND);
          expect(existsSync(join(root, DEVENV_RELEASE_RECORD_PATH))).toBeFalse();
        });
      }
      const old = lockWithDevenv({ type: 'git', url, rev: v203 });
      await withRepo({ lock: old }, async (root) => {
        const recording = await recordDevenvRelease(root);
        expect(recording.kind === 'refused' ? recording.message : '').toContain(
          'which only v2.0.3 names, not a release from v2.0.4 on',
        );
      });

      // A remote whose tags cannot be listed is refused, not passed, and the earlier verdict stays for the check to judge.
      const unreachable = pathToFileURL(join(remoteRoot, 'unreachable')).href;
      const earlier = recordJson({ remote: url, rev: v204, tag: 'v2.0.4' });
      await withRepo(
        { lock: lockWithDevenv({ type: 'git', url: unreachable, rev: v204 }), record: earlier },
        async (root) => {
          const recording = await recordDevenvRelease(root);
          expect(recording.kind === 'refused' ? recording.message : '').toContain(
            `cannot list the release tags of ${unreachable}`,
          );
          expect(await recordOf(root)).toBe(earlier);
        },
      );
    });
  });

  it('has nothing to record without a devenv node, and refuses a lock it cannot read', async () => {
    expect(await withRepo({}, recordDevenvRelease)).toEqual({ kind: 'no-devenv' });
    const noNode = '{"nodes":{"root":{"inputs":{}}},"root":"root","version":7}\n';
    expect(await withRepo({ lock: noNode }, recordDevenvRelease)).toEqual({ kind: 'no-devenv' });

    const unreadable = await withRepo({ lock: '{"nodes":' }, recordDevenvRelease);
    expect(unreadable.kind === 'refused' ? unreadable.message : '').toContain(
      `${DEVENV_LOCK_PATH} is not a devenv lock`,
    );
    const noRemote = await withRepo(
      { lock: lockWithDevenv({ rev: 'f'.repeat(40), type: 'path' }) },
      recordDevenvRelease,
    );
    expect(noRemote.kind === 'refused' ? noRemote.message : '').toContain(
      `${DEVENV_LOCK_PATH} locks devenv with no remote`,
    );
  });

  it('reports each outcome as a row of the update table', () => {
    const record = { remote: GITHUB_DEVENV, rev: 'a'.repeat(40), tag: 'v2.4.0' };
    const reason = `v2.4.0 names ${'a'.repeat(40)}`;
    const target = DEVENV_RELEASE_RECORD_PATH;
    expect(recordingResult({ kind: 'recorded', created: true, record })).toEqual({ target, action: 'created', reason });
    expect(recordingResult({ kind: 'recorded', created: false, record })).toEqual({
      target,
      action: 'updated',
      reason,
    });
    expect(recordingResult({ kind: 'current', record })).toEqual({ target, action: 'unchanged', reason });
    expect(recordingResult({ kind: 'refused', message: 'no release' })).toEqual({
      target,
      action: 'drifted',
      reason: 'no release',
    });
    expect(recordingResult({ kind: 'no-devenv' })).toBeNull();
  });
});
