import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { type Git, gitIn, releaseRecord, releaseRecordPath, writeReleaseRecord } from './release-record.js';

/** `git init` and one empty commit in `directory`. */
function commitOnce(directory: string): void {
  const git = gitIn(directory);
  git(['init', '-q', '-b', 'main']);
  git([
    '-c',
    'user.name=fixture',
    '-c',
    'user.email=fixture@example.invalid',
    'commit',
    '-q',
    '--allow-empty',
    '-m',
    'first',
  ]);
}

/**
 * A git repository with one commit and a release binary beside it, removed afterwards. `enclosed` puts it inside
 * another repository: what git discovers above a checkout whose own `.git` is gone.
 */
function checkout(run: (root: string, binary: string) => Promise<void>, enclosed = false): () => Promise<void> {
  return async () => {
    const top = mkdtempSync(join(tmpdir(), 'cowshed-release-record-'));
    try {
      if (enclosed) commitOnce(top);
      const root = enclosed ? mkdtempSync(join(top, 'checkout-')) : top;
      commitOnce(root);
      const binary = join(root, 'cowshed');
      writeFileSync(binary, 'a release binary');
      await run(root, binary);
    } finally {
      rmSync(top, { recursive: true, force: true });
    }
  };
}

const sha256 = (path: string) => createHash('sha256').update(readFileSync(path)).digest('hex');

/**
 * Git in `cwd` that cannot discover a repository above `cwd`: GIT_CEILING_DIRECTORIES set for its own child process
 * only. "A checkout git cannot answer for" means its own repository is gone; without the ceiling, git answers for
 * whatever repository encloses the temp directory, as a gate's RAM temp root once did. `gitIn` passes the process's
 * environment, which this leaves unchanged for every other case.
 */
function gitWithin(cwd: string): Git {
  return (args) => {
    const result = Bun.spawnSync(['git', ...args], {
      cwd,
      env: {
        ...process.env,
        GIT_CONFIG_GLOBAL: '/dev/null',
        GIT_CONFIG_NOSYSTEM: '1',
        GIT_CEILING_DIRECTORIES: dirname(cwd),
      },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    if (result.exitCode !== 0) {
      throw new Error(`git ${args.join(' ')} in ${cwd} exited ${result.exitCode}: ${result.stderr.toString().trim()}`);
    }
    return result.stdout.toString().trim();
  };
}

describe('the release record beside a binary', () => {
  it(
    'names the commit HEAD names, its commit time, and the SHA-256 of the binary',
    checkout(async (root, binary) => {
      const git = gitIn(root);
      const record = await releaseRecord(binary, git);
      expect(record).toEqual({
        commit: git(['rev-parse', 'HEAD']),
        commitTime: Number(git(['log', '-1', '--format=%ct', 'HEAD'])),
        sha256: sha256(binary),
      });
      writeReleaseRecord(binary, record);
      expect(JSON.parse(readFileSync(releaseRecordPath(binary), 'utf8'))).toEqual(record);
      expect(releaseRecordPath(binary)).toBe(`${binary}.build`);
    }),
  );

  it(
    'gives a binary replayed unchanged at a later commit that commit, bound to the same bytes',
    checkout(async (root, binary) => {
      const git = gitIn(root);
      const first = await releaseRecord(binary, git);
      git([
        '-c',
        'user.name=fixture',
        '-c',
        'user.email=fixture@example.invalid',
        'commit',
        '-q',
        '--allow-empty',
        '-m',
        'docs only',
      ]);
      const later = await releaseRecord(binary, git);
      expect(later.commit).toBe(git(['rev-parse', 'HEAD']));
      expect(later.commit).not.toBe(first.commit);
      expect(later.sha256).toBe(first.sha256);
    }),
  );

  it(
    'refuses a checkout git cannot answer for instead of writing a record without a commit',
    checkout(async (root) => {
      // A plain directory beside the checkout, inside the enclosing repository: no `.git` of its own ever existed.
      const plain = mkdtempSync(join(dirname(root), 'plain-'));
      const binary = join(plain, 'cowshed');
      writeFileSync(binary, 'a release binary');
      await expect(releaseRecord(binary, gitWithin(plain))).rejects.toThrow('git rev-parse HEAD');
    }, true),
  );

  it(
    'names the commit of the checkout a subdirectory of it belongs to, as the release script run from a package does',
    checkout(async (root, binary) => {
      const git = gitIn(root);
      const subdirectory = join(root, 'packages', 'cowshed');
      mkdirSync(subdirectory, { recursive: true });
      const record = await releaseRecord(binary, gitIn(subdirectory));
      expect(record.commit).toBe(git(['rev-parse', 'HEAD']));
    }, true),
  );
});
