import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gitIn, releaseRecord, releaseRecordPath, writeReleaseRecord } from './release-record.js';

/** A git repository with one commit and a release binary beside it, removed afterwards. */
function checkout(run: (root: string, binary: string) => Promise<void>): () => Promise<void> {
  return async () => {
    const root = mkdtempSync(join(tmpdir(), 'cowshed-release-record-'));
    try {
      const git = gitIn(root);
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
      const binary = join(root, 'cowshed');
      writeFileSync(binary, 'a release binary');
      await run(root, binary);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  };
}

const sha256 = (path: string) => createHash('sha256').update(readFileSync(path)).digest('hex');

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
    checkout(async (root, binary) => {
      rmSync(join(root, '.git'), { recursive: true, force: true });
      await expect(releaseRecord(binary, gitIn(root))).rejects.toThrow('git rev-parse HEAD');
    }),
  );
});
