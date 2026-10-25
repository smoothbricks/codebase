// Tests for land-publish.ts: which gate task results a land publishes. Each case is a plain call, so the branch is
// proven without a process.

import { describe, expect, it } from 'bun:test';
import { hashList, succeededHashes } from './land-publish.ts';

const task = (hash: unknown, status: unknown = 0) => ({
  taskId: 'app:build',
  target: 'build',
  projectName: 'app',
  hash,
  cacheStatus: 'local-cache-hit',
  status,
});

const summary = (tasks: readonly unknown[]) =>
  JSON.stringify({ run: { command: 'nx run-many -t gate', startTime: '', endTime: '', inner: false }, tasks });

describe('succeededHashes', () => {
  it('lists the hashes of the tasks that succeeded, in run order', () => {
    expect(succeededHashes(summary([task('a'), task('b')]))).toEqual({ kind: 'hashes', hashes: ['a', 'b'] });
  });

  it('leaves out a task whose status is not 0', () => {
    expect(succeededHashes(summary([task('a'), task('b', 1)]))).toEqual({ kind: 'hashes', hashes: ['a'] });
  });

  it('lists each hash once, however many tasks name it', () => {
    expect(succeededHashes(summary([task('a'), task('b'), task('a')]))).toEqual({ kind: 'hashes', hashes: ['a', 'b'] });
  });

  it('lists nothing for a run with no tasks', () => {
    expect(succeededHashes(summary([]))).toEqual({ kind: 'hashes', hashes: [] });
  });

  it('accepts a hash of the longest length the cache route takes', () => {
    const longest = 'h'.repeat(128);
    expect(succeededHashes(summary([task(longest)]))).toEqual({ kind: 'hashes', hashes: [longest] });
  });

  it('refuses a malformed task hash, even on a task that failed', () => {
    for (const hash of ['bad/hash', 'a.b', '', 'h'.repeat(129)]) {
      expect(succeededHashes(summary([task('a'), task(hash, 1)]))).toEqual({
        kind: 'refused',
        why: `the run summary has an invalid task hash: ${JSON.stringify(hash)}`,
      });
    }
  });

  it('refuses a task without a string hash', () => {
    expect(succeededHashes(summary([task(7)])).kind).toBe('refused');
  });

  it('refuses a task without a numeric status', () => {
    expect(succeededHashes(summary([task('a', '0')]))).toEqual({
      kind: 'refused',
      why: `the run summary has a task without a numeric status: ${JSON.stringify(task('a', '0'))}`,
    });
  });

  it('refuses a summary that is not JSON or lacks its run object or tasks array', () => {
    expect(succeededHashes('{').kind).toBe('refused');
    expect(succeededHashes(JSON.stringify({ tasks: [] })).kind).toBe('refused');
    expect(succeededHashes(JSON.stringify({ run: {} })).kind).toBe('refused');
  });
});

describe('hashList', () => {
  it('accepts a JSON array of distinct valid hashes', () => {
    expect(hashList('["a","b-1_c"]')).toEqual({ kind: 'hashes', hashes: ['a', 'b-1_c'] });
  });

  it('accepts an empty array', () => {
    expect(hashList('[]')).toEqual({ kind: 'hashes', hashes: [] });
  });

  it('refuses a repeated hash', () => {
    expect(hashList('["a","a"]')).toEqual({ kind: 'refused', why: 'the hash list repeats a task hash' });
  });

  it('refuses anything but an array of valid hashes', () => {
    for (const text of ['{}', '[1]', '["bad/hash"]', 'nope']) {
      expect(hashList(text).kind).toBe('refused');
    }
  });
});
