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

const summary = (tasks: readonly unknown[], startTime: unknown = '2026-10-25T21:00:00.250Z') =>
  JSON.stringify({ run: { command: 'nx run-many -t gate', startTime, endTime: '', inner: false }, tasks });

/** The second the gate began; the summaries above began a quarter second later. */
const BEGAN = Date.parse('2026-10-25T21:00:00.000Z');

describe('succeededHashes', () => {
  it('lists the hashes of the tasks that succeeded, in run order', () => {
    expect(succeededHashes(BEGAN, summary([task('a'), task('b')]))).toEqual({ kind: 'hashes', hashes: ['a', 'b'] });
  });

  it('leaves out a task whose status is not 0', () => {
    expect(succeededHashes(BEGAN, summary([task('a'), task('b', 1)]))).toEqual({ kind: 'hashes', hashes: ['a'] });
  });

  it('lists each hash once, however many tasks name it', () => {
    expect(succeededHashes(BEGAN, summary([task('a'), task('b'), task('a')]))).toEqual({
      kind: 'hashes',
      hashes: ['a', 'b'],
    });
  });

  it('lists nothing for a run with no tasks', () => {
    expect(succeededHashes(BEGAN, summary([]))).toEqual({ kind: 'hashes', hashes: [] });
  });

  it('accepts a hash of the longest length the cache route takes', () => {
    const longest = 'h'.repeat(128);
    expect(succeededHashes(BEGAN, summary([task(longest)]))).toEqual({ kind: 'hashes', hashes: [longest] });
  });

  it('refuses a malformed task hash, even on a task that failed', () => {
    for (const hash of ['bad/hash', 'a.b', '', 'h'.repeat(129)]) {
      expect(succeededHashes(BEGAN, summary([task('a'), task(hash, 1)]))).toEqual({
        kind: 'refused',
        why: `the run summary has an invalid task hash: ${JSON.stringify(hash)}`,
      });
    }
  });

  it('refuses a task without a string hash', () => {
    expect(succeededHashes(BEGAN, summary([task(7)])).kind).toBe('refused');
  });

  it('refuses a task without a numeric status', () => {
    expect(succeededHashes(BEGAN, summary([task('a', '0')]))).toEqual({
      kind: 'refused',
      why: `the run summary has a task without a numeric status: ${JSON.stringify(task('a', '0'))}`,
    });
  });

  it('refuses a summary that is not JSON or lacks its run object or tasks array', () => {
    expect(succeededHashes(BEGAN, '{').kind).toBe('refused');
    expect(succeededHashes(BEGAN, JSON.stringify({ tasks: [] })).kind).toBe('refused');
    expect(succeededHashes(BEGAN, JSON.stringify({ run: {} })).kind).toBe('refused');
  });

  it('refuses a summary an earlier invocation began, or one with no start time', () => {
    expect(succeededHashes(BEGAN, summary([task('a')], '2026-10-25T20:59:59.999Z'))).toEqual({
      kind: 'refused',
      why: 'the run summary is of an invocation that began at 2026-10-25T20:59:59.999Z, before this gate began at 2026-10-25T21:00:00.000Z',
    });
    expect(succeededHashes(BEGAN, summary([task('a')], null))).toEqual({
      kind: 'refused',
      why: 'the run summary has no start time: null',
    });
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
