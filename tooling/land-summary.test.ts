import { describe, expect, it } from 'bun:test';
import { summarizeFailures } from '@smoothbricks/nx-plugin/failure-summary';

describe('land failure summary', () => {
  it('deduplicates exact task IDs and messages without merging distinct errors or their locations', () => {
    const first =
      'error: daemon on /tmp/first/d.sock to answer PING: no event within 30000 ms\n    at fixture.ts:113:24';
    const second =
      'error: daemon on /tmp/second/d.sock to answer PING: no event within 30000 ms\n    at fixture.ts:114:24';
    const output = [
      '> nx run nx-plugin:test-shard3',
      '',
      first,
      '',
      first,
      '',
      second,
      '',
      ' NX   Running target gate failed',
      '',
      'Failed tasks:',
      '',
      '- nx-plugin:test-shard3',
      '- nx-plugin:test-shard3',
      '',
    ].join('\n');
    expect(summarizeFailures(output)).toBe('failed nx-plugin:test-shard3\n' + first + '\n\n' + second + '\n');
  });

  it('retains multiline assertion diffs verbatim and drops only known passing-test progress', () => {
    const message =
      'error: expect(received).toEqual(expected)\n\n- []\n+ [\n+   "child still works in fixture",\n+ ]\n\n    at fixture-repo.test.ts:206:43\n(fail) abandoned fixture client';
    const output = [
      '> nx run cli:test-shard-3',
      '',
      '(pass) unrelated case [1.00ms]',
      '',
      message,
      '',
      ' NX   Running target gate failed',
      '',
      'Failed tasks:',
      '',
      '- cli:test-shard-3',
      '',
    ].join('\n');
    expect(summarizeFailures(output)).toBe('failed cli:test-shard-3\n' + message + '\n');
  });

  it('keeps tasks separate, strips color and does not include successful compact task lines', () => {
    const output =
      '\u001b[31m> nx run a:lint\u001b[0m\n\nfile.ts:4:2 error TS2322: wrong type\n\n+  nx run a:build [local cache]\n> nx run b:test\n\nunknown tool says broken\n\n NX   Running target gate failed\n\nFailed tasks:\n\n- a:lint\n- b:test\n';
    expect(summarizeFailures(output)).toBe(
      'failed a:lint\nfile.ts:4:2 error TS2322: wrong type\n\nfailed b:test\nunknown tool says broken\n',
    );
  });

  it('reports missing diagnostics honestly and names stopped and unfinished tasks', () => {
    const output =
      'Tasks stopped before they finished:\n\n- b:test\n\nTasks the run ended without finishing:\n\n- c:build\n\nFailed tasks:\n\n- a:test\n';
    expect(summarizeFailures(output)).toBe(
      'failed a:test\ndiagnostic unavailable: this run printed no task output\n\nstopped b:test: Nx stopped it before it finished\n\nunfinished c:build: the run ended without finishing it\n',
    );
  });

  it('extracts exact errors from verbose failed tasks instead of replaying fixture progress and daemon dumps', () => {
    const message =
      'error: the daemon to answer PING: no event within 30000 ms\nopen test spans: [lockfile restart]\nno change in /tmp/fixture/nx\n    at counted-cargo.ts:113:24\n(fail) lockfile restart';
    const output = [
      '> nx run nx-plugin:test-shard3',
      '',
      'Updated workspace versions in bun.lock',
      'bun.lock workspace versions are valid.',
      '[NX daemon] DEBUG listening',
      'Time taken for startup 42ms',
      '',
      message,
      '...........',
      '',
      '22 pass',
      '1 fail',
      'Ran 24 tests across 5 files.',
      '',
      ' NX   Running target gate failed',
      '',
      'Failed tasks:',
      '',
      '- nx-plugin:test-shard3',
      '',
    ].join('\n');
    expect(summarizeFailures(output)).toBe('failed nx-plugin:test-shard3\n' + message + '\n');
  });

  it('does not hide failures before tasks start or invent evidence for an empty log', () => {
    expect(summarizeFailures('nx: the daemon died\nnx: an exact second error')).toBe(
      'Gate failed without a task-level failure summary:\nnx: the daemon died\nnx: an exact second error\n',
    );
    expect(summarizeFailures('')).toContain('diagnostic unavailable: this run printed no output');
  });
});
