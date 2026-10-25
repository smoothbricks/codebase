// Tests for land-attribution.ts: which tasks a red gate failed, and which commits of the land change their inputs.
// Each case is a plain call over values, so every branch is proven without a process.

import { describe, expect, it } from 'bun:test';
import {
  attribute,
  type Commit,
  failedTasks,
  parseInputs,
  renderAttribution,
  type TaskInputs,
} from './land-attribution.ts';

const GATE_BEGAN = Date.parse('2026-10-25T21:00:00.000Z');
/** run.json as Nx writes it, with the fields land-attribution.ts reads. */
const summary = (tasks: readonly unknown[], startTime: unknown = '2026-10-25T21:00:00.250Z'): string =>
  JSON.stringify({ run: { command: 'nx run-many -t gate', startTime, endTime: '', inner: false }, tasks });
const row = (taskId: string, status: number) => ({
  taskId,
  target: taskId.split(':')[1],
  projectName: taskId.split(':')[0],
  hash: 'h',
  cacheStatus: 'cache-miss',
  status,
});

describe('failedTasks', () => {
  const read: readonly { readonly name: string; readonly tasks: readonly unknown[]; readonly failed: string[] }[] = [
    { name: 'a run that passed fails nothing', tasks: [row('a:lint', 0), row('a:test', 0)], failed: [] },
    {
      name: 'every nonzero exit is a failure, named once and sorted',
      tasks: [row('b:test', 1), row('a:lint', 0), row('a:test', 130), row('b:test', 1)],
      failed: ['a:test', 'b:test'],
    },
    {
      name: 'a task whose hashing failed has no hash and still failed',
      tasks: [{ taskId: 'a:lint', target: 'lint', projectName: 'a', status: 1 }],
      failed: ['a:lint'],
    },
    { name: 'a run of no tasks fails nothing', tasks: [], failed: [] },
  ];
  for (const { name, tasks, failed } of read) {
    it(name, () => {
      expect(failedTasks(summary(tasks), GATE_BEGAN)).toEqual({ kind: 'read', value: failed });
    });
  }

  it('reads a summary that began in the same second as the gate', () => {
    expect(failedTasks(summary([row('a:lint', 1)], '2026-10-25T21:00:00.000Z'), GATE_BEGAN)).toEqual({
      kind: 'read',
      value: ['a:lint'],
    });
  });

  const refusedCases: readonly (readonly [string, string, string])[] = [
    ['not JSON', '{', 'the run summary is not JSON'],
    ['no run object', JSON.stringify({ tasks: [] }), 'the run summary has no run object or no tasks array'],
    ['no tasks array', JSON.stringify({ run: { startTime: '2026-10-25T21:00:01.000Z' } }), 'no tasks array'],
    ['no start time', summary([], null), 'the run summary has no start time: null'],
    ['an unreadable start time', summary([], 'yesterday'), 'the run summary has no start time: "yesterday"'],
    [
      'a start before the gate began: an earlier invocation wrote it',
      summary([row('a:lint', 1)], '2026-10-25T20:59:59.999Z'),
      'the run summary is of an invocation that began at 2026-10-25T20:59:59.999Z, before this gate began at 2026-10-25T21:00:00.000Z',
    ],
    ['a row without a task id', summary([{ status: 1 }]), 'the run summary has a task without a task id'],
    [
      'a row without a numeric status, even beside a failure',
      summary([row('a:lint', 1), { taskId: 'a:test', status: '1' }]),
      'without a task id or a numeric status',
    ],
  ];
  for (const [name, text, why] of refusedCases) {
    it(`refuses a summary with ${name}`, () => {
      const reading = failedTasks(text, GATE_BEGAN);
      expect(reading.kind).toBe('refused');
      if (reading.kind === 'refused') expect(reading.why).toContain(why);
    });
  }
});

describe('attribution', () => {
  const fix: Commit = { sha: 'a'.repeat(40), subject: 'fix(lib): the fix', paths: ['lib/src/index.ts'] };
  const docs: Commit = { sha: 'b'.repeat(40), subject: 'docs: a note', paths: ['README.md'] };
  const both: Commit = { sha: 'c'.repeat(40), subject: 'feat: both', paths: ['README.md', 'app/src/main.ts'] };
  /** What land-inputs.mts lists: app:test waits for lib:build, so lib's source is among its inputs. */
  const inputs: TaskInputs = {
    'lib:lint': ['lib/src/index.ts', 'package.json'],
    'app:test': ['app/src/main.ts', 'lib/src/index.ts'],
    'app:lint': ['app/src/main.ts'],
  };

  const cases: readonly {
    readonly name: string;
    readonly commits: readonly Commit[];
    readonly failed: readonly string[];
    readonly reached: Readonly<Record<string, readonly Commit[]>>;
  }[] = [
    {
      name: 'names the commits that change a file Nx hashes for the task, in the land order',
      commits: [fix, docs, both],
      failed: ['app:test'],
      reached: { 'app:test': [fix, both] },
    },
    {
      name: 'names a commit that changes an input of a task the failed one waits for',
      commits: [fix, docs],
      failed: ['lib:lint', 'app:test'],
      reached: { 'app:test': [fix], 'lib:lint': [fix] },
    },
    {
      name: 'names no commit for a failure no commit reaches',
      commits: [docs],
      failed: ['app:lint'],
      reached: { 'app:lint': [] },
    },
    {
      name: 'names no commit for a land of no commits',
      commits: [],
      failed: ['lib:lint'],
      reached: { 'lib:lint': [] },
    },
    {
      name: 'names no commit that only deletes an input, since a deleted path is no input of the failed tree',
      commits: [{ sha: 'd'.repeat(40), subject: 'refactor: drop a file', paths: ['lib/src/old.ts'] }],
      failed: ['lib:lint'],
      reached: { 'lib:lint': [] },
    },
    {
      name: 'names each failed task once',
      commits: [both],
      failed: ['app:lint', 'app:lint'],
      reached: { 'app:lint': [both] },
    },
  ];
  for (const { name, commits, failed, reached } of cases) {
    it(name, () => {
      expect(attribute(commits, inputs, failed)).toEqual(
        Object.entries(reached).map(([task, named]) => ({ task, commits: named })),
      );
    });
  }

  it('refuses a failed task whose inputs Nx did not list', () => {
    expect(() => attribute([fix], inputs, ['lib:test'])).toThrow('the task inputs hold no files for lib:test');
  });

  it('says which commits reach each failure, and what a failure no commit reaches fails on', () => {
    expect(renderAttribution(attribute([fix, docs], inputs, ['app:lint', 'lib:lint']), 2)).toBe(
      "app:lint failed; none of the land's 2 commits changes a file Nx hashes for it or for a task it waits for (a commit that only deletes files is not seen): it fails on what the land shares with main, or on an input that is no workspace file\n" +
        "lib:lint failed; 1 of the land's 2 commits change its inputs: aaaaaaaaaa fix(lib): the fix\n",
    );
  });

  it('reads the inputs land-inputs.mts prints and refuses anything else', () => {
    expect(parseInputs(JSON.stringify(inputs))).toEqual({ kind: 'read', value: inputs });
    for (const text of ['[]', '{"a:lint":"a.ts"}', '{"a:lint":[1]}', '{']) {
      expect(parseInputs(text).kind).toBe('refused');
    }
  });
});
