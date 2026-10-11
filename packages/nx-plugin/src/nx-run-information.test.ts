import { expect, it } from 'bun:test';
import type { Task } from 'nx/src/config/task-graph';
import { StoreRunInformationLifeCycle } from 'nx/src/tasks-runner/life-cycles/store-run-information-life-cycle';

/**
 * Guards the run-information hunk of patches/nx@23.2.1.patch. A discrete worker can fail hashing before startTasks:
 * the orchestrator still reports failure through endTasks, with endTime but no startTime. Stock Nx dereferences the
 * absent timing and crashes over the hash error. Keep the failed result, but never invent an execution start.
 * Drop the hunk only when upstream accepts this lifecycle ordering, retaining these regressions.
 */
const NOW = '2026-10-11T17:00:00.000Z';
const START = Date.parse('2026-10-11T16:38:43.000Z');
const END = Date.parse('2026-10-11T16:38:43.418Z');

function task(timing: Pick<Task, 'startTime' | 'endTime'> = {}): Task {
  return {
    id: 'p:check',
    target: { project: 'p', target: 'check' },
    overrides: {},
    outputs: [],
    cache: false,
    ...timing,
  };
}

it('records a failure before startTasks without inventing a start time or masking its error', () => {
  const stored: unknown[] = [];
  const cycle = new StoreRunInformationLifeCycle(
    'nx run p:check',
    (details) => stored.push(details),
    () => NOW,
  );
  const failed = task({ endTime: END });
  cycle.startCommand();
  expect(() =>
    cycle.endTasks([{ task: failed, status: 'failure', code: 1, terminalOutput: 'write EPIPE' }]),
  ).not.toThrow();
  cycle.endCommand();
  expect(stored).toEqual([
    {
      run: { command: 'nx run p:check', startTime: NOW, endTime: NOW, inner: false },
      tasks: [
        {
          taskId: failed.id,
          target: 'check',
          projectName: 'p',
          hash: undefined,
          startTime: undefined,
          endTime: new Date(END).toISOString(),
          params: '',
          cacheStatus: 'cache-miss',
          status: 1,
        },
      ],
    },
  ]);
  expect(failed.startTime).toBeUndefined();
});

it('uses the completion clock when an unstarted failure has neither timestamp', () => {
  const stored: unknown[] = [];
  const cycle = new StoreRunInformationLifeCycle(
    'check',
    (details) => stored.push(details),
    () => NOW,
  );
  cycle.startCommand();
  cycle.endTasks([{ task: task(), status: 'failure', code: 1 }]);
  cycle.endCommand();
  expect(stored).toMatchObject([{ tasks: [{ startTime: undefined, endTime: NOW, status: 1 }] }]);
});

it('retains supplied task timing even without a startTasks notification', () => {
  const stored: unknown[] = [];
  const cycle = new StoreRunInformationLifeCycle(
    'check',
    (details) => stored.push(details),
    () => NOW,
  );
  cycle.startCommand();
  cycle.endTasks([{ task: task({ startTime: START, endTime: END }), status: 'success', code: 0 }]);
  cycle.endCommand();
  expect(stored).toMatchObject([
    { tasks: [{ startTime: new Date(START).toISOString(), endTime: new Date(END).toISOString(), status: 0 }] },
  ]);
});

it('keeps normal started-task and cached-task timing unchanged', () => {
  for (const status of ['success', 'local-cache', 'remote-cache'] satisfies readonly (
    | 'success'
    | 'local-cache'
    | 'remote-cache'
  )[]) {
    const stored: unknown[] = [];
    const cycle = new StoreRunInformationLifeCycle(
      'check',
      (details) => stored.push(details),
      () => NOW,
    );
    const started = task({ startTime: START, endTime: END });
    cycle.startCommand();
    cycle.startTasks([started]);
    cycle.endTasks([{ task: started, status, code: 0 }]);
    cycle.endCommand();
    expect(stored).toMatchObject([
      {
        tasks: [
          {
            startTime: new Date(START).toISOString(),
            endTime: new Date(END).toISOString(),
            status: 0,
            cacheStatus: status === 'success' ? 'cache-miss' : `${status}-hit`,
          },
        ],
      },
    ]);
  }
});
