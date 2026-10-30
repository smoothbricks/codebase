import { expect, it } from 'bun:test';
import type { ProcessLineage } from '@smoothbricks/cowshed/process-lineage';
import type { HangProbeResult } from './hang-diagnostics.js';
import { MBX_QUEUE_SNAPSHOT_MS, snapshotMbxQueue } from './mbx-queue.js';

it('maps public admission positions only for live task PIDs, including descendants outside the group', async () => {
  const calls: { file: string; args: readonly string[]; budget: number }[] = [];
  const lineageCalls: (readonly number[])[] = [];
  let lineageElapsed = 1;
  let probeElapsed = 1;
  let abortDuring: 'probe' | 'lineages' | null = null;
  const lineage = (pid: number, ppid: number, pgid: number, zombie = false): ProcessLineage => ({
    pid,
    ppid,
    pgid,
    zombie,
  });
  let clock = 10;
  const exited = (stdout: string): HangProbeResult => ({ status: 'exited', code: 0, stdout, stderr: '' });
  const activity = [
    { kind: 'waiter', live: true, pid: 11, ticket: 70, queue_position: 4 },
    { kind: 'waiter', live: true, pid: 12, ticket: 80, queue_position: 7 },
    { kind: 'waiter', live: true, pid: 20, ticket: 90, queue_position: 8 },
    { kind: 'waiter', live: true, pid: 13, ticket: 100, queue_position: 9 },
    { kind: 'waiter', live: false, pid: 14, ticket: 110, queue_position: null },
    { kind: 'lease', live: null, pid: null, ticket: null, queue_position: null },
  ];
  const replies: HangProbeResult[] = [
    exited(JSON.stringify({ version: 2, scheduler: { activity, additive: 'accepted' }, extra: 1 })),
  ];
  const lineages: (readonly ProcessLineage[] | Error)[] = [
    // 11 reparented but retains the task group; 12 changed group but descends through 15; 13 is a zombie.
    [lineage(10, 1, 10), lineage(11, 1, 10), lineage(12, 15, 12), lineage(20, 1, 20), lineage(13, 10, 10, true)],
    [lineage(15, 10, 15)],
  ];
  const host = {
    now: () => clock,
    async probe(file: string, args: readonly string[], budget: number) {
      calls.push({ file, args, budget });
      if (file !== 'mbx') throw new Error('membership must never spawn a process');
      clock += probeElapsed;
      if (abortDuring === 'probe') controller.abort();
      const result = replies.shift();
      if (result === undefined) throw new Error('unexpected admission probe');
      return result;
    },
    async lineages(pids: readonly number[]) {
      lineageCalls.push(pids);
      clock += lineageElapsed;
      if (abortDuring === 'lineages') controller.abort();
      const result = lineages.shift();
      if (result === undefined) throw new Error('unexpected native lineage read');
      if (result instanceof Error) throw result;
      return result;
    },
  };
  let controller = new AbortController();
  const observe = () => snapshotMbxQueue(10, { MBX_CACHE_DIR: '/pool' }, controller.signal, host);
  expect(await observe()).toEqual({
    ok: true,
    requests: [
      { pid: 11, ticket: 70, queuePosition: 4 },
      { pid: 12, ticket: 80, queuePosition: 7 },
    ],
  });
  expect(calls).toEqual([
    { file: 'mbx', args: ['stats', '--scheduler', '--json', '--activity'], budget: MBX_QUEUE_SNAPSHOT_MS },
  ]);
  expect(lineageCalls).toEqual([[10, 11, 12, 20, 13], [15]]);

  // Empty public state needs no process-table read and does not manufacture an admission event.
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [] } })));
  expect(await observe()).toEqual({ ok: true, requests: [] });
  expect(calls.at(-1)?.file).toBe('mbx');

  // A vanished/reused PID outside the task is not its queue, even when mbx still lists it.
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [activity[0]] } })));
  lineages.push([lineage(10, 1, 10), lineage(11, 1, 11)]);
  expect(await observe()).toEqual({ ok: true, requests: [] });

  // Legacy/unavailable foreign rows cannot fail this task before PID membership is established.
  replies.push(
    exited(
      JSON.stringify({
        version: 2,
        scheduler: {
          activity: [
            { kind: 'waiter', pid: null, ticket: null, queue_position: null, live: true },
            { kind: 'waiter', pid: 20, ticket: null, queue_position: null, live: null },
          ],
        },
      }),
    ),
  );
  lineages.push([lineage(10, 1, 10), lineage(20, 1, 20)]);
  expect(await observe()).toEqual({
    ok: true,
    requests: [],
    diagnostics: [expect.stringContaining('task membership unavailable')],
  });
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [{ ...activity[0], ticket: 0 }] } })));
  lineages.push([lineage(10, 1, 10), lineage(11, 10, 10)]);
  expect(await observe()).toEqual({ ok: true, requests: [{ pid: 11, ticket: 0, queuePosition: 4 }] });
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [{ ...activity[0], ticket: null }] } })));
  lineages.push([lineage(10, 1, 10), lineage(11, 10, 10)]);
  expect(await observe()).toMatchObject({ ok: false, detail: expect.stringContaining('lacks ticket/queue position') });
  // Unknown liveness for a task waiter is a failure, never queue/empty evidence.
  replies.push(
    exited(
      JSON.stringify({ version: 2, scheduler: { activity: [{ ...activity[0], live: null, queue_position: null }] } }),
    ),
  );
  lineages.push([lineage(10, 1, 10), lineage(11, 10, 10)]);
  expect(await observe()).toMatchObject({ ok: false, detail: expect.stringContaining('live status unavailable') });

  for (const reply of [
    exited('{'),
    exited(JSON.stringify({ version: 1, scheduler: { activity: [] } })),
    exited(JSON.stringify({ scheduler: { activity: [] } })),
    exited(JSON.stringify({ version: 2, scheduler: { activity: [{ kind: 'waiter', pid: 11 }] } })),
    { status: 'timed-out', detail: 'probe exceeded bound', stdout: '', stderr: 'diagnostic' } satisfies HangProbeResult,
    { status: 'exited', code: 2, stdout: '', stderr: 'pool unreadable' } satisfies HangProbeResult,
  ]) {
    replies.push(reply);
    expect(await observe()).toMatchObject({ ok: false, detail: expect.any(String) });
  }
  // Native operational failures remain failures, never empty membership or a reason to disable observation.
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [activity[0]] } })));
  lineages.push(new Error('cannot read pid 11: permission denied (errno 1)'));
  expect(await observe()).toMatchObject({
    ok: false,
    detail: expect.stringContaining('native process lineages: cannot read pid 11: permission denied (errno 1)'),
  });
  // A disappeared PID is omitted by the native owner and proves no task membership.
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [activity[0]] } })));
  lineages.push([lineage(10, 1, 10)]);
  expect(await observe()).toEqual({ ok: true, requests: [] });
  // Native work spends the same snapshot budget; a late observation never becomes queue evidence.
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [activity[0]] } })));
  lineages.push([lineage(10, 1, 10), lineage(11, 10, 10)]);
  lineageElapsed = MBX_QUEUE_SNAPSHOT_MS;
  expect(await observe()).toMatchObject({ ok: false, detail: expect.stringContaining('deadline reached') });
  // Empty stats is not evidence when the snapshot's deadline was spent collecting it.
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [] } })));
  probeElapsed = MBX_QUEUE_SNAPSHOT_MS;
  expect(await observe()).toMatchObject({ ok: false, detail: expect.stringContaining('deadline reached') });
  probeElapsed = 1;
  lineageElapsed = 1;
  // Cancellation at either awaited boundary cannot manufacture empty or queued membership.
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [activity[0]] } })));
  lineages.push([lineage(10, 1, 10), lineage(11, 10, 10)]);
  abortDuring = 'lineages';
  expect(await observe()).toMatchObject({ ok: false, detail: expect.stringContaining('deadline reached') });
  controller = new AbortController();
  abortDuring = 'probe';
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [] } })));
  expect(await observe()).toMatchObject({ ok: false, detail: expect.stringContaining('deadline reached') });
  controller.abort();
  const before = calls.length;
  const beforeLineages = lineageCalls.length;
  expect(await observe()).toMatchObject({ ok: false, detail: expect.stringContaining('deadline reached') });
  expect(calls).toHaveLength(before);
  expect(lineageCalls).toHaveLength(beforeLineages);
  expect(lineages).toEqual([]);
  expect(calls.every(({ file }) => file === 'mbx')).toBe(true);
  expect(replies).toEqual([]);
});
