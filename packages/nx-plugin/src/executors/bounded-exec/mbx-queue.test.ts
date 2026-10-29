import { expect, it } from 'bun:test';
import type { HangDiagnosticHost, HangProbeResult } from './hang-diagnostics.js';
import { MBX_QUEUE_SNAPSHOT_MS, snapshotMbxQueue } from './mbx-queue.js';

it('maps public admission positions only for live task PIDs, including descendants outside the group', async () => {
  const calls: { file: string; args: readonly string[]; budget: number }[] = [];
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
    // 11 reparented but retains the task group; 12 changed group but descends through 15; 13 is a zombie.
    exited('10 1 10 S\n11 1 10 S\n12 15 12 S\n20 1 20 S\n13 10 10 Z\n'),
    exited('15 10 15 S\n'),
  ];
  const host: Pick<HangDiagnosticHost, 'probe'> = {
    async probe(file, args, budget) {
      calls.push({ file, args, budget });
      clock += 1;
      const result = replies.shift();
      if (result === undefined) throw new Error('unexpected admission probe');
      return result;
    },
  };
  const controller = new AbortController();
  const observe = () => snapshotMbxQueue(10, { MBX_CACHE_DIR: '/pool' }, controller.signal, host, () => clock);
  expect(await observe()).toEqual({
    ok: true,
    requests: [
      { pid: 11, ticket: 70, queuePosition: 4 },
      { pid: 12, ticket: 80, queuePosition: 7 },
    ],
  });
  expect(calls).toEqual([
    { file: 'mbx', args: ['stats', '--scheduler', '--json', '--activity'], budget: MBX_QUEUE_SNAPSHOT_MS },
    { file: 'ps', args: ['-o', 'pid=,ppid=,pgid=,stat=', '-p', '10,11,12,20,13'], budget: MBX_QUEUE_SNAPSHOT_MS - 1 },
    { file: 'ps', args: ['-o', 'pid=,ppid=,pgid=,stat=', '-p', '15'], budget: MBX_QUEUE_SNAPSHOT_MS - 2 },
  ]);

  // Empty public state needs no process-table read and does not manufacture an admission event.
  replies.push(exited(JSON.stringify({ version: 2, scheduler: { activity: [] } })));
  expect(await observe()).toEqual({ ok: true, requests: [] });
  expect(calls.at(-1)?.file).toBe('mbx');

  // A vanished/reused PID outside the task is not its queue, even when mbx still lists it.
  replies.push(
    exited(JSON.stringify({ version: 2, scheduler: { activity: [activity[0]] } })),
    exited('10 1 10 S\n11 1 11 S\n'),
  );
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
    exited('10 1 10 S\n20 1 20 S\n'),
  );
  expect(await observe()).toEqual({
    ok: true,
    requests: [],
    diagnostics: [expect.stringContaining('task membership unavailable')],
  });
  replies.push(
    exited(JSON.stringify({ version: 2, scheduler: { activity: [{ ...activity[0], ticket: 0 }] } })),
    exited('10 1 10 S\n11 10 10 S\n'),
  );
  expect(await observe()).toEqual({ ok: true, requests: [{ pid: 11, ticket: 0, queuePosition: 4 }] });
  replies.push(
    exited(JSON.stringify({ version: 2, scheduler: { activity: [{ ...activity[0], ticket: null }] } })),
    exited('10 1 10 S\n11 10 10 S\n'),
  );
  expect(await observe()).toMatchObject({ ok: false, detail: expect.stringContaining('lacks ticket/queue position') });
  // Unknown liveness for a task waiter is a failure, never queue/empty evidence.
  replies.push(
    exited(
      JSON.stringify({ version: 2, scheduler: { activity: [{ ...activity[0], live: null, queue_position: null }] } }),
    ),
    exited('10 1 10 S\n11 10 10 S\n'),
  );
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
  replies.push(
    exited(JSON.stringify({ version: 2, scheduler: { activity: [activity[0]] } })),
    exited('malformed ps row'),
  );
  expect(await observe()).toMatchObject({ ok: false, detail: expect.stringContaining('invalid scoped ps row') });
  controller.abort();
  const before = calls.length;
  expect(await observe()).toMatchObject({ ok: false, detail: expect.stringContaining('deadline reached') });
  expect(calls).toHaveLength(before);
  expect(replies).toEqual([]);
});
