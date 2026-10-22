import { describe, expect, it, jest } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import fc from 'fast-check';
import { churnFailures, startChurn } from './__tests__/churn-group.js';
import {
  DEADLINE_PATIENCE,
  describeMembersLeft,
  describeTeardown,
  judgeReap,
  killProcessGroup,
  killProcessGroupNow,
  processGroupHasMembers,
  signalProcessGroup,
  TEARDOWN_PATIENCE,
} from './process-group.js';

/**
 * Ending a process group is decided by a pure function over facts, so its properties are properties of function calls:
 * no clock, process or environment. What the shell does with a verdict is proved against real processes.
 */

/** What a subtraction of two counters can hand over, including what is not a number. */
const anyReading = fc.oneof(
  fc.double({ min: -1e6, max: 1e9, noNaN: true }),
  fc.constantFrom(Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY),
);

describe('the decision to signal a process group again', () => {
  it('leaves an empty group alone at any round, whatever the patience', () => {
    fc.assert(
      fc.property(anyReading, anyReading, (round, rounds) => {
        expect(judgeReap({ round, patience: { rounds, pauseMs: 0 }, members: false })).toEqual({ kind: 'empty' });
      }),
    );
  });

  it('signals a group that keeps its members once per round of its patience, then gives up and keeps giving up', () => {
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 500 }), (rounds) => {
        const verdicts = Array.from(
          { length: rounds + 3 },
          (_, round) => judgeReap({ round, patience: { rounds, pauseMs: 0 }, members: true }).kind,
        );
        expect(verdicts.slice(0, rounds).every((kind) => kind === 'signal')).toBe(true);
        expect(verdicts.slice(rounds)).toEqual(['members-left', 'members-left', 'members-left']);
      }),
    );
  });

  it('signals only while its patience is a positive finite number of rounds ahead, whatever the facts', () => {
    fc.assert(
      fc.property(anyReading, anyReading, (round, rounds) => {
        const verdict = judgeReap({ round, patience: { rounds, pauseMs: 0 }, members: true });
        if (verdict.kind === 'signal') {
          expect(Number.isFinite(rounds - round)).toBe(true);
          expect(rounds - round).toBeGreaterThan(0);
        }
      }),
    );
  });

  it('has patiences that signal again after a pause, the one on the way out holding the exit up for little', () => {
    // A kill that is not repeated misses the member forked as it went out, and so does one repeated with no pause
    // between the signals: a fork already under way joins the group after they have all been sent. A process that is
    // ending should hold its exit up for no more than a moment, and a deadline's kill may wait for the group to empty.
    for (const patience of [TEARDOWN_PATIENCE, DEADLINE_PATIENCE]) {
      expect(Number.isInteger(patience.rounds)).toBe(true);
      expect(patience.rounds).toBeGreaterThanOrEqual(2);
      expect(patience.pauseMs).toBeGreaterThanOrEqual(1);
    }
    expect(TEARDOWN_PATIENCE.rounds * TEARDOWN_PATIENCE.pauseMs).toBeLessThanOrEqual(100);
    expect(DEADLINE_PATIENCE.rounds * DEADLINE_PATIENCE.pauseMs).toBeGreaterThanOrEqual(1000);
  });

  it('names the group, and the two ways a group left behind can mislead, in what is told of it', () => {
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 4_194_304 }), (group) => {
        const told = describeMembersLeft(group);
        expect(told).toContain(`process group ${group} still had members`);
        expect(told).toContain('left the group');
        expect(told).toContain('not been reaped');
        // The wait reaped the leader, so the leader is not what this says is listed, and nothing here is unconfirmed.
        expect(told).not.toContain('unconfirmed');
      }),
    );
  });

  it('names the command and the group in what a blocking kill leaves, as listed and not as running', () => {
    fc.assert(
      fc.property(fc.string({ minLength: 1 }), fc.integer({ min: 1, max: 4_194_304 }), (command, group) => {
        const told = describeTeardown({ command, group });
        expect(told).toContain(command);
        expect(told).toContain(`process group ${group} was still listed`);
        expect(told).toContain('unconfirmed');
        // Nothing here establishes that the kill worked, nor that members remain: what the unreaped leader alone
        // explains is neither.
        const rest = told.replace(command, '');
        expect(rest).not.toContain('was killed');
        expect(rest).not.toContain('still had members');
      }),
    );
  });
});

describe('a process group on this host', () => {
  it('is listed while it has a member and not once it has none', async () => {
    const child = spawn('sh', ['-c', 'exec tail -f /dev/null'], { detached: true, stdio: 'ignore' });
    const exited = once(child, 'exit');
    if (child.pid === undefined) throw new Error('the group leader did not start');
    expect(processGroupHasMembers(child.pid)).toBe(true);
    signalProcessGroup(child.pid, 'SIGKILL');
    await exited;
    expect(processGroupHasMembers(child.pid)).toBe(false);
    // A group that is already gone is not an error to signal.
    expect(() => signalProcessGroup(child.pid ?? 0, 'SIGKILL')).not.toThrow();
  });

  it('lists a leader nobody has reaped, which a kill that blocks cannot tell from a survivor', async () => {
    // The leader of a group nothing survives in is listed until its parent reaps it. A kill that blocks this process,
    // the parent, cannot reap it meanwhile and so finds the group listed after every signal: that is what
    // `describeTeardown` says "unconfirmed" about, and this is the evidence it may say no more.
    const child = spawn('sh', ['-c', 'exec sleep 600'], { detached: true, stdio: 'ignore' });
    const exited = once(child, 'exit');
    if (child.pid === undefined) throw new Error('the group leader did not start');
    expect(killProcessGroupNow(child.pid, TEARDOWN_PATIENCE)).toBe(true);
    await exited;
    expect(processGroupHasMembers(child.pid)).toBe(false);
  });

  it('ends a group while the clock is faked, because its pauses are host time and not a timer', async () => {
    // A test that fakes the clock stops every timer until it moves the clock, and a kill that paced itself on one
    // would wait for ever: the bounded-exec cases that fake the clock end a run at its bound and then kill its group.
    // Nothing here moves the clock. The pauses let the event loop reap the leader of a group nothing survives in, so
    // the group is empty when the kill is done.
    const child = spawn('sh', ['-c', 'exec sleep 600'], { detached: true, stdio: 'ignore' });
    const exited = once(child, 'exit');
    if (child.pid === undefined) throw new Error('the group leader did not start');
    jest.useFakeTimers();
    try {
      expect(await killProcessGroup(child.pid, DEADLINE_PATIENCE)).toBe(false);
    } finally {
      jest.useRealTimers();
    }
    await exited;
  });

  it('keeps its process alive while a kill on the event loop waits, with nothing else pending', () => {
    // `Atomics.waitAsync` keeps nothing alive: a process with only that pending exits under it, saying nothing (Node
    // ends it with an unsettled top-level await, and Bun lets a top-level await hold it, so this script has none). The
    // pause pairs it with a timer for that reason. Here nothing else is pending (the leader is not waited for and has
    // no stdio), so a pause that let the process go would end it before it said what the kill found.
    const script = [
      "import { spawn } from 'node:child_process';",
      `import { DEADLINE_PATIENCE, killProcessGroup } from ${JSON.stringify(join(import.meta.dir, 'process-group.ts'))};`,
      "const leader = spawn('sh', ['-c', 'exec sleep 600'], { detached: true, stdio: 'ignore' });",
      "if (leader.pid === undefined) throw new Error('the group leader did not start');",
      'leader.unref();',
      'killProcessGroup(leader.pid, DEADLINE_PATIENCE).then((left) => console.log(JSON.stringify({ left })));',
    ].join('\n');
    const run = spawnSync(process.execPath, ['-e', script], { encoding: 'utf8' });
    expect({ status: run.status, stdout: run.stdout.trim(), stderr: run.stderr }).toEqual({
      status: 0,
      stdout: '{"left":false}',
      stderr: '',
    });
  });

  it('treats a failure of the probe or the signal as a failure, never as a verdict', () => {
    // Only ESRCH proves a group empty and EPERM a member that cannot be signalled; a pid that is not a number is
    // neither, and nothing about the group follows from it.
    expect(() => processGroupHasMembers(Number.NaN)).toThrow();
    expect(() => signalProcessGroup(Number.NaN, 'SIGKILL')).toThrow();
  });

  it('is left with no member when members fork as the kill goes out, however the caller waits', async () => {
    // macOS does not hold a fork back for a group signal in flight, so a member forked as SIGKILL goes out is not
    // reached by it. The churn group forks as fast as it can while the kill is made, and the end of file of its FIFO is
    // the death of the last member: a kill that is not repeated leaves a case that only the backstop ends. The two
    // callers, one that can wait for nothing else and one that waits on the event loop, run the same loop.
    const kills = [
      ['blocking', (group: number) => killProcessGroupNow(group, TEARDOWN_PATIENCE)],
      ['on the event loop', (group: number) => killProcessGroup(group, DEADLINE_PATIENCE)],
    ] as const;
    for (const [how, kill] of kills) {
      for (const attempt of [1, 2, 3]) {
        const directory = await mkdtemp(join(tmpdir(), 'process-group-'));
        try {
          const churn = await startChurn(directory);
          await kill(churn.group);
          // The event-loop caller has waited for the group to empty, so a process that still holds the FIFO is a
          // survivor. The blocking caller leaves its leader unreaped and may leave members dying: only the end of
          // file speaks for it.
          const survivors = how === 'on the event loop' && churn.survivors();
          expect({ how, attempt, survivors, gone: await churn.gone, stderr: churnFailures(churn.stderr()) }).toEqual({
            how,
            attempt,
            survivors: false,
            gone: '',
            stderr: [],
          });
        } finally {
          await rm(directory, { recursive: true, force: true });
        }
      }
    }
  });
});
