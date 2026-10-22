import { describe, expect, it } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { closeSync, constants, mkdtempSync, openSync, readSync, realpathSync, rmSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  type BoundedCommand,
  type BoundedOutcome,
  type Clock,
  runBounded,
} from '../../../nx-plugin/managed/raw/tooling/direnv/cargo-fetch-run.ts';

/**
 * The shell that runs a fetch under its bound, against real child processes. Whether the deadline has passed is
 * decided by cargo-fetch-bound.ts (properties in cargo-fetch-bound.property.test.ts), so what is left to prove here is
 * what the shell does with a verdict: the group dies, the output is kept, nothing outlives the call. Time is the one
 * thing the test holds: a clock it moves by hand, so the deadline arrives after the command is up and never races it.
 */

const DEADLINE_MS = 60_000;

const RUN_MODULE = new URL('../../../nx-plugin/managed/raw/tooling/direnv/cargo-fetch-run.ts', import.meta.url)
  .pathname;

/**
 * A command whose members fork as fast as they can: four forkers start a sleeper and take it away again, over and over
 * (so that few of their processes are alive at a time), and behind them stand sleepers that make a signal take long to
 * go round the group. Every member holds the write end of `$1`, a FIFO, and the command says its pid, which leads the
 * group, to the FIFO `$2` once all of it is up. A forker stops after 2000 rounds, so a group nothing kills ends itself,
 * and a sleeper lasts ten minutes, longer than the no-progress backstop, so one that the kill missed is still there
 * when a case waiting for the FIFO's end of file would be ended by it. A forker's `kill` can find its sleeper already
 * taken by the group kill and say so on stderr: that is the fixture racing the kill, and the one line a case accepts.
 */
const CHURN = [
  'exec 3>"$1"',
  'forker() {',
  '  n=0; while [ $n -lt 2000 ]; do sleep 600 & pid=$!; kill $pid; wait $pid; n=$((n+1)); done',
  '}',
  'forker & forker & forker & forker &',
  'i=0; while [ $i -lt 150 ]; do sleep 600 & i=$((i+1)); done',
  'echo "$$" > "$2"',
  'wait',
].join('\n');

/**
 * The source of a program that runs CHURN for a case that ends the process running it. Its clock never wakes anything,
 * so the deadline cannot arrive: only the signal the case sends can end the group.
 */
const runnerSource = (cwd: string, lifetime: string, up: string): string =>
  [
    `import { runBounded } from ${JSON.stringify(RUN_MODULE)};`,
    'const inert = { now: () => 0, after: () => () => {} };',
    `await runBounded(${JSON.stringify({
      argv: ['sh', '-c', CHURN, 'sh', lifetime, up],
      cwd,
      env: { PATH: process.env.PATH ?? '' },
      deadlineMs: DEADLINE_MS,
      echo: false,
    })}, inert);`,
  ].join('\n');

/** A clock that moves only when `advance` does, waking what is due. */
function manualClock() {
  let now = 0;
  const wakeups = new Set<{ readonly at: number; readonly wake: () => void }>();
  const clock: Clock = {
    now: () => now,
    after: (ms, wake) => {
      const wakeup = { at: now + ms, wake };
      wakeups.add(wakeup);
      return () => {
        wakeups.delete(wakeup);
      };
    },
  };
  return {
    clock,
    advance(ms: number) {
      now += ms;
      for (const wakeup of [...wakeups]) {
        if (wakeup.at > now) continue;
        wakeups.delete(wakeup);
        wakeup.wake();
      }
    },
    pending: () => wakeups.size,
  };
}

const command = (cwd: string, ...argv: string[]): BoundedCommand => ({
  argv,
  cwd,
  env: { PATH: process.env.PATH ?? '' },
  deadlineMs: DEADLINE_MS,
  echo: false,
});

const text = (bytes: Uint8Array) => new TextDecoder().decode(bytes);

/** What a forker of CHURN says when the group kill takes its sleeper before its own `kill` does: the race. */
const FORKER_RACE = /: kill: .*No such process$/;

/** The lines a command wrote to stderr that are not that race: a case reports them and accepts none. */
const failures = (stderr: Uint8Array): string[] =>
  text(stderr)
    .split('\n')
    .filter((line) => line !== '' && !FORKER_RACE.test(line));

/**
 * Whether a process still holds the write end of the FIFO `reader` was opened on without waiting: a read that would
 * wait finds one (EAGAIN) and end of file finds none. Nothing in these tests writes to the FIFO.
 */
function writerRemains(reader: number): boolean {
  try {
    return readSync(reader, Buffer.alloc(1)) > 0;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'EAGAIN') return true;
    throw error;
  }
}

describe('a command run under its bound', () => {
  it('is killed with its whole process group at its deadline, however its members take a signal', async () => {
    // A fetch whose git child stalls: a shell that starts two members that never end, one deaf to every polite signal,
    // then says `up`. Every member holds the write end of `lifetime`, so its end of file is the death of the last one;
    // the deadline arrives only after `up`, so every member exists when the shell kills the group.
    const tree = [
      'exec 3>"$1"',
      '(trap "" TERM INT HUP; exec tail -f /dev/null) &',
      'tail -f /dev/null &',
      'echo up > "$2"',
      'wait',
    ].join('\n');
    const scratch = mkdtempSync(join(realpathSync(tmpdir()), 'cargo-fetch-run-'));
    const time = manualClock();
    let run: Promise<BoundedOutcome> | undefined;
    try {
      const lifetime = join(scratch, 'lifetime');
      const up = join(scratch, 'up');
      execFileSync('mkfifo', [lifetime, up]);
      // Each end of a FIFO opens only once the other does: read both before the command can write.
      const ended = readFile(lifetime, 'utf8');
      const said = readFile(up, 'utf8');
      run = runBounded(command(scratch, 'sh', '-c', tree, 'sh', lifetime, up), time.clock);
      expect(await said).toBe('up\n');

      time.advance(DEADLINE_MS);
      expect(await run).toMatchObject({ kind: 'expired', elapsedMs: DEADLINE_MS });
      expect(await ended).toBe('');
      expect(time.pending()).toBe(0);
    } finally {
      // A case that failed before the deadline must not leave its tree running.
      time.advance(DEADLINE_MS);
      await run?.catch(() => undefined);
      rmSync(scratch, { recursive: true, force: true });
    }
  });

  it('leaves no member behind when members fork as the kill goes out, as they do on macOS', async () => {
    // macOS does not hold a fork back for a group signal in flight, so a member forked as SIGKILL goes out is not
    // reached by it, and CHURN forks as fast as it can while the deadline arrives. A sleeper the signal missed holds
    // the write end of `lifetime` when the call returns, and a read of the FIFO that would wait finds it. Each of three
    // groups has its own chance of missing one, so a shell that signals once fails this nearly every time and one that
    // repeats the kill never does.
    const scratch = mkdtempSync(join(realpathSync(tmpdir()), 'cargo-fetch-run-'));
    try {
      for (const attempt of [1, 2, 3]) {
        const time = manualClock();
        const lifetime = join(scratch, `lifetime-${attempt}`);
        const up = join(scratch, `up-${attempt}`);
        execFileSync('mkfifo', [lifetime, up]);
        // A FIFO opens for reading without waiting when no process has it open for writing yet, and then lets the
        // command open its end.
        const reader = openSync(lifetime, constants.O_RDONLY | constants.O_NONBLOCK);
        const said = readFile(up, 'utf8');
        let run: Promise<BoundedOutcome> | undefined;
        try {
          run = runBounded(command(scratch, 'sh', '-c', CHURN, 'sh', lifetime, up), time.clock);
          // A command that cannot start, or ends before it is up, would leave the read of `up` waiting for ever.
          const first = await Promise.race([said, run]);
          if (typeof first !== 'string') {
            const ending = JSON.stringify({ kind: first.kind, stderr: text(first.stderr) });
            throw new Error(`the command ended before it was up: ${ending}`);
          }
          const group = Number.parseInt(first, 10);
          time.advance(DEADLINE_MS);
          const outcome = await run;
          expect(outcome).toMatchObject({ kind: 'expired', group });
          expect({ attempt, stderr: failures(outcome.stderr), writerRemains: writerRemains(reader) }).toEqual({
            attempt,
            stderr: [],
            writerRemains: false,
          });
        } finally {
          // An attempt that failed before the deadline must not leave its group running.
          time.advance(DEADLINE_MS);
          await run?.catch(() => undefined);
          closeSync(reader);
        }
      }
    } finally {
      rmSync(scratch, { recursive: true, force: true });
    }
  });

  it('leaves no member behind when the process running them is ended as they fork, as they do on macOS', async () => {
    // The same group with the kill made on the way out: a child process runs the command on a clock that never wakes,
    // so only the SIGTERM it is sent once the group is up can end the group. It must end by that signal, saying
    // nothing on stderr, and the end of file of `lifetime` is the death of the last member, however long a loaded host
    // takes to finish exiting what the kill reached. A member the kill missed is never the last to end, so a shell that
    // kills the group once on the way out leaves a case that only the backstop ends.
    const scratch = mkdtempSync(join(realpathSync(tmpdir()), 'cargo-fetch-run-'));
    try {
      for (const attempt of [1, 2, 3]) {
        const lifetime = join(scratch, `lifetime-${attempt}`);
        const up = join(scratch, `up-${attempt}`);
        execFileSync('mkfifo', [lifetime, up]);
        // Each end of a FIFO opens only once the other does: read both before the command can write.
        const lifetimeEnded = readFile(lifetime, 'utf8');
        const said = readFile(up, 'utf8');
        const stderr: Buffer[] = [];
        const runner = spawn(process.execPath, ['-e', runnerSource(scratch, lifetime, up)], {
          stdio: ['ignore', 'ignore', 'pipe'],
        });
        runner.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));
        const ended = new Promise<{ code: number | null; signal: NodeJS.Signals | null; error?: string }>((resolve) => {
          runner.once('exit', (code, signal) => resolve({ code, signal }));
          runner.once('error', (error) => resolve({ code: null, signal: null, error: error.message }));
        });
        try {
          // A runner that cannot start, or ends before the group is up, would leave the read of `up` waiting for ever.
          const first = await Promise.race([said, ended]);
          if (typeof first !== 'string') {
            const ending = JSON.stringify({ ...first, stderr: Buffer.concat(stderr).toString() });
            throw new Error(`the runner ended before the group was up: ${ending}`);
          }
          runner.kill('SIGTERM');
          expect(await ended).toEqual({ code: null, signal: 'SIGTERM' });
          expect({ attempt, stderr: Buffer.concat(stderr).toString() }).toEqual({ attempt, stderr: '' });
          expect(await lifetimeEnded).toBe('');
        } finally {
          // An attempt that failed before the signal must not leave its group running.
          runner.kill('SIGTERM');
          await ended;
        }
      }
    } finally {
      rmSync(scratch, { recursive: true, force: true });
    }
  });

  it('returns what a command that ends before its deadline printed and exited with, leaving no timer behind', async () => {
    const scratch = mkdtempSync(join(realpathSync(tmpdir()), 'cargo-fetch-run-'));
    try {
      for (const [script, exitCode] of [
        ['printf out; printf err >&2; exit 3', 3],
        ['printf out; printf err >&2; kill -KILL $$', 137],
        ['printf out; printf err >&2', 0],
      ] as const) {
        const time = manualClock();
        const outcome = await runBounded(command(scratch, 'sh', '-c', script), time.clock);
        expect(outcome.kind).toBe('exited');
        if (outcome.kind !== 'exited') throw new Error(`${script} did not end by itself`);
        expect({
          script,
          exitCode: outcome.exitCode,
          stdout: text(outcome.stdout),
          stderr: text(outcome.stderr),
        }).toEqual({
          script,
          exitCode,
          stdout: 'out',
          stderr: 'err',
        });
        expect(time.pending()).toBe(0);
      }
    } finally {
      rmSync(scratch, { recursive: true, force: true });
    }
  });

  it('rejects a command that cannot start, leaving no hook on this process behind', async () => {
    const hooks = () => ['exit', 'SIGINT', 'SIGTERM', 'SIGHUP'].map((event) => process.listenerCount(event));
    const before = hooks();
    await expect(
      runBounded(command(tmpdir(), '/nonexistent/cargo-fetch-run-test'), manualClock().clock),
    ).rejects.toThrow();
    expect(hooks()).toEqual(before);
  });
});
