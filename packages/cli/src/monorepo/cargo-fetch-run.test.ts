import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
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

describe('a command run under its bound', () => {
  it('is killed with everything it started when its deadline arrives, however its members take a signal', async () => {
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
