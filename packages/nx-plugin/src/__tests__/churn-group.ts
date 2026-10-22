import { execFileSync, spawn } from 'node:child_process';
import { closeSync, constants, openSync, readSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

/**
 * The script of a command whose members fork as fast as they can: four forkers start a sleeper and take it away again,
 * over and over (so that few of their processes are alive at a time), and behind them stand sleepers that make a signal
 * take long to go round the group. macOS does not hold a fork back for a group signal in flight, so a member forked as
 * a SIGKILL goes out is not reached by it, and a kill that is not repeated leaves one behind nearly every time.
 *
 * Every member holds the write end of the FIFO `$1`, and the command says its pid, which leads the group, to the FIFO
 * `$2` once all of it is up. A forker stops after 2000 rounds, so a group nothing kills ends itself, and a sleeper
 * lasts ten minutes, longer than the no-progress backstop, so one the kill missed is still there when a case waiting
 * for the FIFO's end of file would be ended by it. `ignoreTerm` makes every member but the command itself deaf to
 * SIGTERM.
 */
export function churnScript(options: { readonly ignoreTerm?: boolean } = {}): string {
  const deaf = options.ignoreTerm === true ? 'trap "" TERM; ' : '';
  return [
    'exec 3>"$1"',
    'forker() {',
    `  ${deaf}n=0; while [ $n -lt 2000 ]; do sleep 600 & pid=$!; kill -KILL $pid; wait $pid; n=$((n+1)); done`,
    '}',
    'forker & forker & forker & forker &',
    `( ${deaf}i=0; while [ $i -lt 150 ]; do sleep 600 & i=$((i+1)); done ) &`,
    'wait $!',
    'echo "$$" > "$2"',
    'wait',
  ].join('\n');
}

/** What a forker says when a group kill takes its sleeper before its own `kill` does: the fixture's race, no failure. */
const FORKER_RACE = /: kill: .*No such process$/;

/** The lines a churn group wrote to stderr that are not that race: a case reports them and accepts none. */
export function churnFailures(stderr: string): string[] {
  return stderr.split('\n').filter((line) => line !== '' && !FORKER_RACE.test(line));
}

/**
 * Whether a process holds the write end of the FIFO `reader` was opened on without waiting, right now: a read that
 * would wait finds one (EAGAIN) and end of file finds none. A member that is dying still holds it, so ask only after
 * a kill that has waited for its group to empty. Nothing in these fixtures writes to the FIFO.
 */
export function writerRemains(reader: number): boolean {
  try {
    return readSync(reader, Buffer.alloc(1)) > 0;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'EAGAIN') return true;
    throw error;
  }
}

/**
 * Opens `fifo` for reading without waiting, as `writerRemains` wants: it is allowed with no writer yet, and it lets
 * the command open its end.
 */
export function openForProbe(fifo: string): number {
  return openSync(fifo, constants.O_RDONLY | constants.O_NONBLOCK);
}

export interface ChurnGroup {
  /** The process group's id, which is the command's pid. */
  readonly group: number;
  /** Ends, empty, when no process holds the write end of the group's FIFO any more: the last member has gone. */
  readonly gone: Promise<string>;
  /** `writerRemains` for the group's FIFO; once `gone` has ended no process holds it, so it is false. */
  survivors(): boolean;
  /** What the command has written to stderr so far. */
  stderr(): string;
}

/** Starts the churn command in `directory` as a process group of its own, and resolves once all of it is up. */
export async function startChurn(directory: string): Promise<ChurnGroup> {
  const lifetime = join(directory, 'lifetime');
  const up = join(directory, 'up');
  execFileSync('mkfifo', [lifetime, up]);
  // Each end of a FIFO opens only once the other does: read both before the command can write.
  // The probe's reader opens first, without waiting, which lets the command open its end; it is closed once the last
  // writer has gone.
  const reader = openForProbe(lifetime);
  let eof = false;
  const gone = readFile(lifetime, 'utf8').finally(() => {
    eof = true;
    closeSync(reader);
  });
  const said = readFile(up, 'utf8');
  const child = spawn('sh', ['-c', churnScript(), 'sh', lifetime, up], {
    detached: true,
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  const written: Buffer[] = [];
  child.stderr.on('data', (chunk: Buffer) => written.push(chunk));
  const stderr = (): string => Buffer.concat(written).toString();
  const ended = new Promise<{ code: number | null; signal: NodeJS.Signals | null; error?: string }>((resolve) => {
    child.once('exit', (code, signal) => resolve({ code, signal }));
    child.once('error', (error) => resolve({ code: null, signal: null, error: error.message }));
  });
  // A command that cannot start, or ends before it is up, would leave the read of `up` waiting for ever.
  const first = await Promise.race([said, ended]);
  if (typeof first !== 'string') {
    throw new Error(`the churn command ended before it was up: ${JSON.stringify({ ...first, stderr: stderr() })}`);
  }
  return { group: Number.parseInt(first, 10), gone, survivors: () => !eof && writerRemains(reader), stderr };
}
