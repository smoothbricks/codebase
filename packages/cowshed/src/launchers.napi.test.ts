/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { constants } from 'node:fs';
import { mkdtemp, open, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';
import {
  ADMISSION_LAST,
  CowshedError,
  canCancelOwner,
  HOLD_LAST,
  type HoldEvent,
  type HoldRun,
  type LauncherExit,
  LineDecoder,
  type LockRun,
  type LockWaiterEvent,
  openHost,
  spawnHold,
  spawnLandTurn,
  spawnLock,
  spawnSubtreeRoot,
} from '../dist/ts/index.js';
import { DescriptorEvents } from '../dist/ts/launchers.js';
import { parseHoldEvent, parseJobSubtreeAdmission } from '../dist/ts/validators.generated.js';

const encode = (text: string) => new TextEncoder().encode(text);

/**
 * What a run's command writes to the fifo at `path`, or a failure if the run ends first; the
 * pending read is released by the fixture's disposal.
 */
async function heard(path: string, run: { readonly exit: Promise<LauncherExit> }): Promise<string> {
  const first = await Promise.race([
    readFile(path, 'utf8').then((text) => ({ text })),
    run.exit.then((exit) => ({ exit })),
  ]);
  if ('exit' in first) {
    throw new Error(`the run ended before writing ${path}: ${JSON.stringify(first.exit)}`);
  }
  return first.text;
}

/**
 * One test's directory, fifos and launched runs. `dispose` owns every run on any outcome, once
 * however often it is called. It first opens a peer of every fifo, a reader and writer in one, kept
 * open until every run has ended, so a process that opens a fifo at any time, before disposal or
 * after it, never waits for the other end; it stops each run through its verb's own request, hands
 * every fifo one line per read its runs make of it, and awaits every run's end before closing the
 * peers and removing the directory. A failed assertion leaks no process, and every wait in these
 * tests is on a fifo, never a clock.
 */
class Fixture {
  readonly root: string;
  readonly #fifos: { readonly path: string; readonly reads: number }[] = [];
  #disposed: Promise<void> | undefined;
  readonly #stops: (() => void)[] = [];
  readonly #ends: Promise<unknown>[] = [];

  private constructor(root: string) {
    this.root = root;
  }

  static async create(prefix: string): Promise<Fixture> {
    return new Fixture(await mkdtemp(join(tmpdir(), prefix)));
  }

  /** A fifo the fixture's runs read `reads` times (a run only writing it reads it 0 times). */
  fifo(name: string, reads: number): string {
    const path = join(this.root, name);
    const made = spawnSync('mkfifo', [path]);
    expect(made.status).toBe(0);
    this.#fifos.push({ path, reads });
    return path;
  }

  /** Own `run`: `stop` is its verb's request to end, and its exit is awaited at disposal. */
  own<R extends { readonly exit: Promise<LauncherExit> }>(run: R, stop: (run: R) => void): R {
    this.#stops.push(() => stop(run));
    this.#ends.push(run.exit);
    return run;
  }

  dispose(): Promise<void> {
    this.#disposed ??= this.#dispose();
    return this.#disposed;
  }

  async #dispose(): Promise<void> {
    const peers = await Promise.all(
      this.#fifos.map(async ({ path, reads }) => ({
        handle: await open(path, constants.O_RDWR | constants.O_NONBLOCK),
        reads,
      })),
    );
    try {
      for (const stop of this.#stops) {
        stop();
      }
      // A shell's read takes one line and leaves the rest for the next.
      await Promise.all(peers.map(({ handle, reads }) => handle.write('go\n'.repeat(reads))));
      await Promise.all(this.#ends);
    } finally {
      await Promise.all(peers.map(({ handle }) => handle.close()));
      await rm(this.root, { recursive: true, force: true });
    }
  }
}

const stopLock = (run: LockRun) => run.forward('TERM');
const stopHold = (run: HoldRun) => run.control.close();
const nothing = () => {};

/** The next event an iterator yields, which must exist. */
async function next<T>(events: AsyncIterator<T>): Promise<T> {
  const result = await events.next();
  if (result.done === true) {
    throw new Error('the descriptor ended before the event this test waits for');
  }
  return result.value;
}

const exited = (code: number): Pick<LauncherExit, 'status'> => ({ status: { kind: 'exited', code } });

describe('LineDecoder', () => {
  const started = '{"kind":"failed","reason":"no"}';

  it('decodes whole lines across chunk boundaries and ends cleanly after a whole line', () => {
    const decoder = new LineDecoder('handle', parseJobSubtreeAdmission, ADMISSION_LAST);
    const line = '{"kind":"refused","reason":"foldUnavailable"}\n';
    expect(decoder.push(encode(line.slice(0, 10)))).toEqual([]);
    expect(decoder.push(encode(line.slice(10)))).toEqual([{ event: { kind: 'refused', reason: 'foldUnavailable' } }]);
    expect(decoder.end()).toEqual([]);
  });

  it('refuses a malformed line, a truncated line and a line after the last, keeping their bytes', () => {
    const malformed = new LineDecoder('state', parseHoldEvent, HOLD_LAST).push(encode('{"kind":"nope"}\n'));
    expect(malformed).toHaveLength(1);
    const [line] = malformed;
    expect(line !== undefined && 'violation' in line && line.violation.bytes).toEqual(encode('{"kind":"nope"}'));

    const truncated = new LineDecoder('state', parseHoldEvent, HOLD_LAST);
    expect(truncated.push(encode(started))).toEqual([]);
    const [end] = truncated.end();
    expect(end !== undefined && 'violation' in end && end.violation.reason).toContain('without its newline');

    const after = new LineDecoder('state', parseHoldEvent, HOLD_LAST).push(encode(`${started}\n${started}\n`));
    expect(after[0]).toEqual({ event: { kind: 'failed', reason: 'no' } });
    expect(after[1] !== undefined && 'violation' in after[1] && after[1].violation.reason).toContain('sends last');
  });

  it('a second admission record is a violation, never an overwrite', () => {
    const record = '{"kind":"refused","reason":"foldUnavailable"}\n';
    const decoded = new LineDecoder('handle', parseJobSubtreeAdmission, ADMISSION_LAST).push(encode(record + record));
    expect(decoded).toHaveLength(2);
    const [, second] = decoded;
    expect(second !== undefined && 'violation' in second).toBe(true);
  });
});

describe('DescriptorEvents', () => {
  it('yields the events before a violation, then throws integrity carrying it, and keeps it', async () => {
    const stream = new PassThrough();
    const events = new DescriptorEvents(stream, new LineDecoder('state', parseHoldEvent, HOLD_LAST), {});
    stream.end(encode('{"kind":"failed","reason":"no"}\n{"kind":"nope"}\n'));
    expect(await next(events)).toEqual({ kind: 'failed', reason: 'no' });
    const thrown = await events.next().then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(thrown).toBeInstanceOf(CowshedError);
    if (!(thrown instanceof CowshedError)) {
      throw new Error('the state iterator threw no CowshedError');
    }
    expect(thrown.code).toBe('integrity');
    expect(thrown.violation?.descriptor).toBe('state');
    expect(thrown.violation?.bytes).toEqual(encode('{"kind":"nope"}'));
    expect(thrown.violation?.reason).toContain('sends last');
    await events.drained;
    expect(events.violation).toEqual(thrown.violation);
  });
});

describe('DescriptorEvents on a read failure', () => {
  it('throws the failure, keeps it, and ends what it can no longer observe', async () => {
    const stream = new PassThrough();
    let lost = 0;
    const events = new DescriptorEvents(stream, new LineDecoder('state', parseHoldEvent, HOLD_LAST), {
      onLost: () => {
        lost += 1;
      },
    });
    stream.destroy(new Error('the pipe broke'));
    await events.drained;
    const thrown = await events.next().then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(thrown).toBeInstanceOf(CowshedError);
    expect(events.failure).toContain('the pipe broke');
    expect(lost).toBe(1);
  });
});

describe('canCancelOwner', () => {
  it('admits the owner request only while the waiter is queued', () => {
    const said = (...kinds: LockWaiterEvent['kind'][]) => new Set(kinds);
    expect(canCancelOwner(said())).toBe(false);
    expect(canCancelOwner(said('queued'))).toBe(true);
    expect(canCancelOwner(said('queued', 'notSignalled'))).toBe(true);
    for (const ending of ['acquired', 'failed', 'signalled', 'commandFailed'] as const) {
      expect(canCancelOwner(said('queued', ending))).toBe(false);
    }
  });
});

describe('Fixture', () => {
  it('releases a run that opens its fifo only after disposal began', async () => {
    const fixture = await Fixture.create('cowshed-launch-fixture-');
    try {
      const started = fixture.fifo('started', 0);
      const gate = fixture.fifo('gate', 1);
      const later = fixture.fifo('later', 1);
      // The child opens `later` only after disposal hands `gate` its line, so that open happens
      // after the peers exist, whatever the scheduling.
      const run = fixture.own(
        await spawnLock({
          path: join(fixture.root, 'lock'),
          argv: ['sh', '-c', 'echo started > "$1"; read x < "$2"; read y < "$3"', 'sh', started, gate, later],
        }),
        nothing,
      );
      expect(await heard(started, run)).toBe('started\n');
      await fixture.dispose();
      expect(await run.exit).toEqual(exited(0));
    } finally {
      await fixture.dispose();
    }
  });
});

describe('spawnLock', () => {
  it('queues behind a holder and acquires once its command ends', async () => {
    const fixture = await Fixture.create('cowshed-launch-lock-');
    try {
      const lock = join(fixture.root, 'lock');
      const held = fixture.fifo('held', 0);
      const release = fixture.fifo('release', 1);
      const first = fixture.own(
        await spawnLock({ path: lock, argv: ['sh', '-c', 'echo held > "$1"; read x < "$2"', 'sh', held, release] }),
        stopLock,
      );
      const acquired = await next(first.state);
      expect(await heard(held, first)).toBe('held\n');

      const second = fixture.own(await spawnLock({ path: lock, argv: ['true'] }), stopLock);
      const queued = await next(second.state);
      if (acquired.kind !== 'acquired' || queued.kind !== 'queued') {
        throw new Error(`the lock said ${acquired.kind}, then ${queued.kind}`);
      }
      expect(queued.inode).toEqual(acquired.inode);

      await writeFile(release, 'go\n');
      expect(await first.exit).toEqual(exited(0));
      expect((await next(second.state)).kind).toBe('acquired');
      expect(await second.exit).toEqual(exited(0));
    } finally {
      await fixture.dispose();
    }
  });

  it('cancels its owned owner only through the queued request', async () => {
    const fixture = await Fixture.create('cowshed-launch-cancel-');
    const host = await openHost();
    try {
      const lock = join(fixture.root, 'lock');
      const held = fixture.fifo('held', 0);
      const release = fixture.fifo('release', 1);
      const owner = fixture.own(
        await spawnLock({ path: lock, argv: ['sh', '-c', 'echo held > "$1"; read x < "$2"', 'sh', held, release] }),
        stopLock,
      );
      expect((await next(owner.state)).kind).toBe('acquired');
      expect(await heard(held, owner)).toBe('held\n');
      const recorded = (await host.lockHolders(lock)).owner?.pid;
      if (recorded === undefined) {
        throw new Error('lock-holders names no owner of a held lock');
      }

      const waiter = fixture.own(await spawnLock({ path: lock, ownedPids: [recorded], argv: ['true'] }), stopLock);
      expect((await next(waiter.state)).kind).toBe('queued');
      waiter.cancelOwner();
      expect(await next(waiter.state)).toEqual({ kind: 'signalled', pid: recorded });
      expect(await waiter.exit).toEqual(exited(103));
      // The owner forwarded the SIGTERM to its command, which ended with it.
      expect(await owner.exit).toEqual(exited(143));
      expect(() => waiter.cancelOwner()).toThrow(CowshedError);
    } finally {
      await fixture.dispose();
      await host.close();
    }
  });
});

describe('spawnHold', () => {
  it('reports a command that exited, and ends with its status once released', async () => {
    const fixture = await Fixture.create('cowshed-launch-true-');
    try {
      const run = fixture.own(await spawnHold({ argv: ['true'] }), stopHold);
      expect((await next(run.state)).kind).toBe('started');
      const ended: HoldEvent = { kind: 'exited', status: { kind: 'exited', code: 0 } };
      expect(await next(run.state)).toEqual(ended);
      run.control.release();
      run.control.close();
      expect(await run.exit).toEqual(exited(0));
    } finally {
      await fixture.dispose();
    }
  });

  it('signals the held group on request, and ends with the signal once released', async () => {
    const fixture = await Fixture.create('cowshed-launch-hold-');
    try {
      const wait = fixture.fifo('wait', 1);
      const run = fixture.own(await spawnHold({ argv: ['sh', '-c', 'read x < "$1"', 'sh', wait] }), stopHold);
      expect((await next(run.state)).kind).toBe('started');
      run.control.signal('TERM');
      expect(await next(run.state)).toEqual({ kind: 'signalled', signal: 'TERM' });
      expect((await next(run.state)).kind).toBe('exited');
      run.control.release();
      run.control.close();
      expect(await run.exit).toEqual(exited(143));
    } finally {
      await fixture.dispose();
    }
  });

  it('tears the held group down when the control descriptor ends before a release', async () => {
    const fixture = await Fixture.create('cowshed-launch-eof-');
    try {
      const wait = fixture.fifo('wait', 1);
      const run = fixture.own(await spawnHold({ argv: ['sh', '-c', 'read x < "$1"', 'sh', wait] }), stopHold);
      const started = await next(run.state);
      if (started.kind !== 'started') {
        throw new Error(`hold said ${started.kind} first`);
      }
      run.control.close();
      // The teardown answers each signal it sends, SIGTERM first, then the leader's exit.
      const answered: HoldEvent[] = [];
      for (let event = await next(run.state); event.kind !== 'exited'; event = await next(run.state)) {
        answered.push(event);
      }
      expect(answered[0]).toEqual({ kind: 'signalled', signal: 'TERM' });
      expect(answered.every((event) => event.kind === 'signalled' || event.kind === 'notSignalled')).toBe(true);
      expect(await run.exit).toEqual(exited(143));
      expect(() => process.kill(started.pid, 0)).toThrow();
      expect(() => run.control.release()).toThrow(CowshedError);
    } finally {
      await fixture.dispose();
    }
  });
});

describe('spawnSubtreeRoot', () => {
  it('a refused admission is the verb record, and the command still runs', async () => {
    const fixture = await Fixture.create('cowshed-launch-subtree-');
    const socket = process.env.COWSHED_SUPERVISOR_SOCKET;
    delete process.env.COWSHED_SUPERVISOR_SOCKET;
    try {
      const out = join(fixture.root, 'ran');
      const run = fixture.own(await spawnSubtreeRoot({ argv: ['sh', '-c', 'echo ran > "$1"', 'sh', out] }), nothing);
      expect(await run.admission).toEqual({
        kind: 'record',
        admission: { kind: 'refused', reason: 'foldUnavailable' },
      });
      expect(await run.exit).toEqual(exited(0));
      expect(await readFile(out, 'utf8')).toBe('ran\n');
    } finally {
      if (socket !== undefined) {
        process.env.COWSHED_SUPERVISOR_SOCKET = socket;
      }
      await fixture.dispose();
    }
  });
});

describe('spawnLandTurn', () => {
  it('fails with a wrapper exit code when its gateway does not answer', async () => {
    const fixture = await Fixture.create('cowshed-launch-land-');
    try {
      // Nothing listens at absent.sock, so the verb cannot take the turn: it ends with its
      // environment-missing wrapper code (104) and never starts the command.
      const run = fixture.own(
        await spawnLandTurn({
          repo: 'launcher-witness',
          gatewaySocket: join(fixture.root, 'absent.sock'),
          argv: ['true'],
        }),
        nothing,
      );
      expect(await run.exit).toEqual(exited(104));
    } finally {
      await fixture.dispose();
    }
  });
});
