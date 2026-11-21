import { describe, expect, it } from 'bun:test';
import { createConnection, type Socket } from 'node:net';
import { CaseGate, type CaseGateEffects, GO, gateCases, runWallMs, serveCaseGate } from './case-gate.js';

/** A gate over `own` cores whose effects are recorded in order, on a clock the test moves. */
function gateOf(own: number, walls: Record<string, number> = {}) {
  const said: string[] = [];
  const clock = { now: 0 };
  const effects: CaseGateEffects = {
    start: (id) => said.push(`start ${id}`),
    reserve: (id) => said.push(`reserve ${id}`),
    release: (id) => said.push(`release ${id}`),
  };
  const gate = new CaseGate(
    own,
    (id) => walls[id] ?? 0,
    effects,
    () => clock.now,
  );
  return { gate, said, clock };
}

describe('a reserved nextest leaf whose cases each start on a core', () => {
  //#region smoo/cowshed!n/a-case-starts-on-a-core
  it('starts a case on a free core of its grant and asks the pool nothing', () => {
    const { gate, said } = gateOf(2);
    expect(gate.arrived('a')).toBe(true);
    expect(gate.arrived('b')).toBe(true);
    expect(said).toEqual(['start a', 'start b']);
    expect(gate.summary()).toEqual({ own: 2, lent: 0, queued: 0, runningAtEnd: 0, lentCoreMs: 0, waitedCaseMs: 0 });
  });

  it('holds one pool request for a case beyond its grant, starts it on the core the pool grants and gives that core back at its end', () => {
    const { gate, said, clock } = gateOf(1);
    gate.arrived('long');
    gate.arrived('short');
    expect(said).toEqual(['start long', 'reserve short']);
    clock.now = 20_000;
    gate.granted('short');
    clock.now = 21_000;
    gate.ended('short');
    expect(said.slice(2)).toEqual(['start short', 'release short']);
    expect(gate.summary()).toEqual({
      own: 1,
      lent: 1,
      queued: 0,
      runningAtEnd: 0,
      lentCoreMs: 1_000,
      waitedCaseMs: 20_000,
    });
  });

  it("gives a freed core of its grant to the waiting case with the longest recorded wall and withdraws that case's request", () => {
    const { gate, said } = gateOf(1, { quick: 2_000, slow: 60_000, slower: 90_000 });
    gate.arrived('first');
    gate.arrived('quick');
    gate.arrived('slower');
    gate.arrived('slow');
    gate.ended('first');
    expect(said.slice(-2)).toEqual(['release slower', 'start slower']);
    // The grant's one core is taken again: the next case to end on the pool's core frees only that.
    gate.granted('quick');
    gate.ended('quick');
    expect(said.slice(-2)).toEqual(['start quick', 'release quick']);
  });

  it('gives a core the pool grants straight back when its case no longer waits', () => {
    const { gate, said } = gateOf(1);
    gate.arrived('a');
    gate.arrived('b');
    gate.ended('a');
    expect(said).toEqual(['start a', 'reserve b', 'release b', 'start b']);
    gate.granted('b');
    expect(said.at(-1)).toBe('release b');
    expect(gate.summary().lent).toBe(0);
  });

  it('withdraws the request of a case that ends while it waits', () => {
    const { gate, said } = gateOf(1);
    gate.arrived('a');
    gate.arrived('b');
    gate.ended('b');
    expect(said).toEqual(['start a', 'reserve b', 'release b']);
    // Its core of the grant is still a's: a later case waits again.
    gate.arrived('c');
    expect(said.at(-1)).toBe('reserve c');
  });

  it('refuses a case that is already at the gate', () => {
    const { gate } = gateOf(1);
    expect(gate.arrived('a')).toBe(true);
    expect(gate.arrived('a')).toBe(false);
  });

  it('records a case that waited 20 s and ran 1 s as a run of 1 s', () => {
    const { gate, clock } = gateOf(1);
    gate.arrived('held');
    gate.arrived('waited');
    clock.now = 20_000;
    gate.granted('waited');
    // nextest timed the case from its shim's start: 21 s.
    expect(gate.outcome('waited')).toEqual({ kind: 'ran', waitedMs: 20_000 });
    expect(gate.outcome('held')).toEqual({ kind: 'ran', waitedMs: 0 });
    expect(runWallMs(21_000, 20_000)).toBe(1_000);
  });

  it('takes its state when the run begins to end: the case on the grant ran, the waiting one never starts on the core the teardown frees, and each wait counts once', () => {
    // A land gate's leaf of 2026-11-21: one core, one case on it for 178.7 s, fifteen waiting; the SIGTERM that ended
    // the case on the core handed that core to a waiting case, which was recorded as a run of 0.1 s after a 181.6 s wait.
    const { gate, said, clock } = gateOf(1);
    gate.arrived('long');
    clock.now = 1_000;
    gate.arrived('queued');
    clock.now = 180_000;
    gate.ending();
    clock.now = 181_600;
    gate.ended('long');
    gate.granted('queued');
    gate.ended('queued');
    gate.arrived('late');
    gate.ended('late');
    expect(said).toEqual(['start long', 'reserve queued', 'release queued', 'release queued', 'release late']);
    expect(gate.outcome('long')).toEqual({ kind: 'ran', waitedMs: 0 });
    expect(gate.outcome('queued')).toEqual({ kind: 'queued', waitedMs: 179_000 });
    expect(gate.outcome('late')).toEqual({ kind: 'queued', waitedMs: 0 });
    expect(gate.summary()).toEqual({
      own: 1,
      lent: 0,
      queued: 2,
      runningAtEnd: 1,
      lentCoreMs: 0,
      waitedCaseMs: 179_000,
    });
    // A second end changes nothing.
    gate.ending();
    expect(gate.summary().runningAtEnd).toBe(1);
  });

  it('counts a case that ended while it waited, before the run ended, as never started', () => {
    const { gate, clock } = gateOf(1);
    gate.arrived('a');
    gate.arrived('b');
    clock.now = 5_000;
    gate.ended('b');
    gate.ending();
    expect(gate.outcome('b')).toEqual({ kind: 'queued', waitedMs: 5_000 });
    expect(gate.summary()).toMatchObject({ own: 1, queued: 1, runningAtEnd: 1, waitedCaseMs: 5_000 });
  });

  /** A shim's side of the gate: connect, name the case, and read what the gate answers until it ends. */
  function shim(socket: string, line: string): { client: Socket; answered: Promise<string> } {
    const client = createConnection(socket);
    client.setEncoding('utf8');
    let text = '';
    const answered = new Promise<string>((resolve) => {
      client.on('data', (chunk: string) => {
        text += chunk;
        if (text.includes('\n')) resolve(text);
      });
      client.on('close', () => resolve(text));
      client.on('error', () => resolve(text));
    });
    client.on('connect', () => client.write(line));
    return { client, answered };
  }

  it("answers a case over its socket when it starts, and reads the end of its connection as the case's end", async () => {
    const seen: string[] = [];
    let ended!: () => void;
    const closed = new Promise<void>((resolve) => (ended = resolve));
    const served = await serveCaseGate({
      arrived: (id) => {
        seen.push(`arrived ${id}`);
        served.ok && served.gate.start(id);
        return true;
      },
      ended: (id) => {
        seen.push(`ended ${id}`);
        ended();
      },
      invalid: (detail) => seen.push(`invalid ${detail}`),
    });
    if (!served.ok) throw new Error(served.error);
    try {
      const { client, answered } = shim(served.gate.socket, '{"binary":"pkg::bin","test":"mod::case"}\n');
      expect(await answered).toBe(`${GO}\n`);
      client.destroy();
      await closed;
      expect(seen).toEqual(['arrived pkg::bin::mod::case', 'ended pkg::bin::mod::case']);
    } finally {
      expect(await served.gate.close()).toBeNull();
    }
  });

  it('ends a connection whose line names no case, or whose case the leaf refuses, unanswered', async () => {
    const invalid: string[] = [];
    const served = await serveCaseGate({
      arrived: () => false,
      ended: () => {},
      invalid: (detail) => invalid.push(detail),
    });
    if (!served.ok) throw new Error(served.error);
    try {
      expect(await shim(served.gate.socket, 'moved\n').answered).toBe('');
      expect(await shim(served.gate.socket, '{"binary":"b","test":"t"}\n').answered).toBe('');
      expect(invalid).toHaveLength(2);
      expect(invalid[1]).toContain('b::t');
    } finally {
      expect(await served.gate.close()).toBeNull();
    }
  });

  it("names each waiting case's request the work its leaf has left: its recorded walls less the cases that ran", () => {
    const asked: [string, number][] = [];
    const walls = new Map([
      ['a', 100],
      ['b', 50],
      ['c', 30],
    ]);
    const { cases } = gateCases(
      1,
      walls,
      () => {},
      (id, left) => {
        asked.push([id, left]);
        return { granted: new Promise<boolean>(() => {}), release: () => {} };
      },
    );
    cases.arrived('a');
    cases.arrived('b');
    // `a` ran and ended: `b` takes the grant's core, and `c` asks with `a`'s wall gone from what is left.
    cases.ended('a');
    cases.arrived('c');
    expect(asked).toEqual([
      ['b', 180],
      ['c', 80],
    ]);
  });
  //#endregion smoo/cowshed!n/a-case-starts-on-a-core
});
