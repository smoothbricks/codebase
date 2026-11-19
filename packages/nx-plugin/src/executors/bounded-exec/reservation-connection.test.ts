import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import {
  type Connection,
  type ConnectionEffect,
  type ConnectionEvent,
  connectionStep,
  opened,
} from './reservation-connection.js';

/**
 * A leaf's connection to its run's reservation endpoint (reservation-connection.ts) judged over generated orders of
 * what its process asks and what its socket says: the real `connectionStep` decides, and each property states what
 * must hold after every event of every order. No socket is opened here.
 */

const RUNS = { seed: 20261119, numRuns: 2000 };
const ENDPOINT = '/run/mbx-r-1/r.sock';

const grantedLine = (id: number, cpus = 2): string => `${JSON.stringify({ type: 'granted', id, cpus })}\n`;
const refusedLine = (id: number): string => `${JSON.stringify({ type: 'refused', id, error: 'no memory' })}\n`;
const ERROR_LINE = '{"type":"error","error":"not a reservation request"}\n';

/** What a process and its socket do, before the process's ids are known. */
type Act =
  | { readonly act: 'reserve'; readonly cpus: number; readonly min: number }
  /** Release the request made `nth`, counted round the requests made so far. */
  | { readonly act: 'release'; readonly nth: number }
  | { readonly act: 'data'; readonly text: string }
  | { readonly act: 'error' }
  | { readonly act: 'close' };

const IDS = [1, 2, 3, 4];
const LINES = [
  ...IDS.map((id) => grantedLine(id)),
  ...IDS.map((id) => refusedLine(id)),
  ERROR_LINE,
  // Lines that are no reply: no JSON, JSON of another shape, a blank line.
  'listening\n',
  '{"type":"granted","id":1}\n',
  '\n',
];

/** Chunks as a socket delivers them: whole lines, lines cut anywhere, and anything else. */
const chunkArb: fc.Arbitrary<string> = fc.oneof(
  { weight: 6, arbitrary: fc.constantFrom(...LINES) },
  {
    weight: 2,
    arbitrary: fc
      .tuple(fc.constantFrom(...LINES), fc.nat(), fc.boolean())
      .map(([text, at, head]) => (head ? text.slice(0, at % (text.length + 1)) : text.slice(at % (text.length + 1)))),
  },
  { weight: 1, arbitrary: fc.string({ maxLength: 6 }) },
);

const actArb: fc.Arbitrary<Act> = fc.oneof(
  {
    weight: 5,
    arbitrary: fc
      .record({ cpus: fc.integer({ min: 1, max: 6 }), min: fc.integer({ min: 1, max: 8 }) })
      .map(({ cpus, min }): Act => ({ act: 'reserve', cpus, min })),
  },
  { weight: 4, arbitrary: fc.nat({ max: 7 }).map((nth): Act => ({ act: 'release', nth })) },
  { weight: 8, arbitrary: chunkArb.map((text): Act => ({ act: 'data', text })) },
  { weight: 1, arbitrary: fc.constant<Act>({ act: 'error' }) },
  { weight: 1, arbitrary: fc.constant<Act>({ act: 'close' }) },
);

interface Step {
  readonly event: ConnectionEvent;
  readonly before: Connection;
  readonly after: Connection;
  readonly effects: readonly ConnectionEffect[];
}

/**
 * `acts` as a process delivers them: each request under the next id of its own, and a release only of a request it
 * made, since the handle that releases is the request's.
 */
function live(acts: readonly Act[]): Step[] {
  const steps: Step[] = [];
  let connection = opened(ENDPOINT);
  let made = 0;
  for (const act of acts) {
    let event: ConnectionEvent;
    if (act.act === 'reserve') {
      made += 1;
      event = { type: 'reserve', id: made, cpus: act.cpus, priority: 'low', min: act.min };
    } else if (act.act === 'release') {
      if (made === 0) continue;
      event = { type: 'release', id: (act.nth % made) + 1 };
    } else if (act.act === 'data') event = { type: 'data', text: act.text };
    else if (act.act === 'error') event = { type: 'error', message: 'connect ENOENT', code: 'ENOENT' };
    else event = { type: 'close' };
    const [after, effects] = connectionStep(connection, event);
    steps.push({ event, before: connection, after, effects });
    connection = after;
  }
  return steps;
}

/** Each property holds for every generated order. */
const everyOrder = (holds: (steps: readonly Step[]) => void): void =>
  fc.assert(
    fc.property(fc.array(actArb, { maxLength: 24, size: 'max' }), (acts) => holds(live(acts))),
    RUNS,
  );

/** The index of each step at which `id` was reserved, answered (and how), released and lost. */
function history(steps: readonly Step[], id: number) {
  const at = (holds: (step: Step) => boolean): number[] => steps.flatMap((step, index) => (holds(step) ? [index] : []));
  const does = (step: Step, what: ConnectionEffect['do']): ConnectionEffect[] =>
    step.effects.filter((effect) => effect.do === what && 'id' in effect && effect.id === id);
  return {
    reserved: at((step) => step.event.type === 'reserve' && step.event.id === id),
    released: at((step) => step.event.type === 'release' && step.event.id === id),
    answered: at((step) => does(step, 'answer').length > 0),
    granted: at((step) =>
      step.effects.some((effect) => effect.do === 'answer' && effect.id === id && effect.answer.ok),
    ),
    lost: at((step) => does(step, 'lose').length > 0),
    answers: steps.flatMap((step) => does(step, 'answer')).length,
    losses: steps.flatMap((step) => does(step, 'lose')).length,
  };
}

const requests = (steps: readonly Step[]): number[] =>
  steps.flatMap((step) => (step.event.type === 'reserve' ? [step.event.id] : []));
const endedAt = (steps: readonly Step[]): number => steps.findIndex((step) => step.after.closed !== null);

describe("a leaf's connection to its run's reservation endpoint", () => {
  it('walks its ordinary paths', () => {
    const refusal = (message: string, errno: string | null = null) => ({ endpoint: ENDPOINT, errno, message });
    const reserve = (id: number, cpus: number, min = 1, name?: string): ConnectionEvent => ({
      type: 'reserve',
      id,
      cpus,
      priority: 'low',
      min,
      name,
    });
    const cases: { name: string; events: ConnectionEvent[]; effects: ConnectionEffect[][] }[] = [
      {
        name: 'a request is written with its minimum cut to its ask, granted, and given back',
        events: [
          reserve(1, 6, 3),
          reserve(2, 2, 5, 'nextest:a::b'),
          reserve(3, 4),
          { type: 'data', text: grantedLine(1, 4) },
          { type: 'release', id: 1 },
          { type: 'release', id: 1 },
        ],
        effects: [
          [{ do: 'ref' }, { do: 'write', line: '{"type":"reserve","id":1,"cpus":6,"priority":"low","min":3}\n' }],
          [
            { do: 'ref' },
            {
              do: 'write',
              line: '{"type":"reserve","id":2,"cpus":2,"priority":"low","min":2,"name":"nextest:a::b"}\n',
            },
          ],
          [{ do: 'ref' }, { do: 'write', line: '{"type":"reserve","id":3,"cpus":4,"priority":"low"}\n' }],
          [{ do: 'answer', id: 1, answer: { ok: true, cpus: 4 } }],
          [{ do: 'write', line: '{"type":"release","id":1}\n' }],
          [],
        ],
      },
      {
        name: 'a refusal answers its request, and a request released while it waits is withdrawn',
        events: [reserve(1, 3), reserve(2, 1), { type: 'data', text: refusedLine(1) }, { type: 'release', id: 2 }],
        effects: [
          [{ do: 'ref' }, { do: 'write', line: '{"type":"reserve","id":1,"cpus":3,"priority":"low"}\n' }],
          [{ do: 'ref' }, { do: 'write', line: '{"type":"reserve","id":2,"cpus":1,"priority":"low"}\n' }],
          [{ do: 'answer', id: 1, answer: { ok: false, refusal: refusal('no memory') } }],
          [
            { do: 'answer', id: 2, answer: { ok: false, refusal: refusal('released while it waited') } },
            { do: 'write', line: '{"type":"release","id":2}\n' },
            { do: 'unref' },
          ],
        ],
      },
      {
        name: 'the endpoint going fails what waits, loses what is held, and answers what asks after',
        events: [
          reserve(1, 1),
          reserve(2, 2),
          { type: 'data', text: grantedLine(1, 1) },
          { type: 'error', message: 'read ECONNRESET', code: 'ECONNRESET' },
          { type: 'close' },
          reserve(3, 1),
          { type: 'release', id: 1 },
        ],
        effects: [
          [{ do: 'ref' }, { do: 'write', line: '{"type":"reserve","id":1,"cpus":1,"priority":"low"}\n' }],
          [{ do: 'ref' }, { do: 'write', line: '{"type":"reserve","id":2,"cpus":2,"priority":"low"}\n' }],
          [{ do: 'answer', id: 1, answer: { ok: true, cpus: 1 } }],
          [
            { do: 'lose', id: 1, why: `mbx reservation endpoint ${ENDPOINT}: read ECONNRESET` },
            {
              do: 'answer',
              id: 2,
              answer: {
                ok: false,
                refusal: refusal(`mbx reservation endpoint ${ENDPOINT}: read ECONNRESET`, 'ECONNRESET'),
              },
            },
            { do: 'hang-up' },
          ],
          [],
          [
            {
              do: 'answer',
              id: 3,
              answer: {
                ok: false,
                refusal: refusal(`mbx reservation endpoint ${ENDPOINT}: read ECONNRESET`, 'ECONNRESET'),
              },
            },
          ],
          [],
        ],
      },
    ];
    for (const { name, events, effects } of cases) {
      let connection = opened(ENDPOINT);
      const done = events.map((event) => {
        const [next, did] = connectionStep(connection, event);
        connection = next;
        return did;
      });
      expect({ name, effects: done }).toEqual({ name, effects });
    }
  });

  it('ends the connection, saying why, at an answer that is none', () => {
    const cases = [
      { said: 'listening\n', why: `mbx reservation endpoint ${ENDPOINT} answered "listening", which is no reply` },
      { said: '\n', why: `mbx reservation endpoint ${ENDPOINT} answered "", which is no reply` },
      {
        said: '{"type":"granted","id":1}\n',
        why: `mbx reservation endpoint ${ENDPOINT} answered "{\\"type\\":\\"granted\\",\\"id\\":1}", which is no reply`,
      },
      {
        said: ERROR_LINE,
        why: `mbx reservation endpoint ${ENDPOINT} closed this connection: not a reservation request`,
      },
      {
        said: grantedLine(1) + grantedLine(1),
        why: `mbx reservation endpoint ${ENDPOINT} answered reservation 1 twice`,
      },
    ];
    for (const { said, why } of cases) {
      const [asked] = connectionStep(opened(ENDPOINT), { type: 'reserve', id: 1, cpus: 2, priority: 'low', min: 1 });
      const [ended] = connectionStep(asked, { type: 'data', text: said });
      expect({ said, closed: ended.closed }).toEqual({
        said,
        closed: { endpoint: ENDPOINT, errno: null, message: why },
      });
    }
  });

  it('answers every request once: by the pool, by its own release, or by the end of its connection', () => {
    everyOrder((steps) => {
      const ended = endedAt(steps);
      const last = steps.at(-1)?.after;
      for (const id of requests(steps)) {
        const { answers } = history(steps, id);
        expect(answers).toBeLessThanOrEqual(1);
        // Unanswered, it still waits; and nothing waits once the connection has ended.
        expect({ id, waits: last?.entries.get(id) === 'waiting' }).toEqual({ id, waits: answers === 0 });
        if (ended !== -1) expect({ id, answers }).toEqual({ id, answers: 1 });
      }
    });
  });

  it('loses every grant still held when its connection ends, once, and none that was given back', () => {
    everyOrder((steps) => {
      const ended = endedAt(steps);
      for (const id of requests(steps)) {
        const { granted, released, lost, losses } = history(steps, id);
        const grantedAt = granted[0] ?? -1;
        const heldAtEnd = ended !== -1 && grantedAt !== -1 && grantedAt < ended && !released.some((at) => at < ended);
        expect({ id, losses, at: lost[0] ?? -1 }).toEqual({
          id,
          losses: heldAtEnd ? 1 : 0,
          at: heldAtEnd ? ended : -1,
        });
      }
    });
  });

  it('writes nothing once it has ended, and hangs up once, as it ends', () => {
    everyOrder((steps) => {
      const ended = endedAt(steps);
      const hangUps = steps.flatMap((step, at) => (step.effects.some((effect) => effect.do === 'hang-up') ? [at] : []));
      expect(hangUps).toEqual(ended === -1 ? [] : [ended]);
      if (ended === -1) return;
      for (const step of steps.slice(ended + 1)) {
        expect(step.effects.every((effect) => effect.do === 'answer')).toBe(true);
        expect(step.after).toBe(step.before);
      }
    });
  });

  it('tells the endpoint of every request, and of every one given back while it can hear', () => {
    everyOrder((steps) => {
      for (const step of steps) {
        const written = step.effects.flatMap((effect) => (effect.do === 'write' ? [effect.line] : []));
        const open = step.before.closed === null;
        if (open && step.event.type === 'reserve') {
          expect(written).toHaveLength(1);
          expect(written[0]).toStartWith(`{"type":"reserve","id":${step.event.id},"cpus":${step.event.cpus},`);
        } else if (open && step.event.type === 'release' && step.before.entries.has(step.event.id)) {
          expect(written).toEqual([`{"type":"release","id":${step.event.id}}\n`]);
        } else expect(written).toEqual([]);
      }
    });
  });

  it('keeps its process alive exactly while a request waits or holds', () => {
    everyOrder((steps) => {
      let referenced = false;
      for (const step of steps) {
        for (const effect of step.effects) {
          if (effect.do === 'ref') referenced = true;
          if (effect.do === 'unref') referenced = false;
        }
        // An ended connection's socket is destroyed, which keeps nothing alive.
        if (step.after.closed === null) expect(referenced).toBe(step.after.entries.size > 0);
      }
    });
  });

  it('reads the same answers however its socket cuts them', () => {
    fc.assert(
      fc.property(
        fc.array(fc.constantFrom(...LINES), { maxLength: 8 }),
        fc.array(fc.nat(), { maxLength: 6 }),
        (lines, cuts) => {
          const asked = IDS.reduce(
            (connection, id) =>
              connectionStep(connection, { type: 'reserve', id, cpus: 2, priority: 'low', min: 1 })[0],
            opened(ENDPOINT),
          );
          const said = lines.join('');
          const at = [...new Set(cuts.map((cut) => cut % (said.length + 1)))].sort((a, b) => a - b);
          const pieces = [0, ...at].map((from, index) => said.slice(from, at[index] ?? said.length));
          const read = (texts: readonly string[]) => {
            let connection = asked;
            const effects: ConnectionEffect[] = [];
            for (const text of texts) {
              const [next, did] = connectionStep(connection, { type: 'data', text });
              connection = next;
              effects.push(...did);
            }
            return { connection, effects };
          };
          expect(read(pieces)).toEqual(read([said]));
        },
      ),
      RUNS,
    );
  });
});
