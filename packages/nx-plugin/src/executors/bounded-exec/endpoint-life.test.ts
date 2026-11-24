import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import {
  type EndpointEffect,
  type EndpointEvent,
  type EndpointLife,
  endpointCapabilities,
  endpointStep,
  spawned,
} from './endpoint-life.js';
import { REAP_MS } from './lease-wait.js';

/**
 * The life of a run's reservation endpoint (endpoint-life.ts) judged over generated interleavings of what its shell
 * can observe: the real `endpointStep` decides, and each property states what must hold after every event of every
 * order. No process, socket, timer or clock runs here; the machine's one timer is the `expired` event.
 */

const PID = 4242;
const RUNS = { seed: 20261119, numRuns: 2000 };

const stdout = (text: string): EndpointEvent => ({ type: 'stdout', text });
const stderr = (text: string): EndpointEvent => ({ type: 'stderr', text });
const probe = (text: string): EndpointEvent => ({ type: 'probe', text });
const close = (code: number | null, signal: string | null = null): EndpointEvent => ({ type: 'close', code, signal });
const unspawned = (message: string): EndpointEvent => ({ type: 'error', message });
const STDOUT_END: EndpointEvent = { type: 'stdout-end' };
const PROBE_END: EndpointEvent = { type: 'probe-end' };
const RUN_END: EndpointEvent = { type: 'run-end' };
const EXPIRED: EndpointEvent = { type: 'expired' };

const GATES = '{"type":"capabilities","test_gate":true}\n';
const GATES_WITH_AGING = '{"type":"capabilities","test_gate":true,"aging_ms":30000}\n';
const NO_GATE = '{"type":"capabilities","test_gate":false}\n';
const NO_GATE_WITH_AGING = '{"type":"capabilities","test_gate":false,"aging_ms":30000}\n';
/** What an mbx that does not know the request answers before it closes the connection. */
const UNKNOWN = '{"type":"error","error":"unknown request type"}\n';

/** Chunks as a pipe delivers them: whole lines, lines cut anywhere, several at once, and anything else. */
const chunks = (whole: readonly string[]): fc.Arbitrary<string> =>
  fc.oneof(
    fc.constantFrom(...whole),
    fc
      .tuple(fc.constantFrom(...whole), fc.nat(), fc.boolean())
      .map(([text, at, head]) => (head ? text.slice(0, at % (text.length + 1)) : text.slice(at % (text.length + 1)))),
    fc.string({ maxLength: 8 }),
  );

const eventArb: fc.Arbitrary<EndpointEvent> = fc.oneof(
  { weight: 5, arbitrary: chunks(['listening\n', 'listening\nagain\n', 'mbx: unknown command\n', '\n']).map(stdout) },
  { weight: 1, arbitrary: fc.constant(STDOUT_END) },
  { weight: 2, arbitrary: chunks(['mbx: reservations need scheduler.enabled\n', 'warning\n']).map(stderr) },
  { weight: 1, arbitrary: fc.string({ maxLength: 8 }).map(unspawned) },
  {
    weight: 2,
    arbitrary: fc.oneof(
      fc.integer({ min: 0, max: 2 }).map((code) => close(code)),
      fc.constantFrom('SIGKILL', 'SIGTERM').map((signal) => close(null, signal)),
    ),
  },
  { weight: 4, arbitrary: chunks([GATES, GATES_WITH_AGING, NO_GATE, NO_GATE_WITH_AGING, UNKNOWN, '\n']).map(probe) },
  { weight: 1, arbitrary: fc.constant(PROBE_END) },
  { weight: 2, arbitrary: fc.constant(RUN_END) },
  { weight: 2, arbitrary: fc.constant(EXPIRED) },
);

/**
 * Any order of events, after a start that got as far as one of these: a uniformly random order seldom says
 * `listening` whole before its child ends, so the later phases would go unvisited.
 */
const traceArb: fc.Arbitrary<EndpointEvent[]> = fc
  .tuple(
    fc.constantFrom<EndpointEvent[]>(
      [],
      [stdout('listening\n')],
      [stdout('listening\n'), probe(GATES)],
      [stdout('listening\n'), probe(GATES), RUN_END],
      [stdout('mbx: unknown command\n')],
    ),
    fc.array(eventArb, { maxLength: 16, size: 'max' }),
  )
  .map(([reached, rest]) => [...reached, ...rest]);

interface Step {
  readonly event: EndpointEvent;
  readonly before: EndpointLife;
  readonly after: EndpointLife;
  readonly effects: readonly EndpointEffect[];
}

/**
 * `trace` as a run delivers it. Its own end reaches the endpoint only once its start was answered served: the run
 * holds no endpoint before that, so a `run-end` earlier in the trace is one that could not be sent.
 */
function live(trace: readonly EndpointEvent[]): Step[] {
  const steps: Step[] = [];
  let life = spawned(PID);
  let served = false;
  for (const event of trace) {
    if (event.type === 'run-end' && !served) continue;
    const [after, effects] = endpointStep(life, event);
    steps.push({ event, before: life, after, effects });
    served ||= effects.some((effect) => effect.do === 'serve');
    life = after;
  }
  return steps;
}

const effectsOf = (steps: readonly Step[]): EndpointEffect[] => steps.flatMap((step) => step.effects);
const count = (steps: readonly Step[], what: EndpointEffect['do']): number =>
  effectsOf(steps).filter((effect) => effect.do === what).length;
const firstLine = (text: string): string | null => (text.includes('\n') ? text.slice(0, text.indexOf('\n')) : null);

/** Each property holds for every generated order of events. */
/** Whether the machine's timer is armed before and after each step: an `arm` starts it, a `disarm` or its expiry ends it. */
function timers(steps: readonly Step[]): { readonly before: boolean; readonly after: boolean }[] {
  let armed = false;
  return steps.map((step) => {
    const before = armed;
    if (step.event.type === 'expired') armed = false;
    for (const effect of step.effects) {
      if (effect.do === 'arm') {
        expect(armed).toBe(false);
        armed = true;
      }
      if (effect.do === 'disarm') armed = false;
    }
    return { before, after: armed };
  });
}
const everyOrder = (holds: (steps: readonly Step[]) => void): void =>
  fc.assert(
    fc.property(traceArb, (trace) => holds(live(trace))),
    RUNS,
  );

describe("the life of a run's reservation endpoint", () => {
  it('walks its ordinary paths', () => {
    const reap: EndpointEffect = { do: 'arm', ms: REAP_MS };
    const disarm: EndpointEffect = { do: 'disarm' };
    const cases: { name: string; events: EndpointEvent[]; effects: EndpointEffect[][] }[] = [
      {
        name: 'an endpoint that listens and gates is served, and ends with its run',
        events: [stdout('listen'), stdout('ing\nserving\n'), probe(GATES), RUN_END, close(0)],
        effects: [
          [],
          [{ do: 'probe' }],
          [{ do: 'hang-up' }, { do: 'serve', capabilities: { gates: true, agingMs: null } }],
          [{ do: 'close-stdin' }, reap],
          [disarm, { do: 'conclude', ended: { how: 'mbx reservations ended with status 0', clean: true } }],
        ],
      },
      {
        name: 'an endpoint whose probe ends unanswered is served and gates nothing',
        events: [stdout('listening\n'), PROBE_END, PROBE_END],
        effects: [[{ do: 'probe' }], [{ do: 'serve', capabilities: { gates: false, agingMs: null } }], []],
      },
      {
        name: 'a child that says anything else is killed, and refused once it has ended',
        events: [stdout('mbx 1.2.3\nlistening\n'), close(null, 'SIGKILL')],
        effects: [
          [{ do: 'kill' }, reap],
          [disarm, { do: 'refuse', why: 'mbx reservations said "mbx 1.2.3", not that it was listening' }],
        ],
      },
      {
        name: 'a child that ends without a line is refused by how it ended and the last thing it said',
        events: [stderr('mbx: warning\nmbx: reservations need '), stderr('scheduler.enabled\n'), close(1)],
        effects: [
          [],
          [],
          [{ do: 'refuse', why: 'mbx reservations ended with status 1: mbx: reservations need scheduler.enabled' }],
        ],
      },
      {
        name: 'a child whose output ends without a line is refused by how it ended, once it has',
        events: [stderr('mbx: reservations need scheduler.enabled\n'), STDOUT_END, close(1)],
        effects: [
          [],
          [reap],
          [
            disarm,
            { do: 'refuse', why: 'mbx reservations ended with status 1: mbx: reservations need scheduler.enabled' },
          ],
        ],
      },
      {
        name: 'a child that closes its output without a line and runs on is killed, and refused for what it did',
        events: [stdout('listenin'), STDOUT_END, stdout('g\n'), EXPIRED, close(null, 'SIGKILL')],
        effects: [
          [],
          [reap],
          [],
          [{ do: 'kill' }, reap],
          [
            disarm,
            {
              do: 'refuse',
              why: `mbx reservations closed its standard output after "listenin", without saying it was listening, and had not ended ${REAP_MS}ms later`,
            },
          ],
        ],
      },
      {
        name: 'a child that could not be spawned is refused by its error, and its close says nothing more',
        events: [unspawned('spawn mbx ENOENT'), close(-2)],
        effects: [[{ do: 'refuse', why: 'mbx reservations could not run: spawn mbx ENOENT' }], []],
      },
      {
        name: 'an endpoint whose child ends before its probe answers is refused by how it ended, and never served',
        events: [stdout('listening\n'), stderr('mbx: pool store is locked\n'), close(1), probe(GATES), PROBE_END],
        effects: [
          [{ do: 'probe' }],
          [],
          [{ do: 'hang-up' }, { do: 'refuse', why: 'mbx reservations ended with status 1: mbx: pool store is locked' }],
          [],
          [],
        ],
      },
      {
        name: "an endpoint that ends while it serves is said at its run's end, and nothing is signalled",
        events: [stdout('listening\n'), probe(NO_GATE), close(null, 'SIGTERM'), RUN_END],
        effects: [
          [{ do: 'probe' }],
          [{ do: 'hang-up' }, { do: 'serve', capabilities: { gates: false, agingMs: null } }],
          [],
          [{ do: 'conclude', ended: { how: 'mbx reservations ended on SIGTERM', clean: false } }],
        ],
      },
      {
        name: 'an endpoint that outlives its standard input is killed, and its end says so',
        events: [stdout('listening\n'), probe(GATES), RUN_END, EXPIRED, close(null, 'SIGKILL')],
        effects: [
          [{ do: 'probe' }],
          [{ do: 'hang-up' }, { do: 'serve', capabilities: { gates: true, agingMs: null } }],
          [{ do: 'close-stdin' }, reap],
          [{ do: 'kill' }, reap],
          [
            disarm,
            {
              do: 'conclude',
              ended: {
                how: `mbx reservations ended on SIGKILL, killed after it outlived its standard input by ${REAP_MS}ms`,
                clean: false,
              },
            },
          ],
        ],
      },
      {
        name: 'an endpoint that a kill does not end either is left as it is, and its end says so',
        events: [stdout('listening\n'), probe(GATES), RUN_END, EXPIRED, EXPIRED, close(null, 'SIGKILL')],
        effects: [
          [{ do: 'probe' }],
          [{ do: 'hang-up' }, { do: 'serve', capabilities: { gates: true, agingMs: null } }],
          [{ do: 'close-stdin' }, reap],
          [{ do: 'kill' }, reap],
          [
            { do: 'unref-child' },
            {
              do: 'conclude',
              ended: {
                how: `mbx reservations outlived its standard input by ${REAP_MS}ms; pid ${PID} had not ended ${REAP_MS}ms after it was killed and is left as it is`,
                clean: false,
              },
            },
          ],
          [],
        ],
      },
      {
        name: 'a child refused for its first line that a kill does not end is refused all the same, and named',
        events: [stdout('mbx 1.2.3\n'), EXPIRED],
        effects: [
          [{ do: 'kill' }, reap],
          [
            { do: 'unref-child' },
            {
              do: 'refuse',
              why: `mbx reservations said "mbx 1.2.3", not that it was listening; pid ${PID} had not ended ${REAP_MS}ms after it was killed and is left as it is`,
            },
          ],
        ],
      },
    ];
    for (const { name, events, effects } of cases) {
      expect({ name, effects: live(events).map((step) => step.effects) }).toEqual({ name, effects });
    }
  });

  //#region smoo/cowshed!n/a-case-starts-on-a-core
  it('takes only a `capabilities` answer with `test_gate` as an mbx that gates test cases', () => {
    expect(endpointCapabilities(GATES.trimEnd())).toEqual({ gates: true, agingMs: null });
    expect(endpointCapabilities(NO_GATE.trimEnd())).toEqual({ gates: false, agingMs: null });
    expect(endpointCapabilities(UNKNOWN.trimEnd())).toEqual({ gates: false, agingMs: null });
    expect(endpointCapabilities('{"type":"capabilities"}')).toEqual({ gates: false, agingMs: null });
    expect(endpointCapabilities('listening')).toEqual({ gates: false, agingMs: null });
    expect(endpointCapabilities('{"type":"capabilities","test_gate":true')).toEqual({ gates: false, agingMs: null });
  });

  it("serves with the capabilities its probe's first line answers, and with none when the probe ends first", () => {
    everyOrder((steps) => {
      let reply = '';
      for (const step of steps) {
        if (step.before.phase !== 'probing') continue;
        if (step.event.type === 'probe') reply += step.event.text;
        const serve = step.effects.find((effect) => effect.do === 'serve');
        const line = firstLine(reply);
        if (step.event.type === 'probe' && line !== null)
          expect(serve).toEqual({ do: 'serve', capabilities: endpointCapabilities(line) });
        else if (serve !== undefined)
          expect(serve).toEqual({ do: 'serve', capabilities: { gates: false, agingMs: null } });
      }
    });
  });
  //#endregion smoo/cowshed!n/a-case-starts-on-a-core

  //#region smoo/cowshed!n/a-minimum-for-a-whole-suite
  it('serves the pool aging bound from a capabilities answer and keeps gates as today', () => {
    const withAging = live([
      stdout('listening\n'),
      probe('{"type":"capabilities","test_gate":true,"aging_ms":30000}\n'),
    ]);
    expect(effectsOf(withAging).find((effect) => effect.do === 'serve')).toEqual({
      do: 'serve',
      capabilities: { gates: true, agingMs: 30_000 },
    });

    const zeroAgingNoGate = live([
      stdout('listening\n'),
      probe('{"type":"capabilities","test_gate":false,"aging_ms":0}\n'),
    ]);
    expect(effectsOf(zeroAgingNoGate).find((effect) => effect.do === 'serve')).toEqual({
      do: 'serve',
      capabilities: { gates: false, agingMs: 0 },
    });
  });

  it('serves a null aging bound when capabilities omit aging_ms or state an invalid one', () => {
    const omitted = live([stdout('listening\n'), probe(GATES)]);
    expect(effectsOf(omitted).find((effect) => effect.do === 'serve')).toEqual({
      do: 'serve',
      capabilities: { gates: true, agingMs: null },
    });

    for (const invalid of [
      '{"type":"capabilities","test_gate":true,"aging_ms":-1}\n',
      '{"type":"capabilities","test_gate":true,"aging_ms":"30000"}\n',
      '{"type":"capabilities","test_gate":true,"aging_ms":1.5}\n',
    ]) {
      const steps = live([stdout('listening\n'), probe(invalid)]);
      expect(effectsOf(steps).find((effect) => effect.do === 'serve')).toEqual({
        do: 'serve',
        capabilities: { gates: false, agingMs: null },
      });
    }
  });
  //#endregion smoo/cowshed!n/a-minimum-for-a-whole-suite

  it("answers its start once, and its run's end once and only after it served", () => {
    everyOrder((steps) => {
      expect(count(steps, 'serve') + count(steps, 'refuse')).toBeLessThanOrEqual(1);
      expect(count(steps, 'conclude')).toBeLessThanOrEqual(count(steps, 'serve'));
      const ended = steps.findIndex((step) => step.after.phase === 'ended');
      if (ended !== -1) expect(effectsOf(steps.slice(ended + 1))).toEqual([]);
    });
  });

  it('waits on nothing of a child that has ended, and serves no endpoint whose child ended first', () => {
    everyOrder((steps) => {
      const ended = steps.findIndex((step) => step.event.type === 'close' || step.event.type === 'error');
      if (ended === -1) return;
      for (const step of steps.slice(ended)) {
        expect(['gone', 'ended']).toContain(step.after.phase);
        expect(step.effects.map((effect) => effect.do)).not.toContain('serve');
      }
    });
  });
  it('serves only a child whose first line is `listening`, however its output is cut, and names any other first line', () => {
    everyOrder((steps) => {
      let out = '';
      for (const step of steps) {
        if (step.before.phase !== 'starting' || step.event.type !== 'stdout') continue;
        out += step.event.text;
        const line = firstLine(out);
        if (line === null) expect(step.after.phase).toBe('starting');
        else if (line === 'listening') expect(step.effects.map((effect) => effect.do)).toContain('probe');
        else {
          expect(step.effects.map((effect) => effect.do)).toContain('kill');
          for (const effect of effectsOf(steps)) {
            if (effect.do === 'refuse') {
              expect(effect.why).toStartWith(
                `mbx reservations said ${JSON.stringify(line)}, not that it was listening`,
              );
            }
          }
        }
      }
      if (count(steps, 'serve') === 1) expect(firstLine(out)).toBe('listening');
    });
  });

  it('refuses a child that ends before its first line by how it ended', () => {
    everyOrder((steps) => {
      for (const step of steps) {
        if (step.before.phase !== 'starting') continue;
        if (step.event.type === 'close') {
          const ended = step.event.signal === null ? `with status ${step.event.code}` : `on ${step.event.signal}`;
          expect(step.effects).toHaveLength(1);
          expect(step.effects[0]).toMatchObject({ do: 'refuse', why: expect.stringContaining(`ended ${ended}`) });
        }
        if (step.event.type === 'error') {
          expect(step.effects).toEqual([
            { do: 'refuse', why: `mbx reservations could not run: ${step.event.message}` },
          ]);
        }
      }
    });
  });

  it('waits for a first line only while its child can still say one', () => {
    everyOrder((steps) => {
      const silent = steps.findIndex((step) => ['stdout-end', 'close', 'error'].includes(step.event.type));
      if (silent === -1) return;
      for (const step of steps.slice(silent)) expect(step.after.phase).not.toBe('starting');
    });
  });
  it('kills only a child that said something else, or one whose timer ran out', () => {
    everyOrder((steps) => {
      for (const step of steps) {
        if (!step.effects.some((effect) => effect.do === 'kill')) continue;
        expect(['stdout', 'expired']).toContain(step.event.type);
        expect(step.after.phase).toBe('ending');
      }
    });
  });

  it('arms one timer at a time, and leaves none armed once it has ended', () => {
    everyOrder((steps) => {
      const armed = timers(steps);
      steps.forEach((step, at) => {
        if (step.after.phase === 'ended') expect(armed[at]?.after).toBe(false);
      });
    });
  });

  it("waits for its child's end only under an armed timer, and for no more than two of them", () => {
    everyOrder((steps) => {
      const armed = timers(steps);
      steps.forEach((step, at) => {
        if (step.after.phase === 'ending') expect(armed[at]?.after).toBe(true);
      });
      const last = steps.at(-1)?.after;
      if (last?.phase !== 'ending') return;
      const [once] = endpointStep(last, EXPIRED);
      expect(endpointStep(once, EXPIRED)[0].phase).toBe('ended');
    });
  });

  it('arms its timer only to reap its child: its start and its probe wait for events alone', () => {
    everyOrder((steps) => {
      const armed = timers(steps);
      steps.forEach((step, at) => {
        for (const effect of step.effects) {
          if (effect.do === 'arm') expect(effect.ms).toBe(REAP_MS);
        }
        if (step.after.phase !== 'ending') expect(armed[at]?.after).toBe(false);
      });
    });
  });
  it('does nothing at an expiry no armed timer could send, so no outcome is read from a clock', () => {
    everyOrder((steps) => {
      const armed = timers(steps);
      steps.forEach((step, at) => {
        if (step.event.type !== 'expired' || armed[at]?.before === true) return;
        expect({ after: step.after, effects: step.effects }).toEqual({ after: step.before, effects: [] });
      });
    });
  });

  //#region smoo/cowshed!n/an-nx-run-serves-its-leaves-one-reservation-endpoint
  it('lets the run exit when the killed child or its pipes outlive the final reap bound', () => {
    everyOrder((steps) => {
      for (const { event, before, effects } of steps) {
        const outputs = effects.map((effect) => effect.do);
        if (event.type === 'expired' && before.phase === 'ending' && before.killed) {
          expect(outputs).toEqual(['unref-child', before.refusing === null ? 'conclude' : 'refuse']);
        } else {
          expect(outputs).not.toContain('unref-child');
        }
      }
    });
  });
  //#endregion smoo/cowshed!n/an-nx-run-serves-its-leaves-one-reservation-endpoint
});
