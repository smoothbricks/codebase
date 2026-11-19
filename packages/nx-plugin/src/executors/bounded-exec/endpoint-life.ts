import typia from 'typia';
import { CONTROL_MS, REAP_MS } from './lease-wait.js';

/**
 * The life of an Nx run's mbx reservation endpoint in the run that started it (mbx-reservations.ts;
 * specs/cowshed/05_gateway.md, "Parallelism inside a job"), as a state machine over what the run can observe of its
 * child. Pure: no I/O, no clock and no timer. The shell maps the child's and the probe's events into `EndpointEvent`s
 * and executes the `EndpointEffect`s each step answers, in their order; it decides nothing.
 *
 * The run waits in four phases, each for events only:
 *
 * - `starting`, for the child's first line on its standard output, or its end. `listening` opens the probe; any other
 *   line is refused and the child killed. A child whose standard output ends without a line can say none: its start
 *   is refused, by how it ended when it ends within the reap bound, and it is killed when it does not.
 * - `probing`, for the endpoint's answer to `{"type":"capabilities"}` on the probe's connection, or that connection's
 *   end: whether the installed mbx's shim gates test cases (05_gateway.md, "A case starts on a core"). The start is
 *   then answered served, which publishes the socket to the run's tasks.
 * - `serving`, for the run's end. A child that ends first is `gone`, and the run's end says how.
 * - `ending`, for the child's end once a pipe of it closed (its standard input by the run, its standard output by
 *   itself) or it was killed, with the reap timer armed.
 *
 * The start is answered once (`serve` or `refuse`) and a served endpoint's end once (`conclude`); `ended` takes no
 * event.
 */
export type EndpointLife =
  | { readonly phase: 'starting'; readonly out: string; readonly said: string; readonly pid: number | undefined }
  | {
      readonly phase: 'probing';
      readonly reply: string;
      readonly said: string;
      readonly pid: number | undefined;
      readonly gone: Ended | null;
    }
  | { readonly phase: 'serving'; readonly said: string; readonly pid: number | undefined }
  | { readonly phase: 'gone'; readonly ended: Ended }
  | {
      readonly phase: 'ending';
      readonly pid: number | undefined;
      readonly said: string;
      /** Whether the child was sent SIGKILL; otherwise a pipe of it closed and its end is awaited. */
      readonly killed: boolean;
      /**
       * Why its start is refused; null for a served endpoint ending with its run. An unkilled child that ends is
       * refused by how it ended instead, which says more.
       */
      readonly refusing: string | null;
    }
  | { readonly phase: 'ended' };

/** How a served endpoint ended, and whether that was its ordinary end: status 0 with nothing said, unkilled. */
export interface Ended {
  readonly how: string;
  readonly clean: boolean;
}

/** What the shell observes. `run-end` is the run's own: its `postTasksExecution`, or a held run's question answered. */
export type EndpointEvent =
  | { readonly type: 'stdout'; readonly text: string }
  | { readonly type: 'stdout-end' }
  | { readonly type: 'stderr'; readonly text: string }
  /** Node's `error` of the child: it could not be spawned. */
  | { readonly type: 'error'; readonly message: string }
  /** Node's `close` of the child: it has exited and its pipes have ended. */
  | { readonly type: 'close'; readonly code: number | null; readonly signal: string | null }
  | { readonly type: 'probe'; readonly text: string }
  /** The probe's connection ended or failed. */
  | { readonly type: 'probe-end' }
  | { readonly type: 'run-end' }
  /** The timer the last `arm` started has run out. */
  | { readonly type: 'expired' };

/** What the shell does. `refuse` and `conclude` remove the socket's directory before they answer. */
export type EndpointEffect =
  /** Connect to the endpoint's socket and send it `{"type":"capabilities"}`. */
  | { readonly do: 'probe' }
  /** End the probe's connection. */
  | { readonly do: 'hang-up' }
  /** Answer the start: the endpoint serves, and its mbx gates test cases or does not. */
  | { readonly do: 'serve'; readonly gates: boolean }
  /** Answer the start: no endpoint, and why. */
  | { readonly do: 'refuse'; readonly why: string }
  | { readonly do: 'close-stdin' }
  | { readonly do: 'kill' }
  | { readonly do: 'arm'; readonly ms: number }
  | { readonly do: 'disarm' }
  /** Answer the run's end. */
  | { readonly do: 'conclude'; readonly ended: Ended };

export type EndpointStep = readonly [EndpointLife, readonly EndpointEffect[]];

/** What is kept of the endpoint's stderr to say why it ended: its last bytes. */
const SAID_BYTES = 4096;

/** The life of a child just spawned; `pid` is what names it should it not end when killed. */
export function spawned(pid: number | undefined): EndpointLife {
  return { phase: 'starting', out: '', said: '', pid };
}
const ENDED: EndpointLife = { phase: 'ended' };

type CapabilitiesReply = { type: 'capabilities'; test_gate: boolean };
const parseCapabilities = typia.json.createValidateParse<CapabilitiesReply>();

/**
 * Whether the mbx that answered `line` to `{"type":"capabilities"}` gates test cases. One that does not know the
 * request answers an error, which gates nothing.
 */
/**
 * Whether the mbx that answered `line` to `{"type":"capabilities"}` gates test cases. One that does not know the
 * request answers an error, which gates nothing, and so does a line that is no JSON: typia's parse validates what
 * `JSON.parse` returns and throws what it throws.
 */
export function gatesTests(line: string): boolean {
  try {
    const reply = parseCapabilities(line);
    return reply.success && reply.data.test_gate;
  } catch {
    return false;
  }
}

/** `text` up to its first line break, or null while it holds none. */
function firstLine(text: string): string | null {
  const end = text.indexOf('\n');
  return end === -1 ? null : text.slice(0, end);
}

function saying(said: string, text: string): string {
  return (said + text).slice(-SAID_BYTES);
}

function closed(said: string, code: number | null, signal: string | null): Ended {
  const last = said.trim().split('\n').at(-1) ?? '';
  return {
    how: `mbx reservations ended ${signal === null ? `with status ${code}` : `on ${signal}`}${last === '' ? '' : `: ${last}`}`,
    clean: signal === null && code === 0 && last === '',
  };
}

function unspawned(message: string): Ended {
  return { how: `mbx reservations could not run: ${message}`, clean: false };
}

/** How the child ended, when `event` is its end. */
function endOf(said: string, event: EndpointEvent): Ended | null {
  if (event.type === 'close') return closed(said, event.code, event.signal);
  if (event.type === 'error') return unspawned(event.message);
  return null;
}

/** The next state of `life` at `event`, and what the shell does about it. */
export function endpointStep(life: EndpointLife, event: EndpointEvent): EndpointStep {
  switch (life.phase) {
    case 'starting':
      return starting(life, event);
    case 'probing':
      return probing(life, event);
    case 'serving':
      return serving(life, event);
    case 'gone':
      return event.type === 'run-end' ? [ENDED, [{ do: 'conclude', ended: life.ended }]] : [life, []];
    case 'ending':
      return ending(life, event);
    case 'ended':
      return [life, []];
  }
}

function unanswered(phase: string): never {
  throw new Error(
    `the run's end reached its endpoint while ${phase}: the run holds no endpoint before its start is answered`,
  );
}

function starting(life: EndpointLife & { phase: 'starting' }, event: EndpointEvent): EndpointStep {
  const ended = endOf(life.said, event);
  if (ended !== null) return [ENDED, [{ do: 'refuse', why: ended.how }]];
  switch (event.type) {
    case 'stderr':
      return [{ ...life, said: saying(life.said, event.text) }, []];
    case 'stdout': {
      const out = life.out + event.text;
      const line = firstLine(out);
      if (line === null) return [{ ...life, out }, []];
      if (line === 'listening') {
        return [
          { phase: 'probing', reply: '', said: life.said, pid: life.pid, gone: null },
          [{ do: 'probe' }, { do: 'arm', ms: CONTROL_MS }],
        ];
      }
      return [
        {
          phase: 'ending',
          pid: life.pid,
          said: life.said,
          killed: true,
          refusing: `mbx reservations said ${JSON.stringify(line)}, not that it was listening`,
        },
        [{ do: 'kill' }, { do: 'arm', ms: REAP_MS }],
      ];
    }
    case 'stdout-end': {
      const unsaid = `mbx reservations closed its standard output ${life.out === '' ? '' : `after ${JSON.stringify(life.out)}, `}without saying it was listening`;
      return [
        { phase: 'ending', pid: life.pid, said: life.said, killed: false, refusing: unsaid },
        [{ do: 'arm', ms: REAP_MS }],
      ];
    }
    case 'run-end':
      return unanswered(life.phase);
    default:
      return [life, []];
  }
}

function probing(life: EndpointLife & { phase: 'probing' }, event: EndpointEvent): EndpointStep {
  const served: EndpointLife =
    life.gone === null ? { phase: 'serving', said: life.said, pid: life.pid } : { phase: 'gone', ended: life.gone };
  const ended = endOf(life.said, event);
  if (ended !== null) return [{ ...life, gone: life.gone ?? ended }, []];
  switch (event.type) {
    case 'stderr':
      return [{ ...life, said: saying(life.said, event.text) }, []];
    case 'probe': {
      const reply = life.reply + event.text;
      const line = firstLine(reply);
      if (line === null) return [{ ...life, reply }, []];
      return [served, [{ do: 'disarm' }, { do: 'hang-up' }, { do: 'serve', gates: gatesTests(line) }]];
    }
    case 'probe-end':
      return [served, [{ do: 'disarm' }, { do: 'serve', gates: false }]];
    case 'expired':
      return [served, [{ do: 'hang-up' }, { do: 'serve', gates: false }]];
    case 'run-end':
      return unanswered(life.phase);
    default:
      return [life, []];
  }
}

function serving(life: EndpointLife & { phase: 'serving' }, event: EndpointEvent): EndpointStep {
  const ended = endOf(life.said, event);
  if (ended !== null) return [{ phase: 'gone', ended }, []];
  switch (event.type) {
    case 'stderr':
      return [{ ...life, said: saying(life.said, event.text) }, []];
    case 'run-end':
      return [
        { phase: 'ending', said: life.said, pid: life.pid, killed: false, refusing: null },
        [{ do: 'close-stdin' }, { do: 'arm', ms: REAP_MS }],
      ];
    default:
      return [life, []];
  }
}

function ending(life: EndpointLife & { phase: 'ending' }, event: EndpointEvent): EndpointStep {
  const ended = endOf(life.said, event);
  if (ended !== null) {
    if (life.refusing !== null) {
      return [ENDED, [{ do: 'disarm' }, { do: 'refuse', why: life.killed ? life.refusing : ended.how }]];
    }
    const outlived: Ended = life.killed
      ? { how: `${ended.how}, killed after it outlived its standard input by ${REAP_MS}ms`, clean: false }
      : ended;
    return [ENDED, [{ do: 'disarm' }, { do: 'conclude', ended: outlived }]];
  }
  switch (event.type) {
    case 'stderr':
      return [{ ...life, said: saying(life.said, event.text) }, []];
    case 'expired': {
      // A closed pipe did not end it within the reap bound: kill it, and wait that bound once more.
      if (!life.killed) {
        const refusing = life.refusing === null ? null : `${life.refusing}, and had not ended ${REAP_MS}ms later`;
        return [{ ...life, killed: true, refusing }, [{ do: 'kill' }, { do: 'arm', ms: REAP_MS }]];
      }
      // Neither did the kill: it is stuck in the kernel, or something it started holds its pipes. The wait ends and
      // says so; the child is left as it is (05_gateway.md, "A child's exit and its output are reap-class waits").
      const left = `${life.pid === undefined ? 'it' : `pid ${life.pid}`} had not ended ${REAP_MS}ms after it was killed and is left as it is`;
      if (life.refusing !== null) return [ENDED, [{ do: 'refuse', why: `${life.refusing}; ${left}` }]];
      const how = `mbx reservations outlived its standard input by ${REAP_MS}ms; ${left}`;
      return [ENDED, [{ do: 'conclude', ended: { how, clean: false } }]];
    }
    default:
      return [life, []];
  }
}
