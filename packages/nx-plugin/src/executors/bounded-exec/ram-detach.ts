import { connect } from 'node:net';
import { StringDecoder } from 'node:string_decoder';
import type { CommandOutput } from './ram-temp.js';

/**
 * The silence the client allows the gateway before its first heartbeat line (05_gateway.md, "Deadlines"; Rust
 * `UNPROVEN_SILENCE`): a gateway of an earlier build never heartbeats, and its slow answers still arrive within the
 * minute doctor always gave it.
 */
export const UNPROVEN_SILENCE_MS = 60_000;

/** The silence allowed once a heartbeat has come: three 5 s heartbeats (Rust `deadline::HEARTBEAT_SILENCE`). */
export const HEARTBEAT_SILENCE_MS = 15_000;

/** A detach answers its native output, which the gateway bounds; no answer grows past that. */
const ANSWER_BYTES_MAX = 2 * 64 * 1024 * 6 + 4096;

/** Monotonic milliseconds and the one silence timer; `after` answers its cancel. A test fires its timer itself. */
export interface SilenceTimers {
  now(): number;
  after(ms: number, fire: () => void): () => void;
}

const SYSTEM_TIMERS: SilenceTimers = {
  now: () => performance.now(),
  after(ms, fire) {
    const timer = setTimeout(fire, ms);
    return () => clearTimeout(timer);
  },
};

/** The last thing the client saw the gateway do, which a `SILENT` failure names. */
type GatewayProgress = { kind: 'nothing' } | { kind: 'sent' } | { kind: 'heartbeat'; seq: number };

function describeProgress(progress: GatewayProgress): string {
  switch (progress.kind) {
    case 'nothing':
      return 'nothing';
    case 'sent':
      return 'request sent';
    case 'heartbeat':
      return `heartbeat ${progress.seq}`;
  }
}

/**
 * Typed gateway mutation: no caller-owned native child or arbitrary argv. The request is a one-shot control op, so the
 * answer is read under the gateway's silence rule (05_gateway.md, "Deadlines"), never under a total ceiling: until the
 * first heartbeat line the gateway may be silent for {@link UNPROVEN_SILENCE_MS}, after each one for
 * {@link HEARTBEAT_SILENCE_MS}. A silent gateway ends this caller's wait as `SILENT`, with unknown custody; the gateway's
 * detach keeps running, and its late answer still completes `pending`.
 */
export function detachGatewayRamDisk(
  socket: string,
  device: string,
  registryEntry: string,
  timers: SilenceTimers = SYSTEM_TIMERS,
): Promise<CommandOutput> {
  let resolve!: (output: CommandOutput) => void;
  const answer = new Promise<CommandOutput>((done) => {
    resolve = done;
  });
  let complete!: (output: CommandOutput | null) => void;
  const completion = new Promise<CommandOutput | null>((done) => {
    complete = done;
  });
  const started = timers.now();
  let last: GatewayProgress = { kind: 'nothing' };
  let silenceMs = UNPROVEN_SILENCE_MS;
  let disarm = () => {};
  const decoder = new StringDecoder('utf8');
  let received = '';
  let dispatched = false;
  let bounded = false;
  let finished = false;
  const pending = { pid: null, completion };
  const unknown = (reason: string): CommandOutput => ({ status: 1, stdout: '', stderr: reason, pending });
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a unix-socket connect completes or fails at once, and the silence timer listen() arms below ends the whole exchange (05_gateway.md, "Deadlines")
  const connection = connect(socket);
  const finish = (output: CommandOutput | null, reason = 'gateway detach terminal status unavailable') => {
    if (finished) return;
    finished = true;
    disarm();
    complete(output);
    if (!bounded) resolve(output ?? unknown(reason));
    connection.destroy();
  };
  const silent = () => {
    bounded = true;
    resolve(
      unknown(
        `SILENT: ram-detach: the gateway at ${socket} wrote no heartbeat or answer for ${silenceMs} ms (waited ${Math.max(0, timers.now() - started)} ms; last progress: ${describeProgress(last)}); the gateway's detach keeps running, not killed`,
      ),
    );
    connection.unref();
  };
  /** Allow the gateway `silenceMs` from now; once the caller's wait has ended there is nothing left to bound. */
  const listen = () => {
    if (bounded || finished) return;
    disarm();
    disarm = timers.after(silenceMs, silent);
  };
  listen();
  connection.on('connect', () => {
    dispatched = true;
    connection.end(`${JSON.stringify({ op: 'ram-detach', device, registryEntry })}\n`);
    last = { kind: 'sent' };
  });
  connection.on('data', (chunk: Buffer) => {
    received += decoder.write(chunk);
    for (let newline = received.indexOf('\n'); newline >= 0; newline = received.indexOf('\n')) {
      const line = received.slice(0, newline);
      received = received.slice(newline + 1);
      // The gateway heartbeats an answer that is not ready yet (05_gateway.md, "Deadlines"): never the answer.
      const seq = heartbeatSeq(line);
      if (seq !== null) {
        last = { kind: 'heartbeat', seq };
        silenceMs = HEARTBEAT_SILENCE_MS;
        listen();
        continue;
      }
      // A line past the cap is refused before it is read, whatever chunk completed it.
      if (Buffer.byteLength(line) > ANSWER_BYTES_MAX)
        return finish(null, 'gateway detach reply exceeds its bounded output contract');
      const parsed = readDetachAnswer(line);
      if (parsed === null) finish(null, `gateway detach reply proves no terminal outcome: ${line.slice(0, 512)}`);
      else finish(parsed);
      return;
    }
    // Native output is bounded by the gateway contract; never buffer an unbounded peer reply.
    if (Buffer.byteLength(received) > ANSWER_BYTES_MAX)
      finish(null, 'gateway detach reply exceeds its bounded output contract');
  });
  connection.on('error', (error) => {
    if (!dispatched) finish({ status: 103, stdout: '', stderr: `gateway detach did not start: ${error.message}` });
    else finish(null, `gateway detach connection ended after dispatch: ${error.message}`);
  });
  connection.on('close', () => finish(null, 'gateway detach closed without an authoritative outcome'));
  return answer;
}

/**
 * The heartbeat's `progress` when `line` is exactly a gateway heartbeat, `{"ok":<bool>,"progress":<u64>}` and nothing
 * else (Rust `ProgressLine`, `deny_unknown_fields`); `null` for any other line, which is the answer.
 */
function heartbeatSeq(line: string): number | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return null;
  if (Object.keys(parsed).length !== 2 || !('ok' in parsed) || !('progress' in parsed)) return null;
  const { ok, progress } = parsed;
  return typeof ok === 'boolean' && typeof progress === 'number' && Number.isInteger(progress) && progress >= 0
    ? progress
    : null;
}

/** Only the gateway can attest whether its exact native child ran and was reaped. */
export function readDetachAnswer(line: string): CommandOutput | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null || !('ok' in parsed)) return null;
  if (parsed.ok === false && 'code' in parsed && parsed.code === 'invalid-request') {
    return {
      status: 103,
      stdout: '',
      stderr: `gateway detach did not start: ${'error' in parsed ? String(parsed.error) : 'invalid request'}`,
    };
  }
  if (
    parsed.ok !== true ||
    !('ramDetach' in parsed) ||
    typeof parsed.ramDetach !== 'object' ||
    parsed.ramDetach === null
  )
    return null;
  const result = parsed.ramDetach;
  if (!('outcome' in result)) return null;
  if (result.outcome === 'unknown') return null;
  if (result.outcome === 'not-started' && 'reason' in result && typeof result.reason === 'string') {
    return { status: 103, stdout: '', stderr: `gateway detach did not start: ${result.reason}` };
  }
  if (
    result.outcome !== 'terminal' ||
    !('status' in result) ||
    typeof result.status !== 'object' ||
    result.status === null ||
    !('stdout' in result) ||
    typeof result.stdout !== 'string' ||
    !('stderr' in result) ||
    typeof result.stderr !== 'string' ||
    !('stdoutTruncated' in result) ||
    typeof result.stdoutTruncated !== 'boolean' ||
    !('stderrTruncated' in result) ||
    typeof result.stderrTruncated !== 'boolean'
  )
    return null;
  const status = result.status;
  const code =
    'exit' in status && typeof status.exit === 'number' && Number.isSafeInteger(status.exit) && status.exit >= 0
      ? status.exit
      : 'signal' in status &&
          typeof status.signal === 'number' &&
          Number.isSafeInteger(status.signal) &&
          status.signal > 0
        ? 128 + status.signal
        : null;
  return code === null
    ? null
    : {
        status: code,
        stdout: result.stdout,
        stderr:
          result.stderr +
          (result.stdoutTruncated || result.stderrTruncated
            ? '\n[gateway detach output truncated at the native inline limit]'
            : ''),
      };
}
