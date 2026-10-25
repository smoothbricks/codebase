import { connect } from 'node:net';
import type { CommandOutput } from './ram-temp.js';

/** Typed gateway mutation: no caller-owned native child or arbitrary argv. */
export function detachGatewayRamDisk(
  socket: string,
  device: string,
  registryEntry: string,
  deadlineMs: number,
): Promise<CommandOutput> {
  let resolve!: (output: CommandOutput) => void;
  const answer = new Promise<CommandOutput>((done) => {
    resolve = done;
  });
  let complete!: (output: CommandOutput | null) => void;
  const completion = new Promise<CommandOutput | null>((done) => {
    complete = done;
  });
  const connection = connect(socket);
  let received = '';
  let dispatched = false;
  let bounded = false;
  let finished = false;
  const pending = { pid: null, completion };
  const unknown = (reason: string): CommandOutput => ({ status: 1, stdout: '', stderr: reason, pending });
  const finish = (output: CommandOutput | null, reason = 'gateway detach terminal status unavailable') => {
    if (finished) return;
    finished = true;
    clearTimeout(timer);
    complete(output);
    if (!bounded) resolve(output ?? unknown(reason));
    connection.destroy();
  };
  const timer = setTimeout(() => {
    bounded = true;
    resolve(unknown(`no gateway detach answer within ${deadlineMs} ms; gateway mutation keeps running, not killed`));
    connection.unref();
  }, deadlineMs);
  connection.on('connect', () => {
    dispatched = true;
    connection.end(`${JSON.stringify({ op: 'ram-detach', device, registryEntry })}\n`);
  });
  connection.on('data', (chunk: Buffer) => {
    received += chunk.toString('utf8');
    // Native output is bounded by the gateway contract; never buffer an unbounded peer reply.
    if (received.length > 2 * 64 * 1024 * 6 + 4096)
      return finish(null, 'gateway detach reply exceeds its bounded output contract');
    for (let newline = received.indexOf('\n'); newline >= 0; newline = received.indexOf('\n')) {
      const line = received.slice(0, newline);
      received = received.slice(newline + 1);
      // The gateway heartbeats an answer that is not ready yet (05_gateway.md, "Deadlines"): never the answer.
      if (isHeartbeat(line)) continue;
      const parsed = readDetachAnswer(line);
      if (parsed === null) finish(null, `gateway detach reply proves no terminal outcome: ${line.slice(0, 512)}`);
      else finish(parsed);
      return;
    }
  });
  connection.on('error', (error) => {
    if (!dispatched) finish({ status: 103, stdout: '', stderr: `gateway detach did not start: ${error.message}` });
    else finish(null, `gateway detach connection ended after dispatch: ${error.message}`);
  });
  connection.on('close', () => finish(null, 'gateway detach closed without an authoritative outcome'));
  return answer;
}

/** `{"ok":true,"progress":<n>}`: a gateway control answer's heartbeat line (05_gateway.md, "Deadlines"). */
function isHeartbeat(line: string): boolean {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return false;
  }
  return (
    typeof parsed === 'object' &&
    parsed !== null &&
    'ok' in parsed &&
    parsed.ok === true &&
    'progress' in parsed &&
    typeof parsed.progress === 'number' &&
    !('ramDetach' in parsed)
  );
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
