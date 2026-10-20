/**
 * Standalone supervisor: <runtime> runner-hook.js <id> <go|cargo> <executable> [args...].
 * The parent requests native JSON output and supplies a private duplex socket on fd 3.
 * Each newline JSON {id,phase} must be echoed exactly after applying the watchdog state.
 * Build is acknowledged before spawn; each native transition is acknowledged before its
 * stdout record is forwarded. Terminal follows child close and drained output, then its
 * acknowledgement closes the channel. The tool receives only descriptors 0, 1, and 2.
 */
import { type ChildProcess, spawn } from 'node:child_process';
import { Socket } from 'node:net';
import type { Readable, Writable } from 'node:stream';

type Runner = 'go' | 'cargo';
type Phase = 'build' | 'active' | 'terminal';

interface Lifecycle {
  announce(phase: Phase): Promise<Error | null>;
  close(): Promise<void>;
}

function failure(context: string, error: unknown): Error {
  return new Error(`${context}: ${error instanceof Error ? error.message : String(error)}`);
}

// This executable inherits lib es2022; Promise.withResolvers requires es2024.
function write(stream: Writable, bytes: Buffer | string): Promise<Error | null> {
  return new Promise((resolve) => {
    stream.write(bytes, (error) => resolve(error ?? null));
  });
}

/** Only native stdout records change the lifecycle; arbitrary runner text never does. */
function nativePhase(runner: Runner): (line: Buffer) => Phase | null {
  const packages = new Set<string>();
  let cargoActive = false;
  return (line) => {
    if (cargoActive || line[0] !== 123) return null;
    let event: unknown;
    try {
      event = JSON.parse(line.toString('utf8'));
    } catch (error) {
      if (error instanceof SyntaxError) return null;
      throw error;
    }
    if (typeof event !== 'object' || event === null) return null;
    if (runner === 'cargo') {
      if (
        'reason' in event &&
        event.reason === 'build-finished' &&
        'success' in event &&
        typeof event.success === 'boolean'
      ) {
        // Even a failed build has left compilation. Nothing emitted by a test, doctest,
        // or custom harness can put Cargo back into build or finish this invocation.
        cargoActive = true;
        return 'active';
      }
      return null;
    }
    if (!('Action' in event) || !('Package' in event) || typeof event.Package !== 'string' || 'Test' in event) {
      return null;
    }
    switch (event.Action) {
      case 'start':
        packages.add(event.Package);
        break;
      case 'pass':
      case 'fail':
      case 'skip':
        packages.delete(event.Package);
        break;
      default:
        return null;
    }
    return packages.size === 0 ? 'build' : 'active';
  };
}

/** One outstanding phase, acknowledged only after the executor applies its watchdog state. */
function lifecycle(id: number, failed: (error: Error) => void): Lifecycle {
  const socket = new Socket({ fd: 3, readable: true, writable: true });
  let pending: { message: string; resolve: (error: Error | null) => void } | null = null;
  let received = '';
  let error: Error | null = null;
  let closing = false;
  let phase: Phase | null = null;
  const fail = (cause: Error): void => {
    if (error !== null) return;
    error = cause;
    const waiting = pending;
    pending = null;
    waiting?.resolve(cause);
    failed(cause);
    socket.destroy();
  };
  socket.on('error', (cause) => fail(failure('lifecycle channel failed', cause)));
  socket.on('end', () => {
    if (!closing) fail(new Error('lifecycle channel ended before terminal acknowledgement'));
  });
  socket.on('close', () => {
    if (!closing && error === null) fail(new Error('lifecycle channel closed before terminal acknowledgement'));
  });
  socket.on('data', (chunk: Buffer) => {
    received += chunk.toString('utf8');
    if (pending === null || !pending.message.startsWith(received)) {
      fail(new Error(`unexpected lifecycle acknowledgement: ${JSON.stringify(received)}`));
    } else if (received === pending.message) {
      const waiting = pending;
      pending = null;
      received = '';
      waiting.resolve(null);
    }
  });
  return {
    async announce(next: Phase): Promise<Error | null> {
      if (error !== null) return error;
      if (phase === next) return null;
      if (pending !== null) throw new Error('concurrent lifecycle announcements');
      const result = await new Promise<Error | null>((resolve) => {
        const message = `${JSON.stringify({ id, phase: next })}\n`;
        pending = { message, resolve };
        socket.write(message, (cause) => {
          if (cause) fail(failure('writing lifecycle phase failed', cause));
        });
      });
      if (result === null) phase = next;
      return result;
    },
    async close(): Promise<void> {
      closing = true;
      if (socket.destroyed) return;
      await new Promise<void>((resolve) => {
        socket.once('close', resolve);
        // The shell retains this endpoint for later commands: shutdown/end would poison
        // every inherited reference. Terminal ACK drained our writes; close only ours.
        socket.destroy();
      });
    },
  };
}

/** Await each destination write, bounding queued output and preserving its original bytes. */
async function forward(source: Readable, target: Writable): Promise<Error | null> {
  try {
    for await (const chunk of source) {
      const bytes: unknown = chunk;
      if (!Buffer.isBuffer(bytes)) throw new Error('child output was not a byte stream');
      const error = await write(target, bytes);
      if (error !== null) return error;
    }
    return null;
  } catch (error) {
    return failure('reading child output failed', error);
  }
}

/** Buffer only possible JSON records; ACK their transition before releasing any of their bytes. */
async function forwardStdout(
  source: Readable,
  inspect: (line: Buffer) => Promise<Error | null>,
): Promise<Error | null> {
  let pending: Buffer[] = [];
  let start = true;
  let json = false;
  const record = async (line: Buffer): Promise<Error | null> => {
    const error = await inspect(line);
    return error ?? (await write(process.stdout, line));
  };
  try {
    for await (const chunk of source) {
      const bytes: unknown = chunk;
      if (!Buffer.isBuffer(bytes)) throw new Error('child stdout was not a byte stream');
      let offset = 0;
      while (offset < bytes.length) {
        if (start) json = bytes[offset] === 123;
        const newline = bytes.indexOf(10, offset);
        const end = newline === -1 ? bytes.length : newline + 1;
        const piece = bytes.subarray(offset, end);
        let error: Error | null = null;
        if (json && newline === -1) {
          pending.push(piece);
        } else if (json) {
          let line = piece;
          if (pending.length !== 0) {
            pending.push(piece);
            line = Buffer.concat(pending);
            pending = [];
          }
          error = await record(line);
        } else {
          error = await write(process.stdout, piece);
        }
        if (error !== null) return error;
        start = newline !== -1;
        offset = end;
      }
    }
    return pending.length === 0 ? null : await record(Buffer.concat(pending));
  } catch (error) {
    return failure('reading child stdout failed', error);
  }
}

/** argv: invocation ID, runner kind, original executable, then its unchanged arguments. */
async function main([idText, runner, executable, ...args]: readonly string[]): Promise<void> {
  const id = Number(idText);
  if (
    idText === undefined ||
    !/^\d+$/.test(idText) ||
    !Number.isSafeInteger(id) ||
    (runner !== 'go' && runner !== 'cargo') ||
    executable === undefined ||
    executable === ''
  ) {
    process.stderr.write('runner-hook: usage: <invocation-id> <go|cargo> <executable> [arguments...]\n');
    process.exitCode = 2;
    return;
  }
  let child: ChildProcess | null = null;
  let failed = false;
  let interrupted: NodeJS.Signals | null = null;
  const fail = (error: Error): void => {
    if (failed) return;
    failed = true;
    process.exitCode = 1;
    process.stderr.write(`runner-hook: ${error.message}\n`);
    child?.kill('SIGKILL');
  };
  const forwardedSignals: NodeJS.Signals[] = ['SIGINT', 'SIGTERM', 'SIGHUP', 'SIGQUIT'];
  const signals = forwardedSignals.map((name) => {
    const listener = (): void => {
      interrupted = name;
      child?.kill(name);
    };
    process.on(name, listener);
    return { name, listener };
  });
  process.stdout.on('error', fail);
  process.stderr.on('error', fail);
  let channel: Lifecycle | null = null;
  let code: number | null = null;
  let exitSignal: NodeJS.Signals | null = null;
  try {
    channel = lifecycle(id, fail);
    const ready = await channel.announce('build');
    if (ready !== null) fail(ready);
    if (!failed && interrupted === null) {
      // Descriptor 3 belongs only to this supervisor, never the tool or its descendants.
      const running = spawn(executable, args, { stdio: ['inherit', 'pipe', 'pipe'] });
      child = running;
      const closed = new Promise<void>((resolve) => {
        running.once('close', (status, reason) => {
          code = status;
          exitSignal = reason;
          resolve();
        });
      });
      running.on('error', (error) => fail(failure(`could not run ${JSON.stringify(executable)}`, error)));
      const scan = nativePhase(runner);
      const control = channel;
      const stdout = forwardStdout(running.stdout, async (line) => {
        const next = scan(line);
        return next === null ? null : await control.announce(next);
      }).then((error) => {
        if (error !== null) fail(failure('forwarding stdout failed', error));
      });
      const stderr = forward(running.stderr, process.stderr).then((error) => {
        if (error !== null) fail(failure('forwarding stderr failed', error));
      });
      await Promise.all([closed, stdout, stderr]);
    } else {
      exitSignal = interrupted;
    }
  } catch (error) {
    fail(failure('supervisor failed', error));
  }
  // Write callbacks complete only once downstream has accepted every forwarded byte.
  // Flush diagnostics too, before terminal and before re-raising a child's signal.
  for (const error of await Promise.all([write(process.stdout, ''), write(process.stderr, '')])) {
    if (error !== null) fail(failure('draining supervisor output failed', error));
  }
  if (channel !== null) {
    const error = await channel.announce('terminal');
    if (error !== null) fail(error);
    await channel.close();
  }
  for (const { name, listener } of signals) process.off(name, listener);
  process.exitCode = failed ? 1 : (code ?? 1);
  if (!failed && exitSignal !== null) process.kill(process.pid, exitSignal);
}

await main(process.argv.slice(2));
