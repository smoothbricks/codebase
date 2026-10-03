/**
 * The processes behind the counted Cargo (`counted-cargo.ts`): every `cargo metadata` child, and the
 * process a child leaves running behind it when asked to, as a real Cargo leaves rustc. Each holds one
 * control connection to the fixture's broker in the test process for its whole life and reports its
 * life over it; the broker answers each step when the test lets it. Nothing here waits on a clock:
 * a child is held by a read on its connection, and a process's death is its connection's EOF.
 *
 * The protocol is a line per message, the broker's payload text JSON-quoted:
 *
 *   child → broker       start <pid> | read <grandchild pid | -> | end <exit status>
 *   grandchild → broker  grandchild <pid> <parent pid>
 *   broker → child       read <0|1 leave a grandchild> <canned answer | null> | answer <failure stderr | null>
 *
 * The broker never speaks to a grandchild; it ends a grandchild's connection on `release`, and a
 * grandchild lives exactly until its connection ends.
 */
import { spawn } from 'node:child_process';
import { closeSync, openSync, writeSync } from 'node:fs';
import { connect, type Socket } from 'node:net';
import { constants } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

/** What a `cargo metadata` child or a grandchild tells the broker. */
export type ProcessReport =
  | { readonly kind: 'start'; readonly pid: number }
  | { readonly kind: 'read'; readonly grandchild: number | null }
  | { readonly kind: 'end'; readonly status: number }
  | { readonly kind: 'grandchild'; readonly pid: number; readonly parent: number };

/** What the broker tells a `cargo metadata` child, each when its gate lets it. */
export type BrokerOrder =
  | { readonly kind: 'read'; readonly grandchild: boolean; readonly canned: string | null }
  | { readonly kind: 'answer'; readonly failure: string | null };

export const BROKER_HOST = '127.0.0.1';

export function encodeReport(report: ProcessReport): string {
  switch (report.kind) {
    case 'start':
      return `start ${report.pid}\n`;
    case 'read':
      return `read ${report.grandchild ?? '-'}\n`;
    case 'end':
      return `end ${report.status}\n`;
    case 'grandchild':
      return `grandchild ${report.pid} ${report.parent}\n`;
  }
}

export function encodeOrder(order: BrokerOrder): string {
  switch (order.kind) {
    case 'read':
      return `read ${order.grandchild ? 1 : 0} ${JSON.stringify(order.canned)}\n`;
    case 'answer':
      return `answer ${JSON.stringify(order.failure)}\n`;
  }
}

function integer(text: string | undefined, what: string): number | Error {
  const value = Number(text);
  return text !== undefined && text !== '' && Number.isSafeInteger(value) && value >= 0
    ? value
    : new Error(`${what} is not a non-negative integer: ${JSON.stringify(text)}`);
}

function text(json: string): string | null | Error {
  let value: unknown;
  try {
    value = JSON.parse(json);
  } catch (error) {
    return new Error(`payload is not JSON: ${json}`, { cause: error });
  }
  return typeof value === 'string' || value === null ? value : new Error(`expected a JSON string or null, got ${json}`);
}

export function parseReport(line: string): ProcessReport | Error {
  const [kind, first, second, ...extra] = line.split(' ');
  if (extra.length > 0 || (kind === 'grandchild') === (second === undefined)) {
    return new Error(`malformed report ${JSON.stringify(line)}`);
  }
  switch (kind) {
    case 'start': {
      const pid = integer(first, 'start pid');
      return pid instanceof Error ? pid : { kind: 'start', pid };
    }
    case 'read': {
      if (first === '-') return { kind: 'read', grandchild: null };
      const grandchild = integer(first, 'grandchild pid');
      return grandchild instanceof Error ? grandchild : { kind: 'read', grandchild };
    }
    case 'end': {
      const status = integer(first, 'exit status');
      return status instanceof Error ? status : { kind: 'end', status };
    }
    case 'grandchild': {
      const pid = integer(first, 'grandchild pid');
      if (pid instanceof Error) return pid;
      const parent = integer(second, 'parent pid');
      return parent instanceof Error ? parent : { kind: 'grandchild', pid, parent };
    }
    default:
      return new Error(`unknown report ${JSON.stringify(line)}`);
  }
}

export function parseOrder(line: string): BrokerOrder | Error {
  if (line.startsWith('read ')) {
    const flag = line.slice(5, 6);
    if ((flag !== '0' && flag !== '1') || line[6] !== ' ') return new Error(`malformed order ${JSON.stringify(line)}`);
    const canned = text(line.slice(7));
    return canned instanceof Error ? canned : { kind: 'read', grandchild: flag === '1', canned };
  }
  if (line.startsWith('answer ')) {
    const failure = text(line.slice(7));
    return failure instanceof Error ? failure : { kind: 'answer', failure };
  }
  return new Error(`unknown order ${JSON.stringify(line)}`);
}

/**
 * Calls `onLine` with each newline-terminated line `socket` delivers, in order. Returns what has
 * arrived since the last newline: a peer that died mid-line leaves it there for diagnostics.
 */
export function readLines(socket: Socket, onLine: (line: string) => void): () => string {
  let pending = Buffer.alloc(0);
  socket.on('data', (chunk: Buffer) => {
    let data = pending.length === 0 ? chunk : Buffer.concat([pending, chunk]);
    for (let newline = data.indexOf(10); newline !== -1; newline = data.indexOf(10)) {
      onLine(data.toString('utf8', 0, newline));
      data = data.subarray(newline + 1);
    }
    pending = data;
  });
  return () => pending.toString('utf8');
}

/** One process's end, said on its stderr: the parent that reads it, or the log a grandchild writes. */
function fail(message: string): never {
  writeSync(2, `counted cargo process ${process.pid}: ${message}\n`);
  process.exit(1);
}

function connectToBroker(port: number): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const socket = connect({ host: BROKER_HOST, port });
    socket.once('error', reject);
    socket.once('connect', () => {
      socket.off('error', reject);
      resolve(socket);
    });
  });
}

/** The broker's orders, one at a time, in the order they arrive; `null` once the broker has hung up. */
function orders(socket: Socket): () => Promise<BrokerOrder | null> {
  const arrived: (BrokerOrder | Error)[] = [];
  const waiting: ((order: BrokerOrder | Error | null) => void)[] = [];
  let closed = false;
  readLines(socket, (line) => {
    const order = parseOrder(line);
    const waiter = waiting.shift();
    if (waiter === undefined) arrived.push(order);
    else waiter(order);
  });
  socket.on('close', () => {
    closed = true;
    for (const waiter of waiting.splice(0)) waiter(null);
  });
  socket.on('error', (error) => fail(`connection to the broker failed: ${error.message}`));
  return async () => {
    const next =
      arrived.shift() ??
      (closed
        ? null
        : await new Promise<BrokerOrder | Error | null>((resolve) => {
            waiting.push(resolve);
          }));
    if (next instanceof Error) fail(next.message);
    return next;
  };
}

function write(stream: NodeJS.WritableStream, data: string | Buffer): Promise<void> {
  return new Promise((resolve, reject) => {
    stream.write(data, (error) => (error ? reject(error) : resolve()));
  });
}

/** What the real `cargo metadata` prints, and the status it exits with; its stderr is this child's. */
function runReal(real: string, args: readonly string[]): Promise<{ stdout: Buffer; status: number }> {
  return new Promise((resolve, reject) => {
    const cargo = spawn(real, args, { stdio: ['ignore', 'pipe', 'inherit'] });
    const chunks: Buffer[] = [];
    cargo.stdout.on('data', (chunk: Buffer) => chunks.push(chunk));
    cargo.once('error', reject);
    cargo.once('close', (code, signal) =>
      resolve({
        stdout: Buffer.concat(chunks),
        status: code ?? 128 + (signal === null ? 0 : constants.signals[signal]),
      }),
    );
  });
}

/**
 * A process left running in this child's process group, as real Cargo leaves rustc. Its output goes
 * to a log in the fixture, never this child's pipes, so it holds nothing of Cargo's open.
 */
function leaveGrandchild(port: number, directory: string): number {
  const log = openSync(join(directory, `grandchild-of-${process.pid}.log`), 'a');
  try {
    const program = fileURLToPath(import.meta.url);
    const grandchild = spawn(process.execPath, [program, 'grandchild', String(port), String(process.pid)], {
      stdio: ['ignore', log, log],
    });
    const pid = grandchild.pid;
    if (pid === undefined) fail('the grandchild did not start');
    grandchild.once('error', (error) => fail(`the grandchild failed: ${error.message}`));
    grandchild.unref();
    return pid;
  } finally {
    closeSync(log);
  }
}

/** One `cargo metadata` child: reports its life, and takes each step only when the broker orders it. */
async function metadataChild(port: number, real: string, directory: string, args: readonly string[]): Promise<void> {
  const socket = await connectToBroker(port);
  const next = orders(socket);
  socket.write(encodeReport({ kind: 'start', pid: process.pid }));

  const read = await next();
  if (read === null) fail('the broker hung up before letting this child read the workspace');
  if (read.kind !== 'read') fail(`expected the order to read, got ${read.kind}`);
  const produced = read.canned === null ? await runReal(real, args) : { stdout: Buffer.from(read.canned), status: 0 };
  const grandchild = read.grandchild ? leaveGrandchild(port, directory) : null;
  socket.write(encodeReport({ kind: 'read', grandchild }));

  const answer = await next();
  if (answer === null) fail('the broker hung up before letting this child answer');
  if (answer.kind !== 'answer') fail(`expected the order to answer, got ${answer.kind}`);
  let status = produced.status;
  if (answer.failure === null) {
    await write(process.stdout, produced.stdout);
  } else {
    await write(process.stderr, answer.failure);
    status = 101;
  }
  socket.end(encodeReport({ kind: 'end', status }));
  // The process ends when the connection is done and the event loop drains, with every byte written.
  process.exitCode = status;
}

/** A grandchild: registers, and lives exactly as long as its connection to the broker. */
async function grandchild(port: number, parent: number): Promise<void> {
  const socket = await connectToBroker(port);
  socket.on('error', (error) => fail(`connection to the broker failed: ${error.message}`));
  socket.on('data', (chunk: Buffer) => fail(`the broker spoke to a grandchild: ${JSON.stringify(String(chunk))}`));
  socket.write(encodeReport({ kind: 'grandchild', pid: process.pid, parent }));
}

async function main([role, portText, ...rest]: readonly string[]): Promise<void> {
  const port = integer(portText, 'broker port');
  if (port instanceof Error) fail(port.message);
  switch (role) {
    case 'metadata': {
      const [real, directory, ...args] = rest;
      if (real === undefined || directory === undefined) fail('usage: metadata <port> <real> <directory> <args...>');
      return metadataChild(port, real, directory, args);
    }
    case 'grandchild': {
      const parent = integer(rest[0], 'parent pid');
      if (parent instanceof Error) fail(parent.message);
      return grandchild(port, parent);
    }
    default:
      fail(`unknown role ${JSON.stringify(role)}`);
  }
}

if (import.meta.main) {
  await main(process.argv.slice(2)).catch((error: unknown) =>
    fail(error instanceof Error ? (error.stack ?? error.message) : String(error)),
  );
}
