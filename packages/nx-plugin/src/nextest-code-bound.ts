#!/usr/bin/env bun
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * A nextest run-wrapper that bounds a test's code time rather than its wall time
 * (specs/cowshed/08_testing.md, "Integration tests (real substrate)").
 *
 * A real-APFS test spends part of its wall time queued for the host disk-lifecycle lease, behind
 * every other cowshed process on the host (05_gateway.md, "Disk-lifecycle lease"). That queue is
 * the host's, not the test's code, so a wall-clock bound fails a fast test on a busy host. The
 * wrapper names a fresh lease-wait record in `COWSHED_LEASE_WAIT_RECORD`, runs the test binary,
 * which every disk-lease client of the test's process tree appends its waits to, and then judges
 * the test's code time, its wall time less the union of those waits, against the bound it was
 * configured with. It says the split on stderr after the test body, and fails a passing test whose
 * code time is over the bound. nextest's own slow-timeout stays the backstop that ends a hang.
 *
 * Usage, as nextest invokes it: `nextest-code-bound.ts <bound-seconds> <test-binary> [args…]`.
 */

/** The variable naming the record; the same name every disk-lease client reads. */
export const LEASE_WAIT_RECORD_ENV = 'COWSHED_LEASE_WAIT_RECORD';

/** One line of the record: a wait's start, before its client asks, or its end, however it ended. */
export type WaitEdge =
  | {
      readonly kind: 'start';
      readonly pid: number;
      readonly seq: number;
      readonly atUs: number;
      readonly diskClass: string;
    }
  | { readonly kind: 'end'; readonly pid: number; readonly seq: number; readonly atUs: number };

export type ParsedRecord = { ok: true; edges: WaitEdge[] } | { ok: false; error: string };

const INTEGER = /^\d+$/;

/** Reads a record as its clients write it. Any line that is not one of the two shapes is refused. */
export function parseWaitRecord(text: string): ParsedRecord {
  const edges: WaitEdge[] = [];
  for (const line of text.split('\n')) {
    if (line === '') {
      continue;
    }
    const fields = line.split(' ');
    const [kind, pid, seq, atUs] = fields;
    const numeric = [pid, seq, atUs].every((field) => field !== undefined && INTEGER.test(field));
    if (kind === 'start' && fields.length === 5 && numeric) {
      edges.push({ kind, pid: Number(pid), seq: Number(seq), atUs: Number(atUs), diskClass: fields[4] });
    } else if (kind === 'end' && fields.length === 4 && numeric) {
      edges.push({ kind, pid: Number(pid), seq: Number(seq), atUs: Number(atUs) });
    } else {
      return { ok: false, error: `line ${JSON.stringify(line)} is neither a wait's start nor its end` };
    }
  }
  return { ok: true, edges };
}

/** A wait whose end never reached the record: its process died while waiting, or still waits. */
export interface OpenWait {
  readonly pid: number;
  readonly seq: number;
  readonly startedUs: number;
}

export interface CodeTime {
  readonly wallUs: number;
  /** The union of every wait, clipped to the test's run: concurrent waits count once. */
  readonly waitUs: number;
  readonly codeUs: number;
  readonly waits: number;
  /** Counted to the test's end: the record cannot say when they stopped. */
  readonly open: readonly OpenWait[];
  /** Ends with no start, which can only follow an unwritable start: counted as code time. */
  readonly strayEnds: number;
}

/** The test's code time: its wall time from `startedUs` to `endedUs` less the union of its waits. */
export function codeTime(edges: readonly WaitEdge[], startedUs: number, endedUs: number): CodeTime {
  const starts = new Map<string, { atUs: number; pid: number; seq: number }>();
  const intervals: Array<[number, number]> = [];
  let strayEnds = 0;
  for (const edge of edges) {
    const key = `${edge.pid}:${edge.seq}`;
    if (edge.kind === 'start') {
      starts.set(key, { atUs: edge.atUs, pid: edge.pid, seq: edge.seq });
      continue;
    }
    const start = starts.get(key);
    if (start === undefined) {
      strayEnds += 1;
      continue;
    }
    starts.delete(key);
    intervals.push([start.atUs, edge.atUs]);
  }
  const open = [...starts.values()].map(({ pid, seq, atUs }) => ({ pid, seq, startedUs: atUs }));
  for (const wait of open) {
    intervals.push([wait.startedUs, endedUs]);
  }
  const waits = intervals.length;
  const clipped = intervals
    .map(([from, to]): [number, number] => [Math.max(from, startedUs), Math.min(to, endedUs)])
    .filter(([from, to]) => to > from)
    .sort((left, right) => left[0] - right[0]);
  let waitUs = 0;
  let reach = startedUs;
  for (const [from, to] of clipped) {
    const begin = Math.max(from, reach);
    if (to > begin) {
      waitUs += to - begin;
      reach = to;
    }
  }
  const wallUs = Math.max(0, endedUs - startedUs);
  return { wallUs, waitUs, codeUs: wallUs - waitUs, waits, open, strayEnds };
}

const seconds = (us: number): string => `${(us / 1_000_000).toFixed(1)}s`;

/** The one line the wrapper prints after the test body. */
export function describeCodeTime(time: CodeTime, boundUs: number): string {
  const verdict = time.codeUs > boundUs ? 'OVER its bound' : 'within its bound';
  const parts = [
    `nextest-code-bound: code ${seconds(time.codeUs)} of ${seconds(boundUs)}, ${verdict}`,
    `(wall ${seconds(time.wallUs)} less ${seconds(time.waitUs)} waiting for the host disk lease over ${time.waits} waits)`,
  ];
  for (const wait of time.open) {
    parts.push(`; pid ${wait.pid}'s wait ${wait.seq} never ended and counts to the test's end`);
  }
  if (time.strayEnds > 0) {
    parts.push(`; ${time.strayEnds} wait ends had no recorded start and count as code time`);
  }
  return parts.join(' ');
}

const BOUND_PATTERN = /^\d+(\.\d+)?$/;

/** The signals the wrapper passes on to the test rather than dying of. */
const FORWARDED: readonly NodeJS.Signals[] = ['SIGTERM', 'SIGINT', 'SIGHUP'];

/** Runs the test under the record, judges it, and exits as the judgment says. */
async function main(argv: readonly string[]): Promise<never> {
  const [bound, binary, ...args] = argv;
  if (bound === undefined || !BOUND_PATTERN.test(bound) || Number(bound) <= 0 || binary === undefined) {
    process.stderr.write('nextest-code-bound: usage: nextest-code-bound.ts <bound-seconds> <test-binary> [args…]\n');
    process.exit(2);
  }
  const boundUs = Number(bound) * 1_000_000;
  const record = join(tmpdir(), `cowshed-lease-waits-${process.pid}-${randomUUID()}`);
  writeFileSync(record, '', { mode: 0o600, flag: 'wx' });
  const startedUs = Math.round((performance.timeOrigin + performance.now()) * 1000);
  const child = spawn(binary, args, { stdio: 'inherit', env: { ...process.env, [LEASE_WAIT_RECORD_ENV]: record } });
  // nextest stops a test by signalling its process group, which the child is in; the wrapper stays
  // to report what the stopped test spent, and passes on a signal sent to it alone.
  const handlers = FORWARDED.map((signal) => {
    const handler = () => {
      child.kill(signal);
    };
    process.on(signal, handler);
    return { signal, handler };
  });
  const exit = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((settle) => {
    child.once('error', (error) => {
      process.stderr.write(`nextest-code-bound: could not start ${binary}: ${error.message}\n`);
      settle({ code: 1, signal: null });
    });
    child.once('exit', (code, signal) => settle({ code, signal }));
  });
  const endedUs = Math.round((performance.timeOrigin + performance.now()) * 1000);
  for (const { signal, handler } of handlers) {
    process.removeListener(signal, handler);
  }
  const parsed = parseWaitRecord(readFileSync(record, 'utf8'));
  rmSync(record, { force: true });
  if (!parsed.ok) {
    process.stderr.write(`nextest-code-bound: the lease-wait record ${record} is unreadable: ${parsed.error}\n`);
    process.exit(exit.code === 0 ? 1 : (exit.code ?? 1));
  }
  const time = codeTime(parsed.edges, startedUs, endedUs);
  process.stderr.write(`${describeCodeTime(time, boundUs)}\n`);
  if (exit.signal !== null) {
    process.kill(process.pid, exit.signal);
  }
  if (exit.code !== 0) {
    process.exit(exit.code ?? 1);
  }
  process.exit(time.codeUs > boundUs ? 1 : 0);
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main(process.argv.slice(2));
}
