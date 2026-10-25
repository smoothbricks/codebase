import { connect, type Socket } from 'node:net';
import { readDetachAnswer } from './ram-detach.js';
import type { RamDiskLease, RamUnavailable, Result } from './ram-temp.js';

/**
 * The client of the cowshed gateway's `ram-lease` (specs/cowshed/05_gateway.md, "RAM disks"). The RAM temp volume's
 * disk lives in the `diskimages-helper` that `hdiutil attach ram://<n>` forks. Forked inside a task's job, the helper is
 * that job's member, and the job's end retires it with the disk under every other job's lease on the volume. The helper
 * belongs to the gateway, which no job contains and which attaches the disk; no job's retirement reaches it.
 *
 * The client writes one line, `{"op":"ram-lease","sectors":<n>}`, and keeps its connection open; the gateway answers
 * one line, `{"ok":true,"ramDisk":{"device":"/dev/diskN","registryEntry":"0x…"}}` or a refusal. The connection is the
 * lease: the gateway holds the disk until the client writes `{"end":"keep"}`, after which the disk is the caller's, or
 * `{"end":"release"}`, which the gateway answers with its detach as `ram-detach` does. A connection that ends first,
 * at the client's deadline or with its process, has the gateway detach the disk. An end the gateway never answers leaves
 * nothing known: the gateway may have handed the disk over or left it to its next startup scan, so the caller detaches it
 * by its entry.
 */

export const RAM_LEASE_OP = 'ram-lease';

const WHOLE_DISK = /^\/dev\/disk\d+$/;
const REGISTRY_ENTRY = /^0x[0-9a-f]+$/;
/** A release answers what `ram-detach` does, whose native output the gateway bounds; no answer grows past that. */
const ANSWER_BYTES_MAX = 2 * 64 * 1024 * 6 + 4096;

/** What the lease's connection gave next: one answer line, or why it gave none. */
type Next =
  | { readonly kind: 'line'; readonly line: string }
  | { readonly kind: 'closed' }
  | { readonly kind: 'failed'; readonly error: Error }
  | { readonly kind: 'late' };

/** The disk a granted lease holds. */
interface Granted {
  readonly device: string;
  readonly registryEntry: string;
}

/** Ask the gateway at `socket` for a RAM disk of `sectors` 512-byte sectors; its lease, or why there is none. */
export async function attachRamDisk(
  socket: string,
  sectors: number,
  deadlineMs: number,
): Promise<Result<RamDiskLease, string | RamUnavailable>> {
  const connection = connect(socket);
  const next = answers(connection);
  connection.on('connect', () => connection.write(`${JSON.stringify({ op: RAM_LEASE_OP, sectors })}\n`));
  const granted = readGrant(await next(deadlineMs), socket, deadlineMs);
  if (!granted.ok) {
    connection.destroy();
    return granted;
  }
  // The open connection is the lease; it never keeps this process alive on its own.
  connection.unref();
  return { ok: true, value: heldLease(connection, next, granted.value, deadlineMs) };
}

/** The connection's answer lines, one per call, each awaited for at most `deadlineMs`. */
function answers(connection: Socket): (deadlineMs: number) => Promise<Next> {
  let received = '';
  let gone: Next | null = null;
  let waiting: ((next: Next) => void) | null = null;
  const deliver = () => {
    const wake = waiting;
    if (wake === null) return;
    const newline = received.indexOf('\n');
    let next = gone;
    if (newline >= 0) {
      next = { kind: 'line', line: received.slice(0, newline) };
      received = received.slice(newline + 1);
    } else if (gone !== null && received !== '') {
      next = { kind: 'line', line: received };
      received = '';
    }
    if (next === null) return;
    waiting = null;
    wake(next);
  };
  connection.on('data', (chunk: Buffer) => {
    received += chunk.toString('utf8');
    if (received.length <= ANSWER_BYTES_MAX) return deliver();
    received = '';
    connection.destroy(new Error(`the gateway's answer exceeds ${ANSWER_BYTES_MAX} bytes`));
  });
  connection.on('error', (error) => {
    gone ??= { kind: 'failed', error };
    deliver();
  });
  connection.on('close', () => {
    gone ??= { kind: 'closed' };
    deliver();
  });
  return (deadlineMs) =>
    new Promise((resolve) => {
      const timer = setTimeout(() => {
        waiting = null;
        resolve({ kind: 'late' });
      }, deadlineMs);
      waiting = (next) => {
        clearTimeout(timer);
        resolve(next);
      };
      deliver();
    });
}

/** The lease the attach answer grants, or why it grants none. */
function readGrant(next: Next, socket: string, deadlineMs: number): Result<Granted, string | RamUnavailable> {
  switch (next.kind) {
    case 'late':
      return {
        ok: false,
        error: `the gateway answered no ram-lease within ${deadlineMs} ms; this client closed the lease, so the gateway detaches any disk its attach still makes`,
      };
    case 'failed':
      return {
        ok: false,
        error:
          'code' in next.error && (next.error.code === 'ENOENT' || next.error.code === 'ECONNREFUSED')
            ? {
                kind: 'ram-unavailable',
                reason: 'gateway-absent',
                detail: `gateway ${socket} is unavailable: ${next.error.message}`,
              }
            : `gateway ram-lease I/O failed: ${next.error.message}`,
      };
    case 'closed':
      return { ok: false, error: 'the gateway closed ram-lease without an answer' };
    case 'line':
      return readAnswer(next.line);
  }
}

/** The fields of one JSON object answer line, or null for anything else. */
function fieldsOf(line: string): Readonly<Record<string, unknown>> | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  return typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)
    ? Object.fromEntries(Object.entries(parsed))
    : null;
}

/** The whole disk and registry entry a `ram-lease` answer names, or why the answer names none. */
function readAnswer(line: string): Result<Granted, string | RamUnavailable> {
  const fields = fieldsOf(line);
  if (fields === null) {
    return { ok: false, error: `the gateway answered ram-lease with something that is not an answer: ${line}` };
  }
  if (fields.ok !== true) {
    const why = `${String(fields.code ?? 'refused')}: ${String(fields.error ?? '')}`;
    return {
      ok: false,
      error: String(fields.error ?? '').endsWith('unknown gateway control operation')
        ? { kind: 'ram-unavailable', reason: 'gateway-unsupported', detail: `gateway predates ram-lease (${why})` }
        : `the gateway refused ram-lease (${why})`,
    };
  }
  const ramDisk = fields.ramDisk;
  const device =
    typeof ramDisk === 'object' && ramDisk !== null && 'device' in ramDisk && typeof ramDisk.device === 'string'
      ? ramDisk.device
      : null;
  const registryEntry =
    typeof ramDisk === 'object' &&
    ramDisk !== null &&
    'registryEntry' in ramDisk &&
    typeof ramDisk.registryEntry === 'string'
      ? ramDisk.registryEntry
      : null;
  return device !== null && WHOLE_DISK.test(device) && registryEntry !== null && REGISTRY_ENTRY.test(registryEntry)
    ? { ok: true, value: { device, registryEntry } }
    : { ok: false, error: `the gateway's ram-lease answer names no whole disk and registry entry: ${line}` };
}

/** The lease `connection` holds on `disk`; each end is one line, half-closed, whose answer is awaited `deadlineMs`. */
function heldLease(
  connection: Socket,
  next: (deadlineMs: number) => Promise<Next>,
  disk: Granted,
  deadlineMs: number,
): RamDiskLease {
  let ended: 'keep' | 'release' | null = null;
  const end = async (how: 'keep' | 'release'): Promise<Result<string, string>> => {
    if (ended !== null) {
      return {
        ok: false,
        error: `the ram-lease of ${disk.device} already ended at its ${ended}; nothing is known of a detach`,
      };
    }
    ended = how;
    connection.end(`${JSON.stringify({ end: how })}\n`);
    const answer = await next(deadlineMs);
    connection.destroy();
    if (answer.kind === 'line') return { ok: true, value: answer.line };
    const why =
      answer.kind === 'late'
        ? `none within ${deadlineMs} ms`
        : answer.kind === 'failed'
          ? answer.error.message
          : 'it closed the connection';
    return {
      ok: false,
      error: `the gateway answered no ${how} of the ram-lease of ${disk.device} (${why}): ${
        how === 'keep'
          ? 'nothing is known: the gateway may have handed the disk over or left it to its next startup scan; detach it by its entry'
          : 'the lease is lost, so nothing is known of a detach'
      }`,
    };
  };
  return {
    device: disk.device,
    registryEntry: disk.registryEntry,
    async keep() {
      const answer = await end('keep');
      if (!answer.ok) return answer;
      return fieldsOf(answer.value)?.ok === true
        ? { ok: true, value: undefined }
        : { ok: false, error: `the gateway refused to keep the ram-lease of ${disk.device}: ${answer.value}` };
    },
    async release() {
      const answer = await end('release');
      if (!answer.ok) return answer;
      const output = readDetachAnswer(answer.value);
      return output === null
        ? {
            ok: false,
            error: `the gateway's release of the ram-lease of ${disk.device} proves no detach outcome: ${answer.value}`,
          }
        : { ok: true, value: output };
    },
  };
}
