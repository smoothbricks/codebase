import { connect } from 'node:net';
import type { RamUnavailable, Result } from './ram-temp.js';

/**
 * The client of the cowshed gateway's `ram-attach` (specs/cowshed/05_gateway.md, "RAM disks"). The RAM temp volume's
 * disk lives in the `diskimages-helper` that `hdiutil attach ram://<n>` forks. Forked inside a task's job, the helper is
 * that job's member, and the job's end retires it with the disk under every other job's lease on the volume. The helper
 * belongs to the gateway, which no job contains and which attaches the disk; no job's retirement reaches it.
 *
 * The client writes one line, `{"op":"ram-attach","sectors":<n>}`, and half-closes; the gateway answers one line,
 * `{"ok":true,"ramDisk":{"device":"/dev/diskN"}}` or a refusal. A client that stops waiting at its deadline leaves the
 * gateway's attach running: a disk it makes then is a stray, which the volume's next creator detaches.
 */

export const RAM_ATTACH_OP = 'ram-attach';

const WHOLE_DISK = /^\/dev\/disk\d+$/;

/** Ask the gateway at `socket` for a RAM disk of `sectors` 512-byte sectors; its whole disk, or why there is none. */
export function attachRamDisk(
  socket: string,
  sectors: number,
  deadlineMs: number,
): Promise<Result<string, string | RamUnavailable>> {
  let resolve!: (outcome: Result<string, string | RamUnavailable>) => void;
  const promise = new Promise<Result<string, string | RamUnavailable>>((settled) => {
    resolve = settled;
  });
  const connection = connect(socket);
  let answer = '';
  let settled = false;
  const settle = (outcome: Result<string, string | RamUnavailable>) => {
    if (settled) {
      return;
    }
    settled = true;
    clearTimeout(timer);
    connection.destroy();
    resolve(outcome);
  };
  const timer = setTimeout(
    () =>
      settle({
        ok: false,
        error: `the gateway answered no ram-attach within ${deadlineMs} ms; its attach keeps running, and a disk it makes is a stray the next creator detaches`,
      }),
    deadlineMs,
  );
  connection.on('connect', () => connection.end(`${JSON.stringify({ op: RAM_ATTACH_OP, sectors })}\n`));
  connection.on('data', (chunk: Buffer) => {
    answer += chunk.toString('utf8');
  });
  connection.on('error', (error) =>
    settle({
      ok: false,
      error:
        'code' in error && (error.code === 'ENOENT' || error.code === 'ECONNREFUSED')
          ? {
              kind: 'ram-unavailable',
              reason: 'gateway-absent',
              detail: `gateway ${socket} is unavailable: ${error.message}`,
            }
          : `gateway ram-attach I/O failed: ${error.message}`,
    }),
  );
  connection.on('close', () => settle(readAnswer(answer)));
  return promise;
}

/** The whole disk a `ram-attach` answer names, or why the answer names none. */
function readAnswer(text: string): Result<string, string | RamUnavailable> {
  const line = text.split('\n', 1)[0] ?? '';
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return { ok: false, error: `the gateway answered ram-attach with something that is not an answer: ${line}` };
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { ok: false, error: `the gateway answered ram-attach with something that is not an answer: ${line}` };
  }
  const fields: Readonly<Record<string, unknown>> = Object.fromEntries(Object.entries(parsed));
  if (fields.ok !== true) {
    const why = `${String(fields.code ?? 'refused')}: ${String(fields.error ?? '')}`;
    return {
      ok: false,
      error: String(fields.error ?? '').endsWith('unknown gateway control operation')
        ? { kind: 'ram-unavailable', reason: 'gateway-unsupported', detail: `gateway predates ram-attach (${why})` }
        : `the gateway refused ram-attach (${why})`,
    };
  }
  const ramDisk = fields.ramDisk;
  const device =
    typeof ramDisk === 'object' && ramDisk !== null && 'device' in ramDisk && typeof ramDisk.device === 'string'
      ? ramDisk.device
      : null;
  return device !== null && WHOLE_DISK.test(device)
    ? { ok: true, value: device }
    : { ok: false, error: `the gateway's ram-attach answer names no whole disk: ${line}` };
}
