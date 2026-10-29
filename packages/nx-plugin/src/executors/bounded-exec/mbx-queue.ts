import typia from 'typia';
import { type HangDiagnosticHost, runProbe } from './hang-diagnostics.js';
import { CONTROL_MS } from './lease-wait.js';

/** The additive projection of mbx stats --scheduler --json --activity that the admission observer consumes. */
interface SchedulerStats {
  version: 2;
  scheduler: {
    activity: {
      kind: string;
      pid: number | null;
      ticket: number | null;
      queue_position: number | null;
      live: boolean | null;
    }[];
  };
}
const validateStats = typia.createValidate<SchedulerStats>();

export interface MbxQueuedRequest {
  readonly pid: number;
  readonly ticket: number;
  readonly queuePosition: number;
}

export type MbxQueueSnapshot =
  | { readonly ok: true; readonly requests: readonly MbxQueuedRequest[]; readonly diagnostics?: readonly string[] }
  | { readonly ok: false; readonly detail: string };

/** One observation, including all scoped process reads, has this bounded control-plane budget. */
export const MBX_QUEUE_SNAPSHOT_MS = CONTROL_MS;
export const MBX_QUEUE_POLL_MS = 1_000;

interface ProcessIdentity {
  readonly pid: number;
  readonly ppid: number;
  readonly pgid: number;
  readonly stat: string;
}

/**
 * Read only the public pool snapshot and the listed waiters' process identities/ancestors. A live member of the task's
 * process group, or its live descendant, corroborates a PID; a command name never does. No host process census is used.
 * The caller supplies the leaf's environment so stats observes the same machine-wide pool as the leaf.
 */
export async function snapshotMbxQueue(
  rootPid: number,
  environment: NodeJS.ProcessEnv,
  signal: AbortSignal,
  host: Pick<HangDiagnosticHost, 'probe'> = {
    probe: (file, args, timeoutMs, abort) => runProbe(file, args, timeoutMs, abort, environment),
  },
  now: () => number = Date.now,
): Promise<MbxQueueSnapshot> {
  const endsAt = now() + MBX_QUEUE_SNAPSHOT_MS;
  const probe = async (file: string, args: readonly string[], emptyAllowed = false) => {
    const remaining = endsAt - now();
    if (remaining <= 0 || signal.aborted) return { ok: false, detail: 'admission snapshot deadline reached' } as const;
    const result = await host.probe(file, args, remaining, signal);
    const command = `${file} ${args.join(' ')}`;
    if (result.status !== 'exited') {
      return { ok: false, detail: `${command}: ${result.status}: ${result.detail}; stderr=${result.stderr}` } as const;
    }
    if (result.code !== 0 && !(emptyAllowed && result.code === 1 && result.stdout === '' && result.stderr === '')) {
      return { ok: false, detail: `${command}: exit=${result.code}; stderr=${result.stderr}` } as const;
    }
    if (result.stderr.trim() !== '') return { ok: false, detail: `${command}: stderr=${result.stderr}` } as const;
    return { ok: true, stdout: result.stdout } as const;
  };
  const stats = await probe('mbx', ['stats', '--scheduler', '--json', '--activity']);
  if (!stats.ok) return stats;
  let wire: unknown;
  try {
    wire = JSON.parse(stats.stdout);
  } catch (error) {
    return { ok: false, detail: `mbx stats --scheduler --json --activity: invalid JSON: ${String(error)}` };
  }
  const validated = validateStats(wire);
  if (!validated.success) {
    return {
      ok: false,
      detail: `mbx stats --scheduler --json --activity: invalid scheduler.activity: ${JSON.stringify(validated.errors)}`,
    };
  }
  const waiters = validated.data.scheduler.activity.filter((row) => row.kind === 'waiter' && row.live !== false);
  const diagnostics: string[] = [];
  const candidates = waiters.flatMap((row) => {
    const pid = row.pid;
    if (pid === null || !Number.isSafeInteger(pid) || pid < 1) {
      diagnostics.push(
        'mbx stats: pending waiter has no valid PID; task membership unavailable, not queue or admission evidence',
      );
      return [];
    }
    return [{ ...row, pid }];
  });
  if (candidates.length === 0) return { ok: true, requests: [], ...(diagnostics.length === 0 ? {} : { diagnostics }) };
  const table = new Map<number, ProcessIdentity>();
  const inspected = new Set<number>();
  let pending = [...new Set([rootPid, ...candidates.map((row) => row.pid)])];
  while (pending.length > 0) {
    const result = await probe('ps', ['-o', 'pid=,ppid=,pgid=,stat=', '-p', pending.join(',')], true);
    if (!result.ok) return result;
    for (const pid of pending) inspected.add(pid);
    for (const line of result.stdout.split('\n').filter((row) => row.trim() !== '')) {
      const match = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s*$/.exec(line);
      if (match === null) return { ok: false, detail: `admission snapshot: invalid scoped ps row: ${line}` };
      const [, pid, ppid, pgid, stat] = match;
      if (pid === undefined || ppid === undefined || pgid === undefined || stat === undefined) {
        return { ok: false, detail: `admission snapshot: incomplete scoped ps row: ${line}` };
      }
      const entry = { pid: Number(pid), ppid: Number(ppid), pgid: Number(pgid), stat };
      if (!pending.includes(entry.pid))
        return { ok: false, detail: `admission snapshot: unexpected ps pid=${entry.pid}` };
      table.set(entry.pid, entry);
    }
    pending = [
      ...new Set(
        [...table.values()]
          .filter(
            (entry) =>
              !entry.stat.startsWith('Z') &&
              entry.pgid !== rootPid &&
              entry.pid !== rootPid &&
              entry.ppid > 1 &&
              !inspected.has(entry.ppid),
          )
          .map((entry) => entry.ppid),
      ),
    ];
  }
  const belongs = (pid: number): boolean => {
    const seen = new Set<number>();
    for (let entry = table.get(pid); entry !== undefined && !seen.has(entry.pid); entry = table.get(entry.ppid)) {
      if (entry.stat.startsWith('Z')) return false;
      if (entry.pid === rootPid || entry.pgid === rootPid) return true;
      seen.add(entry.pid);
    }
    return false;
  };
  const requests: MbxQueuedRequest[] = [];
  for (const row of candidates) {
    if (!belongs(row.pid)) continue;
    if (row.live === null)
      return {
        ok: false,
        detail: `mbx stats --scheduler --json --activity: waiter pid=${row.pid} live status unavailable`,
      };
    if (
      row.ticket === null ||
      !Number.isSafeInteger(row.ticket) ||
      row.ticket < 0 ||
      row.queue_position === null ||
      !Number.isSafeInteger(row.queue_position) ||
      row.queue_position < 1
    ) {
      return {
        ok: false,
        detail: `mbx stats --scheduler --json --activity: live waiter pid=${row.pid} lacks ticket/queue position`,
      };
    }
    requests.push({ pid: row.pid, ticket: row.ticket, queuePosition: row.queue_position });
  }
  return { ok: true, requests, ...(diagnostics.length === 0 ? {} : { diagnostics }) };
}
