import { connect } from 'node:net';

/**
 * The client half of cowshed's host disk-lifecycle lease (specs/cowshed/05_gateway.md,
 * "Disk-lifecycle lease"), for the RAM temp volume's disk commands.
 *
 * An attach spends most of its time in StorageKit's `syncAllDisks`, which does not finish while
 * the mount table keeps changing, so the cowshed gateway schedules every disk tool on the host in
 * two classes that never overlap. This module's `hdiutil` and `diskutil` calls take the same
 * lease cowshed's own do, when the gateway is there to ask. The lease spaces commands out; it
 * never decides whether one runs: without a grant, the command runs unleased.
 */

/** The cowshed gateway's control socket, at the store root every peer agrees on. */
export const GATEWAY_SOCKET = '/private/cowshed/store/gateway.sock';

export type DiskClass = 'storage' | 'namespace';

/**
 * The class each disk tool draws on, by absolute path: every `diskutil` verb, `hdiutil` and
 * `newfs_apfs` change the set of disks through `storagekitd`; `mount_apfs` and `umount` change the
 * mount table. Every other program takes no lease.
 */
const CLASS_BY_PROGRAM: Record<string, DiskClass> = {
  '/usr/sbin/diskutil': 'storage',
  '/usr/bin/hdiutil': 'storage',
  '/System/Library/Filesystems/apfs.fs/Contents/Resources/newfs_apfs': 'storage',
  '/sbin/mount_apfs': 'namespace',
  '/sbin/umount': 'namespace',
};

export function diskClassOf(file: string): DiskClass | null {
  return Object.hasOwn(CLASS_BY_PROGRAM, file) ? CLASS_BY_PROGRAM[file] : null;
}

export interface LeaseBounds {
  /** For the `queued` answer a lease-scheduling gateway sends at once. */
  ackMs: number;
  /** For any answer once the request is half-closed. */
  afterCloseMs: number;
  /** For the grant, once queued. */
  grantMs: number;
}

/** A granted lease, held until released, or why the command runs without one. */
export type LeaseOutcome = { granted: true; release: () => void } | { granted: false; reason: string; absent: boolean };

/**
 * Ask the gateway at `socket` for a `diskClass` lease for `command`: one request line, no
 * half-close; `queued` comes at once and `granted` when the command may run, and closing the
 * connection releases the lease. A gateway that predates leases says nothing until it reads EOF,
 * so a request not queued within `ackMs` is half-closed and that gateway's refusal read.
 */
export function takeDiskLease(
  socket: string,
  diskClass: DiskClass,
  command: string,
  bounds: LeaseBounds,
): Promise<LeaseOutcome> {
  // `Promise.withResolvers` would read better but needs lib es2024; this package inherits lib
  // es2022 from tsconfig.base.json.
  let resolve!: (outcome: LeaseOutcome) => void;
  const promise = new Promise<LeaseOutcome>((settled) => {
    resolve = settled;
  });
  const connection = connect(socket);
  let state: 'connecting' | 'asked' | 'half-closed' | 'queued' | 'settled' = 'connecting';
  let buffered = '';
  let timer: NodeJS.Timeout | undefined;
  const settle = (outcome: LeaseOutcome) => {
    if (state === 'settled') {
      return;
    }
    state = 'settled';
    clearTimeout(timer);
    if (!outcome.granted) {
      connection.destroy();
    }
    resolve(outcome);
  };
  const unleased = (reason: string, absent = false) => settle({ granted: false, reason, absent });
  const wait = (ms: number, onExpiry: () => void) => {
    clearTimeout(timer);
    timer = setTimeout(onExpiry, ms);
  };
  const answer = (line: string) => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      unleased(`the gateway answered something that is not an answer: ${line}`);
      return;
    }
    const field = (key: string): unknown =>
      typeof parsed === 'object' && parsed !== null ? Reflect.get(parsed, key) : undefined;
    const lease = field('lease');
    if (field('ok') !== true) {
      const why = `${String(field('code') ?? 'refused')}: ${String(field('error') ?? '')}`;
      unleased(
        state === 'half-closed'
          ? `the gateway predates disk leases (${why}); restart it with \`cowshed setup\``
          : `the gateway refused the lease (${why})`,
      );
    } else if (state === 'half-closed') {
      unleased('the gateway did not queue the request in time; it is alive but overloaded');
    } else if (state === 'asked' && lease === 'queued') {
      state = 'queued';
      wait(bounds.grantMs, () => unleased(`the gateway granted nothing within ${bounds.grantMs} ms`));
    } else if (state === 'queued' && lease === 'granted') {
      settle({ granted: true, release: () => connection.destroy() });
    } else {
      unleased(`the gateway answered out of turn: ${line}`);
    }
  };
  connection.on('connect', () => {
    state = 'asked';
    connection.write(`${JSON.stringify({ op: 'disk-lease', class: diskClass, command })}\n`);
    wait(bounds.ackMs, () => {
      state = 'half-closed';
      connection.end();
      wait(bounds.afterCloseMs, () => unleased('the gateway answered nothing'));
    });
  });
  connection.on('data', (chunk: Buffer) => {
    buffered += chunk.toString('utf8');
    for (let newline = buffered.indexOf('\n'); newline !== -1; newline = buffered.indexOf('\n')) {
      const line = buffered.slice(0, newline);
      buffered = buffered.slice(newline + 1);
      answer(line);
    }
  });
  connection.on('error', (error) => {
    const absent = state === 'connecting';
    unleased(
      absent ? `the gateway's control socket ${socket} does not answer (${error.message})` : error.message,
      absent,
    );
  });
  connection.on('close', () => unleased('the gateway closed the lease connection'));
  return promise;
}
