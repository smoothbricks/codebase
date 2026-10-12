import { appendFileSync } from 'node:fs';
import { holdLease, type LeaseBounds, type UnleasedCause } from './gateway-lease.js';

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

export type DiskClass = 'storage' | 'namespace';

/**
 * The class each disk tool draws on, by absolute path and, for `diskutil`, its verb.
 * `hdiutil`, `newfs_apfs`, and every `diskutil` verb except `renameVolume` change the set of
 * disks through `storagekitd`. `mount_apfs` and `umount` change the mount table. `diskutil
 * renameVolume` changes neither, so it takes no lease (specs/cowshed/05_gateway.md,
 * "Disk-lifecycle lease"). Every other program takes no lease.
 */
const CLASS_BY_PROGRAM: Record<string, DiskClass> = {
  '/usr/sbin/diskutil': 'storage',
  '/usr/bin/hdiutil': 'storage',
  '/System/Library/Filesystems/apfs.fs/Contents/Resources/newfs_apfs': 'storage',
  '/sbin/mount_apfs': 'namespace',
  '/sbin/umount': 'namespace',
};

export function diskClassOf(file: string, args: readonly string[] = []): DiskClass | null {
  if (file === '/usr/sbin/diskutil' && args[0] === 'renameVolume') {
    return null;
  }
  return Object.hasOwn(CLASS_BY_PROGRAM, file) ? CLASS_BY_PROGRAM[file] : null;
}

/** A granted lease, held until released, or why the command runs without one. */
export type LeaseOutcome =
  | { granted: true; release: () => void }
  | { granted: false; cause: UnleasedCause; reason: string };

/**
 * Names the file every lease wait of a test's process tree is appended to; unset outside the real-APFS lane. The
 * record exists so the real-APFS lane's wrapper can judge a test's code time without its lease waits
 * (specs/cowshed/08_testing.md, "Integration tests (real substrate)").
 */
export const LEASE_WAIT_RECORD_ENV = 'COWSHED_LEASE_WAIT_RECORD';

/**
 * A process's lease-wait record. `begin` appends the wait's `start` line and returns the function that appends its
 * `end` line once the wait is over.
 */
export interface WaitRecord {
  begin(diskClass: DiskClass): () => void;
}

/** Wall-clock microseconds since the Unix epoch: the clock every lease-wait stamp is read from. */
export function unixMicros(): number {
  return Math.round((performance.timeOrigin + performance.now()) * 1000);
}

/**
 * The lease-wait record at `path`. A wait is two lines: `start <pid> <seq> <unix-µs> <class>` before the client asks,
 * and `end <pid> <seq> <unix-µs>` once it is over, granted or not. `seq` counts this record's waits from 0. Each line is
 * one append, so concurrent processes' lines never interleave. A record that cannot be written is said once per process
 * and never fails a wait: its waits then count as the test's code time, which can only fail the bound.
 */
export function waitRecord(path: string): WaitRecord {
  let next = 0;
  let said = false;
  const append = (line: string): void => {
    try {
      appendFileSync(path, line, { mode: 0o600 });
    } catch (error) {
      if (!said) {
        said = true;
        const message = error instanceof Error ? error.message : String(error);
        process.stderr.write(
          `cowshed: disk-lease wait record ${path} is unwritable (${message}); this process's lease waits count as its test's code time\n`,
        );
      }
    }
  };
  return {
    begin(diskClass) {
      const seq = next;
      next += 1;
      append(`start ${process.pid} ${seq} ${unixMicros()} ${diskClass}\n`);
      return () => append(`end ${process.pid} ${seq} ${unixMicros()}\n`);
    },
  };
}

/** This process's record, read from the environment on first use: `undefined` until then, `null` when unset. */
let environmentRecord: WaitRecord | null | undefined;

function leaseWaitRecord(): WaitRecord | null {
  if (environmentRecord === undefined) {
    const path = process.env[LEASE_WAIT_RECORD_ENV];
    environmentRecord = path === undefined ? null : waitRecord(path);
  }
  return environmentRecord;
}

/**
 * Ask the gateway at `socket` for a `diskClass` lease for `command` (`holdLease`). The wait is appended to this
 * process's lease-wait record when the environment names one, and its end is appended however the ask ends.
 */
export async function takeDiskLease(
  socket: string,
  diskClass: DiskClass,
  command: string,
  bounds: LeaseBounds,
): Promise<LeaseOutcome> {
  const endWait = leaseWaitRecord()?.begin(diskClass);
  try {
    const lease = await holdLease(socket, { op: 'disk-lease', class: diskClass, command }, 'disk leases', bounds);
    return lease.granted ? { granted: true, release: lease.release } : lease;
  } finally {
    endWait?.();
  }
}
