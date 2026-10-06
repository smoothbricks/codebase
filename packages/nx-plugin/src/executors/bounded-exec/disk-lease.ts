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

/** A granted lease, held until released, or why the command runs without one. */
export type LeaseOutcome =
  | { granted: true; release: () => void }
  | { granted: false; cause: UnleasedCause; reason: string };

/** Ask the gateway at `socket` for a `diskClass` lease for `command` (`holdLease`). */
export async function takeDiskLease(
  socket: string,
  diskClass: DiskClass,
  command: string,
  bounds: LeaseBounds,
): Promise<LeaseOutcome> {
  const lease = await holdLease(socket, { op: 'disk-lease', class: diskClass, command }, 'disk leases', bounds);
  return lease.granted ? { granted: true, release: lease.release } : lease;
}
