import { type ChildProcess, spawn } from 'node:child_process';
import { constants } from 'node:fs';
import { mkdtemp, open, readdir, readFile, rm, stat, statfs, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { diskClassOf, GATEWAY_SOCKET, takeDiskLease } from './disk-lease.js';

/**
 * One RAM-backed APFS volume per host user, shared by every test task that runs
 * on the host, so test temp files never reach the SSD.
 *
 * Measured on macOS 26.6:
 * - `hdiutil attach ram://<sectors>` allocates lazily: an attached 8 GiB volume
 *   costs ~13 MB until written, so the capacity is a ceiling, not a reservation.
 * - Deleting files does not hand pages back (no TRIM through the RAM device):
 *   the volume's cost is its high-water mark until it is detached. That is why
 *   the volume is detached as soon as no task holds a lease, rather than kept.
 * - The memory is pageable (it belongs to diskimages-helper), so it reaches the
 *   SSD only through swap under memory pressure.
 * - A RAM disk belongs to the legacy DiskImages framework: attaching one starts
 *   a `diskimages-helper` that exits on detach, with no AppleDiskImageDevice and
 *   no `diskimagesiod`, so it spends none of the AppleDiskImages2 attach budget
 *   that cowshed's images and tests draw on (specs/cowshed/01_storage.md, "How
 *   the APFS host degrades"; measured 2026-10-05).
 * - Attach + format + mount costs ~2 s and is DiskArbitration traffic, so the
 *   volume is created once, lazily, and every task takes a subdirectory.
 * - Only DiskArbitration's own mountpoint (`/Volumes/<volume name>`) is safe to
 *   ask for: `diskutil mount -mountPoint <dir>` was refused as not privileged
 *   (0xf8da0009), and storagekitd escalated it to an administrator dialog whose
 *   wait blocked every diskutil on the host. A default mount raised no authd
 *   request (log show, 2026-10-04).
 * - Without authorization the volume mounts only `noowners`: launchd refuses a
 *   plist from it ("Caller specified a plist with bad ownership/permissions"),
 *   so a test that bootstraps a launchd job keeps the plist outside TMPDIR.
 *
 * Every step runs under one kernel lock (`O_EXLOCK` on a file in /private/tmp),
 * held only while the volume or the lease set changes, and dropped by the
 * kernel when the holder dies. A lease is a directory named after the task
 * process's pid; a lease whose pid is gone is reclaimed by the next task that
 * takes the lock, and the volume is detached when the last live lease ends.
 *
 * The volume is formatted and mounted under a staging name, marked, and only
 * then renamed to its own name: `diskutil rename` moves a mounted volume's
 * /Volumes mountpoint with it (measured 2026-10-06). A creator that dies part
 * way leaves debris at the staging name, never an unmarked volume at the
 * mountpoint, which DiskArbitration would make every later volume dodge as
 * "<name> 1". The host's own image list is the only record of what was
 * attached: before creating, a task detaches every RAM disk of this user and
 * this volume's exact size that is mounted nowhere but this volume's names,
 * because a creator killed or deadlined mid-step leaves one behind — measured
 * 2026-10-06: a deadlined `hdiutil attach` whose diskimages-helper finished the
 * attach after `hdiutil` was killed, and a volume mounted with no marker. A
 * disk with a lease directory on it is never detached; its error names it.
 *
 * An image attached from a file below a lease belongs to whoever attached it:
 * cowshed's real-APFS fixtures release theirs under the production per-image
 * lease, from scratch roots outside TMPDIR (specs/cowshed/08_testing.md). This
 * module never detaches one, and never deletes through a mount below a lease.
 * A dead lease that still backs an image or holds a mount is kept, with the
 * volume under it, and every task that takes a lease names it. Every disk
 * child runs under a deadline, in its own process group so the deadline also
 * reaches the helper it spawned, so a wedged DiskArbitration fails the task
 * instead of every task queued on the lock.
 *
 * A cowshed sandbox denies writes to /private/tmp (EPERM on the lock), and
 * cannot attach disk images at all; there the task keeps the workspace's own
 * TMPDIR and says so once on stderr.
 */

/** macOS `open(2)` flag: take an exclusive flock atomically with the open. Absent from `fs.constants`. */
const O_EXLOCK = 0x20;
const SECTOR_BYTES = 512;
const MIB = 1024 * 1024;
const GIB = 1024 * MIB;
/**
 * The volume's ceiling. Pages are allocated as they are written and returned when the
 * volume detaches, so this bounds what a runaway suite can take, not what a gate costs.
 * Measured 2026-10-04: a full parallel smoothbricks `lint test build host-controller-test`
 * gate peaked at 1.61 GiB used with 18 concurrent leases; 8 GiB is five times that. That
 * gate also staged cowshed's real-APFS fixture images on the volume, which now stay under
 * /private/tmp, so the peak is an upper bound.
 */
export const RAM_TEMP_CAPACITY_BYTES = 8 * GIB;
/** A failed task with less than this free on the volume is reported as having run out of space. */
const FULL_FREE_BYTES = 64 * MIB;
/** `<pid>-<mkdtemp suffix>`: the pid says whether the lease's task is alive. */
const LEASE_NAME = /^(\d+)-[A-Za-z0-9]{6}$/;
const MARKER_FILE = '.smoo-ram';

export interface CommandOutput {
  status: number;
  stdout: string;
  stderr: string;
}

/** The host programs the volume lifecycle runs. A seam so the state machine is testable without DiskArbitration. */
export interface HostCommands {
  run(file: string, args: readonly string[]): Promise<CommandOutput>;
}

export interface RamTempPaths {
  /** Where DiskArbitration mounts the volume: `/Volumes/<volumeName>`. Short for sun_path (104 bytes). */
  mountpoint: string;
  lockFile: string;
  /** APFS volume name; names the mountpoint. */
  volumeName: string;
  /** The volume's name while it is formatted and marked, before it is renamed to `volumeName`. */
  stagingName: string;
  /** Login name `hdiutil info` reports as the attaching user of this module's RAM disks. */
  user: string;
}

export function ramTempPaths(user: { uid: number; username: string }): RamTempPaths {
  const volumeName = `smoo-ram-${user.uid}`;
  return {
    mountpoint: join('/Volumes', volumeName),
    lockFile: `/private/tmp/${volumeName}.lock`,
    volumeName,
    stagingName: `${volumeName}-new`,
    user: user.username,
  };
}

/** An attached RAM disk, as `hdiutil info` lists it. */
export interface RamDisk {
  /** The whole disk (`/dev/diskN`): detaching it removes its container and volumes. */
  physical: string;
  sectors: number;
  /** Login name of the user that attached it. */
  user: string;
  /** Where its volumes are mounted, if anywhere. */
  mountpoints: string[];
}

export interface RamTempLease {
  /** The task's TMPDIR: a directory on the volume that only this task uses. */
  directory: string;
  mountpoint: string;
  capacityBytes: number;
}

export type RamTempAcquisition =
  /** `held`: dead leases that could not be reclaimed, each with what still holds it. */
  | { kind: 'leased'; lease: RamTempLease; held: string[] }
  /** Inside a cowshed sandbox: /private/tmp is not writable and no image can attach. */
  | { kind: 'sandboxed'; detail: string };

/** Removing a lease: done, or refused because something attached below it still needs its files. */
type LeaseRemoval = { kind: 'removed' } | { kind: 'held'; by: string[] };

/** The leases the volume still carries after dead ones are reclaimed. */
interface Survivors {
  live: number;
  /** Dead leases kept because something attached below them; described for the task's output. */
  held: string[];
}

/** A host command or filesystem step failed while the volume was being created, mounted or detached. */
type ProvisionFailed = { kind: 'provision-failed'; step: string; detail: string };

export type RamTempError =
  | ProvisionFailed
  /** A failed task left the volume (nearly) full: the likely cause of its failure. */
  | { kind: 'volume-full'; mountpoint: string; capacityBytes: number; freeBytes: number };

export type Result<T, E> = { ok: true; value: T } | { ok: false; error: E };

export function describeRamTempError(error: RamTempError): string {
  switch (error.kind) {
    case 'provision-failed':
      return `RAM temp volume: ${error.step} failed: ${error.detail}`;
    case 'volume-full':
      return `RAM temp volume ${error.mountpoint} is full: ${Math.round(error.freeBytes / MIB)} MiB free of ${Math.round(error.capacityBytes / MIB)} MiB; raise RAM_TEMP_CAPACITY_BYTES in @smoothbricks/nx-plugin`;
  }
}

export class RamTempVolume {
  /** The RAM disk's size, which with the attaching user identifies this module's disks on the host. */
  private readonly sectors: number;

  constructor(
    private readonly paths: RamTempPaths,
    private readonly capacityBytes: number,
    private readonly commands: HostCommands,
    private readonly pid: number = process.pid,
    private readonly isAlive: (pid: number) => boolean = processIsAlive,
  ) {
    this.sectors = Math.ceil(capacityBytes / SECTOR_BYTES);
  }

  /** Take a lease: create the volume if no task holds one, and a private directory on it. */
  async acquire(): Promise<Result<RamTempAcquisition, RamTempError>> {
    const lock = await this.lock();
    if (!lock.ok) {
      return lock.error.code === 'EPERM'
        ? { ok: true, value: { kind: 'sandboxed', detail: `${this.paths.lockFile}: ${lock.error.message}` } }
        : failed('lock', lock.error.message);
    }
    try {
      const mounted = await this.mountedCapacity();
      let held: string[] = [];
      if (mounted === null) {
        const created = await this.create();
        if (!created.ok) {
          return created;
        }
      } else {
        const survivors = await this.reclaimDeadLeases();
        if (!survivors.ok) {
          return survivors;
        }
        held = survivors.value.held;
      }
      const directory = await mkdtemp(join(this.paths.mountpoint, `${this.pid}-`));
      return {
        ok: true,
        value: {
          kind: 'leased',
          lease: { directory, mountpoint: this.paths.mountpoint, capacityBytes: mounted ?? this.capacityBytes },
          held,
        },
      };
    } catch (error) {
      return failed('lease', errorText(error));
    } finally {
      await lock.value.close();
    }
  }

  /** End a lease; the last live lease detaches the volume, returning its RAM. */
  async release(lease: RamTempLease): Promise<Result<void, RamTempError>> {
    const lock = await this.lock();
    if (!lock.ok) {
      return failed('lock', lock.error.message);
    }
    try {
      if ((await this.mountedCapacity()) === null) {
        return { ok: true, value: undefined };
      }
      const removed = await this.removeLease(lease.directory);
      if (!removed.ok) {
        return removed;
      }
      if (removed.value.kind === 'held') {
        return failed('release', holdingText(lease.directory, removed.value.by));
      }
      const survivors = await this.reclaimDeadLeases();
      if (!survivors.ok) {
        return survivors;
      }
      if (survivors.value.live > 0 || survivors.value.held.length > 0) {
        return { ok: true, value: undefined };
      }
      return await this.detach();
    } catch (error) {
      return failed('release', errorText(error));
    } finally {
      await lock.value.close();
    }
  }

  /** After a failed task: whether the volume ran out of space, which is then the error to report. */
  async fullness(lease: RamTempLease): Promise<RamTempError | null> {
    try {
      const fs = await statfs(lease.mountpoint);
      const freeBytes = fs.bavail * fs.bsize;
      return freeBytes < FULL_FREE_BYTES
        ? { kind: 'volume-full', mountpoint: lease.mountpoint, capacityBytes: lease.capacityBytes, freeBytes }
        : null;
    } catch {
      return null;
    }
  }

  private async lock(): Promise<Result<{ close(): Promise<void> }, { code: string | undefined; message: string }>> {
    try {
      const handle = await open(
        this.paths.lockFile,
        constants.O_RDWR | constants.O_CREAT | constants.O_NOFOLLOW | O_EXLOCK,
        0o600,
      );
      return { ok: true, value: handle };
    } catch (error) {
      return { ok: false, error: { code: errorCode(error), message: errorText(error) } };
    }
  }

  /** The volume's capacity when it is mounted at the mountpoint, identified by its marker; else null. */
  private async mountedCapacity(): Promise<number | null> {
    const marker = await readFile(join(this.paths.mountpoint, MARKER_FILE), 'utf8').catch(() => null);
    if (marker?.trim() !== this.paths.volumeName) {
      return null;
    }
    const [volume, parent] = await Promise.all([stat(this.paths.mountpoint), stat(join(this.paths.mountpoint, '..'))]);
    if (volume.dev === parent.dev) {
      return null;
    }
    const fs = await statfs(this.paths.mountpoint);
    return fs.blocks * fs.bsize;
  }

  /** Remove leases whose task is gone; what remains on the volume. */
  private async reclaimDeadLeases(): Promise<Result<Survivors, RamTempError>> {
    const survivors: Survivors = { live: 0, held: [] };
    for (const name of await readdir(this.paths.mountpoint)) {
      const match = LEASE_NAME.exec(name);
      if (!match) {
        continue;
      }
      if (this.isAlive(Number(match[1]))) {
        survivors.live += 1;
        continue;
      }
      const directory = join(this.paths.mountpoint, name);
      const removed = await this.removeLease(directory);
      if (!removed.ok) {
        return removed;
      }
      if (removed.value.kind === 'held') {
        survivors.held.push(holdingText(directory, removed.value.by));
      }
    }
    return { ok: true, value: survivors };
  }

  /**
   * Anything attached from below the lease keeps the lease. An image backed by a file there would
   * be left pinned to a deleted file, and detaching it here would race the owner that releases it
   * under its own lease. A mount there is backed elsewhere — a cowshed workspace adopted from a
   * fixture checkout, say — and `rm -r` would delete its contents through the mount.
   */
  private async removeLease(directory: string): Promise<Result<LeaseRemoval, RamTempError>> {
    const holders = await this.holders(`${directory}/`);
    if (!holders.ok) {
      return holders;
    }
    if (holders.value.length > 0) {
      return { ok: true, value: { kind: 'held', by: holders.value } };
    }
    await rm(directory, { recursive: true, force: true });
    return { ok: true, value: { kind: 'removed' } };
  }

  private async create(): Promise<Result<void, RamTempError>> {
    const strays = await this.reclaimStrays();
    if (!strays.ok) {
      return strays;
    }
    const attached = await this.commands.run('/usr/bin/hdiutil', ['attach', '-nomount', `ram://${this.sectors}`]);
    if (attached.status !== 0) {
      // A deadlined attach took its helper's process group down with it; one that attached
      // regardless is a stray the next creator detaches.
      return failed('hdiutil attach', commandText(attached));
    }
    const physical = /^(\/dev\/disk\d+)\b/.exec(attached.stdout)?.[1];
    if (!physical) {
      return failed('hdiutil attach', `no device in: ${attached.stdout.trim()}`);
    }
    const provisioned = await this.provision(physical);
    if (provisioned.ok) {
      return provisioned;
    }
    const detached = await this.commands.run('/usr/bin/hdiutil', ['detach', '-force', physical]);
    return detached.status === 0
      ? provisioned
      : failed(
          provisioned.error.step,
          `${provisioned.error.detail}; hdiutil detach -force ${physical} then failed (${commandText(detached)}), so it stays attached until the next creator reclaims it`,
        );
  }

  /**
   * Format, mount and mark the volume under its staging name, then rename it into place: the
   * volume reaches the mountpoint already marked, so no crash leaves it there unmarked.
   */
  private async provision(physical: string): Promise<Result<void, ProvisionFailed>> {
    const container = await this.commands.run('/usr/sbin/diskutil', ['apfs', 'createContainer', physical]);
    const containerDisk = operationDisk(container.stdout);
    if (container.status !== 0 || !containerDisk) {
      return failed('diskutil apfs createContainer', commandText(container));
    }
    const added = await this.commands.run('/usr/sbin/diskutil', [
      'apfs',
      'addVolume',
      containerDisk,
      'APFS',
      this.paths.stagingName,
      '-nomount',
    ]);
    const volume = operationDisk(added.stdout);
    if (added.status !== 0 || !volume) {
      return failed('diskutil apfs addVolume', commandText(added));
    }
    // No -mountPoint: see the module comment. DiskArbitration names the mountpoint after the
    // volume, and appends " 1" when a /Volumes entry already holds the name.
    const mounted = await this.commands.run('/usr/sbin/diskutil', ['mount', '-mountOptions', 'nobrowse', volume]);
    if (mounted.status !== 0) {
      return failed('diskutil mount', commandText(mounted));
    }
    const staging = join('/Volumes', this.paths.stagingName);
    const staged = await this.expectMounted(volume, staging, 'diskutil mount');
    if (!staged.ok) {
      return staged;
    }
    try {
      await writeFile(join(staging, MARKER_FILE), `${this.paths.volumeName}\n`);
    } catch (error) {
      return failed('marker', errorText(error));
    }
    const renamed = await this.commands.run('/usr/sbin/diskutil', ['rename', volume, this.paths.volumeName]);
    if (renamed.status !== 0) {
      return failed('diskutil rename', commandText(renamed));
    }
    return await this.expectMounted(volume, this.paths.mountpoint, 'diskutil rename');
  }

  private async expectMounted(volume: string, expected: string, step: string): Promise<Result<void, ProvisionFailed>> {
    const table = await this.commands.run('/sbin/mount', []);
    if (table.status !== 0) {
      return failed('mount (mount table)', commandText(table));
    }
    const point = new RegExp(`^/dev/${volume} on (.+) \\(`, 'm').exec(table.stdout)?.[1];
    return point === expected
      ? { ok: true, value: undefined }
      : failed(step, `${volume} mounted at ${point ?? 'nowhere'}, expected ${expected}`);
  }

  /** This module's RAM disks attached on the host: this user's, of this volume's exact size, mounted nowhere else. */
  private async attached(): Promise<Result<RamDisk[], ProvisionFailed>> {
    const info = await this.commands.run('/usr/bin/hdiutil', ['info']);
    if (info.status !== 0) {
      return failed('hdiutil info', commandText(info));
    }
    const ours = (point: string) => {
      // DiskArbitration's " 1" suffix: a volume that found its name taken.
      const name = point.replace(/ \d+$/, '');
      return name === this.paths.mountpoint || name === join('/Volumes', this.paths.stagingName);
    };
    return {
      ok: true,
      value: ramDisksIn(info.stdout).filter(
        (disk) => disk.user === this.paths.user && disk.sectors === this.sectors && disk.mountpoints.every(ours),
      ),
    };
  }

  /**
   * Detach what earlier creators left attached: no marked volume is mounted, so every RAM disk of
   * this volume is debris — unformatted, staged, or mounted without its marker. One carrying a
   * lease, or with anything attached below it, is still in use and stops the creation instead.
   */
  private async reclaimStrays(): Promise<Result<void, RamTempError>> {
    const strays = await this.attached();
    if (!strays.ok) {
      return strays;
    }
    for (const disk of strays.value) {
      for (const point of disk.mountpoints) {
        const leases = await readdir(point).then(
          (names) => ({ ok: true as const, names: names.filter((name) => LEASE_NAME.test(name)) }),
          (error: unknown) => ({ ok: false as const, detail: errorText(error) }),
        );
        if (!leases.ok) {
          return failed('reclaim', `${disk.physical} at ${point}: ${leases.detail}`);
        }
        if (leases.names.length > 0) {
          return failed(
            'reclaim',
            `${disk.physical} at ${point} has no marker but carries leases ${leases.names.join(', ')}; not detached`,
          );
        }
        const holders = await this.holders(`${point}/`);
        if (!holders.ok) {
          return holders;
        }
        if (holders.value.length > 0) {
          return failed('reclaim', holdingText(point, holders.value));
        }
      }
      const detached = await this.commands.run('/usr/bin/hdiutil', ['detach', '-force', disk.physical]);
      if (detached.status !== 0) {
        return failed(`hdiutil detach (stray ${disk.physical})`, commandText(detached));
      }
    }
    return { ok: true, value: undefined };
  }

  private async detach(): Promise<Result<void, RamTempError>> {
    // Every lease is gone, so nothing below the volume should be attached; whatever is would lose
    // its backing file to the detach.
    const holders = await this.holders(`${this.paths.mountpoint}/`);
    if (!holders.ok) {
      return holders;
    }
    if (holders.value.length > 0) {
      return failed('detach', holdingText(this.paths.mountpoint, holders.value));
    }
    const disks = await this.attached();
    if (!disks.ok) {
      return disks;
    }
    const disk = disks.value.find((candidate) => candidate.mountpoints.includes(this.paths.mountpoint));
    if (!disk) {
      return failed(
        'detach',
        `hdiutil info lists no RAM disk of ${this.paths.user} with ${this.sectors} sectors mounted at ${this.paths.mountpoint}`,
      );
    }
    let detached = await this.commands.run('/usr/bin/hdiutil', ['detach', disk.physical]);
    if (detached.status !== 0) {
      detached = await this.commands.run('/usr/bin/hdiutil', ['detach', '-force', disk.physical]);
    }
    if (detached.status !== 0) {
      return failed('hdiutil detach', commandText(detached));
    }
    // DiskArbitration removes the /Volumes directory it created when the volume goes.
    return { ok: true, value: undefined };
  }

  /**
   * What is attached from below `prefix`: images whose backing file is there, read from the I/O
   * Registry, and mounts. `hdiutil info` omits attached images while another image attaches or
   * detaches (measured by cowshed's scratch-root sweep), and tests attach images concurrently.
   */
  private async holders(prefix: string): Promise<Result<string[], RamTempError>> {
    const registry = await this.commands.run('/usr/sbin/ioreg', ['-r', '-c', 'AppleDiskImageDevice', '-l', '-w0']);
    if (registry.status !== 0) {
      return failed('ioreg (attached disk images)', commandText(registry));
    }
    const mounts = await this.commands.run('/sbin/mount', []);
    if (mounts.status !== 0) {
      return failed('mount (mount table)', commandText(mounts));
    }
    return {
      ok: true,
      value: [
        ...imagesBackedUnder(registry.stdout, prefix),
        ...mountsBelow(mounts.stdout, prefix).map((point) => `${point} (mounted)`),
      ],
    };
  }
}

/** `directory` stays, with the volume under it: something attached below it still reads its files. */
function holdingText(directory: string, holders: readonly string[]): string {
  return `${directory} is kept: ${holders.join(', ')} still attached from below it; detach through its owner`;
}

/**
 * Every image in `ioreg -r -c AppleDiskImageDevice -l -w0` output whose `DiskImageURL` names a
 * file under `prefix`, as `<backing file> (/dev/diskN)`. Each device starts a
 * `+-o AppleDiskImageDevice` block; the first `BSD Name` below it is the image's whole disk.
 */
export function imagesBackedUnder(registry: string, prefix: string): string[] {
  const images: string[] = [];
  for (const block of registry.split(/^\+-o AppleDiskImageDevice/m)) {
    const url = /"DiskImageURL" = "file:\/\/([^"]+)"/.exec(block)?.[1];
    const disk = /"BSD Name" = "(disk\d+)"/.exec(block)?.[1];
    const image = url === undefined ? undefined : decodeURIComponent(url);
    if (image?.startsWith(prefix) && disk !== undefined) {
      images.push(`${image} (/dev/${disk})`);
    }
  }
  return images;
}

/** Mount points in `mount` output (`<device> on <path> (<options>)`) that lie under `prefix`. */
export function mountsBelow(table: string, prefix: string): string[] {
  const points: string[] = [];
  for (const line of table.split('\n')) {
    const point = /^\S+ on (.+) \([^)]*\)$/.exec(line)?.[1];
    if (point?.startsWith(prefix)) {
      points.push(point);
    }
  }
  return points;
}

/** The disk `diskutil apfs` reports it created: "Disk from APFS operation: disk271". */
export function operationDisk(stdout: string): string | null {
  return /^Disk from APFS operation: (disk\d+(?:s\d+)?)$/m.exec(stdout)?.[1] ?? null;
}

/**
 * The RAM disks in `hdiutil info` output. Images are separated by a row of `=`; a RAM disk's
 * `image-path` is `ram://<sectors>`, and its entity rows are `<dev>\t<content hint>\t<mountpoint>`,
 * the whole disk first.
 */
export function ramDisksIn(info: string): RamDisk[] {
  const disks: RamDisk[] = [];
  for (const block of info.split(/^=+$/m)) {
    const sectors = /^image-path\s*:\s*ram:\/\/(\d+)\s*$/m.exec(block)?.[1];
    const user = /^mounting user\s*:\s*(\S+)\s*$/m.exec(block)?.[1];
    const entities = [...block.matchAll(/^(\/dev\/disk\d+(?:s\d+)*)\t[^\t\n]*(?:\t([^\n]*))?$/gm)];
    const physical = entities[0]?.[1];
    if (sectors === undefined || user === undefined || physical === undefined) {
      continue;
    }
    const mountpoints = entities.flatMap((entity) => {
      const point = entity[2]?.trimEnd();
      return point ? [point] : [];
    });
    disks.push({ physical, sectors: Number(sectors), user, mountpoints });
  }
  return disks;
}

/**
 * A disk child that DiskArbitration never answers would wedge the task, and every task queued
 * behind the lock, forever; measured: `diskutil mount` hung past 60 s while the host's
 * DiskArbitration was saturated. Cowshed bounds its disk children the same way.
 */
const HOST_COMMAND_DEADLINE_MS = 120_000;

/** Whether this process already said the cowshed gateway is not there to lease disk commands. */
let gatewayAbsenceSaid = false;

/**
 * Every disk tool runs under cowshed's host disk-lifecycle lease when the gateway answers
 * (`disk-lease.ts`): its attaches and mounts then never starve cowshed's, nor cowshed's them. The
 * lease wait comes before the deadline starts, and the lease is released once the command is
 * answered. Without a grant the command runs unleased and stderr says why; an absent gateway is
 * said once per process.
 */
export function createHostCommands(
  deadlineMs: number = HOST_COMMAND_DEADLINE_MS,
  gatewaySocket: string = GATEWAY_SOCKET,
): HostCommands {
  return {
    async run(file, args) {
      const diskClass = diskClassOf(file);
      if (diskClass === null) {
        return runBounded(file, args, deadlineMs);
      }
      const lease = await takeDiskLease(gatewaySocket, diskClass, [file, ...args].join(' '), {
        ackMs: 2_000,
        afterCloseMs: 5_000,
        grantMs: deadlineMs,
      });
      if (!lease.granted && !(lease.absent && gatewayAbsenceSaid)) {
        gatewayAbsenceSaid ||= lease.absent;
        process.stderr.write(`RAM temp volume: ${file} runs without cowshed's disk lease: ${lease.reason}\n`);
      }
      try {
        return await runBounded(file, args, deadlineMs);
      } finally {
        if (lease.granted) {
          lease.release();
        }
      }
    },
  };
}

function runBounded(file: string, args: readonly string[], deadlineMs: number): Promise<CommandOutput> {
  // `Promise.withResolvers` would read better but needs lib es2024; this package inherits lib
  // es2022 from tsconfig.base.json.
  let resolve!: (output: CommandOutput) => void;
  const promise = new Promise<CommandOutput>((settled) => {
    resolve = settled;
  });
  // `detached` leads a process group, so the deadline kills what the child spawned too:
  // `hdiutil attach` hands the attach to a diskimages-helper, and one that outlived its
  // killed `hdiutil` finished the attach later, an image nobody detached (2026-10-06).
  const child = spawn(file, [...args], { detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  child.stdout.on('data', (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));
  const text = (chunks: Buffer[]) => Buffer.concat(chunks).toString('utf8');
  // Answered without waiting for the pipes: a helper that escaped the group may hold them.
  const timer = setTimeout(() => {
    resolve({
      status: 1,
      stdout: text(stdout),
      stderr: `no answer within ${deadlineMs} ms; ${killGroup(child)}`,
    });
  }, deadlineMs);
  child.on('error', (error) => {
    clearTimeout(timer);
    resolve({ status: 1, stdout: text(stdout), stderr: error.message });
  });
  child.on('close', (code, signal) => {
    clearTimeout(timer);
    const error = text(stderr);
    resolve({
      status: code ?? 1,
      stdout: text(stdout),
      stderr: error === '' && signal !== null ? `killed by ${signal}` : error,
    });
  });
  return promise;
}

/** SIGKILL the child's whole process group; what happened, for the error. */
function killGroup(child: ChildProcess): string {
  if (child.pid === undefined) {
    return 'never started';
  }
  try {
    process.kill(-child.pid, 'SIGKILL');
    return `killed process group ${child.pid}`;
  } catch (error) {
    return `killing process group ${child.pid} failed: ${errorText(error)}`;
  }
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM: alive, owned by someone else.
    return errorCode(error) === 'EPERM';
  }
}

function failed<T>(step: string, detail: string): Result<T, ProvisionFailed> {
  return { ok: false, error: { kind: 'provision-failed', step, detail } };
}

function commandText(output: CommandOutput): string {
  return `exit ${output.status}: ${(output.stderr || output.stdout).trim()}`;
}

function errorCode(error: unknown): string | undefined {
  return typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string'
    ? error.code
    : undefined;
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
