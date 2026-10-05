import { execFile } from 'node:child_process';
import { constants } from 'node:fs';
import { mkdtemp, open, readdir, readFile, rm, stat, statfs, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

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
 * An image attached from a file below a lease belongs to whoever attached it:
 * cowshed's real-APFS fixtures release theirs under the production per-image
 * lease, from scratch roots outside TMPDIR (specs/cowshed/08_testing.md). This
 * module never detaches one, and never deletes through a mount below a lease.
 * A dead lease that still backs an image or holds a mount is kept, with the
 * volume under it, and every task that takes a lease names it. Every disk
 * child runs under a deadline, so a wedged DiskArbitration fails the task
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
 */
export const RAM_TEMP_CAPACITY_BYTES = 64 * GIB;
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
  stateFile: string;
  /** APFS volume name; names the mountpoint and identifies the volume when its mountpoint cannot. */
  volumeName: string;
}

export function ramTempPaths(uid: number): RamTempPaths {
  const volumeName = `smoo-ram-${uid}`;
  return {
    mountpoint: join('/Volumes', volumeName),
    lockFile: `/private/tmp/${volumeName}.lock`,
    stateFile: `/private/tmp/${volumeName}.device`,
    volumeName,
  };
}

/** The attached devices of a volume this module created, as recorded in its state file. */
interface VolumeDevices {
  /** The RAM disk itself (`/dev/diskN`): detaching it removes container and volume. */
  physical: string;
  /** The APFS volume (`diskMsK`) that mounts at the mountpoint. */
  volume: string;
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

export type RamTempError =
  /** A host command or filesystem step failed while the volume was being created, mounted or detached. */
  | { kind: 'provision-failed'; step: string; detail: string }
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
  constructor(
    private readonly paths: RamTempPaths,
    private readonly capacityBytes: number,
    private readonly commands: HostCommands,
    private readonly pid: number = process.pid,
    private readonly isAlive: (pid: number) => boolean = processIsAlive,
  ) {}

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
    const stale = await this.detachRecorded();
    if (!stale.ok) {
      return stale;
    }
    const sectors = Math.ceil(this.capacityBytes / SECTOR_BYTES);
    const attached = await this.commands.run('/usr/bin/hdiutil', ['attach', '-nomount', `ram://${sectors}`]);
    if (attached.status !== 0) {
      return failed('hdiutil attach', commandText(attached));
    }
    const physical = /^(\/dev\/disk\d+)\b/.exec(attached.stdout)?.[1];
    if (!physical) {
      return failed('hdiutil attach', `no device in: ${attached.stdout.trim()}`);
    }
    const devices: VolumeDevices = { physical, volume: '' };
    const provisioned = await this.provision(devices);
    if (!provisioned.ok) {
      await this.commands.run('/usr/bin/hdiutil', ['detach', '-force', physical]);
      await rm(this.paths.stateFile, { force: true });
    }
    return provisioned;
  }

  private async provision(devices: VolumeDevices): Promise<Result<void, RamTempError>> {
    // Recorded before anything can fail half-way, so a crash leaves the next creator a device to detach.
    await writeFile(this.paths.stateFile, JSON.stringify(devices), { mode: 0o600 });
    const container = await this.commands.run('/usr/sbin/diskutil', ['apfs', 'createContainer', devices.physical]);
    const containerDisk = operationDisk(container.stdout);
    if (container.status !== 0 || !containerDisk) {
      return failed('diskutil apfs createContainer', commandText(container));
    }
    const added = await this.commands.run('/usr/sbin/diskutil', [
      'apfs',
      'addVolume',
      containerDisk,
      'APFS',
      this.paths.volumeName,
      '-nomount',
    ]);
    const volume = operationDisk(added.stdout);
    if (added.status !== 0 || !volume) {
      return failed('diskutil apfs addVolume', commandText(added));
    }
    devices.volume = volume;
    await writeFile(this.paths.stateFile, JSON.stringify(devices), { mode: 0o600 });
    // No -mountPoint: see the module comment. DiskArbitration names the mountpoint after the
    // volume, and appends " 1" when a stale /Volumes entry already holds the name.
    const mounted = await this.commands.run('/usr/sbin/diskutil', ['mount', '-mountOptions', 'nobrowse', volume]);
    if (mounted.status !== 0) {
      return failed('diskutil mount', commandText(mounted));
    }
    const table = await this.commands.run('/sbin/mount', []);
    const point = new RegExp(`^/dev/${volume} on (.+) \\(`, 'm').exec(table.stdout)?.[1];
    if (point !== this.paths.mountpoint) {
      return failed('diskutil mount', `${volume} mounted at ${point ?? 'nowhere'}, expected ${this.paths.mountpoint}`);
    }
    await writeFile(join(this.paths.mountpoint, MARKER_FILE), `${this.paths.volumeName}\n`);
    return { ok: true, value: undefined };
  }

  /** Detach what the state file records, if its volume is still the one this module named. */
  private async detachRecorded(): Promise<Result<void, RamTempError>> {
    const recorded = await readFile(this.paths.stateFile, 'utf8').catch(() => null);
    if (recorded === null) {
      return { ok: true, value: undefined };
    }
    const devices = parseDevices(recorded);
    if (devices && devices.volume !== '') {
      const info = await this.commands.run('/usr/sbin/diskutil', ['info', devices.volume]);
      // A device number is reused by the next image attached after ours went away: only a
      // volume that still carries this module's name is ours to detach.
      if (info.status === 0 && volumeNameOf(info.stdout) === this.paths.volumeName) {
        const detached = await this.commands.run('/usr/bin/hdiutil', ['detach', '-force', devices.physical]);
        if (detached.status !== 0) {
          return failed('hdiutil detach (stale volume)', commandText(detached));
        }
      }
    }
    await rm(this.paths.stateFile, { force: true });
    return { ok: true, value: undefined };
  }

  private async detach(): Promise<Result<void, RamTempError>> {
    const recorded = await readFile(this.paths.stateFile, 'utf8').catch(() => null);
    const devices = recorded === null ? null : parseDevices(recorded);
    if (!devices) {
      return failed('detach', `${this.paths.stateFile} does not record the mounted volume's device`);
    }
    // Every lease is gone, so nothing below the volume should be attached; whatever is would lose
    // its backing file to the detach.
    const holders = await this.holders(`${this.paths.mountpoint}/`);
    if (!holders.ok) {
      return holders;
    }
    if (holders.value.length > 0) {
      return failed('detach', holdingText(this.paths.mountpoint, holders.value));
    }
    let detached = await this.commands.run('/usr/bin/hdiutil', ['detach', devices.physical]);
    if (detached.status !== 0) {
      detached = await this.commands.run('/usr/bin/hdiutil', ['detach', '-force', devices.physical]);
    }
    if (detached.status !== 0) {
      return failed('hdiutil detach', commandText(detached));
    }
    await rm(this.paths.stateFile, { force: true });
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

export function volumeNameOf(diskutilInfo: string): string | null {
  return /^\s*Volume Name:\s*(.+)$/m.exec(diskutilInfo)?.[1]?.trim() ?? null;
}

function parseDevices(text: string): VolumeDevices | null {
  try {
    const value: unknown = JSON.parse(text);
    if (
      typeof value === 'object' &&
      value !== null &&
      'physical' in value &&
      'volume' in value &&
      typeof value.physical === 'string' &&
      typeof value.volume === 'string'
    ) {
      return { physical: value.physical, volume: value.volume };
    }
  } catch {
    // An unparsable record names nothing to detach.
  }
  return null;
}

/**
 * A disk child that DiskArbitration never answers would wedge the task, and every task queued
 * behind the lock, forever; measured: `diskutil mount` hung past 60 s while the host's
 * DiskArbitration was saturated. Cowshed bounds its disk children the same way.
 */
const HOST_COMMAND_DEADLINE_MS = 120_000;

export function createHostCommands(): HostCommands {
  return {
    run(file, args) {
      return new Promise((resolve) => {
        // The registry listing of every attached image runs to megabytes on a busy host
        // (2 MB measured with 136 images); execFile's 1 MB default truncates it into a failure.
        execFile(
          file,
          [...args],
          { encoding: 'utf8', maxBuffer: 64 * MIB, timeout: HOST_COMMAND_DEADLINE_MS, killSignal: 'SIGKILL' },
          (error, stdout, stderr) => {
            if (error === null) {
              resolve({ status: 0, stdout, stderr });
              return;
            }
            const status = typeof error.code === 'number' ? error.code : 1;
            const detail = error.killed ? `no answer within ${HOST_COMMAND_DEADLINE_MS} ms; killed` : error.message;
            resolve({ status, stdout, stderr: stderr === '' || error.killed ? detail : stderr });
          },
        );
      });
    },
  };
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

function failed<T>(step: string, detail: string): Result<T, RamTempError> {
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
