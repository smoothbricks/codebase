import { type ChildProcess, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { type BigIntStats, constants, type Stats } from 'node:fs';
import {
  type FileHandle,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readdir,
  readFile,
  rename,
  rm,
  rmdir,
  stat,
  statfs,
  writeFile,
} from 'node:fs/promises';
import { Socket } from 'node:net';
import { basename, dirname, join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { O_EXLOCK } from '@smoothbricks/cargo-retain/open-flags';
import { DEADLINE_PATIENCE, describeMembersLeft, killProcessGroup } from '../../process-group.js';
import { isRecord } from '../../type-guards.js';
import { diskClassOf, type PhaseShare, recordedShare, takeDiskLease } from './disk-lease.js';
import { GATEWAY_SOCKET } from './gateway-lease.js';
import {
  COWSHED_EXECUTABLE_ENV,
  type CoverageGap,
  parseHandle,
  type SignalledProcess,
  type SubtreeHandle,
  type SubtreeOwner,
} from './owned-subtree.js';
import { attachRamDisk } from './ram-attach.js';
import { detachGatewayRamDisk } from './ram-detach.js';
import {
  captureHeldVerdict,
  type HeldCandidate,
  type HeldContext,
  type HeldVerdict,
  parseHeldVerdict,
  validateHeldVerdict,
} from './ram-temp-held.js';
import type { TempVolumeSpace } from './verdict.js';

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
 *   that cowshed's images and tests draw on (specs/cowshed/01a_apfs.md, "How
 *   the APFS host degrades"; measured 2026-10-05).
 * - The disk lives as long as that helper, and a helper forked inside a job is
 *   the job's member, which the job's end retires under every other lease. So
 *   the helper belongs to the cowshed gateway, which no job contains and which
 *   attaches the disk under the creating task's lease (`ram-lease`,
 *   specs/cowshed/05_gateway.md, "RAM disks"); a task never does.
 * - Attach + format + mount costs ~2 s and is DiskArbitration traffic, so the
 *   volume is created once, lazily, and every task takes a subdirectory.
 * - Spotlight's mds keeps one IOKit right for every APFS volume it admits, for
 *   the life of the mds process, and admits no volume whose kernel-resolved
 *   mount path contains `/var/folders/` (specs/cowshed/01a_apfs.md, "Spotlight
 *   media references"). Measured 2026-10-22: a RAM volume mounted at /Volumes
 *   or /private/tmp left mds one right; the same mount under
 *   /private/var/folders left none. So the volume mounts below the per-user root
 *   `cowshed setup` makes, `/private/var/folders/cowshed/<uid>`, and a task
 *   refuses to create it while that root is missing or not the user's 0700
 *   directory.
 * - The kernel mounts it there: `mount_apfs` needs no authorization, where
 *   `diskutil mount -mountPoint <dir>` was refused as not privileged
 *   (0xf8da0009) and storagekitd escalated it to an administrator dialog whose
 *   wait blocked every diskutil on the host (log show, 2026-10-04).
 * - Without authorization the volume mounts only `noowners`: launchd refuses a
 *   plist from it ("Caller specified a plist with bad ownership/permissions"),
 *   so a test that bootstraps a launchd job keeps the plist outside TMPDIR.
 *
 * One kernel lock (`O_EXLOCK` on a file in /private/tmp), dropped by the kernel
 * when its holder dies, admits changes to a small ledger beside it and to the
 * set of lease directories. It is held only to read and publish those: the
 * host-wide probes (`ioreg`, `mount`, the process census), directory cleanup,
 * provisioning and detach run outside it, under claims the ledger publishes.
 * One caller creates or detaches the volume at a time, and a caller that finds
 * either in flight yields and looks again; each dead lease is settled by the
 * one caller that claimed it, and every other caller passes it by. A claim
 * names its owner's pid; death voids it only with no pending detach. Before it
 * removes a lease or detaches the volume, a caller takes the lock again and
 * checks that the volume, the lease directory and its own claim are still as
 * it claimed them. A lease is a directory named after the task process's pid;
 * a lease whose pid is gone is reclaimed by the next task, and the volume is
 * detached when the last lease ends. The pid's death proves only that the task
 * process is gone, not what it started: its command runs in a process group of
 * its own, and every process started with the lease as its `TMPDIR` writes
 * there wherever it works. So a dead lease is removed only once every process
 * working in it, by working directory or by `TMPDIR`, has been stopped.
 *
 * The volume is created under its final name and never renamed. It is mounted by `mount_apfs` at a
 * private staging directory and marked there, unmounted (`umount`), and mounted by `mount_apfs` again at its
 * mountpoint, so it reaches the mountpoint already marked. A Disk Arbitration rename names whatever volume
 * holds the path it recorded, and a force-unmounted volume leaves a bare directory there on the host's Data
 * volume, so no rename is made. `noowners` lets the marker be written, since a fresh volume's root is owned
 * by root (measured 2026-10-15). A creator that dies part way leaves debris at the staging directory or as an
 * unmounted volume, never an unmarked volume at the mountpoint. No Disk Arbitration call made either
 * directory, so none removes it: this module removes both once the volume is gone.
 * The I/O Registry is the only record of what was attached, and it names every disk's volume. A task inventories the
 * RAM disks that are this volume's and no other (`volumeDisks`): of this user and this volume's exact size, mounting
 * nowhere but at its mountpoint or staging directory, and either carrying a volume called exactly its name, which a
 * volume is made under and keeps, or recorded by the exact registry entry its owner holds a receipt or its ledger holds
 * custody for, which covers an attach whose format never finished. A RAM disk of that size that is neither, another
 * volume's, a test's, or an unformatted one nothing recorded, is ignored, however it is mounted. The gateway holds
 * each disk it attaches under its creator's lease until the creator keeps it, its volume mounted and marked at the
 * mountpoint, and detaches it when the creator releases it or goes away first: a creator killed or deadlined mid-step
 * leaves no disk behind, where one did before the lease (measured 2026-10-06: a deadlined `hdiutil attach` whose
 * diskimages-helper finished the attach after `hdiutil` was killed). A gateway restart leaves an unended lease's disk
 * to the next gateway's startup scan. Before creating, a task detaches each disk of that inventory its owner holds a
 * receipt for. A disk it holds no receipt for, one with a lease directory on it, or one with something attached below
 * it stops the creation instead; its error names it.
 *
 * An image attached from a file below a lease belongs to whoever attached it:
 * cowshed's real-APFS fixtures release theirs under the production per-image
 * lease, from scratch roots outside TMPDIR (specs/cowshed/08_testing.md). This
 * module never detaches one, and never deletes through a mount below a lease.
 * A dead lease that still backs an image or holds a mount is kept, with the
 * volume under it, and every task that takes a lease names it. Where every
 * holder has an identity a cheap `lstat` re-proves (a mount below the lease),
 * the ledger keeps that proof (`ram-temp-held.ts`) and later tasks check it
 * instead of probing the host; any change sends the lease back to a probe.
 * SAFE disk children delegate their execution budget and reap to cowshed. Protected
 * image commands are never killed at the caller's deadline. A detach publishes its
 * exact custody before it spawns. While its live caller still waits on the facade the
 * custody is `waiting`, and other callers queue behind it as behind any claim. Once
 * that caller stops waiting, or its run throws without an answer, the custody is
 * `reported`: no lease is admitted until a normal terminal answer or the bound registry
 * entry's absence proves it safe. A dead caller alone proves neither; its death only
 * turns queueing into reporting. The facade pid is a diagnostic, never authority.
 *
 * A cowshed sandbox denies the volume's paths (EPERM reading its mountpoint or
 * marker, or opening the lock in /private/tmp) and cannot attach disk images at
 * all; whichever refusal comes first, the task keeps the workspace's own TMPDIR
 * and says so once on stderr.
 */

/** Contended opens yield without occupying a filesystem worker needed by the lock holder. */
const LOCK_RETRY_MS = 50;
/**
 * A caller waiting this long on another's creation or detach says so on stderr, and again each
 * time as long again passes: a claim whose owner pid was reused after a crash never voids itself.
 */
const CLAIM_WAIT_NOTICE_MS = 15_000;
const SECTOR_BYTES = 512;
const MIB = 1024 * 1024;
const GIB = 1024 * MIB;
/**
 * The revision of the volume's protocol. Checkouts at different revisions run on one host at
 * once, and each is safe only beside its own: revision 1 leases under the kernel lock alone,
 * revision 2 leases and detaches outside it, behind claims in a ledger revision 1 never reads,
 * and revision 3 mounts below the per-user cowshed root instead of at the `/Volumes` path
 * DiskArbitration gave revisions 1 and 2. So no revision shares a volume with another. It names
 * the volume, its mountpoint, its lock and its ledger, and adds as many sectors to the RAM disk,
 * whose size and user are what a creator's stray reclaim matches: no revision detaches another's
 * disk, mounted or still attaching. A volume of an earlier revision is detached by that
 * revision's own last release.
 */
const RAM_TEMP_REVISION = 3;
/**
 * The volume's ceiling, plus the revision's sectors. Pages are allocated as they are written and
 * returned when the volume detaches, so this bounds what a runaway suite can take, not what a gate
 * costs. Measured 2026-10-04: a full parallel smoothbricks `lint test build host-controller-test`
 * gate peaked at 1.61 GiB used with 18 concurrent leases; 8 GiB is five times that. That
 * gate also staged cowshed's real-APFS fixture images on the volume, which now stay outside it,
 * so the peak is an upper bound.
 */
export const RAM_TEMP_CAPACITY_BYTES = 8 * GIB + RAM_TEMP_REVISION * SECTOR_BYTES;
/** A failed task with less than this free on the volume is reported as having run out of space. */
const FULL_FREE_BYTES = 64 * MIB;
/** `<pid>-<mkdtemp suffix>`: the pid says whether the lease's task is alive. */
const LEASE_NAME = /^(\d+)-[A-Za-z0-9]{6}$/;
/** The random suffix `mkdtemp` gives a lease name. */
const LEASE_SUFFIX_BYTES = 6;
/** Darwin's `sockaddr_un.sun_path` holds 104 bytes, the last a NUL: a socket's path is at most 103. */
const SOCKET_PATH_BYTES = 103;
/**
 * The one length rule of the RAM temp layout (specs/cowshed/01_storage.md, "RAM temp path budget"): a lease directory,
 * the TMPDIR its task gets, is at most this many bytes. It is the layout's worst case, a test namespace's at the largest
 * uid and pid: `/private/var/folders/cowshed/` (29), a u32 uid (10), `/<ns>` (8), `/ram3` (5), `/<pid ≤ 99999>-` (7)
 * and the suffix (6). Production's worst case is 57.
 */
export const RAM_TMPDIR_MAX_BYTES = 65;
/** What the rule leaves below a lease for a socket's `/<names>`: 38 bytes. */
export const RAM_TMPDIR_SOCKET_BUDGET_BYTES = SOCKET_PATH_BYTES - RAM_TMPDIR_MAX_BYTES;

/** The length in bytes of the lease directory process `pid` makes below `mountpoint`. */
export function leaseDirectoryBytes(mountpoint: string, pid: number): number {
  return Buffer.byteLength(`${mountpoint}/${pid}-`) + LEASE_SUFFIX_BYTES;
}
const MARKER_FILE = '.smoo-ram';
/**
 * The scratch of a run that did not pass, moved out of its lease when the lease is reclaimed: a directory beside the
 * leases on the same volume, so the move is one `rename(2)` and copies nothing. An entry is
 * `<archived at, epoch ms, 13 digits>-<lease name>`, so the names sort oldest first.
 */
const ARCHIVE_DIR = '.archive';
const ARCHIVE_NAME = /^\d{13}-\d+-[A-Za-z0-9]{6}$/;
/**
 * Archives on their way out, below the archive directory: each is renamed here as `<deleter pid>-<archive name>` and
 * deleted by that process in the background. A dead deleter's entries are claimed by renaming them to the next one's pid.
 */
const DELETING_DIR = '.deleting';
const DELETING_NAME = /^(\d+)-(\d{13}-\d+-[A-Za-z0-9]{6})$/;
/**
 * How many archives the volume keeps: the failing run under triage and the one before it, the pair a triage compares to
 * tell a flake from a regression. A stated reason, not a measurement.
 */
export const ARCHIVES_KEPT = 2;
/**
 * Below this share of the volume free, the volume keeps no archive: live leases need the space more than evidence does.
 * Half of the 8 GiB volume is 4 GiB, 2.5 times the 1.61 GiB a full parallel gate peaked at (measured 2026-10-04, see
 * `RAM_TEMP_CAPACITY_BYTES`).
 */
const ARCHIVE_FLOOR_SHARE = 0.5;

export interface CommandOutput {
  status: number;
  stdout: string;
  stderr: string;
  /** A bounded wait or a signalled facade is not a terminal disk-child receipt. */
  readonly pending?: {
    /** Diagnostic only: neither this pid's death nor reuse releases detach authority. */
    readonly pid: number | null;
    /** This exact child object's normal exit; null means native completion could not be proved. */
    readonly completion: Promise<CommandOutput | null>;
  };
}

/** The host programs the volume lifecycle runs. A seam so the state machine is testable without DiskArbitration. */
/**
 * Binds an `hdiutil detach <device>` to the I/O Registry entry the caller inventoried the device as: cowshed reads the
 * entry again inside the disk admission and detaches only while it still has the device as its whole disk.
 */
export interface DetachBinding {
  readonly registryEntry: string;
}

/** A RAM disk the gateway attached and holds until this caller keeps or releases it; the gateway detaches it if the caller goes away first. */
export interface RamDiskLease {
  /** Its whole disk, `/dev/diskN`. */
  readonly device: string;
  /** Its I/O Registry entry, `0x…`, as the gateway read it right after its attach. */
  readonly registryEntry: string;
  /**
   * The caller owns the disk from here. Err: the gateway refused, or no answer came, and then nothing is known: the
   * gateway may have handed the disk over or left it to its next startup scan, so the caller detaches it by its entry.
   */
  keep(): Promise<Result<void, string>>;
  /** The gateway detaches the disk now. Ok: its detach answer; Err: the lease was lost (no answer), so nothing is known of a detach. */
  release(): Promise<Result<CommandOutput, string>>;
}

export interface HostCommands {
  run(file: string, args: readonly string[], deadlineMs?: number, binding?: DetachBinding): Promise<CommandOutput>;
  /**
   * Attach a RAM disk of `sectors` 512-byte sectors through its owner, answering its lease. The owner is the cowshed
   * gateway (`ram-lease`), never this task: a helper the task's own job forked would end with it. The gateway holds the
   * disk until the lease is kept or released, and detaches it if this caller goes away first.
   */
  attachRam(sectors: number): Promise<Result<RamDiskLease, string | RamUnavailable>>;
  /**
   * Exact attach receipts owned by this caller; names and capacity never establish this authority. A disk it owns
   * counts as the volume's even with nothing mounted, which covers an attach whose format never finished; one it holds
   * no receipt for counts only as the volume of its exact name.
   */
  ownsRamDisk?(disk: AttachedRamDisk): boolean;
  detachRam(device: string, registryEntry: string): Promise<CommandOutput>;
}

/**
 * How long a process a task left in its subtree gets to exit on SIGTERM before it gets SIGKILL:
 * the time a daemon takes to stop its watcher and its plugin workers, and well under the grace a
 * bounded leg gives its own command.
 */
export const LEFTOVER_GRACE_MS = 5_000;

/** A lease's subtree handle file: beside the lease on the volume, never in it, so the command cannot remove it. */
function subtreeRecordPath(directory: string): string {
  return `${directory}.subtree`;
}

/** One coverage gap the supervisor reported for a subtree it retired, as text. */
export function describeCoverageGap(gap: CoverageGap): string {
  return gap.pid === null ? gap.kind : `${gap.kind} (pid ${gap.pid})`;
}

/** One process a retirement signalled, as `<pid> (<image>)`. */
export function describeSignalled(entry: SignalledProcess): string {
  return `${entry.pid} (${entry.image ?? 'unknown image'})`;
}

/**
 * A test run's host namespace: seven lowercase base-36 characters, the pid of the process that minted it in the first
 * five (zero-padded, so the first is a digit, which begins no production name) and that process's ordinal in the last
 * two. The pid tells a later run whether the namespace's owner is gone. Mirrors `cowshed_os::namespace::TestNamespace`.
 */
export type TestNamespace = { readonly kind: 'test'; readonly name: string };

/**
 * Whose names a host's RAM temp volume carries: production's, or one test run's own (specs/cowshed/01_storage.md,
 * "Host namespace"). Every path and name the volume takes derives from it (`ramTempPaths`), so no name a test makes can
 * be one production reads, sweeps or reclaims, and no guard compares them. Mirrors
 * `cowshed_os::namespace::HostNamespace`.
 */
export type HostNamespace = { readonly kind: 'production' } | TestNamespace;

/** The namespace of the user's own volume, which no test leases, creates or detaches. */
export const PRODUCTION: HostNamespace = { kind: 'production' };

const NAMESPACE_BASE = 36;
const NAMESPACE_PID_DIGITS = 5;
const NAMESPACE_ORDINAL_DIGITS = 2;

/** `value` as `digits` zero-padded lowercase base-36 digits; the refusal names it `what` when it does not fit them. */
function base36Digits(value: number, digits: number, what: string): string {
  if (!Number.isSafeInteger(value) || value < 0 || value >= NAMESPACE_BASE ** digits) {
    throw new RangeError(`${what} ${value} does not fit a test namespace's ${digits} base-36 digits`);
  }
  return value.toString(NAMESPACE_BASE).padStart(digits, '0');
}

/**
 * The namespace process `pid` mints as its `ordinal`th, encoded as `TestNamespace::of` encodes it:
 * `testNamespace(94769, 3)` is `0214h03`. A pid or an ordinal that does not fit its digits throws.
 */
export function testNamespace(pid: number, ordinal: number): TestNamespace {
  const pidDigits = base36Digits(pid, NAMESPACE_PID_DIGITS, 'pid');
  const ordinalDigits = base36Digits(ordinal, NAMESPACE_ORDINAL_DIGITS, 'ordinal');
  return { kind: 'test', name: `${pidDigits}${ordinalDigits}` };
}

export interface RamTempPaths {
  /**
   * The root the volume mounts below: a directory of `uid`'s with mode 0700, holding the mountpoint and the staging
   * directory. Production's is the per-user root `cowshed setup` makes, `/private/var/folders/cowshed/<uid>`; a test
   * namespace's is the directory of its name below that root, which its fixture makes. mds admits no volume mounted
   * below `/var/folders/`.
   */
  root: string;
  /** The user that must own `root`. */
  uid: number;
  /**
   * Where the kernel mounts the volume: `<root>/ram<revision>`, so no two revisions stack on one directory. Its length
   * bounds every lease's, which {@link RAM_TMPDIR_MAX_BYTES} caps.
   */
  mountpoint: string;
  /**
   * The kernel lock, and the ledger beside it: `/private/tmp/<volumeName>.lock` for production,
   * `<root>/ram<revision>.lock` for a test namespace, which keeps its ordinary scratch beside the lock in its own root.
   */
  lockFile: string;
  /** APFS volume name; its marker holds it. `smoo-ram<revision>-<uid>`, with `-<ns>` after it for a test namespace. */
  volumeName: string;
  /** Where the volume is mounted to be marked before it is mounted at `mountpoint`. */
  staging: string;
  /**
   * The longest lease directory, a task's TMPDIR, this layout admits: {@link RAM_TMPDIR_MAX_BYTES} for every layout
   * `ramTempPaths` makes. A unit fixture that moves the volume into its own scratch makes no TMPDIR and states
   * `Number.MAX_SAFE_INTEGER`, which survives JSON as Infinity does not.
   */
  leaseBytesMax: number;
}

/**
 * Where the volume of `user` in `namespace` lives and what it is called. Production's are the user's own: the per-user
 * root `/private/var/folders/cowshed/<uid>`, the mountpoint `<root>/ram3` and staging directory `<root>/ram3-new` below
 * it, the volume `smoo-ram3-<uid>` and its lock in `/private/tmp`. A test namespace's lie in the directory of its name
 * below that root, `<root>/<ns>`: its mountpoint, staging directory and lock (`ram3.lock`) in it, and its name after
 * the volume's (`smoo-ram3-<uid>-<ns>`), so none of its paths or names is production's.
 */
export function ramTempPaths(user: { uid: number }, namespace: HostNamespace): RamTempPaths {
  const userRoot = `/private/var/folders/cowshed/${user.uid}`;
  const leaf = `ram${RAM_TEMP_REVISION}`;
  const own = `smoo-${leaf}-${user.uid}`;
  const production = namespace.kind === 'production';
  const root = production ? userRoot : `${userRoot}/${namespace.name}`;
  const mountpoint = `${root}/${leaf}`;
  return {
    root,
    uid: user.uid,
    mountpoint,
    lockFile: production ? `/private/tmp/${own}.lock` : `${mountpoint}.lock`,
    volumeName: production ? own : `${own}-${namespace.name}`,
    staging: `${mountpoint}-new`,
    leaseBytesMax: RAM_TMPDIR_MAX_BYTES,
  };
}

/** An APFS volume of a RAM disk, as the I/O Registry names it and the mount table places it. */
export interface RamVolume {
  /** Its `FullName`. A volume is made under its final name and never renamed, so the name says whose it is. */
  name: string;
  /** Where it is mounted, if anywhere. */
  mountpoints: string[];
}

/** An attached RAM disk, as the I/O Registry and the mount table show it. */
export interface RamDisk {
  /** Its `IOHDIXHDDriveOutKernel` node's registry entry ID: no later disk has it, where a BSD name passes to the next. */
  entry: string;
  /**
   * The whole disk (`/dev/diskN`): detaching it removes its container and volumes. Null while the disk has published
   * no media yet, part way through its attach.
   */
  physical: string | null;
  sectors: number;
  /** The uid that attached it. */
  uid: number;
  /** Where its volumes are mounted, if anywhere. */
  mountpoints: string[];
  /** Its APFS volumes, by the exact names the I/O Registry gives them: none before it is formatted. */
  volumes: RamVolume[];
}

/** A RAM disk with its whole disk published, which a detach can name. */
export interface AttachedRamDisk extends RamDisk {
  physical: string;
}

export interface RamTempLease {
  /** The task's TMPDIR: a directory on the volume that only this task uses. */
  directory: string;
  mountpoint: string;
  /**
   * The attach of the volume the lease was taken on ({@link MountedVolume.attach}). A read of the mountpoint that finds
   * another attach, or none, is not this lease's volume: its free space is some other filesystem's.
   */
  attach: string;
}

export interface RamUnavailable {
  readonly kind: 'ram-unavailable';
  readonly reason: 'gateway-unsupported' | 'gateway-absent' | 'unproven-owner';
  readonly detail: string;
}

interface GatewayVolumeMarker {
  readonly version: 1;
  readonly volumeName: string;
  readonly owner: 'gateway';
  readonly registryEntry: string;
}

export type RamTempAcquisition =
  /**
   * `held`: dead leases that could not be reclaimed, each with what still holds it. `reaped`: the
   * processes the supervisor retired from dead leases' recorded subtrees before those leases went,
   * as `<pid> (<image>)`. `gaps`: where the supervisor's view of those subtrees may have missed one.
   */
  | {
      kind: 'leased';
      lease: RamTempLease;
      held: string[];
      reaped: string[];
      gaps: string[];
      /** What became of each dead lease's scratch this acquire reclaimed: archived and kept, or deleted and why. */
      retained: string[];
    }
  /** Inside a cowshed sandbox: /private/tmp is not writable and no image can attach. */
  | { kind: 'sandboxed'; detail: string }
  | { kind: 'unavailable'; reason: RamUnavailable['reason']; detail: string };

/** A host command or filesystem step failed while the volume was being created, mounted or detached. */
type ProvisionFailed = { kind: 'provision-failed'; step: string; detail: string };

export type RamTempError =
  | RamUnavailable
  | ProvisionFailed
  /** A failed task left the volume (nearly) full: the likely cause of its failure. */
  | { kind: 'volume-full'; mountpoint: string; capacityBytes: number; freeBytes: number }
  /** The lease went while its task held it: its volume was detached or replaced, or its directory removed. */
  | { kind: 'lease-lost'; lease: string; mountpoint: string; detail: string }
  /**
   * The root the volume mounts below, the per-user root or a test namespace's directory, is missing or is not the
   * user's 0700 directory.
   */
  | { kind: 'root-unready'; root: string; detail: string }
  /** The lease this caller would make is longer than its layout admits: refused before anything is made. */
  | { kind: 'lease-path-budget'; mountpoint: string; pid: number; bytes: number; most: number };

export type Result<T, E> = { ok: true; value: T } | { ok: false; error: E };

export function describeRamTempError(error: RamTempError): string {
  switch (error.kind) {
    case 'ram-unavailable':
      return `RAM acceleration unavailable (${error.reason}): ${error.detail}`;
    case 'provision-failed':
      return `RAM temp volume: ${error.step} failed: ${error.detail}`;
    case 'volume-full':
      return `RAM temp volume ${error.mountpoint} is full: ${Math.round(error.freeBytes / MIB)} MiB free of ${Math.round(error.capacityBytes / MIB)} MiB; raise RAM_TEMP_CAPACITY_BYTES in @smoothbricks/nx-plugin`;
    case 'lease-lost':
      return `RAM temp volume: lease ${error.lease} was lost while its task held it: ${error.detail}; what the task wrote to its TMPDIR went with it`;
    case 'root-unready':
      return `RAM temp volume: ${error.root} ${error.detail}; run "cowshed setup", which makes it a directory of yours with mode 0700`;
    case 'lease-path-budget':
      return `RAM temp volume: a lease below ${error.mountpoint} for pid ${error.pid} would be ${error.bytes} bytes, past the ${error.most} its layout admits, leaving ${SOCKET_PATH_BYTES - error.most} below it for a socket's names within sun_path (specs/cowshed/01_storage.md, "RAM temp path budget")`;
  }
}

/** One pass's host attachments: the disk images (backing file and whole disk) and the mount points. */
interface DiskImage {
  readonly image: string;
  readonly disk: string;
}

interface Inventory {
  readonly images: readonly DiskImage[];
  readonly mounts: readonly string[];
}

/** What is attached from below a path prefix, as one pass sees it. */
type Holders = (prefix: string) => Promise<Result<string[], RamTempError>>;

/** A caller's mark on a claim: its pid says whether it still runs, its token which of its operations made it. */
interface ClaimOwner {
  readonly pid: number;
  readonly token: string;
}

/** Published before spawn, retained across caller death. Null registry identity means only a terminal receipt suffices. */
interface DetachCustody {
  readonly physical: string;
  readonly registryEntry: string | null;
  /**
   * `waiting` while its caller still awaits the facade, so other callers queue; `reported` once that caller stopped
   * waiting or its run threw, so other callers reconcile against the registry or refuse.
   */
  readonly phase: 'waiting' | 'reported';
  /** Diagnostic only: the facade its caller stopped waiting on, when one was spawned. */
  readonly facadePid: number | null;
}

/** The volume is being created or detached: no other caller takes a lease or detaches meanwhile. */
interface LifecycleClaim {
  readonly kind: 'create' | 'detach';
  readonly owner: ClaimOwner;
  readonly pending?: DetachCustody;
}

/** One caller is settling the lease `name`, as it was when claimed: stopping, probing and removing it. */
interface LeaseClaim {
  readonly name: string;
  readonly identity: string;
  readonly owner: ClaimOwner;
}

/** A dead lease proved held, and the proof a later pass validates instead of probing the host again. */
interface HeldEntry {
  readonly lease: string;
  readonly verdict: HeldVerdict;
}

/** What the lock admits changes to. Dead callers void only claims with no pending detach authority. */
interface Ledger {
  readonly lifecycle: LifecycleClaim | null;
  readonly claims: readonly LeaseClaim[];
  readonly held: readonly HeldEntry[];
}

const EMPTY_LEDGER: Ledger = { lifecycle: null, claims: [], held: [] };

/** The volume as one locked step finds it mounted. */
interface MountedVolume {
  readonly registryEntry: string;
  /** This attach of the volume: its root's and its marker's identity, never times that change while it is in use. */
  readonly key: string;
  /**
   * The attach alone: its root's identity and its I/O Registry entry. A rewrite of the marker changes {@link key}, which
   * provenance then proves again, while the leases on the attach stay where they are.
   */
  readonly attach: string;
  readonly device: number;
  /** The lease directories on it. */
  readonly leases: readonly string[];
}

/**
 * One acquire or release. `claimed`: the ledger as it last published holds one of its claims.
 * `made`: the lease directory an acquire made, with its identity, until the caller has it.
 * `subtrees`: the supervisor this caller asks to retire a dead lease's recorded subtree.
 */
interface Operation {
  provenance?: string;
  readonly owner: ClaimOwner;
  readonly subtrees: SubtreeOwner;
  claimed: boolean;
  made: { readonly directory: string; readonly identity: string } | null;
}

/** What one locked step publishes, and what it found. */
interface Step<T> {
  readonly ledger: Ledger;
  readonly value: T;
}

/** A lease a caller claimed to settle, with its identity when claimed. */
interface Candidate {
  readonly name: string;
  readonly directory: string;
  readonly identity: string;
  /** The caller's own ending lease, whose task reaped it, rather than a dead one. */
  readonly own: boolean;
  /** Its scratch is archived, not deleted: its task died, or its own run did not pass. */
  readonly keep: boolean;
}

/** A lease kept because something is attached below it, described for the task's output. */
interface HeldLease {
  readonly name: string;
  readonly text: string;
}

/** A claim step's finding: leases a cached verdict still proves held, and leases this caller now settles. */
interface Claimed {
  readonly ledger: Ledger;
  readonly held: readonly HeldLease[];
  readonly candidates: readonly Candidate[];
}

/** What settling claimed leases found: those kept, the processes retired, the gaps, and the verdicts to publish. */
interface Settled {
  readonly held: readonly HeldLease[];
  readonly reaped: readonly string[];
  readonly retained: readonly string[];
  readonly gaps: readonly string[];
  readonly verdicts: readonly HeldEntry[];
}

/** A reclaimed lease whose scratch was renamed into the archive, and the name it has there. */
interface Archived {
  readonly directory: string;
  readonly archive: string;
}

/**
 * This process's background deletions of archives, one at a time in order. Nothing that takes or releases a lease waits
 * on it; the detach of the volume does, since a deletion still walking the volume would hold it busy.
 */
let archiveDeletion: Promise<void> = Promise.resolve();

/** Settles once every archive deletion this process has queued so far has finished; a test seam, and the detach's wait. */
export function archiveDeletions(): Promise<void> {
  return archiveDeletion;
}

/** How long a caller has waited on one lifecycle claim, and how many notices it has printed. */
interface ClaimWait {
  readonly token: string;
  readonly since: number;
  readonly notices: number;
}

type Taken =
  | { readonly kind: 'wait'; readonly claim: LifecycleClaim }
  | { readonly kind: 'create' }
  | {
      readonly kind: 'leased';
      readonly volume: MountedVolume;
      readonly directory: string;
      readonly held: readonly HeldLease[];
      readonly candidates: readonly Candidate[];
    };

type Releasing =
  | { readonly kind: 'lost'; readonly detail: string }
  | { readonly kind: 'wait'; readonly claim: LifecycleClaim }
  | {
      readonly kind: 'claimed';
      readonly volume: MountedVolume;
      readonly held: readonly HeldLease[];
      readonly candidates: readonly Candidate[];
    };

/** The lock refused for want of permission, as inside a cowshed sandbox; else a volume error. */
type LockedError = RamTempError | { kind: 'lock-denied'; detail: string };

export class RamTempVolume {
  /** The RAM disk's size, which with the attaching user identifies this module's disks on the host. */
  private readonly sectors: number;
  /** The claims and held verdicts the lock admits changes to, beside the lock file. */
  private readonly ledgerPath: string;
  /**
   * Where optional RAM's ordinary fallback makes its owned scratch: the lock's own directory, which an unavailable answer
   * has just written. For the user's volume that is `/private/tmp`, outside every checkout, so no Git or Cargo walk from
   * the command's temp files finds the workspace it builds; the workspace's runtime alias resolves into
   * `<mount>/.cowshed/run`, inside the checkout, and the inherited TMPDIR may be RAM. A test namespace's lock lies in
   * its own root, so its ordinary scratch does too.
   */
  readonly ordinaryParent: string;

  constructor(
    private readonly paths: RamTempPaths,
    capacityBytes: number,
    private readonly commands: HostCommands,
    private readonly pid: number = process.pid,
    private readonly isAlive: (pid: number) => boolean = processIsAlive,
    private readonly policy: 'required' | 'optional' = 'required',
  ) {
    this.sectors = Math.ceil(capacityBytes / SECTOR_BYTES);
    this.ledgerPath = `${paths.lockFile}.ledger`;
    this.ordinaryParent = dirname(paths.lockFile);
  }

  /**
   * Take a lease: create the volume if no task holds one, and a private directory on it. A dead
   * lease found on the way is settled through `subtrees`, the supervisor that owns the caller.
   */
  async acquire(subtrees: SubtreeOwner): Promise<Result<RamTempAcquisition, RamTempError>> {
    const bytes = leaseDirectoryBytes(this.paths.mountpoint, this.pid);
    if (bytes > this.paths.leaseBytesMax) {
      return {
        ok: false,
        error: {
          kind: 'lease-path-budget',
          mountpoint: this.paths.mountpoint,
          pid: this.pid,
          bytes,
          most: this.paths.leaseBytesMax,
        },
      };
    }
    const readiness = await rootReadiness(this.paths);
    if (!readiness.ok) return readiness;
    const op: Operation = { owner: { pid: this.pid, token: randomUUID() }, subtrees, claimed: false, made: null };
    let result: Result<RamTempAcquisition, LockedError>;
    try {
      result = await this.lease(op);
    } catch (error) {
      result = failed('lease', errorText(error));
    }
    const ended = await this.end(op);
    if (result.ok) {
      if (ended.ok) {
        return result;
      }
      // Its claims could not be dropped, so the lease is not handed out; the one made goes back.
      const undone = await this.undo(op);
      return { ok: false, error: alsoFailed(ended.error, undone) };
    }
    // The caller gets no lease, so the one made for it goes; a returned lease is never touched.
    const cleanup = [...(ended.ok ? [] : [ended.error]), ...(await this.undo(op))];
    if (result.error.kind === 'ram-unavailable' && this.policy === 'optional' && cleanup.length === 0) {
      return { ok: true, value: { kind: 'unavailable', reason: result.error.reason, detail: result.error.detail } };
    }
    if (result.error.kind !== 'lock-denied') {
      return { ok: false, error: alsoFailed(result.error, cleanup) };
    }
    if (cleanup.length === 0) {
      return { ok: true, value: { kind: 'sandboxed', detail: result.error.detail } };
    }
    const denied: ProvisionFailed = { kind: 'provision-failed', step: 'lock', detail: result.error.detail };
    return { ok: false, error: alsoFailed(denied, cleanup) };
  }

  /** Make a lease directory for `op`'s caller, recorded so a failed acquire takes it back. */
  private async make(op: Operation): Promise<string> {
    const directory = await mkdtemp(join(this.paths.mountpoint, `${this.pid}-`));
    const identity = await leaseIdentity(directory);
    op.made = identity === null ? null : { directory, identity };
    return directory;
  }

  /**
   * Undo a failed acquire: take back the lease made for it, which may have been the volume's last
   * with every release already past it, so then detach the volume as the last release would have.
   * Answers what failed on the way.
   */
  private async undo(op: Operation): Promise<RamTempError[]> {
    const made = op.made !== null;
    const unmade = await this.unmake(op);
    if (!unmade.ok) {
      return [unmade.error];
    }
    if (!made) {
      return [];
    }
    const idle = await this.locked(
      op,
      'detach',
      async (ledger, volume): Promise<Step<MountedVolume | null>> =>
        volume !== null && volume.leases.length === 0 && ledger.lifecycle === null && ledger.claims.length === 0
          ? { ledger: { ...ledger, lifecycle: { kind: 'detach', owner: op.owner } }, value: volume }
          : { ledger, value: null },
    );
    const detached = !idle.ok || idle.value === null ? idle : await this.detach(op, idle.value);
    const failures: RamTempError[] = [];
    if (!detached.ok) {
      failures.push(
        detached.error.kind === 'lock-denied'
          ? { kind: 'provision-failed', step: 'lock', detail: detached.error.detail }
          : detached.error,
      );
    }
    const ended = await this.end(op);
    if (!ended.ok) {
      failures.push(ended.error);
    }
    return failures;
  }

  /**
   * Take back the lease made for a caller that never got it. No task has used it, so it is empty:
   * `rmdir` removes only that, follows no link, and refuses a directory with a mount on it. A
   * directory that is no longer the one made, by identity, is left where it is and named.
   */
  private async unmake(op: Operation): Promise<Result<void, RamTempError>> {
    const made = op.made;
    if (made === null) {
      return { ok: true, value: undefined };
    }
    try {
      const identity = await leaseIdentity(made.directory);
      if (identity !== null && identity !== made.identity) {
        return failed('lease rollback', `${made.directory} is no longer the lease made for this task; left in place`);
      }
      if (identity !== null) {
        await rmdir(made.directory);
      }
      op.made = null;
      return { ok: true, value: undefined };
    } catch (error) {
      return failed('lease rollback', `${made.directory}: ${errorText(error)}`);
    }
  }

  private async lease(op: Operation): Promise<Result<RamTempAcquisition, LockedError>> {
    let waited: ClaimWait | null = null;
    for (;;) {
      const taken = await this.locked(op, 'lease', async (ledger, volume): Promise<Step<Taken>> => {
        if (ledger.lifecycle !== null) {
          return { ledger, value: { kind: 'wait', claim: ledger.lifecycle } };
        }
        if (volume === null) {
          return { ledger: { ...ledger, lifecycle: { kind: 'create', owner: op.owner } }, value: { kind: 'create' } };
        }
        // The lease exists before the lock drops, so no release finds the volume empty under it.
        const directory = await this.make(op);
        const claimed = await this.claim(ledger, volume, op.owner, null, false);
        return {
          ledger: claimed.ledger,
          value: { kind: 'leased', volume, directory, held: claimed.held, candidates: claimed.candidates },
        };
      });
      if (!taken.ok) {
        return taken;
      }
      const value = taken.value;
      switch (value.kind) {
        case 'wait': {
          // Another caller is creating or detaching the volume: look again once it has.
          const yielded = await this.yieldTo(op, value.claim, waited);
          if (!yielded.ok) return yielded;
          waited = yielded.value;
          continue;
        }
        case 'create':
          return await this.create(op);
        case 'leased': {
          const settled = await this.settle(op, value.volume, value.candidates);
          if (!settled.ok) {
            return settled;
          }
          if (value.candidates.length > 0) {
            const published = await this.locked(op, 'lease', async (ledger, current) => ({
              ledger: settledLedger(ledger, current, op.owner, settled.value.verdicts),
              value: undefined,
            }));
            if (!published.ok) {
              return published;
            }
          }
          return {
            ok: true,
            value: {
              kind: 'leased',
              lease: {
                directory: value.directory,
                mountpoint: this.paths.mountpoint,
                attach: value.volume.attach,
              },
              held: [...value.held, ...settled.value.held].map((lease) => lease.text),
              reaped: [...settled.value.reaped],
              retained: [...settled.value.retained],
              gaps: [...settled.value.gaps],
            },
          };
        }
      }
    }
  }

  /** With this caller's creation claim published: provision the volume, then take the first lease on it. */
  private async create(op: Operation): Promise<Result<RamTempAcquisition, LockedError>> {
    const provisioned = await this.provisionVolume(op);
    if (!provisioned.ok) {
      return provisioned;
    }
    const placed = await this.locked(
      op,
      'lease',
      async (ledger, volume): Promise<Step<Result<RamTempLease, RamTempError>>> => {
        const next = withoutClaimsOf(ledger, op.owner);
        if (ledger.lifecycle?.owner.token !== op.owner.token) {
          return {
            ledger: next,
            value: failed(
              'create',
              `the creation claim in ${this.ledgerPath} was lost while the volume was provisioned`,
            ),
          };
        }
        if (volume === null) {
          return {
            ledger: next,
            value: failed('create', `${this.paths.mountpoint} is not the marked volume once provisioned`),
          };
        }
        const directory = await this.make(op);
        return {
          ledger: next,
          value: { ok: true, value: { directory, mountpoint: this.paths.mountpoint, attach: volume.attach } },
        };
      },
    );
    if (!placed.ok) {
      return placed;
    }
    if (!placed.value.ok) {
      return placed.value;
    }
    return {
      ok: true,
      value: {
        kind: 'leased',
        lease: placed.value.value,
        held: [],
        reaped: [],
        retained: [],
        gaps: [],
      },
    };
  }

  /**
   * Record the supervisor's handle on the subtree whose command uses `lease`, beside the lease, so
   * that if this task dies before it retires that subtree, whoever settles the dead lease can hand
   * the handle back to the supervisor instead of guessing from a census who worked there.
   */
  async record(lease: RamTempLease, handle: SubtreeHandle): Promise<Result<void, RamTempError>> {
    try {
      await writeFile(subtreeRecordPath(lease.directory), `${JSON.stringify(handle)}\n`, { mode: 0o600 });
      return { ok: true, value: undefined };
    } catch (error) {
      return failed('record subtree', errorText(error));
    }
  }

  /**
   * End a lease; the last lease detaches the volume, returning its RAM. `keep` archives the lease's scratch instead of
   * deleting it: its run did not pass. A dead lease found on the way is settled through `subtrees`. Answers what became
   * of every scratch this release reclaimed, its own included.
   */
  async release(lease: RamTempLease, keep: boolean, subtrees: SubtreeOwner): Promise<Result<string[], RamTempError>> {
    const op: Operation = { owner: { pid: this.pid, token: randomUUID() }, subtrees, claimed: false, made: null };
    let result: Result<string[], LockedError>;
    try {
      result = await this.releaseLease(op, lease, keep);
    } catch (error) {
      result = failed('release', errorText(error));
    }
    const ended = await this.end(op);
    if (result.ok) {
      return ended.ok ? result : ended;
    }
    const primary: RamTempError =
      result.error.kind === 'lock-denied'
        ? { kind: 'provision-failed', step: 'lock', detail: result.error.detail }
        : result.error;
    return { ok: false, error: alsoFailed(primary, ended.ok ? [] : [ended.error]) };
  }

  private async releaseLease(
    op: Operation,
    lease: RamTempLease,
    keep: boolean,
  ): Promise<Result<string[], LockedError>> {
    const own = basename(lease.directory);
    let waited: ClaimWait | null = null;
    for (;;) {
      const taken = await this.locked(op, 'release', async (ledger, volume): Promise<Step<Releasing>> => {
        // A lease that went while its task held it is said, never released as though it were there.
        const held = leaseOn(lease, volume);
        if ('lost' in held) {
          return { ledger, value: { kind: 'lost', detail: held.lost } };
        }
        if (ledger.lifecycle !== null) {
          return { ledger, value: { kind: 'wait', claim: ledger.lifecycle } };
        }
        const claimed = await this.claim(ledger, held.on, op.owner, own, keep);
        return {
          ledger: claimed.ledger,
          value: { kind: 'claimed', volume: held.on, held: claimed.held, candidates: claimed.candidates },
        };
      });
      if (!taken.ok) {
        return taken;
      }
      const value = taken.value;
      switch (value.kind) {
        case 'lost':
          return {
            ok: false,
            error: { kind: 'lease-lost', lease: lease.directory, mountpoint: lease.mountpoint, detail: value.detail },
          };
        case 'wait': {
          const yielded = await this.yieldTo(op, value.claim, waited);
          if (!yielded.ok) return yielded;
          waited = yielded.value;
          continue;
        }
        case 'claimed': {
          const settled = await this.settle(op, value.volume, value.candidates);
          if (!settled.ok) {
            return settled;
          }
          const idle = await this.locked(op, 'release', async (ledger, current): Promise<Step<boolean>> => {
            const next = settledLedger(ledger, current, op.owner, settled.value.verdicts);
            // The last lease is gone, and nobody else settles one or changes the volume: detach it.
            return current?.key === value.volume.key &&
              current.leases.length === 0 &&
              next.lifecycle === null &&
              next.claims.length === 0
              ? { ledger: { ...next, lifecycle: { kind: 'detach', owner: op.owner } }, value: true }
              : { ledger: next, value: false };
          });
          if (!idle.ok) {
            return idle;
          }
          const kept = [...value.held, ...settled.value.held].find((lease) => lease.name === own);
          if (kept !== undefined) {
            return failed('release', kept.text);
          }
          if (!idle.value) {
            return { ok: true, value: [...settled.value.retained] };
          }
          const detached = await this.detach(op, value.volume);
          if (!detached.ok) {
            return detached;
          }
          // The volume's pages go with it, its archives included: what the lines above kept lived until now.
          return {
            ok: true,
            value: [
              ...settled.value.retained,
              ...(settled.value.retained.length > 0
                ? [`${this.paths.mountpoint} detached with its last lease, and every archive on it went with it`]
                : []),
            ],
          };
        }
      }
    }
  }

  /**
   * Wait out another caller's creation or detach. Liveness of its owner is its pid's, so a crashed
   * owner whose pid was reused holds every caller here; past each `CLAIM_WAIT_NOTICE_MS` on the same
   * claim, stderr names it with the wait so far.
   */
  private async waitOut(claim: LifecycleClaim, waited: ClaimWait | null): Promise<ClaimWait> {
    const now = performance.now();
    const wait = waited?.token === claim.owner.token ? waited : { token: claim.owner.token, since: now, notices: 0 };
    const elapsed = now - wait.since;
    const due = Math.floor(elapsed / CLAIM_WAIT_NOTICE_MS);
    if (due > wait.notices) {
      process.stderr.write(
        `RAM temp volume: waited ${(elapsed / 1000).toFixed(1)} s on the ${claim.kind} claim of pid ${claim.owner.pid} (token ${claim.owner.token}) in ${this.ledgerPath}${claim.pending === undefined ? `; if that pid is not ${claim.kind === 'create' ? 'creating' : 'detaching'} ${this.paths.mountpoint}, remove the ledger once no task uses the volume` : `; that caller still waits on its detach of ${claim.pending.physical}, whose custody the ledger keeps`}\n`,
      );
    }
    await sleep(LOCK_RETRY_MS);
    return { ...wait, notices: Math.max(wait.notices, due) };
  }

  /**
   * After a failed task: whether the volume ran out of space, which is then the error to report. A lease no longer on
   * its volume is said by its release, and a volume that cannot be read has no space to report.
   */
  async fullness(lease: RamTempLease): Promise<RamTempError | null> {
    const space = await this.space(lease);
    if (space.kind !== 'measured' || space.freeBytes >= FULL_FREE_BYTES) {
      return null;
    }
    const { mountpoint, capacityBytes, freeBytes } = space;
    return { kind: 'volume-full', mountpoint, capacityBytes, freeBytes };
  }

  /**
   * The lease's volume and the space left on it now, measured only while the mountpoint holds the attach the lease was
   * taken on and the lease is still on it: a bare mountpoint's statfs reads the host's Data volume, whose free space is
   * not the lease's. A lease that went with its volume, or from it, is `lost`; a read that failed is `unread`.
   */
  async space(lease: RamTempLease): Promise<TempVolumeSpace> {
    const { directory, mountpoint } = lease;
    try {
      const observed = await this.observe();
      if (!observed.ok) {
        const detail =
          observed.error.kind === 'lock-denied' ? observed.error.detail : describeRamTempError(observed.error);
        return { kind: 'unread', lease: directory, mountpoint, detail };
      }
      const held = leaseOn(lease, observed.value);
      if ('lost' in held) {
        return { kind: 'lost', lease: directory, mountpoint, detail: held.lost };
      }
      const fs = await statfs(mountpoint);
      return {
        kind: 'measured',
        lease: directory,
        mountpoint,
        capacityBytes: fs.blocks * fs.bsize,
        freeBytes: fs.bavail * fs.bsize,
      };
    } catch (error) {
      return { kind: 'unread', lease: directory, mountpoint, detail: errorText(error) };
    }
  }

  private async lock(): Promise<Result<{ close(): Promise<void> }, { code: string | undefined; message: string }>> {
    for (;;) {
      try {
        const handle = await open(
          this.paths.lockFile,
          constants.O_RDWR | constants.O_CREAT | constants.O_NOFOLLOW | constants.O_NONBLOCK | O_EXLOCK,
          0o600,
        );
        return { ok: true, value: handle };
      } catch (error) {
        const code = errorCode(error);
        if (code !== 'EAGAIN' && code !== 'EWOULDBLOCK') {
          return { ok: false, error: { code, message: errorText(error) } };
        }
        // Blocking O_EXLOCK opens consume libuv workers. Four waiters can occupy the entire
        // pool while the holder's read/close waits for a worker, deadlocking every host task.
        await sleep(LOCK_RETRY_MS);
      }
    }
  }

  /**
   * Yield to another caller's lifecycle claim. Every claim is queued behind, a detach whose live caller still waits on
   * its facade included. Reported custody, or waiting custody whose caller died, admits nothing until its exact
   * registry entry is gone: the caller's liveness only chooses between queueing and reporting, never clears custody.
   */
  private async yieldTo(
    op: Operation,
    claim: LifecycleClaim,
    waited: ClaimWait | null,
  ): Promise<Result<ClaimWait | null, LockedError>> {
    const pending = claim.pending;
    if (pending === undefined || (pending.phase === 'waiting' && this.isAlive(claim.owner.pid))) {
      return { ok: true, value: await this.waitOut(claim, waited) };
    }
    const reconciled = await this.reconcileDetach(op, claim);
    return reconciled.ok ? { ok: true, value: waited } : reconciled;
  }

  /** A detach nobody waits on any more can be reconciled only against its exact registry identity, outside the lock. */
  private async reconcileDetach(op: Operation, claim: LifecycleClaim): Promise<Result<void, LockedError>> {
    const pending = claim.pending;
    if (pending === undefined) return { ok: true, value: undefined };
    if (pending.registryEntry !== null) {
      const registry = await this.commands.run('/usr/sbin/ioreg', ['-r', '-c', 'IOHDIXHDDriveOutKernel', '-l', '-w0']);
      if (registry.status !== 0) return failed('detach custody', commandText(registry));
      const disks = ramDisksIn(registry.stdout, '');
      if (!disks.ok) return failed('detach custody', disks.error);
      if (!disks.value.some((disk) => disk.entry === pending.registryEntry)) {
        return this.locked(op, 'detach custody', async (ledger) => ({
          ledger: sameDetach(ledger.lifecycle, claim.owner, pending) ? { ...ledger, lifecycle: null } : ledger,
          value: undefined,
        }));
      }
    }
    return failed(
      'detach custody',
      `${pending.physical} (registry ${pending.registryEntry ?? 'unbound'}, ${pending.phase} by pid ${claim.owner.pid}, facade ${pending.facadePid ?? 'unrecorded'}) still has pending detach authority in ${this.ledgerPath}; no lease admitted until terminal completion${pending.registryEntry === null ? '' : ' or that registry entry disappears'}`,
    );
  }

  /** Publish authority before spawn: a crash anywhere after this point must not admit a lease past the detach. */
  private async runDetach(
    op: Operation,
    physical: string,
    registryEntry: string | null,
  ): Promise<Result<CommandOutput, RamTempError>> {
    const custody: DetachCustody = { physical, registryEntry, phase: 'waiting', facadePid: null };
    const begun = await this.locked(op, 'detach custody', async (ledger): Promise<Step<boolean>> => {
      const claim = ledger.lifecycle;
      if (claim?.owner.token !== op.owner.token || claim.pending !== undefined) return { ledger, value: false };
      return { ledger: { ...ledger, lifecycle: { ...claim, pending: custody } }, value: true };
    });
    if (!begun.ok)
      return begun.error.kind === 'lock-denied'
        ? failed('lock', begun.error.detail)
        : { ok: false, error: begun.error };
    if (!begun.value)
      return failed('detach custody', 'the operation no longer owns an idle lifecycle claim; nothing detached');
    let output: CommandOutput;
    try {
      if (registryEntry === null)
        throw new Error('detach has no captured registry identity; retained without dispatch');
      output = await this.commands.detachRam(physical, registryEntry);
    } catch (error) {
      // Whether a child started is unknown, so its custody is reported, never dropped.
      const reported = await this.reportDetach(op, custody, null);
      const detail = `the detach of ${physical} gave no answer (${errorText(error)}); its custody stays reported in ${this.ledgerPath}`;
      return failed('detach custody', reported.ok ? detail : `${detail}; ${describeRamTempError(reported.error)}`);
    }
    const pending = output.pending;
    if (pending === undefined) {
      const finished = await this.finishDetach(op, custody, false);
      return finished.ok ? { ok: true, value: output } : finished;
    }
    const retained = await this.reportDetach(op, custody, pending.pid);
    // The gateway owns the mutation. Only its authoritative terminal/not-started reply clears this request; a lost connection does not.
    void pending.completion
      .then(async (terminal) => {
        if (terminal === null) return;
        const finished = await this.finishDetach(op, custody, true);
        process.stderr.write(
          `RAM temp volume: detach of ${physical} completed after its caller stopped waiting (${commandText(terminal)}); ${finished.ok ? 'terminal receipt processed' : describeRamTempError(finished.error)}\n`,
        );
      })
      .catch((error: unknown) => {
        process.stderr.write(
          `RAM temp volume: detach completion for ${physical} could not publish: ${errorText(error)}\n`,
        );
      });
    return retained.ok ? { ok: true, value: output } : retained;
  }

  /** Only this operation's exact terminal receipt can clear its pending authority; a late receipt cannot clear a successor. */
  private async finishDetach(
    op: Operation,
    custody: DetachCustody,
    release: boolean,
  ): Promise<Result<void, RamTempError>> {
    const finished = await this.locked(op, 'detach completion', async (ledger) => {
      const claim = ledger.lifecycle;
      if (claim === null || !sameDetach(claim, op.owner, custody)) return { ledger, value: undefined };
      return {
        ledger: { ...ledger, lifecycle: release ? null : { kind: claim.kind, owner: claim.owner } },
        value: undefined,
      };
    });
    if (finished.ok) return finished;
    return finished.error.kind === 'lock-denied'
      ? failed('lock', finished.error.detail)
      : { ok: false, error: finished.error };
  }

  /** Its caller stops waiting on the detach: the exact custody becomes reported, its facade pid kept for diagnosis. */
  private async reportDetach(
    op: Operation,
    custody: DetachCustody,
    facadePid: number | null,
  ): Promise<Result<void, RamTempError>> {
    const reported = await this.locked(op, 'detach custody', async (ledger): Promise<Step<undefined>> => {
      const claim = ledger.lifecycle;
      if (claim === null || !sameDetach(claim, op.owner, custody)) return { ledger, value: undefined };
      return {
        ledger: { ...ledger, lifecycle: { ...claim, pending: { ...custody, phase: 'reported', facadePid } } },
        value: undefined,
      };
    });
    if (reported.ok) return reported;
    return reported.error.kind === 'lock-denied'
      ? failed('lock', reported.error.detail)
      : { ok: false, error: reported.error };
  }

  /** Drop finished claims, never the authority of a detach whose caller merely stopped waiting. */
  private async end(op: Operation): Promise<Result<void, RamTempError>> {
    if (!op.claimed) {
      return { ok: true, value: undefined };
    }
    const dropped = await this.locked(op, 'claim release', async (ledger) => ({
      ledger: withoutClaimsOf(ledger, op.owner),
      value: undefined,
    }));
    if (dropped.ok) {
      return dropped;
    }
    return dropped.error.kind === 'lock-denied'
      ? failed('lock', dropped.error.detail)
      : { ok: false, error: dropped.error };
  }

  /**
   * One step under the lock: read the ledger, void the claims of dead owners and the verdicts of
   * another volume or a lease that is gone, observe the volume, and publish what `change` returns.
   * `change` reads metadata and takes or names directories; it runs no host command.
   */
  private async locked<T>(
    op: Operation,
    step: string,
    change: (ledger: Ledger, volume: MountedVolume | null) => Promise<Step<T>>,
  ): Promise<Result<T, LockedError>> {
    for (;;) {
      // Registry work stays outside the lock. A changed locked identity invalidates this
      // snapshot; release the lock and prove the replacement afresh before any mutation.
      const initialLedger = await this.readLedger();
      if (!initialLedger.ok) return initialLedger;
      const before = await this.observe();
      if (!before.ok) return before;
      if (before.value !== null && op.provenance !== before.value.key) {
        const validated = await this.validateProvenance(before.value);
        if (!validated.ok) {
          // The last release detaches outside the lock, so the volume can go between its observation and this
          // registry read: a volume no longer there is looked at again, and only the one still mounted is refused.
          const again = await this.observe();
          if (!again.ok) return again;
          if (again.value?.key !== before.value.key) continue;
          return validated;
        }
        op.provenance = before.value.key;
      }
      const lock = await this.lock();
      if (!lock.ok) {
        return lock.error.code === 'EPERM'
          ? { ok: false, error: { kind: 'lock-denied', detail: `${this.paths.lockFile}: ${lock.error.message}` } }
          : failed('lock', lock.error.message);
      }
      try {
        const read = await this.readLedger();
        if (!read.ok) return read;
        const observed = await this.observe();
        if (!observed.ok) return observed;
        const volume = observed.value;
        if (volume !== null && (volume.key !== before.value?.key || op.provenance !== volume.key)) {
          op.provenance = undefined;
          continue;
        }
        const next = await change(prune(read.value.ledger, volume, this.isAlive), volume);
        const text = ledgerText(next.ledger);
        if (text !== (read.value.text ?? ledgerText(EMPTY_LEDGER))) await publishLedger(this.ledgerPath, text);
        op.claimed =
          next.ledger.lifecycle?.owner.token === op.owner.token ||
          next.ledger.claims.some((claim) => claim.owner.token === op.owner.token);
        return { ok: true, value: next.value };
      } catch (error) {
        return failed(step, errorText(error));
      } finally {
        await lock.value.close();
      }
    }
  }

  /** The ledger as published, and its text; none yet is an empty one. Only this module writes it, whole. */
  private async readLedger(): Promise<Result<{ ledger: Ledger; text: string | null }, RamTempError>> {
    let text: string;
    try {
      const handle = await open(this.ledgerPath, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        text = await handle.readFile('utf8');
      } finally {
        await handle.close();
      }
    } catch (error) {
      if (errorCode(error) === 'ENOENT') {
        return { ok: true, value: { ledger: EMPTY_LEDGER, text: null } };
      }
      return failed('ledger', `${this.ledgerPath}: ${errorText(error)}`);
    }
    // An unreadable ledger may hold detached-child authority even after every caller died.
    // Preserve it and refuse admission rather than suggesting pid death authorizes its removal.
    const refused = (detail: string) =>
      failed<{ ledger: Ledger; text: string | null }>(
        'ledger',
        `${this.ledgerPath} ${detail}; retained; no lease or detach admitted while its authority is unreadable`,
      );
    let wire: unknown;
    try {
      wire = JSON.parse(text);
    } catch (error) {
      return refused(`is not JSON: ${errorText(error)}`);
    }
    if (isRecord(wire) && wire.version !== 1 && wire.version !== 2) {
      return refused(`has unsupported version ${JSON.stringify(wire.version)}`);
    }
    const ledger = parseLedger(wire);
    return ledger === undefined ? refused("is not this module's ledger") : { ok: true, value: { ledger, text } };
  }

  /**
   * The volume when it is mounted at the mountpoint, identified by its marker; else null. Its key
   * names this attach: the root's and the marker's identity and birth, and the marker's change
   * time, which nothing changes once it is written. The root's own times change with every lease.
   */
  private async observe(): Promise<Result<MountedVolume | null, LockedError>> {
    const markerPath = join(this.paths.mountpoint, MARKER_FILE);
    let markerText: string;
    try {
      const handle = await open(markerPath, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        markerText = await handle.readFile('utf8');
      } finally {
        await handle.close();
      }
    } catch (error) {
      if (errorCode(error) === 'ENOENT') return { ok: true, value: null };
      return errorCode(error) === 'EPERM' ? denied(markerPath, error) : failed('provenance', errorText(error));
    }
    if (markerText.trim() === this.paths.volumeName) {
      return {
        ok: false,
        error: {
          kind: 'ram-unavailable',
          reason: 'unproven-owner',
          detail: `${this.paths.mountpoint} has a legacy name-only marker; no lease, reclaim or detach admitted`,
        },
      };
    }
    let marker: unknown;
    try {
      marker = JSON.parse(markerText);
    } catch (error) {
      return failed('provenance', `malformed volume marker: ${errorText(error)}`);
    }
    if (
      !isRecord(marker) ||
      marker.version !== 1 ||
      marker.volumeName !== this.paths.volumeName ||
      marker.owner !== 'gateway' ||
      typeof marker.registryEntry !== 'string' ||
      !/^0x[0-9a-f]+$/.test(marker.registryEntry)
    ) {
      return failed('provenance', 'the marker is not version-1 gateway provenance for this volume');
    }
    let root: BigIntStats;
    let parent: BigIntStats;
    let mark: BigIntStats;
    let names: string[];
    try {
      [root, parent, mark] = await Promise.all([
        stat(this.paths.mountpoint, { bigint: true }),
        stat(join(this.paths.mountpoint, '..'), { bigint: true }),
        lstat(markerPath, { bigint: true }),
      ]);
      names = await readdir(this.paths.mountpoint);
    } catch (error) {
      if (errorCode(error) === 'EPERM') return denied(this.paths.mountpoint, error);
      // The marker was read, then the volume went: the last release detaches it outside the lock, and its mountpoint
      // with it. What is mounted now is nothing, as when the marker is not there to read.
      if (errorCode(error) === 'ENOENT') return { ok: true, value: null };
      throw error;
    }
    if (root.dev === parent.dev || !mark.isFile())
      return failed('provenance', 'gateway marker is not on a separately mounted RAM volume');
    const rootIdentity = `root ${root.dev}:${root.ino}:${root.birthtimeNs}`;
    const registry = `registry ${marker.registryEntry}`;
    return {
      ok: true,
      value: {
        registryEntry: marker.registryEntry,
        key: [rootIdentity, `marker ${mark.dev}:${mark.ino}:${mark.birthtimeNs}:${mark.ctimeNs}`, registry].join(' '),
        attach: `${rootIdentity} ${registry}`,
        device: Number(root.dev),
        leases: names.filter((name) => LEASE_NAME.test(name)),
      },
    };
  }

  /** Prove this immutable marker belongs to the exact live attachment, outside the lock. */
  private async validateProvenance(volume: MountedVolume): Promise<Result<void, RamTempError>> {
    const registry = await this.commands.run('/usr/sbin/ioreg', ['-r', '-c', 'IOHDIXHDDriveOutKernel', '-l', '-w0']);
    const mounts = await this.commands.run('/sbin/mount', []);
    if (registry.status !== 0 || mounts.status !== 0)
      return failed('provenance', commandText(registry.status !== 0 ? registry : mounts));
    const parsed = ramDisksIn(registry.stdout, mounts.stdout);
    if (!parsed.ok) return failed('provenance', parsed.error);
    const disk = parsed.value.find((candidate) => candidate.entry === volume.registryEntry);
    return disk !== undefined &&
      disk.physical !== null &&
      disk.uid === this.paths.uid &&
      disk.sectors === this.sectors &&
      disk.mountpoints.includes(this.paths.mountpoint)
      ? { ok: true, value: undefined }
      : failed(
          'provenance',
          `recorded registry entry ${volume.registryEntry} no longer names this mounted volume; no lease or detach admitted`,
        );
  }

  /** Capture only the exact whole disk this attach answered, before any filesystem mutation. */
  private async attachedIdentity(physical: string): Promise<Result<AttachedRamDisk, ProvisionFailed>> {
    const registry = await this.commands.run('/usr/sbin/ioreg', ['-r', '-c', 'IOHDIXHDDriveOutKernel', '-l', '-w0']);
    if (registry.status !== 0) return failed('attach identity', commandText(registry));
    const parsed = ramDisksIn(registry.stdout, '');
    if (!parsed.ok) return failed('attach identity', parsed.error);
    const matches = parsed.value.filter((disk) => disk.physical === physical);
    const [disk] = matches;
    return matches.length === 1 &&
      disk !== undefined &&
      disk.physical !== null &&
      disk.uid === this.paths.uid &&
      disk.sectors === this.sectors
      ? { ok: true, value: { ...disk, physical: disk.physical } }
      : failed(
          'attach identity',
          `${physical} is not the unique registry identity of this gateway attach; no provisioning or guessed detach`,
        );
  }

  /**
   * Under the lock: claim `own`, the caller's ending lease if any, and every dead lease no other
   * caller is settling. A dead lease whose cached verdict still validates is named held and left
   * unclaimed: that proof costs `lstat` calls, not a host-wide probe, so no caller waits on it.
   */
  private async claim(
    ledger: Ledger,
    volume: MountedVolume,
    owner: ClaimOwner,
    own: string | null,
    keepOwn: boolean,
  ): Promise<Claimed> {
    const held: HeldLease[] = [];
    const candidates: Candidate[] = [];
    const claims = [...ledger.claims];
    let verdicts = ledger.held;
    for (const name of volume.leases) {
      if ((name !== own && this.isAlive(leasePid(name))) || ledger.claims.some((claim) => claim.name === name)) {
        continue;
      }
      const directory = join(this.paths.mountpoint, name);
      const identity = await leaseIdentity(directory);
      if (identity === null) {
        continue;
      }
      const cached = verdicts.find((entry) => entry.lease === name);
      if (cached !== undefined) {
        const valid = await validateHeldVerdict(cached.verdict, heldContext(volume, name, identity));
        if (valid.ok && valid.value) {
          held.push({
            name,
            text: holdingText(
              directory,
              cached.verdict.holders.map((proof) => proof.description),
            ),
          });
          continue;
        }
        // Changed or unreadable: the verdict goes, and the lease is probed afresh.
        verdicts = verdicts.filter((entry) => entry !== cached);
      }
      claims.push({ name, identity, owner });
      candidates.push({ name, directory, identity, own: name === own, keep: name !== own || keepOwn });
    }
    return { ledger: { ...ledger, claims, held: verdicts }, held, candidates };
  }

  /**
   * Outside the lock: settle the leases this caller claimed. Every dead one's recorded subtree is
   * handed back to the supervisor first, and only then is the pass's one inventory read, so no
   * inventory older than a lease's retirement approves its removal. Anything attached from below a
   * lease keeps it: an image backed by a file there would be left pinned to a deleted file, and
   * detaching it here would race the owner that releases it under its own lease; a mount there is
   * backed elsewhere — a cowshed workspace adopted from a fixture checkout, say — and `rm -r` would
   * delete its contents through the mount. A kept lease gets a verdict to publish, when its
   * holders have cheap lifetime keys. A lease nothing holds is reclaimed once the lock confirms it
   * is as claimed: a kept one's scratch is renamed into the archive (`retain`), any other deleted.
   */
  private async settle(
    op: Operation,
    volume: MountedVolume,
    candidates: readonly Candidate[],
  ): Promise<Result<Settled, LockedError>> {
    if (candidates.length === 0) {
      return { ok: true, value: { held: [], reaped: [], gaps: [], retained: [], verdicts: [] } };
    }
    const reaped: string[] = [];
    const gaps: string[] = [];
    const held: HeldLease[] = [];
    for (const candidate of candidates) {
      // A task retires its own subtree before it releases; a dead one could not. Its subtree is the
      // supervisor's to end, named by the handle the task recorded — never by who works in the lease
      // (specs/cowshed/07_api.md, `JobSubtreeHandle`). A lease with no readable handle recorded none
      // to retire: its task died before its command was admitted, or that admission was refused.
      if (candidate.own) {
        continue;
      }
      const recorded = await readFile(subtreeRecordPath(candidate.directory), 'utf8').catch((error: unknown) => {
        if (errorCode(error) === 'ENOENT') return null;
        throw error;
      });
      const handle = recorded === null ? null : parseHandle(recorded);
      if (handle === null) {
        continue;
      }
      const retired = await op.subtrees.retire(handle, LEFTOVER_GRACE_MS);
      switch (retired.kind) {
        case 'retired':
          reaped.push(...retired.signalled.map(describeSignalled));
          if (retired.signalledIncomplete) {
            gaps.push(`${candidate.directory}: the supervisor may have ended processes it could not name`);
          }
          gaps.push(...retired.gaps.map((gap) => `${candidate.directory}: ${describeCoverageGap(gap)}`));
          break;
        case 'refused':
          // An ended job's supervisor retired its whole tree as it ended, and a spent handle was
          // already retired: either way nothing of that subtree is left to end.
          if (retired.reason !== 'jobEnded' && retired.reason !== 'spentHandle') {
            held.push({
              name: candidate.name,
              text: `${candidate.directory} is kept: its subtree is not this task's to retire (supervisor: ${retired.reason}); it goes once that subtree's job ends`,
            });
          }
          break;
        case 'failed':
          return failed('retire', `${candidate.directory}: ${retired.detail}`);
      }
    }
    const inventory = await this.readInventory();
    if (!inventory.ok) {
      return inventory;
    }
    const verdicts: HeldEntry[] = [];
    const removable: Candidate[] = [];
    for (const candidate of candidates) {
      if (held.some((lease) => lease.name === candidate.name)) {
        continue;
      }
      const below = candidatesBelow(inventory.value, `${candidate.directory}/`);
      if (below.length === 0) {
        removable.push(candidate);
        continue;
      }
      const by = below.map((holder) => holder.description);
      held.push({ name: candidate.name, text: holdingText(candidate.directory, by) });
      // No verdict for a holder without a cheap lifetime key: the next pass probes it again.
      const captured = await captureHeldVerdict(heldContext(volume, candidate.name, candidate.identity), below);
      if (captured.ok && captured.value !== undefined) {
        verdicts.push({ lease: candidate.name, verdict: captured.value });
      }
    }
    const archived: Archived[] = [];
    if (removable.length > 0) {
      const confirmed = await this.locked(
        op,
        'reclaim',
        async (ledger, current): Promise<Step<Candidate[]>> => ({
          ledger,
          value: current?.key === volume.key ? await this.unchanged(ledger, op.owner, removable) : [],
        }),
      );
      if (!confirmed.ok) {
        return confirmed;
      }
      for (const candidate of confirmed.value) {
        // Its subtree was retired above, or had ended: the handle beside the lease is spent either way.
        await rm(subtreeRecordPath(candidate.directory), { force: true });
        if (candidate.keep) {
          // One rename on the volume: the scratch stays whole and readable, and nothing waits on a deletion.
          const archive = `${String(Date.now()).padStart(13, '0')}-${candidate.name}`;
          await mkdir(join(this.paths.mountpoint, ARCHIVE_DIR), { recursive: true, mode: 0o700 });
          await rename(candidate.directory, join(this.paths.mountpoint, ARCHIVE_DIR, archive));
          archived.push({ directory: candidate.directory, archive });
          continue;
        }
        // Nothing holds the lease, so its directories may be made removable first: `rm -r` cannot
        // empty a readonly one. Only this volume's device is changed (see prepareForRemoval).
        await prepareForRemoval(candidate.directory, volume.device);
        await rm(candidate.directory, { recursive: true, force: true });
      }
    }
    const retained = archived.length === 0 ? [] : await this.retain(volume, archived);
    return { ok: true, value: { held, reaped, gaps, retained, verdicts } };
  }

  /**
   * Keep the newest {@link ARCHIVES_KEPT} archives while at least {@link ARCHIVE_FLOOR_SHARE} of the volume is free, and
   * none below that; rename every other one out of the archive and delete it in the background. Answers, for each lease
   * just archived, where its scratch is kept or why it was not. The floor is read once, here, as an archive arrives.
   */
  private async retain(volume: MountedVolume, archived: readonly Archived[]): Promise<string[]> {
    const root = join(this.paths.mountpoint, ARCHIVE_DIR);
    const [fs, names] = await Promise.all([statfs(this.paths.mountpoint), readdir(root)]);
    const freeBytes = fs.bavail * fs.bsize;
    const floorBytes = Math.ceil(fs.blocks * fs.bsize * ARCHIVE_FLOOR_SHARE);
    const archives = names.filter((name) => ARCHIVE_NAME.test(name)).sort();
    const kept = freeBytes < floorBytes ? [] : archives.slice(-ARCHIVES_KEPT);
    await mkdir(join(root, DELETING_DIR), { recursive: true, mode: 0o700 });
    for (const name of archives.filter((candidate) => !kept.includes(candidate))) {
      await rename(join(root, name), join(root, DELETING_DIR, `${this.pid}-${name}`)).catch((error: unknown) => {
        // Another caller's retention renamed it first, and deletes it.
        if (errorCode(error) !== 'ENOENT') throw error;
      });
    }
    this.deleteArchives(volume.device);
    return archived.map(({ directory, archive }) =>
      kept.includes(archive)
        ? `${directory} archived at ${join(root, archive)}: kept until ${ARCHIVES_KEPT} newer archives replace it, less than ${Math.round(floorBytes / MIB)} MiB of the volume is free when one arrives, or the volume detaches with its last lease`
        : freeBytes < floorBytes
          ? `${directory} deleted: the volume has ${Math.round(freeBytes / MIB)} MiB free, below the ${Math.round(floorBytes / MIB)} MiB under which it keeps no archive`
          : `${directory} deleted: ${ARCHIVES_KEPT} newer archives are kept`,
    );
  }

  /**
   * Queue the deletion of the archives this process renamed out, and of any a dead deleter left, behind this process's
   * earlier ones. Nothing waits for it. A failure is said on stderr and leaves its entry for the next deleter.
   */
  private deleteArchives(device: number): void {
    archiveDeletion = archiveDeletion.then(() =>
      this.emptyDeleting(device).catch((error: unknown) => {
        process.stderr.write(
          `RAM temp volume: could not delete an archive in ${join(this.paths.mountpoint, ARCHIVE_DIR, DELETING_DIR)}: ${errorText(error)}; the next archive's retention retries it\n`,
        );
      }),
    );
  }

  private async emptyDeleting(device: number): Promise<void> {
    const root = join(this.paths.mountpoint, ARCHIVE_DIR, DELETING_DIR);
    const names = await readdir(root).catch((error: unknown) => {
      if (errorCode(error) === 'ENOENT') return [];
      throw error;
    });
    for (const name of names) {
      const claim = DELETING_NAME.exec(name);
      if (claim === null) {
        process.stderr.write(`RAM temp volume: ${join(root, name)} is no archive being deleted; left as it is\n`);
        continue;
      }
      let entry = join(root, name);
      const deleter = Number(claim[1]);
      if (deleter !== this.pid) {
        if (this.isAlive(deleter)) continue;
        const mine = join(root, `${this.pid}-${claim[2]}`);
        const taken = await rename(entry, mine).then(
          () => true,
          (error: unknown) => {
            if (errorCode(error) === 'ENOENT') return false;
            throw error;
          },
        );
        if (!taken) continue;
        entry = mine;
      }
      // An archive keeps its lease's modes, so a readonly directory in it is made removable first (see prepareForRemoval).
      await prepareForRemoval(entry, device);
      await rm(entry, { recursive: true, force: true });
    }
  }

  /**
   * Under the lock: the claimed leases still as claimed — this caller's claim on the same
   * directory, and a task still gone. Any other is left for a fresh look, never removed.
   */
  private async unchanged(ledger: Ledger, owner: ClaimOwner, candidates: readonly Candidate[]): Promise<Candidate[]> {
    const confirmed: Candidate[] = [];
    for (const candidate of candidates) {
      const claimed = ledger.claims.some(
        (claim) =>
          claim.name === candidate.name && claim.identity === candidate.identity && claim.owner.token === owner.token,
      );
      if (
        claimed &&
        (candidate.own || !this.isAlive(leasePid(candidate.name))) &&
        (await leaseIdentity(candidate.directory)) === candidate.identity
      ) {
        confirmed.push(candidate);
      }
    }
    return confirmed;
  }

  /** Check the root the volume mounts below, detach what earlier creators left, then attach and provision the volume. */
  private async provisionVolume(op: Operation): Promise<Result<void, RamTempError>> {
    try {
      const [root, parent] = await Promise.all([stat(this.paths.mountpoint), stat(join(this.paths.mountpoint, '..'))]);
      if (root.dev === parent.dev && (await readdir(this.paths.mountpoint)).length !== 0) {
        return {
          ok: false,
          error: {
            kind: 'ram-unavailable',
            reason: 'unproven-owner',
            detail: `${this.paths.mountpoint} is an unmounted directory with unowned contents; nothing removed or hidden by a new mount`,
          },
        };
      }
    } catch (error) {
      if (errorCode(error) !== 'ENOENT') return failed('provenance', errorText(error));
    }
    const root = await rootReadiness(this.paths);
    if (!root.ok) {
      return root;
    }
    const strays = await this.reclaimStrays(op, this.holdersPass());
    if (!strays.ok) {
      return strays;
    }
    const attached = await this.commands.attachRam(this.sectors);
    if (!attached.ok) {
      // An attach whose answer never came may still finish; this caller closed its lease, so the gateway detaches it.
      return typeof attached.error === 'string'
        ? failed('ram-lease', attached.error)
        : { ok: false, error: attached.error };
    }
    // Every path from here ends the lease: kept once the volume is mounted and marked at its mountpoint, released on
    // any failure or throw before that.
    const lease = attached.value;
    const kept = await this.provisionAndKeep(lease).catch((error: unknown) =>
      failed<void>('provision', errorText(error)),
    );
    return kept.ok ? kept : this.rollBack(op, lease, kept.error);
  }

  /** Provision the leased disk, and keep it once its volume is mounted and marked at its mountpoint. */
  private async provisionAndKeep(lease: RamDiskLease): Promise<Result<void, ProvisionFailed>> {
    const identity = await this.attachedIdentity(lease.device);
    const provisioned = identity.ok ? await this.provision(lease.device, identity.value.entry) : identity;
    if (!provisioned.ok) return provisioned;
    const kept = await lease.keep();
    return kept.ok ? kept : failed('ram-lease keep', kept.error);
  }

  /**
   * End the lease of a volume that failed. The gateway detaches the disk on release, by its registry entry and without
   * force, so a busy owned staging volume is retained. A lease lost to a gateway restart answers no release: this caller
   * then detaches the exact disk itself, under its ledger's custody, so no later lease is admitted on a disk the next
   * gateway's startup scan removes.
   */
  private async rollBack(
    op: Operation,
    lease: RamDiskLease,
    failure: ProvisionFailed,
  ): Promise<Result<never, ProvisionFailed>> {
    const released = await lease.release();
    const lost = released.ok ? [] : [released.error];
    const command = released.ok ? released : await this.runDetach(op, lease.device, lease.registryEntry);
    if (!command.ok)
      return failed(failure.step, [failure.detail, ...lost, describeRamTempError(command.error)].join('; '));
    const detached = command.value;
    if (detached.pending !== undefined) {
      return failed(failure.step, [failure.detail, ...lost, commandText(detached)].join('; '));
    }
    // The detach takes every volume of the disk with it. What it leaves at the staging directory and the
    // mountpoint is a bare directory on the host's Data volume each: removed here, and already absent is done.
    const detachFailure =
      detached.status === 0
        ? []
        : [`gateway detach ${lease.device} then failed (${commandText(detached)}); owned staging is retained`];
    const directoryFailures =
      detached.status === 0 ? await removeMountDirectories([this.paths.staging, this.paths.mountpoint]) : [];
    return failed(failure.step, [failure.detail, ...lost, ...detachFailure, ...directoryFailures].join('; '));
  }

  /**
   * Create the volume under its final name, mount it with `mount_apfs` at the private staging
   * directory, and mark it there. Then `umount` that private mount and mount the volume with
   * `mount_apfs` again at the mountpoint, after the marker, so the volume never reaches the
   * mountpoint unmarked. Nothing is renamed (see the module comment). The staging mount is the
   * kernel's, so the kernel releases it: a Disk Arbitration unmount waits in its one queue behind
   * its own probe of the fresh RAM disk (5.1 s for the `zfs` probe alone in `log stream`;
   * 6.5–6.7 s per unmount against 64–69 ms for `umount`, measured 2026-10-15). A crash before
   * the final mount leaves the volume attached at the staging directory or unmounted; the next
   * creator's reclaim detaches either.
   */
  private async provision(physical: string, registryEntry: string): Promise<Result<void, ProvisionFailed>> {
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
      this.paths.volumeName,
      '-nomount',
    ]);
    const volume = operationDisk(added.stdout);
    if (added.status !== 0 || !volume) {
      return failed('diskutil apfs addVolume', commandText(added));
    }
    const staging = this.paths.staging;
    try {
      await mkdir(staging, { recursive: true });
    } catch (error) {
      return failed('staging directory', errorText(error));
    }
    // `noowners`: a fresh volume's root is owned by root, and the marker write is denied without it.
    const mounted = await this.commands.run('/sbin/mount_apfs', ['-o', 'nobrowse,noowners', `/dev/${volume}`, staging]);
    if (mounted.status !== 0) {
      return failed('mount_apfs (staging)', commandText(mounted));
    }
    const atStaging = await this.expectMounted(volume, staging, 'mount_apfs (staging)');
    if (!atStaging.ok) {
      return atStaging;
    }
    try {
      const marker: GatewayVolumeMarker = {
        version: 1,
        volumeName: this.paths.volumeName,
        owner: 'gateway',
        registryEntry,
      };
      await writeFile(join(staging, MARKER_FILE), `${JSON.stringify(marker)}\n`);
    } catch (error) {
      return failed('marker', errorText(error));
    }
    const unmounted = await this.commands.run('/sbin/umount', [staging]);
    if (unmounted.status !== 0) {
      return failed('umount (staging)', commandText(unmounted));
    }
    // `mount_apfs` mounts at a directory of this module's choosing without authorization; `diskutil mount
    // -mountPoint` raised an administrator dialog (see the module comment).
    try {
      await mkdir(this.paths.mountpoint, { recursive: true, mode: 0o700 });
    } catch (error) {
      return failed('mountpoint directory', errorText(error));
    }
    const placing = await this.commands.run('/sbin/mount_apfs', [
      '-o',
      'nobrowse,noowners',
      `/dev/${volume}`,
      this.paths.mountpoint,
    ]);
    if (placing.status !== 0) {
      return failed('mount_apfs (mountpoint)', commandText(placing));
    }
    const placed = await this.expectMounted(volume, this.paths.mountpoint, 'mount_apfs (mountpoint)');
    if (!placed.ok) {
      return placed;
    }
    try {
      await rmdir(staging);
    } catch (error) {
      return failed('staging directory', errorText(error));
    }
    return placed;
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

  /**
   * This volume's RAM disks attached on the host, read from the I/O Registry, which omits no attached disk: the ones
   * `volumeDisks` names. A RAM disk of this user and size that is not this volume's, another volume's, a test's, or an
   * unformatted one nothing recorded, is not its to inventory or detach, and is ignored. One of its own still
   * publishing its media is named, never passed over.
   */
  private async attached(): Promise<Result<AttachedRamDisk[], RamTempError>> {
    const registry = await this.commands.run('/usr/sbin/ioreg', ['-r', '-c', 'IOHDIXHDDriveOutKernel', '-l', '-w0']);
    if (registry.status !== 0) {
      return failed('ioreg (RAM disks)', commandText(registry));
    }
    const table = await this.commands.run('/sbin/mount', []);
    if (table.status !== 0) {
      return failed('mount (mount table)', commandText(table));
    }
    const disks = ramDisksIn(registry.stdout, table.stdout);
    if (!disks.ok) {
      return failed('ioreg (RAM disks)', disks.error);
    }
    const custody = await this.custody();
    if (!custody.ok) {
      return custody;
    }
    const attached: AttachedRamDisk[] = [];
    for (const disk of volumeDisks(
      disks.value,
      this.paths,
      this.sectors,
      (candidate) =>
        custody.value.has(candidate.entry) ||
        (candidate.physical !== null &&
          this.commands.ownsRamDisk?.({ ...candidate, physical: candidate.physical }) === true),
    )) {
      const { physical } = disk;
      if (physical === null) {
        return failed(
          'ioreg (RAM disks)',
          `a RAM disk of uid ${disk.uid} with ${disk.sectors} sectors has published no media yet; look again once its attach settles`,
        );
      }
      attached.push({ ...disk, physical });
    }
    return { ok: true, value: attached };
  }

  /** The registry entries the ledger keeps pending detach custody of: exact, and published before the spawn. */
  private async custody(): Promise<Result<ReadonlySet<string>, RamTempError>> {
    const read = await this.readLedger();
    if (!read.ok) {
      return read;
    }
    const entry = read.value.ledger.lifecycle?.pending?.registryEntry;
    return { ok: true, value: new Set(entry === undefined || entry === null ? [] : [entry]) };
  }

  /**
   * Detach what earlier creators left attached: no marked volume is mounted, so every RAM disk of this volume
   * (`attached`: by its exact name or its exact record) is debris — unformatted and recorded, staged, or mounted
   * without its marker. One carrying a lease, or with anything attached below it, is still in use and stops the
   * creation instead, and so does one its owner holds no receipt for: it is named, never detached.
   */
  private async reclaimStrays(op: Operation, holders: Holders): Promise<Result<void, RamTempError>> {
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
        const held = await holders(`${point}/`);
        if (!held.ok) {
          return held;
        }
        if (held.value.length > 0) {
          return failed('reclaim', holdingText(point, held.value));
        }
      }
      if (this.commands.ownsRamDisk?.(disk) !== true) {
        return {
          ok: false,
          error: {
            kind: 'ram-unavailable',
            reason: 'unproven-owner',
            detail: `${disk.physical} has no exact gateway creation receipt; no reclaim or detach admitted`,
          },
        };
      }
      const command = await this.runDetach(op, disk.physical, disk.entry);
      if (!command.ok) return command;
      const detached = detachOutcome(command.value, disk, `hdiutil detach (stray ${disk.physical})`);
      if (!detached.ok) {
        return detached;
      }
    }
    return { ok: true, value: undefined };
  }

  /**
   * With this caller's detach claim published, which no caller takes a lease past. Every lease is
   * gone, so nothing below the volume should be attached; whatever is would lose its backing file
   * to the detach. The lock confirms the volume, still empty, and the claim before it goes.
   */
  private async detach(op: Operation, volume: MountedVolume): Promise<Result<void, LockedError>> {
    const inventory = await this.readInventory();
    if (!inventory.ok) {
      return inventory;
    }
    const held = candidatesBelow(inventory.value, `${this.paths.mountpoint}/`).map((holder) => holder.description);
    if (held.length > 0) {
      return failed('detach', holdingText(this.paths.mountpoint, held));
    }
    const disks = await this.attached();
    if (!disks.ok) {
      return disks;
    }
    const disk = disks.value.find((candidate) => candidate.mountpoints.includes(this.paths.mountpoint));
    if (!disk) {
      return failed(
        'detach',
        `the I/O Registry lists no RAM disk of uid ${this.paths.uid} with ${this.sectors} sectors mounted at ${this.paths.mountpoint} as volume ${this.paths.volumeName}`,
      );
    }
    const unchanged = await this.locked(
      op,
      'detach',
      async (ledger, current): Promise<Step<boolean>> => ({
        ledger,
        value:
          ledger.lifecycle?.owner.token === op.owner.token &&
          current?.key === volume.key &&
          current.leases.length === 0,
      }),
    );
    if (!unchanged.ok) {
      return unchanged;
    }
    if (!unchanged.value) {
      // A lease or another attach arrived despite the claim: the volume stays for it.
      return { ok: true, value: undefined };
    }
    // Detaching frees the archives' pages anyway, but a deletion of this process's still walking the volume holds it busy.
    await archiveDeletions();
    const command = await this.runDetach(op, disk.physical, disk.entry);
    if (!command.ok) return command;
    const detached = detachOutcome(command.value, disk, 'hdiutil detach');
    if (!detached.ok) {
      return detached;
    }
    // No Disk Arbitration call made the mountpoint, so none removes it: this module does, now its volume is gone.
    const left = await removeMountDirectories([this.paths.mountpoint]);
    return left.length === 0 ? { ok: true, value: undefined } : failed('detach', left.join('; '));
  }

  /** The host's attachments as one pass reads them: one `ioreg` run and one `mount` run. */
  private async readInventory(): Promise<Result<Inventory, RamTempError>> {
    const registry = await this.commands.run('/usr/sbin/ioreg', ['-r', '-c', 'AppleDiskImageDevice', '-l', '-w0']);
    if (registry.status !== 0) {
      return failed('ioreg (attached disk images)', commandText(registry));
    }
    const mounts = await this.commands.run('/sbin/mount', []);
    if (mounts.status !== 0) {
      return failed('mount (mount table)', commandText(mounts));
    }
    return { ok: true, value: { images: diskImages(registry.stdout), mounts: mountPoints(mounts.stdout) } };
  }

  /**
   * The holder check of one creation pass. The first check reads the attachments and every later
   * check in the pass reads the same ones, so a pass runs `ioreg` and `mount` once however many
   * disks it visits. What is attached from below a prefix: images whose backing file is there,
   * and mounts. `hdiutil info` omits attached images while another image attaches or detaches
   * (measured by cowshed's scratch-root sweep), and tests attach images concurrently.
   */
  private holdersPass(): Holders {
    let inventory: Promise<Result<Inventory, RamTempError>> | undefined;
    return async (prefix) => {
      inventory ??= this.readInventory();
      const read = await inventory;
      return read.ok
        ? { ok: true, value: candidatesBelow(read.value, prefix).map((holder) => holder.description) }
        : read;
    };
  }
}

/** What is attached from below `prefix` in `inventory`: images whose backing file is there, and mounts. */
function candidatesBelow(inventory: Inventory, prefix: string): HeldCandidate[] {
  return [
    ...inventory.images
      .filter((entry) => entry.image.startsWith(prefix))
      .map(
        (entry): HeldCandidate => ({
          kind: 'image',
          imagePath: entry.image,
          diskPath: `/dev/${entry.disk}`,
          description: `${entry.image} (/dev/${entry.disk})`,
        }),
      ),
    ...inventory.mounts
      .filter((point) => point.startsWith(prefix))
      .map((point): HeldCandidate => ({ kind: 'mount', mountpoint: point, description: `${point} (mounted)` })),
  ];
}

/**
 * Whether the root is a directory its volume may mount below: the per-user root `cowshed setup` makes, or a test
 * namespace's directory its fixture made, each a directory, not a link, the user's, mode 0700. A link could place the
 * mount outside `/var/folders/`, where mds admits it, and another owner or mode exposes the volume.
 */
async function rootReadiness(paths: RamTempPaths): Promise<Result<void, RamTempError>> {
  const unready = (detail: string): Result<void, RamTempError> => ({
    ok: false,
    error: { kind: 'root-unready', root: paths.root, detail },
  });
  let info: Stats;
  try {
    info = await lstat(paths.root);
  } catch (error) {
    return unready(errorCode(error) === 'ENOENT' ? 'is missing' : `cannot be read (${errorText(error)})`);
  }
  if (!info.isDirectory()) {
    return unready('is not a directory');
  }
  if (info.uid !== paths.uid) {
    return unready(`is owned by uid ${info.uid}, not ${paths.uid}`);
  }
  const mode = info.mode & 0o777;
  return mode === 0o700 ? { ok: true, value: undefined } : unready(`has mode ${mode.toString(8)}, not 700`);
}

/**
 * Remove directories a volume was mounted on, once it is gone. `rmdir` removes only an empty directory, follows no
 * link and refuses one with a mount on it; already absent is done. Answers each failure.
 */
async function removeMountDirectories(directories: readonly string[]): Promise<string[]> {
  const failures: string[] = [];
  for (const directory of directories) {
    try {
      await rmdir(directory);
    } catch (error) {
      if (errorCode(error) !== 'ENOENT') {
        failures.push(`rmdir ${directory} then failed (${errorText(error)})`);
      }
    }
  }
  return failures;
}

/**
 * The volume one read found at the lease's mountpoint, while the lease is still on that attach of it; else why it is
 * not. Every lease's `TMPDIR` is a directory on one attach: once that attach is gone, the path is a bare directory on
 * the host's Data volume, or on a later attach, which a write through `TMPDIR` can make again.
 */
function leaseOn(
  lease: RamTempLease,
  volume: MountedVolume | null,
): { readonly on: MountedVolume } | { readonly lost: string } {
  if (volume === null) {
    return {
      lost: `no marked volume is mounted at ${lease.mountpoint} any more: it was detached while the task held its lease`,
    };
  }
  if (volume.attach !== lease.attach) {
    return {
      lost: `${lease.mountpoint} holds another attach of the volume than the one the lease was taken on: it was detached and made again while the task held its lease`,
    };
  }
  return volume.leases.includes(basename(lease.directory))
    ? { on: volume }
    : { lost: `${lease.directory} is gone from the volume while the task held it` };
}

/** The pid a lease directory is named after: whether its task is alive. */
function leasePid(name: string): number {
  return Number(LEASE_NAME.exec(name)?.[1]);
}

/** A lease directory's identity: device, inode and birth, which a directory made again at its path does not share. */
async function leaseIdentity(directory: string): Promise<string | null> {
  try {
    const info = await lstat(directory, { bigint: true });
    return `${info.dev}:${info.ino}:${info.birthtimeNs}`;
  } catch (error) {
    if (errorCode(error) === 'ENOENT') {
      return null;
    }
    throw error;
  }
}

/** What a held verdict is bound to: this attach of the volume, and this lease directory on it. */
function heldContext(volume: MountedVolume, name: string, identity: string): HeldContext {
  return { volumeKey: volume.key, leaseKey: `${name} ${identity}`, ramDevice: String(volume.device) };
}

/**
 * Dead callers void only claims without pending detach custody. A pending command's authority
 * survives every pid observation; verdicts still belong only to this attach of the volume.
 */
function prune(ledger: Ledger, volume: MountedVolume | null, isAlive: (pid: number) => boolean): Ledger {
  return {
    lifecycle:
      ledger.lifecycle !== null && (ledger.lifecycle.pending !== undefined || isAlive(ledger.lifecycle.owner.pid))
        ? ledger.lifecycle
        : null,
    claims: ledger.claims.filter((claim) => isAlive(claim.owner.pid)),
    held:
      volume === null
        ? []
        : ledger.held.filter(
            (entry) => entry.verdict.context.volumeKey === volume.key && volume.leases.includes(entry.lease),
          ),
  };
}

/** The operation token and registry incarnation, never a pid or reusable BSD name alone, identify a pending detach. */
function sameDetach(claim: LifecycleClaim | null, owner: ClaimOwner, custody: DetachCustody): boolean {
  return (
    claim?.owner.token === owner.token &&
    claim.pending?.physical === custody.physical &&
    claim.pending.registryEntry === custody.registryEntry
  );
}

function withoutClaimsOf(ledger: Ledger, owner: ClaimOwner): Ledger {
  return {
    lifecycle:
      ledger.lifecycle?.owner.token === owner.token && ledger.lifecycle.pending === undefined ? null : ledger.lifecycle,
    claims: ledger.claims.filter((claim) => claim.owner.token !== owner.token),
    held: ledger.held,
  };
}

/** The ledger once a caller has settled its claims: they are dropped, and its verdicts replace older ones. */
function settledLedger(
  ledger: Ledger,
  volume: MountedVolume | null,
  owner: ClaimOwner,
  verdicts: readonly HeldEntry[],
): Ledger {
  const current = verdicts.filter(
    (entry) => volume !== null && entry.verdict.context.volumeKey === volume.key && volume.leases.includes(entry.lease),
  );
  const settled = withoutClaimsOf(ledger, owner);
  return {
    ...settled,
    held: [...settled.held.filter((entry) => !current.some((fresh) => fresh.lease === entry.lease)), ...current],
  };
}

/** The ledger's wire form, version 1. */
function ledgerText(ledger: Ledger): string {
  // Older readers reject version 2 instead of pruning the pending authority by the caller's pid.
  return `${JSON.stringify({ version: ledger.lifecycle?.pending === undefined ? 1 : 2, lifecycle: ledger.lifecycle, claims: ledger.claims, held: ledger.held })}\n`;
}

/** Publish the ledger in one rename, so no reader, and no crash, ever leaves half of it. */
async function publishLedger(path: string, text: string): Promise<void> {
  const staged = `${path}.${randomUUID()}`;
  try {
    await writeFile(staged, text, { flag: 'wx', mode: 0o600 });
    await rename(staged, path);
  } catch (error) {
    await rm(staged, { force: true }).catch((cleanup: unknown) => {
      throw new AggregateError(
        [error, cleanup],
        `${staged}: ${errorText(error)}; removing it failed: ${errorText(cleanup)}`,
      );
    });
    throw error;
  }
}

/**
 * The ledger in its wire form; undefined when it is not one. A malformed held verdict is dropped,
 * so its lease is probed afresh; it never stands for a lease nothing holds.
 */
function parseLedger(value: unknown): Ledger | undefined {
  if (!isRecord(value) || (value.version !== 1 && value.version !== 2)) {
    return undefined;
  }
  const { lifecycle: lifecycleWire, claims: claimsWire, held: heldWire } = value;
  if (!Array.isArray(claimsWire) || !Array.isArray(heldWire)) {
    return undefined;
  }
  let lifecycle: LifecycleClaim | null = null;
  if (lifecycleWire !== null) {
    if (!isRecord(lifecycleWire)) {
      return undefined;
    }
    const { kind } = lifecycleWire;
    const owner = parseOwner(lifecycleWire.owner);
    if ((kind !== 'create' && kind !== 'detach') || owner === undefined) {
      return undefined;
    }
    const pending = lifecycleWire.pending === undefined ? undefined : parseDetachCustody(lifecycleWire.pending);
    if (lifecycleWire.pending !== undefined && pending === undefined) return undefined;
    lifecycle = pending === undefined ? { kind, owner } : { kind, owner, pending };
  }
  if ((value.version === 2) !== (lifecycle?.pending !== undefined)) return undefined;
  const claims: LeaseClaim[] = [];
  for (const entry of claimsWire) {
    if (!isRecord(entry)) {
      return undefined;
    }
    const { name, identity } = entry;
    const owner = parseOwner(entry.owner);
    if (typeof name !== 'string' || !LEASE_NAME.test(name) || typeof identity !== 'string' || owner === undefined) {
      return undefined;
    }
    claims.push({ name, identity, owner });
  }
  const held: HeldEntry[] = [];
  for (const entry of heldWire) {
    if (!isRecord(entry)) {
      continue;
    }
    const { lease } = entry;
    const verdict = parseHeldVerdict(entry.verdict);
    if (typeof lease === 'string' && verdict !== undefined) {
      held.push({ lease, verdict });
    }
  }
  return { lifecycle, claims, held };
}

function parseDetachCustody(value: unknown): DetachCustody | undefined {
  if (!isRecord(value)) return undefined;
  const { physical, registryEntry, phase, facadePid } = value;
  if (
    (phase !== 'waiting' && phase !== 'reported') ||
    typeof physical !== 'string' ||
    !/^\/dev\/disk\d+$/.test(physical) ||
    (registryEntry !== null && (typeof registryEntry !== 'string' || !/^0x[0-9a-f]+$/.test(registryEntry))) ||
    (facadePid !== null && (typeof facadePid !== 'number' || !Number.isSafeInteger(facadePid) || facadePid <= 0))
  )
    return undefined;
  return { physical, registryEntry, phase, facadePid };
}

function parseOwner(value: unknown): ClaimOwner | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const { pid, token } = value;
  return typeof pid === 'number' && Number.isInteger(pid) && pid > 0 && typeof token === 'string'
    ? { pid, token }
    : undefined;
}

/** `directory` stays, with the volume under it: something attached below it still reads its files. */
function holdingText(directory: string, holders: readonly string[]): string {
  return `${directory} is kept: ${holders.join(', ')} still attached from below it; detach through its owner`;
}

/** `primary` stays the error; what its cleanup then failed to undo is named after it, not dropped. */
function alsoFailed(primary: RamTempError, cleanup: readonly RamTempError[]): RamTempError {
  if (cleanup.length === 0) {
    return primary;
  }
  const after = `cleanup then failed: ${cleanup.map(describeRamTempError).join('; ')}`;
  switch (primary.kind) {
    case 'provision-failed':
    case 'ram-unavailable':
    case 'root-unready':
      return { ...primary, detail: `${primary.detail}; ${after}` };
    case 'volume-full':
    case 'lease-lost':
    case 'lease-path-budget':
      return { kind: 'provision-failed', step: 'cleanup', detail: `${describeRamTempError(primary)}; ${after}` };
  }
}

/**
 * Give the owner `rwx` on `directory` and on every directory below it, keeping every other mode
 * bit, so `rm -r` can empty them. Only directories are changed: a link is never descended
 * (`withFileTypes` reports it as a link), and each directory is opened `O_NOFOLLOW` and changed
 * through that handle, never by its path. A directory on another device than `device` is refused
 * before it is changed. Node has no `openat`, so entries are named by path; the walk checks each
 * directory's identity again after its `readdir`, and a directory swapped under its path stops it.
 */
async function prepareForRemoval(directory: string, device: number): Promise<void> {
  let handle: FileHandle;
  try {
    handle = await open(directory, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  } catch (error) {
    // Already gone: `rm -f` ignores that too.
    if (errorCode(error) === 'ENOENT') {
      return;
    }
    throw error;
  }
  try {
    const info = await handle.stat();
    if (info.dev !== device) {
      throw new Error(`${directory} is on device ${info.dev}, not the lease's device ${device}; not changed`);
    }
    if ((info.mode & 0o700) !== 0o700) {
      await handle.chmod((info.mode & 0o7777) | 0o700).catch((error: unknown) => {
        throw new Error(`${directory}: chmod u+rwx failed: ${errorText(error)}`);
      });
    }
    const entries = await readdir(directory, { withFileTypes: true });
    const named = await lstat(directory);
    if (named.dev !== info.dev || named.ino !== info.ino) {
      throw new Error(`${directory} was replaced while it was walked; not changed further`);
    }
    for (const entry of entries) {
      if (entry.isDirectory()) {
        await prepareForRemoval(join(directory, entry.name), device);
      }
    }
  } finally {
    await handle.close();
  }
}

/**
 * Every disk image in `ioreg -r -c AppleDiskImageDevice -l -w0` output. Each device starts a
 * `+-o AppleDiskImageDevice` block; the first `BSD Name` below it is the image's whole disk.
 */
function diskImages(registry: string): DiskImage[] {
  const images: DiskImage[] = [];
  for (const block of registry.split(/^\+-o AppleDiskImageDevice/m)) {
    const url = /"DiskImageURL" = "file:\/\/([^"]+)"/.exec(block)?.[1];
    const disk = /"BSD Name" = "(disk\d+)"/.exec(block)?.[1];
    if (url !== undefined && disk !== undefined) {
      images.push({ image: decodeURIComponent(url), disk });
    }
  }
  return images;
}

/** Mount points in `mount` output (`<device> on <path> (<options>)`). */
function mountPoints(table: string): string[] {
  const points: string[] = [];
  for (const line of table.split('\n')) {
    const point = /^\S+ on (.+) \([^)]*\)$/.exec(line)?.[1];
    if (point !== undefined) {
      points.push(point);
    }
  }
  return points;
}

/**
 * Every image in `ioreg` output whose `DiskImageURL` names a file under `prefix`, as
 * `<backing file> (/dev/diskN)`.
 */
export function imagesBackedUnder(registry: string, prefix: string): string[] {
  return diskImages(registry)
    .filter((entry) => entry.image.startsWith(prefix))
    .map((entry) => `${entry.image} (/dev/${entry.disk})`);
}

/** Mount points in `mount` output (`<device> on <path> (<options>)`) that lie under `prefix`. */
export function mountsBelow(table: string, prefix: string): string[] {
  return mountPoints(table).filter((point) => point.startsWith(prefix));
}

/** The disk `diskutil apfs` reports it created: "Disk from APFS operation: disk271". */
export function operationDisk(stdout: string): string | null {
  return /^Disk from APFS operation: (disk\d+(?:s\d+)?)$/m.exec(stdout)?.[1] ?? null;
}

/**
 * The RAM disks in `ioreg -r -c IOHDIXHDDriveOutKernel -l -w0` output, with the mounts `mount` output gives them. A RAM
 * disk is a legacy DiskImages device served by a diskimages-helper: each starts a `+-o IOHDIXHDDriveOutKernel` entry
 * whose own properties carry `image-path` <"ram://<sectors>"> and `owner-uid`, and the `BSD Name`s below it are its
 * whole disk first, then its APFS container and volumes; each volume node carries its `FullName` beside its `BSD Name`,
 * which is how a volume's name reaches its mounts. The registry is the kernel's record, and unlike `hdiutil info`,
 * which drops images while another attaches or detaches, it leaves out no attached disk. So an entry this cannot read
 * is an error, never a disk left out: a partial read must not stand for an absent disk.
 */
export function ramDisksIn(registry: string, table: string): Result<RamDisk[], string> {
  const mounted = new Map<string, string[]>();
  for (const [, device, point] of table.matchAll(/^\/dev\/(disk\d+(?:s\d+)*) on (.+) \([^)]*\)$/gm)) {
    if (device !== undefined && point !== undefined) {
      mounted.set(device, [...(mounted.get(device) ?? []), point]);
    }
  }
  const disks: RamDisk[] = [];
  for (const entry of registry.split(/^(?=\+-o )/m)) {
    if (entry.trim() === '') {
      continue;
    }
    const header = entry.slice(0, entry.indexOf('\n') === -1 ? undefined : entry.indexOf('\n'));
    const entryId = /<class IOHDIXHDDriveOutKernel, id (0x[0-9a-f]+),/.exec(header)?.[1];
    if (!header.startsWith('+-o ') || entryId === undefined) {
      return { ok: false, error: `not an IOHDIXHDDriveOutKernel entry: ${header.slice(0, 160)}` };
    }
    // The entry's own properties end at its first child; only they name its image and owner.
    const firstChild = entry.indexOf('+-o ', header.length);
    const own = entry.slice(header.length, firstChild === -1 ? undefined : firstChild);
    const imagePath = /"image-path" = <"([^"]*)">/.exec(own)?.[1];
    const owner = /"owner-uid" = (\d+)/.exec(own)?.[1];
    // Every node's property list closes before the next node: a read cut short leaves one open.
    const opened = entry.match(/^[| ]*\{\s*$/gm)?.length ?? 0;
    const closed = entry.match(/^[| ]*\}\s*$/gm)?.length ?? 0;
    if (opened === 0 || opened !== closed || imagePath === undefined || owner === undefined) {
      return {
        ok: false,
        error: `an incomplete entry (${opened} property lists opened, ${closed} closed${imagePath === undefined ? ', no image-path' : ''}${owner === undefined ? ', no owner-uid' : ''}): ${header.slice(0, 160)}`,
      };
    }
    const sectors = /^ram:\/\/(\d+)$/.exec(imagePath)?.[1];
    if (sectors === undefined) {
      // A file-backed legacy image, not a RAM disk.
      continue;
    }
    const names = [...entry.matchAll(/"BSD Name" = "(disk\d+(?:s\d+)*)"/g)].flatMap((match) =>
      match[1] === undefined ? [] : [match[1]],
    );
    const volumes: RamVolume[] = [];
    for (const node of entry.split(/^(?=[| ]*\+-o )/m)) {
      if (!/^[| ]*\+-o [^\n]*<class AppleAPFSVolume,/.test(node)) {
        continue;
      }
      const name = /^[| ]*"FullName" = "(.*)"[ \t]*$/m.exec(node)?.[1];
      const device = /"BSD Name" = "(disk\d+(?:s\d+)*)"/.exec(node)?.[1];
      if (name !== undefined) {
        volumes.push({ name, mountpoints: device === undefined ? [] : (mounted.get(device) ?? []) });
      }
    }
    disks.push({
      entry: entryId,
      physical: names[0] === undefined ? null : `/dev/${names[0]}`,
      sectors: Number(sectors),
      uid: Number(owner),
      mountpoints: names.flatMap((name) => mounted.get(name) ?? []),
      volumes,
    });
  }
  return { ok: true, value: disks };
}

/**
 * The RAM disks of `disks` that are this volume's, and no others. A disk is the volume's only when it has its user and
 * its exact size, mounts nowhere but at its mountpoint or staging directory, and either carries a volume called exactly
 * its name that is mounted, or is recorded: `recorded` says the disk's exact registry entry is one this volume's owner
 * holds a receipt or its ledger holds custody for, which covers an attach whose format never finished. A disk's size
 * and owner establish nothing, so an unmounted disk of the same size that nothing recorded is not the volume's.
 */
export function volumeDisks(
  disks: readonly RamDisk[],
  paths: Pick<RamTempPaths, 'uid' | 'mountpoint' | 'staging' | 'volumeName'>,
  sectors: number,
  recorded: (disk: RamDisk) => boolean,
): RamDisk[] {
  // The mount table lists a mount by the kernel's path; both paths are written as that path, so they compare exactly.
  const ours = (point: string) => point === paths.mountpoint || point === paths.staging;
  return disks.filter(
    (disk) =>
      disk.uid === paths.uid &&
      disk.sectors === sectors &&
      disk.mountpoints.every(ours) &&
      (disk.volumes.some((volume) => volume.name === paths.volumeName && volume.mountpoints.length > 0) ||
        recorded(disk)),
  );
}

/**
 * Detach a RAM disk an inventory found, by its whole-disk name, bound to the registry entry the inventory read it as.
 * A BSD name passes to the next disk once its disk goes, and a detach can queue for the disk admission meanwhile, so
 * the gateway reads the entry again inside its own admission, immediately before it spawns `hdiutil`, and
 * refuses, detaching nothing, unless the entry still names this whole disk (specs/cowshed/06_cli.md, `--registry-entry`).
 * That refusal is this detach's failure, named, and never retried with `-force`: this process did not just attach the
 * disk. The window that remains is cowshed's, from its read to hdiutil's own open of the name.
 */
export async function detachRamDisk(
  commands: Pick<HostCommands, 'detachRam'>,
  disk: AttachedRamDisk,
  step: string,
): Promise<Result<void, ProvisionFailed>> {
  const detached = await commands.detachRam(disk.physical, disk.entry);
  return detachOutcome(detached, disk, step);
}

function detachOutcome(detached: CommandOutput, disk: AttachedRamDisk, step: string): Result<void, ProvisionFailed> {
  if (detached.status === 0) {
    return { ok: true, value: undefined };
  }
  // A cowshed wrapper exit (100–106) whose stderr says it refused: the entry no longer names the disk.
  const refused =
    detached.status >= 100 &&
    detached.status <= 106 &&
    detached.stderr.includes('refused') &&
    detached.stderr.includes('nothing was detached');
  return failed(
    step,
    refused
      ? `${disk.physical} not detached: its registry entry ${disk.entry} no longer names it (${commandText(detached)})`
      : commandText(detached),
  );
}

/**
 * A disk child that DiskArbitration never answers would wedge the task, and every task queued
 * behind the lock, forever; measured: `diskutil mount` hung past 60 s while the host's
 * DiskArbitration was saturated. Cowshed bounds its disk children the same way.
 */
const HOST_COMMAND_DEADLINE_MS = 120_000;

/** Disk tools delegate their lease and process lifetime to cowshed, with one image policy. */
export function diskCommand(
  file: string,
  args: readonly string[],
  deadlineMs: number,
  binding?: DetachBinding,
  gatewaySocket?: string,
): { args: string[]; lifetime: 'stop-wait' | 'facade-owned' } | null {
  const program = basename(file);
  if (program !== 'hdiutil' && program !== 'diskutil') {
    return null;
  }
  // Match SystemCommandRunner's protected-image policy, including its case-sensitive verbs.
  const image = program === 'hdiutil' || args[0] === 'image' || args[0] === 'eject';
  const bound = binding === undefined ? [] : ['--registry-entry', binding.registryEntry];
  const gateway = gatewaySocket === undefined ? [] : ['--gateway-socket', gatewaySocket];
  return {
    args: image
      ? ['disk-command', program, ...gateway, ...bound, '--', ...args]
      : ['disk-command', program, ...gateway, '--deadline-ms', String(deadlineMs), '--', ...args],
    lifetime: image ? 'stop-wait' : 'facade-owned',
  };
}

/** Whether this process already said the cowshed gateway is not there to lease disk commands. */
let gatewayAbsenceSaid = false;

/**
 * How long this process stops asking a gateway that predates leases before it asks again: each
 * ask costs that gateway's 2 s of silence, and `cowshed setup` may replace it meanwhile. A gateway
 * that predates only `alone` leases is asked shared for as long.
 */
const PREDATES_RECHECK_MS = 60_000;

/** When this process last found the gateway at each control socket predating leases, or `alone` ones. */
const predatesSeen = new Map<string, { at: number; what: 'leases' | 'alone' }>();

/**
 * hdiutil and diskutil delegate process lifetime and disk leasing to cowshed: safe commands
 * pass their total budget and wait for the facade's actual exit after child reap. Only image
 * commands have a caller stop-wait deadline; killing an attach or detach can leak its kernel
 * device until reboot. Never acquire a second lease around the facade. Other leased helpers
 * take their lease here. Recorded lease waits ask for `alone`; an older gateway is asked
 * shared at once, and for a minute after.
 *
 * `gatewaySocket` is the gateway every disk lease, facade admission and RAM attach and detach goes through. It has no
 * default: the caller names it. The Nx executor names the host's own, `GATEWAY_SOCKET`, which `disk-command` defaults
 * to, so the facade is not told of it and a cowshed that predates `--gateway-socket` still serves it. A host proof
 * names its fixture's, which the facade is told of.
 */
export function createHostCommands(
  gatewaySocket: string,
  defaultDeadlineMs: number = HOST_COMMAND_DEADLINE_MS,
  wantedShare: PhaseShare = recordedShare(),
  cowshed = process.env[COWSHED_EXECUTABLE_ENV] ?? 'cowshed',
): HostCommands {
  return {
    async run(file, args, callDeadlineMs, binding) {
      const deadlineMs = callDeadlineMs ?? defaultDeadlineMs;
      const disk = diskCommand(
        file,
        args,
        deadlineMs,
        binding,
        gatewaySocket === GATEWAY_SOCKET ? undefined : gatewaySocket,
      );
      if (disk !== null) {
        return runBounded(cowshed, disk.args, deadlineMs, disk.lifetime);
      }
      const diskClass = diskClassOf(file);
      const seen = predatesSeen.get(gatewaySocket);
      const predates = seen !== undefined && Date.now() - seen.at < PREDATES_RECHECK_MS ? seen.what : null;
      if (diskClass === null || predates === 'leases') {
        return runBounded(file, args, deadlineMs);
      }
      const command = [file, ...args].join(' ');
      const bounds = { ackMs: 2_000, afterCloseMs: 5_000, grantMs: deadlineMs };
      const share = predates === 'alone' ? 'shared' : wantedShare;
      let lease = await takeDiskLease(gatewaySocket, diskClass, command, bounds, share);
      if (!lease.granted && lease.cause === 'predates-field' && share === 'alone') {
        predatesSeen.set(gatewaySocket, { at: Date.now(), what: 'alone' });
        lease = await takeDiskLease(gatewaySocket, diskClass, command, bounds, 'shared');
      }
      if (!lease.granted) {
        if (lease.cause === 'predates') {
          predatesSeen.set(gatewaySocket, { at: Date.now(), what: 'leases' });
        }
        if (!(lease.cause === 'absent' && gatewayAbsenceSaid)) {
          gatewayAbsenceSaid ||= lease.cause === 'absent';
          process.stderr.write(`RAM temp volume: ${file} runs without cowshed's disk lease: ${lease.reason}\n`);
        }
      }
      try {
        return await runBounded(file, args, deadlineMs);
      } finally {
        if (lease.granted) {
          lease.release();
        }
      }
    },
    detachRam(device, registryEntry) {
      return detachGatewayRamDisk(gatewaySocket, device, registryEntry);
    },
    attachRam(sectors) {
      return attachRamDisk(gatewaySocket, sectors, defaultDeadlineMs);
    },
  };
}

function runBounded(
  file: string,
  args: readonly string[],
  deadlineMs: number,
  lifetime: 'kill' | 'stop-wait' | 'facade-owned' = 'kill',
): Promise<CommandOutput> {
  // `Promise.withResolvers` would read better but needs lib es2024; this package inherits lib
  // es2022 from tsconfig.base.json.
  let resolve!: (output: CommandOutput) => void;
  const promise = new Promise<CommandOutput>((settled) => {
    resolve = settled;
  });
  // Ordinary helpers own their process group. The facade alone owns disk-child termination,
  // reap and lease custody; a wrapper's exit or signal is not the disk child's exit or signal.
  const child = spawn(file, [...args], { detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
  const stdout: Buffer[] = [];
  let complete: ((output: CommandOutput | null) => void) | undefined;
  const completion =
    lifetime === 'stop-wait'
      ? new Promise<CommandOutput | null>((settled) => {
          complete = settled;
        })
      : null;
  const stderr: Buffer[] = [];
  child.stdout.on('data', (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));
  const text = (chunks: Buffer[]) => Buffer.concat(chunks).toString('utf8');
  // Set once the deadline has fired and is killing the group: from then on it owns the result.
  let timedOut = false;
  let stoppedWaiting = false;
  // Safe disk commands have no Node timer: the CLI owns admission, execution and reap.
  // Protected commands keep draining their pipes after stop-wait: closing a live facade's
  // output is not proof its native child ended, and must not manufacture an early exit.
  const timer =
    lifetime === 'facade-owned'
      ? undefined
      : setTimeout(() => {
          if (completion !== null) {
            stoppedWaiting = true;
            child.unref();
            // A piped child's streams are sockets; only a socket can be unref'd.
            if (child.stdout instanceof Socket) child.stdout.unref();
            if (child.stderr instanceof Socket) child.stderr.unref();
            resolve({
              status: 1,
              pending: { pid: child.pid ?? null, completion },
              stdout: text(stdout),
              stderr: [
                text(stderr),
                `no answer within ${deadlineMs} ms; cowshed command ${child.pid ?? 'unknown'} left running, not killed: disk-image operations must finish to release their kernel devices`,
              ]
                .filter(Boolean)
                .join('\n'),
            });
            return;
          }
          // A `close` that comes while the group is being killed must not end the call with an ordinary completion
          // before the kill is done: the deadline answers.
          timedOut = true;
          void killGroup(child).then((killed) => {
            resolve({ status: 1, stdout: text(stdout), stderr: `no answer within ${deadlineMs} ms; ${killed}` });
          });
        }, deadlineMs);
  child.on('error', (error) => {
    clearTimeout(timer);
    if (timedOut) return;
    const output = { status: 1, stdout: text(stdout), stderr: error.message };
    complete?.(child.pid === undefined ? output : null);
    resolve(
      child.pid !== undefined && completion !== null ? { ...output, pending: { pid: child.pid, completion } } : output,
    );
  });
  child.on('close', (code, signal) => {
    clearTimeout(timer);
    if (timedOut) return;
    const error = text(stderr);
    const output = {
      status: code ?? 1,
      stdout: text(stdout),
      stderr: signal === null ? error : [error, `killed by ${signal}`].filter(Boolean).join('\n'),
    };
    // Wrapper errors can report wait/reap failure with a native child still alive (06_cli.md).
    const terminal = code !== null && (code < 100 || code > 106);
    complete?.(terminal ? output : null);
    if (!terminal && completion !== null && stoppedWaiting) {
      process.stderr.write(
        `RAM temp volume: cowshed command ${child.pid ?? 'unknown'} ended without a native terminal receipt: ${commandText(output)}\n`,
      );
    }
    resolve(!terminal && completion !== null ? { ...output, pending: { pid: child.pid ?? null, completion } } : output);
  });
  return promise;
}

/**
 * SIGKILL the child's whole process group, again while it lists members; what happened, for the error. A group that
 * is still listed when the kill gives up is not called killed.
 */
async function killGroup(child: ChildProcess): Promise<string> {
  if (child.pid === undefined) {
    return 'never started';
  }
  try {
    const left = await killProcessGroup(child.pid, DEADLINE_PATIENCE);
    return left
      ? `signalled its process group, but ${describeMembersLeft(child.pid)}`
      : `killed process group ${child.pid}`;
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

/**
 * A cowshed sandbox's refusal of one of the volume's own paths: the same answer as its refusal of the lock, so the task
 * keeps its workspace TMPDIR (`sandboxed`) whichever path the sandbox refuses first. A job's sandbox admits no part of
 * the RAM volume it cannot use, and this module reads that refusal rather than a second rule saying where it runs.
 */
function denied<T>(path: string, error: unknown): Result<T, LockedError> {
  return { ok: false, error: { kind: 'lock-denied', detail: `${path}: ${errorText(error)}` } };
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
