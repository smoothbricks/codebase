import { type ChildProcess, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import {
  type FileHandle,
  lstat,
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
import { basename, join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  awaitExit,
  type ProcessEntry,
  pidsWorkingIn,
  processTable,
  terminate,
  withDescendants,
} from '../../testing.js';
import { isRecord } from '../../type-guards.js';
import { diskClassOf, type PhaseShare, recordedShare, takeDiskLease } from './disk-lease.js';
import { GATEWAY_SOCKET } from './gateway-lease.js';
import {
  captureHeldVerdict,
  type HeldCandidate,
  type HeldContext,
  type HeldVerdict,
  parseHeldVerdict,
  validateHeldVerdict,
} from './ram-temp-held.js';

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
 * One kernel lock (`O_EXLOCK` on a file in /private/tmp), dropped by the kernel
 * when its holder dies, admits changes to a small ledger beside it and to the
 * set of lease directories. It is held only to read and publish those: the
 * host-wide probes (`ioreg`, `mount`, the process census), directory cleanup,
 * provisioning and detach run outside it, under claims the ledger publishes.
 * One caller creates or detaches the volume at a time, and a caller that finds
 * either in flight yields and looks again; each dead lease is settled by the
 * one caller that claimed it, and every other caller passes it by. A claim
 * names its owner's pid, so a claim whose owner died is void. Before it
 * removes a lease or detaches the volume, a caller takes the lock again and
 * checks that the volume, the lease directory and its own claim are still as
 * it claimed them. A lease is a directory named after the task process's pid;
 * a lease whose pid is gone is reclaimed by the next task, and the volume is
 * detached when the last lease ends.
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
 * volume under it, and every task that takes a lease names it. Where every
 * holder has an identity a cheap `lstat` re-proves (a mount below the lease),
 * the ledger keeps that proof (`ram-temp-held.ts`) and later tasks check it
 * instead of probing the host; any change sends the lease back to a probe.
 * Every disk child runs under a deadline, in its own process group so the
 * deadline also reaches the helper it spawned, so a wedged DiskArbitration
 * fails the task instead of every task waiting on the volume. A detach is the
 * exception: never killed at its deadline, because an interrupted detach can
 * wedge DiskArbitration itself; its wait is bounded and reported by name, and
 * a stray's detach gets the short stray bound, so one wedged stray cannot
 * hold the create claim while a chain of tasks queues behind it.
 *
 * A cowshed sandbox denies writes to /private/tmp (EPERM on the lock), and
 * cannot attach disk images at all; there the task keeps the workspace's own
 * TMPDIR and says so once on stderr.
 */

/** macOS `open(2)` flag: take an exclusive flock atomically with the open. Absent from `fs.constants`. */
const O_EXLOCK = 0x20;
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
 * revision 2 leases and detaches outside it, behind claims in a ledger revision 1 never reads.
 * So no revision shares a volume with another. It names the volume, its lock and its ledger, and
 * adds as many sectors to the RAM disk, whose size and user are what a creator's stray reclaim
 * matches: neither revision detaches the other's disk, mounted or still attaching.
 */
const RAM_TEMP_REVISION = 2;
/**
 * The volume's ceiling, plus the revision's sectors. Pages are allocated as they are written and
 * returned when the volume detaches, so this bounds what a runaway suite can take, not what a gate
 * costs. Measured 2026-10-04: a full parallel smoothbricks `lint test build host-controller-test`
 * gate peaked at 1.61 GiB used with 18 concurrent leases; 8 GiB is five times that. That
 * gate also staged cowshed's real-APFS fixture images on the volume, which now stay under
 * /private/tmp, so the peak is an upper bound.
 */
export const RAM_TEMP_CAPACITY_BYTES = 8 * GIB + RAM_TEMP_REVISION * SECTOR_BYTES;
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
  run(file: string, args: readonly string[], deadlineMs?: number): Promise<CommandOutput>;
}

/**
 * How long a process a task left in its lease gets to exit on SIGTERM before it gets SIGKILL: the
 * time a daemon takes to stop its watcher and its plugin workers, and well under the grace a
 * bounded leg gives its own command.
 */
const LEFTOVER_GRACE_MS = 5_000;

/**
 * The processes still working in a lease directory once its task is over, and how they end. A
 * seam so the volume's state machine is testable without any.
 */
export interface LeaseProcesses {
  /**
   * End every process whose working directory is in `directory`, and everything those started,
   * and answer each as `<pid> (<command>)`. Every one has exited when this settles; one that
   * would not rejects with its name.
   */
  stopWorkingIn(directory: string): Promise<string[]>;
}

/** The host reads and signals cleanup uses; a seam for process ordering and signal failures. */
export interface ProcessHost {
  /** The processes whose working directory is `directory` or lies beneath it. */
  workingIn(directory: string): Promise<number[]>;
  /** Every process on the host. */
  table(): Promise<ProcessEntry[]>;
  signal(pid: number, signal: NodeJS.Signals): void;
}

/** A `ps` state in which a process starts nothing: stopped (`T`, `t` when traced) or exited (`Z`). */
const STARTS_NOTHING = /^[TtZ]/;

/**
 * The processes `host` reads. A lease belongs to one task and its command is gone by the time the
 * lease ends, so what still works in it is what the command left behind. A fixture's Nx daemon is
 * the usual one: Nx detaches it into a session of its own, so the process-group kill that ends a
 * timed-out command never reaches it, and a test process that is killed runs no teardown of its
 * own. Deleting the lease asks such a daemon to leave only by deleting its record, which it
 * notices within a second when it is idle and not at all while it is wedged; a signal does not
 * depend on the daemon noticing, and a process left on the volume is what keeps it from
 * detaching.
 *
 * A leftover keeps running while it is looked for, and a child it starts after the process table
 * was read is in no table that was. So each one found is held with SIGSTOP and the table is read
 * again, until a read names nothing that the read before did not already show stopped: a stopped
 * process starts nothing, and whatever it started before it stopped is in every table read after.
 * Only then does each get SIGTERM, and SIGCONT to act on it. A child whose parent exited before it
 * could be held has lost its ancestry, so once the stopped-table barrier holds, the lease is asked
 * again what works in it, including when that table names nothing, to recover reparented children.
 */
export function leaseProcessesOn(host: ProcessHost): LeaseProcesses {
  return {
    async stopWorkingIn(directory) {
      let working = (await host.workingIn(directory)).filter((pid) => pid !== process.pid);
      if (working.length === 0) {
        return [];
      }
      const start = Date.now();
      const deadline = start + LEFTOVER_GRACE_MS;
      const known = new Map<number, ProcessEntry>();
      let held = new Set<number>();
      let failures: unknown[] | undefined;
      let tableReads = 0;
      try {
        for (;;) {
          const roots = [...working, ...known.keys()];
          const current = withDescendants(await host.table(), roots).filter((entry) => entry.pid !== process.pid);
          tableReads += 1;
          for (const entry of current) {
            known.set(entry.pid, entry);
          }
          if (current.every((entry) => held.has(entry.pid) && STARTS_NOTHING.test(entry.stat))) {
            working = (await host.workingIn(directory)).filter((pid) => pid !== process.pid);
            if (working.every((pid) => held.has(pid))) {
              break;
            }
          }
          const now = Date.now();
          if (now > deadline) {
            const list = current.map((entry) => `${entry.pid} (${entry.command}, state ${entry.stat})`).join(', ');
            throw new Error(
              `processes working in ${directory}: ${list || 'no current processes'} did not stop spawning within ${LEFTOVER_GRACE_MS}ms after ${tableReads} table reads and ${now - start}ms; cwd roots: ${working.join(', ') || 'none'}`,
            );
          }
          for (const entry of current) {
            host.signal(entry.pid, 'SIGSTOP');
          }
          held = new Set(current.filter((entry) => STARTS_NOTHING.test(entry.stat)).map((entry) => entry.pid));
        }
        for (const entry of known.values()) {
          host.signal(entry.pid, 'SIGTERM');
        }
      } catch (error) {
        failures = [error];
      } finally {
        // SIGTERM is pending while stopped. One failed resume must not strand the other holds.
        for (const entry of known.values()) {
          try {
            host.signal(entry.pid, 'SIGCONT');
          } catch (error) {
            failures ??= [];
            failures.push(
              new Error(
                `${entry.pid} (${entry.command}): SIGCONT failed: ${error instanceof Error ? error.message : String(error)}`,
                { cause: error },
              ),
            );
          }
        }
      }
      if (failures !== undefined) {
        if (failures.length === 1) {
          throw failures[0];
        }
        throw new AggregateError(
          failures,
          `processes working in ${directory}: ${failures.map((error) => (error instanceof Error ? error.message : String(error))).join('; ')}`,
        );
      }
      const leftovers = [...known.values()];
      try {
        await awaitExit(leftovers, `processes working in ${directory}`, LEFTOVER_GRACE_MS);
      } catch {
        for (const entry of leftovers) {
          host.signal(entry.pid, 'SIGKILL');
        }
        await awaitExit(leftovers, `processes working in ${directory} after SIGKILL`, LEFTOVER_GRACE_MS);
      }
      return leftovers.map((entry) => `${entry.pid} (${entry.command.slice(0, 160)})`);
    },
  };
}

export const hostLeaseProcesses: LeaseProcesses = leaseProcessesOn({
  workingIn: pidsWorkingIn,
  table: processTable,
  signal: terminate,
});

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
  const volumeName = `smoo-ram${RAM_TEMP_REVISION}-${user.uid}`;
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
  /**
   * `held`: dead leases that could not be reclaimed, each with what still holds it. `reaped`: the
   * processes of dead leases that were still working there and were stopped before the lease went.
   */
  | { kind: 'leased'; lease: RamTempLease; held: string[]; reaped: string[] }
  /** Inside a cowshed sandbox: /private/tmp is not writable and no image can attach. */
  | { kind: 'sandboxed'; detail: string };

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

/** The volume is being created or detached: no other caller takes a lease or detaches meanwhile. */
interface LifecycleClaim {
  readonly kind: 'create' | 'detach';
  readonly owner: ClaimOwner;
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

/** What the lock admits changes to, published beside the lock file. Claims of dead owners are void. */
interface Ledger {
  readonly lifecycle: LifecycleClaim | null;
  readonly claims: readonly LeaseClaim[];
  readonly held: readonly HeldEntry[];
}

const EMPTY_LEDGER: Ledger = { lifecycle: null, claims: [], held: [] };

/** The volume as one locked step finds it mounted. */
interface MountedVolume {
  readonly capacity: number;
  /** This attach of the volume: its root's and its marker's identity, never times that change while it is in use. */
  readonly key: string;
  readonly device: number;
  /** The lease directories on it. */
  readonly leases: readonly string[];
}

/**
 * One acquire or release. `claimed`: the ledger as it last published holds one of its claims.
 * `made`: the lease directory an acquire made, with its identity, until the caller has it.
 */
interface Operation {
  readonly owner: ClaimOwner;
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

/** What settling claimed leases found: those kept, the processes stopped, and the verdicts to publish. */
interface Settled {
  readonly held: readonly HeldLease[];
  readonly reaped: readonly string[];
  readonly verdicts: readonly HeldEntry[];
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
  | { readonly kind: 'unmounted' }
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

  constructor(
    private readonly paths: RamTempPaths,
    private readonly capacityBytes: number,
    private readonly commands: HostCommands,
    private readonly pid: number = process.pid,
    private readonly isAlive: (pid: number) => boolean = processIsAlive,
    private readonly processes: LeaseProcesses = hostLeaseProcesses,
  ) {
    this.sectors = Math.ceil(capacityBytes / SECTOR_BYTES);
    this.ledgerPath = `${paths.lockFile}.ledger`;
  }

  /** Take a lease: create the volume if no task holds one, and a private directory on it. */
  async acquire(): Promise<Result<RamTempAcquisition, RamTempError>> {
    const op: Operation = { owner: { pid: this.pid, token: randomUUID() }, claimed: false, made: null };
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
        const claimed = await this.claim(ledger, volume, op.owner, null);
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
        case 'wait':
          // Another caller is creating or detaching the volume: look again once it has.
          waited = await this.waitOut(value.claim, waited);
          continue;
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
                capacityBytes: value.volume.capacity,
              },
              held: [...value.held, ...settled.value.held].map((lease) => lease.text),
              reaped: [...settled.value.reaped],
            },
          };
        }
      }
    }
  }

  /** With this caller's creation claim published: provision the volume, then take the first lease on it. */
  private async create(op: Operation): Promise<Result<RamTempAcquisition, LockedError>> {
    const provisioned = await this.provisionVolume();
    if (!provisioned.ok) {
      return provisioned;
    }
    const placed = await this.locked(
      op,
      'lease',
      async (ledger, volume): Promise<Step<Result<string, RamTempError>>> => {
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
        return { ledger: next, value: { ok: true, value: await this.make(op) } };
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
        lease: { directory: placed.value.value, mountpoint: this.paths.mountpoint, capacityBytes: this.capacityBytes },
        held: [],
        reaped: [],
      },
    };
  }

  /**
   * Stop what the task left working in its lease, before the lease ends: the processes whose
   * working directory is in it and everything they started, answered as `<pid> (<command>)`.
   * Runs outside the volume's lock, which one lease's leftovers must not hold up the others
   * behind, and costs a scan of every process only when the lease still holds anything.
   */
  async reap(lease: RamTempLease): Promise<Result<string[], RamTempError>> {
    try {
      if ((await readdir(lease.directory)).length === 0) {
        return { ok: true, value: [] };
      }
      return { ok: true, value: await this.processes.stopWorkingIn(lease.directory) };
    } catch (error) {
      if (errorCode(error) === 'ENOENT') {
        return { ok: true, value: [] };
      }
      return failed('reap', errorText(error));
    }
  }

  /** End a lease; the last lease detaches the volume, returning its RAM. */
  async release(lease: RamTempLease): Promise<Result<void, RamTempError>> {
    const op: Operation = { owner: { pid: this.pid, token: randomUUID() }, claimed: false, made: null };
    let result: Result<void, LockedError>;
    try {
      result = await this.releaseLease(op, basename(lease.directory));
    } catch (error) {
      result = failed('release', errorText(error));
    }
    const ended = await this.end(op);
    if (result.ok) {
      return ended;
    }
    const primary: RamTempError =
      result.error.kind === 'lock-denied'
        ? { kind: 'provision-failed', step: 'lock', detail: result.error.detail }
        : result.error;
    return { ok: false, error: alsoFailed(primary, ended.ok ? [] : [ended.error]) };
  }

  private async releaseLease(op: Operation, own: string): Promise<Result<void, LockedError>> {
    let waited: ClaimWait | null = null;
    for (;;) {
      const taken = await this.locked(op, 'release', async (ledger, volume): Promise<Step<Releasing>> => {
        if (volume === null) {
          return { ledger, value: { kind: 'unmounted' } };
        }
        if (ledger.lifecycle !== null) {
          return { ledger, value: { kind: 'wait', claim: ledger.lifecycle } };
        }
        const claimed = await this.claim(ledger, volume, op.owner, own);
        return {
          ledger: claimed.ledger,
          value: { kind: 'claimed', volume, held: claimed.held, candidates: claimed.candidates },
        };
      });
      if (!taken.ok) {
        return taken;
      }
      const value = taken.value;
      switch (value.kind) {
        case 'unmounted':
          return { ok: true, value: undefined };
        case 'wait':
          waited = await this.waitOut(value.claim, waited);
          continue;
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
          return idle.value ? await this.detach(op, value.volume) : { ok: true, value: undefined };
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
        `RAM temp volume: waited ${(elapsed / 1000).toFixed(1)} s on the ${claim.kind} claim of pid ${claim.owner.pid} (token ${claim.owner.token}) in ${this.ledgerPath}; if that pid is not ${claim.kind === 'create' ? 'creating' : 'detaching'} ${this.paths.mountpoint}, remove the ledger once no task uses the volume\n`,
      );
    }
    await sleep(LOCK_RETRY_MS);
    return { ...wait, notices: Math.max(wait.notices, due) };
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

  /** Drop whatever claims `op` still holds: an operation that failed part way leaves none behind it. */
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
    const lock = await this.lock();
    if (!lock.ok) {
      return lock.error.code === 'EPERM'
        ? { ok: false, error: { kind: 'lock-denied', detail: `${this.paths.lockFile}: ${lock.error.message}` } }
        : failed('lock', lock.error.message);
    }
    try {
      const read = await this.readLedger();
      if (!read.ok) {
        return read;
      }
      const volume = await this.observe();
      const next = await change(prune(read.value.ledger, volume, this.isAlive), volume);
      const text = ledgerText(next.ledger);
      if (text !== (read.value.text ?? ledgerText(EMPTY_LEDGER))) {
        await publishLedger(this.ledgerPath, text);
      }
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
    // Claims that cannot be read cannot be honoured, so no lease or volume change is safe until it is
    // removed; only this module writes it, in one rename, so a malformed one is not its own.
    const refused = (detail: string) =>
      failed<{ ledger: Ledger; text: string | null }>(
        'ledger',
        `${this.ledgerPath} ${detail}; remove it once no task uses the volume`,
      );
    let wire: unknown;
    try {
      wire = JSON.parse(text);
    } catch (error) {
      return refused(`is not JSON: ${errorText(error)}`);
    }
    const ledger = parseLedger(wire);
    return ledger === undefined ? refused("is not this module's ledger") : { ok: true, value: { ledger, text } };
  }

  /**
   * The volume when it is mounted at the mountpoint, identified by its marker; else null. Its key
   * names this attach: the root's and the marker's identity and birth, and the marker's change
   * time, which nothing changes once it is written. The root's own times change with every lease.
   */
  private async observe(): Promise<MountedVolume | null> {
    const markerPath = join(this.paths.mountpoint, MARKER_FILE);
    const marker = await readFile(markerPath, 'utf8').catch(() => null);
    if (marker?.trim() !== this.paths.volumeName) {
      return null;
    }
    const [root, parent, mark] = await Promise.all([
      stat(this.paths.mountpoint, { bigint: true }),
      stat(join(this.paths.mountpoint, '..'), { bigint: true }),
      lstat(markerPath, { bigint: true }),
    ]);
    if (root.dev === parent.dev) {
      return null;
    }
    const [fs, names] = await Promise.all([statfs(this.paths.mountpoint), readdir(this.paths.mountpoint)]);
    return {
      capacity: fs.blocks * fs.bsize,
      key: [
        `root ${root.dev}:${root.ino}:${root.birthtimeNs}`,
        `marker ${mark.dev}:${mark.ino}:${mark.birthtimeNs}:${mark.ctimeNs}`,
      ].join(' '),
      device: Number(root.dev),
      leases: names.filter((name) => LEASE_NAME.test(name)),
    };
  }

  /**
   * Under the lock: claim `own`, the caller's ending lease if any, and every dead lease no other
   * caller is settling. A dead lease whose cached verdict still validates is named held and left
   * unclaimed: that proof costs `lstat` calls, not a host-wide probe, so no caller waits on it.
   */
  private async claim(ledger: Ledger, volume: MountedVolume, owner: ClaimOwner, own: string | null): Promise<Claimed> {
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
      candidates.push({ name, directory, identity, own: name === own });
    }
    return { ledger: { ...ledger, claims, held: verdicts }, held, candidates };
  }

  /**
   * Outside the lock: settle the leases this caller claimed. The processes every dead one still
   * runs are stopped first, and only then is the pass's one inventory read, so no inventory older
   * than a lease's settled census approves its removal. Anything attached from below a lease keeps
   * it: an image backed by a file there would be left pinned to a deleted file, and detaching it
   * here would race the owner that releases it under its own lease; a mount there is backed
   * elsewhere — a cowshed workspace adopted from a fixture checkout, say — and `rm -r` would
   * delete its contents through the mount. A kept lease gets a verdict to publish, when its
   * holders have cheap lifetime keys. A lease nothing holds is removed once the lock confirms it
   * is as claimed.
   */
  private async settle(
    op: Operation,
    volume: MountedVolume,
    candidates: readonly Candidate[],
  ): Promise<Result<Settled, LockedError>> {
    if (candidates.length === 0) {
      return { ok: true, value: { held: [], reaped: [], verdicts: [] } };
    }
    const reaped: string[] = [];
    for (const candidate of candidates) {
      // Its task is gone, so what works in it is what that task left: stopped before the lease
      // is removed, even where the removal is refused below. A task reaps its own (`reap`).
      if (!candidate.own && (await readdir(candidate.directory)).length > 0) {
        reaped.push(...(await this.processes.stopWorkingIn(candidate.directory)));
      }
    }
    const inventory = await this.readInventory();
    if (!inventory.ok) {
      return inventory;
    }
    const held: HeldLease[] = [];
    const verdicts: HeldEntry[] = [];
    const removable: Candidate[] = [];
    for (const candidate of candidates) {
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
        // Nothing holds the lease, so its directories may be made removable first: `rm -r` cannot
        // empty a readonly one. Only this volume's device is changed (see prepareForRemoval).
        await prepareForRemoval(candidate.directory, volume.device);
        await rm(candidate.directory, { recursive: true, force: true });
      }
    }
    return { ok: true, value: { held, reaped, verdicts } };
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

  /** Detach what earlier creators left, then attach and provision the volume. */
  private async provisionVolume(): Promise<Result<void, RamTempError>> {
    const strays = await this.reclaimStrays(this.holdersPass());
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
  private async reclaimStrays(holders: Holders): Promise<Result<void, RamTempError>> {
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
      const detached = await this.commands.run(
        '/usr/bin/hdiutil',
        ['detach', '-force', disk.physical],
        STRAY_DETACH_DEADLINE_MS,
      );
      if (detached.status !== 0) {
        return failed(`hdiutil detach (stray ${disk.physical})`, commandText(detached));
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
        `hdiutil info lists no RAM disk of ${this.paths.user} with ${this.sectors} sectors mounted at ${this.paths.mountpoint}`,
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
 * The ledger with the claims of dead owners voided, and the verdicts kept only for leases still on
 * this attach of the volume. A claim's owner is judged as a lease's is, by its pid.
 */
function prune(ledger: Ledger, volume: MountedVolume | null, isAlive: (pid: number) => boolean): Ledger {
  return {
    lifecycle: ledger.lifecycle !== null && isAlive(ledger.lifecycle.owner.pid) ? ledger.lifecycle : null,
    claims: ledger.claims.filter((claim) => isAlive(claim.owner.pid)),
    held:
      volume === null
        ? []
        : ledger.held.filter(
            (entry) => entry.verdict.context.volumeKey === volume.key && volume.leases.includes(entry.lease),
          ),
  };
}

function withoutClaimsOf(ledger: Ledger, owner: ClaimOwner): Ledger {
  return {
    lifecycle: ledger.lifecycle?.owner.token === owner.token ? null : ledger.lifecycle,
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
  return `${JSON.stringify({ version: 1, lifecycle: ledger.lifecycle, claims: ledger.claims, held: ledger.held })}\n`;
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
  if (!isRecord(value) || value.version !== 1) {
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
    lifecycle = { kind, owner };
  }
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
      return { ...primary, detail: `${primary.detail}; ${after}` };
    case 'volume-full':
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

/**
 * How long one creator waits on a stray's detach before it reports the stray by name and
 * gives the create claim back. A stray detach that makes no progress is DiskArbitration's
 * wedge, not this volume's: at 120 s a chain of creators each waited the full bound on the
 * same stray and the claim never moved. The detach itself is never killed (below); the
 * bound is on the wait, and the next creator finds the stray gone or names it again.
 */
const STRAY_DETACH_DEADLINE_MS = 30_000;

/** Disk tools have one process owner: cowshed's detached, file-backed command layer. */
export function diskCommand(file: string, args: readonly string[]): string[] | null {
  const program = basename(file);
  return program === 'hdiutil' || program === 'diskutil' ? ['disk-command', program, '--', ...args] : null;
}

/**
 * The variable naming the cowshed executable that runs `cowshed disk-command`. Unset, it is the installed `cowshed`
 * on PATH. A repository that tests its own cowshed sets it, on the targets that test it, to that checkout's build:
 * an installed cowshed that predates a command its own checkout added would fail the very code under test.
 */
export const COWSHED_EXECUTABLE_ENV = 'COWSHED_EXECUTABLE';

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
 * Disk-image tools delegate both process lifetime and disk leasing to cowshed. In particular,
 * a caller timeout never kills an attach or detach, which can leak its kernel device until
 * reboot. Other disk tools take the host lease here; acquiring it again around cowshed would
 * deadlock a queued storage phase against its own caller. A process whose lease waits are
 * recorded asks for its phase `alone`; a gateway that refuses `alone` is asked shared at once,
 * and for a minute after.
 */
export function createHostCommands(
  defaultDeadlineMs: number = HOST_COMMAND_DEADLINE_MS,
  gatewaySocket: string = GATEWAY_SOCKET,
  wantedShare: PhaseShare = recordedShare(),
  cowshed = process.env[COWSHED_EXECUTABLE_ENV] ?? 'cowshed',
): HostCommands {
  return {
    async run(file, args, callDeadlineMs) {
      const deadlineMs = callDeadlineMs ?? defaultDeadlineMs;
      const disk = diskCommand(file, args);
      if (disk !== null) {
        return runBounded(cowshed, disk, deadlineMs, false);
      }
      const diskClass = diskClassOf(file, args);
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
  };
}

function runBounded(
  file: string,
  args: readonly string[],
  deadlineMs: number,
  killOnDeadline = true,
): Promise<CommandOutput> {
  // `Promise.withResolvers` would read better but needs lib es2024; this package inherits lib
  // es2022 from tsconfig.base.json.
  let resolve!: (output: CommandOutput) => void;
  const promise = new Promise<CommandOutput>((settled) => {
    resolve = settled;
  });
  // Ordinary children own their process group so their deadline also reaps helpers. Disk
  // commands instead run through cowshed, which owns their detached lifetime and output.
  const child = spawn(file, [...args], { detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  child.stdout.on('data', (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));
  const text = (chunks: Buffer[]) => Buffer.concat(chunks).toString('utf8');
  // The facade owns its disk child's output files. Stop waiting, not the disk operation:
  // closing these pipes cannot wedge the disk tool, and cowshed still holds its disk lease.
  const timer = setTimeout(() => {
    if (!killOnDeadline) {
      child.stdout.destroy();
      child.stderr.destroy();
      child.unref();
      resolve({
        status: 1,
        stdout: text(stdout),
        stderr: `no answer within ${deadlineMs} ms; cowshed command ${child.pid ?? 'unknown'} left running, not killed: disk-image operations must finish to release their kernel devices`,
      });
      return;
    }
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
