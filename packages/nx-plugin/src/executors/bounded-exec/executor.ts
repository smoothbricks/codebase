import { type ChildProcess, type IOType, spawn } from 'node:child_process';
import { accessSync, constants, existsSync, readFileSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { availableParallelism, constants as osConstants, userInfo } from 'node:os';
import { delimiter, isAbsolute, join, resolve as resolvePath } from 'node:path';
import { createInterface } from 'node:readline';
import { Duplex, Readable } from 'node:stream';
import { workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory.js';
import {
  DEADLINE_PATIENCE,
  describeMembersLeft,
  killProcessGroup,
  processGroupHasMembers,
  signalProcessGroup,
} from '../../process-group.js';
import { AdmissionObservation, type QueueHost } from './admission-queue.js';
import { type BoundEvent, type BoundKind, type Bounds, earliestBound, stepBounds, UNARMED } from './bounds.js';
import { DIAGNOSTIC_CONTEXT_ENV, DIAGNOSTIC_MODULE_ENV, grantMemoryEnvironment } from './child-exit-diagnostics.js';
import { type CpuPool, createCpuPool } from './cpu-pool.js';
import type { CpuRise } from './cpu-rise.js';
import { GATEWAY_SOCKET } from './gateway-lease.js';
import type { HangDiagnostics } from './hang-diagnostics.js';
import { captureHangDiagnostics, HANG_CAPTURE_MS } from './hang-diagnostics.js';
import { type LeafPeak, PEAK_FD, peakWaiter, readWaiterReport, underWaiter } from './leaf-peak.js';
import type { LockWait } from './lock-wait.js';
import {
  askedCores,
  MBX_GRANT_FD,
  mbxIdleGated,
  mbxLeaf,
  mbxPool,
  withCountedReservation,
  withMbxLeaf,
} from './mbx-pool.js';
import { describeUnserved, RESERVATIONS_ENV, type Reservation, reserveCores, runEndpoint } from './mbx-reservations.js';
import { killedFromOutside, type RunOutcome, runUntilNotKilledForMemory } from './memory-retry.js';
import { createNoSpaceWatch } from './no-space.js';
import {
  COWSHED_EXECUTABLE_ENV,
  cowshedSubtreeOwner,
  HANDLE_FD,
  readAdmission,
  SUPERVISOR_SOCKET_ENV,
  type SubtreeAdmission,
  type SubtreeOwner,
} from './owned-subtree.js';
import { askParentPool, describeGrant } from './parent-grant.js';
import { type LastPeak, readLastPeak } from './peak-charge.js';
import { CPU_POOL_ENV, type InheritedCpuPool, readInheritedCpuPool } from './pool-environment.js';
import { createRunProgress, withProgressReport, withTestResultReport } from './progress.js';
import { PROGRESS_SOCKET_ENV, type ProgressChannel, serveProgressChannel } from './progress-channel.js';
import {
  createHostCommands,
  describeCoverageGap,
  describeRamTempError,
  describeSignalled,
  LEFTOVER_GRACE_MS,
  PRODUCTION,
  RAM_TEMP_CAPACITY_BYTES,
  type RamTempLease,
  RamTempVolume,
  ramTempPaths,
} from './ram-temp.js';
import { createRunnerLifecycle, withFixtureLifecycle, withRunnerLifecycle } from './runner-lifecycle.js';
import { CPU_TOKENS_ENV, runnerDemand, sizeRunner, sizeRunnerToGrant } from './runner-sizing.js';
import { checkRuntimePin, describeRuntimePinRefusal } from './runtime-pin.js';
import { type BoundedExecOptions, boundedCommand } from './schema.js';
import { createTestResultWriter, type TestResultWriter } from './test-result-writer.js';
import {
  type BoundedExecRecord,
  type BoundedExecTask,
  BUN_TEST,
  clearTaskDirectory,
  describeNoSpace,
  type ExpiredBound,
  JUNIT_FILE,
  JUNIT_REPORT_ENV,
  judgeOperationalFailure,
  judgeRun,
  type NoSpaceSighting,
  readJunitReport,
  taskDirectory,
  withJunitReport,
  writeRecord,
} from './verdict.js';

const DEFAULT_KILL_AFTER_MS = 10_000;
const REAP_AFTER_FORCE_KILL_MS = 2_000;
type IdleDiagnosticResult =
  | { kind: 'pending'; startedAtMs: number | null }
  | { kind: 'captured'; snapshot: HangDiagnostics }
  | { kind: 'failed'; detail: string };
/** The pool descriptor: only a grant may give it, so neither the target's nor the inherited environment passes it on. */
const POOL_ENV = [CPU_TOKENS_ENV, CPU_POOL_ENV];
/**
 * The variables a grant sets for the command it sizes: no target's environment may set them. A jobserver the executor
 * itself inherits, a parent make's or its cowshed job's, still reaches an ungranted command by Cargo's own rule; a
 * granted one's sub-pool names its own fifo over it.
 */
const BUDGET_ENV = [
  ...POOL_ENV,
  'CARGO_MAKEFLAGS',
  'MAKEFLAGS',
  'MFLAGS',
  DIAGNOSTIC_CONTEXT_ENV,
  DIAGNOSTIC_MODULE_ENV,
];

export interface BoundedExecContext {
  root: string;
  /** The Nx task this run executes as, and the directory its verdict and report go to. */
  record?: { task: BoundedExecTask; directory: string };
  /**
   * The class this task's reserved leaves name on their run's mbx endpoint, by which mbx keys its run-time predictions
   * of them: `nx:<workspace>:<task id>`, the workspace named by its root package.json. Absent outside an Nx task, or in
   * a workspace whose package.json names none: its leaves then reserve unnamed.
   */
  leafClass?: string;
  /** Owns child stdout when present; otherwise it is retained and forwarded normally. */
  stdout?: (chunk: string) => void;
}

/** The part of Nx's executor context that names the running task. */
export interface NxTaskContext {
  root: string;
  projectName?: string;
  targetName?: string;
  configurationName?: string;
  taskGraph?: { tasks: Record<string, { hash?: string }> };
}

export type BoundedExecResult = RunOutcome;

interface RunState {
  settled: boolean;
  expiry: ExpiredBound | null;
  forceKillNeeded: boolean;
  /** This executor sent the command's process group a signal: a SIGKILL it died of is not from outside. */
  sentSignal: boolean;
  /** The sub-pool's close error when it did not close cleanly: the run fails. */
  poolCloseError: string | null;
  /** Why the command's subtree could not be retired, when the supervisor's client failed: the run fails. */
  retireError: string | null;
  /** The first output line that said ENOSPC, with the task's volume read when it arrived. */
  noSpace: Promise<NoSpaceSighting> | null;
  /** The lock waits the expiry's snapshot named: empty unless that capture completed. Judged with the expiry. */
  lockWaits: readonly LockWait[];
  /** The processes the expiry's snapshot saw gain CPU time: empty unless that capture completed. Judged with the expiry. */
  cpuRising: readonly CpuRise[];
}

export interface ProcessTreeKiller {
  kill(pid: number, signal: NodeJS.Signals): Promise<void>;
}

/** Where the command's TMPDIR comes from; `null` leaves the inherited one. */
export type TempVolume = Pick<
  RamTempVolume,
  'acquire' | 'release' | 'fullness' | 'space' | 'record' | 'ordinaryParent'
>;

/** Workspace-provided cowshed launchers, in the order a workspace that provides one names them. */
const WORKSPACE_COWSHED_LAUNCHERS = [
  'packages/cowshed/bin/cowshed',
  'node_modules/@smoothbricks/cowshed/bin/cowshed',
] as const;

/**
 * The cowshed a run's disk commands use, in precedence order: the target's named one, an
 * explicit one inherited from the parent process, the installed one on PATH, and — only when
 * PATH provides none — the launcher this workspace itself provides (its own cowshed package,
 * or a linked one's bin). An environment with no cowshed anywhere keeps the bare name, so
 * its spawn failure names the missing installed tool.
 */
export function resolveCowshedExecutable(
  root: string,
  named: string | undefined,
  inherited: string | undefined,
  pathValue: string | undefined,
  cwd: string,
  exists: (path: string) => boolean = (path) => {
    try {
      accessSync(path, constants.X_OK);
      return true;
    } catch {
      return false;
    }
  },
): string | undefined {
  // A target-named executable is workspace-relative or absolute against the workspace root.
  // An inherited explicit keeps the coordinate the parent gave it, exactly as the host
  // commands always spawned it: an absolute one verbatim, a relative or bare one for
  // spawn's own resolution — retargeting it to the root would silently change the tool.
  if (named !== undefined) return resolvePath(root, named);
  if (inherited !== undefined) return inherited;
  // An empty PATH component names the spawning process's current directory, as POSIX
  // spawn resolution does — the executor never chdirs, so that is the caller's cwd,
  // not the workspace root.
  const onPath = (pathValue ?? '').split(delimiter).some((dir) => exists(join(dir === '' ? cwd : dir, 'cowshed')));
  if (onPath) return undefined;
  for (const candidate of WORKSPACE_COWSHED_LAUNCHERS) {
    const launcher = join(root, candidate);
    if (exists(launcher)) return launcher;
  }
  return undefined;
}

export default function boundedExecExecutor(
  options: BoundedExecOptions,
  context: NxTaskContext,
): Promise<BoundedExecResult> {
  return runDefaultBoundedExec(options, recordContext(context));
}

/**
 * Runs with the normal process, RAM lease and CPU pool owners, with an optional stdout sink, on the user's RAM temp
 * volume where there is one, through the host's own gateway: the Nx executor's runs, never a test's.
 */
export function runDefaultBoundedExec(
  options: BoundedExecOptions,
  context: BoundedExecContext,
): Promise<BoundedExecResult> {
  // Only macOS has `hdiutil ram://`; elsewhere the command keeps the inherited TMPDIR.
  return runBoundedExecWith(options, context, (cowshed) =>
    process.platform === 'darwin'
      ? new RamTempVolume(
          ramTempPaths(userInfo(), PRODUCTION),
          RAM_TEMP_CAPACITY_BYTES,
          createHostCommands(GATEWAY_SOCKET, undefined, undefined, cowshed),
          undefined,
          undefined,
          'optional',
        )
      : null,
  );
}

/**
 * The default runner with every owner but its RAM volume, which its caller makes: `volume` makes the volume for the
 * cowshed the run's disk commands go through. The Nx executor makes the user's; a host proof passes a fixture volume
 * (`fixtureRamVolume`), so no test leases, creates or detaches the user's volume.
 */
export function runBoundedExecWith(
  options: BoundedExecOptions,
  context: BoundedExecContext,
  volume: (cowshed: string | undefined) => TempVolume | null,
): Promise<BoundedExecResult> {
  // A target that names the cowshed its disk commands run through names it for this run's own RAM volume work and
  // for its command alike. Nx resolves `{workspaceRoot}/…` in an option to a workspace-relative path, which the
  // command, starting in another directory, could not find: it gets the absolute one.
  const named = options.env?.[COWSHED_EXECUTABLE_ENV];
  const cowshed = resolveCowshedExecutable(
    context.root,
    named,
    process.env[COWSHED_EXECUTABLE_ENV],
    process.env.PATH,
    process.cwd(),
  );
  const run: BoundedExecOptions =
    cowshed === undefined ? options : { ...options, env: { ...options.env, [COWSHED_EXECUTABLE_ENV]: cowshed } };
  const tempVolume = volume(cowshed);
  // The command's processes are owned by the supervisor that serves this job, when one does
  // (specs/cowshed/07_api.md, `JobSubtreeHandle`); with none, nothing here may end what it leaves.
  const owner = process.env[SUPERVISOR_SOCKET_ENV] === undefined ? null : cowshedSubtreeOwner(cowshed ?? 'cowshed');
  // A command a parent's CPU pool started draws its tokens from that pool. The pool is named by the environment the
  // parent set for this command; a run no parent started has none.
  const inherited = readInheritedCpuPool(process.env);
  return runBoundedExec(run, context, createProcessTreeKiller(), tempVolume, inherited, captureHangDiagnostics, owner);
}

/**
 * Nx gives an executor the task's project, target and configuration, not its id; the id is the
 * key of the task graph entry they name. Outside a task graph the run records nothing and its
 * reserved leaves name no class.
 */
export function recordContext(context: NxTaskContext): BoundedExecContext {
  const { root, projectName, targetName, configurationName, taskGraph } = context;
  if (!projectName || !targetName) {
    return { root };
  }
  const id = `${projectName}:${targetName}${configurationName ? `:${configurationName}` : ''}`;
  const task = taskGraph?.tasks[id];
  if (task === undefined) {
    return { root };
  }
  const workspace = workspaceName(root);
  return {
    root,
    record: {
      task: { id, hash: task.hash ?? null },
      directory: taskDirectory(workspaceDataDirectoryForWorkspace(root), id),
    },
    ...(workspace === undefined ? {} : { leafClass: `nx:${workspace}:${id}` }),
  };
}

/** The workspace's name: its root package.json's `name`, which every checkout of the repository shares. */
function workspaceName(root: string): string | undefined {
  const path = join(root, 'package.json');
  if (!existsSync(path)) return undefined;
  const manifest: unknown = JSON.parse(readFileSync(path, 'utf8'));
  return typeof manifest === 'object' && manifest !== null && 'name' in manifest && typeof manifest.name === 'string'
    ? manifest.name
    : undefined;
}

/**
 * Run the command, sized to the tokens its parent's CPU pool (`cpu-pool.ts`) lends it; a run that inherits no pool
 * runs it as asked. Both bounds start once it is granted: a wait for the pool is not the command's time.
 *
 * `inherited` is the parent's pool: an enclosing bounded-exec's sub-pool or the Nx run's own (`run-cpu-pool.ts`). A
 * granted run serves a sub-pool of its grant to its own descendants, closed before the grant returns. A grant whose
 * connection closes while the command runs (the lending parent's exit) ends its accounting, never the command: the
 * command runs on over budget to its own exit, and the run says so.
 */
export async function runBoundedExec(
  options: BoundedExecOptions,
  context: BoundedExecContext,
  killer: ProcessTreeKiller,
  tempVolume: TempVolume | null,
  inherited: InheritedCpuPool | null = null,
  captureDiagnostics: typeof captureHangDiagnostics = captureHangDiagnostics,
  owner: SubtreeOwner | null = null,
  queueHost?: QueueHost,
): Promise<BoundedExecResult> {
  const directory = context.record?.directory ?? null;
  // The task's last recorded peak, and the tokens it was lent when the peak was measured, are its charge in its parent's
  // pool, read once before a run's clear removes them: a command run again after a kill is charged by the record the
  // killed run replaced, not by the peak it reached before it was killed.
  const lastPeak = directory !== null && inherited !== null ? await readLastPeak(directory) : null;
  return runUntilNotKilledForMemory(
    (alone) =>
      runCommand(options, context, killer, tempVolume, inherited, captureDiagnostics, owner, queueHost, {
        lastPeak,
        alone,
      }),
    inherited !== null,
    context.record?.task.id,
  );
}

/** What a run of the command is told that the run before it was not: the task's charge, and whether it runs alone. */
interface Attempt {
  readonly lastPeak: LastPeak | null;
  readonly alone: boolean;
}

/** One run of the command, from the pool's grant to its record. */
async function runCommand(
  options: BoundedExecOptions,
  context: BoundedExecContext,
  killer: ProcessTreeKiller,
  tempVolume: TempVolume | null,
  inherited: InheritedCpuPool | null,
  captureDiagnostics: typeof captureHangDiagnostics,
  owner: SubtreeOwner | null,
  queueHost: QueueHost | undefined,
  attempt: Attempt,
): Promise<BoundedExecResult> {
  const cwd = resolveCwd(options.cwd, context.root);
  const directory = context.record?.directory ?? null;
  const { lastPeak } = attempt;
  // A previous run's verdict and report go first, so neither can be read as this run's.
  if (directory !== null) {
    await clearTaskDirectory(directory);
  }
  const requested = boundedCommand(options);
  const timeoutMs = options.timeoutMs;
  const idleTimeoutMs = options.idleTimeoutMs;
  const killAfterMs = options.killAfterMs ?? DEFAULT_KILL_AFTER_MS;
  const fixture: { command: string; timeoutMs: number } | null =
    options.fixtureCommand !== undefined && options.fixtureTimeoutMs !== undefined
      ? { command: options.fixtureCommand, timeoutMs: options.fixtureTimeoutMs }
      : null;
  const outputChunks: string[] = [];
  let resultWriter: TestResultWriter | null = null;
  let resultFailure: string | null = null;
  const state: RunState = {
    settled: false,
    expiry: null,
    forceKillNeeded: false,
    sentSignal: false,
    poolCloseError: null,
    retireError: null,
    noSpace: null,
    lockWaits: [],
    cpuRising: [],
  };
  const appendStdout = (chunk: Buffer | string): void => {
    const text = chunk.toString();
    if (context.stdout !== undefined) {
      context.stdout(text);
    } else {
      outputChunks.push(text);
      process.stdout.write(text);
    }
  };
  const appendStderr = (chunk: Buffer | string): void => {
    const text = chunk.toString();
    outputChunks.push(text);
    process.stderr.write(text);
  };
  // Told, never thrown, wherever the command is being ended: a diagnostic must neither replace the failure it
  // accompanies nor skip the cleanup that follows (the signal handlers, the CPU grant, the RAM lease, the run's record).
  const tell = (text: string): void => {
    try {
      appendStderr(text);
    } catch {
      // Nobody is left to tell.
    }
  };
  // A run refused before its command exists says why and records an operational failure, and no peak: nothing ran.
  const refuseUnstarted = async (message: string): Promise<BoundedExecResult> => {
    appendStderr(message);
    if (context.record !== undefined) {
      const { task, directory: recordDirectory } = context.record;
      await writeRecord(recordDirectory, {
        task: task.id,
        hash: task.hash,
        verdict: judgeOperationalFailure(1, null),
        peakBytes: null,
      });
    }
    return { success: false, terminalOutput: outputChunks.join('') };
  };

  // The fixture pair names one segment: a fixture with no build budget, or a budget with no fixture, is a
  // target that does not say what it runs, so it does not run. Units are counted at the end of that segment.
  if (
    (options.fixtureCommand === undefined) !== (options.fixtureTimeoutMs === undefined) ||
    (options.unitsCommand !== undefined && options.fixtureCommand === undefined) ||
    (fixture !== null && !(fixture.timeoutMs > 0))
  ) {
    return refuseUnstarted(
      'bounded-exec: fixtureCommand and a positive fixtureTimeoutMs must be named together, and unitsCommand only with them; refusing to run the command\n',
    );
  }

  // A leg that runs bun runs the Bun its workspace declares, or it does not run. This comes before the RAM lease and the
  // CPU tokens: a refusal that waited in its pool's queue first would say nothing for minutes.
  const runtime = await checkRuntimePin({
    root: context.root,
    command: [fixture?.command, options.unitsCommand, requested].filter((part) => part !== undefined).join(' && '),
    env: { ...withoutPoolEnv(process.env), ...options.env },
    cwd,
    pathSource: options.env?.PATH === undefined ? 'the inherited environment' : "the target's env",
  });
  if (runtime !== null) {
    return refuseUnstarted(describeRuntimePinRefusal(runtime, context.record?.task.id));
  }
  if (lastPeak !== null && !lastPeak.ok) {
    return refuseUnstarted(
      `bounded-exec: ${lastPeak.error}; it is this task's memory charge, so the command does not run\n`,
    );
  }
  // A task with no recorded peak is charged nothing (null) until it has one. A peak is charged in proportion to the
  // tokens the run is lent when that is more than the run that measured it was lent.
  const charge = lastPeak?.ok === true ? lastPeak.peakBytes : null;
  const chargeParallelism = lastPeak?.ok === true ? lastPeak.parallelism : null;

  let lease: RamTempLease | null = null;
  let ordinaryTemp: string | null = null;
  // Before spawn there is nothing to retire. Once started, only the owner's terminal retirement permits deletion.
  let subtreeRetired = true;
  // A lease is removed when its command ends, so it is taken only where whatever the command leaves
  // in it can be ended by its owner first: the supervisor's subtree retirement, never a census.
  if (tempVolume !== null && owner === null) {
    appendStderr(
      `RAM temp volume not leased: no cowshed supervisor owns this command's processes (${SUPERVISOR_SOCKET_ENV} is unset); TMPDIR stays ${process.env.TMPDIR ?? '(unset)'}\n`,
    );
  } else if (tempVolume !== null && owner !== null) {
    const acquired = await tempVolume.acquire(owner);
    if (!acquired.ok) {
      const message = `${describeRamTempError(acquired.error)}\n`;
      process.stderr.write(message);
      return { success: false, terminalOutput: message };
    }
    if (acquired.value.kind === 'leased') {
      lease = acquired.value.lease;
      for (const held of acquired.value.held) {
        appendStderr(`RAM temp volume: dead lease ${held}\n`);
      }
      for (const reaped of acquired.value.reaped) {
        appendStderr(`RAM temp volume: the supervisor stopped ${reaped}, which a dead task's command left running\n`);
      }
      for (const gap of acquired.value.gaps) {
        appendStderr(`RAM temp volume: the supervisor's view of a dead task's command has a gap: ${gap}\n`);
      }
      for (const retained of acquired.value.retained) {
        appendStderr(`RAM temp volume: dead lease ${retained}\n`);
      }
    } else if (acquired.value.kind === 'unavailable') {
      const unavailable = `RAM temp volume unavailable (${acquired.value.reason}: ${acquired.value.detail})`;
      if (options.env?.TMPDIR !== undefined) {
        appendStderr(`${unavailable}; using target-provided TMPDIR ${options.env.TMPDIR} (medium not inferred)\n`);
      } else {
        try {
          ordinaryTemp = await mkdtemp(join(tempVolume.ordinaryParent, `smoo-bounded-exec-${process.pid}-`));
        } catch (error) {
          appendStderr(
            `${unavailable}; could not create ordinary scratch under ${tempVolume.ordinaryParent}: ${error instanceof Error ? error.message : String(error)}\n`,
          );
          return { success: false, terminalOutput: outputChunks.join('') };
        }
        appendStderr(`${unavailable}; using owned ordinary TMPDIR ${ordinaryTemp}\n`);
      }
    } else {
      appendStderr(
        `RAM temp volume unavailable in this sandbox (${acquired.value.detail}); TMPDIR stays ${process.env.TMPDIR ?? '(unset)'}\n`,
      );
    }
  }
  // The supervisor's answer to the command's own admission, once it is spawned as an owned subtree root, and once
  // any lease has recorded the handle for whoever settles that lease should this task die first.
  let admission: Promise<SubtreeAdmission> | null = null;
  const retireSubtree = async (): Promise<void> => {
    if (admission === null || owner === null) {
      return;
    }
    const answer = await admission;
    admission = null;
    if (answer.kind !== 'admitted') {
      appendStderr(
        `cowshed: the command ran without an owned subtree (${answer.kind === 'refused' ? `supervisor: ${answer.reason}` : answer.detail}); nothing retires what it left running\n`,
      );
      return;
    }
    // What the command left running is a leak, whichever way the command ended: its supervisor ends exactly the
    // processes its tree records under the command, before any lease goes, and each is said so its owner fixes it.
    const retired = await owner.retire(answer.handle, LEFTOVER_GRACE_MS);
    switch (retired.kind) {
      case 'retired':
        subtreeRetired = true;
        for (const entry of retired.signalled) {
          appendStderr(`cowshed: the supervisor stopped ${describeSignalled(entry)}, which the command left running\n`);
        }
        if (retired.signalledIncomplete) {
          appendStderr("cowshed: the supervisor may have stopped processes of the command's that it could not name\n");
        }
        for (const gap of retired.gaps) {
          appendStderr(
            `cowshed: the supervisor's view of the command's processes has a gap: ${describeCoverageGap(gap)}\n`,
          );
        }
        return;
      case 'refused':
        appendStderr(`cowshed: the supervisor refused to retire the command's processes (${retired.reason})\n`);
        return;
      case 'failed':
        state.retireError = retired.detail;
        appendStderr(`cowshed: could not retire the command's processes: ${retired.detail}\n`);
        return;
    }
  };
  // Failed scratch remains evidence: RAM leases are archived; ordinary scratch is retained at its reported path.
  let tempReleased: Promise<void> | null = null;
  const releaseLease = (keep: boolean): Promise<void> => {
    tempReleased ??= (async () => {
      await retireSubtree();
      if (ordinaryTemp !== null) {
        const held = ordinaryTemp;
        ordinaryTemp = null;
        if (keep || state.retireError !== null || !subtreeRetired) {
          appendStderr(
            `Ordinary temp directory retained at ${held}: ${!subtreeRetired ? 'the command subtree was not retired' : 'the run did not pass'}\n`,
          );
        } else {
          try {
            await rm(held, { recursive: true, force: true });
          } catch (error) {
            appendStderr(
              `Ordinary temp directory retained at ${held}: cleanup failed: ${error instanceof Error ? error.message : String(error)}\n`,
            );
          }
        }
      }
      if (lease === null || owner === null) {
        return;
      }
      const held = lease;
      lease = null;
      const released = await tempVolume?.release(held, keep || state.retireError !== null, owner);
      if (released?.ok) {
        for (const retained of released.value) {
          appendStderr(`RAM temp volume: lease ${retained}\n`);
        }
      } else if (released) {
        appendStderr(`${describeRamTempError(released.error)}\n`);
      }
    })();
    return tempReleased;
  };

  // The wait is one span line, as cowshed's own spans read. Every Nx run that inherits no pool serves its own
  // (`run-cpu-pool.ts`), so a task takes its tokens from a parent's pool; a command that inherits none runs exactly as
  // asked and nothing is said.
  const cores = availableParallelism();
  const mbx = mbxPool(process.env);
  const demand = runnerDemand(requested, options.parallelism, cores, process.env, options.env);
  const asked = Date.now();
  // A pool that answered and refused is a live budget, and running beside it would count the run twice: the run fails
  // instead. A pool whose endpoint is gone (its holder exited) lends nothing, and the run goes on as asked.
  const parent = await askParentPool(inherited, {
    want: demand.want,
    charge,
    parallelism: chargeParallelism,
    alone: attempt.alone,
    task: context.record?.task.id,
  });
  for (const said of parent.said) appendStderr(said);
  const { grant, refusal: parentRefusal, wait: parentWait } = parent;
  const waitedMs = Date.now() - asked;
  let cpu: BoundedExecRecord['cpu'];
  let sized: { command: string; env: Record<string, string> } = { command: requested, env: {} };
  // With mbx the leaf's admission is mbx's one pool, not a parent's tokens. A leaf mbx does not admit itself asks for
  // its runner's count, a maximum, and its runner runs on the cores it is granted, which only the command learns
  // (specs/cowshed/05_gateway.md, "Parallelism inside a job"). No grant and no sub-pool beside it.
  const leaf = mbx === null ? null : mbxLeaf(mbx, demand, requested);
  if (grant !== null) {
    sized = sizeRunner(demand, requested, grant.tokens);
    cpu = { tokens: grant.tokens, want: demand.want, waitedMs };
    for (const said of describeGrant(grant, { waitedMs, want: demand.want, runner: demand.kind, charge })) {
      appendStderr(said);
    }
  } else if (leaf?.kind === 'reserve') {
    sized = { command: sizeRunnerToGrant(leaf.demand, requested), env: {} };
  } else if (leaf !== null) {
    sized = sizeRunner(demand, requested, demand.want);
  }
  // The run's sub-pool and grant are given back exactly once, however the run ends. The sub-pool closes first, so no
  // descendant still draws on tokens the grant has returned. A reserved leaf's mbx grant covers the leaf's conclusion,
  // its subtree's retirement included (11_shell.md), so it goes back after `releaseLease`.
  let pool: CpuPool | null = null;
  let reservation: Reservation | null = null;
  const releaseReservation = (): void => reservation?.release();
  // The run's progress channel (progress-channel.ts), served when the run has an idle bound for its records to restart,
  // and closed once its command is gone. A channel that leaves its socket behind says so; the run's verdict stands.
  let progressChannel: ProgressChannel | null = null;
  let progressClosed: Promise<void> | null = null;
  const closeProgress = (): Promise<void> => {
    progressClosed ??= (async () => {
      const left = (await progressChannel?.close()) ?? null;
      if (left !== null) appendStderr(`bounded-exec: ${left}\n`);
    })();
    return progressClosed;
  };
  let cpuReleased: Promise<void> | null = null;
  const releaseCpu = (): Promise<void> => {
    cpuReleased ??= (async () => {
      try {
        const outcome = await pool?.close();
        if (outcome !== undefined && !outcome.ok) {
          state.poolCloseError = outcome.error;
          appendStderr(`cowshed: cpu pool did not close cleanly: ${outcome.error}\n`);
        }
      } finally {
        grant?.release();
      }
    })();
    return cpuReleased;
  };
  const queue = new AdmissionObservation({
    over: () => state.settled || state.expiry !== null,
    pid: () => child.pid,
    environment: () => mergeEnv(lease?.directory ?? ordinaryTemp, env),
    tell,
    step: (event) => stepTo(event),
    testing: () => isTesting(),
    queueHost,
  });
  // Records the run's verdict when it has a task to record as. An `expiry` is set only when a bound ended the run.
  // Observation and ownership failures remain operational even after valid queue evidence; cancellation alone does not.
  const recordRun = async (
    exitCode: number,
    expiry: ExpiredBound | null,
    elapsedMs: number,
    operational: boolean,
    noSpace: NoSpaceSighting | null,
    lockWaits: readonly LockWait[],
    cpuRising: readonly CpuRise[],
    peakBytes: number | null,
  ): Promise<void> => {
    if (context.record === undefined) {
      return;
    }
    const { task, directory: recordDirectory } = context.record;
    const report = await readJunitReport(
      recordDirectory,
      requested.search(BUN_TEST) !== -1 && resultWriter !== null
        ? (results) => {
            const error = resultWriter?.reconcile(results) ?? null;
            if (error !== null) {
              resultFailure ??= error;
              appendStderr(`bounded-exec: ${error}\n`);
            }
          }
        : undefined,
    ).finally(async () => {
      const error = (await resultWriter?.close()) ?? null;
      if (error !== null) {
        resultFailure ??= error;
        appendStderr(`bounded-exec: ${error}\n`);
      }
    });
    const admissionQueue = queue.queueWait(reservation !== null);
    const verdict =
      operational || resultFailure !== null
        ? judgeOperationalFailure(exitCode, report)
        : judgeRun({
            exitCode,
            elapsedMs,
            expiry,
            report,
            noSpace,
            lockWaits,
            cpuRising,
            admissionQueue,
          });
    if (verdict.outcome === 'bound' && verdict.bound === 'test') {
      appendStderr(`Every failing test failed only on its per-test timeout: ${verdict.tests.join(', ')}\n`);
    }
    await writeRecord(recordDirectory, {
      task: task.id,
      hash: task.hash,
      verdict,
      ...(cpu ? { cpu } : {}),
      ...(parentWait ? { cpuWait: parentWait } : {}),
      peakBytes,
    });
  };
  // A run refused before its command starts says why, gives back its grant and RAM, and records the failure.
  const refuse = async (message: string): Promise<BoundedExecResult> => {
    appendStderr(message);
    await releaseCpu();
    await closeProgress();
    // The command never started, so its lease holds nothing of its own to keep.
    await releaseLease(false);
    await recordRun(1, null, Date.now() - asked, false, null, [], [], null);
    return { success: false, terminalOutput: outputChunks.join('') };
  };
  if (parentRefusal !== null) {
    return refuse(`cowshed: cpu-pool parent pool refused: ${parentRefusal}; refusing to run the command\n`);
  }
  // A reserved leaf takes its cores from its Nx run's mbx reservation endpoint (mbx-reservations.ts). A run that serves
  // none said why in its own output, which a gate's failures-only output never shows, so the refusal says it too; the
  // leaf does not run beside the pool.
  const endpoint = leaf?.kind === 'reserve' ? runEndpoint(process.env) : null;
  if (endpoint !== null && endpoint.kind !== 'served') {
    return refuse(
      `cowshed: a reserved leaf takes its cores from its Nx run's mbx reservation endpoint, and ${RESERVATIONS_ENV} names none: ${describeUnserved(endpoint)}; refusing to run the command\n`,
    );
  }
  const reserved = leaf?.kind === 'reserve' && endpoint?.kind === 'served' ? { leaf, endpoint: endpoint.socket } : null;

  // A granted run serves a sub-pool of its own grant, so its descendants draw from that and never from above it.
  // A pool that cannot be set up refuses the run: no granted command runs beside its grant.
  if (grant !== null) {
    const created = await createCpuPool(grant.tokens, grant.closed, null);
    if (!created.ok) {
      return refuse(`cowshed: cpu pool unavailable: ${created.error}; refusing to run the command\n`);
    }
    pool = created.pool;
  }
  // A running test reports its own progress on the run's progress channel, and each record restarts the idle bound as a
  // finished test does; a record that is not one fails the run as a broken reporter. A run with no idle bound has
  // nothing for a record to restart and is served none. Only the command learns the socket, from its environment, so
  // these handlers run after it is spawned and its bounds are wired below.
  if (idleTimeoutMs !== undefined) {
    const served = await serveProgressChannel({
      moved: () => {
        if (fixtureDone) stepTo({ kind: 'reported', at: Date.now(), testing: isTesting() });
      },
      invalid: (detail) => {
        if (lifecycleFailure !== null || state.expiry !== null || state.settled) return;
        lifecycleFailure = `progress channel: ${detail}`;
        appendStderr(`bounded-exec: ${lifecycleFailure}\n`);
        termination = terminate();
      },
    });
    if (!served.ok) {
      return refuse(
        `bounded-exec: the run's progress channel could not be served (${served.error}); refusing to run the command\n`,
      );
    }
    progressChannel = served.channel;
  }
  const native = directory === null ? { command: sized.command, env: {} } : withTestResultReport(sized.command);
  const command =
    directory === null
      ? withProgressReport(native.command)
      : await withJunitReport(withProgressReport(native.command), directory);
  // A runner bounded-exec cannot see in the command (a script that spawns `bun test`) writes its JUnit report to this
  // path when it is set. The grant's sizing outranks the target's own environment: the runner may use what it was
  // granted, not what it was configured with. The sub-pool's environment comes last: a descendant reads only the pool
  // this run serves.
  const env: Record<string, string> = {
    ...(directory === null ? {} : { [JUNIT_REPORT_ENV]: join(directory, JUNIT_FILE) }),
    ...targetEnv(options.env, appendStderr),
    ...sized.env,
    ...native.env,
    ...(pool?.environment ?? {}),
    ...(await grantMemoryEnvironment(grant?.charged ?? null, process.env)),
    ...(leaf?.kind === 'cargo' ? { MBX_GRANT_FD: String(MBX_GRANT_FD) } : {}),
    ...(progressChannel === null ? {} : { [PROGRESS_SOCKET_ENV]: progressChannel.socket }),
  };
  // A fixture rides as segment 0 of the lifecycle. Without the channel (Windows) it is plain shell text ahead
  // of the command: the progress bound runs during it and its own ceiling does not. Under mbx a leaf that names its
  // units counts them at the end of that segment, under the fixture's ceiling, its fixture's own output on stderr, and
  // reserves after it (`withCountedReservation`).
  const counted =
    process.platform !== 'win32' && leaf?.kind === 'reserve' && fixture !== null && options.unitsCommand !== undefined;
  const segment =
    fixture === null ? null : counted ? `{ ${fixture.command}; } >&2 && ${options.unitsCommand}` : fixture.command;
  const fixtureHooked =
    process.platform === 'win32' || segment === null ? null : withFixtureLifecycle(segment, command);
  const hooked = process.platform === 'win32' ? null : (fixtureHooked ?? withRunnerLifecycle(command));
  const lifecycle = hooked === null ? null : createRunnerLifecycle(hooked.runners);
  let lifecycleFailure: string | null = null;
  const isTesting = (): boolean => lifecycleFailure !== null || (lifecycle?.isTesting() ?? true);
  const startedAt = Date.now();
  // With a supervisor, `cowshed subtree-root` asks it for a handle on itself, writes the answer to the handle
  // descriptor, and execs `/bin/sh -c command` in the same process: the command's pid, group and life are unchanged.
  const unadmitted = hooked?.command ?? (segment === null ? command : `${segment} && ${command}`);
  const shellCommand =
    leaf?.kind === 'reserve' && counted && fixtureHooked !== null
      ? withCountedReservation(fixtureHooked.fixture, fixtureHooked.main, leaf)
      : withMbxLeaf(unadmitted, leaf);
  // Only leaves known to be scheduled can exclude zero-hold time. Others keep lifecycle hang protection.
  const queued = mbxIdleGated(leaf, sized.command, { ...process.env, ...env });
  // fd 4 is unused without a subtree admission, so mbx's descriptor takes fd 5 either way: a Cargo leaf's admission
  // stream, or a reserved leaf's ask for cores and their count (mbx-pool.ts, `MBX_GRANT_FD`).
  const grantSlots: IOType[] = leaf !== null ? ['ignore', 'pipe'] : [];
  const rootGrantSlot: IOType[] = leaf !== null ? ['pipe'] : [];
  const root = owner?.rootArgv(shellCommand);
  // The command's own descriptors past stderr, from fd 3: the lifecycle channel, the subtree admission, mbx's grant.
  const descriptors: IOType[] =
    root === undefined
      ? hooked === null && leaf === null
        ? []
        : [hooked === null ? 'ignore' : 'pipe', ...grantSlots]
      : [hooked === null ? 'ignore' : 'pipe', 'pipe', ...rootGrantSlot];
  // A run that records its verdict records its command's peak too: the command runs under a waiter that reports it.
  const waiter = context.record === undefined ? null : peakWaiter();
  const measured =
    waiter?.kind === 'waiter'
      ? underWaiter(waiter.runtime, root?.file ?? '/bin/sh', root?.args ?? ['-c', shellCommand], descriptors)
      : null;
  let child: ChildProcess;
  const results = directory === null ? null : await createTestResultWriter(directory, { cwd, root: context.root });
  if (results !== null && !results.ok) return refuse(`bounded-exec: ${results.error}\n`);
  resultWriter = results?.ok ? results.writer : null;
  try {
    // fd 3 is the runner lifecycle's duplex socket when the command is hooked; the subtree admission takes fd 4, which
    // cowshed closes fd 4 before exec; fd 5 is a Cargo leaf's mbx admission stream or a reserved leaf's ask and grant;
    // the waiter, when there is one, passes them on and reports the command's peak on fd 6.
    child =
      measured !== null
        ? spawn(measured.file, measured.args, {
            cwd,
            env: mergeEnv(lease?.directory ?? ordinaryTemp, env),
            stdio: measured.stdio,
            detached: process.platform !== 'win32',
            windowsHide: true,
          })
        : root === undefined
          ? spawn(shellCommand, [], {
              cwd,
              env: mergeEnv(lease?.directory ?? ordinaryTemp, env),
              shell: true,
              stdio: descriptors.length === 0 ? 'pipe' : ['pipe', 'pipe', 'pipe', ...descriptors],
              detached: process.platform !== 'win32',
              windowsHide: true,
            })
          : spawn(root.file, root.args, {
              cwd,
              env: mergeEnv(lease?.directory ?? ordinaryTemp, env),
              stdio: ['pipe', 'pipe', 'pipe', ...descriptors],
              detached: process.platform !== 'win32',
              windowsHide: true,
            });
  } catch (error) {
    // Nothing started, so nothing runs under the grant: it goes back here, with the RAM lease.
    return refuse(`cowshed: could not start the command: ${error instanceof Error ? error.message : String(error)}\n`);
  }
  subtreeRetired = false;
  const reportChannel = child.stdio.at(PEAK_FD);
  const waiterReport =
    measured === null ? null : readWaiterReport(reportChannel instanceof Readable ? reportChannel : null);
  // The leaf's runner is its command: under a waiter, which leads the group, the child the waiter names; else the
  // group's leader itself. A hang snapshot names and samples it (specs/cowshed/08_testing.md).
  const runnerPid: Promise<number | null> = waiterReport?.started ?? Promise.resolve(child.pid ?? null);
  let runner: number | null = null;
  void runnerPid.then((pid) => {
    runner = pid;
  });
  if (root !== undefined) {
    const channel = child.stdio[HANDLE_FD];
    const leased = lease;
    admission = readAdmission(channel instanceof Readable ? channel : null).then(async (answer) => {
      if (answer.kind === 'admitted' && leased !== null && tempVolume !== null) {
        const recorded = await tempVolume.record(leased, answer.handle);
        if (!recorded.ok) {
          appendStderr(`${describeRamTempError(recorded.error)}\n`);
        }
      }
      return answer;
    });
  }

  // Progress in the output is a finished test, never output itself: a runner's SLOW warnings and a hung test's heartbeat
  // keep printing. A running test reports its own progress on the progress channel instead.
  const progress = createRunProgress(
    command,
    resultWriter === null
      ? undefined
      : (result) => {
          const error = resultWriter?.append(result) ?? null;
          if (error !== null && resultFailure === null) {
            resultFailure = error;
            lifecycleFailure ??= error;
            appendStderr(`bounded-exec: ${error}\n`);
            termination = terminate();
          }
        },
  );
  // A write that found no space is said once, in the output, by whatever made it; the volume is read when it is said,
  // since the writer usually removes what it half-wrote before it exits and the volume no longer looks full by then.
  const noSpace = createNoSpaceWatch();
  const sight = async (line: string): Promise<NoSpaceSighting> => {
    const held = lease;
    return { line, volume: held === null ? null : ((await tempVolume?.space(held)) ?? null) };
  };
  // Which bound is armed, and when it expires, is bounds.ts's decision; one timer waits for the earliest of them.
  let bounds: Bounds = UNARMED;
  let boundTimer: NodeJS.Timeout | undefined;
  // The fixture's own output is compilation, not the command's progress: it is not scanned for finished tests.
  let fixtureDone = fixture === null || lifecycle === null;
  // Whether a segment past the fixture ever announced: without one, the shell's exit is the fixture's.
  let mainStarted = fixture === null || lifecycle === null;
  let captureTimer: NodeJS.Timeout | undefined;
  let idleCapture: { expiresAtMs: number; controller: AbortController; result: IdleDiagnosticResult } | undefined;
  const cancelCapture = (): void => {
    clearTimeout(captureTimer);
    idleCapture?.controller.abort();
  };
  const scheduleCapture = (expiresAtMs: number): void => {
    const capture: NonNullable<typeof idleCapture> = {
      expiresAtMs,
      controller: new AbortController(),
      result: { kind: 'pending', startedAtMs: null },
    };
    idleCapture = capture;
    captureTimer = setTimeout(
      () => {
        if (state.settled || state.expiry !== null || idleCapture !== capture) return;
        const group = child.pid;
        if (group === undefined) {
          idleCapture.result = { kind: 'failed', detail: 'runner PID unavailable; no holder conclusion' };
          return;
        }
        idleCapture.result = { kind: 'pending', startedAtMs: Date.now() };
        void runnerPid
          .then((pid) => {
            if (pid === null)
              throw new Error('runner PID unavailable: its waiter named no command; no holder conclusion');
            return captureDiagnostics({
              pid,
              pgid: process.platform === 'win32' ? null : group,
              expiresAtMs: capture.expiresAtMs,
              deadlineMs: Math.max(0, Math.min(HANG_CAPTURE_MS, capture.expiresAtMs - Date.now())),
              signal: capture.controller.signal,
            });
          })
          .then(
            (snapshot) => {
              if (idleCapture === capture) idleCapture.result = { kind: 'captured', snapshot };
            },
            (error: unknown) => {
              if (idleCapture === capture) {
                idleCapture.result = {
                  kind: 'failed',
                  detail: error instanceof Error ? error.message : String(error),
                };
              }
            },
          );
      },
      Math.max(0, expiresAtMs - Date.now() - HANG_CAPTURE_MS),
    );
  };
  const expiredAs = (kind: BoundKind): ExpiredBound =>
    kind === 'idle'
      ? { kind, limitMs: idleTimeoutMs ?? 0, tests: progress.inFlight() }
      : kind === 'fixture'
        ? { kind, limitMs: fixture?.timeoutMs ?? 0 }
        : { kind, limitMs: timeoutMs ?? 0 };
  let checkingIdle = false;
  const armBoundTimer = (): void => {
    clearTimeout(boundTimer);
    if (state.settled || state.expiry !== null) return;
    const next = earliestBound(checkingIdle ? { ...bounds, idle: null } : bounds);
    if (next !== null) {
      boundTimer = setTimeout(
        () => {
          void confirmExpiry(next.kind, next.at);
        },
        Math.max(0, next.at - Date.now()),
      );
    }
  };
  const confirmExpiry = async (kind: BoundKind, at: number): Promise<void> => {
    if (state.settled || state.expiry !== null) return;
    const scheduled = earliestBound(checkingIdle ? { ...bounds, idle: null } : bounds);
    if (scheduled?.kind !== kind || scheduled.at !== at) return;
    if (kind === 'idle' && leaf?.kind === 'cargo') {
      if (checkingIdle) return;
      checkingIdle = true;
      armBoundTimer();
      try {
        // Drain an older poll, then take a fresh bounded snapshot before idle can end queued work. Total and fixture
        // ceilings remain armed while this observation runs.
        await queue.refresh(true);
      } finally {
        checkingIdle = false;
      }
    }
    if (state.settled || state.expiry !== null) return;
    const next = earliestBound(bounds);
    if (next !== null && next.at <= Date.now()) expire(expiredAs(next.kind));
    else armBoundTimer();
  };
  const stepTo = (event: BoundEvent): void => {
    if (state.settled || state.expiry !== null) return;
    const idleBefore = bounds.idle;
    bounds = stepBounds(
      bounds,
      {
        ...(idleTimeoutMs === undefined ? {} : { idleMs: idleTimeoutMs }),
        ...(fixture === null || lifecycle === null ? {} : { fixtureMs: fixture.timeoutMs }),
        ...(timeoutMs === undefined ? {} : { totalMs: timeoutMs }),
      },
      event,
    );
    if (bounds.idle !== idleBefore) {
      cancelCapture();
      idleCapture = undefined;
      if (bounds.idle !== null) scheduleCapture(bounds.idle);
    }
    armBoundTimer();
  };

  // Only the CHILD's own output is read for progress. The diagnostics below go
  // through appendStderr as well, and re-arming on those would let a teardown
  // message extend the very bound that just fired.
  const updateIdle = (finished: boolean, wasTesting: boolean): void => {
    stepTo({ kind: 'progress', at: Date.now(), testing: isTesting(), wasTesting, finished });
  };
  const scanProgress = (stream: 'stdout' | 'stderr', text: Buffer | string): void => {
    if (!fixtureDone) return;
    updateIdle(progress.scan(stream, text), isTesting());
    const error = progress.error();
    if (error !== null && lifecycleFailure === null) {
      lifecycleFailure = error;
      appendStderr(`bounded-exec: ${error}\n`);
      termination = terminate();
    }
  };
  if (lifecycle !== null) {
    const channel = child.stdio[3];
    if (!(channel instanceof Duplex)) throw new Error('runner lifecycle requires a duplex child descriptor');
    const failLifecycle = (detail: string, wasTesting = isTesting()): void => {
      if (lifecycleFailure !== null || state.expiry !== null) return;
      lifecycleFailure = detail;
      appendStderr(`bounded-exec: runner lifecycle failed: ${detail}\n`);
      updateIdle(false, wasTesting);
    };
    const lines = createInterface({ input: channel });
    lines.on('line', (line) => {
      const wasTesting = isTesting();
      const event = lifecycle.accept(line);
      if (event === null) {
        failLifecycle(lifecycle.error() ?? 'invalid lifecycle event', wasTesting);
        return;
      }
      updateIdle(false, wasTesting);
      if (event.id === 0 && event.phase === 'terminal' && fixture !== null) {
        fixtureDone = true;
        stepTo({ kind: 'fixture-done' });
      }
      if (event.id > 0) mainStarted = true;
      // Tool startup and native lifecycle-bearing stdout wait for their phase acknowledgement.
      channel.write(`${JSON.stringify(event)}\n`);
    });
    // readline re-emits its input's errors on the interface, where one with no listener is thrown: a
    // runner killed with unread lifecycle bytes resets the channel (ECONNRESET on Linux).
    lines.on('error', (error) => failLifecycle(error.message));
    channel.on('error', (error) => failLifecycle(error.message));
    channel.on('end', () => {
      if (!lifecycle.finished()) failLifecycle('supervisor channel ended without an owned terminal event');
    });
  }
  child.stdout?.on('data', (chunk: Buffer | string) => {
    const text = chunk.toString();
    scanProgress('stdout', chunk);
    const said = noSpace.scan('stdout', text);
    if (said !== null) state.noSpace = sight(said);
    appendStdout(text);
  });
  child.stderr?.on('data', (chunk: Buffer | string) => {
    const text = chunk.toString();
    scanProgress('stderr', chunk);
    const said = noSpace.scan('stderr', text);
    if (said !== null) state.noSpace = sight(said);
    appendStderr(text);
  });

  // The graceful pass: SIGTERM to the whole group. A signal that fails is told, not thrown: every caller has more to do
  // after it, `terminate` the force-kill and the signal handler its releases and the signal it gives back.
  const killChildTree = async (): Promise<void> => {
    const pid = child.pid;
    if (!pid) {
      return;
    }
    try {
      state.sentSignal = true;
      await killer.kill(pid, 'SIGTERM');
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      tell(`Terminating process group ${pid} failed: ${reason}\n`);
    }
  };

  const onProcessExit = (): void => {
    void killChildTree();
  };
  let signalCleanup: Promise<void> | null = null;
  const onTerminationSignal = (signal: NodeJS.Signals): void => {
    removeSignalHandlers();
    cancelCapture();
    clearTimeout(boundTimer);
    // Preserve explicit live membership before cancellation removes the queued process. A failed bounded observation
    // leaves the admission unconfirmed, never a manufactured queue or admission.
    signalCleanup = (leaf?.kind !== 'cargo' ? Promise.resolve() : queue.refresh(false))
      .finally(() => {
        queue.stop();
        return killChildTree();
      })
      .finally(closeProgress)
      .finally(releaseCpu)
      .finally(() => releaseLease(true))
      .finally(releaseReservation)
      .finally(async () =>
        recordRun(
          signalToExitCode(signal),
          state.expiry,
          Date.now() - startedAt,
          state.poolCloseError !== null || lifecycleFailure !== null || state.retireError !== null,
          state.noSpace === null ? null : await state.noSpace,
          state.lockWaits,
          state.cpuRising,
          null,
        ),
      )
      .finally(() => process.kill(process.pid, signal));
  };
  const onSigint = (): void => onTerminationSignal('SIGINT');
  const onSigterm = (): void => onTerminationSignal('SIGTERM');
  const onSighup = (): void => onTerminationSignal('SIGHUP');
  const removeSignalHandlers = (): void => {
    process.removeListener('exit', onProcessExit);
    process.removeListener('SIGINT', onSigint);
    process.removeListener('SIGTERM', onSigterm);
    process.removeListener('SIGHUP', onSighup);
  };

  process.on('exit', onProcessExit);
  process.on('SIGINT', onSigint);
  process.on('SIGTERM', onSigterm);
  process.on('SIGHUP', onSighup);

  // `Promise.withResolvers` would read better here but needs lib es2024; this
  // package inherits lib es2022 from tsconfig.base.json.
  let resolveExit!: (value: { code: number | null; signal: NodeJS.Signals | null }) => void;
  const exitPromise = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
    resolveExit = resolve;
    child.once('error', (error) => {
      appendStderr(`${error.message}\n`);
      state.settled = true;
      queue.stop();
      resolve({ code: 1, signal: null });
    });
    child.once(hooked === null ? 'exit' : 'close', (code: number | null, signal: NodeJS.Signals | null) => {
      state.settled = true;
      queue.stop();
      resolve({ code, signal });
    });
  });

  // Both bounds escalate through one path, so a run expires at most once and
  // the report always names which bound did it. A bare "timed out" would leave
  // the reader unable to tell a wedged toolchain from a loaded machine — the
  // two have opposite fixes.
  let termination: Promise<void> = Promise.resolve();
  const expire = (bound: ExpiredBound): void => {
    if (state.settled || state.expiry !== null) {
      return;
    }
    state.expiry = bound;
    clearTimeout(boundTimer);
    const elapsedMs = Date.now() - startedAt;
    appendStderr(
      bound.kind === 'idle'
        ? `\nCommand made no progress: no test finished or reported progress for ${bound.limitMs}ms (idleTimeoutMs=${bound.limitMs}) after ${elapsedMs}ms of runtime; ${
            bound.tests.length > 0
              ? `still running: ${bound.tests.join(', ')}`
              : 'the runner named no test still running'
          } (cwd=${cwd}): ${command}\n${
            queue.unconfirmed === null
              ? ''
              : `The last mbx admission snapshot failed (${queue.unconfirmed}), so the wait it could not confirm earned no idle credit\n`
          }`
        : bound.kind === 'fixture'
          ? `\nFixture command timed out after ${elapsedMs}ms (fixtureTimeoutMs=${bound.limitMs}, cwd=${cwd}): ${segment ?? command}\n`
          : `\nCommand timed out after ${elapsedMs}ms (timeoutMs=${bound.limitMs}, cwd=${cwd}): ${command}\n`,
    );
    if (bound.kind === 'idle') {
      const observed = idleCapture?.result;
      state.lockWaits = observed?.kind === 'captured' ? observed.snapshot.lockWaits : [];
      state.cpuRising = observed?.kind === 'captured' ? observed.snapshot.cpuRising : [];
      appendStderr(
        `Hang diagnostics: runner pid=${runner ?? 'unavailable'} pgid=${
          process.platform === 'win32' ? 'unavailable' : (child.pid ?? 'unavailable')
        } expiryAtMs=${Date.now()} scheduledExpiryAtMs=${idleCapture?.expiresAtMs ?? 'unavailable'}\n`,
      );
      if (observed?.kind === 'captured') {
        appendStderr(
          `Hang diagnostics: outcome=${observed.snapshot.outcome} capturedAtMs=${observed.snapshot.capturedAtMs} actualExpiryOffsetMs=${
            Date.now() - observed.snapshot.capturedAtMs
          }\n${observed.snapshot.text}`,
        );
      } else {
        appendStderr(
          `Hang diagnostics: outcome=incomplete; ${
            observed?.kind === 'failed'
              ? `capture failed: ${observed.detail}`
              : `capture incomplete at expiry (startedAtMs=${observed?.startedAtMs ?? 'not started'})`
          }; no empty-holder conclusion\n`,
        );
      }
    }
    cancelCapture();
    termination = terminate();
  };

  // Whether the group still lists members. A probe that fails proves nothing about the group, so it is told, and the
  // group is taken as listed: a force-kill that was not needed costs nothing, and one that was is not skipped.
  const groupListed = (pid: number): boolean => {
    try {
      return processGroupHasMembers(pid);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      tell(`Probing process group ${pid} for members failed: ${reason}; taking it as still listing members\n`);
      return true;
    }
  };

  // A timed-out run ends with its whole process group, not with its leader.
  // The graceful signal reaches every member that exists when it is sent, but
  // a member that ignores SIGTERM, or one forked while the signal goes out
  // (macOS does not hold a fork back for a group signal in flight), outlives
  // a leader that exits on it. The grace ends when the leader exits or
  // killAfterMs passes, whichever comes first; whatever is left then is killed.
  // SIGKILL is no different: a member forked while it goes out survives it too,
  // so the force-kill is repeated while the group lists members. What the repeats
  // cannot end is said, and a failure of the probe or of a signal is shown.
  const forceKill = async (pid: number): Promise<void> => {
    try {
      state.sentSignal = true;
      if (await killProcessGroup(pid, DEADLINE_PATIENCE, (group) => killer.kill(group, 'SIGKILL'))) {
        tell(`${describeMembersLeft(pid)}\n`);
      }
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      tell(`Force-kill of process group ${pid} failed: ${reason}\n`);
    }
  };
  const terminate = async (): Promise<void> => {
    await killChildTree();
    if (!state.settled && killAfterMs > 0) {
      await settledWithin(exitPromise, killAfterMs);
    }
    if (state.settled) {
      if (child.pid !== undefined && groupListed(child.pid)) {
        state.forceKillNeeded = true;
        appendStderr(
          `Force-killing what the ${state.expiry === null ? 'command' : 'timed-out command'} left in its process group: ${command}\n`,
        );
        await forceKill(child.pid);
      }
      return;
    }
    state.forceKillNeeded = true;
    appendStderr(
      `Force-killing ${state.expiry === null ? 'command' : 'timed out command'} after killAfterMs=${killAfterMs}: ${command}\n`,
    );
    if (child.pid !== undefined) await forceKill(child.pid);
    // The repeats wait for the group to empty, the leader with it. Windows has no process groups, so there they have
    // nothing to wait on once the one signal is sent, and the leader's exit is awaited as it always was.
    if (process.platform === 'win32' || child.pid === undefined) {
      await settledWithin(exitPromise, REAP_AFTER_FORCE_KILL_MS);
    }
    if (!state.settled) {
      resolveExit({ code: 1, signal: 'SIGKILL' });
    }
  };

  // The fixture's ceiling lives and dies with its segment: the segment's terminal clears it, so it can never
  // fire on a command the fixture already handed over to.
  stepTo({
    kind: 'spawn',
    at: Date.now(),
    testing: isTesting(),
    fixture: fixture !== null && lifecycle !== null,
    fixtureFirst: counted && lifecycle !== null,
    queued,
  });
  if (leaf !== null) {
    const channel = child.stdio.at(MBX_GRANT_FD);
    if (!(channel instanceof Duplex)) throw new Error('an mbx leaf requires its mbx descriptor');
    const failGrant = (detail: string): void => {
      if (lifecycleFailure !== null || state.expiry !== null || state.settled) return;
      lifecycleFailure ??= `mbx grant descriptor: ${detail}`;
      appendStderr(`bounded-exec: ${lifecycleFailure}\n`);
      termination = terminate();
    };
    channel.on('error', (error) => failGrant(error.message));
    if (reserved !== null) {
      // The shell asks for its cores in one line and reads the count granted (mbx-pool.ts, `reservation`). A refusal
      // ends the descriptor unanswered: the shell's read fails, and nothing runs.
      const { leaf: reservedLeaf, endpoint } = reserved;
      let asked = '';
      channel.setEncoding('utf8');
      channel.on('data', (text: string) => {
        if (reservation !== null) {
          failGrant(`the command asked for cores a second time: ${JSON.stringify(text)}`);
          return;
        }
        asked += text;
        const end = asked.indexOf('\n');
        if (end === -1) return;
        const line = asked.slice(0, end);
        const cpus = askedCores(line, reservedLeaf.demand.want);
        if (cpus === null) {
          failGrant(`the command asked for ${JSON.stringify(line)} cores, not 1 to ${reservedLeaf.demand.want}`);
          channel.end();
          return;
        }
        queue.waiting();
        tell(`cowshed: mbx reserve wait start cpus=${cpus} priority=${reservedLeaf.priority}\n`);
        const held = reserveCores(endpoint, cpus, reservedLeaf.priority, context.leafClass);
        reservation = held;
        void held.granted.then((granted) => {
          if (!granted.ok) {
            failGrant(`mbx did not grant the reservation: ${granted.error}`);
            channel.end();
            return;
          }
          // A run that ended while it waited gives the grant back with its CPU (`releaseCpu`).
          if (state.settled || state.expiry !== null) return;
          queue.admitted = true;
          tell(`cowshed: mbx reserve granted elapsed=${Date.now() - startedAt}ms priority=${reservedLeaf.priority}\n`);
          channel.end(`${granted.cpus}\n`);
          stepTo({ kind: 'granted', at: Date.now(), testing: isTesting(), fixture: !fixtureDone });
          void granted.lost.then((why) => {
            if (state.settled || cpuReleased !== null) return;
            appendStderr(
              `cowshed: mbx reservation lost while the command ran (${why}); it runs on unheld until it ends\n`,
            );
          });
        });
      });
    } else {
      // Read every byte after spawn accounting: a chunk can contain several grants and releases. EOF is neither.
      let said = false;
      channel.on('data', (chunk: Buffer) => {
        for (const byte of chunk) {
          if (byte === 103) {
            if (!said) {
              queue.admitted = true;
              said = true;
              tell(`cowshed: mbx cargo granted elapsed=${Date.now() - startedAt}ms\n`);
            }
            stepTo({ kind: 'granted', at: Date.now(), testing: isTesting(), fixture: !fixtureDone });
          } else if (byte === 114) {
            stepTo({ kind: 'released', at: Date.now() });
          } else {
            failGrant(`unexpected byte ${byte}`);
            return;
          }
        }
      });
      // Only mbx's activity report says whether a Cargo leaf's compiler or test waits in the pool.
      queue.poll();
    }
  }

  // A grant's connection is a ledger entry, not the command's owner: the lending parent's exit ends it, and what runs
  // under it runs on over budget until it finishes. Its close, while the command runs, is said and changes nothing
  // else; only a bound or a signal stops the command. Our own release after the command settled is not a loss;
  // `cpuReleased` marks it.
  if (grant !== null) {
    const held = grant;
    void held.closed.then(() => {
      if (state.settled || cpuReleased !== null) {
        return;
      }
      appendStderr(
        `cowshed: cpu-pool grant ended: the parent pool closed its ${held.tokens}-token grant while the command ran; it runs on over budget until it ends: ${command}\n`,
      );
    });
  }

  const { code: exitCode, signal: exitSignal } = await exitPromise;
  if (signalCleanup !== null) {
    await signalCleanup;
    return { success: false, terminalOutput: outputChunks.join('') };
  }
  queue.stop();
  await queue.idle();
  await termination;
  clearTimeout(boundTimer);
  cancelCapture();
  removeSignalHandlers();
  // The runner is gone, and with it every test that could report on the run's progress channel. Its sub-pool closes
  // first, so no descendant still draws on tokens the grant returns; then the grant goes back to its parent or the host.
  await closeProgress();
  await releaseCpu();

  const code = exitCode ?? signalToExitCode(exitSignal);
  if (code !== 0 && state.expiry === null) {
    // No bound of this executor ended it, so a signal it died of came from outside (the kernel's OOM killer sends
    // SIGKILL): the signal is named, never folded into a bare status.
    const ended =
      exitCode === null && exitSignal !== null
        ? `was killed by ${exitSignal} (status ${code})`
        : `exited with status ${code}`;
    appendStderr(
      fixture !== null && !mainStarted
        ? `Fixture command ${ended}: ${segment ?? fixture.command}\n`
        : `Command ${ended}: ${command}\n`,
    );
  }

  if (state.expiry !== null && !state.forceKillNeeded) {
    appendStderr(`Timed out command exited after graceful termination: ${command}\n`);
  }

  // ENOSPC said in the output names the writer's volume as it stood then. Without it, a volume still (nearly) full now
  // is the likely cause, and the volume is named.
  const seen = state.noSpace === null ? null : await state.noSpace;
  if (seen !== null && (code !== 0 || state.expiry !== null)) {
    const writer = context.record?.task.id;
    appendStderr(`${writer === undefined ? '' : `${writer}: `}${describeNoSpace(seen)}\n`);
  } else {
    const full = code !== 0 && lease !== null ? await tempVolume?.fullness(lease) : null;
    if (full) {
      appendStderr(`${describeRamTempError(full)}\n`);
    }
  }
  await releaseLease(code !== 0 || state.expiry !== null || state.poolCloseError !== null || lifecycleFailure !== null);
  releaseReservation();

  // The command's peak, as its waiter reported it once both ended; a peak that was not measured is said, never zero.
  const reported: LeafPeak | null =
    waiterReport === null ? (waiter?.kind === 'unmeasured' ? waiter : null) : await waiterReport.peak;
  if (reported?.kind === 'unmeasured') {
    appendStderr(`bounded-exec: this run's memory peak was not measured (${reported.reason}); its record names none\n`);
  }
  const peakBytes = reported?.kind === 'measured' ? reported.bytes : null;

  // Broken resource ownership, a lost native lifecycle, or leftovers that could not be retired fail even if a shell
  // branch later exited 0: its verdict is an ordinary operational failure, never a successful or silently unbounded run.
  const operationalFailure = state.poolCloseError !== null || lifecycleFailure !== null || state.retireError !== null;
  await recordRun(
    code,
    state.expiry,
    Date.now() - startedAt,
    operationalFailure,
    seen,
    state.lockWaits,
    state.cpuRising,
    peakBytes,
  );
  const killedForMemory = killedFromOutside({
    sentSignal: state.sentSignal,
    bounded: state.expiry !== null,
    exitSignal,
    code,
  });
  return {
    success: !operationalFailure && resultFailure === null && state.expiry === null && code === 0,
    terminalOutput: outputChunks.join(''),
    ...(killedForMemory ? { killedForMemory: true } : {}),
  };
}

export function createProcessTreeKiller(): ProcessTreeKiller {
  return {
    async kill(pid, signal) {
      signalProcessGroup(pid, signal);
    },
  };
}

function resolveCwd(cwd: string | undefined, root: string): string {
  if (!cwd) {
    return root;
  }
  return isAbsolute(cwd) ? cwd : join(root, cwd);
}

/** Owned scratch is the command's TMPDIR unless the target names one itself. */
function mergeEnv(directory: string | null, env: Record<string, string>): NodeJS.ProcessEnv {
  const inherited = withoutPoolEnv(process.env);
  return directory === null ? { ...inherited, ...env } : { ...inherited, TMPDIR: directory, ...env };
}

/** The process's environment without the CPU pool's descriptor: a pool and its count only a grant may give. */
function withoutPoolEnv(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const kept = { ...env };
  for (const name of POOL_ENV) {
    delete kept[name];
  }
  return kept;
}

/**
 * The target's environment without the CPU budget's variables: the budget sets those for the command it grants,
 * so a value the target names is ignored, and `say` tells the reader so.
 */
function targetEnv(env: Record<string, string> | undefined, say: (text: string) => void): Record<string, string> {
  const kept = { ...env };
  for (const name of BUDGET_ENV) {
    if (name in kept) {
      say(`cowshed: ${name} in the target's env is ignored: the CPU budget sets it\n`);
      delete kept[name];
    }
  }
  return kept;
}

/** The status a shell gives a command a signal ended: 128 plus the signal's number (SIGKILL 137, SIGTERM 143). */
function signalToExitCode(signal: NodeJS.Signals | null): number {
  return signal === null ? 1 : 128 + osConstants.signals[signal];
}

/** Resolves when `promise` settles or `ms` passes, whichever is first, leaving no timer behind. */
async function settledWithin(promise: Promise<unknown>, ms: number): Promise<void> {
  let timer: NodeJS.Timeout | undefined;
  try {
    await Promise.race([promise, new Promise<void>((resolve) => (timer = setTimeout(resolve, ms)))]);
  } finally {
    clearTimeout(timer);
  }
}
