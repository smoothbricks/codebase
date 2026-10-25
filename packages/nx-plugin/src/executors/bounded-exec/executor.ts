import { type ChildProcess, spawn } from 'node:child_process';
import { accessSync, constants } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { availableParallelism, userInfo } from 'node:os';
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

import {
  CPU_POOL_ENV,
  type CpuPool,
  createCpuPool,
  type HostWaits,
  type InheritedCpuPool,
  readInheritedCpuPool,
  takeParentCpuTokens,
} from './cpu-pool.js';
import {
  askAcrossInstall,
  CPU_TOKENS_ENV,
  type CpuBudget,
  gatewayCpuBudget,
  type InstallRetry,
  type RetryClock,
  runnerDemand,
  sizeRunner,
  type TokenGrant,
  wallClock,
} from './cpu-tokens.js';
import { GATEWAY_SOCKET, type InstallRestart } from './gateway-lease.js';
import type { HangDiagnostics } from './hang-diagnostics.js';
import { captureHangDiagnostics, HANG_CAPTURE_MS } from './hang-diagnostics.js';
import type { LockWait } from './lock-wait.js';
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
import { createRunProgress, withProgressReport } from './progress.js';
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
import { createRunnerLifecycle, withRunnerLifecycle } from './runner-lifecycle.js';
import { checkRuntimePin, describeRuntimePinRefusal } from './runtime-pin.js';
import type { BoundedExecOptions } from './schema.js';
import {
  type BoundedExecRecord,
  type BoundedExecTask,
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
const EXIT_CODE_BY_SIGNAL: Partial<Record<NodeJS.Signals, number>> = {
  SIGHUP: 129,
  SIGINT: 130,
  SIGTERM: 143,
};

/** The pool descriptor: only a grant may give it, so neither the target's nor the inherited environment passes it on. */
const POOL_ENV = [CPU_TOKENS_ENV, CPU_POOL_ENV];
/**
 * The variables the CPU budget sets for the command it grants: no target's environment may set them. A jobserver the
 * executor itself inherits, a parent make's, still reaches an ungranted command by Cargo's own rule; a granted one's
 * sub-pool names its own fifo over it.
 */
const BUDGET_ENV = [...POOL_ENV, 'CARGO_MAKEFLAGS', 'MAKEFLAGS', 'MFLAGS'];

export interface BoundedExecContext {
  root: string;
  /** The Nx task this run executes as, and the directory its verdict and report go to. */
  record?: { task: BoundedExecTask; directory: string };
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

export interface BoundedExecResult {
  success: boolean;
  terminalOutput: string;
}

interface RunState {
  settled: boolean;
  expiry: ExpiredBound | null;
  forceKillNeeded: boolean;
  /** The sub-pool's close error when it did not close cleanly: the run fails. */
  poolCloseError: string | null;
  /** Why the command's subtree could not be retired, when the supervisor's client failed: the run fails. */
  retireError: string | null;
  /** The first output line that said ENOSPC, with the task's volume read when it arrived. */
  noSpace: Promise<NoSpaceSighting> | null;
  /** The lock waits the expiry's snapshot named: empty unless that capture completed. Judged with the expiry. */
  lockWaits: readonly LockWait[];
}

type GrantedTokens = Extract<TokenGrant, { granted: true }>;

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
 * Runs with the normal process, RAM lease and CPU-budget owners, with an optional stdout sink, on the user's RAM temp
 * volume where there is one, through the host's own gateway: the Nx executor's runs, never a test's.
 */
export function runDefaultBoundedExec(
  options: BoundedExecOptions,
  context: BoundedExecContext,
): Promise<BoundedExecResult> {
  // Only macOS has `hdiutil ram://`; elsewhere the command keeps the inherited TMPDIR.
  return runBoundedExecWith(
    options,
    context,
    (cowshed) =>
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
    GATEWAY_SOCKET,
  );
}

/**
 * The default runner with every owner but its RAM volume and the gateway its CPU budget asks, which its caller names:
 * `volume` makes the volume for the cowshed the run's disk commands go through, and `gatewaySocket` is the gateway the
 * run takes its CPU tokens from. The Nx executor names the host's own; a host proof passes a fixture volume
 * (`fixtureRamVolume`) and the control socket of its fixture gateway, so no test leases, creates or detaches the user's
 * volume, or takes tokens from the user's gateway.
 */
export function runBoundedExecWith(
  options: BoundedExecOptions,
  context: BoundedExecContext,
  volume: (cowshed: string | undefined) => TempVolume | null,
  gatewaySocket: string,
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
  return runBoundedExec(
    run,
    context,
    createProcessTreeKiller(),
    tempVolume,
    gatewayCpuBudget(gatewaySocket),
    inherited,
    captureHangDiagnostics,
    wallClock,
    owner,
  );
}

/**
 * Nx gives an executor the task's project, target and configuration, not its id; the id is the
 * key of the task graph entry they name. Outside a task graph the run records nothing.
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
  return {
    root,
    record: {
      task: { id, hash: task.hash ?? null },
      directory: taskDirectory(workspaceDataDirectoryForWorkspace(root), id),
    },
  };
}

/**
 * The host budget's grant, asked across an install that restarts the gateway (05_gateway.md, "A queued CPU ask
 * outlives an install").
 * `parentCut` is the install that cut off a parent pool's queued ask, so the run asks the host at once, with no ask
 * of its own first. Otherwise the run's own ask is the one cut off. Either way the ask is retried once against the
 * gateway the install starts, and `say` names the install that cut it off.
 */
async function takeHostAcrossInstall(
  budget: CpuBudget,
  want: number,
  checkout: string,
  command: string,
  parentCut: InstallRestart | null,
  clock: RetryClock,
  say: (message: string) => void,
): Promise<InstallRetry> {
  const ask = () => budget.take(want, checkout, command);
  let source: string;
  let install: InstallRestart;
  if (parentCut !== null) {
    source = 'parent pool';
    install = parentCut;
  } else {
    const first = await ask();
    if (first.granted || first.cause !== 'install-restart') {
      return first;
    }
    source = 'gateway';
    install = first.install;
  }
  // A pool child's pool ended with the old gateway: it asks the host as background work, and says so.
  const asked = source === 'parent pool' ? " as background work, not at its pool's class" : '';
  say(
    `cowshed: cpu-tokens ${source} cut off by install ${install.pid} (${install.binary}) restarting the gateway; asking the gateway it starts once${asked}, within ${install.servesWithinMs} ms\n`,
  );
  return askAcrossInstall(ask, install, clock);
}

/**
 * Run the command once its runner holds CPU tokens from `budget` (`cpu-tokens.ts`), sized to the
 * grant; `null`, or a budget nobody serves, runs it as asked. Both bounds start once it is
 * granted: a wait for the host is not the command's time.
 *
 * `inherited` is the parent's CPU pool (`cpu-pool.ts`) that lends this run its tokens. The run asks that pool first
 * and the host budget only when the pool refuses. Whatever it is granted, the run serves its own sub-pool to its
 * descendants, closed before the grant returns. A grant whose connection closes while the command runs (a gateway
 * restart, or the lending parent's exit) ends its accounting, never the command: the command runs on over budget to
 * its own exit, and the run says so. An install that restarts the gateway while the ask waits is asked once more
 * against the gateway it starts; if none serves within the install's bound, the run is refused.
 */
export async function runBoundedExec(
  options: BoundedExecOptions,
  context: BoundedExecContext,
  killer: ProcessTreeKiller,
  tempVolume: TempVolume | null,
  budget: CpuBudget | null,
  inherited: InheritedCpuPool | null = null,
  captureDiagnostics: typeof captureHangDiagnostics = captureHangDiagnostics,
  retryClock: RetryClock = wallClock,
  owner: SubtreeOwner | null = null,
): Promise<BoundedExecResult> {
  const cwd = resolveCwd(options.cwd, context.root);
  const directory = context.record?.directory ?? null;
  // A previous run's verdict and report go first, so neither can be read as this run's.
  if (directory !== null) {
    await clearTaskDirectory(directory);
  }
  const requested = buildCommand(options);
  const timeoutMs = options.timeoutMs;
  const idleTimeoutMs = options.idleTimeoutMs;
  const killAfterMs = options.killAfterMs ?? DEFAULT_KILL_AFTER_MS;
  const outputChunks: string[] = [];
  const state: RunState = {
    settled: false,
    expiry: null,
    forceKillNeeded: false,
    poolCloseError: null,
    retireError: null,
    noSpace: null,
    lockWaits: [],
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

  // A leg that runs bun runs the Bun its workspace declares, or it does not run. This comes before the RAM lease and the
  // CPU tokens: a refusal that waited in the CPU queue first would say nothing for minutes.
  const runtime = await checkRuntimePin({
    root: context.root,
    command: requested,
    env: { ...withoutPoolEnv(process.env), ...options.env },
    cwd,
    pathSource: options.env?.PATH === undefined ? 'the inherited environment' : "the target's env",
  });
  if (runtime !== null) {
    appendStderr(describeRuntimePinRefusal(runtime, context.record?.task.id));
    if (context.record !== undefined) {
      const { task, directory: recordDirectory } = context.record;
      await writeRecord(recordDirectory, { task: task.id, hash: task.hash, verdict: judgeOperationalFailure(1, null) });
    }
    return { success: false, terminalOutput: outputChunks.join('') };
  }

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

  // The wait is one span line, as cowshed's own spans read. On a host without a gateway the Nx run serves its own
  // pool (`run-cpu-pool.ts`), taken below as a parent's; a command no pool and no gateway answers runs exactly as
  // asked and nothing is said.
  const cores = availableParallelism();
  const demand = runnerDemand(requested, options.parallelism, cores, process.env, options.env);
  const asked = Date.now();
  // A run a parent's CPU pool serves takes its tokens there. Only an absent pool, or one an install's restart cut off,
  // leaves the host budget to ask. A pool that answered and refused is a live budget of its own -- a lent grant, or a
  // gate's or a resident's relay to the host -- so asking the host beside it would count the run twice; the run fails
  // instead.
  let grant: GrantedTokens | null = null;
  let parentRefusal: string | null = null;
  // The parent's gateway is gone with the install's restart, so its run asks the host across the restart at once.
  let parentCut: { install: InstallRestart; reason: string } | null = null;
  if (inherited !== null) {
    const fromParent = await takeParentCpuTokens(inherited, demand.want);
    if (fromParent.granted) {
      grant = fromParent;
    } else if (fromParent.cause === 'install-restart') {
      parentCut = { install: fromParent.install, reason: fromParent.reason };
    } else if (fromParent.cause === 'absent') {
      appendStderr(
        `cowshed: cpu-tokens parent pool absent: ${fromParent.reason}; asking the host budget as background work, not at the pool's class\n`,
      );
    } else {
      parentRefusal = fromParent.reason;
    }
  }
  let unbudgeted: string | null = null;
  let restartRefusal: string | null = null;
  if (grant === null && parentRefusal === null && budget !== null) {
    const fromHost = await takeHostAcrossInstall(
      budget,
      demand.want,
      context.root,
      requested,
      parentCut?.install ?? null,
      retryClock,
      appendStderr,
    );
    if (fromHost.granted) {
      grant = fromHost;
    } else if (fromHost.cause === 'install-deadline') {
      restartRefusal = fromHost.reason;
    } else if (fromHost.cause !== 'absent') {
      unbudgeted = fromHost.reason;
    }
  } else if (parentCut !== null) {
    parentRefusal = parentCut.reason;
  }
  const waitedMs = Date.now() - asked;
  let cpu: BoundedExecRecord['cpu'];
  let sized: { command: string; env: Record<string, string> } = { command: requested, env: {} };
  if (grant !== null) {
    sized = sizeRunner(demand, requested, grant.tokens);
    cpu = { tokens: grant.tokens, want: demand.want, waitedMs, ...(grant.inherited ? { inherited: true } : {}) };
    appendStderr(
      `cowshed: cpu-tokens wait done elapsed=${waitedMs}ms status=ok ${grant.inherited ? 'inherited=true ' : ''}tokens=${grant.tokens}/${demand.want} runner=${demand.kind}\n`,
    );
  } else if (unbudgeted !== null) {
    appendStderr(
      `cowshed: cpu-tokens wait done elapsed=${waitedMs}ms status=err runner=${demand.kind} runs unbudgeted: ${unbudgeted}\n`,
    );
  }
  // The run's sub-pool and grant are given back exactly once, however the run ends. The sub-pool closes first, so no
  // descendant still draws on tokens the grant has returned.
  let pool: CpuPool | null = null;
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
  // Records the run's verdict when it has a task to record as. An `expiry` is set only when a bound ended the run.
  // An `operational` run failed on its CPU budget (a sub-pool that did not close): an ordinary failure, whatever
  // bound also fired, so it is never judged as a bound.
  const recordRun = async (
    exitCode: number,
    expiry: ExpiredBound | null,
    elapsedMs: number,
    operational: boolean,
    noSpace: NoSpaceSighting | null,
    lockWaits: readonly LockWait[],
  ): Promise<void> => {
    if (context.record === undefined) {
      return;
    }
    const { task, directory: recordDirectory } = context.record;
    const report = await readJunitReport(recordDirectory);
    const verdict = operational
      ? judgeOperationalFailure(exitCode, report)
      : judgeRun({ exitCode, elapsedMs, expiry, report, noSpace, lockWaits });
    if (verdict.outcome === 'bound' && verdict.bound === 'test') {
      appendStderr(`Every failing test failed only on its per-test timeout: ${verdict.tests.join(', ')}\n`);
    }
    await writeRecord(recordDirectory, { task: task.id, hash: task.hash, verdict, ...(cpu ? { cpu } : {}) });
  };
  // A run refused before its command starts says why, gives back its grant and RAM, and records the failure.
  const refuse = async (message: string): Promise<BoundedExecResult> => {
    appendStderr(message);
    await releaseCpu();
    // The command never started, so its lease holds nothing of its own to keep.
    await releaseLease(false);
    await recordRun(1, null, Date.now() - asked, false, null, []);
    return { success: false, terminalOutput: outputChunks.join('') };
  };
  if (parentRefusal !== null) {
    return refuse(
      `cowshed: cpu-tokens parent pool refused: ${parentRefusal}; refusing to run the command without asking the host budget\n`,
    );
  }

  if (restartRefusal !== null) {
    return refuse(`cowshed: cpu-tokens ${restartRefusal}; refusing to run the command\n`);
  }

  // Descendant work queued for host CPU tokens, as its pool hears it (`host-wait`): while any is held the idle clock
  // is stopped, because a queued job is not an idle one (specs/cowshed/05_gateway.md, "The bounds start at the
  // grant"). The clock it stops is made with the command below; a wait held before then is counted and honoured there.
  let heldWaits = 0;
  let idleClock: { stop(): void; restart(): void } | null = null;
  const hostWaits: HostWaits = {
    begin: () => {
      heldWaits += 1;
      if (heldWaits === 1) idleClock?.stop();
    },
    end: () => {
      heldWaits -= 1;
      if (heldWaits === 0) idleClock?.restart();
    },
  };

  // A granted run serves a sub-pool of its own grant, so its descendants draw from that and never from above it.
  // A pool that cannot be set up refuses the run: no command runs unbudgeted.
  if (grant !== null) {
    const created = await createCpuPool(grant.tokens, grant.closed, hostWaits);
    if (!created.ok) {
      return refuse(`cowshed: cpu pool unavailable: ${created.error}; refusing to run the command\n`);
    }
    pool = created.pool;
  }
  const command =
    directory === null
      ? withProgressReport(sized.command)
      : await withJunitReport(withProgressReport(sized.command), directory);
  // A runner bounded-exec cannot see in the command (a script that spawns `bun test`) writes its JUnit report to this
  // path when it is set. The grant's sizing outranks the target's own environment: the runner may use what it was
  // granted, not what it was configured with. The sub-pool's environment comes last: a descendant reads only the pool
  // this run serves.
  const env: Record<string, string> = {
    ...(directory === null ? {} : { [JUNIT_REPORT_ENV]: join(directory, JUNIT_FILE) }),
    ...targetEnv(options.env, appendStderr),
    ...sized.env,
    ...(pool?.environment ?? {}),
  };
  const hooked = process.platform === 'win32' ? null : withRunnerLifecycle(command);
  const lifecycle = hooked === null ? null : createRunnerLifecycle(hooked.runners);
  let lifecycleFailure: string | null = null;
  const isTesting = (): boolean => lifecycleFailure !== null || (lifecycle?.isTesting() ?? true);
  const startedAt = Date.now();
  // With a supervisor, `cowshed subtree-root` asks it for a handle on itself, writes the answer to the handle
  // descriptor, and execs `/bin/sh -c command` in the same process: the command's pid, group and life are unchanged.
  const shellCommand = hooked?.command ?? command;
  const root = owner?.rootArgv(shellCommand);
  let child: ChildProcess;
  try {
    // fd 3 is the runner lifecycle's duplex socket when the command is hooked; the subtree admission takes fd 4, which
    // `cowshed subtree-root` closes before it execs, so fd 3 reaches the shell unchanged.
    child =
      root === undefined
        ? spawn(shellCommand, [], {
            cwd,
            env: mergeEnv(lease?.directory ?? ordinaryTemp, env),
            shell: true,
            stdio: hooked === null ? 'pipe' : ['pipe', 'pipe', 'pipe', 'pipe'],
            detached: process.platform !== 'win32',
            windowsHide: true,
          })
        : spawn(root.file, root.args, {
            cwd,
            env: mergeEnv(lease?.directory ?? ordinaryTemp, env),
            stdio: ['pipe', 'pipe', 'pipe', hooked === null ? 'ignore' : 'pipe', 'pipe'],
            detached: process.platform !== 'win32',
            windowsHide: true,
          });
  } catch (error) {
    // Nothing started, so nothing runs under the grant: it goes back here, with the RAM lease.
    return refuse(`cowshed: could not start the command: ${error instanceof Error ? error.message : String(error)}\n`);
  }
  subtreeRetired = false;
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

  // Progress is a finished test, not output: a runner's SLOW warnings and a hung test's heartbeat keep printing.
  const progress = createRunProgress(command);
  // A write that found no space is said once, in the output, by whatever made it; the volume is read when it is said,
  // since the writer usually removes what it half-wrote before it exits and the volume no longer looks full by then.
  const noSpace = createNoSpaceWatch();
  const sight = async (line: string): Promise<NoSpaceSighting> => {
    const held = lease;
    return { line, volume: held === null ? null : ((await tempVolume?.space(held)) ?? null) };
  };
  let idleTimer: NodeJS.Timeout | undefined;
  let captureTimer: NodeJS.Timeout | undefined;
  let idleCapture: { expiresAtMs: number; controller: AbortController; result: IdleDiagnosticResult } | undefined;
  const cancelCapture = (): void => {
    clearTimeout(captureTimer);
    idleCapture?.controller.abort();
  };
  // What is left of the current no-progress interval. A finished test sets it back to `idleTimeoutMs`; a held host
  // wait keeps it unspent: the clock is stopped while descendant work is queued for CPU tokens, and runs on from where
  // it stopped once the last wait ends. `queuedMs` is the queued time this interval has not counted, for the report.
  let idleLeftMs = idleTimeoutMs ?? 0;
  let queuedMs = 0;
  let queuedSinceMs: number | null = null;
  const scheduleIdle = (limitMs: number, leftMs: number): void => {
    clearTimeout(idleTimer);
    cancelCapture();
    const capture: NonNullable<typeof idleCapture> = {
      expiresAtMs: Date.now() + leftMs,
      controller: new AbortController(),
      result: { kind: 'pending', startedAtMs: null },
    };
    idleCapture = capture;
    captureTimer = setTimeout(
      () => {
        if (state.settled || state.expiry !== null || idleCapture !== capture) return;
        const pid = child.pid;
        if (pid === undefined) {
          idleCapture.result = { kind: 'failed', detail: 'runner PID unavailable; no holder conclusion' };
          return;
        }
        idleCapture.result = { kind: 'pending', startedAtMs: Date.now() };
        void Promise.resolve()
          .then(() =>
            captureDiagnostics({
              pid,
              pgid: process.platform === 'win32' ? null : pid,
              expiresAtMs: capture.expiresAtMs,
              deadlineMs: Math.max(0, Math.min(HANG_CAPTURE_MS, capture.expiresAtMs - Date.now())),
              signal: capture.controller.signal,
            }),
          )
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
      Math.max(0, leftMs - HANG_CAPTURE_MS),
    );
    idleTimer = setTimeout(() => expire({ kind: 'idle', limitMs, tests: progress.inFlight() }, queuedMs), leftMs);
  };
  const armIdleTimer = (): void => {
    if (idleTimeoutMs === undefined || state.settled || state.expiry !== null || !isTesting()) {
      return;
    }
    idleLeftMs = idleTimeoutMs;
    queuedMs = 0;
    if (queuedSinceMs !== null) {
      queuedSinceMs = Date.now();
      return;
    }
    scheduleIdle(idleTimeoutMs, idleLeftMs);
  };
  idleClock = {
    stop: () => {
      if (idleTimeoutMs === undefined || state.settled || state.expiry !== null || queuedSinceMs !== null) {
        return;
      }
      const now = Date.now();
      queuedSinceMs = now;
      if (idleCapture !== undefined) {
        idleLeftMs = Math.max(0, idleCapture.expiresAtMs - now);
      }
      clearTimeout(idleTimer);
      cancelCapture();
      idleCapture = undefined;
      appendStderr(
        `bounded-exec: idle clock stopped: a descendant's work is queued for host CPU tokens; ${idleLeftMs}ms of idleTimeoutMs=${idleTimeoutMs} left\n`,
      );
    },
    restart: () => {
      if (idleTimeoutMs === undefined || state.settled || state.expiry !== null || queuedSinceMs === null) {
        return;
      }
      const waitedMs = Date.now() - queuedSinceMs;
      queuedMs += waitedMs;
      queuedSinceMs = null;
      if (!isTesting()) {
        appendStderr(`bounded-exec: host CPU wait ended after ${waitedMs}ms; no test runner is active\n`);
        return;
      }
      appendStderr(
        `bounded-exec: idle clock restarted: no descendant's work is queued for host CPU tokens after ${waitedMs}ms; ${idleLeftMs}ms of idleTimeoutMs=${idleTimeoutMs} left\n`,
      );
      scheduleIdle(idleTimeoutMs, idleLeftMs);
    },
  };

  // Only the CHILD's own output is read for progress. The diagnostics below go
  // through appendStderr as well, and re-arming on those would let a teardown
  // message extend the very bound that just fired.
  const updateIdle = (finished: boolean, wasTesting: boolean): void => {
    if (!isTesting()) {
      clearTimeout(idleTimer);
      cancelCapture();
      idleCapture = undefined;
    } else if (finished || !wasTesting) {
      armIdleTimer();
    }
  };
  const scanProgress = (stream: 'stdout' | 'stderr', text: string): void => {
    updateIdle(progress.scan(stream, text), isTesting());
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
    createInterface({ input: channel }).on('line', (line) => {
      const wasTesting = isTesting();
      const event = lifecycle.accept(line);
      if (event === null) {
        failLifecycle(lifecycle.error() ?? 'invalid lifecycle event', wasTesting);
        return;
      }
      updateIdle(false, wasTesting);
      // Tool startup and native lifecycle-bearing stdout wait for their phase acknowledgement.
      channel.write(`${JSON.stringify(event)}\n`);
    });
    channel.on('error', (error) => failLifecycle(error.message));
    channel.on('end', () => {
      if (!lifecycle.finished()) failLifecycle('supervisor channel ended without an owned terminal event');
    });
  }
  child.stdout?.on('data', (chunk: Buffer | string) => {
    const text = chunk.toString();
    scanProgress('stdout', text);
    const said = noSpace.scan('stdout', text);
    if (said !== null) state.noSpace = sight(said);
    appendStdout(text);
  });
  child.stderr?.on('data', (chunk: Buffer | string) => {
    const text = chunk.toString();
    scanProgress('stderr', text);
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
      await killer.kill(pid, 'SIGTERM');
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      tell(`Terminating process group ${pid} failed: ${reason}\n`);
    }
  };

  const onProcessExit = (): void => {
    void killChildTree();
  };
  const onTerminationSignal = (signal: NodeJS.Signals): void => {
    removeSignalHandlers();
    cancelCapture();
    void killChildTree()
      .finally(releaseCpu)
      .finally(() => releaseLease(true))
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
      resolve({ code: 1, signal: null });
    });
    child.once(hooked === null ? 'exit' : 'close', (code: number | null, signal: NodeJS.Signals | null) => {
      state.settled = true;
      resolve({ code, signal });
    });
  });

  // Both bounds escalate through one path, so a run expires at most once and
  // the report always names which bound did it. A bare "timed out" would leave
  // the reader unable to tell a wedged toolchain from a loaded machine — the
  // two have opposite fixes.
  let termination: Promise<void> = Promise.resolve();
  // `queuedMs` is the time the idle interval did not count because descendant work was queued for host CPU tokens.
  const expire = (bound: ExpiredBound, queuedMs = 0): void => {
    if (state.settled || state.expiry !== null) {
      return;
    }
    state.expiry = bound;
    clearTimeout(totalTimer);
    clearTimeout(idleTimer);
    const elapsedMs = Date.now() - startedAt;
    appendStderr(
      bound.kind === 'idle'
        ? `\nCommand made no progress: no test finished for ${bound.limitMs}ms (idleTimeoutMs=${bound.limitMs})${
            queuedMs > 0 ? `, not counting ${queuedMs}ms queued for host CPU tokens,` : ''
          } after ${elapsedMs}ms of runtime; ${
            bound.tests.length > 0
              ? `still running: ${bound.tests.join(', ')}`
              : 'the runner named no test still running'
          } (cwd=${cwd}): ${command}\n`
        : `\nCommand timed out after ${elapsedMs}ms (timeoutMs=${bound.limitMs}, cwd=${cwd}): ${command}\n`,
    );
    if (bound.kind === 'idle') {
      const observed = idleCapture?.result;
      state.lockWaits = observed?.kind === 'captured' ? observed.snapshot.lockWaits : [];
      appendStderr(
        `Hang diagnostics: runner pid=${child.pid ?? 'unavailable'} pgid=${
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

  const totalTimer =
    timeoutMs === undefined ? undefined : setTimeout(() => expire({ kind: 'total', limitMs: timeoutMs }), timeoutMs);
  armIdleTimer();
  // A wait its pool heard before the command started stops the clock it was not yet there to stop.
  if (heldWaits > 0) idleClock.stop();

  // A grant's connection is a ledger entry, not the command's owner (05_gateway.md "Host CPU budget": a gateway
  // restart forgets every grant, and what runs under it runs on over budget until it finishes). Its close, while the
  // command runs, is said and changes nothing else; only a bound or a signal stops the command. Our own release after
  // the command settled is not a loss; `cpuReleased` marks it.
  if (grant !== null) {
    const held = grant;
    void held.closed.then(() => {
      if (state.settled || cpuReleased !== null) {
        return;
      }
      appendStderr(
        `cowshed: cpu-tokens grant ended: the ${held.inherited ? 'parent pool' : 'gateway'} closed its ${held.tokens}-token grant while the command ran; it runs on over budget until it ends: ${command}\n`,
      );
    });
  }

  const { code: exitCode, signal: exitSignal } = await exitPromise;
  await termination;
  clearTimeout(totalTimer);
  clearTimeout(idleTimer);
  cancelCapture();
  removeSignalHandlers();
  // The runner is gone. Its sub-pool closes first, so no descendant still draws on tokens the grant returns;
  // then the grant goes back to its parent or the host.
  await releaseCpu();

  const code = exitCode ?? signalToExitCode(exitSignal);
  if (code !== 0 && state.expiry === null) {
    appendStderr(`Command exited with status ${code}: ${command}\n`);
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

  // Broken resource ownership, a lost native lifecycle, or leftovers that could not be retired fail even if a shell
  // branch later exited 0: its verdict is an ordinary operational failure, never a successful or silently unbounded run.
  const operationalFailure = state.poolCloseError !== null || lifecycleFailure !== null || state.retireError !== null;
  await recordRun(code, state.expiry, Date.now() - startedAt, operationalFailure, seen, state.lockWaits);
  return {
    success: !operationalFailure && state.expiry === null && code === 0,
    terminalOutput: outputChunks.join(''),
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

function buildCommand(options: BoundedExecOptions): string {
  const parts = [options.command];
  if (Array.isArray(options.args)) {
    parts.push(...options.args);
  } else if (options.args) {
    parts.push(options.args);
  }
  if (options.forwardAllArgs !== false && options.__unparsed__?.length) {
    parts.push(...options.__unparsed__);
  }
  return parts.join(' ');
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

function signalToExitCode(signal: NodeJS.Signals | null): number {
  if (!signal) {
    return 1;
  }
  return EXIT_CODE_BY_SIGNAL[signal] ?? 1;
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
