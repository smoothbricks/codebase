import { type ChildProcess, spawn } from 'node:child_process';
import { accessSync, constants } from 'node:fs';
import { availableParallelism, userInfo } from 'node:os';
import { delimiter, isAbsolute, join, resolve as resolvePath } from 'node:path';
import { workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory.js';

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
  CPU_TOKENS_ENV,
  type CpuBudget,
  gatewayCpuBudget,
  runnerDemand,
  sizeRunner,
  type TokenGrant,
} from './cpu-tokens.js';
import type { HangDiagnostics } from './hang-diagnostics.js';
import { captureHangDiagnostics, HANG_CAPTURE_MS } from './hang-diagnostics.js';
import { createNoSpaceWatch } from './no-space.js';
import { createRunProgress, withProgressReport } from './progress.js';
import {
  COWSHED_EXECUTABLE_ENV,
  createHostCommands,
  describeRamTempError,
  RAM_TEMP_CAPACITY_BYTES,
  type RamTempLease,
  RamTempVolume,
  ramTempPaths,
} from './ram-temp.js';
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

/** The variables the CPU budget sets for the command it grants: no target's environment may set them. */
const BUDGET_ENV = [CPU_TOKENS_ENV, CPU_POOL_ENV];

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
  /** The first output line that said ENOSPC, with the task's volume read when it arrived. */
  noSpace: Promise<NoSpaceSighting> | null;
}

type GrantedTokens = Extract<TokenGrant, { granted: true }>;

export interface ProcessTreeKiller {
  kill(pid: number, signal: NodeJS.Signals): Promise<void>;
}

/** Where the command's TMPDIR comes from; `null` leaves the inherited one. */
export type TempVolume = Pick<RamTempVolume, 'acquire' | 'release' | 'fullness' | 'space' | 'reap'>;

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

/** Runs with the normal process, RAM lease and CPU-budget owners, with an optional stdout sink. */
export function runDefaultBoundedExec(
  options: BoundedExecOptions,
  context: BoundedExecContext,
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
  // Only macOS has `hdiutil ram://`; elsewhere the command keeps the inherited TMPDIR.
  const tempVolume =
    process.platform === 'darwin'
      ? new RamTempVolume(
          ramTempPaths(userInfo()),
          RAM_TEMP_CAPACITY_BYTES,
          createHostCommands(undefined, undefined, undefined, cowshed),
        )
      : null;
  // A command a parent's CPU pool started draws its tokens from that pool. The pool is named by the environment the
  // parent set for this command; a run no parent started has none.
  const inherited = readInheritedCpuPool(process.env);
  return runBoundedExec(run, context, createProcessTreeKiller(), tempVolume, gatewayCpuBudget(), inherited);
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
 * Run the command once its runner holds CPU tokens from `budget` (`cpu-tokens.ts`), sized to the
 * grant; `null`, or a budget nobody serves, runs it as asked. Both bounds start once it is
 * granted: a wait for the host is not the command's time.
 *
 * `inherited` is the parent's CPU pool (`cpu-pool.ts`) that lends this run its tokens. The run asks that pool first
 * and the host budget only when the pool refuses. Whatever it is granted, the run serves its own sub-pool to its
 * descendants, closed before the grant returns. A grant whose connection closes while the command runs (a gateway
 * restart, or the lending parent's exit) ends its accounting, never the command: the command runs on over budget to
 * its own exit, and the run says so.
 */
export async function runBoundedExec(
  options: BoundedExecOptions,
  context: BoundedExecContext,
  killer: ProcessTreeKiller,
  tempVolume: TempVolume | null,
  budget: CpuBudget | null,
  inherited: InheritedCpuPool | null = null,
  captureDiagnostics: typeof captureHangDiagnostics = captureHangDiagnostics,
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
    noSpace: null,
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

  let lease: RamTempLease | null = null;
  if (tempVolume !== null) {
    const acquired = await tempVolume.acquire();
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
        appendStderr(`RAM temp volume: stopped ${reaped}, which a dead task left working in its lease\n`);
      }
    } else {
      appendStderr(
        `RAM temp volume unavailable in this sandbox (${acquired.value.detail}); TMPDIR stays ${process.env.TMPDIR ?? '(unset)'}\n`,
      );
    }
  }
  const releaseLease = async (): Promise<void> => {
    if (lease === null) {
      return;
    }
    const held = lease;
    lease = null;
    // What the command left working in its lease is a leak, whichever way the command ended; it
    // is stopped before the lease goes, so the volume can detach, and said, so its owner fixes it.
    const reaped = await tempVolume?.reap(held);
    if (reaped?.ok) {
      for (const leftover of reaped.value) {
        appendStderr(`RAM temp volume: stopped ${leftover}, which the command left working in its lease\n`);
      }
    } else if (reaped) {
      appendStderr(`${describeRamTempError(reaped.error)}\n`);
    }
    const released = await tempVolume?.release(held);
    if (released && !released.ok) {
      appendStderr(`${describeRamTempError(released.error)}\n`);
    }
  };

  // The wait is one span line, as cowshed's own spans read. On a host without a gateway the Nx run serves its own
  // pool (`run-cpu-pool.ts`), taken below as a parent's; a command no pool and no gateway answers runs exactly as
  // asked and nothing is said.
  const cores = availableParallelism();
  const demand = runnerDemand(requested, options.parallelism, cores, process.env, options.env);
  const asked = Date.now();
  // A run a parent's CPU pool serves takes its tokens there. Only an absent pool leaves the host budget to ask.
  // A pool that answered and refused is a live budget of its own -- a lent grant, or a gate's or a resident's
  // relay to the host -- so asking the host beside it would count the run twice; the run fails instead.
  let grant: GrantedTokens | null = null;
  let parentRefusal: string | null = null;
  if (inherited !== null) {
    const fromParent = await takeParentCpuTokens(inherited, demand.want);
    if (fromParent.granted) {
      grant = fromParent;
    } else if (fromParent.cause === 'absent') {
      appendStderr(`cowshed: cpu-tokens parent pool refused: ${fromParent.reason}; asking the host budget\n`);
    } else {
      parentRefusal = fromParent.reason;
    }
  }
  let unbudgeted: string | null = null;
  if (grant === null && parentRefusal === null && budget !== null) {
    const fromHost = await budget.take(demand.want, context.root, requested);
    if (fromHost.granted) {
      grant = fromHost;
    } else if (fromHost.cause !== 'absent') {
      unbudgeted = fromHost.reason;
    }
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
  ): Promise<void> => {
    if (context.record === undefined) {
      return;
    }
    const { task, directory: recordDirectory } = context.record;
    const report = await readJunitReport(recordDirectory);
    const verdict = operational
      ? judgeOperationalFailure(exitCode, report)
      : judgeRun({ exitCode, elapsedMs, expiry, report, noSpace });
    if (verdict.outcome === 'bound' && verdict.bound === 'test') {
      appendStderr(`Every failing test failed only on its per-test timeout: ${verdict.tests.join(', ')}\n`);
    }
    await writeRecord(recordDirectory, { task: task.id, hash: task.hash, verdict, ...(cpu ? { cpu } : {}) });
  };
  // A run refused before its command starts says why, gives back its grant and RAM, and records the failure.
  const refuse = async (message: string): Promise<BoundedExecResult> => {
    appendStderr(message);
    await releaseCpu();
    await releaseLease();
    await recordRun(1, null, Date.now() - asked, false, null);
    return { success: false, terminalOutput: outputChunks.join('') };
  };
  if (parentRefusal !== null) {
    return refuse(
      `cowshed: cpu-tokens parent pool refused: ${parentRefusal}; refusing to run the command without asking the host budget\n`,
    );
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
  const startedAt = Date.now();
  let child: ChildProcess;
  try {
    child = spawn(command, [], {
      cwd,
      env: mergeEnv(lease, env),
      shell: true,
      detached: process.platform !== 'win32',
      windowsHide: true,
    });
  } catch (error) {
    // Nothing started, so nothing runs under the grant: it goes back here, with the RAM lease.
    return refuse(`cowshed: could not start the command: ${error instanceof Error ? error.message : String(error)}\n`);
  }

  // Progress is a finished test, not output: a runner's SLOW warnings and a hung test's heartbeat keep printing.
  const progress = createRunProgress();
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
    if (idleTimeoutMs === undefined || state.settled || state.expiry !== null) {
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
      appendStderr(
        `bounded-exec: idle clock restarted: no descendant's work is queued for host CPU tokens after ${waitedMs}ms; ${idleLeftMs}ms of idleTimeoutMs=${idleTimeoutMs} left\n`,
      );
      scheduleIdle(idleTimeoutMs, idleLeftMs);
    },
  };

  // Only the CHILD's own output is read for progress. The diagnostics below go
  // through appendStderr as well, and re-arming on those would let a teardown
  // message extend the very bound that just fired.
  child.stdout?.on('data', (chunk: Buffer | string) => {
    const text = chunk.toString();
    if (progress.scan('stdout', text)) armIdleTimer();
    const said = noSpace.scan('stdout', text);
    if (said !== null) state.noSpace = sight(said);
    appendStdout(text);
  });
  child.stderr?.on('data', (chunk: Buffer | string) => {
    const text = chunk.toString();
    if (progress.scan('stderr', text)) armIdleTimer();
    const said = noSpace.scan('stderr', text);
    if (said !== null) state.noSpace = sight(said);
    appendStderr(text);
  });

  const killChildTree = async (force: boolean): Promise<void> => {
    const pid = child.pid;
    if (!pid) {
      return;
    }
    await killer.kill(pid, force ? 'SIGKILL' : 'SIGTERM');
  };

  const onProcessExit = (): void => {
    void killChildTree(false);
  };
  const onTerminationSignal = (signal: NodeJS.Signals): void => {
    removeSignalHandlers();
    cancelCapture();
    void killChildTree(false)
      .finally(releaseCpu)
      .finally(releaseLease)
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
    child.once('exit', (code, signal) => {
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

  // A timed-out run ends with its whole process group, not with its leader.
  // The graceful signal reaches every member that exists when it is sent, but
  // a member that ignores SIGTERM, or one forked while the signal goes out
  // (macOS does not hold a fork back for a group signal in flight), outlives
  // a leader that exits on it. The grace ends when the leader exits or
  // killAfterMs passes, whichever comes first; whatever is left then is killed.
  const terminate = async (): Promise<void> => {
    await ignoreKillError(killChildTree(false));
    if (!state.settled && killAfterMs > 0) {
      await settledWithin(exitPromise, killAfterMs);
    }
    if (state.settled) {
      if (child.pid !== undefined && processGroupHasMembers(child.pid)) {
        state.forceKillNeeded = true;
        appendStderr(
          `Force-killing what the ${state.expiry === null ? 'command' : 'timed-out command'} left in its process group: ${command}\n`,
        );
        await ignoreKillError(killChildTree(true));
      }
      return;
    }
    state.forceKillNeeded = true;
    appendStderr(
      `Force-killing ${state.expiry === null ? 'command' : 'timed out command'} after killAfterMs=${killAfterMs}: ${command}\n`,
    );
    await ignoreKillError(killChildTree(true));
    await settledWithin(exitPromise, REAP_AFTER_FORCE_KILL_MS);
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
  await releaseLease();

  // A run whose sub-pool did not close cleanly fails even when the command exited 0, and whatever bound also fired:
  // its verdict is an ordinary failure, never a bound.
  const operationalFailure = state.poolCloseError !== null;
  await recordRun(code, state.expiry, Date.now() - startedAt, operationalFailure, seen);

  return {
    success: !operationalFailure && state.expiry === null && code === 0,
    terminalOutput: outputChunks.join(''),
  };
}

export function createProcessTreeKiller(): ProcessTreeKiller {
  return {
    kill(pid, signal) {
      return killProcessGroup(pid, signal);
    },
  };
}

function killProcessGroup(pid: number, signal: NodeJS.Signals): Promise<void> {
  try {
    // On POSIX the executor spawns with detached: true, so the child leads a
    // process group of its own and -pid signals all of it at once, including
    // grandchildren that tree-walk libraries miss when parents die and
    // children get reparented to init. Only the group is named: the leader may
    // already be reaped, and its pid reused by an unrelated process, while the
    // group lives on in its other members. Windows has no process groups.
    process.kill(process.platform === 'win32' ? pid : -pid, signal);
  } catch {
    // ESRCH: the group (or the Windows process) has already exited.
  }
  return Promise.resolve();
}

/**
 * Whether any process is left in the group `pgid` leads. A group outlives its
 * reaped leader for as long as one member does, and no process can take its id
 * meanwhile. Windows has no process groups, so there is nothing left to name.
 */
function processGroupHasMembers(pgid: number): boolean {
  if (process.platform === 'win32') {
    return false;
  }
  try {
    process.kill(-pgid, 0);
    return true;
  } catch (error) {
    // EPERM: a member exists but runs as another user.
    return error instanceof Error && 'code' in error && error.code === 'EPERM';
  }
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

/** The lease's directory is the command's TMPDIR unless the target names one itself. */
function mergeEnv(lease: RamTempLease | null, env: Record<string, string>): NodeJS.ProcessEnv {
  const inherited = withoutBudgetEnv(process.env);
  return lease === null ? { ...inherited, ...env } : { ...inherited, TMPDIR: lease.directory, ...env };
}

/** The process's environment without the CPU budget's variables: a pool and its count only a grant may give. */
function withoutBudgetEnv(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const kept = { ...env };
  for (const name of BUDGET_ENV) {
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

async function ignoreKillError(promise: Promise<void>): Promise<void> {
  try {
    await promise;
  } catch {
    // The process tree may have already exited between timeout and kill.
  }
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
