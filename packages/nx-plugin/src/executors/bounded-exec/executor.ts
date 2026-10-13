import { type ChildProcess, spawn } from 'node:child_process';
import { availableParallelism, userInfo } from 'node:os';
import { isAbsolute, join, resolve as resolvePath } from 'node:path';
import { workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory.js';

import {
  CPU_POOL_ENV,
  type CpuPool,
  createCpuPool,
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
  type ExpiredBound,
  JUNIT_FILE,
  JUNIT_REPORT_ENV,
  judgeOperationalFailure,
  judgeRun,
  readJunitReport,
  taskDirectory,
  withJunitReport,
  writeRecord,
} from './verdict.js';

const DEFAULT_KILL_AFTER_MS = 10_000;
const REAP_AFTER_FORCE_KILL_MS = 2_000;
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
  /** Set when a borrowed grant closed while the command ran, and the command was stopped as a lost grant. */
  grantLost: boolean;
  /** The sub-pool's close error when it did not close cleanly: the run fails. */
  poolCloseError: string | null;
}

type GrantedTokens = Extract<TokenGrant, { granted: true }>;

export interface ProcessTreeKiller {
  kill(pid: number, signal: NodeJS.Signals): Promise<void>;
}

/** Where the command's TMPDIR comes from; `null` leaves the inherited one. */
export type TempVolume = Pick<RamTempVolume, 'acquire' | 'release' | 'fullness' | 'reap'>;

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
  const cowshed = named === undefined ? undefined : resolvePath(context.root, named);
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
 * descendants, closed before the grant returns. A borrowed grant that closes while the command runs stops the command
 * as a lost grant, which is a failure.
 */
export async function runBoundedExec(
  options: BoundedExecOptions,
  context: BoundedExecContext,
  killer: ProcessTreeKiller,
  tempVolume: TempVolume | null,
  budget: CpuBudget | null,
  inherited: InheritedCpuPool | null = null,
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
    grantLost: false,
    poolCloseError: null,
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

  // The wait is one span line, as cowshed's own spans read. With no gateway listening, as on any
  // host without cowshed, the command runs exactly as asked and nothing is said.
  const cores = availableParallelism();
  const demand = runnerDemand(requested, options.parallelism, cores);
  const asked = Date.now();
  // A run a parent's CPU pool lent tokens to takes them there. Only an absent pool leaves the host budget to ask.
  // A pool that answered and refused still holds tokens of its own: asking the host then would hold them while waiting
  // on another budget, so the run fails instead.
  let grant: GrantedTokens | null = null;
  let parentRefusal: string | null = null;
  if (inherited !== null) {
    const fromParent = await takeParentCpuTokens(inherited, demand.want, cores);
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
    sized = sizeRunner(demand.kind, requested, grant.tokens);
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
  // An `operational` run failed on its CPU budget (a lost grant, or a sub-pool that did not close): an ordinary
  // failure, whatever bound also fired, so it is never judged as a bound.
  const recordRun = async (
    exitCode: number,
    expiry: ExpiredBound | null,
    elapsedMs: number,
    operational: boolean,
  ): Promise<void> => {
    if (context.record === undefined) {
      return;
    }
    const { task, directory: recordDirectory } = context.record;
    const report = await readJunitReport(recordDirectory);
    const verdict = operational
      ? judgeOperationalFailure(exitCode, report)
      : judgeRun({ exitCode, elapsedMs, expiry, report });
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
    await recordRun(1, null, Date.now() - asked, false);
    return { success: false, terminalOutput: outputChunks.join('') };
  };
  if (parentRefusal !== null) {
    return refuse(
      `cowshed: cpu-tokens parent pool refused: ${parentRefusal}; refusing to run the command without asking the host budget\n`,
    );
  }

  // A granted run serves a sub-pool of its own grant, so its descendants draw from that and never from above it.
  // A pool that cannot be set up refuses the run: no command runs unbudgeted.
  if (grant !== null) {
    const created = await createCpuPool(grant.tokens, grant.closed);
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
  let idleTimer: NodeJS.Timeout | undefined;
  const armIdleTimer = (): void => {
    if (idleTimeoutMs === undefined || state.settled || state.expiry !== null) {
      return;
    }
    clearTimeout(idleTimer);
    idleTimer = setTimeout(
      () => expire({ kind: 'idle', limitMs: idleTimeoutMs, tests: progress.inFlight() }),
      idleTimeoutMs,
    );
  };

  // Only the CHILD's own output is read for progress. The diagnostics below go
  // through appendStderr as well, and re-arming on those would let a teardown
  // message extend the very bound that just fired.
  child.stdout?.on('data', (chunk: Buffer | string) => {
    const text = chunk.toString();
    if (progress.scan('stdout', text)) armIdleTimer();
    appendStdout(text);
  });
  child.stderr?.on('data', (chunk: Buffer | string) => {
    const text = chunk.toString();
    if (progress.scan('stderr', text)) armIdleTimer();
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
  const expire = (bound: ExpiredBound): void => {
    if (state.settled || state.expiry !== null || state.grantLost) {
      return;
    }
    state.expiry = bound;
    clearTimeout(totalTimer);
    clearTimeout(idleTimer);
    const elapsedMs = Date.now() - startedAt;
    appendStderr(
      bound.kind === 'idle'
        ? `\nCommand made no progress: no test finished for ${bound.limitMs}ms (idleTimeoutMs=${bound.limitMs}) after ${elapsedMs}ms of runtime; ${
            bound.tests.length > 0
              ? `still running: ${bound.tests.join(', ')}`
              : 'the runner named no test still running'
          } (cwd=${cwd}): ${command}\n`
        : `\nCommand timed out after ${elapsedMs}ms (timeoutMs=${bound.limitMs}, cwd=${cwd}): ${command}\n`,
    );
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

  // A borrowed grant ends with the ancestor that lent it, and the command runs on tokens that ancestor takes back
  // then. Stopping the command is a lost grant: a failure, not a bound. Our own release after the command settled
  // is not a loss; `cpuReleased` marks it.
  const onGrantLost = (): void => {
    if (state.settled || state.grantLost || cpuReleased !== null) {
      return;
    }
    state.grantLost = true;
    appendStderr(
      `cowshed: cpu-tokens grant lost: its parent pool closed while the command ran; stopping it: ${command}\n`,
    );
    // A bound may already be terminating the still-running command. The loss remains an ordinary
    // operational failure, but that existing termination owns the signals and grace.
    if (state.expiry === null) termination = terminate();
  };
  if (grant?.inherited) {
    void grant.closed.then(onGrantLost);
  }

  const { code: exitCode, signal: exitSignal } = await exitPromise;
  await termination;
  clearTimeout(totalTimer);
  clearTimeout(idleTimer);
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

  // A test that dies of ENOSPC rarely says where it was writing; the volume can.
  const full = code !== 0 && lease !== null ? await tempVolume?.fullness(lease) : null;
  if (full) {
    appendStderr(`${describeRamTempError(full)}\n`);
  }
  await releaseLease();

  // A run whose grant was lost, or whose sub-pool did not close cleanly, fails even when the command exited 0, and
  // whatever bound also fired: its verdict is an ordinary failure, never a bound.
  const operationalFailure = state.grantLost || state.poolCloseError !== null;
  await recordRun(code, state.expiry, Date.now() - startedAt, operationalFailure);

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
