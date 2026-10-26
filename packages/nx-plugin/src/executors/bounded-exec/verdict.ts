import { mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { describeHolders, type LockWait, type Opener } from './lock-wait.js';

/**
 * Why one bounded run ended, recorded per Nx task so a caller can tell a load-sensitive bound
 * from a real failure without reading the run's output.
 *
 * - `bound`: the run hit a wall-clock bound and nothing else failed. Either the command
 *   outlived `timeoutMs` (`total`), or every failing test in the runner's report failed on
 *   its runner's per-test timeout (`test`). A loaded host produces exactly these.
 * - `wedged`: `idleTimeoutMs` fired: no test finished for that long during a chargeable test interval. This proves
 *   the suite's completion bound was exceeded, not that every process stopped: an expensive active fixture can
 *   exceed it too. The verdict names tests reported running but never finished and tests the report says were
 *   aborted; diagnostics distinguish a stalled wait from a progressing but oversized lifecycle.
 *   When the snapshot named a lock wait, `lockWaits` names the file the run holds on a numbered descriptor and the
 *   processes outside the run that also hold it open: open-handle evidence, not a kernel lock owner.
 * - `no-space`: the run failed and its output said a write found no space (ENOSPC). Whatever its tests then reported,
 *   they failed on the disk, not on their own: the verdict names the line that said so and, when the task held one,
 *   its RAM temp volume as it stood when that line arrived.
 * - `failed`: anything else that did not pass. That includes an assertion named in the report,
 *   or a non-zero exit with no report to say why.
 */
export type BoundedExecVerdict =
  | { readonly outcome: 'passed' }
  | { readonly outcome: 'bound'; readonly bound: 'total'; readonly limitMs: number; readonly elapsedMs: number }
  | { readonly outcome: 'bound'; readonly bound: 'test'; readonly tests: readonly string[] }
  | {
      readonly outcome: 'wedged';
      readonly idleMs: number;
      readonly elapsedMs: number;
      readonly tests: readonly string[];
      /** Lock waits the snapshot named: holder evidence (open handles), not a kernel lock owner. */
      readonly lockWaits: readonly LockWait[];
    }
  | ({ readonly outcome: 'no-space'; readonly exitCode: number; readonly tests: readonly string[] } & NoSpaceSighting)
  | { readonly outcome: 'failed'; readonly exitCode: number; readonly tests: readonly string[] };

/**
 * The task's RAM temp volume, read when a line saying ENOSPC arrived: the writer's own lease and the space left on the
 * volume it was taken on (`measured`), or why there was none to measure: the lease was no longer on that volume
 * (`lost`), or the volume could not be read (`unread`).
 */
export type TempVolumeSpace =
  | {
      readonly kind: 'measured';
      readonly lease: string;
      readonly mountpoint: string;
      readonly capacityBytes: number;
      readonly freeBytes: number;
    }
  | { readonly kind: 'lost' | 'unread'; readonly lease: string; readonly mountpoint: string; readonly detail: string };

/** The first output line that said a write found no space, and the task's RAM temp volume then; `null` without one. */
export interface NoSpaceSighting {
  readonly line: string;
  readonly volume: TempVolumeSpace | null;
}

/** One task's verdict, keyed by the Nx task and the hash it ran at. */
export interface BoundedExecRecord {
  readonly task: string;
  readonly hash: string | null;
  readonly verdict: BoundedExecVerdict;
  /**
   * The parent CPU pool's grant the run started under: tokens granted of those asked for, and
   * how long it waited for the pool. Absent when it ran ungranted; written for the reader of a slow
   * run, never read back.
   */
  readonly cpu?: {
    readonly tokens: number;
    readonly want: number;
    readonly waitedMs: number;
  };
}

/** The Nx task a run executes as. Nx gives an executor no task id, only its parts. */
export interface BoundedExecTask {
  readonly id: string;
  readonly hash: string | null;
}

/** The environment variable naming the absolute path a command writes its JUnit report to. */
export const JUNIT_REPORT_ENV = 'BOUNDED_EXEC_JUNIT';

/**
 * JUnit failure `type`s that mean "the runner's per-test timeout fired". `cargo-nextest` writes
 * `test timeout` for a test it terminated at its slow-timeout; `bun test` writes `TimeoutError`
 * for a test or hook that outlived `--timeout`. Every other type is a failure of the test itself.
 */
const TIMEOUT_FAILURE_TYPES: Readonly<Record<string, true>> = { 'test timeout': true, TimeoutError: true };

/**
 * The JUnit failure that means "the runner stopped this test because its command was told to stop". When the
 * SIGTERM bounded-exec sends at a bound reaches `cargo-nextest`, it ends the tests still running and reports each as
 * a `test abort` whose message names the signal: SIGTERM, or SIGKILL once the force-kill has followed. A test that
 * dies of any other signal (SIGSEGV, SIGABRT) is a crash of the test itself.
 */
const CANCELLED_ABORT_TYPE = 'test abort';
const CANCELLED_ABORT_MESSAGE = /^process aborted with signal (?:15 \(SIGTERM\)|9 \(SIGKILL\))$/;

type FailureKind = 'timedOut' | 'cancelled' | 'failed';

function failureKind(type: string, message: string): FailureKind {
  if (Object.hasOwn(TIMEOUT_FAILURE_TYPES, type)) {
    return 'timedOut';
  }
  return type === CANCELLED_ABORT_TYPE && CANCELLED_ABORT_MESSAGE.test(message) ? 'cancelled' : 'failed';
}

/** The failing tests of one JUnit report, split by what failed them. */
export interface JunitFailures {
  /** Failed only on the runner's per-test timeout. */
  readonly timedOut: readonly string[];
  /** Aborted by the runner because its command was told to stop, not failed by anything they did. */
  readonly cancelled: readonly string[];
  readonly failed: readonly string[];
}

/**
 * Read the failing test cases out of a JUnit document. A case fails when it holds a `failure`
 * or `error` element; it counts as timed out only when every such element is a timeout.
 * Retry elements (`flakyFailure`, `rerunFailure`, ...) describe attempts, not the verdict.
 */
export function junitFailures(xml: string): JunitFailures {
  const timedOut: string[] = [];
  const cancelled: string[] = [];
  const failed: string[] = [];
  const text = xml.replace(/<!\[CDATA\[[\s\S]*?\]\]>/g, '').replace(/<!--[\s\S]*?-->/g, '');
  const tags = /<(\/?)(testcase|failure|error)\b((?:[^>"']|"[^"]*"|'[^']*')*?)(\/?)>/g;
  let current: { name: string; kinds: FailureKind[] } | null = null;
  const close = (): void => {
    if (current === null) {
      return;
    }
    const [first] = current.kinds;
    if (first !== undefined) {
      const kind = current.kinds.every((one) => one === first) ? first : 'failed';
      (kind === 'timedOut' ? timedOut : kind === 'cancelled' ? cancelled : failed).push(current.name);
    }
    current = null;
  };
  for (const [, slash, tag, attributes = '', selfClosing] of text.matchAll(tags)) {
    if (tag === 'testcase') {
      if (slash === '/') {
        close();
        continue;
      }
      close();
      const name = attribute(attributes, 'name') ?? '';
      const classname = attribute(attributes, 'classname');
      current = { name: classname ? `${classname} ${name}` : name, kinds: [] };
      if (selfClosing === '/') {
        close();
      }
      continue;
    }
    if (slash === '/' || current === null) {
      continue;
    }
    current.kinds.push(failureKind(attribute(attributes, 'type') ?? '', attribute(attributes, 'message') ?? ''));
  }
  close();
  return { timedOut, cancelled, failed };
}

/** One test case of a JUnit report: nextest's binary id (its `classname`), the test's name and its seconds. */
export interface JunitCaseTime {
  readonly binary: string;
  readonly name: string;
  readonly seconds: number;
}

/** Every test case of a JUnit document with the `time` it ran for; a case without one is not timed and not listed. */
export function junitCaseTimes(xml: string): JunitCaseTime[] {
  const text = xml.replace(/<!\[CDATA\[[\s\S]*?\]\]>/g, '').replace(/<!--[\s\S]*?-->/g, '');
  const cases: JunitCaseTime[] = [];
  for (const [, attributes = ''] of text.matchAll(/<testcase\b((?:[^>"']|"[^"]*"|'[^']*')*?)\/?>/g)) {
    const time = attribute(attributes, 'time');
    const seconds = time === null ? Number.NaN : Number(time);
    const name = attribute(attributes, 'name');
    if (name === null || !Number.isFinite(seconds)) {
      continue;
    }
    cases.push({ binary: attribute(attributes, 'classname') ?? '', name, seconds });
  }
  return cases;
}

function attribute(attributes: string, name: string): string | null {
  const match = new RegExp(`(?:^|\\s)${name}\\s*=\\s*("([^"]*)"|'([^']*)')`).exec(attributes);
  if (match === null) {
    return null;
  }
  return decodeEntities(match[2] ?? match[3] ?? '');
}

function decodeEntities(value: string): string {
  return value.replace(/&(#x[0-9a-fA-F]+|#[0-9]+|amp|lt|gt|quot|apos);/g, (_, entity: string) => {
    switch (entity) {
      case 'amp':
        return '&';
      case 'lt':
        return '<';
      case 'gt':
        return '>';
      case 'quot':
        return '"';
      case 'apos':
        return "'";
      default:
        return String.fromCodePoint(
          entity.startsWith('#x') ? Number.parseInt(entity.slice(2), 16) : Number.parseInt(entity.slice(1), 10),
        );
    }
  });
}

/**
 * The bound that ended a run. `total` is a wall-clock ceiling: it answers "is this run unbounded?". `idle` is the
 * no-progress bound: it answers "is this run hung?", and carries the tests the runner reported still running.
 *
 * These are different questions and one number cannot answer both. A ceiling tight enough to catch a hang promptly
 * is also tight enough to fail a correct run on a busy machine; a ceiling loose enough never to do that cannot catch
 * a hang promptly. Keeping the two bounds separate is what makes each honest.
 */
export type ExpiredBound =
  | { readonly kind: 'total'; readonly limitMs: number }
  | { readonly kind: 'idle'; readonly limitMs: number; readonly tests: readonly string[] };

/** How a run ended, as the executor saw it. */
export interface RunEnd {
  readonly exitCode: number;
  readonly elapsedMs: number;
  readonly expiry: ExpiredBound | null;
  /** The run's report, or `null` when the command was asked for none or wrote none. */
  readonly report: JunitFailures | null;
  /** The first output line that said ENOSPC, or `null` when none did. */
  readonly noSpace: NoSpaceSighting | null;
  /** The lock waits the expiry's snapshot named; empty when none, or when that capture did not complete. */
  readonly lockWaits: readonly LockWait[];
}

/**
 * Judge a run. A run that failed after its output said ENOSPC failed on the disk: that is read first, ahead of the
 * tests it then failed, a bound it then hit or a hang the full disk caused; a passing run that printed it (a test of
 * its own ENOSPC handling) passed. A test that failed on its own is a failure even when a bound fired too, and the
 * no-progress bound is never load: the report's assertions and the idle bound are read next. A test the runner aborted
 * because the total bound told its command to stop is not one that failed; one it aborted at the idle bound is a test
 * still running.
 */
export function judgeRun(run: RunEnd): BoundedExecVerdict {
  if (run.noSpace !== null && (run.exitCode !== 0 || run.expiry !== null)) {
    const tests = run.report === null ? [] : [...run.report.failed, ...run.report.timedOut, ...run.report.cancelled];
    return { outcome: 'no-space', exitCode: run.exitCode === 0 ? 1 : run.exitCode, tests, ...run.noSpace };
  }
  if (run.expiry?.kind === 'idle') {
    const tests = [...new Set([...run.expiry.tests, ...(run.report?.cancelled ?? [])])];
    return { outcome: 'wedged', idleMs: run.expiry.limitMs, elapsedMs: run.elapsedMs, tests, lockWaits: run.lockWaits };
  }
  if (run.report !== null && run.report.failed.length > 0) {
    return { outcome: 'failed', exitCode: run.exitCode, tests: run.report.failed };
  }
  // The tests the runner aborted when the ceiling's SIGTERM reached it are the bound's casualties: they are in the
  // report as failures, but nothing about them failed. Aborted with no total bound to explain it, they are failures,
  // whatever else the run shows: a zero exit or a per-test timeout does not excuse a test someone else stopped.
  if (run.expiry?.kind === 'total') {
    return { outcome: 'bound', bound: 'total', limitMs: run.expiry.limitMs, elapsedMs: run.elapsedMs };
  }
  if (run.report !== null && run.report.cancelled.length > 0) {
    return { outcome: 'failed', exitCode: run.exitCode, tests: run.report.cancelled };
  }
  if (run.exitCode === 0) {
    return { outcome: 'passed' };
  }
  if (run.report !== null && run.report.timedOut.length > 0) {
    return { outcome: 'bound', bound: 'test', tests: run.report.timedOut };
  }
  return { outcome: 'failed', exitCode: run.exitCode, tests: [] };
}

/**
 * A run that failed for a reason its command did not cause: its CPU grant was lost, or its sub-pool did not close
 * cleanly. It is an ordinary failure whatever the command exited with and whatever bound also fired, and the tests it
 * reports are failures, never bounds. A failed run never records exit 0.
 */
export function judgeOperationalFailure(exitCode: number, report: JunitFailures | null): BoundedExecVerdict {
  const tests = report === null ? [] : [...report.failed, ...report.timedOut, ...report.cancelled];
  return { outcome: 'failed', exitCode: exitCode === 0 ? 1 : exitCode, tests };
}

/** Where `task`'s verdict and report live: this checkout's own Nx workspace-data directory. */
export function taskDirectory(workspaceData: string, taskId: string): string {
  return join(workspaceData, 'bounded-exec', encodeURIComponent(taskId));
}

export const VERDICT_FILE = 'verdict.json';
export const JUNIT_FILE = 'report.xml';
const NEXTEST_REPORT_CONFIG_FILE = 'nextest-report.toml';

/**
 * `bun test`, or nextest's `nextest run` under any cargo spelling, as one shell word sequence.
 * Global, for `match` and `replace`; `search` ignores the flag where only presence matters.
 *
 * `bun` is any unquoted command word whose basename is `bun`, so a runtime pinned by path
 * (`../bun-runtime/.runtime/bun test`) is the runner too. Capture group 1 is that word, which
 * a rewrite must keep: it is the binary the target chose. An assignment (`BUN_EXE=../x/bun test`
 * runs the command `test`) and a longer basename (`debug-bun test`) are not it.
 */
export const BUN_TEST = /(?<=^|[\s;&|()])((?:[^\s;&|()<>'"`\\=]*\/)?bun)\s+test(?=\s|$)/g;
export const NEXTEST_RUN = /(?<=^|[\s;&|()-])nextest\s+run(?=\s|$)/g;
export const GO_TEST = /(?<=^|[\s;&|()])go\s+test(?=\s|$)/;

/**
 * `command` with its test runner asked to write a JUnit report into `directory`, the report
 * `judgeRun` reads. Both runners are recognised by their invocation, so every target that runs
 * one gets a report without restating it: `bun test` takes the path as flags, and nextest takes
 * it only from configuration, so it gets a one-key tool config written beside the report.
 *
 * A command that runs no runner, or more than one (two runs would overwrite one report), or
 * that already chose bun's reporter, is returned unchanged and judged by its exit alone.
 */
export async function withJunitReport(command: string, directory: string): Promise<string> {
  const bun = command.match(BUN_TEST)?.length ?? 0;
  const nextest = command.match(NEXTEST_RUN)?.length ?? 0;
  if (bun + nextest !== 1) {
    return command;
  }
  const report = join(directory, JUNIT_FILE);
  if (bun === 1) {
    return /(^|\s)--reporter(=|\s|$)/.test(command)
      ? command
      : command.replace(
          BUN_TEST,
          (_match, runtime: string) => `${runtime} test --reporter=junit --reporter-outfile=${shellQuote(report)}`,
        );
  }
  const config = join(directory, NEXTEST_REPORT_CONFIG_FILE);
  await writeFile(config, `[profile.default.junit]\npath = ${JSON.stringify(report)}\n`);
  return command.replace(NEXTEST_RUN, () => `nextest run --tool-config-file ${shellQuote(`smoo-report:${config}`)}`);
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

/** Remove a previous run's verdict and report, so neither can be read as this run's. */
export async function clearTaskDirectory(directory: string): Promise<void> {
  await mkdir(directory, { recursive: true });
  await Promise.all([
    rm(join(directory, VERDICT_FILE), { force: true }),
    rm(join(directory, JUNIT_FILE), { force: true }),
  ]);
}

export async function writeRecord(directory: string, record: BoundedExecRecord): Promise<void> {
  const target = join(directory, VERDICT_FILE);
  const staged = `${target}.${process.pid}`;
  await writeFile(staged, `${JSON.stringify(record, null, 2)}\n`);
  await rename(staged, target);
}

/** The record in `directory`, or `null` when there is none. A malformed record throws. */
export async function readRecord(directory: string): Promise<BoundedExecRecord | null> {
  let text: string;
  try {
    text = await readFile(join(directory, VERDICT_FILE), 'utf8');
  } catch (error) {
    if (isNotFound(error)) {
      return null;
    }
    throw error;
  }
  return parseRecord(JSON.parse(text));
}

/** The JUnit report a command wrote into `directory`, or `null` when it wrote none. */
export async function readJunitReport(directory: string): Promise<JunitFailures | null> {
  try {
    return junitFailures(await readFile(join(directory, JUNIT_FILE), 'utf8'));
  } catch (error) {
    if (isNotFound(error)) {
      return null;
    }
    throw error;
  }
}

function isNotFound(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT';
}

function parseRecord(value: unknown): BoundedExecRecord {
  if (!isObject(value) || typeof value.task !== 'string' || !(typeof value.hash === 'string' || value.hash === null)) {
    throw new Error(`not a bounded-exec record: ${JSON.stringify(value)}`);
  }
  return { task: value.task, hash: value.hash, verdict: parseVerdict(value.verdict) };
}

function parseVerdict(value: unknown): BoundedExecVerdict {
  if (isObject(value)) {
    switch (value.outcome) {
      case 'passed':
        return { outcome: 'passed' };
      case 'bound':
        if (value.bound === 'total' && typeof value.limitMs === 'number' && typeof value.elapsedMs === 'number') {
          return { outcome: 'bound', bound: 'total', limitMs: value.limitMs, elapsedMs: value.elapsedMs };
        }
        if (value.bound === 'test' && isStrings(value.tests)) {
          return { outcome: 'bound', bound: 'test', tests: value.tests };
        }
        break;
      case 'wedged': {
        // A record written before lock waits were named carries none.
        const lockWaits = value.lockWaits === undefined ? [] : parseLockWaits(value.lockWaits);
        if (
          typeof value.idleMs === 'number' &&
          typeof value.elapsedMs === 'number' &&
          isStrings(value.tests) &&
          lockWaits !== null
        ) {
          return { outcome: 'wedged', idleMs: value.idleMs, elapsedMs: value.elapsedMs, tests: value.tests, lockWaits };
        }
        break;
      }
      case 'no-space': {
        const volume = parseTempVolumeSpace(value.volume);
        if (typeof value.exitCode === 'number' && isStrings(value.tests) && typeof value.line === 'string' && volume) {
          return {
            outcome: 'no-space',
            exitCode: value.exitCode,
            tests: value.tests,
            line: value.line,
            volume: volume.value,
          };
        }
        break;
      }
      case 'failed':
        if (typeof value.exitCode === 'number' && isStrings(value.tests)) {
          return { outcome: 'failed', exitCode: value.exitCode, tests: value.tests };
        }
        break;
    }
  }
  throw new Error(`not a bounded-exec verdict: ${JSON.stringify(value)}`);
}

/** A process id as the census writes it: a positive safe integer. */
function isPid(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}

function isPids(value: unknown): value is number[] {
  return Array.isArray(value) && value.every(isPid);
}

/** Lock waits as recorded, or `null` when any part of them is malformed. */
function parseLockWaits(value: unknown): LockWait[] | null {
  if (!Array.isArray(value)) return null;
  const waits: LockWait[] = [];
  for (const wait of value) {
    if (!isObject(wait) || typeof wait.path !== 'string' || !isPids(wait.waiters) || !Array.isArray(wait.holders)) {
      return null;
    }
    const holders: Opener[] = [];
    for (const holder of wait.holders) {
      if (!isObject(holder) || !isPid(holder.pid) || typeof holder.command !== 'string') return null;
      holders.push({ pid: holder.pid, command: holder.command });
    }
    waits.push({ path: wait.path, waiters: wait.waiters, holders });
  }
  return waits;
}

/** A recorded volume, wrapped so a well-formed `null` (no RAM volume) differs from a malformed one (`undefined`). */
function parseTempVolumeSpace(value: unknown): { value: TempVolumeSpace | null } | undefined {
  if (value === null) {
    return { value: null };
  }
  if (!isObject(value) || typeof value.lease !== 'string' || typeof value.mountpoint !== 'string') {
    return undefined;
  }
  const { lease, mountpoint } = value;
  // A record written before the kinds were named measured whatever filesystem held the mountpoint then.
  if (
    (value.kind === 'measured' || value.kind === undefined) &&
    typeof value.capacityBytes === 'number' &&
    typeof value.freeBytes === 'number'
  ) {
    return {
      value: { kind: 'measured', lease, mountpoint, capacityBytes: value.capacityBytes, freeBytes: value.freeBytes },
    };
  }
  if ((value.kind === 'lost' || value.kind === 'unread') && typeof value.detail === 'string') {
    return { value: { kind: value.kind, lease, mountpoint, detail: value.detail } };
  }
  return undefined;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isStrings(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === 'string');
}

/** A failed task of an Nx run, and whether its bounded-exec record shows only a bound. */
export type FailedTask =
  | { readonly task: string; readonly bound: true; readonly verdict: Extract<BoundedExecVerdict, { outcome: 'bound' }> }
  | { readonly task: string; readonly bound: false; readonly reason: string };

/**
 * Each failed task of the run Nx summarised in `runJson` (its cache directory's `run.json`),
 * judged by the record bounded-exec wrote for exactly that run: same task, same hash. A task
 * with no record, or a record from another hash, is not a bound; nothing is assumed.
 */
export async function failedTasksOfRun(runJson: unknown, workspaceData: string): Promise<FailedTask[]> {
  if (!isObject(runJson) || !Array.isArray(runJson.tasks)) {
    throw new Error('run.json has no task list');
  }
  const failed: FailedTask[] = [];
  for (const entry of runJson.tasks) {
    if (!isObject(entry) || typeof entry.taskId !== 'string' || typeof entry.status !== 'number') {
      throw new Error(`run.json holds a malformed task: ${JSON.stringify(entry)}`);
    }
    if (entry.status === 0) {
      continue;
    }
    const task = entry.taskId;
    const record = await readRecord(taskDirectory(workspaceData, task));
    if (record === null) {
      failed.push({ task, bound: false, reason: 'no bounded-exec verdict' });
    } else if (record.task !== task || record.hash !== entry.hash) {
      failed.push({
        task,
        bound: false,
        reason: `its verdict is from hash ${record.hash}, not this run's ${entry.hash}`,
      });
    } else if (record.verdict.outcome === 'bound') {
      failed.push({ task, bound: true, verdict: record.verdict });
    } else {
      failed.push({ task, bound: false, reason: describeVerdict(record.verdict) });
    }
  }
  return failed;
}

export function describeVerdict(verdict: BoundedExecVerdict): string {
  switch (verdict.outcome) {
    case 'passed':
      return 'passed';
    case 'bound':
      return verdict.bound === 'total'
        ? `outlived its ${verdict.limitMs}ms bound (${verdict.elapsedMs}ms)`
        : `per-test timeout only: ${verdict.tests.join(', ')}`;
    case 'wedged':
      return `wedged: no test finished for ${verdict.idleMs}ms after ${verdict.elapsedMs}ms; ${
        verdict.tests.length > 0
          ? `still running: ${verdict.tests.join(', ')}`
          : 'the runner named no test still running'
      }${verdict.lockWaits
        .map((wait) => `; waiting on a lock: ${wait.path} held open by ${describeHolders(wait.holders)}`)
        .join('')}`;
    case 'no-space':
      return `${describeNoSpace(verdict)}${verdict.tests.length > 0 ? `; the tests that then failed: ${verdict.tests.join(', ')}` : ''}`;
    case 'failed':
      return verdict.tests.length > 0
        ? `failed: ${verdict.tests.join(', ')}`
        : `exited ${verdict.exitCode} with no report naming a timeout`;
  }
}

/**
 * What a run's output said about a write that found no space, and where the run's temp files lived then. The free
 * space is read when the line arrives, not after the command exits: by then a writer that failed has usually removed
 * its half-written files and the volume no longer looks full.
 */
export function describeNoSpace(sighting: NoSpaceSighting): string {
  const said = `ENOSPC (no space left on device): the command printed ${JSON.stringify(sighting.line)}`;
  const volume = sighting.volume;
  if (volume === null) {
    return `${said}; it held no RAM temp volume, so the full disk is the one that line's writer wrote to`;
  }
  switch (volume.kind) {
    case 'measured': {
      const mib = (bytes: number) => (bytes / (1024 * 1024)).toFixed(1);
      return `${said}; its TMPDIR ${volume.lease} is on the RAM temp volume ${volume.mountpoint}, which had ${mib(volume.freeBytes)} MiB free of ${mib(volume.capacityBytes)} MiB when that line arrived`;
    }
    case 'lost':
      return `${said}; its TMPDIR ${volume.lease} was no longer on the RAM temp volume at ${volume.mountpoint} when that line arrived, so no free space of that volume was read: ${volume.detail}`;
    case 'unread':
      return `${said}; its TMPDIR ${volume.lease} is on the RAM temp volume ${volume.mountpoint}, whose space could not be read when that line arrived: ${volume.detail}`;
  }
}
