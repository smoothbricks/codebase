import { mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

/**
 * Why one bounded run ended, recorded per Nx task so a caller can tell a load-sensitive bound
 * from a real failure without reading the run's output.
 *
 * - `bound`: the run hit a wall-clock bound and nothing else failed. Either the command
 *   outlived `timeoutMs` (`total`), or every failing test in the runner's report failed on
 *   its runner's per-test timeout (`test`). A loaded host produces exactly these.
 * - `wedged`: `idleTimeoutMs` fired. Silence means a hang, not a slow host, and the fix is the
 *   opposite one, so it is never a `bound`.
 * - `failed`: anything else that did not pass. That includes an assertion named in the report,
 *   or a non-zero exit with no report to say why.
 */
export type BoundedExecVerdict =
  | { readonly outcome: 'passed' }
  | { readonly outcome: 'bound'; readonly bound: 'total'; readonly limitMs: number; readonly elapsedMs: number }
  | { readonly outcome: 'bound'; readonly bound: 'test'; readonly tests: readonly string[] }
  | { readonly outcome: 'wedged'; readonly idleMs: number; readonly elapsedMs: number }
  | { readonly outcome: 'failed'; readonly exitCode: number; readonly tests: readonly string[] };

/** One task's verdict, keyed by the Nx task and the hash it ran at. */
export interface BoundedExecRecord {
  readonly task: string;
  readonly hash: string | null;
  readonly verdict: BoundedExecVerdict;
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

/** The failing tests of one JUnit report, split by whether only a timeout failed them. */
export interface JunitFailures {
  readonly timedOut: readonly string[];
  readonly failed: readonly string[];
}

/**
 * Read the failing test cases out of a JUnit document. A case fails when it holds a `failure`
 * or `error` element; it counts as timed out only when every such element is a timeout.
 * Retry elements (`flakyFailure`, `rerunFailure`, ...) describe attempts, not the verdict.
 */
export function junitFailures(xml: string): JunitFailures {
  const timedOut: string[] = [];
  const failed: string[] = [];
  const text = xml.replace(/<!\[CDATA\[[\s\S]*?\]\]>/g, '').replace(/<!--[\s\S]*?-->/g, '');
  const tags = /<(\/?)(testcase|failure|error)\b((?:[^>"']|"[^"]*"|'[^']*')*?)(\/?)>/g;
  let current: { name: string; types: string[] } | null = null;
  const close = (): void => {
    if (current === null) {
      return;
    }
    if (current.types.length > 0) {
      (current.types.every((type) => Object.hasOwn(TIMEOUT_FAILURE_TYPES, type)) ? timedOut : failed).push(
        current.name,
      );
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
      current = { name: classname ? `${classname} ${name}` : name, types: [] };
      if (selfClosing === '/') {
        close();
      }
      continue;
    }
    if (slash === '/' || current === null) {
      continue;
    }
    current.types.push(attribute(attributes, 'type') ?? '');
  }
  close();
  return { timedOut, failed };
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

/** How a run ended, as the executor saw it. */
export interface RunEnd {
  readonly exitCode: number;
  readonly elapsedMs: number;
  readonly expiry: { readonly kind: 'total' | 'idle'; readonly limitMs: number } | null;
  /** The run's report, or `null` when the command was asked for none or wrote none. */
  readonly report: JunitFailures | null;
}

/**
 * Judge a run. A test that failed on its own is a failure even when a bound fired too, and an
 * idle bound is never load: the report's assertions and the idle bound are read first.
 */
export function judgeRun(run: RunEnd): BoundedExecVerdict {
  if (run.expiry?.kind === 'idle') {
    return { outcome: 'wedged', idleMs: run.expiry.limitMs, elapsedMs: run.elapsedMs };
  }
  if (run.report !== null && run.report.failed.length > 0) {
    return { outcome: 'failed', exitCode: run.exitCode, tests: run.report.failed };
  }
  if (run.expiry?.kind === 'total') {
    return { outcome: 'bound', bound: 'total', limitMs: run.expiry.limitMs, elapsedMs: run.elapsedMs };
  }
  if (run.exitCode === 0) {
    return { outcome: 'passed' };
  }
  if (run.report !== null && run.report.timedOut.length > 0) {
    return { outcome: 'bound', bound: 'test', tests: run.report.timedOut };
  }
  return { outcome: 'failed', exitCode: run.exitCode, tests: [] };
}

/** Where `task`'s verdict and report live: this checkout's own Nx workspace-data directory. */
export function taskDirectory(workspaceData: string, taskId: string): string {
  return join(workspaceData, 'bounded-exec', encodeURIComponent(taskId));
}

export const VERDICT_FILE = 'verdict.json';
export const JUNIT_FILE = 'report.xml';
const NEXTEST_REPORT_CONFIG_FILE = 'nextest-report.toml';

/** `bun test`, or nextest's `nextest run` under any cargo spelling, as one shell word sequence. */
const BUN_TEST = /(?<=^|[\s;&|()])bun\s+test(?=\s|$)/g;
const NEXTEST_RUN = /(?<=^|[\s;&|()-])nextest\s+run(?=\s|$)/g;

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
      : command.replace(BUN_TEST, () => `bun test --reporter=junit --reporter-outfile=${shellQuote(report)}`);
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
      case 'wedged':
        if (typeof value.idleMs === 'number' && typeof value.elapsedMs === 'number') {
          return { outcome: 'wedged', idleMs: value.idleMs, elapsedMs: value.elapsedMs };
        }
        break;
      case 'failed':
        if (typeof value.exitCode === 'number' && isStrings(value.tests)) {
          return { outcome: 'failed', exitCode: value.exitCode, tests: value.tests };
        }
        break;
    }
  }
  throw new Error(`not a bounded-exec verdict: ${JSON.stringify(value)}`);
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
      return `wedged: no output for ${verdict.idleMs}ms after ${verdict.elapsedMs}ms`;
    case 'failed':
      return verdict.tests.length > 0
        ? `failed: ${verdict.tests.join(', ')}`
        : `exited ${verdict.exitCode} with no report naming a timeout`;
  }
}
