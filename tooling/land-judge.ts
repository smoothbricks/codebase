#!/usr/bin/env bun
// LAND-JUDGE: what tooling/land-host.sh asks of a gate that failed.
//
// A land may tolerate a task that failed on a wall-clock bound, and a bound is worth tolerating only if it hides
// nothing. The facts come from two places, both this checkout's own:
//
//   - the output of the Nx run, which names the tasks that failed, the tasks it did not run behind them, and the
//     tasks it stopped. Not Nx's `run.json`: this repository's patched Nx writes it to a per-user directory that
//     every checkout of the workspace shares, so a concurrent run anywhere overwrites it, and
//     `smoo-nx-bound-failures` reads the checkout's own directory, which holds nothing current.
//   - bounded-exec's record of each task it ran, in the checkout's Nx workspace-data directory: its verdict and its
//     JUnit report (@smoothbricks/nx-plugin README, "Bounded Test Targets"). A task clears its own record when it
//     starts, and `forget` clears them before a run, so every record read was written by the run being judged.
//
// Commands (exit 0: the answer is on stdout; 1: the land stops and stderr says why; 2: the question was not asked):
//
//   failures OUTPUT
//       One line per failed task of the run whose Nx output is OUTPUT, in smoo-nx-bound-failures' format:
//       `bound  TASK: WHY` for a task that failed on a wall-clock bound alone, `failed TASK: WHY` for anything else.
//       A timeout is no bound when the test panicked or failed an assertion before it hung: a test thread can panic and
//       then hang until the bound, and nextest reports that test as a timeout. Exit 0 only when every failed task is a
//       `bound` and Nx stopped none.
//   rerun-args TASK
//       How to run TASK once more so every one of its tests runs. nextest stops a run at its first failed test, and a
//       per-test timeout is a failed test, so the tests after it never ran and could hold an assertion failure;
//       `--no-fail-fast` runs them all. bun runs every test already. A task that outlived its total ceiling runs as
//       configured, and nextest with `--no-fail-fast` when its report shows nextest.
//   coverage TASK OUTPUT
//       Whether the run that just failed on a per-test bound accounted for every test its runner started: the runner's
//       own count (nextest's `Starting N tests`, bun's `Ran N tests`) in OUTPUT against the tests its JUnit report
//       holds. A command that outlived its total ceiling was killed mid-run, so what it never reached is unknown.
//   skipped OUTPUT --graph FILE
//       The tasks Nx did not run behind a failed task, from the run's Nx output, as `nx run-many` groups in dependency
//       order, from the task graph `nx run-many --graph=FILE` wrote. A tolerated failure must not hide the tasks
//       behind it.
//   forget [TASK]
//       Deletes bounded-exec's records, of TASK or of every task, so none is read as the next run's.
import { readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory.js';
import { workspaceRoot } from 'nx/src/utils/workspace-root.js';

export type Runner = 'nextest' | 'bun';

/** What a runner's JUnit report says about the run that wrote it. */
export interface Report {
  readonly runner: Runner;
  /** Every test case the report holds, skipped ones included: nextest and bun both count them. */
  readonly tests: number;
  /** The test cases that failed or errored, whatever the failure. */
  readonly failed: number;
}

const ROOT_NAMES: Readonly<Record<string, Runner>> = { 'nextest-run': 'nextest', 'bun test': 'bun' };

function attribute(attributes: string, name: string): string | null {
  const match = new RegExp(`(?:^|\\s)${name}="([^"]*)"`).exec(attributes);
  return match?.[1] ?? null;
}

/** The runner, test count and failed-test count of a JUnit report, or `null` when no known runner wrote it. */
export function parseReport(xml: string): Report | null {
  const attributes = /<testsuites\b([^>]*)>/.exec(xml)?.[1];
  if (attributes === undefined) {
    return null;
  }
  const name = attribute(attributes, 'name');
  const tests = Number(attribute(attributes, 'tests'));
  const failed = Number(attribute(attributes, 'failures') ?? Number.NaN) + Number(attribute(attributes, 'errors') ?? 0);
  const runner = name === null ? undefined : ROOT_NAMES[name];
  return runner === undefined || !Number.isInteger(tests) || !Number.isInteger(failed)
    ? null
    : { runner, tests, failed };
}

const TESTCASE = /<testcase\b(?:[^>"']|"[^"]*"|'[^']*')*?(?:\/>|>([\s\S]*?)<\/testcase>)/g;
const REAL_FAILURE = /panicked at|assertion\b[^\n]*\bfailed|AssertionError/;

/**
 * The first failed test case of a JUnit report whose own output shows a panic or a failed assertion, or `null`. A
 * timeout that follows one is not a bound: the test failed on its own and then hung.
 */
export function panickedTest(xml: string): string | null {
  for (const match of xml.matchAll(TESTCASE)) {
    const body = match[1] ?? '';
    if (/<(?:failure|error)\b/.test(body) && REAL_FAILURE.test(body)) {
      return /\bname="([^"]*)"/.exec(match[0])?.[1] ?? '(unnamed test)';
    }
  }
  return null;
}

const ANSI = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*[A-Za-z]`, 'g');
const DECLARATIONS: Readonly<Record<Runner, RegExp>> = {
  nextest: /\bStarting (\d+) tests? across\b/g,
  bun: /\bRan (\d+) tests? across\b/g,
};

/** How many tests the runner said it started, from the task's own output, or `null` when it never said. */
export function declaredTests(output: string, runner: Runner): number | null {
  const counts = [...output.replace(ANSI, '').matchAll(DECLARATIONS[runner])];
  const last = counts.at(-1)?.[1];
  return last === undefined ? null : Number(last);
}

/** What a failed Nx run names in its closing summary. */
export interface Outcome {
  /** The tasks that failed. */
  readonly failed: readonly string[];
  /** The tasks Nx did not run because a task they depend on failed. */
  readonly skipped: readonly string[];
  /** The tasks Nx killed before they finished. */
  readonly stopped: readonly string[];
}

const SUMMARY_HEADERS: Readonly<Record<keyof Outcome, string>> = {
  failed: 'Failed tasks:',
  skipped: 'Tasks not run because their dependencies failed or --nx-bail=true:',
  stopped: 'Tasks stopped before they finished:',
};

/**
 * The task lists of the closing summary of an Nx run's output: a header line, a blank line, then `- <task id>` lines.
 * The last summary wins, and a list the run did not print is empty. Tasks print their own output above the summary.
 */
export function parseOutcome(output: string): Outcome {
  const lines = output
    .replace(ANSI, '')
    .split('\n')
    .map((line) => line.trimEnd());
  const list = (header: string): string[] => {
    const at = lines.lastIndexOf(header);
    const tasks: string[] = [];
    for (const line of at < 0 ? [] : lines.slice(at + 1)) {
      const item = /^- (\S+)$/.exec(line)?.[1];
      if (item !== undefined) {
        tasks.push(item);
      } else if (line !== '' || tasks.length > 0) {
        break;
      }
    }
    return tasks;
  };
  return {
    failed: list(SUMMARY_HEADERS.failed),
    skipped: list(SUMMARY_HEADERS.skipped),
    stopped: list(SUMMARY_HEADERS.stopped),
  };
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Why a bounded-exec record is a wall-clock bound, or why it is not one. */
type Bound =
  | { readonly kind: 'total'; readonly limitMs: number }
  | { readonly kind: 'test'; readonly tests: readonly string[] }
  | { readonly why: string };

/** The wall-clock bound a bounded-exec record (`verdict.json`, parsed) shows, or what it shows instead. */
function boundOf(task: string, record: unknown): Bound {
  const verdict = isObject(record) && record.task === task ? record.verdict : undefined;
  if (!isObject(verdict)) {
    return { why: 'no bounded-exec verdict' };
  }
  switch (verdict.outcome) {
    case 'bound':
      return verdict.bound === 'total'
        ? { kind: 'total', limitMs: Number(verdict.limitMs) }
        : { kind: 'test', tests: Array.isArray(verdict.tests) ? verdict.tests.map(String) : [] };
    case 'wedged':
      return { why: `wedged: no output for ${String(verdict.idleMs)}ms after ${String(verdict.elapsedMs)}ms` };
    case 'failed':
      return {
        why:
          Array.isArray(verdict.tests) && verdict.tests.length > 0
            ? `failed: ${verdict.tests.join(', ')}`
            : `exited ${String(verdict.exitCode)} with no report naming a timeout`,
      };
    default:
      return { why: `its verdict is '${String(verdict.outcome)}', not a bound` };
  }
}

/** One failed task: whether it failed on a wall-clock bound alone, and what to say of it. */
export interface Failure {
  readonly task: string;
  readonly bound: boolean;
  readonly why: string;
}

/** `task`'s failure, from its bounded-exec record and JUnit report (`null` when it left none). */
export function judgeTask(task: string, record: unknown, reportXml: string | null): Failure {
  const bound = boundOf(task, record);
  if ('why' in bound) {
    return { task, bound: false, why: bound.why };
  }
  const panicked = reportXml === null ? null : panickedTest(reportXml);
  if (panicked !== null) {
    return { task, bound: false, why: `${panicked} panicked or failed an assertion before it timed out` };
  }
  return {
    task,
    bound: true,
    why:
      bound.kind === 'total'
        ? `outlived its ${bound.limitMs}ms bound`
        : `per-test timeout only: ${bound.tests.join(', ')}`,
  };
}

/** Every failed task of a run, judged; `ok` when the run failed, every task failed on a bound alone and Nx stopped none. */
export function judgeRun(
  outcome: Outcome,
  recorded: (task: string) => { record: unknown; report: string | null },
): { readonly ok: boolean; readonly lines: readonly string[] } {
  const failures = outcome.failed.map((task) => {
    const { record, report } = recorded(task);
    return judgeTask(task, record, report);
  });
  const lines = failures.map(({ task, bound, why }) => `${bound ? 'bound ' : 'failed'} ${task}: ${why}`);
  for (const task of outcome.stopped) {
    lines.push(`failed ${task}: Nx stopped it before it finished`);
  }
  if (outcome.failed.length === 0 && outcome.stopped.length === 0) {
    lines.push('failed: the run named no failed task');
  }
  return {
    ok: failures.length > 0 && outcome.stopped.length === 0 && failures.every(({ bound }) => bound),
    lines,
  };
}

export type Coverage =
  | { readonly covered: true; readonly tests: number; readonly timedOut: readonly string[] }
  | { readonly covered: false; readonly why: string };

const NEXTEST_FULL_RUN: readonly string[] = ['--args=--no-fail-fast', '--forwardAllArgs=false'];

/**
 * The extra `nx run` arguments that make a failed task's runner run every test, or why no re-run can prove it. nextest
 * stops a run at its first failed test, and a per-test timeout is one, so it needs `--no-fail-fast`; bun runs them
 * all. A task killed at its total ceiling left no report to say which runner it is, so it runs as configured.
 */
export function fullRun(
  task: string,
  record: unknown,
  reportXml: string | null,
): { readonly args: readonly string[] } | { readonly why: string } {
  const bound = boundOf(task, record);
  if ('why' in bound) {
    return bound;
  }
  const report = reportXml === null ? null : parseReport(reportXml);
  if (bound.kind === 'test') {
    return report === null
      ? { why: "it left no JUnit report of nextest's or bun's, so no re-run can prove every test ran" }
      : { args: report.runner === 'nextest' ? NEXTEST_FULL_RUN : [] };
  }
  return { args: report?.runner === 'nextest' ? NEXTEST_FULL_RUN : [] };
}

/**
 * Whether the run that wrote `record`, `reportXml` and `output` failed on per-test timeouts alone AND accounts for
 * every test its runner started: the runner's own count against the tests its report holds.
 */
export function judgeCoverage(task: string, record: unknown, reportXml: string | null, output: string): Coverage {
  const bound = boundOf(task, record);
  if ('why' in bound) {
    return { covered: false, why: bound.why };
  }
  if (bound.kind === 'total') {
    return {
      covered: false,
      why: `it outlived its ${bound.limitMs}ms ceiling even run alone, so the tests it never reached are unknown`,
    };
  }
  if (reportXml === null) {
    return { covered: false, why: 'it wrote no JUnit report' };
  }
  const report = parseReport(reportXml);
  if (report === null) {
    return { covered: false, why: "its JUnit report is not nextest's or bun's" };
  }
  if (report.failed !== bound.tests.length) {
    return {
      covered: false,
      why: `its report holds ${report.failed} failed tests and the verdict names ${bound.tests.length} per-test timeouts`,
    };
  }
  const panicked = panickedTest(reportXml);
  if (panicked !== null) {
    return { covered: false, why: `${panicked} panicked or failed an assertion before it timed out` };
  }
  const declared = declaredTests(output, report.runner);
  if (declared === null) {
    return { covered: false, why: `its output never says how many tests ${report.runner} started` };
  }
  if (declared !== report.tests) {
    return {
      covered: false,
      why: `${report.runner} started ${declared} tests and its report accounts for ${report.tests}`,
    };
  }
  return { covered: true, tests: report.tests, timedOut: bound.tests };
}

/** A group of tasks one `nx run-many -t TARGET -p PROJECTS --excludeTaskDependencies` runs. */
export interface Group {
  readonly target: string;
  readonly projects: readonly string[];
}

/** The part of Nx's task graph (`nx run-many --graph=FILE.json`) the plan reads. */
export interface TaskGraph {
  readonly tasks: Readonly<Record<string, { readonly target: { readonly project: string; readonly target: string } }>>;
  readonly dependencies: Readonly<Record<string, readonly string[]>>;
  readonly continuousDependencies?: Readonly<Record<string, readonly string[]>>;
}

/**
 * The `skipped` tasks of `graph`, in the order they can run: each level holds the tasks whose dependencies that are
 * also skipped are all in earlier levels, grouped by target. A dependency that ran, passed or failed, is no obstacle,
 * because the caller has judged every failure before it asks. Tasks in one level never wait on each other, so Nx may
 * run them together.
 */
export function skippedGroups(graph: TaskGraph, skipped: readonly string[]): Group[] {
  for (const id of skipped) {
    if (graph.tasks[id] === undefined) {
      throw new Error(`Nx did not run ${id}, which the task graph does not hold: the graph is not this run's`);
    }
  }
  const pending = new Set(skipped);
  const levels = new Map<string, number>();
  const level = (id: string, path: readonly string[]): number => {
    const known = levels.get(id);
    if (known !== undefined) {
      return known;
    }
    if (path.includes(id)) {
      throw new Error(`the task graph has a cycle through ${[...path, id].join(' -> ')}`);
    }
    const behind = [...(graph.dependencies[id] ?? []), ...(graph.continuousDependencies?.[id] ?? [])].filter((dep) =>
      pending.has(dep),
    );
    const own = behind.length === 0 ? 0 : 1 + Math.max(...behind.map((dep) => level(dep, [...path, id])));
    levels.set(id, own);
    return own;
  };
  for (const id of skipped) {
    level(id, []);
  }
  const groups: Group[] = [];
  const depth = Math.max(-1, ...levels.values());
  for (let current = 0; current <= depth; current++) {
    const byTarget = new Map<string, string[]>();
    for (const id of skipped) {
      if (levels.get(id) !== current) {
        continue;
      }
      const { project, target } = graph.tasks[id]?.target ?? { project: '', target: '' };
      byTarget.set(target, [...(byTarget.get(target) ?? []), project]);
    }
    for (const [target, projects] of byTarget) {
      groups.push({ target, projects });
    }
  }
  return groups;
}

function parseGraph(value: unknown): TaskGraph {
  const tasks = isObject(value) ? value.tasks : undefined;
  if (!isObject(tasks) || !isObject(tasks.tasks) || !isObject(tasks.dependencies)) {
    throw new Error('the graph file has no task graph; it is not the output of `nx run-many --graph=FILE.json`');
  }
  return {
    tasks: tasks.tasks as TaskGraph['tasks'],
    dependencies: tasks.dependencies as TaskGraph['dependencies'],
    continuousDependencies: isObject(tasks.continuousDependencies)
      ? (tasks.continuousDependencies as TaskGraph['dependencies'])
      : undefined,
  };
}

function readIfThere(path: string): string | null {
  try {
    return readFileSync(path, 'utf8');
  } catch (error) {
    if (isObject(error) && error.code === 'ENOENT') {
      return null;
    }
    throw error;
  }
}

/** Where bounded-exec keeps one task's verdict and report, under `workspaceData`. */
function taskDirectory(workspaceData: string, task: string): string {
  return join(workspaceData, 'bounded-exec', encodeURIComponent(task));
}

/** What bounded-exec recorded for `task`: its verdict record and JUnit report, `null` when absent. */
function recordedRun(workspaceData: string, task: string): { record: unknown; report: string | null } {
  const directory = taskDirectory(workspaceData, task);
  const verdict = readIfThere(join(directory, 'verdict.json'));
  return { record: verdict === null ? null : JSON.parse(verdict), report: readIfThere(join(directory, 'report.xml')) };
}

function main(argv: readonly string[]): number {
  const { values, positionals } = parseArgs({
    args: [...argv],
    allowPositionals: true,
    options: { data: { type: 'string' }, graph: { type: 'string' } },
  });
  const [command, first, second] = positionals;
  const data = values.data ?? workspaceDataDirectoryForWorkspace(workspaceRoot);
  switch (command) {
    case 'failures': {
      if (first === undefined) {
        throw new Error('usage: land-judge.ts failures OUTPUT');
      }
      const judged = judgeRun(parseOutcome(readFileSync(first, 'utf8')), (task) => recordedRun(data, task));
      process.stdout.write(`${judged.lines.join('\n')}\n`);
      return judged.ok ? 0 : 1;
    }
    case 'rerun-args': {
      if (first === undefined) {
        throw new Error('usage: land-judge.ts rerun-args TASK');
      }
      const { record, report } = recordedRun(data, first);
      const run = fullRun(first, record, report);
      if ('why' in run) {
        process.stderr.write(`blocked ${first}: ${run.why}\n`);
        return 1;
      }
      process.stdout.write(`${run.args.join(' ')}\n`);
      return 0;
    }
    case 'coverage': {
      if (first === undefined || second === undefined) {
        throw new Error('usage: land-judge.ts coverage TASK OUTPUT');
      }
      const { record, report } = recordedRun(data, first);
      const coverage = judgeCoverage(first, record, report, readFileSync(second, 'utf8'));
      if (!coverage.covered) {
        process.stderr.write(`blocked ${first}: ${coverage.why}\n`);
        return 1;
      }
      process.stdout.write(
        `covered ${first}: ${coverage.tests} tests ran, ${coverage.timedOut.length} timed out on their per-test bound: ${coverage.timedOut.join(', ')}\n`,
      );
      return 0;
    }
    case 'skipped': {
      if (first === undefined || values.graph === undefined) {
        throw new Error('usage: land-judge.ts skipped OUTPUT --graph FILE');
      }
      const { skipped } = parseOutcome(readFileSync(first, 'utf8'));
      for (const { target, projects } of skippedGroups(
        parseGraph(JSON.parse(readFileSync(values.graph, 'utf8'))),
        skipped,
      )) {
        process.stdout.write(`group\t${target}\t${projects.join(',')}\n`);
      }
      return 0;
    }
    case 'forget': {
      rmSync(first === undefined ? join(data, 'bounded-exec') : taskDirectory(data, first), {
        recursive: true,
        force: true,
      });
      return 0;
    }
    default:
      throw new Error(
        'usage: land-judge.ts <failures OUTPUT | rerun-args TASK | coverage TASK OUTPUT | skipped OUTPUT --graph FILE | forget [TASK]>',
      );
  }
}

if (import.meta.main) {
  try {
    process.exit(main(process.argv.slice(2)));
  } catch (error) {
    process.stderr.write(`land-judge: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(2);
  }
}
