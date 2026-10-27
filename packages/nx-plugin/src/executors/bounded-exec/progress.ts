import typia from 'typia';
import { invocationSpan, withInvocationFlag } from './runner-sizing.js';
import { BUN_TEST, GO_TEST, NEXTEST_RUN } from './verdict.js';

/**
 * What a run's output says about its forward motion: whether a test finished, and which tests the runner said
 * are running and has not said finished. `idleTimeoutMs` re-arms only on a finished test.
 *
 * Output alone is not progress. nextest repeats a `SLOW` line for every test still running at each slow-timeout
 * period, and a hung test may print a heartbeat, so a bound on silence never fires on either. A finished test is
 * what each runner reports once per test:
 *
 * - nextest: a status line carrying the run's counter, `PASS [   0.438s] (1/4) nt tests::quick` (also `FAIL`,
 *   `LEAK`, a signal name, `TRY 2 FAIL`, ...). A test still running is named by a `SLOW [>  30.000s] (───) …` or
 *   `START` line, which carries no counter; the name is nextest's `<binary-id> <test>`, as its JUnit report
 *   spells `classname name`.
 * - libtest (`cargo test`): `test tests::quick ... ok` (or `FAILED`, `ignored`); a test still running is named by
 *   `test tests::slow has been running for over 60 seconds`.
 * - vitest: timed ✓/× test results, timed ❯ failed-file results, and ↓ skipped-file results. The default
 *   reporter prints a completed file as ✓ file (N tests) Xms, plus slow or failed tests beneath it.
 *   Reporter headers, summaries and stack locations report no new completion.
 * - bun: a line of dots, one per test finished (`--dots`, which `withProgressReport` asks for: bun prints nothing
 *   per passing test when it believes an agent runs it), or a `(pass)`/`(fail)`/`(skip)`/`(todo)` line. bun names
 *   no test before it finishes, so a bun run names nothing in flight.
 * - go: structured `go test -json` package `start` follows the package's build dependencies; package `pass`,
 *   `fail`, or `skip` ends that execution interval. Test terminals re-arm the bound, but `output` and
 *   `build-output` never do. Initial and inter-package builds are outside an active test interval.
 *
 * Execution phases are owned by the private supervisor channel in runner-lifecycle.ts, never these text reports.
 */
export interface RunProgress {
  /** One chunk of one output stream; true when it reports a finished test, file or Go package. */
  scan(stream: OutputStream, chunk: string): boolean;
  /** The tests the runner reported running and has not reported finished, in the order it first named them. */
  inFlight(): string[];
}

export type OutputStream = 'stdout' | 'stderr';

const ANSI = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*[A-Za-z]`, 'g');
/** Lines naming a test or file that finished; group 1 is its name. */
const FINISHED = [
  /^\s*(?:[A-Z0-9][A-Z0-9-]*\s+)+\[\s*[0-9.]+s\]\s+\(\s*\d+\/\d+\)\s+(\S.*)$/,
  /^test (\S+) \.\.\. (?:ok|FAILED|ignored)\b/,
  /^\((?:pass|fail|skip|todo)\) (.+)$/,
  /^\s*[✓×]\s+(\S.*?)\s+\d+ms\b/,
  /^\s*❯\s+(\S.*?)\s+\(\d+ tests?(?: \| \d+ (?:failed|skipped|todo))*\)\s+\d+ms\b/,
  /^\s*↓\s+(\S.*?)\s+\(\d+ tests?(?: \| \d+ (?:skipped|todo))+\)$/,
];
/** Lines naming a test still running; group 1 is its name. */
const RUNNING = [
  /^\s*(?:SLOW|START)\s+\[[^\]]*\]\s+(?:\([^)]*\)\s+)?(\S.*)$/,
  /^test (\S+) has been running for over \d+ seconds$/,
];
const DOTS = /^\.+$/;

type GoTestEvent =
  | { Action: 'start'; Package: string }
  | { Action: 'run' | 'pause' | 'cont'; Package: string; Test: string }
  | { Action: 'pass' | 'fail' | 'skip' | 'bench'; Package: string; Test?: string }
  | { Action: 'output'; Package: string; Output: string; Test?: string };
const isGoTestEvent = typia.createIs<GoTestEvent>();

function goTestEvent(text: string): GoTestEvent | null {
  if (!text.startsWith('{')) return null;
  try {
    const event: unknown = JSON.parse(text);
    return isGoTestEvent(event) ? event : null;
  } catch (error) {
    if (error instanceof SyntaxError) return null;
    throw error;
  }
}

/** A direct Go chain reports tests only through JSON; other commands may also carry text runner reports. */
function onlyGoTests(command: string): boolean {
  let rest = command.trim();
  while (rest.search(GO_TEST) === 0) {
    rest = rest.slice(invocationSpan(rest, GO_TEST).argsEnd).trimStart();
    if (rest === '') return true;
    const separator = /^(?:&&|\|\||;)\s*/.exec(rest);
    if (separator === null) return false;
    rest = rest.slice(separator[0].length);
  }
  return false;
}

/** A stream's unfinished last line, and how many of its dots were already counted. */
interface LineState {
  partial: string;
  countedDots: number;
}

export function createRunProgress(command = ''): RunProgress {
  const goLifecycle = onlyGoTests(command);
  const streams: Record<OutputStream, LineState> = {
    stdout: { partial: '', countedDots: 0 },
    stderr: { partial: '', countedDots: 0 },
  };
  const running = new Set<string>();
  const packages = new Map<string, Set<string>>();

  // One complete line, ANSI stripped, that is not a line of dots: true when it names a finished test.
  const finishedLine = (text: string): boolean => {
    const event = goTestEvent(text);
    if (event !== null) {
      switch (event.Action) {
        case 'start':
          packages.set(event.Package, new Set());
          return false;
        case 'run':
          packages.get(event.Package)?.add(event.Test);
          return false;
        case 'pass':
        case 'fail':
        case 'skip':
        case 'bench':
          if (event.Test === undefined) packages.delete(event.Package);
          else packages.get(event.Package)?.delete(event.Test);
          return true;
        case 'pause':
        case 'cont':
        case 'output':
          return false;
      }
    }
    if (goLifecycle) return false;
    for (const pattern of FINISHED) {
      const name = pattern.exec(text)?.[1];
      if (name !== undefined) {
        running.delete(name);
        return true;
      }
    }
    for (const pattern of RUNNING) {
      const name = pattern.exec(text)?.[1];
      if (name !== undefined) {
        running.add(name);
        return false;
      }
    }
    return false;
  };

  return {
    scan(stream, chunk) {
      const state = streams[stream];
      const lines = (state.partial + chunk).split('\n');
      const rest = lines.pop() ?? '';
      let progressed = false;
      // The first complete line continues the partial one, whose dots were counted as they came.
      let counted = state.countedDots;
      for (const raw of lines) {
        const text = raw.replace(ANSI, '').trimEnd();
        if (DOTS.test(text)) {
          if (!goLifecycle && DOTS.test(text)) {
            progressed ||= text.length > counted;
          }
        } else if (finishedLine(text)) {
          progressed = true;
        }
        counted = 0;
      }
      const tail = rest.replace(ANSI, '');
      const dots = !goLifecycle && DOTS.test(tail) ? tail.length : 0;
      progressed ||= dots > counted;
      state.partial = rest;
      state.countedDots = dots;
      return progressed;
    },
    inFlight: () => [
      ...running,
      ...[...packages].flatMap(([name, tests]) =>
        tests.size === 0 ? [name] : [...tests].map((test) => `${name} ${test}`),
      ),
    ],
  };
}

/**
 * `command` with its one `bun test` asked for a line of dots, one per finished test: the only per-test report bun
 * prints whoever runs it. A command that runs no bun, more than one runner, or already chose bun's dots is returned
 * unchanged.
 */
export function withProgressReport(command: string): string {
  let reported = command;
  for (const match of [...command.matchAll(new RegExp(GO_TEST, 'g'))].reverse()) {
    reported =
      reported.slice(0, match.index) +
      withInvocationFlag(
        reported.slice(match.index),
        GO_TEST,
        /(?<![\w-])-json(?:=(?:true|false))?(?=\s|$)/g,
        '-json',
        true,
      );
  }
  const bun = reported.match(BUN_TEST)?.length ?? 0;
  const nextest = reported.match(NEXTEST_RUN)?.length ?? 0;
  if (bun !== 1 || nextest !== 0 || /(?:^|\s)(?:--dots|--reporter[=\s]dots)(?=\s|$)/.test(reported)) {
    return reported;
  }
  return reported.replace(BUN_TEST, (_match, runtime: string) => `${runtime} test --dots`);
}
