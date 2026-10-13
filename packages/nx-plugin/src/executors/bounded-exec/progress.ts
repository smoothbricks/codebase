import { BUN_TEST, NEXTEST_RUN } from './verdict.js';

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
 * - bun: a line of dots, one per test finished (`--dots`, which `withProgressReport` asks for: bun prints nothing
 *   per passing test when it believes an agent runs it), or a `(pass)`/`(fail)`/`(skip)`/`(todo)` line. bun names
 *   no test before it finishes, so a bun run names nothing in flight.
 *
 * A command that reports no tests makes progress only by exiting.
 */
export interface RunProgress {
  /** One chunk of one of the command's output streams; true when it reports at least one finished test. */
  scan(stream: OutputStream, chunk: string): boolean;
  /** The tests the runner reported running and has not reported finished, in the order it first named them. */
  inFlight(): string[];
}

export type OutputStream = 'stdout' | 'stderr';

const ANSI = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*[A-Za-z]`, 'g');
/** Lines naming a test that finished; group 1 is its name. */
const FINISHED = [
  /^\s*(?:[A-Z0-9][A-Z0-9-]*\s+)+\[\s*[0-9.]+s\]\s+\(\s*\d+\/\d+\)\s+(\S.*)$/,
  /^test (\S+) \.\.\. (?:ok|FAILED|ignored)\b/,
  /^\((?:pass|fail|skip|todo)\) (.+)$/,
];
/** Lines naming a test still running; group 1 is its name. */
const RUNNING = [
  /^\s*(?:SLOW|START)\s+\[[^\]]*\]\s+(?:\([^)]*\)\s+)?(\S.*)$/,
  /^test (\S+) has been running for over \d+ seconds$/,
];
const DOTS = /^\.+$/;

/** A stream's unfinished last line, and how many of its dots were already counted. */
interface LineState {
  partial: string;
  countedDots: number;
}

export function createRunProgress(): RunProgress {
  const streams: Record<OutputStream, LineState> = {
    stdout: { partial: '', countedDots: 0 },
    stderr: { partial: '', countedDots: 0 },
  };
  const running = new Set<string>();

  // One complete line, ANSI stripped, that is not a line of dots: true when it names a finished test.
  const finishedLine = (text: string): boolean => {
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
          progressed ||= text.length > counted;
        } else if (finishedLine(text)) {
          progressed = true;
        }
        counted = 0;
      }
      const tail = rest.replace(ANSI, '');
      const dots = DOTS.test(tail) ? tail.length : 0;
      progressed ||= dots > counted;
      state.partial = rest;
      state.countedDots = dots;
      return progressed;
    },
    inFlight: () => [...running],
  };
}

/**
 * `command` with its one `bun test` asked for a line of dots, one per finished test: the only per-test report bun
 * prints whoever runs it. A command that runs no bun, more than one runner, or already chose bun's dots is returned
 * unchanged.
 */
export function withProgressReport(command: string): string {
  const bun = command.match(BUN_TEST)?.length ?? 0;
  const nextest = command.match(NEXTEST_RUN)?.length ?? 0;
  if (bun !== 1 || nextest !== 0 || /(?:^|\s)(?:--dots|--reporter[=\s]dots)(?=\s|$)/.test(command)) {
    return command;
  }
  return command.replace(BUN_TEST, (_match, runtime: string) => `${runtime} test --dots`);
}
