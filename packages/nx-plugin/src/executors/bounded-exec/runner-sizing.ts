import { BUN_TEST, GO_TEST_RUNNER, NEXTEST_RUN } from './verdict.js';

/**
 * How a runner's parallelism is read from its command and sized to the tokens its CPU pool lends
 * (specs/cowshed/05_gateway.md, "Host scheduling", "Parallelism inside a job").
 *
 * A runner sizes itself to the whole machine unless something sizes it: Nx runs a task per core and
 * each nextest run under it runs a test thread per core again. Before it starts a runner,
 * bounded-exec asks its parent's pool for as many tokens as the runner would run threads or
 * processes at once and sizes the runner to the grant, so the runners of one build tree together
 * stay within that tree's pool.
 */

/** How a runner's parallelism is set, which decides what it asks for and how a grant sizes it. */
export type RunnerKind =
  /** `nextest run`: its test threads, `--test-threads` or `NEXTEST_TEST_THREADS`. */
  | 'nextest'
  /** `bun test --parallel`: its worker processes. */
  | 'bun-parallel'
  /** A runner whose worker count is one flag of its own (`WORKER_FLAGS`). */
  | WorkerFlagKind
  /** `cargo mutants`: its own jobserver's tasks, `--jobserver-tasks`, shared by the `--jobs` cargo processes it starts. */
  | 'cargo-mutants'
  /** A cargo build (`cargo build`/`test`/`clippy`/…, `nextest archive`, `napi build`): its jobs. */
  | 'cargo'
  /** Anything else, one `bun test` process included: one process unless it declares more. */
  | 'process';

/**
 * The inherited ceilings, lowered by configuration, of test threads, which take no make jobserver token: libtest's
 * `RUST_TEST_THREADS` and nextest's `NEXTEST_TEST_THREADS`. Null where neither environment names one.
 */
export interface TestThreadCeilings {
  readonly rust: number | null;
  readonly nextest: number | null;
}

/**
 * What a runner asks its pool for. A cargo build, and a process whose script may start Cargo, carry the test-thread
 * ceilings their sizing keeps; cargo-mutants carries how many cargo processes it runs at once, which share its grant.
 */
export type Demand =
  | { kind: 'cargo'; want: number; testThreads: TestThreadCeilings }
  | { kind: 'process'; want: number; testThreads: TestThreadCeilings }
  | { kind: 'cargo-mutants'; want: number; jobs: number }
  | { kind: 'nextest'; want: number }
  | { kind: 'bun-parallel'; want: number }
  | { kind: 'stryker'; want: number }
  | { kind: 'vitest'; want: number }
  | { kind: 'go-test'; want: number };

/** A runner a leaf reserves for under mbx (mbx-pool.ts, `mbxLeaf`): one that Cargo does not admit through mbx itself. */
export type ReservedDemand = Extract<Demand, { kind: 'nextest' | 'bun-parallel' | 'stryker' | 'process' }>;

/** Every budgeted command learns its grant here, so a script that starts a runner can size it. */
export const CPU_TOKENS_ENV = 'BOUNDED_EXEC_CPU_TOKENS';

/**
 * The cores mbx granted the reservation a leaf runs under: `mbx reserve --cpus N` names at most N to its command, as many
 * as were free once one was.
 */
export const GRANTED_CPUS_ENV = 'MBX_RESERVE_CPUS';

const CARGO_BUILD =
  /(?<=^|[\s;&|()])(?:cargo(?:\s+-\S+)*\s+(?:build|test|check|clippy|rustc|doc|run|nextest\s+archive)|napi\s+build)(?=\s|$)/;
const TEST_THREADS = /--test-threads(?:=|\s+)(\d+)/g;
/** nextest's `--no-capture` (`--nocapture` past `--`): it runs tests serially and ignores `--test-threads`. */
const NO_CAPTURE = /(?<![\w-])--no-?capture(?![\w-])/;
const BUN_PARALLEL = /--parallel(?:=(\d+))?(?=\s|$)/g;
/** `cargo mutants`, cargo's global options allowed between, or the `cargo-mutants` binary itself. */
const CARGO_MUTANTS = /(?<=^|[\s;&|()])(?:cargo(?:\s+-\S+)*|cargo-mutants)\s+mutants(?=\s|$)/;
/** cargo-mutants' own jobserver, one per run, whose tasks every cargo it starts shares: `--jobserver-tasks N`. */
const JOBSERVER_TASKS = /(?<![\w-])--jobserver-tasks(?:=|\s+)(\d+)(?![\w.])/g;
/** cargo-mutants' cargo build/test processes at once: `--jobs N` or `-j N`. */
const MUTANTS_JOBS = /(?<![\w-])(?:--jobs(?:=|\s+)|-j(?:=|\s*))(\d+)(?![\w.])/g;
/** A count, or a share of the cores (`N%`), optionally quoted. Any other word (`"$(nproc)"`) names no count. */
const WORKER_COUNT = /^(["']?)(\d+)(%?)\1$/;
const INHERITED_COUNT = /^[1-9]\d*$/;

type WorkerFlagKind = 'stryker' | 'vitest' | 'go-test';

/**
 * A runner that starts its own workers, one per core or close to it unless one flag of its own names a count or a
 * share of the cores. The grant always sizes it through that flag, which outranks the runner's config file.
 */
interface WorkerFlag {
  /** How the runner is started; the flag counts only in this invocation's own arguments. */
  readonly invocation: RegExp;
  /** The flag with its value, one shell word, captured. */
  readonly flag: RegExp;
  /** The flag naming `count`. */
  readonly sized: (count: string) => string;
  /** The runner's own count when the flag names none, from the cores and the environment the command will see. */
  readonly ownDefault: (cores: number, env: Readonly<NodeJS.ProcessEnv>) => number;
  /** A flag with which the runner runs one worker whatever count it names. */
  readonly serial?: RegExp;
}

/** One shell word: quoted, a `$(…)` substitution, or bare. */
const WORD = String.raw`("[^"]*"|'[^']*'|\$\([^)]*\)|[^\s;&|()'"]+)`;

const WORKER_FLAGS: Readonly<Record<WorkerFlagKind, WorkerFlag>> = {
  /** StrykerJS's `concurrency`: n - 1 workers past four cores, else n. */
  stryker: {
    invocation: /(?<=^|[\s;&|()/])stryker\s+run(?=\s|$)/,
    flag: new RegExp(String.raw`(?<![\w-])(?:--concurrency|-c)(?:=|\s+)${WORD}`, 'g'),
    sized: (count) => `--concurrency=${count}`,
    ownDefault: (cores) => (cores > 4 ? cores - 1 : cores),
  },
  /** Vitest's `maxWorkers`: n - 1 workers outside watch mode, one with file parallelism off. */
  vitest: {
    invocation: /(?<=^|[\s;&|()/])vitest(?:\s+run)?(?=\s|$)/,
    flag: new RegExp(String.raw`(?<![\w-])--max(?:W|-w)orkers(?:=|\s+)${WORD}`, 'g'),
    sized: (count) => `--maxWorkers=${count}`,
    ownDefault: (cores) => Math.max(cores - 1, 1),
    serial: /(?<![\w-])(?:--no-file-parallelism|--file(?:P|-p)arallelism=false)(?![\w-])/,
  },
  /** `go test -p`: packages built and tested at once, `GOMAXPROCS` by default, normally one per core; prebuilt runs alike. */
  'go-test': {
    invocation: GO_TEST_RUNNER,
    flag: new RegExp(String.raw`(?<![\w-])-p(?:=|\s+)${WORD}`, 'g'),
    sized: (count) => `-p=${count}`,
    ownDefault: (cores, env) => parseInheritedTokens(env.GOMAXPROCS) ?? cores,
  },
};

/**
 * What `command` asks for: its kind by invocation, and `declared` (the target's `parallelism`)
 * when it says, else the count the command names (`--test-threads=N`, `--parallel=N`, a worker
 * flag's N or N%, `--jobserver-tasks=N`), else the runner's own default — one thread per core for
 * nextest, a bare `--parallel` and cargo; a worker-flag runner's own (`WORKER_FLAGS`); one per core
 * for cargo-mutants, whose own jobserver defaults to one task per core whatever `--jobs` says; one
 * process for anything else. A nextest run with `--no-capture` runs one test at a time whatever
 * thread count it names, so it asks for one. Positive inherited runner counts are ceilings;
 * configured counts may only lower them.
 */
export function runnerDemand(
  command: string,
  declared: number | undefined,
  cores: number,
  inherited: Readonly<NodeJS.ProcessEnv>,
  configured?: Readonly<NodeJS.ProcessEnv>,
): Demand {
  const kind = runnerKind(command);
  // cargo-mutants' cargo processes at once: `--jobs`, else `CARGO_MUTANTS_JOBS` as the command will see it, else one.
  const jobs =
    kind === 'cargo-mutants'
      ? (namedCounts(invocationArgs(command, CARGO_MUTANTS), MUTANTS_JOBS).at(-1) ??
        parseInheritedTokens(configured?.CARGO_MUTANTS_JOBS ?? inherited.CARGO_MUTANTS_JOBS) ??
        1)
      : 1;
  const requested = declared ?? ownParallelism(kind, command, cores, jobs, { ...inherited, ...configured });
  const ceiling =
    kind === 'nextest'
      ? inheritedCeiling(inherited.NEXTEST_TEST_THREADS, configured?.NEXTEST_TEST_THREADS)
      : kind === 'cargo'
        ? inheritedCeiling(inherited.CARGO_BUILD_JOBS, configured?.CARGO_BUILD_JOBS)
        : null;
  const want = Math.max(1, Math.min(requested, ceiling ?? requested));
  switch (kind) {
    case 'cargo':
    case 'process':
      return {
        kind,
        want,
        testThreads: {
          rust: inheritedCeiling(inherited.RUST_TEST_THREADS, configured?.RUST_TEST_THREADS),
          nextest: inheritedCeiling(inherited.NEXTEST_TEST_THREADS, configured?.NEXTEST_TEST_THREADS),
        },
      };
    case 'cargo-mutants':
      return { kind, want, jobs };
    default:
      return { kind, want };
  }
}

function runnerKind(command: string): RunnerKind {
  if (command.search(NEXTEST_RUN) !== -1) {
    return 'nextest';
  }
  // Stryker before Vitest: its vitest runner is Stryker's workers, not a `vitest` invocation of the command.
  for (const kind of ['stryker', 'vitest', 'go-test'] as const) {
    if (WORKER_FLAGS[kind].invocation.test(command)) {
      return kind;
    }
  }
  if (CARGO_MUTANTS.test(command)) {
    return 'cargo-mutants';
  }
  if (command.search(BUN_TEST) !== -1 && command.search(BUN_PARALLEL) !== -1) {
    return 'bun-parallel';
  }
  return CARGO_BUILD.test(command) ? 'cargo' : 'process';
}

/** What the runner runs at once when the target declares nothing: the count it names, else its own default. */
function ownParallelism(
  kind: RunnerKind,
  command: string,
  cores: number,
  jobs: number,
  env: Readonly<NodeJS.ProcessEnv>,
): number {
  switch (kind) {
    case 'nextest': {
      const named = namedCounts(command, TEST_THREADS);
      return NO_CAPTURE.test(command) ? 1 : named.length > 0 ? Math.max(...named) : cores;
    }
    case 'bun-parallel': {
      const named = namedCounts(command, BUN_PARALLEL);
      return named.length > 0 ? Math.max(...named) : cores;
    }
    case 'stryker':
    case 'vitest':
    case 'go-test': {
      const runner = WORKER_FLAGS[kind];
      const args = invocationArgs(command, runner.invocation);
      if (runner.serial?.test(args)) {
        return 1;
      }
      // The runner takes the last count it is given; a share is of the cores, between one worker and all of them.
      const named = [...args.matchAll(runner.flag)].flatMap((match) => {
        const count = WORKER_COUNT.exec(match[1] ?? '');
        if (count === null) {
          return [];
        }
        const value = Number(count[2]);
        return [count[3] === '%' ? Math.max(1, Math.min(cores, Math.round((cores * value) / 100))) : value];
      });
      return named.at(-1) ?? runner.ownDefault(cores, env);
    }
    case 'cargo-mutants':
      // Its compiles share its own jobserver (one task per core unless named); its `jobs` test runs need a thread each.
      return Math.max(namedCounts(invocationArgs(command, CARGO_MUTANTS), JOBSERVER_TASKS).at(-1) ?? cores, jobs);
    case 'cargo':
      return cores;
    case 'process':
      return 1;
  }
}

function namedCounts(text: string, flag: RegExp): number[] {
  return [...text.matchAll(flag)].flatMap((match) => (match[1] === undefined ? [] : [Number(match[1])]));
}

/**
 * Where `invocation` ends in `command`, and where its own arguments end: the next `;`, `&`, `|`, `(` or `)` outside
 * quotes and `$(…)`.
 */
export function invocationSpan(command: string, invocation: RegExp): { end: number; argsEnd: number } {
  const found = invocation.exec(command);
  const end = found === null ? command.length : found.index + found[0].length;
  let quote: string | null = null;
  let substitutions = 0;
  for (let at = end; at < command.length; at += 1) {
    const char = command[at];
    if (quote !== null) {
      quote = char === quote ? null : quote;
    } else if (char === '"' || char === "'") {
      quote = char;
    } else if (char === '$' && command[at + 1] === '(') {
      substitutions += 1;
      at += 1;
    } else if (char === ')' && substitutions > 0) {
      substitutions -= 1;
    } else if (substitutions === 0 && (char === ';' || char === '&' || char === '|' || char === '(' || char === ')')) {
      return { end, argsEnd: at };
    }
  }
  return { end, argsEnd: command.length };
}

function invocationArgs(command: string, invocation: RegExp): string {
  const { end, argsEnd } = invocationSpan(command, invocation);
  return command.slice(end, argsEnd);
}

/**
 * `command` with every `flag` in `invocation`'s own arguments replaced by `value`; when it names none, `value` is
 * added right after the invocation if `add` says so.
 */
export function withInvocationFlag(
  command: string,
  invocation: RegExp,
  flag: RegExp,
  value: string,
  add: boolean,
): string {
  const { end, argsEnd } = invocationSpan(command, invocation);
  const args = command.slice(end, argsEnd);
  const sizedArgs = args.search(flag) === -1 && add ? ` ${value}${args}` : args.replace(flag, value);
  return `${command.slice(0, end)}${sizedArgs}${command.slice(argsEnd)}`;
}

/**
 * Size the runner to the lesser of its bounded demand and grant, keeping the test-thread ceilings. Rewrite named flags
 * so they cannot outrank that size. `CPU_TOKENS_ENV` still reports the actual held grant.
 *
 * Cargo's rustc count is the pool's: the executor's sub-pool exports its jobserver fifo, so no `CARGO_BUILD_JOBS`
 * comes from the grant (specs/cowshed/05_gateway.md, "Cargo reads its holder's jobserver"). Test threads take no
 * jobserver token, so a cargo build's test threads are its size, and a process's, whose script may start Cargo, are
 * its whole held grant.
 */
export function sizeRunner(
  demand: Demand,
  command: string,
  tokens: number,
): { command: string; env: Record<string, string> } {
  const runnerCount = Math.min(tokens, demand.want);
  const count = String(runnerCount);
  const held = String(tokens);
  switch (demand.kind) {
    case 'nextest':
      return {
        command: command.replace(TEST_THREADS, `--test-threads=${count}`),
        env: { [CPU_TOKENS_ENV]: held, NEXTEST_TEST_THREADS: count },
      };
    case 'bun-parallel':
      return { command: command.replace(BUN_PARALLEL, `--parallel=${count}`), env: { [CPU_TOKENS_ENV]: held } };
    case 'cargo':
    case 'process': {
      const threads = demand.kind === 'cargo' ? runnerCount : tokens;
      const { rust, nextest } = demand.testThreads;
      return {
        command,
        env: {
          [CPU_TOKENS_ENV]: held,
          RUST_TEST_THREADS: String(Math.min(threads, rust ?? threads)),
          NEXTEST_TEST_THREADS: String(Math.min(threads, nextest ?? threads)),
        },
      };
    }
    case 'stryker':
    case 'vitest':
    case 'go-test': {
      const runner = WORKER_FLAGS[demand.kind];
      return {
        command: withInvocationFlag(command, runner.invocation, runner.flag, runner.sized(count), true),
        env: { [CPU_TOKENS_ENV]: held },
      };
    }
    case 'cargo-mutants': {
      // Its own jobserver bounds every compile it starts; each of its `jobs` cargo processes runs tests on its share.
      // `--jobs` is lowered only where it was set: cargo-mutants refuses it beside `--in-place`.
      const jobs = Math.min(demand.jobs, runnerCount);
      const share = String(Math.max(1, Math.floor(runnerCount / jobs)));
      const tasks = withInvocationFlag(command, CARGO_MUTANTS, JOBSERVER_TASKS, `--jobserver-tasks=${count}`, true);
      return {
        command: withInvocationFlag(tasks, CARGO_MUTANTS, MUTANTS_JOBS, `--jobs=${jobs}`, false),
        env: {
          [CPU_TOKENS_ENV]: held,
          ...(demand.jobs > 1 ? { CARGO_MUTANTS_JOBS: String(jobs) } : {}),
          CARGO_BUILD_JOBS: share,
          RUST_TEST_THREADS: share,
          NEXTEST_TEST_THREADS: share,
        },
      };
    }
  }
}

/**
 * The shell statements that run first in a reserved leaf's reservation and size its runner to the grant, which only
 * the command learns as it starts: `CPU_TOKENS_ENV` becomes the cores mbx granted (`GRANTED_CPUS_ENV`), never more
 * than the leaf asked for, its `--cpus` (the runner's own count), which also stands in under an mbx that grants a
 * reservation whole and names none. The test-thread variables a sizing sets follow from it, under their inherited
 * ceilings, outranking the target's own as a numeric grant's do (`sizeRunner`).
 */
export function grantedSizing(demand: ReservedDemand): string {
  const asked = demand.want;
  const named = `\${${GRANTED_CPUS_ENV}:-${asked}}`;
  const tokens = `$${CPU_TOKENS_ENV}`;
  const atMost = (ceiling: number | null): string =>
    ceiling === null ? `"${tokens}"` : `"$(( ${tokens} < ${ceiling} ? ${tokens} : ${ceiling} ))"`;
  const statements = [`export ${CPU_TOKENS_ENV}="$(( ${named} < ${asked} ? ${named} : ${asked} ))"`];
  switch (demand.kind) {
    case 'nextest':
      statements.push(`export NEXTEST_TEST_THREADS="${tokens}"`);
      break;
    case 'process':
      // A process's script may start Cargo, whose test threads take no jobserver token: its whole grant.
      statements.push(
        `export RUST_TEST_THREADS=${atMost(demand.testThreads.rust)}`,
        `export NEXTEST_TEST_THREADS=${atMost(demand.testThreads.nextest)}`,
      );
      break;
    case 'bun-parallel':
    case 'stryker':
      break;
  }
  return statements.map((statement) => `${statement}; `).join('');
}

/**
 * `command` with a reserved runner's named count read from its grant, `CPU_TOKENS_ENV` as `grantedSizing` sets it,
 * where the runner's shell runs it; the runner then runs on all it holds.
 */
export function sizeRunnerToGrant(demand: ReservedDemand, command: string): string {
  const granted = `\${${CPU_TOKENS_ENV}}`;
  switch (demand.kind) {
    case 'nextest':
      return command.replace(TEST_THREADS, `--test-threads=${granted}`);
    case 'bun-parallel':
      return command.replace(BUN_PARALLEL, `--parallel=${granted}`);
    case 'stryker': {
      const runner = WORKER_FLAGS.stryker;
      return withInvocationFlag(command, runner.invocation, runner.flag, runner.sized(granted), true);
    }
    case 'process':
      return command;
  }
}

/** A configured value may lower an inherited ceiling, never raise it. */
function inheritedCeiling(inherited: string | undefined, configured: string | undefined): number | null {
  const left = parseInheritedTokens(inherited);
  const right = parseInheritedTokens(configured);
  return left === null ? right : right === null ? left : Math.min(left, right);
}

/**
 * The count `CPU_TOKENS_ENV` names, as a positive safe integer, or `null` when absent or malformed. It is
 * descriptor input only: a count proves nothing about a live pool and grants nothing. It is a ceiling the
 * runner keeps, and the size an Nx run's own pool takes (`run-cpu-pool.ts`).
 */
export function parseInheritedTokens(value: string | undefined): number | null {
  if (value === undefined || !INHERITED_COUNT.test(value)) {
    return null;
  }
  const tokens = Number(value);
  return Number.isSafeInteger(tokens) ? tokens : null;
}
