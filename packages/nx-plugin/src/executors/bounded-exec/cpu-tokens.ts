import { GATEWAY_SOCKET, holdLease, type UnleasedCause } from './gateway-lease.js';
import { BUN_TEST, NEXTEST_RUN } from './verdict.js';

/**
 * The client half of cowshed's host CPU budget (specs/cowshed/05_gateway.md, "Host CPU budget").
 *
 * Every gate on a host sizes its runners to the whole machine: Nx runs a task per core and each
 * nextest run under it runs a test thread per core again, so a few concurrent gates keep the load
 * average several times the core count and tests that do no disk work time out waiting for a CPU.
 * The cowshed gateway holds one budget of CPU tokens for the host, about one per core. Before it
 * starts a runner, bounded-exec asks for as many tokens as the runner would run threads or
 * processes at once, and sizes the runner to the grant: every gate keeps running in parallel, and
 * the total stays near the core count.
 */

/** How a runner's parallelism is set, which decides what it asks for and how a grant sizes it. */
export type RunnerKind =
  /** `nextest run`: its test threads, `--test-threads` or `NEXTEST_TEST_THREADS`. */
  | 'nextest'
  /** `bun test --parallel`: its worker processes. */
  | 'bun-parallel'
  /** A cargo build (`cargo build`/`test`/`clippy`/…, `nextest archive`, `napi build`): its jobs. */
  | 'cargo'
  /** Anything else, one `bun test` process included: one process unless it declares more. */
  | 'process';

/** What a runner asks the budget for; Cargo also carries its independent test-thread ceiling. */
export type Demand =
  | { kind: 'cargo'; want: number; testThreads: number | null }
  | { kind: Exclude<RunnerKind, 'cargo'>; want: number };

/** Every budgeted command learns its grant here, so a script that starts a runner can size it. */
export const CPU_TOKENS_ENV = 'BOUNDED_EXEC_CPU_TOKENS';

const CARGO_BUILD =
  /(?<=^|[\s;&|()])(?:cargo(?:\s+-\S+)*\s+(?:build|test|check|clippy|rustc|doc|run|nextest\s+archive)|napi\s+build)(?=\s|$)/;
const TEST_THREADS = /--test-threads(?:=|\s+)(\d+)/g;
const BUN_PARALLEL = /--parallel(?:=(\d+))?(?=\s|$)/g;
const INHERITED_COUNT = /^[1-9]\d*$/;

/**
 * What `command` asks for: its kind by invocation, and `declared` (the target's `parallelism`)
 * when it says, else the count the command names (`--test-threads=N`, `--parallel=N`), else the
 * runner's own default — one thread per core for nextest, a bare `--parallel` and cargo, one
 * process for anything else. Positive inherited runner counts are ceilings; configured counts may only lower them.
 */
export function runnerDemand(
  command: string,
  declared: number | undefined,
  cores: number,
  inherited: Readonly<NodeJS.ProcessEnv>,
  configured?: Readonly<NodeJS.ProcessEnv>,
): Demand {
  const kind: RunnerKind =
    command.search(NEXTEST_RUN) !== -1
      ? 'nextest'
      : command.search(BUN_TEST) !== -1 && command.search(BUN_PARALLEL) !== -1
        ? 'bun-parallel'
        : command.search(CARGO_BUILD) !== -1
          ? 'cargo'
          : 'process';
  const counted = kind === 'nextest' ? TEST_THREADS : kind === 'bun-parallel' ? BUN_PARALLEL : null;
  const named =
    counted === null
      ? []
      : [...command.matchAll(counted)].flatMap((match) => (match[1] === undefined ? [] : [Number(match[1])]));
  const requested = declared ?? (named.length > 0 ? Math.max(...named) : kind === 'process' ? 1 : cores);
  const ceiling =
    kind === 'nextest'
      ? inheritedCeiling(inherited.NEXTEST_TEST_THREADS, configured?.NEXTEST_TEST_THREADS)
      : kind === 'cargo'
        ? inheritedCeiling(inherited.CARGO_BUILD_JOBS, configured?.CARGO_BUILD_JOBS)
        : null;
  const want = Math.max(1, Math.min(requested, ceiling ?? requested));
  return kind === 'cargo'
    ? { kind, want, testThreads: inheritedCeiling(inherited.RUST_TEST_THREADS, configured?.RUST_TEST_THREADS) }
    : { kind, want };
}

/**
 * Size the runner to the lesser of its bounded demand and grant, retaining Cargo's test-thread ceiling.
 * Rewrite named flags so they cannot outrank that ceiling. `CPU_TOKENS_ENV` still reports the actual held grant.
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
      return {
        command,
        env: {
          [CPU_TOKENS_ENV]: held,
          CARGO_BUILD_JOBS: count,
          RUST_TEST_THREADS: String(Math.min(runnerCount, demand.testThreads ?? runnerCount)),
        },
      };
    case 'process':
      return { command, env: { [CPU_TOKENS_ENV]: held } };
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
 * descriptor input only: a count proves nothing about a live pool and grants nothing. A missing or malformed
 * descriptor follows normal host admission; a valid one still needs a grant from its endpoint. Connection
 * absence follows the host path, while a connected parent's refusal fails admission to avoid hold-and-wait.
 */
export function parseInheritedTokens(value: string | undefined): number | null {
  if (value === undefined || !INHERITED_COUNT.test(value)) {
    return null;
  }
  const tokens = Number(value);
  return Number.isSafeInteger(tokens) ? tokens : null;
}

/**
 * Tokens held until released, or why the runner starts without the budget. `closed` resolves when
 * the grant's connection closes, however it closes, so a sub-pool built on the grant ends with it.
 * `inherited` marks tokens a parent's CPU pool lent, not tokens the host gateway granted.
 */
export type TokenGrant =
  | { granted: true; tokens: number; release: () => void; closed: Promise<void>; inherited?: true }
  | { granted: false; cause: UnleasedCause; reason: string };

/** Where bounded-exec takes its tokens; the gateway in production, a fake in tests. */
export interface CpuBudget {
  take(want: number, checkout: string, command: string): Promise<TokenGrant>;
}

/**
 * The gateway at `socket`'s budget. A grant has no deadline: the wait is how the budget works, and
 * the gateway answers `queued` at once, so a client knows it is alive. `checkout` is the Nx
 * workspace root: the gateway shares tokens fairly between checkouts.
 */
export function gatewayCpuBudget(socket: string = GATEWAY_SOCKET): CpuBudget {
  return {
    async take(want, checkout, command) {
      const lease = await holdLease(socket, { op: 'cpu-tokens', want, checkout, command }, 'the CPU budget', {
        ackMs: 2_000,
        afterCloseMs: 5_000,
        grantMs: null,
      });
      if (!lease.granted) {
        return lease;
      }
      const tokens = lease.answer.tokens;
      if (typeof tokens !== 'number' || !Number.isInteger(tokens) || tokens < 1 || tokens > want) {
        lease.release();
        return {
          granted: false,
          cause: 'other',
          reason: `the gateway granted ${JSON.stringify(tokens)} tokens for ${want}`,
        };
      }
      return { granted: true, tokens, release: lease.release, closed: lease.closed };
    },
  };
}
