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
  /** A Cargo build, nextest archive, or NAPI build (direct or through the managed toolchain entry): its jobs. */
  | 'cargo'
  /** Anything else, one `bun test` process included: one process unless it declares more. */
  | 'process';

/** What a runner asks the budget for. */
export interface Demand {
  kind: RunnerKind;
  /** The threads or processes it runs at once unbudgeted; at least 1. */
  want: number;
}

/** Every budgeted command learns its grant here, so a script that starts a runner can size it. */
export const CPU_TOKENS_ENV = 'BOUNDED_EXEC_CPU_TOKENS';

const CARGO_BUILD =
  /(?<=^|[\s;&|()])(?:cargo(?:\s+-\S+)*\s+(?:build|test|check|clippy|rustc|doc|run|nextest\s+archive)|napi\s+build|(?:\S*\/)?napi-build\.sh)(?=\s|$)/;
const TEST_THREADS = /--test-threads(?:=|\s+)(\d+)/g;
const BUN_PARALLEL = /--parallel(?:=(\d+))?(?=\s|$)/g;

/**
 * What `command` asks for: its kind by invocation, and `declared` (the target's `parallelism`)
 * when it says, else the count the command names (`--test-threads=N`, `--parallel=N`), else the
 * runner's own default — one thread per core for nextest, a bare `--parallel` and cargo, one
 * process for anything else.
 */
export function runnerDemand(command: string, declared: number | undefined, cores: number): Demand {
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
  const want = declared ?? (named.length > 0 ? Math.max(...named) : kind === 'process' ? 1 : cores);
  return { kind, want: Math.max(1, want) };
}

/**
 * `command` and the environment that size a `kind` runner to `tokens`: a count the command names
 * is rewritten, since a flag outranks the environment, and the runner's own variables are set.
 * Every kind gets `CPU_TOKENS_ENV`.
 */
export function sizeRunner(
  kind: RunnerKind,
  command: string,
  tokens: number,
): { command: string; env: Record<string, string> } {
  const count = String(tokens);
  switch (kind) {
    case 'nextest':
      return {
        command: command.replace(TEST_THREADS, `--test-threads=${count}`),
        env: { [CPU_TOKENS_ENV]: count, NEXTEST_TEST_THREADS: count },
      };
    case 'bun-parallel':
      return { command: command.replace(BUN_PARALLEL, `--parallel=${count}`), env: { [CPU_TOKENS_ENV]: count } };
    case 'cargo':
      return { command, env: { [CPU_TOKENS_ENV]: count, CARGO_BUILD_JOBS: count, RUST_TEST_THREADS: count } };
    case 'process':
      return { command, env: { [CPU_TOKENS_ENV]: count } };
  }
}

/** Tokens held until released, or why the runner starts without the budget. */
export type TokenGrant =
  | { granted: true; tokens: number; release: () => void }
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
      return { granted: true, tokens, release: lease.release };
    },
  };
}
