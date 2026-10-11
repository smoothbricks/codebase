import { BOUNDED_TEST_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { isRecord } from '../../type-guards.js';
import boundedExecExecutor, {
  type BoundedExecResult,
  type NxTaskContext,
  recordContext,
  runDefaultBoundedExec,
} from '../bounded-exec/executor.js';
import { clearTaskDirectory } from '../bounded-exec/verdict.js';
import type { NextestGroupOptions } from './schema.js';

/**
 * The per-test deadline the group's bound is sized from. The group runs its tests one at a time, so
 * `selected tests × this` bounds the whole run when every test stays within it. A test that
 * nextest.toml gives a longer `slow-timeout` can exceed it; this constant does not hide that.
 */
export const NEXTEST_GROUP_PER_TEST_MS = 30_000;

/**
 * The longest delay a Node timer can hold. A longer one fires at once, so a budget above it is
 * refused rather than clamped: a clamped bound would kill a run that the count says is in budget.
 */
export const MAX_TIMER_MS = 2_147_483_647;

/**
 * Lists the group's selection, sizes the bound from it, and runs the group under the existing
 * bounded-exec runner, so the kill, lease and report mechanics stay in that one owner.
 *
 * The count stage records no Nx verdict and echoes nothing: its stdout is the listing and nothing
 * else. Any refusal before the run clears this task's previous verdict and report first, so no
 * earlier run's result can stand for this one. A listing that fails, does not parse, selects nothing,
 * or sizes a budget no timer can hold is refused rather than run unbounded.
 */
export default async function nextestGroupExecutor(
  options: NextestGroupOptions,
  context: NxTaskContext,
): Promise<BoundedExecResult> {
  const chunks: string[] = [];
  const listed = await runDefaultBoundedExec(
    {
      command: options.listCommand,
      cwd: options.cwd,
      timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
      killAfterMs: options.killAfterMs,
      parallelism: 1,
    },
    { root: context.root, stdout: (chunk: string) => chunks.push(chunk) },
  );
  if (!listed.success) {
    return refuse(
      context,
      `${listed.terminalOutput}nextest-group: count stage failed; refusing to run without a bound\n`,
    );
  }
  const selected = selectedRunnableTestCount(chunks.join(''));
  if (!selected.ok) {
    return refuse(context, `nextest-group: ${selected.error}; refusing to run without a bound\n`);
  }
  const budget = groupBudgetMs(selected.count);
  if (!budget.ok) {
    return refuse(context, `nextest-group: ${budget.error}; refusing to run unbounded\n`);
  }
  process.stdout.write(`nextest-group: ${selected.count} selected tests, bounded at ${budget.timeoutMs}ms\n`);
  // Derived groups have max-threads=1: reserve one runnable test process, not an idle host-wide grant.
  return boundedExecExecutor(
    {
      command: options.command,
      cwd: options.cwd,
      timeoutMs: budget.timeoutMs,
      killAfterMs: options.killAfterMs,
      parallelism: 1,
    },
    context,
  );
}

/** Refuses before the run starts: this task's previous verdict and report go, and the refusal is the only record. */
async function refuse(context: NxTaskContext, message: string): Promise<BoundedExecResult> {
  const directory = recordContext(context).record?.directory;
  if (directory !== undefined) {
    await clearTaskDirectory(directory);
  }
  process.stderr.write(message);
  return { success: false, terminalOutput: message };
}

export type GroupBudget = { ok: true; timeoutMs: number } | { ok: false; error: string };

/** The run's bound: its selected tests at the per-test share, refused when no timer can hold it. */
export function groupBudgetMs(count: number): GroupBudget {
  const timeoutMs = count * NEXTEST_GROUP_PER_TEST_MS;
  return timeoutMs <= MAX_TIMER_MS
    ? { ok: true, timeoutMs }
    : {
        ok: false,
        error: `${count} selected tests need ${timeoutMs}ms, longer than a timer can hold (${MAX_TIMER_MS}ms)`,
      };
}

export type SelectedRunnableTests = { ok: true; count: number } | { ok: false; error: string };

/**
 * How many tests a `nextest list --message-format json` listing would run: the matching, non-ignored
 * ones, by nextest's own `filter-match` and `ignored` fields. Ignored tests stay their explicit lane's.
 */
export function selectedRunnableTestCount(listing: string): SelectedRunnableTests {
  let parsed: unknown;
  try {
    parsed = JSON.parse(listing);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    return { ok: false, error: `nextest list printed no JSON listing (${reason})` };
  }
  const suites = isRecord(parsed) ? parsed['rust-suites'] : undefined;
  if (!isRecord(suites)) {
    return { ok: false, error: 'nextest list JSON has no rust-suites object' };
  }
  let count = 0;
  for (const suite of Object.values(suites)) {
    const testcases = isRecord(suite) ? suite.testcases : undefined;
    if (!isRecord(testcases)) {
      return { ok: false, error: 'a nextest list suite has no testcases object' };
    }
    for (const testcase of Object.values(testcases)) {
      const match = isRecord(testcase) ? testcase['filter-match'] : undefined;
      if (
        !isRecord(testcase) ||
        typeof testcase.ignored !== 'boolean' ||
        !isRecord(match) ||
        (match.status !== 'matches' && match.status !== 'mismatch')
      ) {
        return { ok: false, error: 'a nextest list testcase has no ignored flag or filter-match status' };
      }
      if (match.status === 'matches' && !testcase.ignored) {
        count += 1;
      }
    }
  }
  return count === 0 ? { ok: false, error: 'nextest list selects no runnable test' } : { ok: true, count };
}
