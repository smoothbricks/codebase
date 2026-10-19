// A leaf module with no imports: tests' child processes on plain bun load testing.ts, which reads this bound, so
// nothing here may pull in a module that needs the typia transform (bounded-test-policy.ts does).

/**
 * A test target's one bound: the `idleTimeoutMs` with which no test may go unfinished, the hang backstop of
 * specs/cowshed/08_testing.md. It is no wall-clock ceiling: a suite that keeps finishing tests runs as long as it
 * needs. It is the longest any single test may run, so it stays above every slow-timeout class in the plugin's
 * nextest.toml; a bun leg has no per-test deadline of its own (`BUN_TEST_TIMEOUT_FLAG`).
 */
export const BOUNDED_TEST_IDLE_TIMEOUT_MS = 180_000;
export const BOUNDED_TEST_KILL_AFTER_MS = 10_000;
