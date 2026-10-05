/**
 * Where a file-backed trace sink lives.
 *
 * The directory is load-bearing, not cosmetic, for two reasons.
 *
 * Walkers must never see it. The sink is written by several test-worker
 * processes at once while a TypeScript transform plugin may be compiling the
 * same package, and such plugins snapshot, diff, and `fs.watch` every directory
 * under a project root. A SQLite database mutates its directory's membership —
 * `-journal`, `-wal`, and `-shm` sidecars appear and vanish around transactions
 * and connections — so a sink sitting in a walked directory makes an unrelated
 * compile non-reproducible. `.cache` resolves this by construction: it is on
 * @ttsc/unplugin's hardcoded ignore list, so the walk never descends into it,
 * never signs it, and never opens a watch on it. It is also gitignored
 * tree-wide, and the monorepo's package and cargo policy scans skip it.
 *
 * It is per-checkout tool state, never source and never a build output, so it
 * has a directory of its own, `.cache/lmao`, that nothing else writes. A
 * workspace manager that keeps build state off the source tree can then hold
 * exactly that directory of every package (cowshed: a `.cowshed.toml`
 * `[build] state` pattern over the packages), so the sink's page-by-page
 * rewrites of a database that reaches hundreds of megabytes never land in a
 * source image that clones share. `.cache/` itself is not that directory:
 * build tools declare their Nx outputs inside it (`.cache/<tool>/`), and an
 * output must stay with the source. `node_modules/.cache/lmao` was rejected: a
 * package the package manager installed nothing into has no `node_modules`,
 * and the sink creating one makes the package look installed.
 *
 * `tmp` was rejected: its contract is "safe to delete at any moment", and this
 * file must survive the run that wrote it so assertions and post-mortems can
 * read it back.
 *
 * @module sqlite/trace-db-path
 */

/** Directory, relative to a package or workspace root, that holds the trace sink. */
export const TRACE_DB_DIRECTORY = '.cache/lmao';

/** Trace sink filename within {@link TRACE_DB_DIRECTORY}. */
export const TRACE_DB_FILENAME = 'trace-results.db';

/**
 * Sink path used whenever a caller configures SQLite output without naming
 * one, resolved against the process working directory.
 */
export const DEFAULT_TRACE_DB_PATH = `${TRACE_DB_DIRECTORY}/${TRACE_DB_FILENAME}`;
