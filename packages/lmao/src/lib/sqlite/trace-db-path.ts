/**
 * Where a file-backed trace sink lives.
 *
 * The directory name is load-bearing, not cosmetic. The sink is written by
 * several test-worker processes at once while a TypeScript transform plugin
 * may be compiling the same package, and such plugins snapshot, diff, and
 * `fs.watch` every directory under a project root. A SQLite database mutates
 * its directory's membership — `-journal`, `-wal`, and `-shm` sidecars appear
 * and vanish around transactions and connections — so a sink sitting in a
 * walked directory makes an unrelated compile non-reproducible.
 *
 * `.cache` is the one name that resolves this by construction: it is on
 * @ttsc/unplugin's hardcoded ignore list, so the walk never descends into it,
 * never signs it, and never opens a watch on it. It is also this workspace's
 * established location for regenerable tool state (`.cache/ttsc`), it is
 * already gitignored tree-wide, and the monorepo's package and cargo policy
 * scans already skip it.
 *
 * `tmp` is on the same ignore list and was rejected: its contract is "safe to
 * delete at any moment", and this file must survive the run that wrote it so
 * assertions and post-mortems can read it back.
 *
 * The filename carries no leading dot. Hiding the sink only mattered while it
 * sat in a package root next to source.
 *
 * @module sqlite/trace-db-path
 */

/** Directory, relative to a package or workspace root, that holds the trace sink. */
export const TRACE_DB_DIRECTORY = '.cache';

/** Trace sink filename within {@link TRACE_DB_DIRECTORY}. */
export const TRACE_DB_FILENAME = 'trace-results.db';

/**
 * Sink path used whenever a caller configures SQLite output without naming
 * one, resolved against the process working directory.
 */
export const DEFAULT_TRACE_DB_PATH = `${TRACE_DB_DIRECTORY}/${TRACE_DB_FILENAME}`;
