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
 * compile non-reproducible. Every project walker and file watcher skips
 * `node_modules`.
 *
 * It is per-checkout tool state, and belongs where such state is kept.
 * `node_modules/.cache/<tool>` of the package a tool runs in is the JavaScript
 * convention for exactly that (find-cache-dir: babel, webpack, ava, stryker): a
 * directory rewritten on every run, rebuilt when missing, never source and
 * never a build output. A workspace manager that keeps build state off the
 * source tree (cowshed's build volumes) recognises that convention without
 * knowing this library, so the sink's page-by-page rewrites of a database that
 * reaches hundreds of megabytes never land in a source image that clones share.
 * A package-level `.cache/` does not qualify: build tools declare their outputs
 * there (`.cache/<tool>/`), and an output must stay with the source.
 *
 * `tmp` was rejected: its contract is "safe to delete at any moment", and this
 * file must survive the run that wrote it so assertions and post-mortems can
 * read it back.
 *
 * @module sqlite/trace-db-path
 */

/** Directory, relative to a package or workspace root, that holds the trace sink. */
export const TRACE_DB_DIRECTORY = 'node_modules/.cache/lmao';

/** Trace sink filename within {@link TRACE_DB_DIRECTORY}. */
export const TRACE_DB_FILENAME = 'trace-results.db';

/**
 * Sink path used whenever a caller configures SQLite output without naming
 * one, resolved against the process working directory.
 */
export const DEFAULT_TRACE_DB_PATH = `${TRACE_DB_DIRECTORY}/${TRACE_DB_FILENAME}`;
