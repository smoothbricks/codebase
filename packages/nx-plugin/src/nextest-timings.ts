import { existsSync, readFileSync } from 'node:fs';
import typia, { type tags } from 'typia';

/**
 * Measured per-test durations that place a sharded crate's slow tests by time rather than by
 * nextest's name hash, relative to the cargo workspace root, beside `.config/nextest.toml`.
 *
 * A hash spreads many cheap tests evenly but drops a few long ones wherever their names fall: one
 * cowshed-core gate measured eight hash shards at 83 to 276 s of test time around a 168 s mean,
 * because the 51 s, 38 s and 18 s tests shared buckets. The manifest names those tests with their
 * milliseconds; the crate's exception shards run them, longest first onto the least-loaded shard,
 * and the ordinary shards hash only what it does not name.
 */
export const NEXTEST_TIMINGS_PATH = '.config/nextest-timings.json';

/** Crate name → nextest test name → milliseconds, as `smoo-nx-nextest-timings` writes it. */
export type NextestTimings = Record<string, Record<string, number & tags.Type<'uint32'>>>;

const validateNextestTimings = typia.createValidate<NextestTimings>();

/**
 * A name `test(=…)` matches literally, so it carries none of the filterset's own syntax: Rust test
 * paths are identifiers joined by `::`. A name outside that cannot be written into a filter without
 * quoting rules nextest does not document, so it is refused where it is declared.
 */
const FILTERABLE_TEST_NAME = /^[A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*$/;

/** The workspace's manifest, or `{}` when it has none: then every sharded crate hashes all its tests. */
export function readNextestTimings(path: string): NextestTimings {
  if (!existsSync(path)) {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(path, 'utf-8'));
  } catch (error) {
    throw new Error(`${path}: not valid JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
  const validated = validateNextestTimings(parsed);
  if (!validated.success) {
    const errors = validated.errors.map(
      (error) => `${error.path.replace(/^\$input\.?/, '')}: expected ${error.expected}`,
    );
    throw new Error(`${path}: not a nextest timing manifest: ${errors.join(', ')}`);
  }
  for (const [crate, tests] of Object.entries(validated.data)) {
    for (const name of Object.keys(tests)) {
      if (!FILTERABLE_TEST_NAME.test(name)) {
        throw new Error(`${path}: ${crate} names ${JSON.stringify(name)}, which is not a Rust test path`);
      }
    }
  }
  return validated.data;
}

/**
 * The timed tests dealt onto `shards` bins by longest-processing-time: longest first, each onto the
 * bin with the least time so far (the lower index on a tie, the name on equal times), so the result
 * is the same for the same manifest. Bin `i` is exception shard `i + 1`.
 */
export function timedShardMembers(tests: Readonly<Record<string, number>>, shards: number): string[][] {
  const bins = Array.from({ length: shards }, () => ({ ms: 0, names: [] as string[] }));
  const longestFirst = Object.entries(tests).sort(([a, x], [b, y]) => y - x || (a < b ? -1 : a > b ? 1 : 0));
  for (const [name, ms] of longestFirst) {
    let least = bins[0];
    for (const bin of bins) {
      if (least === undefined || bin.ms < least.ms) least = bin;
    }
    if (least === undefined) break;
    least.ms += ms;
    least.names.push(name);
  }
  return bins.map((bin) => bin.names.sort());
}

/** Exactly the named tests, as one filterset; `none()` for no name. */
export function namedTestsFilter(names: readonly string[]): string {
  return names.length === 0 ? 'none()' : names.map((name) => `test(=${name})`).join(' or ');
}

/** A test the manifest names: at least this long in the reports a refresh reads. */
export const NEXTEST_TIMED_MIN_MS = 3000;

/**
 * `crate`'s tests of at least `minMs` in `cases`, by name, with their milliseconds. A name two binaries
 * of the crate share is one entry, as its `test(=…)` selects both, and is charged both times.
 */
export function measuredTimings(
  cases: Iterable<{ readonly binary: string; readonly name: string; readonly seconds: number }>,
  crate: string,
  minMs: number,
): Record<string, number> {
  const total = new Map<string, number>();
  for (const { binary, name, seconds } of cases) {
    if (binary !== crate && !binary.startsWith(`${crate}::`)) continue;
    total.set(name, (total.get(name) ?? 0) + Math.round(seconds * 1000));
  }
  const timed: Record<string, number> = {};
  for (const [name, ms] of [...total].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
    if (ms >= minMs) timed[name] = ms;
  }
  return timed;
}
