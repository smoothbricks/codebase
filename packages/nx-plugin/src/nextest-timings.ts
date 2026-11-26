import { existsSync, readFileSync } from 'node:fs';
import typia, { type tags } from 'typia';
import { CARGO_TEST_EXCEPTIONS_SUFFIX, cargoTestPackageTargetName } from './cargo-workspace.js';

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
export type NextestTimings = Record<string, Record<string, number>>;

/** The manifest as validated on read: every duration is a uint32 of milliseconds. */
type ValidatedNextestTimings = Record<string, Record<string, number & tags.Type<'uint32'>>>;

const validateNextestTimings = typia.createValidate<ValidatedNextestTimings>();

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

/** A nextest hash partition `hash:index/total` (1-based `index` in `1..total`). */
export interface HashPartition {
  readonly index: number;
  readonly total: number;
}

/**
 * Structured plan of one per-crate test piece before rendering to nextest filterset syntax.
 * Every piece is scoped to `packageName` and excludes `serialFilters` (the workspace's active
 * serial test groups, empty off macOS or when none are declared).
 *
 * - `ordinary`: non-serial tests not pinned to the exception shards (neither `excludeExceptional`
 *   nor `excludeNamed`), hash-partitioned when `partition` is set (unsharded crates omit it).
 * - `exceptional-hash`: non-serial exceptional tests (`exceptional`), hash-partitioned when no
 *   timing manifest names tests for this crate.
 * - `exceptional-timed`: non-serial tests explicitly dealt to this shard (`timedMembers`), plus
 *   any unnamed exceptional tests (`unnamedExceptional` on the last shard when `exceptional` exists).
 */
export type StructuredCargoTestPiece =
  | {
      readonly kind: 'ordinary';
      readonly packageName: string;
      readonly targetName: string;
      readonly partition?: HashPartition;
      readonly excludeExceptional: string | null;
      readonly excludeNamed: readonly string[];
      readonly serialFilters: readonly string[];
    }
  | {
      readonly kind: 'exceptional-hash';
      readonly packageName: string;
      readonly targetName: string;
      readonly shardIndex: number;
      readonly partition: HashPartition;
      readonly exceptional: string;
      readonly serialFilters: readonly string[];
    }
  | {
      readonly kind: 'exceptional-timed';
      readonly packageName: string;
      readonly targetName: string;
      readonly shardIndex: number;
      readonly timedMembers: readonly string[];
      readonly unnamedExceptional: { readonly exceptional: string; readonly allNamed: readonly string[] } | null;
      readonly serialFilters: readonly string[];
    };

/** Rendered per-crate piece handed to target construction: its target name, `-E` filterset and optional `--partition`. */
export interface CargoTestPiece {
  readonly partition?: string;
  readonly selector: string;
  readonly targetName: string;
}

/**
 * Plan the per-crate test pieces for `pkgName`.
 *
 * Unsharded crates (`testShards <= 1`) receive one ordinary piece covering all non-serial tests.
 * Sharded crates (`testShards > 1`) lift `exceptional` overrides and `timings`-named slow tests
 * out of the ordinary hash shards into `exceptions-shard1..N`: by time when `timings` names any
 * test (with the last shard also taking every unnamed exceptional test), or by hash otherwise.
 * `specs/cowshed/laws/nextest_pieces.bend` proves that these pieces plus the active serial group
 * runners cover every test of the package exactly once.
 */
export function cargoTestPieces(
  pkgName: string,
  testShards: number,
  exceptional: string | null,
  serialFilters: readonly string[],
  timings: Readonly<Record<string, number>>,
): StructuredCargoTestPiece[] {
  const pieces: StructuredCargoTestPiece[] = [];
  const sharded = testShards > 1;
  const timed = sharded ? timings : {};
  const named = Object.keys(timed);
  const exceptionalHere = sharded ? exceptional : null;
  for (let index = 1; index <= testShards; index += 1) {
    const targetName = cargoTestPackageTargetName(pkgName, sharded ? `shard${index}` : undefined);
    pieces.push({
      kind: 'ordinary',
      packageName: pkgName,
      targetName,
      ...(sharded ? { partition: { index, total: testShards } } : {}),
      excludeExceptional: exceptionalHere,
      excludeNamed: named,
      serialFilters,
    });
  }
  if (exceptionalHere !== null || named.length > 0) {
    // Measured tests are dealt by time; the last shard also takes every exceptional test the
    // manifest does not name, so the exception shards stay an exact cover of the pin.
    const members = named.length === 0 ? null : timedShardMembers(timed, testShards);
    for (let index = 1; index <= testShards; index += 1) {
      const targetName = cargoTestPackageTargetName(pkgName, `${CARGO_TEST_EXCEPTIONS_SUFFIX}-shard${index}`);
      const own = members?.[index - 1];
      if (own === undefined) {
        if (exceptionalHere === null) continue;
        pieces.push({
          kind: 'exceptional-hash',
          packageName: pkgName,
          targetName,
          shardIndex: index,
          partition: { index, total: testShards },
          exceptional: exceptionalHere,
          serialFilters,
        });
      } else {
        pieces.push({
          kind: 'exceptional-timed',
          packageName: pkgName,
          targetName,
          shardIndex: index,
          timedMembers: own,
          unnamedExceptional:
            exceptionalHere !== null && index === testShards ? { exceptional: exceptionalHere, allNamed: named } : null,
          serialFilters,
        });
      }
    }
  }
  return pieces;
}

/**
 * Render a structured piece to the exact nextest `-E` filterset string and optional `--partition`
 * flag that go into the inferred Nx target (and therefore its Nx cache key).
 */
export function renderCargoTestPiece(piece: StructuredCargoTestPiece): CargoTestPiece {
  const serialExclusion = piece.serialFilters.length === 0 ? null : piece.serialFilters.join(' or ');
  const notSerial = serialExclusion === null ? [] : [`not (${serialExclusion})`];
  switch (piece.kind) {
    case 'ordinary': {
      // The tests the hash does not place: nextest.toml's exceptional class and the measured slow tests.
      const pin =
        piece.excludeNamed.length === 0
          ? piece.excludeExceptional
          : piece.excludeExceptional === null
            ? namedTestsFilter(piece.excludeNamed)
            : `(${piece.excludeExceptional}) or (${namedTestsFilter(piece.excludeNamed)})`;
      return {
        ...(piece.partition === undefined
          ? {}
          : { partition: `hash:${piece.partition.index}/${piece.partition.total}` }),
        selector: [`package(${piece.packageName})`, ...(pin === null ? [] : [`not (${pin})`]), ...notSerial].join(
          ' and ',
        ),
        targetName: piece.targetName,
      };
    }
    case 'exceptional-hash':
      return {
        partition: `hash:${piece.partition.index}/${piece.partition.total}`,
        selector: [`package(${piece.packageName}) and (${piece.exceptional})`, ...notSerial].join(' and '),
        targetName: piece.targetName,
      };
    case 'exceptional-timed': {
      const unnamed =
        piece.unnamedExceptional === null
          ? ''
          : ` or ((${piece.unnamedExceptional.exceptional}) and not (${namedTestsFilter(piece.unnamedExceptional.allNamed)}))`;
      return {
        selector: [
          `package(${piece.packageName}) and ((${namedTestsFilter(piece.timedMembers)})${unnamed})`,
          ...notSerial,
        ].join(' and '),
        targetName: piece.targetName,
      };
    }
  }
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
