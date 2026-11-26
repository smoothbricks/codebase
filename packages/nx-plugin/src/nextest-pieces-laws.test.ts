// Exact-cover law for per-crate nextest pieces, bound to its model (specs/cowshed/laws/nextest_pieces.bend;
// packages/nx-plugin/README.md "Cargo Workspace Target Inference"). The model is proven. This driver runs the emitted
// model's corpora and seeded draws (`__tests__/nextest-pieces-model.ts`, emitted by bend-emit and never hand-edited)
// and binds it directly to `cargoTestPieces` in `src/nextest-timings.ts`: every drawn configuration's structured TS
// pieces (plus the serial runner when serial groups are active) are mapped into model `Piece` terms and checked with
// `model.count_selected === 1`.
import { describe, expect, it } from 'bun:test';
import * as model from './__tests__/nextest-pieces-model.js';
import { cargoTestPieces, type StructuredCargoTestPiece } from './nextest-timings.js';

function list<T>(items: readonly T[]): model.List<T> {
  return [...items].reverse().reduce<model.List<T>>((tail, head) => ({ $: 'Con', head, tail }), model.NIL);
}

function draws<A>(seed: bigint, count: number, draw: (rng: model.Rng) => A): A[] {
  const rng = new model.Rng(seed);
  return Array.from({ length: count }, () => draw(rng));
}

function failures<A extends unknown[]>(
  name: string,
  rows: readonly A[],
  law: (...args: A) => [boolean, string],
): string[] {
  return rows.flatMap((row, index) => {
    const [holds, shown] = law(...row);
    return holds ? [] : [`${name} ${index}: ${shown}`];
  });
}

/** Map one structured production piece into its exact model `Piece` term. */
function toModelPiece(piece: StructuredCargoTestPiece): model.Piece {
  const excl_ser = piece.serialFilters.length > 0;
  switch (piece.kind) {
    case 'ordinary':
      return piece.partition === undefined
        ? { $: 'OrdUnsharded', excl_ser }
        : {
            $: 'OrdShard',
            idx: piece.partition.index,
            excl_exc: piece.excludeExceptional !== null,
            excl_named: piece.excludeNamed.length > 0,
            excl_ser,
          };
    case 'exceptional-hash':
      return { $: 'ExcHash', idx: piece.partition.index, excl_ser };
    case 'exceptional-timed':
      return {
        $: 'ExcTimed',
        idx: piece.shardIndex,
        take_unnamed_exc: piece.unnamedExceptional !== null,
        excl_ser,
      };
  }
}

/** Map the package's TS pieces plus the workspace serial runner (when active) into the model's piece list. */
function toModelPlan(
  pieces: readonly StructuredCargoTestPiece[],
  serialFilters: readonly string[],
): model.List<model.Piece> {
  const mapped: model.Piece[] = pieces.map(toModelPiece);
  if (serialFilters.length > 0) mapped.push({ $: 'SerialRunner' });
  return list(mapped);
}

//#region smoo/cowshed!n/native-test-verdicts
describe('the nextest-pieces model, on its enumerated corpus and seeded draws', () => {
  const len = model.PROP_LEN_PIECES_COVER_ONCE;

  it('proves pieces_cover_once, unsharded_covers_once, and non_macos_covers_once with non-vacuous coverage', () => {
    expect([
      ...failures(
        'pieces_cover_once',
        [
          ...model.corpus_pieces_cover_once(),
          ...draws<Parameters<typeof model.law_pieces_cover_once>>(
            model.SEED_PIECES_COVER_ONCE,
            model.PROPS_PIECES_COVER_ONCE,
            (rng) => [
              model.gen_Nat(rng, len),
              model.gen_Bool(rng, len),
              model.gen_Bool(rng, len),
              model.gen_Bool(rng, len),
              model.gen_Test(rng, len),
            ],
          ),
        ],
        model.law_pieces_cover_once,
      ),
      ...failures(
        'unsharded_covers_once',
        [
          ...model.corpus_unsharded_covers_once(),
          ...draws<Parameters<typeof model.law_unsharded_covers_once>>(
            model.SEED_UNSHARDED_COVERS_ONCE,
            model.PROPS_UNSHARDED_COVERS_ONCE,
            (rng) => [
              model.gen_Bool(rng, len),
              model.gen_Bool(rng, len),
              model.gen_Bool(rng, len),
              model.gen_Test(rng, len),
            ],
          ),
        ],
        model.law_unsharded_covers_once,
      ),
      ...failures(
        'non_macos_covers_once',
        [
          ...model.corpus_non_macos_covers_once(),
          ...draws<Parameters<typeof model.law_non_macos_covers_once>>(
            model.SEED_NON_MACOS_COVERS_ONCE,
            model.PROPS_NON_MACOS_COVERS_ONCE,
            (rng) => [
              model.gen_Nat(rng, len),
              model.gen_Bool(rng, len),
              model.gen_Bool(rng, len),
              model.gen_Test(rng, len),
            ],
          ),
        ],
        model.law_non_macos_covers_once,
      ),
    ]).toEqual([]);

    for (const stats of [
      model.check_pieces_cover_once_definedness(),
      model.check_unsharded_covers_once_definedness(),
      model.check_non_macos_covers_once_definedness(),
    ]) {
      expect(stats.passed).toBe(true);
      expect(stats.isVacuous).toBe(false);
    }
  });

  it('holds the definedness companions showing broken plans drop or double-run tests', () => {
    expect([
      ...failures(
        'dropping_unnamed_exc_loses_test',
        [
          ...model.corpus_dropping_unnamed_exc_loses_test(),
          ...draws<Parameters<typeof model.law_dropping_unnamed_exc_loses_test>>(
            model.SEED_DROPPING_UNNAMED_EXC_LOSES_TEST,
            model.PROPS_DROPPING_UNNAMED_EXC_LOSES_TEST,
            (rng) => [model.gen_Bool(rng, len)],
          ),
        ],
        model.law_dropping_unnamed_exc_loses_test,
      ),
      ...failures(
        'missing_serial_exclusion_double_runs',
        [
          ...model.corpus_missing_serial_exclusion_double_runs(),
          ...draws<Parameters<typeof model.law_missing_serial_exclusion_double_runs>>(
            model.SEED_MISSING_SERIAL_EXCLUSION_DOUBLE_RUNS,
            model.PROPS_MISSING_SERIAL_EXCLUSION_DOUBLE_RUNS,
            (rng) => [model.gen_Bool(rng, len)],
          ),
        ],
        model.law_missing_serial_exclusion_double_runs,
      ),
    ]).toEqual([]);

    for (const stats of [
      model.check_dropping_unnamed_exc_loses_test_definedness(),
      model.check_missing_serial_exclusion_double_runs_definedness(),
    ]) {
      expect(stats.passed).toBe(true);
      expect(stats.isVacuous).toBe(false);
    }
  });
});

describe('cargoTestPieces bound to specs/cowshed/laws/nextest_pieces.bend', () => {
  it('covers every test exactly once across drawn shard counts, overrides, timings, and serial configs', () => {
    const rng = new model.Rng(0x5a00_b31c_1001n);
    const nextInt = (maxExclusive: number): number => Number(rng.next() % BigInt(maxExclusive));
    const violations: string[] = [];

    for (let trial = 0; trial < 150; trial += 1) {
      const shards = 1 + nextInt(6); // 1..6 (includes unsharded=1 and sharded=2..6)
      const hasExc = nextInt(2) === 1;
      const timingCount = shards === 1 ? 0 : nextInt(8); // 0..7 named slow tests
      const serialCount = nextInt(3); // 0 (non-macOS / no groups), 1, or 2 serial groups

      const exceptional = hasExc ? 'test(/compile_fail|fail_to_compile/)' : null;
      const serialFilters = Array.from({ length: serialCount }, (_, i) => `(test(/serial_${i}_/))`);
      const timings: Record<string, number> = {};
      for (let i = 0; i < timingCount; i += 1) {
        timings[`slow::t_${i}`] = 3000 + nextInt(20_000);
      }

      const pieces = cargoTestPieces('bound-crate', shards, exceptional, serialFilters, timings);

      // Verify the timed-dealing hypothesis directly on production pieces: every named test is
      // placed in exactly one exceptional-timed piece's `timedMembers` with shardIndex in 1..shards.
      const dealtShardByName = new Map<string, number>();
      if (timingCount > 0) {
        for (const piece of pieces) {
          if (piece.kind !== 'exceptional-timed') continue;
          for (const name of piece.timedMembers) {
            if (dealtShardByName.has(name)) {
              violations.push(`trial ${trial}: named test ${name} dealt to more than one shard`);
            }
            dealtShardByName.set(name, piece.shardIndex);
          }
        }
        for (const name of Object.keys(timings)) {
          const shard = dealtShardByName.get(name);
          if (shard === undefined || shard < 1 || shard > shards) {
            violations.push(`trial ${trial}: named test ${name} dealt to invalid shard ${String(shard)}`);
          }
        }
      }

      const modelPieces = toModelPlan(pieces, serialFilters);
      const dealtValues: model.Dealt[] = [
        { $: 'NotNamed' },
        ...[...new Set(dealtShardByName.values())].map((k): model.Dealt => ({ $: 'ToShard', k })),
      ];

      // Exhaustively check every combination of (exc, dealt, ser, bucket in 1..shards) on this TS plan.
      for (const exc of [false, true]) {
        for (const dealt of dealtValues) {
          for (const ser of [false, true]) {
            for (let bucket = 1; bucket <= shards; bucket += 1) {
              const test: model.Test = { $: 'Test', exc, dealt, ser, bucket };
              const count = model.count_selected(modelPieces, test);
              if (count !== 1) {
                violations.push(
                  `trial ${trial} (shards=${shards}, exc=${String(hasExc)}, timed=${timingCount}, serial=${serialCount}): ` +
                    `${model.show_Test(test)} selected ${count} times`,
                );
              }
            }
          }
        }
      }
    }

    expect(violations).toEqual([]);
  });

  it('catches planted defects in the TS pieces: the binding is not vacuous', () => {
    const exceptional = 'test(/compile_fail/)';
    const serial = ['(test(/serial_probe_/))'];
    const base = cargoTestPieces('probe', 3, exceptional, serial, { 'slow::a': 9000 });

    // Defect 1: dropping `unnamedExceptional` from the last timed exception shard drops unnamed exceptional tests.
    const droppedUnnamed = base.map((piece) =>
      piece.kind === 'exceptional-timed' ? { ...piece, unnamedExceptional: null } : piece,
    );
    expect(
      model.count_selected(toModelPlan(droppedUnnamed, serial), {
        $: 'Test',
        exc: true,
        dealt: { $: 'NotNamed' },
        ser: false,
        bucket: 1,
      }),
    ).toBe(0);

    // Defect 2: omitting `serialFilters` on an ordinary shard while a serial runner is active double-runs serial tests.
    const leakedSerial = base.map((piece) =>
      piece.kind === 'ordinary' && piece.partition?.index === 2 ? { ...piece, serialFilters: [] } : piece,
    );
    expect(
      model.count_selected(toModelPlan(leakedSerial, serial), {
        $: 'Test',
        exc: false,
        dealt: { $: 'NotNamed' },
        ser: true,
        bucket: 2,
      }),
    ).toBe(2);

    // Defect 3: omitting `excludeNamed` on an ordinary shard double-runs a timed test whose hash hits that shard.
    const leakedNamed = base.map((piece) =>
      piece.kind === 'ordinary' && piece.partition?.index === 1 ? { ...piece, excludeNamed: [] } : piece,
    );
    expect(
      model.count_selected(toModelPlan(leakedNamed, serial), {
        $: 'Test',
        exc: false,
        dealt: { $: 'ToShard', k: 1 },
        ser: false,
        bucket: 1,
      }),
    ).toBe(2);
  });
});
//#endregion smoo/cowshed!n/native-test-verdicts
