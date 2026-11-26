import { describe, expect, it } from 'bun:test';
import { nativeTestPhases } from './executors/bounded-exec/native-test.js';
import { judgeRun } from './executors/bounded-exec/verdict.js';
import {
  type CargoTestPiece,
  cargoTestPieces,
  namedTestsFilter,
  renderCargoTestPiece,
  timedShardMembers,
} from './nextest-timings.js';

const EXC = 'test(/compile_fail|fail_to_compile/)';
const SERIAL_A = '(test(/real_apfs_/))';
const SERIAL_B = '(test(/serial_probe_/))';

//#region smoo/cowshed!n/native-test-verdicts
describe('cargoTestPieces and renderCargoTestPiece', () => {
  const cases: ReadonlyArray<{
    readonly label: string;
    readonly pkgName: string;
    readonly testShards: number;
    readonly exceptional: string | null;
    readonly serialFilters: readonly string[];
    readonly timings: Readonly<Record<string, number>>;
    readonly expected: readonly CargoTestPiece[];
  }> = [
    {
      label: 'unsharded without serial groups or overrides',
      pkgName: 'small',
      testShards: 1,
      exceptional: null,
      serialFilters: [],
      timings: {},
      expected: [{ targetName: 'cargo-test-small', selector: 'package(small)' }],
    },
    {
      label: 'unsharded ignores exceptional overrides and excludes active serial groups',
      pkgName: 'small',
      testShards: 1,
      exceptional: EXC,
      serialFilters: [SERIAL_A],
      timings: {},
      expected: [{ targetName: 'cargo-test-small', selector: `package(small) and not (${SERIAL_A})` }],
    },
    {
      label: 'unsharded excludes multiple serial groups joined by or',
      pkgName: 'small',
      testShards: 1,
      exceptional: EXC,
      serialFilters: [SERIAL_A, SERIAL_B],
      timings: {},
      expected: [{ targetName: 'cargo-test-small', selector: `package(small) and not (${SERIAL_A} or ${SERIAL_B})` }],
    },
    {
      label: 'unsharded ignores timings when testShards is 1',
      pkgName: 'small',
      testShards: 1,
      exceptional: EXC,
      serialFilters: [],
      timings: { 'slow::a': 9000 },
      expected: [{ targetName: 'cargo-test-small', selector: 'package(small)' }],
    },
    {
      label: 'sharded without exceptional overrides, timings, or serial groups',
      pkgName: 'plain',
      testShards: 2,
      exceptional: null,
      serialFilters: [],
      timings: {},
      expected: [
        { targetName: 'cargo-test-plain-shard1', partition: 'hash:1/2', selector: 'package(plain)' },
        { targetName: 'cargo-test-plain-shard2', partition: 'hash:2/2', selector: 'package(plain)' },
      ],
    },
    {
      label: 'sharded with exceptional override and no timings',
      pkgName: 'big',
      testShards: 3,
      exceptional: EXC,
      serialFilters: [],
      timings: {},
      expected: [
        { targetName: 'cargo-test-big-shard1', partition: 'hash:1/3', selector: `package(big) and not (${EXC})` },
        { targetName: 'cargo-test-big-shard2', partition: 'hash:2/3', selector: `package(big) and not (${EXC})` },
        { targetName: 'cargo-test-big-shard3', partition: 'hash:3/3', selector: `package(big) and not (${EXC})` },
        {
          targetName: 'cargo-test-big-exceptions-shard1',
          partition: 'hash:1/3',
          selector: `package(big) and (${EXC})`,
        },
        {
          targetName: 'cargo-test-big-exceptions-shard2',
          partition: 'hash:2/3',
          selector: `package(big) and (${EXC})`,
        },
        {
          targetName: 'cargo-test-big-exceptions-shard3',
          partition: 'hash:3/3',
          selector: `package(big) and (${EXC})`,
        },
      ],
    },
    {
      label: 'sharded with exceptional override and serial group (macOS)',
      pkgName: 'big',
      testShards: 2,
      exceptional: EXC,
      serialFilters: [SERIAL_A],
      timings: {},
      expected: [
        {
          targetName: 'cargo-test-big-shard1',
          partition: 'hash:1/2',
          selector: `package(big) and not (${EXC}) and not (${SERIAL_A})`,
        },
        {
          targetName: 'cargo-test-big-shard2',
          partition: 'hash:2/2',
          selector: `package(big) and not (${EXC}) and not (${SERIAL_A})`,
        },
        {
          targetName: 'cargo-test-big-exceptions-shard1',
          partition: 'hash:1/2',
          selector: `package(big) and (${EXC}) and not (${SERIAL_A})`,
        },
        {
          targetName: 'cargo-test-big-exceptions-shard2',
          partition: 'hash:2/2',
          selector: `package(big) and (${EXC}) and not (${SERIAL_A})`,
        },
      ],
    },
    {
      label: 'sharded with timings and no exceptional override',
      pkgName: 'timed',
      testShards: 3,
      exceptional: null,
      serialFilters: [],
      timings: { 'slow::a': 9000, 'slow::b': 5000, 'slow::c': 4000, 'slow::d': 3000 },
      expected: [
        {
          targetName: 'cargo-test-timed-shard1',
          partition: 'hash:1/3',
          selector: 'package(timed) and not (test(=slow::a) or test(=slow::b) or test(=slow::c) or test(=slow::d))',
        },
        {
          targetName: 'cargo-test-timed-shard2',
          partition: 'hash:2/3',
          selector: 'package(timed) and not (test(=slow::a) or test(=slow::b) or test(=slow::c) or test(=slow::d))',
        },
        {
          targetName: 'cargo-test-timed-shard3',
          partition: 'hash:3/3',
          selector: 'package(timed) and not (test(=slow::a) or test(=slow::b) or test(=slow::c) or test(=slow::d))',
        },
        { targetName: 'cargo-test-timed-exceptions-shard1', selector: 'package(timed) and ((test(=slow::a)))' },
        { targetName: 'cargo-test-timed-exceptions-shard2', selector: 'package(timed) and ((test(=slow::b)))' },
        {
          targetName: 'cargo-test-timed-exceptions-shard3',
          selector: 'package(timed) and ((test(=slow::c) or test(=slow::d)))',
        },
      ],
    },
    {
      label: 'sharded with timings and exceptional override: last shard takes unnamed exceptional tests',
      pkgName: 'timed',
      testShards: 3,
      exceptional: EXC,
      serialFilters: [],
      timings: { 'slow::a': 9000, 'slow::b': 5000, 'slow::c': 4000, 'slow::d': 3000 },
      expected: [
        {
          targetName: 'cargo-test-timed-shard1',
          partition: 'hash:1/3',
          selector: `package(timed) and not ((${EXC}) or (test(=slow::a) or test(=slow::b) or test(=slow::c) or test(=slow::d)))`,
        },
        {
          targetName: 'cargo-test-timed-shard2',
          partition: 'hash:2/3',
          selector: `package(timed) and not ((${EXC}) or (test(=slow::a) or test(=slow::b) or test(=slow::c) or test(=slow::d)))`,
        },
        {
          targetName: 'cargo-test-timed-shard3',
          partition: 'hash:3/3',
          selector: `package(timed) and not ((${EXC}) or (test(=slow::a) or test(=slow::b) or test(=slow::c) or test(=slow::d)))`,
        },
        { targetName: 'cargo-test-timed-exceptions-shard1', selector: 'package(timed) and ((test(=slow::a)))' },
        { targetName: 'cargo-test-timed-exceptions-shard2', selector: 'package(timed) and ((test(=slow::b)))' },
        {
          targetName: 'cargo-test-timed-exceptions-shard3',
          selector: `package(timed) and ((test(=slow::c) or test(=slow::d)) or ((${EXC}) and not (test(=slow::a) or test(=slow::b) or test(=slow::c) or test(=slow::d))))`,
        },
      ],
    },
    {
      label: 'sharded with timings, exceptional override, fewer named tests than shards, and serial group',
      pkgName: 'sparse',
      testShards: 3,
      exceptional: EXC,
      serialFilters: [SERIAL_A],
      timings: { 'slow::only': 8000 },
      expected: [
        {
          targetName: 'cargo-test-sparse-shard1',
          partition: 'hash:1/3',
          selector: `package(sparse) and not ((${EXC}) or (test(=slow::only))) and not (${SERIAL_A})`,
        },
        {
          targetName: 'cargo-test-sparse-shard2',
          partition: 'hash:2/3',
          selector: `package(sparse) and not ((${EXC}) or (test(=slow::only))) and not (${SERIAL_A})`,
        },
        {
          targetName: 'cargo-test-sparse-shard3',
          partition: 'hash:3/3',
          selector: `package(sparse) and not ((${EXC}) or (test(=slow::only))) and not (${SERIAL_A})`,
        },
        {
          targetName: 'cargo-test-sparse-exceptions-shard1',
          selector: `package(sparse) and ((test(=slow::only))) and not (${SERIAL_A})`,
        },
        {
          targetName: 'cargo-test-sparse-exceptions-shard2',
          selector: `package(sparse) and ((none())) and not (${SERIAL_A})`,
        },
        {
          targetName: 'cargo-test-sparse-exceptions-shard3',
          selector: `package(sparse) and ((none()) or ((${EXC}) and not (test(=slow::only)))) and not (${SERIAL_A})`,
        },
      ],
    },
  ];

  for (const row of cases) {
    it(`renders byte-identical selectors and target names: ${row.label}`, () => {
      const rendered = cargoTestPieces(
        row.pkgName,
        row.testShards,
        row.exceptional,
        row.serialFilters,
        row.timings,
      ).map(renderCargoTestPiece);
      expect(rendered).toEqual([...row.expected]);
    });
  }
});

describe('timedShardMembers and namedTestsFilter', () => {
  it('deals named tests longest-first onto the least-loaded shard with deterministic ties', () => {
    const bins = timedShardMembers({ z: 5000, a: 5000, c: 4000, b: 3000, d: 3000 }, 3);
    expect(bins).toEqual([['a', 'd'], ['z'], ['b', 'c']]);
    const flattened = bins.flat().sort();
    expect(flattened).toEqual(['a', 'b', 'c', 'd', 'z']);
  });

  it('renders none() for empty names and exact test(=...) disjunction otherwise', () => {
    expect(namedTestsFilter([])).toBe('none()');
    expect(namedTestsFilter(['mod::one'])).toBe('test(=mod::one)');
    expect(namedTestsFilter(['mod::one', 'mod::two'])).toBe('test(=mod::one) or test(=mod::two)');
  });
});

describe('empty-selection serial group policy (pure command and verdict decisions)', () => {
  it('adds --no-tests=fail to a serial group recipe and preserves --no-tests=pass on crate pieces', () => {
    const serial = nativeTestPhases({
      nativeTest: {
        cargoArgs: ['--workspace'],
        selectionArgs: ['-E', SERIAL_B],
        runArgs: ['--no-fail-fast'],
      },
      parallelism: 1,
    });
    expect(serial.command).toContain('--no-tests=fail');
    expect(serial.command).toContain('--no-fail-fast');

    const piece = nativeTestPhases({
      nativeTest: {
        cargoArgs: ['-p', 'partition-probe'],
        selectionArgs: ['-E', 'package(partition-probe)', '--partition', 'hash:1/3'],
        runArgs: ['--no-fail-fast', '--no-tests=pass'],
      },
    });
    expect(piece.command).toContain('--no-tests=pass');
    expect(piece.command).not.toContain('--no-tests=fail');
  });

  it('judges nextest exit 4 (empty selection under --no-tests=fail) as a failed verdict', () => {
    const emptyReport = { timedOut: [], cancelled: [], failed: [] };
    for (const report of [null, emptyReport]) {
      expect(
        judgeRun({
          exitCode: 4,
          elapsedMs: 50,
          expiry: null,
          report,
          noSpace: null,
          lockWaits: [],
          cpuRising: [],
        }),
      ).toEqual({ outcome: 'failed', exitCode: 4, tests: [] });
    }
  });
});
//#endregion smoo/cowshed!n/native-test-verdicts
