import { describe, expect, it } from 'bun:test';
import { mbxIdleGated, mbxLeaf, mbxPool, withMbxLeaf } from './mbx-pool.js';

describe("mbx's one pool at the leaf", () => {
  it('is named only by the canonical mbx environment, at the priority of the job', () => {
    expect(mbxPool({})).toBeNull();
    expect(mbxPool({ MBX_SCHEDULER: '1' })).toBeNull();
    expect(mbxPool({ MBX_SCHEDULER: '0', MBX_CACHE_DIR: '/bc/mbx' })).toBeNull();
    expect(mbxPool({ MBX_SCHEDULER: '1', MBX_CACHE_DIR: '/bc/mbx' })).toEqual({ priority: 'low' });
    expect(mbxPool({ MBX_SCHEDULER: '1', MBX_CACHE_DIR: '/bc/mbx', MBX_SCHEDULER_PRIORITY: 'normal' })).toEqual({
      priority: 'normal',
    });
  });

  it('names no pool inside a reservation: nested work shares the grant it runs under', () => {
    // The 2026-10-29 sb-r22 gate: a bounded-exec suite under `mbx reserve` started an executor that reserved again and
    // waited in the pool behind its own parent's grant until the progress bound ended the suite.
    expect(mbxPool({ MBX_SCHEDULER: '1', MBX_CACHE_DIR: '/bc/mbx', MBX_SCHED_DISABLE: '1' })).toBeNull();
  });

  it('lets a Cargo leaf through mbx as it is and reserves any other leaf at the job count, once', () => {
    const pool = { priority: 'low' } as const;
    for (const kind of ['cargo', 'cargo-mutants', 'nextest'] as const) {
      expect(mbxLeaf(pool, kind, 8)).toEqual({ kind: 'cargo' });
      expect(withMbxLeaf('cargo test -p x', mbxLeaf(pool, kind, 8))).toBe('cargo test -p x');
    }
    const leaf = mbxLeaf(pool, 'process', 4);
    expect(leaf).toEqual({ kind: 'reserve', cpus: 4, priority: 'low' });
    expect(withMbxLeaf("bun test 'a b'", leaf)).toBe(
      "mbx reserve --grant-fd 5 --cpus 4 --priority low -- /bin/sh -c 'bun test '\\''a b'\\'''",
    );
    expect(mbxLeaf(pool, 'vitest', 0)).toEqual({ kind: 'reserve', cpus: 1, priority: 'low' });
  });

  it.each<[string, string, boolean]>([
    ['supported nextest', 'cargo --frozen nextest run', true],
    [
      'the extracted-archive nextest leaf whose queued runners hit the idle bound',
      `extracted="$(node_modules/@smoothbricks/nx-plugin/../nextest-extract/dist/bin/darwin-arm64/smoo-nx-nextest-extract .cache/nextest/archive.tar.zst)" && cargo --frozen nextest run --tool-config-file 'smoo-report:report.toml' --binaries-metadata "$extracted/target/nextest/binaries-metadata.json" --cargo-metadata "$extracted/target/nextest/cargo-metadata.json" --target-dir-remap "$extracted/target" --workspace-remap . -E 'package(core)' --no-fail-fast --no-tests=pass --user-config-file none --tool-config-file "smoo:$PWD/node_modules/@smoothbricks/nx-plugin/nextest.toml"`,
      true,
    ],
    ['selected non-doc Cargo tests', 'cargo test --tests', true],
    ['plain Cargo test can execute doctests', 'cargo test', false],
    ['doctests', 'cargo test --doc', false],
    ['no test execution', 'cargo test --no-run', false],
    ['nextest listing', 'cargo nextest list', false],
    ['configuration override', 'cargo nextest run --config build.jobs=2', false],
    ['toolchain override', 'cargo +nightly nextest run', false],
    ['directory override', 'cargo nextest run --directory other', false],
    ['archive target is unknown at spawn', 'cargo nextest run --archive-file other.tar.zst', false],
    ['direct test binary', './target/debug/deps/test-binary', false],
    ['another runner', 'cargo mutants', false],
    ['direct nextest bypasses the Cargo shim', 'cargo-nextest nextest run', false],
    ['a following unscheduled command', 'cargo nextest run && ./test-binary', false],
  ])('%s', (_title, command, expected) => {
    expect(mbxIdleGated({ kind: 'cargo' }, command, { MBX_SCHEDULER_TESTS: '1' })).toBe(expected);
    expect(mbxIdleGated({ kind: 'cargo' }, command, { MBX_SCHEDULER_TESTS: '0' })).toBe(false);
    expect(mbxIdleGated(null, command, { MBX_SCHEDULER_TESTS: '1' })).toBe(false);
    for (const disabled of [{ MBX_SCHEDULER: '0' }, { MBX_SCHED_DISABLE: '1' }, { MBX_DISABLE: '1' }]) {
      expect(mbxIdleGated({ kind: 'cargo' }, command, { MBX_SCHEDULER_TESTS: '1', ...disabled })).toBe(false);
    }
  });
});
