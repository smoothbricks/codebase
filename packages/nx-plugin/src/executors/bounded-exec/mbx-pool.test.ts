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

  it('lets a building Cargo leaf through mbx as it is and reserves any other leaf at the job count, once', () => {
    const pool = { priority: 'low' } as const;
    for (const kind of ['cargo', 'cargo-mutants', 'nextest'] as const) {
      expect(mbxLeaf(pool, kind, 8, 'cargo test -p x')).toEqual({ kind: 'cargo' });
      expect(withMbxLeaf('cargo test -p x', mbxLeaf(pool, kind, 8, 'cargo test -p x'))).toBe('cargo test -p x');
    }
    const leaf = mbxLeaf(pool, 'process', 4, "bun test 'a b'");
    expect(leaf).toEqual({ kind: 'reserve', cpus: 4, priority: 'low' });
    expect(withMbxLeaf("bun test 'a b'", leaf)).toBe(
      "mbx reserve --grant-fd 5 --cpus 4 --priority low -- /bin/sh -c 'bun test '\\''a b'\\'''",
    );
    expect(mbxLeaf(pool, 'vitest', 0, 'bun vitest run')).toEqual({ kind: 'reserve', cpus: 1, priority: 'low' });
  });

  it('admits a nextest run of prebuilt binaries once for its whole grant, never a ticket per test process', () => {
    // A downstream gate on 2026-10-30: under MBX_SCHEDULER_TESTS=1 each test process of an extracted-archive run
    // queued for its own mbx ticket, and codec tests of at most 0.25 s took 73.5 s apiece.
    const pool = { priority: 'low' } as const;
    const extracted =
      'extracted="$(smoo-nx-nextest-extract target/nextest/archive.tar.zst)" && cargo --frozen nextest run ' +
      '--binaries-metadata "$extracted/target/nextest/binaries-metadata.json" --workspace-remap .';
    expect(mbxLeaf(pool, 'nextest', 6, extracted)).toEqual({ kind: 'reserve', cpus: 6, priority: 'low' });
    expect(withMbxLeaf(extracted, mbxLeaf(pool, 'nextest', 6, extracted))).toStartWith(
      'mbx reserve --grant-fd 5 --cpus 6 --priority low -- /bin/sh -c ',
    );
    expect(mbxLeaf(pool, 'nextest', 4, 'cargo nextest run --archive-file a.tar.zst')).toEqual({
      kind: 'reserve',
      cpus: 4,
      priority: 'low',
    });
    // A nextest run that builds its tests first still lets mbx admit its compilers.
    expect(mbxLeaf(pool, 'nextest', 8, 'cargo nextest run -p x')).toEqual({ kind: 'cargo' });
  });

  it.each<[string, string, boolean]>([
    ['supported nextest', 'cargo --frozen nextest run', true],
    ['two supported nextest legs', 'cargo nextest run && cargo --frozen nextest run --run-ignored only', true],
    ['supported selected Cargo legs', 'cargo test --tests; cargo test --lib', true],
    ['supported alternative legs', 'cargo nextest run || cargo nextest run', true],
    [
      'extracted nextest chain with quoted shell punctuation',
      `extracted="$(bun extract.js archive.tar.zst)" && cargo --frozen nextest run -E 'package(core) & test(first)' && cargo --frozen nextest run --run-ignored only -E 'test(second)'`,
      true,
    ],
    ['a later unscheduled Cargo leg', 'cargo nextest run && cargo test --doc', false],
    ['a later overridden nextest leg', 'cargo nextest run && cargo nextest run --config build.jobs=2', false],
    ['a leading unscheduled leg', './test-binary && cargo nextest run', false],
    ['a background leg', 'cargo nextest run & cargo nextest run', false],
    ['a pipeline leg', 'cargo nextest run | cargo nextest run', false],
    ['a newline starts an unscheduled command', 'cargo nextest run && cargo nextest run\n./test-binary', false],
    ['an escaped quote can hide a separator', "cargo nextest run && cargo nextest run \\'x; ./test-binary '", false],
    ['a quoted configuration override', "cargo nextest run && cargo nextest run '--config' build.jobs=2", false],
    ['a quoted archive override', 'cargo nextest run && cargo nextest run "--archive-file" other.tar.zst', false],
    ['a concatenated quoted option', "cargo nextest run && cargo nextest run --co'nfig' build.jobs=2", false],
    ['a quoted doctest selector', "cargo nextest run && cargo test '--doc'", false],
    [
      'a quoted separator does not hide a later override',
      "cargo nextest run && cargo nextest run -E 'all() | test(/ -- /)' --config build.jobs=2",
      false,
    ],
    ['a quoted real separator ends Cargo options', "cargo test --tests '--' --config", true],
    [
      'a builtin-only command substitution is unscheduled work',
      'cargo nextest run && cargo nextest run "$(while :; do :; done)"',
      false,
    ],
    [
      'an extracted prefix cannot swallow another command',
      'extracted="$(printf archive)" && ./test-binary && extracted="$(printf archive)" && cargo nextest run && cargo nextest run',
      false,
    ],
    [
      'an unresolved expansion can supply a configuration option',
      'cargo nextest run && cargo nextest run ${CONFIG_OPTION:---config} build.jobs=2',
      false,
    ],
    [
      'an unresolved word can supply a configuration option',
      'cargo nextest run && cargo nextest run "$CONFIG_OPTION" build.jobs=2',
      false,
    ],
    [
      'an unquoted glob may expand to a forbidden option',
      'cargo nextest run && cargo nextest run --conf* build.jobs=2',
      false,
    ],
    ['a quoted glob remains one literal filter', "cargo nextest run && cargo nextest run -E 'test(/foo.*/)'", true],
    [
      'unquoted inferred paths may split into options',
      'extracted="$(printf archive)" && cargo nextest run && cargo nextest run $extracted/',
      false,
    ],
    [
      'a quoted inferred path must remain a known path operand',
      'extracted="$(printf archive)" && cargo nextest run && cargo nextest run "$extracted/"',
      false,
    ],
    ['non-shell whitespace cannot name a Cargo executable', 'cargo nextest run && cargo\u00a0nextest run', false],
    [
      'an unquoted tilde can expand to a forbidden option',
      'cargo nextest run && cargo nextest run ~ build.jobs=2',
      false,
    ],
    ['a quoted tilde remains literal', "cargo nextest run && cargo nextest run -E 'test(/~/)'", true],
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
