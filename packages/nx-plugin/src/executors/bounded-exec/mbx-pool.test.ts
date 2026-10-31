import { describe, expect, it } from 'bun:test';
import { mbxIdleGated, mbxLeaf, mbxPool, nxTaskCores, runsPrebuiltNextest, withMbxLeaf } from './mbx-pool.js';
import { runnerDemand } from './runner-sizing.js';

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

  it('lets a building Cargo leaf through mbx as it is and reserves any other leaf at its runner count, once', () => {
    const pool = { priority: 'low' } as const;
    const leafOf = (command: string, declared?: number) =>
      mbxLeaf(pool, runnerDemand(command, declared, 18, {}), command);
    for (const command of ['cargo test -p x', 'cargo mutants', 'cargo nextest run -p x']) {
      expect(leafOf(command)).toEqual({ kind: 'cargo' });
      expect(withMbxLeaf(command, leafOf(command))).toBe(command);
    }
    const leaf = leafOf("bun test 'a b'", 4);
    expect(leaf).toMatchObject({ kind: 'reserve', priority: 'low', demand: { kind: 'process', want: 4 } });
    const reserved = withMbxLeaf("bun test 'a b'", leaf);
    expect(reserved).toStartWith("mbx reserve --grant-fd 5 --cpus 4 --priority low -- /bin/sh -c 'export ");
    expect(reserved).toEndWith(">&2; bun test '\\''a b'\\'''");
    // A Vitest or Go test leaf enters no pool and runs its own default workers (owner's ruling, 2026-10-31).
    expect(leafOf('bun vitest run')).toBeNull();
    expect(leafOf('go test ./...')).toBeNull();
  });

  it('admits a nextest run of prebuilt binaries once for its whole grant, never a ticket per test process', () => {
    // A downstream gate on 2026-10-30: under MBX_SCHEDULER_TESTS=1 each test process of an extracted-archive run
    // queued for its own mbx ticket, and codec tests of at most 0.25 s took 73.5 s apiece.
    const pool = { priority: 'low' } as const;
    const inPlace =
      'cargo --frozen nextest run --binaries-metadata .cache/nextest/binaries-metadata.json ' +
      '--cargo-metadata .cache/nextest/cargo-metadata.json ' +
      '--target-dir-remap "$(node_modules/@smoothbricks/nx-plugin/../nextest-extract/dist/bin/darwin-arm64/smoo-nx-nextest-binaries target-dir)" ' +
      "--workspace-remap . -E 'package(core)' --no-fail-fast --no-tests=pass";
    const bunSpelled = inPlace.replace(
      'node_modules/@smoothbricks/nx-plugin/../nextest-extract/dist/bin/darwin-arm64/smoo-nx-nextest-binaries',
      'bun node_modules/@smoothbricks/nx-plugin/../nextest-extract/dist/ts/bin/smoo-nx-nextest-binaries.js',
    );
    const leafOf = (command: string, declared: number) =>
      mbxLeaf(pool, runnerDemand(command, declared, 18, {}), command);
    for (const command of [inPlace, bunSpelled]) {
      expect(leafOf(command, 6)).toMatchObject({
        kind: 'reserve',
        priority: 'low',
        demand: { kind: 'nextest', want: 6 },
      });
      expect(withMbxLeaf(command, leafOf(command, 6))).toStartWith(
        'mbx reserve --grant-fd 5 --cpus 6 --priority low -- /bin/sh -c ',
      );
    }
    expect(leafOf('cargo nextest run --archive-file a.tar.zst', 4)).toMatchObject({
      kind: 'reserve',
      demand: { kind: 'nextest', want: 4 },
      priority: 'low',
    });
    // A nextest run that builds its tests first still lets mbx admit its compilers.
    expect(leafOf('cargo nextest run -p x', 8)).toEqual({ kind: 'cargo' });
  });

  it("asks for a prebuilt nextest leaf's task share, of which mbx grants as many cores as are free", () => {
    // A gate on 2026-10-30: every cargo-test leaf reserved `--cpus 18` of an 18-core pool, so they ran one at a time
    // and the host sat 70% idle. Knowing no count of its tests, the leaf asks for its task's share of the host.
    const pool = { priority: 'low' } as const;
    const command =
      'cargo --frozen nextest run --binaries-metadata .cache/nextest/binaries-metadata.json --workspace-remap .';
    expect(runsPrebuiltNextest(command)).toBe(true);
    expect(runsPrebuiltNextest('cargo nextest run -p x')).toBe(false);
    const leafOf = (environment: NodeJS.ProcessEnv, declared?: number) =>
      mbxLeaf(pool, runnerDemand(command, declared, nxTaskCores(18, environment), {}), command);
    // Nx runs every task at once under 100%: each leaf counts one core.
    expect(leafOf({ NX_PARALLEL: '100%' })).toMatchObject({ kind: 'reserve', demand: { kind: 'nextest', want: 1 } });
    expect(nxTaskCores(18, { NX_PARALLEL: '6' })).toBe(3);
    expect(nxTaskCores(18, { NX_PARALLEL: '50%' })).toBe(2);
    // Nx's own default of three tasks at once.
    expect(nxTaskCores(18, {})).toBe(6);
    expect(nxTaskCores(2, { NX_PARALLEL: '8' })).toBe(1);
    // A declared count is the target's own word and still wins.
    expect(leafOf({ NX_PARALLEL: '100%' }, 4)).toMatchObject({ kind: 'reserve', demand: { want: 4 } });
  });

  it.each<[string, string, boolean]>([
    ['supported nextest', 'cargo --frozen nextest run', true],
    ['two supported nextest legs', 'cargo nextest run && cargo --frozen nextest run --run-ignored only', true],
    ['supported selected Cargo legs', 'cargo test --tests; cargo test --lib', true],
    ['supported alternative legs', 'cargo nextest run || cargo nextest run', true],
    [
      'in-place nextest chain with quoted shell punctuation',
      `cargo --frozen nextest run --target-dir-remap "$(x/smoo-nx-nextest-binaries target-dir)" -E 'package(core) & test(first)' && cargo --frozen nextest run --run-ignored only -E 'test(second)'`,
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
      'no prefix may run before the first Cargo leg',
      'extracted="$(printf archive)" && cargo nextest run && cargo nextest run',
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
      "the helper's substitution is known only as the value of --target-dir-remap",
      'cargo nextest run && cargo nextest run "$(x/smoo-nx-nextest-binaries target-dir)"',
      false,
    ],
    [
      'an unquoted substitution may split into options',
      'cargo nextest run --target-dir-remap $(x/smoo-nx-nextest-binaries target-dir)',
      false,
    ],
    [
      'a substitution that says more than the target directory is unknown',
      'cargo nextest run --target-dir-remap "$(x/smoo-nx-nextest-binaries target-dir; echo --config)"',
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
      'the in-place nextest leaf whose queued runners hit the idle bound',
      `cargo --frozen nextest run --tool-config-file 'smoo-report:report.toml' --binaries-metadata .cache/nextest/binaries-metadata.json --cargo-metadata .cache/nextest/cargo-metadata.json --target-dir-remap "$(node_modules/@smoothbricks/nx-plugin/../nextest-extract/dist/bin/darwin-arm64/smoo-nx-nextest-binaries target-dir)" --workspace-remap . -E 'package(core)' --no-fail-fast --no-tests=pass --user-config-file none --tool-config-file "smoo:$PWD/node_modules/@smoothbricks/nx-plugin/nextest.toml"`,
      true,
    ],
    [
      "a repository's own in-place runner naming the helper's script through bun",
      `cargo --frozen nextest run --binaries-metadata .cache/nextest/binaries-metadata.json --cargo-metadata .cache/nextest/cargo-metadata.json --target-dir-remap "$(bun node_modules/@smoothbricks/nx-plugin/../nextest-extract/dist/ts/bin/smoo-nx-nextest-binaries.js target-dir)" --workspace-remap . -E 'binary_id(app::integration)' --run-ignored only`,
      true,
    ],
    [
      'another program run through bun is unknown',
      'cargo nextest run --target-dir-remap "$(bun x/other.js target-dir)"',
      false,
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
