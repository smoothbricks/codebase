import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  mbxIdleGated,
  mbxLeaf,
  mbxPool,
  runsPrebuiltNextest,
  withCountedReservation,
  withMbxLeaf,
} from './mbx-pool.js';
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
    expect(reserved).toStartWith(
      'BOUNDED_EXEC_CPU_WANT=4 && export BOUNDED_EXEC_CPU_WANT && mbx reserve --grant-fd 5 --cpus "$BOUNDED_EXEC_CPU_WANT" --priority low -- /bin/sh -c \'export ',
    );
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
      expect(withMbxLeaf(command, leafOf(command, 6))).toContain(
        'mbx reserve --grant-fd 5 --cpus "$BOUNDED_EXEC_CPU_WANT" --priority low -- /bin/sh -c ',
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

  it('builds a reserved leaf’s fixture first, then asks for at most its units, at least one', () => {
    // A gate on 2026-10-30: every cargo-test leaf reserved `--cpus 18` of an 18-core pool, so they ran one at a time
    // and the host sat 70% idle. mbx now grants a reservation the cores that are free, and a piece of two slow tests
    // asking for every core would hold the idle rest of its grant: it asks for its tests, counted after its fixture
    // has built the test binaries they come from.
    const pool = { priority: 'low' } as const;
    const nextest = 'cargo --frozen nextest run --binaries-metadata .cache/nextest/binaries-metadata.json';
    expect(runsPrebuiltNextest(nextest)).toBe(true);
    expect(runsPrebuiltNextest('cargo nextest run -p x')).toBe(false);
    const leaf = mbxLeaf(pool, runnerDemand(nextest, 6, 18, {}), nextest);
    if (leaf?.kind !== 'reserve') throw new Error('a prebuilt nextest leaf is reserved for');
    // A stand-in mbx that says what it was asked for and runs the reservation's command on one granted core.
    const mbx = `mbx() { while [ "$1" != -- ]; do if [ "$1" = --cpus ]; then printf 'reserve %s\\n' "$2"; fi; shift; done; shift; MBX_RESERVE_CPUS=1 "$@"; }; `;
    const fixture = 'echo built >&2';
    const command = 'echo "ran $BOUNDED_EXEC_CPU_TOKENS"';
    const run = (units: string | null) =>
      spawnSync(
        '/bin/sh',
        [
          '-c',
          mbx +
            (units === null
              ? withMbxLeaf(`${fixture} && { ${command}; }`, leaf)
              : withCountedReservation(`{ ${fixture}; } && ${units}`, command, leaf)),
        ],
        { encoding: 'utf8' },
      );
    // Uncounted, the fixture runs inside the reservation, as it always did.
    expect(run(null)).toMatchObject({
      stdout: 'reserve 6\nran 1\n',
      stderr: 'cowshed: mbx reserve running cpus=1 asked=6\nbuilt\n',
    });
    // Counted, it runs first, and the leaf asks for its units, at most its runner's count and at least one.
    const counted = (units: string, asked: number) =>
      expect(run(units)).toMatchObject({
        stdout: `reserve ${asked}\nran 1\n`,
        stderr: `built\ncowshed: mbx reserve running cpus=1 asked=${asked}\n`,
      });
    counted("printf 'a\\nb\\n'", 2);
    counted("printf 'a\\nb\\nc\\nd\\ne\\nf\\ng\\nh\\n'", 6);
    counted('true', 1);
    // A count that fails fails the leaf before anything is reserved.
    const failed = run('false');
    expect(failed).toMatchObject({ stdout: '', stderr: 'built\n' });
    expect(failed.status).not.toBe(0);
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
