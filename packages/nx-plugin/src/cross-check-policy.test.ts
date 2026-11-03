import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  CARGO_CROSS_LINT_COMMAND,
  CARGO_CROSS_LINT_REFUSAL,
  CARGO_CROSS_LINT_REPLAY_ONLY_ENV,
  CARGO_CROSS_LINT_TARGET,
  CARGO_DENY_WARNINGS,
  CARGO_LINT_CLIPPY_COMMAND,
  CARGO_LINUX_TRIPLE,
  CROSS_CHECK_SCRIPT_COMMAND,
  CROSS_CHECK_SCRIPT_NAME,
  cargoFrozen,
  DECLARED_COMMAND_IDENTITY,
  runsFrozenCargo,
} from './cross-check-policy.js';
import { BUILD_OUTPUT_DEPENDENCIES, PLATFORM_TARGET_GLOBS } from './workspace-config-policy.js';

const suffixOf = (glob: string): string => (glob.startsWith('*') ? glob.slice(1) : glob);

describe('Linux cross-check policy', () => {
  it('keeps the cross target out of every platform artifact family', () => {
    // `*-linux` is not decoration: publish-workflow fans out over
    // LINUX_PLATFORM_TARGET_GLOBS to build RELEASE ARTIFACTS on a Linux runner,
    // platformTargetFamily classifies by it, and a matching name flags the whole
    // project as platform-bearing. Naming this validation target `*-linux` would
    // enrol a clippy run in release artifact production — silently, and only
    // visible at publish time.
    for (const glob of PLATFORM_TARGET_GLOBS) {
      expect(CARGO_CROSS_LINT_TARGET.endsWith(suffixOf(glob))).toBe(false);
    }
  });

  it('keeps the cross target out of every build output family', () => {
    // A match here would attach the gate to each project's aggregate `build`,
    // making an ordinary macOS build demand the opt-in cross toolchain.
    for (const dependency of BUILD_OUTPUT_DEPENDENCIES) {
      expect(CARGO_CROSS_LINT_TARGET.endsWith(suffixOf(dependency))).toBe(false);
    }
  });

  it('names targets without colons', () => {
    // Nx CLI syntax is already project:target:configuration.
    expect(CARGO_CROSS_LINT_TARGET).not.toContain(':');
  });

  it('compiles test code for the target without running anything', () => {
    // --all-targets is the entire test story for the cross arm: bins, tests,
    // benches and examples are type-checked for Linux, and nothing is executed
    // because an x86_64 Linux binary cannot run on Apple Silicon. Dropping it
    // would silently stop checking every #[cfg(test)] and #[bench] block.
    expect(CARGO_CROSS_LINT_COMMAND).toContain('--all-targets');
    // No cargo subcommand here may execute what it built.
    expect(CARGO_CROSS_LINT_COMMAND).not.toContain('cargo test');
    expect(CARGO_CROSS_LINT_COMMAND).not.toContain('cargo run');
  });

  it('matches the severity and triple CI lints with', () => {
    // A weaker local command passes where CI fails, which is the failure this
    // gate exists to prevent rather than reproduce.
    expect(CARGO_CROSS_LINT_COMMAND).toContain(CARGO_DENY_WARNINGS);
    expect(CARGO_CROSS_LINT_COMMAND).toContain(`--target ${CARGO_LINUX_TRIPLE}`);
  });

  it('denies warnings through build.warnings, never clippy-driver arguments', () => {
    // Anything after `--` becomes CLIPPY_ARGS, part of every member's fingerprint.
    for (const command of [CARGO_LINT_CLIPPY_COMMAND, CARGO_CROSS_LINT_COMMAND]) {
      expect(command).not.toContain(' -- ');
    }
  });

  it('fails on warnings it replays from units a plain clippy checked in its directory', async () => {
    const project = await mkdtemp(join(tmpdir(), 'cargo-lint-warnings-'));
    const clippy = async (command: string): Promise<{ exitCode: number; output: string }> => {
      const child = Bun.spawn(['sh', '-c', command], {
        cwd: project,
        env: { ...process.env, RUSTC_WRAPPER: '', RUSTC_WORKSPACE_WRAPPER: '' },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      return { exitCode, output: stdout + stderr };
    };
    try {
      await mkdir(join(project, 'src'));
      await writeFile(
        join(project, 'Cargo.toml'),
        '[package]\nname = "warning-fixture"\nversion = "0.1.0"\nedition = "2021"\n[workspace]\n',
      );
      await writeFile(
        join(project, 'Cargo.lock'),
        'version = 4\n\n[[package]]\nname = "warning-fixture"\nversion = "0.1.0"\n',
      );
      await writeFile(join(project, 'src/lib.rs'), 'pub fn planted() {\n    let unused = 1;\n}\n');
      // The gate's command, warnings allowed: same directory, same units. A denied run is no way to
      // check the crate first: in a loaded host suite run, one that failed on the lib's warning left
      // the lib test unchecked, and the rerun checked it.
      const plain = CARGO_LINT_CLIPPY_COMMAND.replace(` ${CARGO_DENY_WARNINGS}`, '');
      expect(plain).not.toBe(CARGO_LINT_CLIPPY_COMMAND);

      const first = await clippy(plain);
      expect(first).toMatchObject({ exitCode: 0 });
      expect(first.output).toContain('Checking warning-fixture');
      expect(first.output).toContain('unused variable');

      // Fresh: the gate checks nothing, replays the plain run's warning, and fails on it.
      const gate = await clippy(CARGO_LINT_CLIPPY_COMMAND);
      expect(gate.exitCode).not.toBe(0);
      expect(gate.output).not.toContain('Checking warning-fixture');
      expect(gate.output).toContain('unused variable');
      expect(gate.output).toContain('warnings are denied by `build.warnings` configuration');
    } finally {
      await rm(project, { recursive: true, force: true });
    }
  });

  it('refuses to start without cargo-zigbuild instead of compiling', async () => {
    // Without this guard a cache miss in a shell lacking the cross toolchain would
    // start a compile that cannot finish. Run for real against a PATH with no
    // cargo-zigbuild: it must refuse, name the fix, and never reach cargo.
    const child = Bun.spawn(['/bin/sh', '-c', CARGO_CROSS_LINT_COMMAND], {
      cwd: tmpdir(),
      env: { PATH: '/usr/bin:/bin' },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()]);
    expect(exitCode).toBe(2);
    expect(stderr).toContain('needs cargo-zigbuild');
    expect(stderr).toContain('bun run check:linux');
  });

  it('prints the refusal the managed pre-push hook reads as a cache miss', () => {
    expect(CARGO_CROSS_LINT_COMMAND).toContain(`echo '${CARGO_CROSS_LINT_REFUSAL}' >&2; exit 2`);
  });

  it('refuses the replay-only probe on a miss, before cargo-zigbuild can start', async () => {
    // The pre-push probe asks for a cached verdict only. Run for real with a
    // cargo-zigbuild on PATH that records any start: it must never run, and the
    // refusal must be the line the hook reads as a miss.
    const bin = await mkdtemp(join(tmpdir(), 'cross-replay-'));
    try {
      const started = join(bin, 'started');
      await writeFile(join(bin, 'cargo-zigbuild'), `#!/bin/sh\ntouch '${started}'\n`);
      await chmod(join(bin, 'cargo-zigbuild'), 0o755);
      const child = Bun.spawn(['/bin/sh', '-c', CARGO_CROSS_LINT_COMMAND], {
        cwd: tmpdir(),
        env: { PATH: `${bin}:/usr/bin:/bin`, [CARGO_CROSS_LINT_REPLAY_ONLY_ENV]: '1' },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [exitCode, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()]);
      expect(exitCode).toBe(2);
      expect(stderr.trim()).toBe(CARGO_CROSS_LINT_REFUSAL);
      expect(await Bun.file(started).exists()).toBe(false);
    } finally {
      await rm(bin, { recursive: true, force: true });
    }
  });

  it('carries the triple explicitly so a host lint cannot pass for a cross one', () => {
    // The devenv profile deliberately exports no CARGO_BUILD_TARGET. Were the
    // triple ambient instead, running this target outside the cross shell would
    // lint the host and report green — a gate that cannot fail.
    expect(CARGO_CROSS_LINT_COMMAND).toContain('--target ');
  });

  it('preserves Cargo home configuration and its relative paths while linting', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cargo-lint-config-'));
    const home = join(root, 'home');
    const project = join(root, 'project');
    try {
      await mkdir(home);
      await mkdir(join(project, 'src'), { recursive: true });
      await writeFile(join(home, 'config.toml'), '[env]\nCONFIG_DATA = { value = "data.txt", relative = true }\n');
      await writeFile(join(root, 'data.txt'), 'configuration-relative data\n');
      await writeFile(
        join(project, 'Cargo.toml'),
        '[package]\nname = "config-fixture"\nversion = "0.1.0"\nedition = "2021"\n[workspace]\n',
      );
      await writeFile(
        join(project, 'Cargo.lock'),
        'version = 4\n\n[[package]]\nname = "config-fixture"\nversion = "0.1.0"\n',
      );
      await writeFile(join(project, 'src/lib.rs'), 'pub const DATA: &str = include_str!(env!("CONFIG_DATA"));\n');
      const child = Bun.spawn(['sh', '-c', CARGO_LINT_CLIPPY_COMMAND], {
        cwd: project,
        env: { ...process.env, CARGO_HOME: home, RUSTC_WRAPPER: '', RUSTC_WORKSPACE_WRAPPER: '' },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect({ exitCode, output: stdout + stderr }).toMatchObject({ exitCode: 0 });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('pins clippy to the committed lockfile and local cache', () => {
    expect(cargoFrozen('clippy --workspace')).toBe('cargo --frozen clippy --workspace');
    // cargo drops a global --frozen in front of an external subcommand, so the
    // flag sits on cargo-zigbuild's own subcommand.
    expect(CARGO_CROSS_LINT_COMMAND).toContain('cargo-zigbuild clippy --frozen ');
    expect(CARGO_CROSS_LINT_COMMAND).not.toContain('cargo --frozen zigbuild');
    expect(CARGO_LINT_CLIPPY_COMMAND).toContain('cargo --frozen clippy');
    // Inference recognizes both spellings, so both get the fetch prerequisite
    // and the toolchain identity.
    expect(runsFrozenCargo(CARGO_CROSS_LINT_COMMAND)).toBe(true);
    expect(runsFrozenCargo(CARGO_LINT_CLIPPY_COMMAND)).toBe(true);
    expect(runsFrozenCargo('cargo clippy --workspace')).toBe(false);
  });

  it('runs the Nx target from the root script in the land gate environment', async () => {
    expect(CROSS_CHECK_SCRIPT_NAME).toBe('check:linux');
    // Nx rather than cargo directly, so caching and project discovery stay Nx's
    // job, and no profile: every managed shell carries the cross toolchain.
    expect(CROSS_CHECK_SCRIPT_COMMAND).toEndWith(`direnv exec "$PWD" nx run-many -t ${CARGO_CROSS_LINT_TARGET}`);
    // The gate's environment: cleared, the identity variables and PATH kept, the checkout's devenv entered over it.
    expect(CROSS_CHECK_SCRIPT_COMMAND).toStartWith(
      `env -i HOME="$HOME" USER="$USER" LOGNAME="$LOGNAME" TERM="$TERM" LANG="$LANG" SSH_AUTH_SOCK="$SSH_AUTH_SOCK" PATH="$PATH" `,
    );
    // The hook keeps the same variables, so a verdict this script writes is one the probe asks for.
    const hook = await Bun.file(join(import.meta.dir, '../managed/raw/tooling/git-hooks/pre-push.sh')).text();
    expect(hook).toContain(`for name in ${DECLARED_COMMAND_IDENTITY.join(' ')}; do`);
    // The pre-push gate never compiles, so a human runs this, and an unbounded
    // clippy must show its progress.
    expect(CROSS_CHECK_SCRIPT_COMMAND).not.toContain('--quiet');
  });
});

/**
 * The managed pre-push hook, run for real in a fixture repository whose `tooling/direnv/repo-path` puts stand-ins
 * first on PATH: `uname` says Darwin, `smoo` passes the denylist, `direnv` enters the checkout's environment, and `nx`
 * prints what a cross run printed and exits with its status, refusing to run in any environment but the land gate's:
 * the checkout's devenv over cowshed's cleared declared-command environment (specs/cowshed/02_workspaces.md, "The
 * declared commands' environment"). The hook must ask there and call a miss a miss and every other failure what it is.
 */
describe('managed pre-push cross probe', () => {
  const hook = join(import.meta.dir, '../managed/raw/tooling/git-hooks/pre-push.sh');
  /** The SDK the checkout's environment names; the caller's shell names another. */
  const CHECKOUT_SDK = '/checkout/MacOSX.sdk';

  interface Push {
    readonly nxOutput: string;
    readonly nxStatus: number;
    /** Whether direnv is on the hook's PATH. */
    readonly direnv?: boolean;
  }

  async function push({ nxOutput, nxStatus, direnv = true }: Push): Promise<{ status: number | null; stderr: string }> {
    const root = await mkdtemp(join(tmpdir(), 'pre-push-cross-'));
    try {
      const bin = join(root, 'bin');
      await mkdir(bin);
      await mkdir(join(root, 'repo', 'tooling', 'direnv'), { recursive: true });
      const stub = async (name: string, body: string) => {
        await writeFile(join(bin, name), `#!/bin/sh\n${body}\n`);
        await chmod(join(bin, name), 0o755);
      };
      await stub('uname', 'echo Darwin');
      await stub('smoo', 'exit 0');
      // Nx keys on the environment it runs in: outside the checkout's, it is not the gate's run. `AR` stands for any
      // variable the caller's shell carries and the checkout does not declare, which `direnv exec` alone lets through:
      // the cargo toolchain input keys on it, and the land gate, which starts from a cleared environment, never sees it.
      await stub(
        'nx',
        `[ "$SDKROOT" = '${CHECKOUT_SDK}' ] || { echo "nx stub: keyed on the caller's SDKROOT $SDKROOT" >&2; exit 1; }\n[ -z "\${AR+set}" ] || { echo "nx stub: keyed on the caller's AR $AR, which the checkout does not declare" >&2; exit 1; }\n[ "$${CARGO_CROSS_LINT_REPLAY_ONLY_ENV}" = 1 ] || { echo "nx stub: the probe would let the cross target compile" >&2; exit 1; }\nprintf '%s\\n' '${nxOutput.replaceAll("'", "'\\''")}'; exit ${nxStatus}`,
      );
      if (direnv) {
        // The checkout's .envrc: `direnv exec <checkout>` enters the environment that checkout declares.
        await stub(
          'direnv',
          `[ "$1" = exec ] && [ "$2" = "$PWD" ] || { echo "direnv stub: unexpected $*" >&2; exit 64; }\nexport SDKROOT='${CHECKOUT_SDK}'\nshift 2\nexec "$@"`,
        );
      }
      // Without direnv, PATH holds the stand-ins alone: no host direnv is found, and no coreutils either, as on a
      // Nix-built Linux host whose /usr/bin and /bin hold none. The refusal must still be said.
      const path = direnv ? `${bin}:$PATH` : bin;
      await writeFile(join(root, 'repo', 'tooling', 'direnv', 'repo-path'), `#!/bin/sh\necho "${path}"\n`);
      await chmod(join(root, 'repo', 'tooling', 'direnv', 'repo-path'), 0o755);
      await writeFile(join(root, 'repo', 'tooling', 'direnv', 'ttsc-env.sh'), '');
      expect(spawnSync('git', ['init', '-q', join(root, 'repo')]).status).toBe(0);
      const run = spawnSync('bash', [hook, 'origin'], {
        cwd: join(root, 'repo'),
        // The caller's shell carries another directory's SDK and an archiver the checkout does not declare, as a shell
        // inside a parent directory's devenv does.
        env: { ...process.env, SDKROOT: '/nix/store/caller-apple-sdk/MacOSX.sdk', AR: 'ar' },
        input: '',
        encoding: 'utf8',
      });
      return { status: run.status, stderr: run.stderr };
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }

  it("passes a hit, asked in the checkout's environment rather than the caller's", async () => {
    const { status, stderr } = await push({ nxOutput: 'Successfully ran target cargo-lint-cross', nxStatus: 0 });
    expect(stderr).not.toContain("keyed on the caller's");
    expect(status).toBe(0);
  });

  it('names a miss when the cross target refused to compile', async () => {
    const { status, stderr } = await push({ nxOutput: CARGO_CROSS_LINT_REFUSAL, nxStatus: 1 });
    expect(status).toBe(1);
    expect(stderr).toContain('NOT cached for this tree');
    expect(stderr).toContain('bun run check:linux');
  });

  it('names any other failure as that, never as a cache miss', async () => {
    const { status, stderr } = await push({
      nxOutput: 'NX   NX_WORKSPACE_ROOT_PATH names another workspace',
      nxStatus: 1,
    });
    expect(status).toBe(1);
    expect(stderr).toContain('failed (exit 1) before it reached');
    expect(stderr).not.toContain('NOT cached');
  });

  it("refuses, naming direnv, when it cannot enter the checkout's environment", async () => {
    const { status, stderr } = await push({
      nxOutput: 'Successfully ran target cargo-lint-cross',
      nxStatus: 0,
      direnv: false,
    });
    expect(status).toBe(1);
    expect(stderr).toContain('direnv is not on PATH');
    expect(stderr).not.toContain('NOT cached');
  });
});
