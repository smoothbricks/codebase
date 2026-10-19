import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  CARGO_CROSS_LINT_COMMAND,
  CARGO_CROSS_LINT_REFUSAL,
  CARGO_CROSS_LINT_TARGET,
  CARGO_DENY_WARNINGS,
  CARGO_LINT_CLIPPY_COMMAND,
  CARGO_LINUX_TRIPLE,
  CROSS_CHECK_SCRIPT_COMMAND,
  CROSS_CHECK_SCRIPT_NAME,
  cargoFrozen,
  DEVENV_CROSS_PROFILE,
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

  it('passes clippy-driver no arguments, so the gate and a plain clippy share one unit set', () => {
    // Anything after `--` becomes CLIPPY_ARGS, part of every member's fingerprint.
    for (const command of [CARGO_LINT_CLIPPY_COMMAND, CARGO_CROSS_LINT_COMMAND]) {
      expect(command).not.toContain(' -- ');
    }
  });

  it('fails on warnings it replays from units a plain clippy already checked', async () => {
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
      const plain = cargoFrozen('clippy --workspace --all-targets');

      const first = await clippy(plain);
      expect(first).toMatchObject({ exitCode: 0 });
      expect(first.output).toContain('Checking warning-fixture');
      expect(first.output).toContain('unused variable');

      // Fresh: the gate replays the plain run's units and their warning, and fails on it.
      const gate = await clippy(CARGO_LINT_CLIPPY_COMMAND);
      expect(gate.exitCode).not.toBe(0);
      expect(gate.output).not.toContain('Checking warning-fixture');
      expect(gate.output).toContain('unused variable');
      expect(gate.output).toContain('warnings are denied by `build.warnings` configuration');

      // And the gate left the plain run's units Fresh in turn.
      const again = await clippy(plain);
      expect(again).toMatchObject({ exitCode: 0 });
      expect(again.output).not.toContain('Checking warning-fixture');
    } finally {
      await rm(project, { recursive: true, force: true });
    }
  });

  it('refuses to start without cargo-zigbuild instead of compiling', async () => {
    // Without this guard a cache miss outside the linux-cross toolchain would
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

  it('drives the Nx target through the cross profile from the root script', () => {
    expect(CROSS_CHECK_SCRIPT_NAME).toBe('check:linux');
    // Activates the profile...
    expect(CROSS_CHECK_SCRIPT_COMMAND).toContain(`-P ${DEVENV_CROSS_PROFILE}`);
    // ...via the managed wrapper, so it resolves devenv config in any fleet repo...
    expect(CROSS_CHECK_SCRIPT_COMMAND).toStartWith('tooling/devenv ');
    // ...and runs Nx rather than cargo directly, so caching and project
    // discovery stay Nx's job.
    expect(CROSS_CHECK_SCRIPT_COMMAND).toContain(`nx run-many -t ${CARGO_CROSS_LINT_TARGET}`);
    // The pre-push gate never enters the cross profile, so a human does — and
    // devenv's own progress has to reach them, or a 0.9 GiB toolchain closure
    // and an unbounded clippy are indistinguishable from a hang.
    expect(CROSS_CHECK_SCRIPT_COMMAND).not.toContain('--quiet');
  });

  it('uses the verb:qualifier root script convention', () => {
    expect(CROSS_CHECK_SCRIPT_NAME).toMatch(/^[a-z]+:[a-z-]+$/);
  });
});

/**
 * The managed pre-push hook, run for real in a fixture repository whose `tooling/direnv/repo-path` puts stand-ins
 * first on PATH: `uname` says Darwin, `smoo` passes the denylist, and `nx` prints what a cross run printed and exits
 * with its status. The hook must call a miss a miss and every other Nx failure what it is.
 */
describe('managed pre-push cross probe', () => {
  const hook = join(import.meta.dir, '../managed/raw/tooling/git-hooks/pre-push.sh');

  async function push(nxOutput: string, nxStatus: number): Promise<{ status: number | null; stderr: string }> {
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
      await stub('nx', `printf '%s\\n' "$NX_STUB_OUTPUT"; exit ${nxStatus}`);
      await writeFile(join(root, 'repo', 'tooling', 'direnv', 'repo-path'), `#!/bin/sh\necho "${bin}:$PATH"\n`);
      await chmod(join(root, 'repo', 'tooling', 'direnv', 'repo-path'), 0o755);
      await writeFile(join(root, 'repo', 'tooling', 'direnv', 'ttsc-env.sh'), '');
      expect(spawnSync('git', ['init', '-q', join(root, 'repo')]).status).toBe(0);
      const run = spawnSync('bash', [hook, 'origin'], {
        cwd: join(root, 'repo'),
        env: { ...process.env, NX_STUB_OUTPUT: nxOutput },
        input: '',
        encoding: 'utf8',
      });
      return { status: run.status, stderr: run.stderr };
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }

  it('passes a hit', async () => {
    expect((await push('Successfully ran target cargo-lint-cross', 0)).status).toBe(0);
  });

  it('names a miss when the cross target refused to compile', async () => {
    const { status, stderr } = await push(CARGO_CROSS_LINT_REFUSAL, 1);
    expect(status).toBe(1);
    expect(stderr).toContain('NOT cached for this tree');
    expect(stderr).toContain('bun run check:linux');
  });

  it('names any other Nx failure as that, never as a cache miss', async () => {
    const { status, stderr } = await push('NX   NX_WORKSPACE_ROOT_PATH names another workspace', 1);
    expect(status).toBe(1);
    expect(stderr).toContain('Nx failed (exit 1) before the Linux cross-compile check reached');
    expect(stderr).not.toContain('NOT cached');
  });
});
