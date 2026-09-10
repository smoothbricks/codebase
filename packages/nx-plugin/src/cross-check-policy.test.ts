import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  CARGO_CROSS_LINT_COMMAND,
  CARGO_CROSS_LINT_TARGET,
  CARGO_LINT_CLIPPY_COMMAND,
  CARGO_LINUX_TRIPLE,
  CROSS_CHECK_SCRIPT_COMMAND,
  CROSS_CHECK_SCRIPT_NAME,
  cargoFrozen,
  DEVENV_CROSS_PROFILE,
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
    expect(CARGO_CROSS_LINT_COMMAND).toContain('-D warnings');
    expect(CARGO_CROSS_LINT_COMMAND).toContain(`--target ${CARGO_LINUX_TRIPLE}`);
  });

  it('fails closed on Darwin without the linux-cross compiler instead of compiling', () => {
    // Without this guard a Darwin cache miss sits in ring's cc-rs looking for
    // x86_64-linux-gnu-gcc. The linux-cross profile exports
    // CC_x86_64_unknown_linux_gnu; Linux CI is already the target.
    expect(CARGO_CROSS_LINT_COMMAND).toContain('CC_x86_64_unknown_linux_gnu');
    expect(CARGO_CROSS_LINT_COMMAND).toContain('uname -s');
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
  }, 30_000);

  it('pins clippy to the committed lockfile and local cache', () => {
    expect(cargoFrozen('clippy --workspace')).toBe('cargo --frozen clippy --workspace');
    expect(CARGO_CROSS_LINT_COMMAND).toContain('cargo --frozen clippy');
    expect(CARGO_LINT_CLIPPY_COMMAND).toContain('cargo --frozen clippy');
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
    // The probe-only pre-push gate never runs this, so a human does — and
    // devenv's own progress has to reach them, or a 0.4 GiB closure and an
    // unbounded clippy are indistinguishable from a hang.
    expect(CROSS_CHECK_SCRIPT_COMMAND).not.toContain('--quiet');
  });

  it('uses the verb:qualifier root script convention', () => {
    expect(CROSS_CHECK_SCRIPT_NAME).toMatch(/^[a-z]+:[a-z-]+$/);
  });
});
