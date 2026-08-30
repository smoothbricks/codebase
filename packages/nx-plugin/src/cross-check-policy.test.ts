import { describe, expect, it } from 'bun:test';

import {
  CARGO_CROSS_LINT_COMMAND,
  CARGO_CROSS_LINT_TARGET,
  CARGO_LINT_CLIPPY_COMMAND,
  CARGO_LINUX_TRIPLE,
  CROSS_CHECK_SCRIPT_COMMAND,
  CROSS_CHECK_SCRIPT_NAME,
  DEVENV_CROSS_PROFILE,
  withProjectCargoHome,
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

  it('gives each project its own CARGO_HOME so parallel clippy does not flock ~/.cargo', () => {
    expect(CARGO_CROSS_LINT_COMMAND).toContain('CARGO_HOME="$PWD/target/cargo-lint-cross-home"');
    expect(CARGO_CROSS_LINT_COMMAND).toContain('ln -sfn "$host_cargo_home/registry"');
    expect(CARGO_LINT_CLIPPY_COMMAND).toContain('CARGO_HOME="$PWD/target/cargo-lint-home"');
    expect(withProjectCargoHome('target/h', 'cargo clippy')).toContain('CARGO_HOME="$PWD/target/h" cargo clippy');
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
    expect(CROSS_CHECK_SCRIPT_COMMAND).toContain('--quiet');
  });

  it('uses the verb:qualifier root script convention', () => {
    expect(CROSS_CHECK_SCRIPT_NAME).toMatch(/^[a-z]+:[a-z-]+$/);
  });
});
