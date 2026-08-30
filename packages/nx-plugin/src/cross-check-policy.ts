/**
 * The Linux compile gate, in one place.
 *
 * CI validates on Linux, so Rust behind a `cfg(target_os)` arm that only Linux
 * compiles is invisible to every check a macOS host can run: a full local lint
 * passes and CI still goes red on unused imports, unresolved names and dead code
 * inside the `not(target_os = "macos")` branch. Closing that needs three pieces
 * to agree — a devenv profile carrying the cross C toolchain, an Nx target per
 * Cargo workspace that asks for the triple, and a root script that runs the
 * second inside the first. They live here together because the plugin infers the
 * target while the CLI installs the script and reports a project missing it; kept
 * apart, the three spellings drift and the gate silently stops gating.
 */

/**
 * The triple CI's Linux job compiles, and the one a macOS host can cross-check.
 * `languages.rust.targets` in the managed devenv module installs its rust-std.
 */
export const CARGO_LINUX_TRIPLE = 'x86_64-unknown-linux-gnu';

/**
 * The devenv profile carrying the cross C compiler, defined in the managed
 * `tooling/direnv/devenv.smoo.nix`. Opt-in because its closure is 0.4 GiB and
 * nothing on Darwin links a Linux object.
 */
export const DEVENV_CROSS_PROFILE = 'linux-cross';

/**
 * Nx target name. No colons, per the repo's target-naming rule.
 *
 * Deliberately NOT suffixed `-linux`: that suffix is the reserved platform
 * family `LINUX_PLATFORM_TARGET_GLOBS`, which the publish workflow fans out over
 * to build release ARTIFACTS on a Linux runner, which `platformTargetFamily`
 * classifies by, and which flags a project as platform-bearing. A validation
 * target that emits nothing must not be swept into artifact machinery, so it
 * carries the neutral `-cross` qualifier — also outside the `-js`/`-napi`/`-wasm`
 * build-output families, so no aggregate `build` picks it up either.
 */
export const CARGO_CROSS_LINT_TARGET = 'cargo-lint-cross';

/**
 * Clippy rather than `cargo check`, and `-D warnings`, because that is exactly
 * what CI's `lint` runs; a weaker local command would pass where CI fails.
 *
 * `--all-targets` is what answers "what does test mean when cross-compiling": it
 * type-checks bins, tests, benches and examples for the target, so test CODE is
 * verified for Linux while nothing is executed — nothing can be, since an x86_64
 * Linux binary does not run on Apple Silicon. There is deliberately no separate
 * cross test target; it could only recompile these crates to produce a harness it
 * must then refuse to launch.
 *
 * `--target` is explicit rather than inherited from a CARGO_BUILD_TARGET exported
 * by the profile, so running this outside the cross shell fails loudly on a
 * missing tool instead of quietly linting the host and reporting green.
 *
 * `cargo fmt` is absent on purpose: formatting is target-independent and already
 * covered by `cargo-lint`, so repeating it here would only cost time.
 */
export const CARGO_CROSS_LINT_COMMAND = `[ -n "\${CC_x86_64_unknown_linux_gnu:-}" ] || [ "$(uname -s)" = Linux ] || { echo 'cargo-lint-cross needs the linux-cross C toolchain; run: bun run check:linux' >&2; exit 2; }; cargo clippy --workspace --all-targets --target ${CARGO_LINUX_TRIPLE} -- -D warnings`;

/** Root `package.json` script name, in the repo's `verb:qualifier` style. */
export const CROSS_CHECK_SCRIPT_NAME = 'check:linux';

/**
 * The developer entry point: one command on macOS that reports what CI's Linux
 * job would. `tooling/devenv` is the managed wrapper, so this resolves the
 * repository's devenv config from any repo in the fleet without naming its path,
 * and `--quiet` keeps the output to Nx's own.
 */
export const CROSS_CHECK_SCRIPT_COMMAND = `tooling/devenv --quiet -P ${DEVENV_CROSS_PROFILE} shell -- nx run-many -t ${CARGO_CROSS_LINT_TARGET}`;
