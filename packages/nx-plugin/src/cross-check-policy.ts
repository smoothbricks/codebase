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

const CARGO_CROSS_LINT_GUARD = `[ -n "\${CC_x86_64_unknown_linux_gnu:-}" ] || [ "$(uname -s)" = Linux ] || { echo 'cargo-lint-cross needs the linux-cross C toolchain; run: bun run check:linux' >&2; exit 2; }`;

/**
 * Point this cargo at a project-local home so parallel Nx cargos do not
 * exclusive-lock `~/.cargo/.package-cache`. Downloaded crates stay shared.
 */
export function withProjectCargoHome(homeRel: string, command: string): string {
  return `host_cargo_home="\${CARGO_HOME:-$HOME/.cargo}"; mkdir -p ${homeRel}; if [ -d "$host_cargo_home/registry" ]; then ln -sfn "$host_cargo_home/registry" ${homeRel}/registry; fi; if [ -d "$host_cargo_home/git" ]; then ln -sfn "$host_cargo_home/git" ${homeRel}/git; fi; CARGO_HOME="$PWD/${homeRel}" ${command}`;
}

/**
 * The prefix `cargoFrozen` writes. Exported so target inference can recognize
 * the commands whose precondition `CARGO_FETCH_TARGET` supplies without
 * re-spelling the flag; a second copy would silently stop matching.
 */
export const CARGO_FROZEN_PREFIX = 'cargo --frozen ';

/**
 * `--frozen` is cargo's `--locked` + `--offline`. Cargo.lock and the registry
 * cache are inputs: inferred cargo must not rewrite the lockfile or fetch.
 * The flag sits on `cargo` so nextest sees a cargo-level option, not its own.
 *
 * The registry cache being an INPUT is a precondition, and offline cargo
 * enforces it at RESOLUTION, before it selects a single target: an offline
 * registry source only reports packages already downloaded, so one absent
 * member of the lockfile's graph makes the whole workspace unresolvable with
 * `no matching package named <dep> found`. The failure therefore has nothing to
 * do with which targets a command builds — `build`, `check`, `test --lib` and
 * `test --no-run` all fail identically on a dependency only a bench needs — and
 * narrowing a command's target selection cannot cure it.
 *
 * Dev- and bench-only dependencies are the exposed class. A build downloads the
 * normal dependency graph, so on a runner that builds before it tests those are
 * warm by accident; nothing downloads a dev-dependency until something compiles
 * a test target, and by then `--frozen` refuses to. `CARGO_FETCH_TARGET` below
 * turns that accident into a stated dependency.
 */
export function cargoFrozen(args: string): string {
  return `${CARGO_FROZEN_PREFIX}${args}`;
}

/**
 * Nx target name. Downloads one Cargo workspace's locked dependency graph so
 * every `cargoFrozen` command in that workspace can resolve offline. No colons,
 * per the repo's target-naming rule, and outside the `-js`/`-napi`/`-wasm` and
 * platform-suffix families so no aggregate sweeps it up.
 */
export const CARGO_FETCH_TARGET = 'cargo-fetch';

/**
 * `--locked` rather than bare `cargo fetch`: this downloads exactly the graph
 * Cargo.lock pins and fails loudly on a stale lockfile. Without it a fetch could
 * re-resolve and hand `--frozen` a cache built from a different graph than the
 * lockfile names, which is the guarantee `--frozen` exists to give.
 *
 * `fetch` and nothing else, because fetching is the only act that needs the
 * network; every compile downstream stays frozen.
 *
 * No `--target`: with no triple named cargo fetches every target's
 * dependencies, so one run also covers `CARGO_CROSS_LINT_TARGET`'s Linux triple.
 */
export const CARGO_FETCH_COMMAND = 'cargo fetch --locked';

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
 *
 * `CARGO_HOME` is per-project (`$PWD/target/cargo-lint-cross-home`). Cargo's
 * package-cache flock lives in CARGO_HOME; three workspaces otherwise serialize
 * on `~/.cargo/.package-cache` even with distinct `--target-dir`. Registry and
 * git are linked to the host home so crates are not re-fetched. `--frozen`
 * keeps that shared registry read-only: lockfile and cache are inputs.
 */
export const CARGO_CROSS_LINT_COMMAND = `${CARGO_CROSS_LINT_GUARD}; ${withProjectCargoHome(
  'target/cargo-lint-cross-home',
  cargoFrozen(
    `clippy --workspace --all-targets --target ${CARGO_LINUX_TRIPLE} --target-dir target/cargo-lint-cross -- -D warnings`,
  ),
)}`;

export const CARGO_LINT_CLIPPY_COMMAND = withProjectCargoHome(
  'target/cargo-lint-home',
  cargoFrozen('clippy --workspace --all-targets --target-dir target/cargo-lint -- -D warnings'),
);

/** Root `package.json` script name, in the repo's `verb:qualifier` style. */
export const CROSS_CHECK_SCRIPT_NAME = 'check:linux';

/**
 * The developer entry point: one command on macOS that reports what CI's Linux
 * job would. `tooling/devenv` is the managed wrapper, so this resolves the
 * repository's devenv config from any repo in the fleet without naming its path,
 * and `--quiet` keeps the output to Nx's own.
 */
export const CROSS_CHECK_SCRIPT_COMMAND = `tooling/devenv --quiet -P ${DEVENV_CROSS_PROFILE} shell -- nx run-many -t ${CARGO_CROSS_LINT_TARGET}`;
