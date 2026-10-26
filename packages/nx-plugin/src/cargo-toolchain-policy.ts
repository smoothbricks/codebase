import { runtimeInput, shellWord } from './runtime-input.js';
/**
 * The toolchain half of a cargo task's identity, shared by the two things that
 * must agree about it: target inference, which attaches it, and the monorepo
 * policy, which refuses a repository's own declaration that drops it.
 */

/**
 * The DECLARED pin, hashed as an ordinary file. devenv resolves this lock into
 * the rustc, cargo, linker, C toolchain and SDK that every cargo command
 * inherits, so a bump here is exactly the event that must invalidate a cached
 * artifact — and it is the half of the identity that a file can state portably.
 *
 * This replaces a runtime input that dumped the ambient cargo environment
 * (every `CARGO_`, `RUST`, `CC`, `CXX`, `AR`, `SDKROOT`, `LIBCLANG_PATH`,
 * `CMAKE_` and `ZIG` prefixed variable, plus the contents of
 * `$CARGO_HOME/config{,.toml}`). That bought the same
 * invalidation at the price of two defects. Its values are absolute
 * `/nix/store/<hash>-…` paths and per-checkout devenv state directories, so no
 * two machines ever agree and no cargo target can share a cache entry with a
 * peer or with CI — a remote cache for Rust was dead on arrival. And the dump
 * differs between a bare shell and a devenv profile for the SAME sources: in
 * the `linux-cross` profile `CARGO_INSTALL_ROOT` moves under
 * `profiles/linux-cross/`, and every variable the profile's tools set appears,
 * so the entry `check:linux` writes inside the profile could never be hit by
 * the pre-push probe, which runs bare. Measured on one repository-root archive
 * target with the dump still in place: bare 18197306115812424011, in-profile
 * 12567954759860890767.
 *
 * A file input has neither defect. The lock's bytes are identical bare and
 * in-profile and on every machine holding the checkout, while the toolchain
 * VERSIONS the dump was really guarding stay covered twice over — by the pin
 * itself, and by the `rustc -vV && cargo -V` runtime input attached beside it,
 * whose stdout is byte-identical in both shells (measured). Nothing
 * machine-local survives. A NARROWED dump was rejected rather than kept: a
 * subset re-acquires a store path the moment anyone widens it.
 *
 * Both fleet locations are stated: the pin lives under `tooling/direnv/` in
 * every repository and additionally at the root in some. A fileset that
 * matches nothing contributes nothing — the same property the inert
 * `rust-toolchain*` entries already rely on.
 */
export const CARGO_TOOLCHAIN_PIN_INPUTS: readonly string[] = [
  '{workspaceRoot}/devenv.lock',
  '{workspaceRoot}/tooling/direnv/devenv.lock',
];

/**
 * The runtime input naming the programs the shell declares for Cargo to compile and link with, by Nix store object:
 * the other half of the identity, which no file states (cargo-toolchain-paths.ts says what it names, and why). Nx runs
 * a runtime input from the workspace root, so the command names the plugin as the workspace's `node_modules` links it.
 */
const CARGO_TOOLCHAIN_PATHS_COMMAND = 'node node_modules/@smoothbricks/nx-plugin/dist/bin/smoo-nx-cargo-toolchain.js';

/** The runtime input of the programs the shell declares, a fresh value for each caller. */
export function cargoToolchainPathsInput(): { runtime: string } {
  return runtimeInput(CARGO_TOOLCHAIN_PATHS_COMMAND);
}

/** An input of a resolved list that can carry the identity: a fileset, or a runtime command. */
export type ReachedInput = string | { readonly runtime: string };

/** Every input a cargo task's key names of its toolchain: the pin files, then the programs the shell declares. */
export function cargoToolchainInputs(): (string | { runtime: string })[] {
  return [...CARGO_TOOLCHAIN_PIN_INPUTS, cargoToolchainPathsInput()];
}

/**
 * The name a repository's own `inputs` declaration uses to reach the whole
 * toolchain identity without restating it.
 *
 * A declared target REPLACES the inferred input list, pin included, so a
 * hand-written `inputs` array silently drops the toolchain identity and the
 * target stops seeing a toolchain bump at all. Inference defines this named
 * input on every project that has cargo targets so a declaration can name one
 * token that cannot drift from what the plugin attaches.
 *
 * The identity holds an input no fileset can state, so a declaration names this
 * rather than restating the lock.
 */
export const CARGO_TOOLCHAIN_NAMED_INPUT = 'cargoToolchain';

/** The toolchain versions `command` prints, from the directory whose rust-toolchain selects them. */
export function cargoRuntimeInput(projectRoot: string, command: string): { runtime: string } {
  return runtimeInput(`sh -c ${shellWord(`cd "$0" && ${command}`)} ${shellWord(projectRoot)}`);
}

/**
 * Does one resolved input string hold a toolchain lock?
 *
 * Resolved: named inputs already expanded. The check is by suffix rather than
 * by exact path because a repository may keep its lock at a location this list
 * does not name; what matters is that a devenv lock is hashed, not which one.
 */
function isCargoToolchainLock(input: string): boolean {
  return !input.startsWith('!') && input.endsWith('/devenv.lock');
}

/**
 * Does a resolved input list carry the toolchain identity?
 *
 * `reached` is what a declaration's inputs hold once their named inputs are resolved. The inference-owned
 * `cargoToolchain`, left unresolved, stands for the whole identity. Otherwise the list must hold both halves itself: a
 * devenv lock that nothing in the list excludes, and the programs runtime input exactly as inference spells it. A lock
 * restated without the programs is refused, however it is reached: no fileset can state the programs the shell
 * declares, so a list that stops at the lock misses a change to the linker.
 */
export function carriesCargoToolchain(reached: readonly ReachedInput[]): boolean {
  if (reached.includes(CARGO_TOOLCHAIN_NAMED_INPUT)) return true;
  const excluded = new Set(
    reached.flatMap((input) => (typeof input === 'string' && input.startsWith('!') ? [input.slice(1)] : [])),
  );
  const hasLock = reached.some(
    (input) => typeof input === 'string' && isCargoToolchainLock(input) && !excluded.has(input),
  );
  const programs = cargoToolchainPathsInput().runtime;
  return hasLock && reached.some((input) => typeof input === 'object' && input.runtime === programs);
}
