/**
 * The toolchain half of a cargo task's identity, shared by the two things that
 * must agree about it: target inference, which attaches it, and the monorepo
 * policy, which refuses a repository's own declaration that drops it.
 */

/**
 * The DECLARED pin, hashed as an ordinary file. devenv resolves this lock into
 * the rustc, cargo, linker, C toolchain and SDK that every cargo command
 * inherits, so a bump here is exactly the event that must invalidate a cached
 * artifact — and it is the only such event the workspace can state portably.
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
 * the `linux-cross` profile `AR` becomes `x86_64-unknown-linux-gnu-ar`,
 * `CARGO_INSTALL_ROOT` moves under `profiles/linux-cross/`, and
 * `CC_x86_64_unknown_linux_gnu` appears, so the entry `check:linux` writes
 * inside the profile could never be hit by the pre-push probe, which runs
 * bare. Measured on one repository-root archive target with the dump still in
 * place: bare 18197306115812424011, in-profile 12567954759860890767.
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
 * The name a repository's own `inputs` declaration uses to reach the pin
 * without restating its paths.
 *
 * A declared target REPLACES the inferred input list, pin included, so a
 * hand-written `inputs` array silently drops the toolchain identity and the
 * target stops seeing a toolchain bump at all. Inference defines this named
 * input on every project that has cargo targets so a declaration can name one
 * token that cannot drift from what the plugin attaches.
 */
export const CARGO_TOOLCHAIN_NAMED_INPUT = 'cargoToolchain';

/**
 * Does one resolved input string carry the toolchain identity?
 *
 * Resolved: named inputs already expanded. The check is by suffix rather than
 * by exact path because a repository may keep its lock at a location this list
 * does not name; what matters is that a devenv lock is hashed, not which one.
 */
export function isCargoToolchainInput(input: string): boolean {
  return !input.startsWith('!') && input.endsWith('/devenv.lock');
}
