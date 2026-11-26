# Managed monorepo files

`@smoothbricks/nx-plugin:managed-files` is the generator used by `smoo monorepo update`, `check`, and `diff`. The
workspace policy registers it as a global sync generator, so `nx sync` and `nx sync:check` use the same implementation.

The plugin owns the templates and workflow renderers. Repository manifests are read from Nx's `Tree`; inferred target
information comes from the resolved Nx graph passed at the boundary. `getProjects(Tree)` is used for workspace manifest
locations, not as a substitute for plugin-inferred targets. No generator installs dependencies or resolves secrets.
Generated workflows explicitly retain checkout credentials only when a job needs authenticated Git remote access after
checkout (push, fetch, or lazy blob fetch), and disable persistence elsewhere.

Test-runner packages also reconcile `tsconfig.test.json` through this generator. The shared test-typecheck policy
supplies no-emit defaults and workspace references from Tree, preserves declared test runtime options and `extends`, and
removes test-project references from package-root `tsconfig.json`. Check and diff report those same changes without
writing; update and native generation apply them with the other managed files. A config that already satisfies policy
keeps its exact bytes. If a needed repair would discard JSONC comments, generation refuses and names the config instead.

The content-ownership and context-derivation functions are pure. Generation stages changes in `Tree`. Updates flush
those changes with Nx; checks inspect them without writing. There is no serialized plan, second change journal, or
rollback system. A failed apply may leave a partial diff. Correct the failure and rerun, or use Git to restore the
intended state. Smoo never resets a working tree automatically.

Repository-owned tails and inline sections retain their existing markers. An orphaned marker or lost/ambiguous anchor is
an error, not permission to discard local content. Files disabled by repository capabilities are left untouched.
Matching in-workspace source symlinks are preserved; dangling, escaping, or drifted links are reported. A small
filesystem boundary inspects links and executable bits because Nx Tree does not expose that metadata. File contents come
from Tree. The formatter resolves the consumer's configuration from disk; formatter configuration staged only in Tree
takes effect on the next generation after that configuration is flushed.

Tests cover pure content rules, generator output with `createTreeWithEmptyWorkspace()`, and real filesystem boundaries.
The idempotence integration test applies the first result, starts a fresh Tree, and requires the second generation to
have no changes. CLI check, diff, and update use the same staging function. The existing Bun registry-install contracts
remain separate process tests; generator tests never require registry authentication.

The macOS release build and unit-test jobs still use the existing devenv environment. Moving generation into Nx does not
remove release validation or introduce another build environment.

The managed `tooling/direnv/devenv.smoo.nix` includes `jactionlint` in its package list and supplies it through an
overlay, pinned to the immutable upstream v2.0.2 release with per-archive SHA-256 hashes for macOS and Linux on x86_64
and arm64. The existing managed-file checks detect a missing or stale module, and `smoo monorepo update` provisions that
same module. Nix installs the prebuilt executable with the shell; no validation-time download or Go compilation occurs.
CI puts the same devenv profile bin on PATH, so CI uses the same pin.

The managed module also supplies `bun`, through an overlay on nixpkgs' own `bun` package, so every managed repository
runs one Bun build. The pin is the daily canary `1.4.5-canary.20261125.1` (Bun commit `0966e99d8`), because no Bun
release has what the fleet needs. `bun lint` exists in canary builds only (oven-sh/bun#46438). On macOS, Bun 1.4.5 and
earlier never reap a `Bun.spawn` child whose exit arrives during a nested event-loop tick, so the child stays a zombie
and its `exited` promise never settles (oven-sh/bun#45365, #45527); Bun main has the fix at `bd92f9bef`, 14 commits
before the pinned one. The archives are the npm registry's per-platform packages, for the three systems nixpkgs' own
package has (`@oven/bun-darwin-aarch64`, `@oven/bun-linux-aarch64` and `@oven/bun-linux-x64-baseline`), where a
published version is immutable and carries its SHA-512; GitHub's `canary` release is a rolling tag and is never fetched.
nixpkgs' derivation still does the rest: autoPatchelf on Linux, the ICU relink and re-sign on macOS.

This build prints 1.4.6 for `bun --version`, the release it precedes, so `packageManager` is `bun@1.4.6` and the
executors and cowshed's wrapper that compare the two agree. When Bun 1.4.6 is released the pin moves to that release and
`packageManager` stays. A repository's own overlays apply after the managed one: a repository that pinned a Bun release
in its `nixpkgs-overlay` removes that entry to run the managed build, and one that builds Bun itself keeps deciding
`pkgs.bun`.

The managed package list also provides `shellcheck-minimal` and an executable-only `pyflakes` package, pinned by the
existing devenv lockfile. The pyflakes package exposes only its checker executable, leaving the repository's configured
Python interpreter and build environment unchanged. Both default integrations remain enabled locally and in CI.

The managed `repo-path` helper keeps an active `VIRTUAL_ENV/bin` ahead of the devenv profile for shell entries and later
hook or tooling calls. With no active environment, its repository entries are unchanged. Its `--print` and
`--github-path` modes use the same entries, with the existing reverse order for GitHub PATH writes. Installed Cargo and
cowshed launchers still prepend their owned directory after composition, so activating Python does not replace them.

Its devenv profile entry is the profile the shell evaluated, `DEVENV_PROFILE`, when `DEVENV_ROOT` is this checkout's
`tooling/direnv`: a store path no other activation can move. Outside that shell — a git hook run from a plain terminal
or from another checkout's shell — it is the checkout's `tooling/direnv/.devenv/profile` link, the only name for that
checkout's profile there.

Shell fixtures for `github-actions-bootstrap` and `repo-path` resolve their required Bash and coreutils executables from
the active toolchain into private tool directories before scrubbing `PATH`. They run without `/usr/bin:/bin`, which does
not provide coreutils on NixOS. The install fixture still delegates lock evaluation to the real Nix executable, but
never adds its profile directory to the child search path, so an ambient devenv cannot choose the install branch.
