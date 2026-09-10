# Nx Plugin

Local Nx plugin for workspace-standard package setup and missing inferred targets.

## Target Ownership

Official `@nx/js/typescript` inference is disabled because it cannot run the workspace's source transformers. A package
with `tsconfig.lib.json` receives transformer-aware `tsc-js` and native `typecheck` targets from this plugin.

`@smoothbricks/nx-plugin` also owns inferred targets Nx does not provide here:

- `typecheck-tests` and `typecheck-tests:watch` from `tsconfig.test.json`
- `test:watch` from explicit `test` commands for Bun and Vitest packages
- Cargo workspace targets from a neighboring workspace-root `Cargo.toml`
- aggregate `build` and `lint` targets

Lint commands are inferred per project. A workspace Biome configuration enables the project-wide Biome check; an ESLint
flat configuration enables ESLint only for existing JavaScript/TypeScript files under that project's `src`. Rust-only
source trees therefore keep their Cargo validation and manifest checks without invoking ESLint on nonexistent JavaScript
inputs. Adding JavaScript or TypeScript sources adds their lint coverage automatically.

Keep `targetDefaults.lint` limited to shared cache policy. Smoo's workspace policy removes static lint executors,
commands, dependencies, inputs and outputs because they override source-aware inference for every project. Project-local
`nx.targets.lint` declarations retain normal Nx override precedence.

## Cargo Workspace Layouts

The plugin discovers Cargo workspaces beside Nx project manifests at any depth:

- **Workspace owner:** the project beside a workspace `Cargo.toml` owns one `cargo-fetch`, `cargo-test-compile`,
  `cargo-test-archive`, `cargo-lint`, cross-check, mutation, bench, and sweep target. Its `cargo-lint` and `cargo-test`
  aggregates reach all workspace crates, including a root `[package]`.
- **Crate owner:** the deepest Nx project directory containing a crate owns `cargo-test-<crate>` targets. Its
  `cargo-lint` delegates to the workspace owner's one verdict, and `lint` never depends on the workspace test suite.
  Projects can own multiple crates. A nested independent workspace has its own prerequisites and does not inherit those
  of an outer workspace.
- **Commands:** validation runs from the governing Cargo workspace directory. One `cargo fmt --all --check` and one
  `cargo --frozen clippy --workspace --all-targets --target-dir target/cargo-lint -- -D warnings` cover every member.
  Clippy of a crate is a check build of its whole closure, so a target directory per crate rebuilt every shared
  dependency once per crate — 49 closures and 11 GiB on one repository — and no per-crate cache hit repaid it. Cargo's
  own lock serializes the invocations that share the directory.
- **Overrides:** normal Nx merging applies. Explicit `nx.targets` fields replace inferred fields, while omitted fields
  retain their inferred base. Use `"dependsOn": ["...", "extra"]` to preserve inferred prerequisites when adding an
  edge; replacing the array makes its author responsible for fetching before frozen Cargo commands.

Member and exclude patterns may use `*` and `?` in any path segment (for example, `packages/*/crates/*`). Unsupported
patterns and member patterns that match no directory fail graph construction instead of silently omitting crates.

### Cargo cache boundaries

`cargo-fetch` is uncached because the registry lives outside Nx outputs. `cargo-test-compile` is also uncached: it warms
Cargo's shared mutable build directory before the bounded runner, even if that directory was deleted after an earlier
successful run. Cargo's own incremental cache avoids recompiling unchanged code. Nx never restores or collects shared
Cargo build directories; lint and test **results**, and dedicated Wasm/N-API **outputs**, remain cacheable.

Validation inputs include the crate's files, local dependency closure, governing manifests and configuration, toolchain
versions, Cargo/Rust/native-build environment, target/profile overrides, and global Cargo configuration. Workspace test
runners additionally hash all workspace member manifests because those affect unified features, and the nextest
executable/configuration. Production test configurations use release compilation and release runners together. Keep
explicit inputs for arbitrary build-script reads or custom environment variables that Cargo metadata cannot name.

Cargo keeps the caller's `CARGO_HOME`. Moving configuration into an isolated home can change relative paths or lose
source replacement, credentials, and toolchain settings; forwarding it with `--config` changes precedence. Registry
access may consequently serialize on Cargo's package-cache lock. Clippy's dedicated target directory keeps its check
artifacts out of the test build directory without splitting one directory per crate.

### External Rust sources

For a repository-root Cargo workspace, declare the shared input once:

```json
{
  "namedInputs": {
    "externalRustCrates": [{ "runtime": "smoo-nx-cargo-hash" }]
  }
}
```

For a package-root Cargo workspace, pass its manifest relative to the Nx root:
`smoo-nx-cargo-hash packages/example/Cargo.toml`. A shared input covering several Cargo workspaces includes one runtime
entry per manifest.

The command queries `cargo metadata --locked --offline` and hashes the resolved external path packages, including
transitive dependencies, Rust sources, target source files, governing Cargo manifests, and Cargo configuration. It also
covers path packages under `node_modules`, which Nx's normal file map ignores. Adding or moving a dependency does not
require maintaining a second list of source roots. The command refuses missing dependencies or an unavailable locked
dependency cache instead of emitting a partial digest.

Git and registry dependencies are identified by `Cargo.lock`; uncommitted changes in a Git repository are not changes to
a pinned Git dependency. Local `path` dependencies stay live and their source edits change the digest. Build-script data
and environment inputs outside the Rust/Cargo inputs above still require explicit Nx inputs.

### Manifest versions and validation inputs

A release rewrites `version` in every package manifest it publishes and in the lockfile entries mirroring them. Those
files are inputs of `lint`, `typecheck` and `typecheck-tests`, so a publish run misses the cache for the whole gate set
over a change that alters no code. Those three targets therefore leave the raw manifests out of their filesets and hash
them by field instead: a `json` input with `excludeFields` for `package.json#version`, and one dropping the lockfile's
`workspaces` section, which is only the mirror of each member's own manifest. Nx hashes both in its native hasher, so
this costs no process. Everything else still counts: a dependency range, an export map, and the lockfile's resolution
table.

Crate manifests are the one kind Nx cannot hash by field, because TOML has no such input. They go through
`smoo-nx-manifest-hash`, which removes `[package].version` and `[workspace.package].version` and nothing else — a
`version` naming a *different* crate under a dependency table stays in the digest, and a manifest the command cannot
parse is hashed raw rather than dropped. A `runtime` input is a process spawn per project per graph computation, and Nx
re-runs an identical command string two to three times rather than memoizing it (measured: 30–39 spawns and +18% wall
time on a fully-cached run when every project declared one), so the plugin declares it only for projects that actually
carry a `Cargo.toml`.

Targets that produce a shipped artifact keep hashing the raw manifests. A crate embeds its version at compile time
through `env!("CARGO_PKG_VERSION")`, so a version-insensitive `build`, `pack`, `tsc-js` or cargo hash would let a
post-bump run hit a pre-bump artifact and publish a binary reporting the previous version.

A dependency's manifests are hashed the same way once the workspace declares the named input, which gives projects this
plugin does not infer a definition to resolve:

```json
{
  "namedInputs": {
    "versionlessProduction": ["production"]
  }
}
```

Without it the dependency half stays `^production`. On an Nx older than 23.2, which rejects a `json` input rather than
ignoring it, the targets keep today's inputs entirely; the same is true of crate manifests when the command is not
installed. Every fallback costs cache hits and never trades away invalidation, because Nx runs a `runtime` input
without reporting a failing one — a fileset that excluded a manifest with no digest replacing it would serve stale
results silently.

## Nx Target Naming

Target names are `{tool}-{output}` names. Use names like `tsc-js`, `tsdown-js`, and `cargo-wasm`; `build` and `lint` are
aggregates.

Concrete targets come from concrete files:

- `tsc-js` emits transformed JavaScript through a temporary JS-only `ttsc` config, restores execute bits on outputs
  declared by `package.json#bin`, then emits declarations and declaration maps from the original project with native
  `tsc`. Direct project references and every cached output lane are preserved.
- `typecheck` is inferred from `tsconfig.lib.json` and runs native `tsc -p tsconfig.lib.json --noEmit`.
- `typecheck-tests` is inferred from `tsconfig.test.json` and runs `tsc -p tsconfig.test.json --noEmit`. It first
  rebuilds the current package's `tsc-js` output (or its non-TypeScript `build`) so self-imports resolve after `clean`.
- `typecheck-tests:watch` is inferred from `tsconfig.test.json` and runs the same typecheck in watch mode.
- `test:watch` is inferred when the package already defines an explicit Bun or Vitest `test` command. The plugin derives
  the corresponding watch command and makes it depend on `typecheck-tests`.
- A workspace-root `Cargo.toml` provides `cargo-test`, `test`, `cargo-lint`, `mutation`, and `bench`.
  `cargo-test-compile` warms one workspace `cargo test --no-run`. `cargo-test-archive` runs one
  `cargo --frozen nextest archive --workspace` into `target/nextest/archive.tar.zst` and is the only cached cargo BUILD:
  it produces a file rather than a mutable build tree. Each member crate gets a cached `cargo-test-<package>` run (30s
  per-test timeout like `bun test --timeout=30000`) whose inputs include that crate and its path dependencies, and whose
  command is `cargo --frozen nextest run --archive-file <archive> --workspace-remap . -E 'package(<crate>)'`. Runners
  therefore execute rather than build: they extract binaries into their own temporary directory, write nothing to
  cargo's flocked `target/`, and fan out instead of chaining. `--workspace-remap` is required — an archive records the
  producing tree's absolute paths, and without it a restored archive hands tests another checkout's
  `CARGO_MANIFEST_DIR`. With it the archive is relocatable, so one cache entry serves every checkout of the same commit;
  nextest re-points `CARGO_BIN_EXE_<name>`/`NEXTEST_BIN_EXE_<name>` at the extracted binaries at runtime, but a test
  that reads them through the compile-time `env!` macro keeps the producing tree's path. Per-crate runners accept an
  empty nextest selection because a valid workspace member may have no tests and a hash partition may legitimately be
  empty. `napi-debug` stays behind `cargo-test-compile`, and a crate in the project that builds the debug cdylib runs
  after it. A crate declaring `[package.metadata.smoothbricks.wasm-bindgen]` also receives the cacheable `cargo-wasm`
  output target in its owning project.
- The plugin's own `nextest.toml` is passed as `--tool-config-file "smoo:$PWD/<path>"`, so it is a layer BENEATH the
  repository's `<cargo-workspace>/.config/nextest.toml` rather than a replacement for it: a repository can raise a
  timeout, add a test group, or declare `archive.include` for a cdylib or fixture the archived test binaries need, and
  its settings win. That file is an input of both the archive and every runner, so changing it invalidates the verdicts
  it governs. `$PWD` keeps the command text identical across checkouts while satisfying nextest's requirement that a
  tool config path be absolute; `--user-config-file none` still holds, because a developer's `~/.config/nextest` must
  not decide a cached verdict.
- A crate whose suite outgrows one bounded window declares `[package.metadata.smoothbricks.test] shards = N`, and gets
  `cargo-test-<package>-shard1..N`, each running `--partition hash:i/N` with the full bound. nextest assigns a test to a
  shard by hashing its name, so the shards stay an exact partition of the crate as tests and test binaries are added,
  and a stale `N` can only make a target slow — never drop a test. Omitting the key means one target, as before.
- Tests that a `nextest.toml` override singles out are lifted out of the hash into `cargo-test-<package>-exceptions`,
  and the shards run the complement. An override marks a class that does not behave like the rest of the suite, and each
  kind breaks a shard differently: a `test-group` only holds within one nextest run, so leaving its members to the hash
  would scatter them across runs and dissolve the mutex; a raised `slow-timeout` marks a test whose cost is not the
  suite's, such as a compile-fail test that rustc's a fixture for 25.6s on a cold target directory against 1.8s warm —
  and every CI runner is cold. One target holds both classes, not one each: they occupy different threads, so its wall
  is the max of the classes rather than their sum. The pin is derived by reading the overrides back out of
  `nextest.toml`, so declaring one there is the whole change. Only a sharded crate gets this target — an unsharded crate
  already runs its whole suite in a single process — and it passes on an empty set, which is the correct answer for a
  platform where the singled-out tests are `cfg`-ed out.
- Canonical `napi` package metadata provides a host `cargo-napi` target and named release targets for each configured
  triple. Linux `--use-napi-cross` targets compile C/C++ dependencies with Clang; the NAPI CLI supplies its downloaded
  GNU sysroot and toolchain flags. This avoids the bundled GCC's unsupported diagnostics-color flag without disabling
  the workspace's sccache wrapper.
- Each `--use-napi-cross` triple also gets a `napi-toolchain-<arch>-linux` prerequisite that extracts the pinned
  `@napi-rs/cross-toolchain-<host>-target-<arch>` archive into `~/.napi-rs`, where the NAPI CLI probes for it. Every
  cross build of that triple — the inferred `napi-<arch>-linux` and any package-local `cli-<arch>-linux` — depends on
  it, so the CLI's own downloader never runs. That downloader `npm pack`s into its own package directory, which under
  Bun's isolated global store is a shared (in CI host-wide) cache: two concurrent cross builds of one triple would
  otherwise pack the same file into the same directory and one would die on the other's cleanup. The prerequisite
  produces no artifact, so it stays out of the aggregate `build` and out of collected platform outputs.
- `build` is inferred only when the project has at least one concrete build target to run, such as inferred `tsc-js`, a
  package-local target like `tsdown-js`, or `cargo-wasm` from this plugin. It depends on output-family wildcard targets:
  `*-js`, `*-web`, `*-html`, `*-css`, `*-ios`, `*-android`, `*-native`, `*-napi`, `*-bun`, and `*-wasm`.

### Dependency output lanes

Compiler targets depend on `^*-js`, not `^build`. The caret asks Nx for every matching JavaScript output target on
project dependencies; it does not directly select their Wasm, N-API, native, web, or aggregate `build` targets. This
keeps a declaration or JavaScript compile from paying for unrelated platform artifacts merely because a dependency
package publishes them.

The selected JavaScript target retains its own `dependsOn` edges. A platform artifact that is genuinely required to
produce that JavaScript therefore still participates transitively:

```text
consumer:tsc-js
└─ dependency:bundle-js
   └─ dependency:cargo-wasm
```

A wildcard without a caret selects the current project instead. For example, a workspace may add `*-wasm` to its
`tsc-js` target default when the current package's generated Wasm module must exist during compilation. The plugin makes
this local relationship explicit for inferred `tsc-js` targets that also have inferred `cargo-wasm`. Package-managed
JavaScript targets with another platform prerequisite must likewise declare that local edge themselves.

N-API binaries are not prerequisites of TypeScript declaration emission, so compiler targets do not pull them in by
default. Aggregate `build` remains the package-completeness boundary and includes every published local output family.

Do not use colon-style Nx target names such as `build:wasm` or `lint:fix`. Nx CLI syntax already uses colons for
`project:target:configuration`, so colon target names are hard to read, easy to confuse with configurations, and awkward
to expose through package scripts. Package scripts may still use names like `build:wasm`; they should delegate to a real
target such as `nx run pkg:cargo-wasm`.

There is no Nx `lint:fix` target; repository formatting is handled by the root `lint:fix` script.

`typecheck-tests` and `typecheck-tests:watch` are inferred only when `tsconfig.test.json` exists. Test typechecking must
not emit `dist-test`. `test:watch` is continuous and depends on `typecheck-tests` before entering Bun or Vitest watch
mode. Smoo validation creates/requires this config for test runners that do not typecheck test files by default.

`tsconfig.test.json` is not a TypeScript build-mode project. It should reference library tsconfigs it needs to typecheck
against, but the package root `tsconfig.json` should not reference `./tsconfig.test.json`. Nx runs test typechecking
through the inferred `typecheck-tests` target, not through `tsc --build`.

## Ensure a Target Before Executing Its Binary

`ensureBuilt` checks a target and its task dependencies against Nx's local cache and the daemon's recorded output
hashes. A full hit returns without running tasks or printing output. A miss runs the task graph through Nx's in-process
runner with streaming output; only a workspace with the daemon disabled falls back to its checkout-local `nx` CLI.

```typescript
import { ensureBuilt } from '@smoothbricks/nx-plugin/ensure-built';

const result = await ensureBuilt({ target: 'my-cli:build', cwd: workspaceRoot });
if (result.disposition === 'failed') {
  if (result.signal !== null) {
    process.kill(process.pid, result.signal);
  }
  process.exit(result.exitCode);
}
```

The packaged `smoo-nx-exec` wrapper performs the build check and then replaces itself with the binary:

```bash
smoo-nx-exec my-cli:build -- ./packages/my-cli/dist/my-cli argument
```

It is deliberately not `nx run <target> && exec`:

- A hit costs one daemon round-trip and a local cache read, not an Nx CLI start.
- A hit is silent. `nx run` replays the cached log and prints its `[local cache]` banner, which a CLI wrapper must not
  print.
- It execs the binary, preserving its argv, exit status, and signals. `nx run` has no notion of handing the process over
  to another binary.

Pass `--workspace-root <dir>` before `--` when invoking the wrapper outside the workspace. The binary path resolves
against the caller's current directory.

## Bun Test Tracing Generator

Configure a package for the Bun test tracing + no-emit test typechecking pattern used in this repo.

```bash
nx generate ./packages/nx-plugin:bun-test-tracing \
  --project @smoothbricks/my-package \
  --opContextModule @smoothbricks/lmao \
  --opContextExport lmaoOpContext \
  --tracerModule @smoothbricks/lmao/testing/bun
```

What it wires:

- `bunfig.toml` preloads for the LMAO Bun test tracing setup
- `src/test-suite-tracer.ts`
- `tsconfig.test.json` with `noEmit` for inferred `typecheck-tests`
- direct test config references to library tsconfigs; package root `tsconfig.json` is left out of the test config graph
- package `package.json` test/lint/devDependency wiring needed for the standard pattern

## Bounded Test Targets

`@smoothbricks/nx-plugin:bounded-exec` runs a shell command with a timeout and force-kill grace period. Test targets use
this executor so hung test processes fail predictably instead of blocking Nx indefinitely.

The shared policy API is exported from `@smoothbricks/nx-plugin/bounded-test-policy` for generators or other workspace
tools that need to normalize package JSON consistently.

```bash
nx generate ./packages/nx-plugin:bounded-test-targets --project @smoothbricks/my-package
```

The generator rewrites `package.json` so `nx.targets.test` uses:

- executor `@smoothbricks/nx-plugin:bounded-exec`
- command preserved from an existing `nx:run-commands` test target or direct `scripts.test`
- `cwd: "{projectRoot}"`
- `timeoutMs: 600000`
- `killAfterMs: 10000`
- package script alias `nx run <project>:test --outputStyle=stream`
