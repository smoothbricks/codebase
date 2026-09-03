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

## Cargo Workspace Layouts

The plugin supports both Cargo placements without moving crate sources:

- **Package-rooted:** `packages/example/package.json` sits beside `packages/example/Cargo.toml`. The package owns the
  whole Cargo target set, and commands keep `cwd: packages/example`.
- **Repository-rooted:** the repository `package.json` sits beside the one root `Cargo.toml`, whose members may live
  below several `packages/*` projects. The root Nx project owns the single `cargo-fetch`, `cargo-test-compile`,
  `cargo-lint`, mutation, bench, and sweep targets. Each crate's `cargo-test-<crate>` target and declared Wasm/N-API
  output targets belong to the deepest Nx project directory containing that crate. All Cargo commands run from the
  repository root, select the crate with `-p <crate>`, and share the root `target/`; cross-project dependencies keep the
  runners in one serialization chain. Member and exclude patterns may use `*` and `?` in any path segment (for example,
  `packages/*/crates/*`); unsupported patterns and member patterns that match no directory fail graph construction
  instead of silently omitting crates.

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
  `cargo-test-compile` is one workspace `cargo test --no-run`. Each member crate gets a cached `cargo-test-<package>`
  run (30s per-test timeout like `bun test --timeout=30000`) whose inputs are that crate and its path deps, so unchanged
  crates are skipped. Package-rooted workspaces use `cargo nextest run --workspace -E 'package(<crate>)'`;
  repository-rooted workspaces additionally use `-p <crate>` to bind the runner to the project that owns the crate.
  Per-crate runners accept an empty nextest selection because a valid workspace member may have no tests and a hash
  partition may legitimately be empty. Those runs are chained across project boundaries so two cargos never share
  `target/`; `napi-debug` sits on that chain after compile. Clippy uses `--target-dir target/cargo-lint` so lint can
  overlap tests. A crate declaring `[package.metadata.smoothbricks.wasm-bindgen]` also receives the cacheable
  `cargo-wasm` output target in its owning project.
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
