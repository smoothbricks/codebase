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

## Nx Target Naming

Target names are `{tool}-{output}` names. Use names like `tsc-js`, `tsdown-js`, and `cargo-wasm`; `build` and `lint` are
aggregates.

Concrete targets come from concrete files:

- `tsc-js` emits transformed JavaScript through a temporary JS-only `ttsc` config, then emits declarations and
  declaration maps from the original project with native `tsc`. Direct project references and every cached output lane
  are preserved.
- `typecheck` is inferred from `tsconfig.lib.json` and runs native `tsc -p tsconfig.lib.json --noEmit`.
- `typecheck-tests` is inferred from `tsconfig.test.json` and runs `tsc -p tsconfig.test.json --noEmit`. It first
  rebuilds the current package's `tsc-js` output (or its non-TypeScript `build`) so self-imports resolve after `clean`.
- `typecheck-tests:watch` is inferred from `tsconfig.test.json` and runs the same typecheck in watch mode.
- `test:watch` is inferred when the package already defines an explicit Bun or Vitest `test` command. The plugin derives
  the corresponding watch command and makes it depend on `typecheck-tests`.
- A workspace-root `Cargo.toml` provides `cargo-test`, `test`, `cargo-lint`, `mutation`, and `bench`.
  `cargo-test-compile` stages test binaries out of `target/` so `cargo-test` can run them in parallel with a per-binary
  timeout without taking Cargo's exclusive target lock (which cannot overlap `napi-debug` or any other cargo writer on
  the same directory). APFS test binaries get `--test-threads=1`. Workspaces with `cdylib` member crates also receive
  the cacheable `cargo-wasm` output target.
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
