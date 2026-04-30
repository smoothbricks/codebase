# Nx Plugin

Local Nx plugin for workspace-standard package setup and missing inferred targets.

## Target Ownership

Official `@nx/js/typescript` owns TypeScript library inference. A package `tsconfig.lib.json` becomes the tool-output
target `tsc-js`; this plugin must not duplicate or rename that target.

`@smoothbricks/nx-plugin` owns only inferred targets Nx does not provide here:

- `typecheck-tests` and `typecheck-tests:watch` from `tsconfig.test.json`
- `test:watch` from explicit `test` commands for Bun and Vitest packages
- `zig-*` targets from `build.zig` steps such as `zig-wasm`
- aggregate `build` and `lint` targets

## Nx Target Naming

Target names are `{tool}-{output}` names. Use names like `tsc-js`, `tsdown-js`, and `zig-wasm`; `build` and `lint` are
aggregates.

Concrete targets come from concrete files:

- `typecheck-tests` is inferred from `tsconfig.test.json` and runs `tsc --noEmit -p tsconfig.test.json`.
- `typecheck-tests:watch` is inferred from `tsconfig.test.json` and runs the same typecheck in watch mode.
- `test:watch` is inferred when the package already defines an explicit Bun or Vitest `test` command. The plugin derives
  the corresponding watch command and makes it depend on `typecheck-tests`.
- `zig-*` is inferred from `build.zig`; each explicit `b.step("name", ...)` becomes `zig-name`.
- `build` is inferred only when the project has at least one concrete build target to run, such as `tsc-js` from the
  official TypeScript plugin, a package-local target like `tsdown-js`, or a `zig-*` target from this plugin. It depends
  on output-family wildcard targets: `*-js`, `*-web`, `*-html`, `*-css`, `*-ios`, `*-android`, `*-native`, `*-napi`,
  `*-bun`, and `*-wasm`.

This is why Zig appears in the convention: the plugin is not guessing from arbitrary Zig source. SmoothBricks requires
`build.zig` to expose named build steps so Nx can create cacheable, addressable targets from those steps.

Do not use colon-style Nx target names such as `build:wasm` or `lint:fix`. Nx CLI syntax already uses colons for
`project:target:configuration`, so colon target names are hard to read, easy to confuse with configurations, and awkward
to expose through package scripts. Package scripts may still use names like `build:wasm`; they should delegate to a real
target such as `nx run pkg:zig-wasm`.

There is no Nx `lint:fix` target; repository formatting is handled by the root `lint:fix` script.

`typecheck-tests` and `typecheck-tests:watch` are inferred only when `tsconfig.test.json` exists. Test typechecking must
not emit `dist-test`. `test:watch` is continuous and depends on `typecheck-tests` before entering Bun or Vitest watch
mode. Smoo validation creates/requires this config for test runners that do not typecheck test files by default.

`tsconfig.test.json` is not a TypeScript build-mode project. It should reference library tsconfigs it needs to typecheck
against, but the package root `tsconfig.json` should not reference `./tsconfig.test.json`. Nx runs test typechecking
through the inferred `typecheck-tests` target, not through `tsc --build`.

Zig targets are inferred only when `build.zig` declares at least one `b.step(...)`. Each step becomes a `zig-*` target,
so a package `build.zig` must have at least one step.

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
