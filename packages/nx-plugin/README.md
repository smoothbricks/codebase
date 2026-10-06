# Nx Plugin

Local Nx plugin for workspace-standard package setup and missing inferred targets.

## Target Ownership

Official `@nx/js/typescript` inference is disabled because it cannot run the workspace's source transformers. A package
with `tsconfig.lib.json` receives transformer-aware `tsc-js` and native `typecheck` targets from this plugin.

`@smoothbricks/nx-plugin` also owns inferred targets Nx does not provide here:

- `typecheck-tests` and `typecheck-tests:watch` from `tsconfig.test.json`
- `test-watch` from explicit `test` commands for Bun and Vitest packages
- Cargo workspace targets from a neighboring workspace-root `Cargo.toml`
- aggregate `build` and `lint` targets
- an uncached `deploy` for a PRIVATE project with a wrangler manifest, running
  `smoo wrangler deploy-stage --stage {args.stage}`

The deploy is one target that takes whichever stage it is given, never a configuration per stage: pull-request stages
are `prN` for unbounded N, so `nx run <project>:deploy --stage=pr42` has to work for a stage no enumeration could list.
It is never cached, because a cache hit would mean "we once uploaded this hash", which a rollback falsifies silently.
Declaring `deploy` locally overrides only the properties it names, so a project adds cross-project ordering with
`"dependsOn": ["...", "backend:deploy"]` and keeps the inferred command; a project that declares `deploy-build` gets
that edge instead of `build`, and the plugin caches that half without inventing its command.

`private: true` is half the detection, because a wrangler manifest alone does not mean deployable: a published library
ships one to document the Durable Object binding it implements, and that manifest carries the same `name`, `main` and
`compatibility_date` a deployable worker's does. So the inferred target goes to packages npm will never publish, and a
published package that really is deployed declares `deploy` itself. Whether CI deploys any of them is a separate
question, answered only by the deploy tags the CLI reads (`stage-deploy-target` and its three siblings).

Lint commands are inferred per project. A workspace Biome configuration enables the project-wide Biome check; an ESLint
flat configuration enables ESLint only for existing JavaScript/TypeScript files under that project's `src`. Rust-only
source trees therefore keep their Cargo validation and manifest checks without invoking ESLint on nonexistent JavaScript
inputs. Adding JavaScript or TypeScript sources adds their lint coverage automatically.

Keep `targetDefaults.lint` limited to shared cache policy. Smoo's workspace policy removes static lint executors,
commands, dependencies, inputs and outputs because they override source-aware inference for every project. Project-local
`nx.targets.lint` declarations retain normal Nx override precedence.

The plugin's own `nx run nx-plugin:test` aggregates four bounded Bun targets. Each uses Bun's native
`--timings=test-timings.json --shard=N/4` partition to balance measured per-file work, while normal discovery still runs
every test file (including newly added files) exactly once. A failed shard fails the aggregate. The shard count and
standard 120-second target deadline stay unchanged. Refresh the portable timing manifest from this package directory
with `bun test --timeout=30000 --timings=test-timings.json --update-timings`; it is scheduling data, not a test
allowlist.

Real Nx fixtures own their cache and workspace-data directories and stop their daemon before removing the fixture.
Fixtures that intentionally strip caller directory overrides declare `cacheDirectory: ".nx/cache"` in their own
`nx.json`; this keeps the native task database local too, rather than leaving a new `~/.nx/<repoKey>` behind. Offline,
path-only Cargo fixtures likewise own a temporary `CARGO_HOME`; production commands still keep the caller's Cargo home.

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
  `cargo --frozen clippy --workspace --all-targets --config 'build.warnings="deny"'` cover every member. Clippy of a
  crate is a check build of its whole closure, so a target directory per crate rebuilt every shared dependency once per
  crate — 49 closures and 11 GiB on one repository — and no per-crate cache hit repaid it. Clippy writes the workspace's
  own `target/`, like every dev build: its check units carry their mode in the unit hash and never collide with build
  units, and the host units a dev build compiled (build scripts and their host-only dependencies) are reused instead of
  compiled a second time. Warnings are denied by Cargo's `build.warnings`, not `-- -D warnings`: arguments after `--`
  are clippy-driver's `CLIPPY_ARGS`, which enter every member's fingerprint, so the gate and a plain
  `cargo clippy --workspace --all-targets` would re-check each other's units. `build.warnings` also fails on warnings
  replayed from Fresh units, so the two share one unit set and the gate still fails. Cargo's own lock serializes the
  invocations that share the directory. `cargo-lint-cross` keeps `target/cargo-lint-cross`: a foreign triple's units are
  distinct anyway, and its own lock keeps the cross check from queueing behind host builds.
- **Scheduling:** inference never throttles Nx. It sets no `parallelism: false` or `parallel: false`, no cap, and no
  edge whose only purpose is keeping two cargo writers off one `target/`: Cargo owns that lock, and a second process
  waits inside cargo. An edge states a real consumer — fetch before frozen cargo, the archive before the runners,
  `napi-debug` before the tests that load its addon. A producer and its consumer inside one task are one `&&` command
  (`mkdir` then the archive, `cargo build` then `wasm-bindgen`); independent commands (`cargo fmt` and clippy,
  per-manifest `cargo fetch`, Biome and ESLint) stay in a `commands` list that Nx runs side by side.
- **Overrides:** normal Nx merging applies. Explicit `nx.targets` fields replace inferred fields, while omitted fields
  retain their inferred base. Use `"dependsOn": ["...", "extra"]` to preserve inferred prerequisites when adding an
  edge; replacing the array makes its author responsible for fetching before frozen Cargo commands.

Member and exclude patterns may use `*` and `?` in any path segment (for example, `packages/*/crates/*`). Unsupported
patterns and member patterns that match no directory fail graph construction instead of silently omitting crates.

### Cargo profiles

The default configuration of every cargo target compiles cargo's `dev` profile: every local build and test, gates, CI
validation and `nx build` share one `target/` and one set of units, so cargo recompiles only what changed. The
`production` configuration adds `--release` to the targets that build shipped artifacts (`cargo-napi`,
`napi-<arch>-<os>`, `cargo-wasm`) and is selected only where an artifact ships: the generated publish workflow's build
and platform-leg steps, the `smoo release` commands it runs, and the build and deploy runs of a production
`smoo github-ci nx-deploy`. Test targets have no `production` configuration, so a release build never compiles release
test binaries. A package-local target that builds a shipped artifact follows the same rule: its default options compile
`dev` and its `production` configuration compiles a shipping profile. A shipping profile other than `release` is only
for an artifact family no other build compiles, such as a size-optimised wasm32 lane; a profile that compiles the same
crates for the same target as `dev` or `release` compiles every dependency a second time.

### Cargo cache boundaries

`cargo-fetch` is uncached because the registry lives outside Nx outputs. `cargo-test-compile` is also uncached: it warms
Cargo's shared mutable build directory before the bounded runner, even if that directory was deleted after an earlier
successful run. Cargo's own incremental cache avoids recompiling unchanged code. Nx never restores or collects shared
Cargo build directories; lint and test **results**, and dedicated Wasm/N-API **outputs**, remain cacheable.

Validation inputs include the crate's files, local dependency closure, governing manifests and configuration, the
declared toolchain pin, and the toolchain's own reported versions. The pin is `devenv.lock`, hashed as an ordinary file
input at both `{workspaceRoot}/devenv.lock` and `{workspaceRoot}/tooling/direnv/devenv.lock` — devenv resolves it into
the rustc, cargo, linker, C toolchain and SDK every cargo command inherits, so bumping it invalidates every cached cargo
target. Workspace test runners additionally hash all workspace member manifests because those affect unified features,
and the nextest executable/configuration.

Nothing machine-local takes part. The ambient cargo environment and the global `$CARGO_HOME/config.toml` are
deliberately not hashed: their values carry absolute store paths and per-checkout state directories, which would make
every cache entry private to the machine that wrote it and make one target hash differently inside a devenv profile than
outside it — enough to make a pre-push cross-compile probe unsatisfiable. Keep explicit inputs for arbitrary
build-script reads or custom environment variables that Cargo metadata cannot name.

A target whose inputs a repository DECLARES replaces the inferred list, pin included. Name the two `devenv.lock` entries
in that declaration, or in the named input it uses, or a toolchain bump will not invalidate it.

Every cached cargo target names the npm packages its command runs as `externalDependencies`: none for cargo itself, and
`@smoothbricks/nx-plugin` for a target that runs its executor or reads its installed files (the nextest tool config, the
archive extractor). Without that input Nx keys a task whose executor is not an `@nx/` one on every package in the
lockfile, so a lockfile-only commit re-ran every cargo verdict. Inside this repository the plugin is a workspace
project, which Nx refuses as an external dependency, so the list is empty here.

No input declaration removes a project's own configuration from a task's key. Nx 23.2.1 adds `ProjectConfiguration` to
every task (`hash_planner.rs` `gather_self_inputs`), and `hash_project_config.rs` hashes the executor, outputs, options,
configurations and parallelism of every target in the project, plus its tags and named inputs (not `inputs`,
`dependsOn`, `cache` or `//`). An edit to one target's command therefore re-keys every target of that project, including
each inferred `cargo-test-*` runner. Only the project a target lives in decides that sensitivity.

Cargo keeps the caller's `CARGO_HOME`. Moving configuration into an isolated home can change relative paths or lose
source replacement, credentials, and toolchain settings; forwarding it with `--config` changes precedence. Registry
access may consequently wait on Cargo's package-cache lock, which Cargo arbitrates itself; Nx adds no ordering for it.

Graph inference persists locked, offline Cargo resolution in Nx's workspace-data directory. Its content key includes the
canonical workspace and manifest paths, the complete indexed manifest set, local path-dependency and governing
manifests, lockfiles, Cargo configuration (including ancestor and `CARGO_HOME` configuration), toolchain pins, the
resolved Cargo executable, and explicit rustup toolchain selection. Changing only Rust sources, other files, or file
timestamps reuses the resolved closure without starting Cargo. The configuration identity here is a local
resolution-cache key, not a machine-specific task input.

Concurrent graph computations share one in-flight resolution per workspace. A native Nx file lock serializes workers;
after acquiring it, a worker checks the persisted result again before starting Cargo. A caller with changed inputs waits
for the current flight and resolves the new content rather than accepting the old closure. Successful cache writes use
atomic rename; failed resolutions are never cached. A corrupt cache is reported and resolved again. Inputs that change
during resolution are retried at most three times, then refused with the cause intact. A WASM-only Nx host cannot
provide the native lock and receives a typed refusal rather than an unserialized fallback.

A plugin worker's normal exit, host disconnect, or handled termination kills Cargo children it still owns; on POSIX it
kills the whole process group, including wrapper descendants. Standalone callers also forward unhandled termination
signals to their owned groups. An uncatchable `SIGKILL` cannot run an exit hook; the kernel still releases its file
lock.

The `@smoothbricks/nx-plugin:cargo-resolve` diagnostics channel reports `{ phase: "join", manifest }` when a caller
joins an existing flight. With no subscribers, inference neither allocates nor publishes an event. The cache regressions
use this readiness signal and Cargo control-channel events to prove concurrency and child teardown without sleep-based
timing assumptions; their outer hang guards report still-open test spans and process/gate state.

Keep workspace-owned runtime state outside Nx's source index. In a cowshed workspace, the root `.nxignore` must include
`.cowshed/`: Nx's watcher reads the root ignore files, not Git's `.git/info/exclude`. Watching the daemon's own log,
plugin sockets or job records turns each graph computation into another file event and another graph computation.

### Runtime inputs

Every runtime input, inferred or declared, runs through `runtime-input.sh`:

```json
{ "runtime": "sh node_modules/@smoothbricks/nx-plugin/runtime-input.sh bun --version" }
```

Nx 23.2.1 hashes a runtime input's stdout and stderr and ignores its exit status, so a failed input becomes a valid key:
its error text, one string for every history, toolchain or file it could not read, and a result cached under it is
replayed for all of them. The script keys on the command's stdout alone and turns a nonzero exit into an Nx error: it
ends stdout with the byte 0xFF, and Nx refuses output that is not UTF-8
(`invalid utf-8 sequence of 1 bytes from index N`, exit 1). That message names neither the input nor its cause; run the
target's runtime inputs (`nx show target <project>:<target> --inputs`) by hand to read the cause.
`runtime-input.test.ts` plants a failure through real Nx and fails if Nx ever hashes those bytes instead of refusing
them.

An input that runs inside a cowshed workspace's sandboxed daemon gets the client's environment, a host shell's `HOME`
included, but not the client's filesystem view (cowshed `04_sandbox.md`). An input that reads the operator's home fails
there, so a runtime input reads only the checkout and its declared environment. A `git` command in one sets
`GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1`: the key is a function of the repository, not of the operator's git
configuration.

### Cargo source runtime inputs

For inferred Cargo targets in a repository-root Cargo workspace, declare the external-source input once:

```json
{
  "namedInputs": {
    "externalRustCrates": [{ "runtime": "sh node_modules/@smoothbricks/nx-plugin/runtime-input.sh smoo-nx-cargo-hash" }]
  }
}
```

For a package-root Cargo workspace, pass its manifest relative to the Nx root:
`smoo-nx-cargo-hash packages/example/Cargo.toml`. A shared input covering several Cargo workspaces includes one runtime
entry per manifest.

By default, the command uses the content-keyed `cargo metadata --locked --offline` resolution and hashes external path
packages, including transitive dependencies, Rust sources, target source files, governing Cargo manifests, and Cargo
configuration. It also covers path packages under `node_modules`, which Nx's normal file map ignores. Packages inside
both the Nx and Cargo workspaces are left to the plugin's inferred file inputs. Adding or moving a dependency does not
require maintaining a second list of source roots. The command refuses missing dependencies or an unavailable locked
dependency cache instead of emitting a partial digest.

### Cargo closure input

Custom Cargo targets that replace inferred inputs need the source closure of the crates they build. The plugin infers it
as the `cargoClosure` named input of every `package.json` project with a member of a Cargo workspace under its root,
where that Cargo workspace's root is itself a project (the root `package.json`, for a repository-root workspace). Name
it in each custom Cargo target's `inputs`, alongside `cargoToolchain`, the target's own scripts and non-Rust inputs:

```json
{
  "nx": {
    "targets": {
      "build": {
        "inputs": ["{projectRoot}/scripts/build.sh", "cargoToolchain", "cargoClosure"]
      }
    }
  }
}
```

Cargo decides the closure. Graph inference checks the content-keyed locked offline resolution of the local packages
under the project's root and every mutable local package they reach through normal, build or dev dependencies; an edit
to a workspace member outside that closure leaves the task key alone. The members Nx's file index holds become filesets:
each package's `.rs` files, `Cargo.toml` files and `.cargo/config[.toml]` below its directory (skipping `target/` and
the other directories the hash command skips), target sources outside it, its governing workspace manifest, the
`Cargo.toml` and `.cargo/config[.toml]` of every directory from the package up to the Nx root, and the workspace's
`Cargo.lock`. Nx hashes those from the file hashes it already keeps, so hashing these inputs starts no process. A graph
with unchanged resolution inputs starts no Cargo process either.

Members the file index cannot hold, outside the Nx workspace or under `node_modules`, are hashed by one runtime
`smoo-nx-cargo-hash --closure <projectRoot> <Cargo.toml>` entry, present only while such members exist. Its cached
resolution checks external manifests too, so a dependency introduced by an external manifest edit is still covered.

A runtime input that cannot be computed, whether from a manifest or locked dependency it cannot resolve or from
arguments it refuses, prints one fixed line (`cargo-input-unavailable`, `cargo-input-invalid-arguments`) and exits
nonzero, and `runtime-input.sh` turns that into an Nx error. Its stderr names the cause, Cargo's own message included,
with the checkout's absolute path written as a relative one and Cargo's timing-dependent lock-wait lines dropped;
refused arguments write nothing. The Cargo target, run by hand, still refuses the missing dependency or manifest with
Cargo's own cause.

Cargo resolution failures (no `cargo` on PATH, a stale `Cargo.lock`, or an unavailable locked dependency) fail graph
inference with `CargoMetadataError`, including the manifest path and Cargo's cause. A member inside the workspace
missing from Nx's file index, or another closure Nx cannot express precisely, fails with `CargoClosureInputError`.
Neither condition falls back to a whole-workspace runtime hash or silently reuses an older closure.

Resolution therefore needs the packages `Cargo.lock` pins already in `CARGO_HOME`, before any task can run: the graph's
own `cargo-fetch` target cannot supply them. The managed shell's devenv task `smoo:cargo-fetch`
(`tooling/direnv/setup-environment.ts --cargo`, gated by its `--cargo --check` status) runs `cargo fetch --locked` for
every project whose `Cargo.toml` declares `[workspace]`, once per change to that workspace's `Cargo.toml`, `Cargo.lock`
or `.cargo/config[.toml]`. Its stamp lives in `$XDG_CACHE_HOME/smoo/cargo-fetched` (`~/.cache/smoo` when that is unset),
never in `CARGO_HOME`: a sandboxed shell may write only Cargo's own paths there. A stamp outside `CARGO_HOME` outlives
it, so the stamp also names the directory `CARGO_HOME` was when the fetch finished (device, inode and creation time): a
deleted and recreated `CARGO_HOME`, or another one, fetches again, while a copied checkout sharing the `CARGO_HOME` does
not. A failed fetch writes no stamp. In CI (`devenv tasks run`) a failed fetch fails; locally the task prints Cargo's
cause, the shell loads, and the next entry retries, while graph inference keeps refusing with that same cause.

In-workspace members follow Nx's file semantics, which differ from the command's own walk in two places: a symlinked
directory inside a package is not followed, and manifests and Cargo configuration above the Nx root (a
`~/.cargo/config.toml` above a checkout in the home directory) are machine-local and not hashed, as for every other
cargo input the plugin infers.

A project the plugin does not infer (a `project.json` without a `package.json`), or one in a Cargo workspace whose root
is not a project, names the command directly. Nx does not substitute `{projectRoot}` into runtime inputs, so each such
project declares its own named input and names its directory:

```json
{
  "namedInputs": {
    "cargoSources": [
      {
        "runtime": "sh node_modules/@smoothbricks/nx-plugin/runtime-input.sh smoo-nx-cargo-hash --include-workspace --closure packages/example"
      },
      "{workspaceRoot}/Cargo.lock",
      "{workspaceRoot}/devenv.lock",
      "{workspaceRoot}/tooling/direnv/devenv.lock"
    ]
  }
}
```

Without `--closure`, `--include-workspace` hashes every mutable local package in the Cargo workspace. For a package-root
workspace, pass its manifest last and name that workspace's `Cargo.lock`. Keep `externalRustCrates` for the plugin's
external-source inference contract; unrelated TypeScript targets need neither input. An Nx `^production` input only
follows existing project graph edges; it cannot replace a missing Cargo dependency closure.

Files git ignores beneath a package directory are not hashed, matching Nx's own file inputs. A generated source is its
producer's output: the consuming target depends on the producer and hashes the source through
`dependentTasksOutputFiles`, so a tree that has not generated it yet hashes like one that has. A package whose own
directory git ignores, such as one installed under `node_modules`, keeps all of its sources, and a directory outside any
Git repository ignores nothing.

The helper exposes the same choices as
`hashCargoPathInputs(manifestPath, workspaceRoot, { includeWorkspace: true, closure: 'packages/example' })`, resolving
`closure` like `manifestPath`. Omitting the options, or setting `includeWorkspace: false`, retains the external-only
behavior.

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

Crate manifests are the one kind Nx cannot hash by field, because TOML has no such input. The plugin hashes them itself,
in the process that builds the graph and is already reading them: a single forward scan over the bytes feeds the hasher
every run it keeps, dropping `[package].version` and the `[workspace.package].version` members inherit and nothing else
— a `version` naming a _different_ crate under a dependency table stays in the digest, and anything a line-oriented scan
cannot read (a multi-line string, an unterminated value) is hashed verbatim rather than guessed at. The digest then
travels as a literal input path that matches no file: Nx hashes a project's `namedInputs` definitions into that
project's configuration hash, which is part of every task in the project and of every task depending on it, so the value
reaches the hash with no process at all. Earlier revisions spawned a command per crate project per graph computation;
that spawn is gone.

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
installed. Every fallback costs cache hits and never trades away invalidation, because Nx runs a `runtime` input without
reporting a failing one — a fileset that excluded a manifest with no digest replacing it would serve stale results
silently.

## Cowshed Build-State Output Policy

Consuming repositories enforce build-volume safety in their Nx lint, using the graph that lint already resolved:
`refuseOutputsUnderBuildState(graph, buildState.paths, workspaceRoot)` from
`@smoothbricks/nx-plugin/build-state-output-policy`. Obtain `buildState` from `cowshed build-state --json` in that
checkout; no graph is constructed by cowshed job admission or doctor.

The validator uses Nx output interpolation for every target and configuration, including resolved project overrides. It
refuses outputs at, beneath, or above a contributed path, and globs whose static prefix can cover one. Absolute outputs
are resolved against the caller's workspace root. `BuildStateOutputError.findings` names each project, target,
configuration, output, and contributed path. Keep cacheable artifacts under source-tree paths such as `dist/` and
`.cache/nextest/`; restoring an output under `target/` or `.nx/` can replace a build-volume link with a real directory.

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
- `test-watch` is inferred when the package already defines an explicit Bun or Vitest `test` command. The plugin derives
  the corresponding watch command and makes it depend on `typecheck-tests`.
- A workspace-root `Cargo.toml` provides `cargo-test`, `test`, `cargo-lint`, `mutation`, and `bench`.
  `cargo-test-compile` warms one workspace `cargo test --no-run`. `cargo-test-archive` runs one
  `cargo --frozen nextest archive --workspace` into `.cache/nextest/archive.tar.zst` and is the only cached cargo BUILD:
  it produces a file outside the build-volume links rather than a mutable build tree. Nx cache restoration must never
  replace Cargo's `target` link. Its inputs, like every cargo-derived fileset, are anchored at `{workspaceRoot}`: Nx
  resolves a `{projectRoot}` glob against only the files its project owns, and a nested package owns the crates under
  it, so a root workspace's archive spelled that way hashes none of its members and replays stale binaries against
  edited crates. Each member crate gets a cached `cargo-test-<package>` run (30s per-test timeout like
  `bun test --timeout=30000`) whose inputs include that crate and its path dependencies, and whose command runs
  `cargo --frozen nextest run --binaries-metadata … --cargo-metadata … --target-dir-remap … --workspace-remap . -E 'package(<crate>)'`
  from the archive's one extraction. Runners therefore execute rather than build: they compile nothing in cargo's build
  tree, and fan out instead of chaining. They do not unpack the archive each: before the run, `smoo-nx-nextest-extract`
  hashes it and extracts it into `<target_directory>/nextest-extracted/<sha256>` unless a runner already did — one
  extracts, the others wait — so a workspace with ~200 test binaries and ~25 crates unpacks them once per archive
  instead of once per crate. The directory is keyed by the archive's bytes, so a rebuilt or cache-restored archive never
  runs an older extraction's binaries, and publishing one removes the others. The target directory is the one
  `cargo metadata` reports for the runner's Cargo workspace, so a `build.target-dir` setting moves the extractions with
  the build tree. An extraction is per-checkout tool state like the rest of that tree, never an Nx output: it lives in
  Cargo's build tree, which a checkout with a build volume keeps off its source tree, and it never sits under a declared
  output such as `.cache/nextest`. `cargo clean` removes it. `--workspace-remap` is required — an archive records the
  producing tree's absolute paths, and without it a restored archive hands tests another checkout's
  `CARGO_MANIFEST_DIR`. With it the archive is relocatable, so one cache entry serves every checkout of the same commit;
  nextest re-points `CARGO_BIN_EXE_<name>`/`NEXTEST_BIN_EXE_<name>` at the extracted binaries at runtime, but a test
  that reads them through the compile-time `env!` macro keeps the producing tree's path. Per-crate runners accept an
  empty nextest selection because a valid workspace member may have no tests and a hash partition may legitimately be
  empty. A crate in the project that builds the debug cdylib runs after `napi-debug`, because its tests load that addon
  from `target/debug`. A crate declaring `[package.metadata.smoothbricks.wasm-bindgen]` also receives the cacheable
  `cargo-wasm` output target in its owning project.
- The plugin's own `nextest.toml` is passed as
  `--tool-config-file "smoo:$PWD/<to workspace root>/node_modules/@smoothbricks/nx-plugin/nextest.toml"`, so it is a
  layer BENEATH the repository's `<cargo-workspace>/.config/nextest.toml` rather than a replacement for it: a repository
  can raise a timeout or declare `archive.include` for a cdylib or fixture the archived test binaries need, and its
  settings win. That file is an input of both the archive and every runner, so changing it invalidates the verdicts it
  governs. `$PWD` keeps the command text identical across checkouts while satisfying nextest's requirement that a tool
  config path be absolute, and naming the file through the workspace root's `node_modules` link — never the plugin's
  real location — keeps it identical for a plugin linked from a checkout outside the workspace, whatever depth each
  workspace sits at; `--user-config-file none` still holds, because a developer's `~/.config/nextest` must not decide a
  cached verdict.
- A crate whose suite outgrows one bounded window declares `[package.metadata.smoothbricks.test] shards = N`, and gets
  `cargo-test-<package>-shard1..N`, each running `--partition hash:i/N` with the full bound. nextest assigns a test to a
  shard by hashing its name, so the shards stay an exact partition of the crate as tests and test binaries are added,
  and a stale `N` can only make a target slow — never drop a test. Omitting the key means one target, as before.
- Tests that a `nextest.toml` override singles out run separately in `cargo-test-<package>-exceptions-shard1..N`, using
  the same declared partition count. Ordinary shards run the complement, so the union still covers every test exactly
  once. The plugin's own overrides raise `slow-timeout` for compile-fail tests and for the real-APFS full-lifecycle
  tests; lifting them out keeps their raised bound from delaying an ordinary shard, and partitioning the class keeps
  every target inside its deadline without relaxing it. Exceptional shards are scheduled like the ordinary ones: nothing
  is exclusive, and isolation between concurrent real-APFS fixtures is the production backend's per-image lease, not Nx
  scheduling. The exceptional filter is derived from the overrides in `nextest.toml`, so adding an override is the whole
  classification change. Only sharded crates need these targets; an unsharded crate already runs its whole suite in one
  process. Empty partitions pass, including platforms where the tests are `cfg`-ed out. `nextest-partitions.test.ts`
  executes the inferred archive and shard commands over 26 tests and asserts that the ordinary shards ran exactly the 13
  ordinary tests once and the exceptional shards exactly the 13 exceptional ones.
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
not emit `dist-test`. `test-watch` is continuous and depends on `typecheck-tests` before entering Bun or Vitest watch
mode. Smoo validation creates/requires this config for test runners that do not typecheck test files by default.

`tsconfig.test.json` is not a TypeScript build-mode project. It should reference library tsconfigs it needs to typecheck
against, but the package root `tsconfig.json` should not reference `./tsconfig.test.json`. Nx runs test typechecking
through the inferred `typecheck-tests` target, not through `tsc --build`.

## Ensure a Target Before Executing Its Binary

`ensureBuilt` checks a target and its task dependencies against Nx's local cache and the daemon's recorded output
hashes. A full hit returns without running tasks or printing output. A miss runs the task graph through Nx's in-process
runner with streaming output; only a workspace with the daemon disabled falls back to its checkout-local `nx` CLI.

An `nx:noop` task, including a command-less target that Nx normalizes to one, runs nothing and so needs no cache record:
the tasks it aggregates answer in its place, and any outputs it declares are verified like every other task's. Any other
uncacheable task is a miss on every call, because only running it can say whether its side effects are current.

The probe and runner use the workspace's installed Nx and its normal cache configuration. A build performed by the Nx
CLI can satisfy the next wrapper invocation without re-executing tasks; changed inputs still go through Nx's runner.
Both invocations must use the same cache location. Relative `NX_CACHE_DIRECTORY` and `NX_WORKSPACE_DATA_DIRECTORY`
values resolve against the Nx workspace root, so they remain checkout-local when inherited by another shell. Absolute
paths remain caller-controlled, including deliberately shared CI locations; an absolute path inherited from another
checkout does not become local merely because the working directory changed.

Before hashing, it compares a native snapshot of the workspace with the daemon's file table. A write the daemon's
watcher has not delivered yet counts as a miss unless every changed path is a declared output of a task in the graph, so
a build's own artifacts never turn the next call noisy, and an edit made moments before the call never hits stale.

The daemon's output records are lossy: they are held in memory, tracked per collapsed directory, and erased by writes it
processes more than two seconds late — including, on a busy daemon, a restore's own writes. When it cannot vouch for a
task's outputs, the working tree is compared with Nx's local cache artifact for that exact task hash, entry by entry as
Nx's restore expands them: each entry must be reachable through real directories and hold the same node types, symlink
text (never followed), file permission bits and bytes. A tree the restore would leave unchanged is a hit, and the
daemon's record is re-armed as Nx's runner does after a restore. A missing or empty artifact, any difference, a read
error, or a write observed during the comparison is a miss.

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
- A miss runs the target through the workspace's `nx` CLI with the child's stdout on stderr, so stdout carries only the
  binary's output (`wrapper | jq` works). Nx's pseudo-terminal writes task output straight to file descriptor 1, which
  only a child process can redirect.

Pass `--workspace-root <dir>` before `--` when invoking the wrapper outside the workspace. The binary path resolves
against the caller's current directory.

Nx state belongs to one workspace. The wrapper's own Nx is the one `NX_WORKSPACE_ROOT_PATH` names, else the nearest
`nx.json` above the current directory. When that is not the requested root, `NX_SOCKET_DIR`, `NX_DAEMON_SOCKET_DIR`,
`NX_WORKSPACE_DATA_DIRECTORY` and `NX_CACHE_DIRECTORY` are not passed on, so the root's daemon, task database and cache
stay its own instead of landing on the socket and database of the workspace that exported them. The managed shell and
Cowshed supervisor publish `NX_WORKSPACE_ROOT_PATH` beside those overrides, so a child entering a scratch root still
knows which workspace owns its inherited state. Without a managed owner, the wrapper derives the root from cwd.

Without `--` and a binary, the wrapper only makes the target current and exits 0, or with the failed run's status:

```bash
smoo-nx-exec my-cli:build --workspace-root "$root"
```

A hit prints nothing, where `nx run` would replay every task's cached log. A miss prints Nx's run of the target on
stderr.

## Nx 23.2.1 Runtime Patch

The repository installs Nx 23.2.1 with [`patches/nx@23.2.1.patch`](../../patches/nx@23.2.1.patch) applied, from an
immutable release tarball: the root `package.json` keeps `devDependencies.nx` at `23.2.1` and sets `overrides.nx` to the
release asset's URL, and `bun.lock` pins its sha512. `tooling/patched-nx.ts` builds the tarball reproducibly from the
registry tarball and the patch; the `Patched Nx` workflow publishes it once from `main`, under a tag named for the
uncompressed tar's sha256, and verifies the served asset on every later run. A tarball, unlike a `patchedDependencies`
entry, stays in Bun's read-only global store (`~/.bun/install/cache/links`), so a sandbox can run Nx without being able
to write it.

`@nx/js` imports `nx` (`nx/release`, `nx/src/…`) without declaring it
([nrwl/nx#34087](https://github.com/nrwl/nx/issues/34087)), and a global-store entry links only declared dependencies,
so `@nx/js` in that store cannot find Nx: its version actions, and this plugin's that extend them, would fail.
`patches/@nx%2Fjs@23.2.1.patch` declares the peer. Being patched, `@nx/js` installs project-local under
`node_modules/.bun/`, where `nx` resolves through the hoisted `node_modules/.bun/node_modules/nx` link to the same
tarball entry; Bun's lock still records only the registry peers, so the placement is what makes the import resolve.

The Nx patch repairs upstream Nx runtime behavior, separately from this plugin's workspace-owner checks:

- **Task history uses the client's native database connection.** Task details and history must share that connection; a
  daemon can have frozen a different database namespace before a later client supplies workspace-data overrides. The
  patch removes the obsolete history RPCs and retains native errors, including post-run failures. See
  [upstream Nx #37268](https://github.com/nrwl/nx/pull/37268).
- **The default cache limit uses the cache's own filesystem.** It takes ten percent of `statfs(cacheDir)` capacity, or
  its nearest existing ancestor when the directory has not been created. This avoids synchronously inventorying every
  mounted disk. Explicit `NX_MAX_CACHE_SIZE` and `nx.json.maxCacheSize` retain their precedence; filesystem errors other
  than a missing directory still fail the operation. See [upstream Nx #37269](https://github.com/nrwl/nx/pull/37269).
- **The daemon keeps graph plugin workers running.** Nx stops an isolated plugin worker after the last phase it has
  hooks for, which for a graph-only plugin is every graph; the daemon recomputes the graph on every tracked file change,
  so each change spawned and loaded every graph plugin's worker again (median 199 ms against 63 ms from edit to graph in
  a one-project fixture, 0.9–1.5 s per worker under a gate's load). On the daemon a worker with graph hooks now lives as
  long as the daemon; one-shot clients and task-only plugins keep the eager shutdown. See
  [upstream Nx #37271](https://github.com/nrwl/nx/pull/37271).
- **Nx resolves typescript and release version actions from the workspace.** Nx 23.2.1 loads both with a plain `require`
  from its own location, which in Bun's global store holds only Nx's own dependencies. There it finds no `typescript`,
  so its dependency analysis skips every source import without a warning (this repository lost 12 of 44 graph edges),
  and `release version` cannot resolve `@nx/js`'s version actions. The patch resolves both through `getNxRequirePaths`
  (the workspace first), falling back to Nx's own location. See
  [upstream Nx #37272](https://github.com/nrwl/nx/pull/37272).
- **A task's dependencies do not depend on which other tasks the run asks for.** A target that depends on `^build`
  through a project with no `build` target gets a dummy task for it, and Nx flattens each dummy into the real tasks
  behind it except a dummy it believes sits in a cycle. `findCycles` reported every task on the depth-first path that
  first reached a cycle, and only the cycles reached before another traversal marked their tasks visited, so the answer
  followed the order of the graph's keys. In a workspace whose projects import each other, a shard requested alone
  (`nx run app:test-4`) and the same shard requested through its aggregate (`nx run-many -t test`) got different
  dependencies and, because `dependentTasksOutputFiles` keys on the dependency tasks' outputs, different hashes: a
  pre-run of one never hit the cache of the other. The patch answers with the tasks that lie on a cycle (Tarjan's
  strongly connected components), so a task's dependencies are a property of the graph behind it. Not yet proposed
  upstream: Nx `master` carries the same `findCycles`.

Publishing or installing `@smoothbricks/nx-plugin` does **not** change a consumer's Nx. A consumer needing these repairs
sets the same `overrides.nx` URL in its root `package.json`, registers the same `@nx/js` patch in its
`patchedDependencies`, regenerates its lockfile, and proves a frozen install and its affected normal Nx gates. Do not
replace the registry dependency with a local link or hide a failure by resetting the database or disabling the daemon.

The patch is version-specific. A changed patch publishes a new release, and consumers move to its URL. On an Nx upgrade,
remove each hunk only when the installed upstream release contains that repair and the task-history namespace,
cache-bound, resident-worker, store-resolution and task-graph regressions pass; preserve any repair not yet released.
When every hunk is upstream, drop the override, the patch, `tooling/patched-nx.ts` and the workflow together.

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

On a macOS host, every `bounded-exec` task runs with its own `TMPDIR` lease: a directory on one RAM-backed APFS volume
per user, so test temp files never reach the SSD. The volume is created lazily by the first task that needs it and
detached when the last live lease ends, since its pages return only on detach. It is a legacy DiskImages RAM disk, so it
spends none of the AppleDiskImages2 attach budget cowshed counts (`specs/cowshed/01_storage.md`). A lease whose task
died is reclaimed by the next task, unless an image is still attached from a file below it or a mount sits below it:
those belong to whoever attached them, so the lease and the volume are kept and every new task names them. A target's
own `env.TMPDIR` wins over the lease. Inside a cowshed sandbox, which cannot write the lock in `/private/tmp`, the task
keeps its inherited `TMPDIR` and says so once. A failed task that left the volume nearly full names the volume.

The volume mounts at `/Volumes/smoo-ram-<uid>`, where DiskArbitration puts it: asking for any other mountpoint escalates
to an administrator dialog that blocks every `diskutil` on the host. That mount is `noowners`, and launchd refuses a
plist from it, so a test that bootstraps a launchd job keeps the plist outside `TMPDIR`.

The volume is formatted, mounted and marked as `smoo-ram-<uid>-new`, then renamed into place, so a creator that dies
part way never leaves an unmarked volume at `/Volumes/smoo-ram-<uid>` for the next one to dodge as `smoo-ram-<uid> 1`.
Before creating, a task detaches every RAM disk of the same user and size that is mounted nowhere but those two names:
the debris of a creator that was killed or ran into its deadline. One that still carries a lease directory, or has
something attached below it, is named in the task's error instead. A disk command that runs past its deadline is killed
with its whole process group, so `hdiutil attach` cannot leave a helper behind that finishes the attach later.

Every `hdiutil` and `diskutil` the volume runs takes cowshed's host disk-lifecycle lease first, when the cowshed gateway
answers at `/private/cowshed/store/gateway.sock` (`specs/cowshed/05_gateway.md`): storage calls and mount-table changes
never overlap on the host, because an attach does not finish while the mount table keeps changing. The lease wait comes
before the command's deadline. Without a grant the command runs unleased and stderr says why; a gateway that is not
running is said once per process. A gateway from before disk leases answers only after 2 s of silence, so once one is
found the process stops asking it for a minute, as cowshed's own commands do.

Every `bounded-exec` command takes CPU tokens from cowshed's host CPU budget before it starts, when the gateway answers
on the same socket (`specs/cowshed/05_gateway.md`, "Host CPU budget"), so concurrent gates on one host keep their
runnable work near the core count instead of each sizing its runners to the whole machine. It asks for as many tokens as
the command's runner runs at once — a `nextest run` its `--test-threads` or one per core, `bun test --parallel=N` N, a
cargo build one per core, anything else one, or the target's `parallelism` option — and sizes the runner to the grant:
`NEXTEST_TEST_THREADS`, a rewritten `--test-threads=`/`--parallel=` count, `CARGO_BUILD_JOBS` and `RUST_TEST_THREADS`,
and `BOUNDED_EXEC_CPU_TOKENS` for every command. The gateway shares tokens fairly between checkouts (Nx workspace
roots). `timeoutMs` and `idleTimeoutMs` start at the grant. The tokens go back when the command exits, or with the
executor's socket when it is killed. The wait prints one
`cowshed: cpu-tokens wait done elapsed=… tokens=<granted>/<asked>` line. With no gateway listening, the command runs
exactly as configured and nothing is said; a gateway from before the budget runs it unbudgeted and stderr says so.

Every `bounded-exec` task records why it ended in `.nx/workspace-data/bounded-exec/<task>/verdict.json`, keyed by the
task id and the hash it ran at:

- `bound`: nothing failed but a wall-clock bound. Either the command outlived `timeoutMs`, or every failing test in the
  runner's JUnit report failed on its runner's per-test timeout (`cargo-nextest` writes `type="test timeout"`,
  `bun test` writes `type="TimeoutError"`). A loaded host produces exactly these.
- `wedged`: `idleTimeoutMs` fired. Silence means a hang, never load.
- `failed`: a test failed on its own, or the command exited non-zero with no report naming a timeout.
- `passed`.

The report needs no target configuration. When the command runs exactly one `bun test` or `nextest run`, the executor
asks that runner for a JUnit report in the task's directory: `bun test` through `--reporter=junit --reporter-outfile`,
nextest through a one-key tool config (nextest takes the report path only from configuration). A `bun` is any unquoted
command word whose basename is `bun`, so `../bun-runtime/.runtime/bun test` is the runner too, and the rewrite keeps the
binary the target named. A script that spawns the runner itself passes
`--reporter=junit --reporter-outfile="$BOUNDED_EXEC_JUNIT"` on; the variable is set for every task. A command with no
runner, or with two (they would write one report), is judged by its exit alone.

`smoo-nx-bound-failures` reads the workspace's last Nx run (`run.json` in the Nx cache directory) against those records
and prints one line per failed task. It exits 0 only when the run failed and every failed task's record, for that task's
hash, is a `bound`; a failed task without one is not. A merge queue uses it to re-run a gate that a loaded host failed,
and to stop on anything else.

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

A `test` aggregate that is a no-op target passes the check when every target it depends on, transitively, is a bounded
leg: `bounded-exec` with a command, a `cwd`, and positive `timeoutMs` and `killAfterMs`. A bare prerequisite target
(`^build`, `build`, a target in another project) is not a bounded leg, so name prerequisites on the legs that read them.
Only a leg that runs `bun test` must start in the project root or its `src/`, where Bun's test-file discovery is cheap;
any other command (a cargo workspace's per-crate nextest legs run from the workspace root, a gate may run a script that
lives in another project) may use any `cwd`.

## Test Fixtures That Run Nx

Most tests never start Nx. Plugin behaviour is the value `createNodesV2` returns, so a test asserts on that value, and
where it needs Nx's verdict on it, it calls the Nx function that gives the verdict, in process: `createTaskGraph` for
the order a run-many runs tasks in, `getOutputsForTargetAndConfiguration` for what a cache hit restores,
`globWithWorkspaceContextSync` for the files a fileset input hashes, and a `runtime` input's command under `sh -c` from
the workspace root for the value Nx keys on. `smoo-nx-exec`'s decisions are functions of Nx-shaped data — selector and
arguments, the environment it hands Nx, the daemon's file table and output verdicts, the cache records, the artifact
tree a restore would leave — and are called directly; its process handling (stdio, exit status, signals, cwd, `execve`)
runs against a stand-in `nx` script with the daemon off, which loads no Nx at all. A test starts `nx` only when that
process is the subject: a fresh daemon serving `smoo-nx-exec`'s probe (outputs it holds no record of, and the socket it
binds for a root entered from another workspace's shell), daemon-hosted plugin workers, task history, a hunk of the
repository's Nx patch, and fixture teardown.

A test that runs real Nx in a temp workspace runs it daemonless unless the daemon is the behaviour under test: a
daemonless Nx leaves nothing running once it exits. A daemon a fixture does start works in the fixture root, idling and
watching files until something stops it, so `@smoothbricks/nx-plugin/testing` (used by this package's and the CLI's
tests) owns those roots:

- `ownedFixtureRoot(suite, prefix)` creates a root under `<tmpdir>/smoothbricks-fixtures/<suite>/run-<pid>`, the run
  directory of the test process that owns it. The first root a process creates for a suite first reclaims every run of
  that suite whose owner pid is gone: each process working in it gets SIGTERM (SIGKILL if it outlives 10 s), then the
  run is deleted. A run whose owner is alive is never touched by another process. The owner itself, on SIGTERM, SIGINT
  or SIGHUP (a bounded test leg's timeout sends SIGTERM, then SIGKILL 10 s later), first stops whatever still works in
  its own run, then dies of that signal.
- `reclaimDeadFixtureRuns(suite)` runs that sweep on demand, for a test that has just watched a fixture-owning process
  die.
- `stopNxDaemon(workspace, stop)` runs the caller's `nx daemon --stop` and waits until the daemon recorded in
  `<workspace>/.nx/workspace-data/d/server-process.json`, and every process it started, has exited.

A fixture retires on every exit path, a throwing body included: stop its daemon, then delete its root. A daemon that
outlives the stop keeps the root, so the next run's sweep still finds it.

## Managed workspace files

`nx generate @smoothbricks/nx-plugin:managed-files` stages the same managed files as `smoo monorepo update`, without
installing packages or resolving secrets. Use Nx `--dry-run` to inspect the changes. The workspace defaults register
this generator for `nx sync` and `nx sync:check`.

The plugin owns the packaged templates, pure rendering and content-preservation functions. The generator reads workspace
files through Nx `Tree` and uses Nx's resolved project graph for inferred targets. The CLI only supplies the filesystem
and process boundary. There is no separate serialized change plan.

Files with a `# smoo-local` tail or `# smoo-local-begin` / `# smoo-local-end` blocks retain those sections. Matching
source symlinks are preserved; conflicting local blocks and broken or external links are reported instead of
overwritten. The generator does not remove a repository-owned file when a capability is disabled.
