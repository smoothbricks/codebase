# Nx Plugin

Local Nx plugin for workspace-standard package setup and missing inferred targets.

Workspace policy preserves configured plugin entries other than the disabled `@nx/js/typescript`, including their
options, include/exclude filters and order. It adds the bare `@smoothbricks/nx-plugin` entry only when the plugin is
absent; repeated updates do not replace an existing configured entry or discard scoped instances. Malformed plugin
values are reported rather than replaced, and an unchanged Nx Tree configuration is not serialized again.

The public plugin entry loads its transformed bootstrap product synchronously, matching Nx's real `handleImport` require
path in Node and Bun. A missing `dist/index.js` refuses by name and gives the `ttsc` bootstrap command; it never returns
an empty graph or loads raw development source in place of transformed code.

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
flat configuration enables ESLint for existing JavaScript/TypeScript files under that project's `src` and the files its
`tsconfig.test.json` declares, wherever they live. TypeScript resolves inherited `include`, `files` and `exclude`; the
same sources, the test declaration and its existing `extends` chain are explicit lint cache inputs. Both lists hold only
files in Nx's workspace file index (.gitignore and .nxignore applied), so a gitignored generated module that a build
left on disk is neither linted nor keyed, and the lint target is the same in every checkout of one tree. Rust-only
source trees keep their Cargo validation and manifest checks without invoking ESLint on nonexistent JavaScript inputs.
Test-config diagnostics retain TypeScript's parsed-command-line types through bootstrap emit; invalid declarations fail
with their diagnostic code and message, while an empty test program (TS18003) contributes no test files. Adding
JavaScript/TypeScript sources or changing the declared test program updates lint coverage automatically. The target's
`metadata.eslintFileSet` retains that exact command and workspace-relative file list for cache inspectors.
`@smoothbricks/nx-plugin/eslint-file-set` owns its type and generated validator; an inspector can resolve each file's
actual ESLint parser program instead of guessing from the project or target name.

Lint inputs include dependency production inputs because type-aware ESLint reads dependencies' declarations. This uses
`^production` unless the workspace opts into the versionless production input described below. Keep
`targetDefaults.lint` limited to shared cache policy or additive inputs such as `["...", "^production"]`. Smoo's
workspace policy removes static lint executors, commands, dependencies, outputs and replacement input lists because they
override source-aware inference for every project. Project-local `nx.targets.lint` declarations retain normal Nx
override precedence.

Smoo's workspace policy also sets `"cacheDirectory": ".nx/cache"` in the root `nx.json`, and `smoo monorepo check` fails
without it. With no `NX_*_DIRECTORY` variable set, as in a host shell, the pinned Nx otherwise moves the task cache and
its database to a per-user `~/.nx/<id>` keyed by the repository's remote, which every checkout of the repository shares.
A configured `cacheDirectory` keeps the cache in the checkout's `.nx/cache` and the task database in its
`.nx/workspace-data` (nx.json has no key for the database's directory). Cowshed links both into the checkout's build
volume, and `src/nx-state-location.test.ts` checks where this repository's host Nx resolves them.

The plugin's own `nx run nx-plugin:test` aggregates four bounded Bun targets. Each uses Bun's native
`--timings=test-timings.json --shard=N/4` partition to balance measured per-file work, while normal discovery still runs
every test file (including newly added files) exactly once. A failed shard fails the aggregate. The shard count and
standard 120-second target deadline stay unchanged. Bun cuts the path-sorted files into four contiguous runs and costs a
file the manifest does not name at the manifest's median, so files added without a refresh cluster into one shard and
can push it past its deadline. Refresh the portable timing manifest from this package directory after adding or slowing
a test file, with
`bun test --timeout=30000 --timings=test-timings.json --update-timings --path-ignore-patterns='**/*-host.test.ts'`; it
is scheduling data, not a test allowlist.

Real Nx fixtures own their cache and workspace-data directories and stop their daemon before removing the fixture.
Fixtures that intentionally strip caller directory overrides declare `cacheDirectory: ".nx/cache"` in their own
`nx.json`; this keeps the native task database local too, rather than leaving a new `~/.nx/<repoKey>` behind. Offline,
path-only Cargo fixtures likewise own a temporary `CARGO_HOME`; production commands still keep the caller's Cargo home.

Fixture sockets and loopback listeners live where a Cowshed sandbox admits them, never at a literal `/tmp` path or on
port 0. A fixture's short socket parent alias lives beside the Nx socket directory the job supplies (the parent of
`NX_SOCKET_DIR`, the workspace's short runtime link), else under `COWSHED_RUNTIME_LINK`, else the temp directory. The
alias points into the fixture's own physical `.nx/sockets`, outside every checkout; its `nx` socket leaf is a real
directory. The foreign-workspace guard therefore sees the fixture as its owner, without a new grant or identity rule. A
shell's variable counts only when it belongs to this checkout, as `nx-socket-dir.sh` decides it; one that leads
elsewhere or cannot be looked at is passed over, and a refusal names it. The temp fallback admits a user-owned parent or
the root-owned public sticky directory with mode 1777, not an inaccessible sticky directory. A socket path over 95 UTF-8
bytes is refused with an error naming it. The shell owner retains a failed socket-directory creation's exit status; the
foreign-workspace provider refuses before running a target and preserves its complete diagnostic. `fixtureNxEnv` gives
each fixture its own `NX_SOCKET_DIR`; retirement stops its daemon before unlinking its verified parent alias and
removing the fixture root. A test whose subject is two daemons names a distinct real leaf for each. A listener another
process connects to (the counted Cargo broker, a remote-cache server) binds through `bindListener`: a port of the
workspace's `COWSHED_PORT_BASE`/`COWSHED_PORT_BLOCK_SIZE` block, lowest first, skipping only ports in use, and port 0
outside a block.

## Cargo Workspace Layouts

The plugin discovers Cargo workspaces beside Nx project manifests at any depth:

- **Workspace owner:** the project beside a workspace `Cargo.toml` owns one `cargo-fetch`, `cargo-test-compile`,
  `cargo-test-archive`, `cargo-lint`, cross-check, mutation, bench, and sweep target. Its `cargo-lint` and `cargo-test`
  aggregates reach all workspace crates, including a root `[package]`; `cargo-test` is the ordinary suite, and the
  project's `test` also reaches the serial test groups (below).
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
  edge; replacing the array makes its author responsible for fetching before frozen Cargo commands and producing the
  archive before any runner consumes it. This repository also declares the archive producer explicitly in both APFS
  runners' owning project targets, so a cold checkout reaches it before extraction.

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

Rust LMAO's opt-in source-revision contract is defined in
[01f](../../specs/lmao/01f_arrow_table_structure.md#smoo/lmao!n/arrow-table-module-id.not-git). Its native and Wasm
targets declare `{ "env": "LMAO_GIT_REVISION" }` as an input. Default dev/test runs leave it unset; publishing jobs set
it to the dispatch commit, so those builds include the explicit revision in their key without making dev runs
commit-keyed. The `production` configuration selects the shipping profile; it does not discover a Git revision. The root
workspace's cached nextest archive hashes that variable too, because it contains the compiled LMAO test binaries.

### Cargo cache boundaries

`cargo-fetch` is uncached because the registry lives outside Nx outputs. `cargo-test-compile` is also uncached: it warms
Cargo's shared mutable build directory before the bounded runner, even if that directory was deleted after an earlier
successful run. Cargo's own incremental cache avoids recompiling unchanged code. Nx never restores or collects shared
Cargo build directories; lint and test **results**, and dedicated Wasm/N-API **outputs**, remain cacheable.

Validation inputs include the crate's files, every file its sources compile in through a literal `include_str!` or
`include_bytes!` path outside the crate's directory (a shipped skill or a shared corpus beside the crates), local
dependency closure, governing manifests and configuration, the declared toolchain pin, and the toolchain's own reported
versions. The pin is `devenv.lock`, hashed as an ordinary file input at both `{workspaceRoot}/devenv.lock` and
`{workspaceRoot}/tooling/direnv/devenv.lock` — devenv resolves it into the rustc, cargo, linker, C toolchain and SDK
every cargo command inherits, so bumping it invalidates every cached cargo target. Workspace test runners additionally
hash all workspace member manifests because those affect unified features, and the nextest executable/configuration.

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

Workspace policy defaults non-production tasks to `["{projectRoot}/**/*", "!{projectRoot}/**/*.md"]`, without a forced
CI workflow input. It migrates the old CI-only `sharedGlobals` reference out of `default` and never re-injects that
definition. Custom non-CI named globals and explicit workflow-target inputs remain repository-owned and unchanged;
workflow checks can still name their own CI inputs.

Policy excludes incidental Markdown through `!{projectRoot}/**/*.md` in `default` and `production`. A task that consumes
Markdown declares it through an input set without a matching negative: Nx exclusions win over positive paths in the same
fileset, so a later explicit path cannot re-include it. Inferred Cargo package filesets remain broad for arbitrary
compiled data, headers, schemas and build-script reads; incidental Markdown beneath a crate can therefore still
invalidate those tasks conservatively. Nx 23.2.1 hashes a cached target that declares no `inputs` over every file under
its project root and ignores the `default` named input, so `smoo monorepo check` refuses such a target: declare its
inputs, or give its target name a `targetDefaults` entry with `"inputs": ["default", "^default"]`.

TypeScript toolchain inputs exclude Markdown beneath workspace-root `patches/`, but retain every other patch byte and
pin, including extensionless `PIN` and `VERSION` files. Native content-hash regressions distinguish those inputs from
README and unrelated-source edits. A task that consumes Markdown declares it through an input set without a matching
negative: Nx exclusions win over positive paths in the same fileset, so a later explicit path cannot re-include it.
Inferred Cargo package filesets remain broad for arbitrary compiled data, headers, schemas and build-script reads;
incidental Markdown beneath a crate can therefore still invalidate those tasks conservatively.

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

A persisted warm cache is checked before waiting for the cold-fill lock. Each Cargo resolution child and the
asynchronous wait for a foreign resolution lock have a 300-second bound. A refusal names the child or lock, the
workspace and elapsed bound; a timed-out child and its owned process group are killed. These bounds do not change the
native synchronous lock acquisition or Cargo's own package-cache arbitration.

A plugin worker's normal exit, host disconnect, or handled termination kills Cargo children it still owns; on POSIX it
kills the whole process group, including wrapper descendants. Standalone callers also forward unhandled termination
signals to their owned groups. An uncatchable `SIGKILL` cannot run an exit hook; the kernel still releases its file
lock.

The `@smoothbricks/nx-plugin:cargo-resolve` diagnostics channel reports `{ phase: "join", manifest }` when a caller
joins an existing flight. With no subscribers, inference neither allocates nor publishes an event. The cache regressions
use this readiness signal and Cargo control-channel events to prove concurrency and child teardown without sleep-based
timing assumptions; their outer hang guards report still-open test spans and process/gate state. The counted Cargo
fixture delegates to the command selected before its shim was installed, with that original search path. An installed
jobserver launcher remains in the chain; it must not rediscover the counted shim and recursively enter the broker. The
real-producer regression covers both counted metadata and forwarded workspace discovery, with one metadata child.

Keep workspace-owned runtime state outside Nx's source index. The root `.nxignore` ignores every dot path with `.*` and
re-includes each tracked dot input by its dot-named prefix, such as `!/.github` or `!/packages/lmao/.cargo`: a dot-named
directory must be re-included to reach its tracked children. Nx's watcher reads only the root `.gitignore` and
`.nxignore`, not nested ignore files or Git's `.git/info/exclude`. `tooling:lint-dot-inputs` is an additive lint
dependency, keyed on the tracked file inventory as well as ignore-file contents. It fails when a tracked dot path lacks
its `!/` line or Nx's ignore checker excludes it, so a newly tracked dot input cannot silently disappear into `.*`.
`nx-dot-input-policy.test.ts` guards native index and file-hash parity for existing tracked inputs, unchanged project
graphs across the policy cutover, and actual daemon graph-pass silence for `.icm*` stores and hidden Bun staging. The
`.nxignore` file itself intentionally has new bytes. A tracked dot edit still reaches the daemon's watcher and graph
recomputation. Watching daemon logs, plugin sockets or job records instead turns each graph computation into another
file event and another graph computation.

### Runtime inputs

Every runtime input, inferred or declared, starts with `sh node_modules/@smoothbricks/nx-plugin/runtime-input.sh`.
Command-specific environment assignments follow the wrapper as `env NAME=value command ...`, so setup failures and
command failures use the same fail-closed path. For example:

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
configuration. Git reads the global ignore file (`~/.config/git/ignore`) even then, so `smoo-nx-cargo-hash`, whose git
queries decide which package sources the key covers, also passes `-c core.excludesFile=/dev/null`.

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

The workspace root manifest also drops `scripts`, `smoo` and `nx` from these three targets' hash. No check reads them:
scripts are commands a developer runs, `smoo.developerLinks` names links whose linked bytes reach a hash only through
their own inputs, and the root project's `nx` configuration is hashed as that project's configuration. Hashing them made
a root `postinstall` or developer-link edit re-key every lint and typecheck in the workspace.

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

## Test Walk Policy

A test pays its setup once and asserts after each step; two cases that test one path are one test. Consuming
repositories enforce this in their root Nx lint with the `smoo-nx-test-walks` bin. The bin judges only pairs with a case
changed since the upstream merge base. Among those pairs it reports each case's nearest refusing partner and its nearest
advisory partner separately, so an advisory pair never hides a refusal. Existing pairs therefore need no allowance
table. `smoo-nx-test-walks --base` prints that base for the target's runtime input, because the verdict depends on it
and no file hash names it. `upstreamBase` picks the upstream in this order: `NX_BASE` (refused when it names no commit),
`refs/remotes/main/main`, the local main's upstream branch, `refs/remotes/origin/main`, then `main`.
`findDuplicateScenarios(root, files, changes)`, `formatDuplicateScenario`, `diffScope` and `upstreamBase` are exported
from `@smoothbricks/nx-plugin/test-walk-policy`, and git failures come back as `GitAnswer` values.

The policy reads bun:test `test`/`it` cases in test files and `#[test]`-family fns in any Rust file. A body that is one
wrapper call, `await withHarness(memoryBackend, async (h) => { … })` or `runtime().block_on(async { … })`, walks the
call itself as its first step, with its body replaced by `$body`. It then walks the wrapper's statements, so a different
backend or runtime still tells two cases apart. A case's identity is part of the comparison, and only cases of one
identity are compared. Identity covers `test.failing` in TypeScript, and `#[should_panic]`, `#[ignore]`, `#[cfg(…)]` and
`#[cfg_attr(…)]` in Rust. It also covers the `before*`/`after*` hooks and the declarations of the enclosing `describe`
chain.

Each top-level statement of a case body is normalized several ways:

- `exact` renames the names the case declares to their declaration index.
- `shape` also abstracts every literal.
- `skeleton` also collapses the declared names.
- `flow` is `exact` with the declared names collapsed.

Field, method, key, struct-literal key, tuple-index and path names are never renamed. A pure assertion reads only what
earlier steps built. It is one of:

- an `expect`/`assert…` call that calls only methods and runs no closure, constructor, rejection or throw matcher;
- an `if (…) throw` guard;
- an `assert*!` that borrows nothing mutably and calls no method on a `let mut` local.

Whether a method reads or acts is not statically decidable. A method that acts inside an assertion, such as
`expect(await queue.drain())`, is treated as a read, and review decides. `TEST-ONE-WALK` refuses two shapes with
`error[TEST-ONE-WALK]` (exit 1):

- **same-path:** the cases share at least two leading statements exactly, and one of them only purely asserts beyond
  them. Move its assertions into the other walk at the statement the diagnostic names.
- **literal-twin:** two or more statements equal once literals are abstracted; identical bodies are reported as verbatim
  repeats. Make one table or property over the values. Keep each original case's label in its table row and include it
  in assertion diagnostics; when rows need different numbers of checks, store all expected values per row and assert
  every one in the common walk.

It advises on a third with `warning[TEST-ONE-WALK]`, which never fails the run. The warning is for the writer and the
reviewer, who decide whether the variants share one walk:

- **same-scenario:** one case's skeleton is the other's head (at least one statement) and tail around exactly one
  statement of its own. The tail is the action and its outcome: at least three statements equal in flow, with at least
  one action among them. The injected step differs, for example which crash window or precondition. Extra trailing pure
  assertions on either side are more checks on the same outcome. Its precision is too low to refuse. Walk the variants
  over one fixture, or make one table over the injected step.

A separate case stays only for a distinct boundary or edge: its setup or path diverges, or it needs a state the walk's
earlier steps destroy. Each diagnostic names the file, both cases, the shared statement count and that remedy.

## Nx Target Naming

Target names are `{tool}-{output}` names. Use names like `tsc-js`, `tsdown-js`, and `cargo-wasm`; `build` and `lint` are
aggregates.

Concrete targets come from concrete files:

- `tsc-js` emits transformed JavaScript through a temporary JS-only `ttsc` config, restores execute bits on outputs
  declared by `package.json#bin`, then emits declarations and declaration maps from the original project with native
  `tsc`. Direct project references and every cached output lane are preserved.
- `typecheck` is inferred from `tsconfig.lib.json` and runs the native compiler `SMOO_TYPECHECK_TSC` names (built by
  `devenv.smoo.nix` from a pinned TypeScript revision) as `-p tsconfig.lib.json --noEmit`, directly rather than through
  `ttsc`. Transform plugins' diagnostics are enforced by `tsc-js`, which runs them.
- `typecheck-tests` is inferred from `tsconfig.test.json` and runs the same compiler on
  `-p tsconfig.test.json --noEmit`. It first rebuilds the current package's `tsc-js` output (or its non-TypeScript
  `build`) so self-imports resolve after `clean`. Transform calls in test files are checked when the test runner
  transforms them.
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
  edited crates. Each member crate gets a cached `cargo-test-<package>` run (30s slow-test warning like
  `bun test --timeout=30000`, no per-test kill) whose inputs include that crate and its path dependencies, and whose
  command runs
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
  cached verdict. Runtime hash inputs use that same workspace-relative installed path, never the physical plugin
  checkout. Both the runtime output and the owning target's command declaration therefore remain portable.
- A crate whose suite is better spread over several runners declares `[package.metadata.smoothbricks.test] shards = N`,
  and gets `cargo-test-<package>-shard1..N`, each running `--partition hash:i/N` under the same progress bound. nextest
  assigns a test to a shard by hashing its name, so the shards stay an exact partition of the crate as tests and test
  binaries are added, and a stale `N` can only make a target slow — never drop a test. Omitting the key means one
  target, as before.
- Tests that a `nextest.toml` override singles out run separately in `cargo-test-<package>-exceptions-shard1..N`, using
  the same declared partition count. Ordinary shards run the complement of that class and of every declared serial group
  (below), so the union still covers every test exactly once. The plugin's own override raises the `slow-timeout`
  warning period for compile-fail tests; lifting them out keeps their long bodies from delaying an ordinary shard, and
  partitioning the class keeps every target inside its progress bound without relaxing it. The remaining non-group
  exceptional tests stay normally schedulable. Every piece runs `--no-fail-fast`, so a failing test does not stop the
  rest of its piece. The exceptional filter is the union of the overrides in the plugin's own `nextest.toml`, so adding
  one there is the whole change to that class. Serial groups are classified separately, by `test-group` overrides in the
  workspace's `.config/nextest.toml`; the plugin's file declares none. Only sharded crates need these targets; an
  unsharded crate already runs all its non-group tests in one process. Empty partitions pass, including platforms where
  the tests are `cfg`-ed out. `nextest-partitions.test.ts` executes the inferred archive and shard commands over 29
  tests and asserts that the ordinary shards ran exactly the 13 ordinary tests once and the exceptional shards exactly
  the 12 exceptional ones. The suite builds one shared fixture and archive, then runs separate bounded cases through the
  real default serial-group executor: a successful count and run with the group's probes exactly once each, three
  refusals that run nothing and clear the task's previous verdict, and a failing run whose ordinary failures still run
  the group's remaining probes. The shared fixture first produces one real passing verdict, before any case runs. Each
  refusal copies that seed and removes its own marker log, so neither a timed-out success case nor a prior refusal that
  wrongly ran can become another case's precondition. The success case still runs and verifies the real executor
  independently. Drivers run the built production Node executor, supplied by the test target's build dependency, rather
  than starting a native TypeScript compiler for every case inside the shard's progress bound. Each case settles its
  owned processes before the next case starts; the fixture is removed only after the final settlement.
- A repository's serial test groups each run in one runner, `cargo-test-group-<group>`, owned by the cargo workspace
  root beside `cargo-test-archive`. A group is declared once, in `<cargo-workspace>/.config/nextest.toml`: a
  `[test-groups.<group>]` table with `max-threads = 1`, and default-profile overrides that name it with `test-group` and
  `platform = 'cfg(target_os="macos")'`. The runner's filter is the union of those overrides, so the runner and the
  per-crate exclusions cannot drift; every crate piece excludes it with `not (…)`, so each test is selected by exactly
  one target. The runner is not inferred off macOS, where nextest applies the group nowhere and no piece excludes its
  tests. It is a `bounded-exec` run of the group's selection under the same progress bound as every test target, and
  asks for one CPU token, matching the group's one runnable test process instead of reserving idle host capacity. The
  run carries `--no-fail-fast`: an ordinary test failure does not stop the group's remaining tests, and a failed verdict
  lists the failed ones. It carries `--no-tests=fail`, so a filter that selects no test fails the group. Ignored tests
  keep their explicit lane. A crate named `group-*` is refused, since its runner name would collide. A repository's
  serial group may measure each test's code time against a class with a nextest run-wrapper from the plugin's source
  (`src/nextest-code-bound.ts`), which warns of a test over its class and never fails or kills it; the group's target
  hashes that file as an input, so changing it reruns the group (`specs/cowshed/08_testing.md`, "Integration tests (real
  substrate)").
- `cargo-test` is the ordinary suite alone: it depends on the crate pieces and never on a serial group, so a run of it
  never reaches a group's real-image tests. The inferred `test` of the cargo workspace root and of every project that
  owns crates depends on `cargo-test` alone. Explicit repository config owns host coverage: this repository's root
  `host-e2e` depends on both real-APFS groups, and `gate` depends on `host-e2e` plus every project's `lint`, `test` and
  `build`. `nx run-many -t gate` therefore covers the host boundary without pulling it into ordinary `test`; each group
  replays from cache unless its own inputs changed.
- A group's runner hashes only the crates its filter can select tests from, with everything they link, every member
  manifest and the repository's nextest config, so a change to an unrelated crate leaves it cached. The filter is read
  as nextest reads it (its precedence, blanks and predicate arguments; `filterScope`): a plain `package(<name>)` or
  `package(=<name>)` bounds a clause to that package, `and`, `&` and `-` keep the narrower side or the left side, and
  `or`, `|` and `+` take every clause. Anything that bounds nothing scopes the group to every member, as the archive is
  hashed: a clause with no package, a glob, substring or regex name, `not`, `deps` or `rdeps`, an argument with a
  backslash or a comma, a filter nextest refuses, or packages that exclude each other. A scope that names a package
  which is not a member of the Cargo workspace is refused when the graph is built.
- Inference orders nothing between two serial groups: each is serial only within its own runner. A repository that needs
  two never concurrent states the edge as explicit target config, `"dependsOn": ["...", "cargo-test-group-<other>"]`
  under `nx.json` `targetDefaults` or the owning project's `nx.targets`, and Nx starts the later group once the earlier
  one has finished. Explicit target declarations are not platform-conditional: dependency-only target nodes and their
  ordering edges remain off macOS even though no executable group runner is inferred. The ordinary `test` and
  `cargo-test` aggregates must still reach neither group.
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
CLI can satisfy the next wrapper invocation without re-executing tasks; uncached current inputs go through Nx's runner.
Both invocations must use the same cache location. Relative `NX_CACHE_DIRECTORY` and `NX_WORKSPACE_DATA_DIRECTORY`
values resolve against the Nx workspace root, so they remain checkout-local when inherited by another shell. Absolute
paths remain caller-controlled, including deliberately shared CI locations; an absolute path inherited from another
checkout does not become local merely because the working directory changed.

Before hashing, it compares a fresh native workspace snapshot with the daemon's file table and sends writes the watcher
has not delivered. It can reuse the exact previously awaited daemon graph only through a witness in the same Nx
workspace-data directory: filesystem-to-daemon reconciliation, an awaited authoritative graph generation, and identical
pre/post native file hashes and membership establish the witness. Every new or changed node is rehashed through Nx,
using device, inode, mode, length and nanosecond modification/change times to reject the native archive's whole-second
mtime ambiguity. The witness also binds the environment and installed Nx/configured plugin package resolution and owned
dependency-module/file closure (physical targets, aliases, package manifests and public outputs), plus the
workspace-resolved TypeScript analyzer. A package-declared dist directory link is proved by its physical tree, including
every node and the link itself; unreadable required module bytes fail loudly. Unknown plugin input contracts, unresolved
required packages, directory links whose external graph inputs are unproved, missing or unreadable witnesses, and any
mismatch require the authoritative graph RPC. An RPC need not publish a replacement witness when its required pre/post
evidence is unavailable or changed. No cache-file age or existence decides currentness. Workspace file membership,
nx.json, project/package manifests and lockfiles have no output exclusions; output writes can therefore refresh this
conservative graph witness too. Only Nx's current task hashes and matching artifacts decide whether anything runs: an
unrelated edit does not force cached-log replay, and an actual input edit cannot hit an old source snapshot. Argument
normalization, per-task environments and plugin pre/post hooks still run.

The daemon's output records are lossy: they are held in memory, tracked per collapsed directory, and erased by writes it
processes more than two seconds late — including, on a busy daemon, a restore's own writes. When it cannot vouch for a
task's outputs, the working tree is compared with Nx's local cache artifact for that exact task hash, entry by entry as
Nx's restore expands them: each entry must be reachable through real directories and hold the same node types, symlink
text (never followed), file permission bits and bytes. A tree the restore would leave unchanged is a hit, and the
daemon's record is re-armed as Nx's runner does after a restore. A missing or empty artifact, any difference, a read
error, or a write observed during the comparison is a miss.

A successful byte comparison may retain a cache-bound node inventory in that same Nx workspace-data directory. It binds
the exact task hashes, declared outputs and artifact paths to every compared working-tree/cache node and both sets of
real parent directories. Reuse requires the same device, inode, type/permission mode, length and nanosecond mtime/ctime
for all those nodes. It is evidence of the same previously compared bytes, never source identity or an mtime freshness
rule. Missing/unreadable evidence or moved nodes require a full comparison; mutations to either the cache or working
tree, parent symlink replacement, and directory membership changes cannot borrow an old equality verdict.

`probeBuilt({ target, cwd })` is the distinct typed read-only provider operation. It returns `EnsureBuiltProbeResult`: a
`miss` with its `MissReason` without starting Nx's runner or writing producer outputs; the existing operational socket
failure; a loud `provider-proof` failure; or a `hit` with `EnsureBuiltReceipt`. `ensureBuilt` is build/stream-only and
its CLI callers retain the existing hit/built/failed result. With the daemon disabled, `probeBuilt` returns a no-daemon
miss instead of running the target. Both operations preserve Nx argument normalization, hooks, per-task environments and
local cache parity; the read-only operation additionally requires exact-cache artifact equality. A managed consumer
requires the installed module's `probeBuilt` function before making any producer call; a missing operation refuses
loudly without falling back to `ensureBuilt` or writing provider outputs.

A probe receipt names the real provider workspace, Git HEAD and HEAD's tree, every task id/current Nx hash in the
selected target's dependency graph, and an independently read census of every Nx-expanded public output. Files carry
SHA-256; symlinks carry their text and mode without following them; directories carry mode, ordered membership and a
child-census digest, including empty directories. Source/revision/output nodes must remain unchanged across capture.
Missing, unreadable or stale output evidence refuses, never starts a build and never signs daemon-state identity.

Git provenance covers Nx's planned FILE inputs and the owning project/dependency manifests and root graph configuration.
Dirty matched inputs (including new or index-only changes) refuse. The native inspector cannot prove deleted membership
or a custom hasher's opaque reads: a deleted provider source or a bounded runtime/env input therefore uses conservative
any-dirty-provider-source refusal, excluding public outputs rather than treating them as committed source. Runtime/env
values remain explicitly labelled `nx-task-hash-only`, not Git provenance. Only the existing toolchain generator's exact
version-only recipe with declared cwd, a declared toolchain FILE pin and proved installed-module closure is bounded;
arbitrary shell/file/runtime commands and unavailable custom/native input proofs categorically refuse receipts. A caller
probing multiple targets uses the existing single-target API and rejects differing provider HEAD/tree identities before
publishing its immutable snapshot. This producer evidence binds a managed linked-package snapshot to one current build;
Nx alone decides whether its current keys and cache/output bytes match. A caller from another workspace never lends its
socket, database or cache overrides to the provider. Inside a managed shell (`DEVENV_RUNTIME`), `ensureBuilt` asks the
packaged `nx-socket-dir.sh` owner for the provider's binding: the root's verified cowshed runtime link when present,
otherwise its canonical checkout spelling under that shell runtime. Outside a managed shell, Nx retains its normal
defaults. A socket-owner refusal returns `disposition: 'failed'` with `reason.kind: 'workspace-socket'`, the selected
`taskId`, `workspaceRoot` and operational `detail`, before any task or daemon client starts. The owner's actual exit or
signal is preserved; the wrapper must not report that the target ran.

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

- An unchanged witnessed graph avoids graph reconstruction/transfer; current task hashing, daemon file/output probes and
  local cache reads remain. This is not a measured end-to-end latency guarantee and does not start an Nx CLI.
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

The repository installs Nx 23.2.1 as an ordinary Bun `patchedDependencies` entry: the root `package.json` lists
`nx@23.2.1` and `@nx/js@23.2.1`, and applies [`patches/nx@23.2.1.patch`](../../patches/nx@23.2.1.patch) to the registry
`nx` package. Bun installs a patched package project-local under `node_modules/.bun/`, so the `nx` the repository runs
is the patched one.

`@nx/js` imports `nx` (`nx/release`, `nx/src/…`) without declaring it
([nrwl/nx#34087](https://github.com/nrwl/nx/issues/34087)). `patches/@nx%2Fjs@23.2.1.patch` declares the peer, so Bun
binds the same pinned Nx through the package's normal dependency namespace.

Native APFS fixtures install this same pinned package and patch offline into their own checkout, using Bun's local
cache, and report each installation's elapsed time. They do not borrow another checkout's `node_modules`: a patched
package's dependency links can traverse lexical `.bun` symlinks outside an explicit sandbox read grant even when the
resolved payload is granted. The workspace's own read authority covers those local links; only shared dependency
payloads receive additional bounded store grants. Installation runs once per suite template, which fixtures clone.

The Nx patch repairs upstream Nx runtime behavior, separately from this plugin's workspace-owner checks:

- **Task history uses the client's native database connection.** Task details and history must share that connection; a
  daemon can have frozen a different database namespace before a later client supplies workspace-data overrides. The
  patch removes the obsolete history RPCs and retains native errors, including post-run failures. See
  [upstream Nx #37268](https://github.com/nrwl/nx/pull/37268).
- **The default cache limit uses the cache's own filesystem.** It takes ten percent of `statfs(cacheDir)` capacity, or
  its nearest existing ancestor when the directory has not been created. This avoids synchronously inventorying every
  mounted disk. Explicit `NX_MAX_CACHE_SIZE` and `nx.json.maxCacheSize` retain their precedence; filesystem errors other
  than a missing directory still fail the operation. See [upstream Nx #37269](https://github.com/nrwl/nx/pull/37269).
  Cowshed exports an explicit caller-overridable default of configured build-volume capacity divided by ten for host
  shells and jobs. The hosted setup action keeps its existing separate `20GB` ceiling.
- **The daemon keeps graph plugin workers running.** Nx stops an isolated plugin worker after the last phase it has
  hooks for, which for a graph-only plugin is every graph; the daemon recomputes the graph on every tracked file change,
  so each change spawned and loaded every graph plugin's worker again (median 199 ms against 63 ms from edit to graph in
  a one-project fixture, 0.9–1.5 s per worker under a gate's load). On the daemon a worker with graph hooks now lives as
  long as the daemon; one-shot clients and task-only plugins keep the eager shutdown. See
  [upstream Nx #37271](https://github.com/nrwl/nx/pull/37271).
- **Nx resolves typescript and release version actions from the workspace.** Nx 23.2.1 loads both with a plain `require`
  from its own location. Where that location cannot see `typescript`, its dependency analysis skips every source import
  without a warning (the first install that exposed this lost 12 of 44 graph edges), and `release version` cannot
  resolve `@nx/js`'s version actions. The patch resolves both through `getNxRequirePaths` (the workspace first), falling
  back to Nx's own location. See [upstream Nx #37272](https://github.com/nrwl/nx/pull/37272).
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
- **A restored output is newer than the output it replaces.** Nx restores a cached output with `std::fs::copy`, which on
  macOS clones the file and keeps its timestamps, so a restored output carried the mtime it had when its cache entry was
  written. Build from input A, rebuild from B, return to A, and the restored A was older than the B output it replaced:
  every consumer that judges freshness by mtime (a Cargo build script's `rerun-if-changed`, make, a file watcher) kept
  what it built from B. The patch stamps each restored file with the time of the restore, as running the task would
  have: after `copyFilesFromCache` for a local hit, and after the native `applyRemoteCacheResults` for a remote hit,
  which untars the artifact with its stored mtimes and restores it with the same copy. Restoring one directory output of
  10,000 1 KiB files took a median 3.4 s for the native remove and copy and 0.79 s more for the stamp, on a machine at
  load average 70–120. Not yet proposed upstream.
- **A file rewritten within the second it was hashed in is hashed again.** Every Nx context (each run without the
  daemon, and the daemon when it starts) hashes the workspace's files through `<workspace-data>/nx_files.nxt`, which on
  Unix keeps each file's hash beside its mtime in whole seconds, and reuses an archived hash while that second is
  unchanged; size and nanoseconds are not compared. A file written again within the second it was hashed in keeps its
  mtime and hashes as the bytes it replaced: a revert replays the cache entry of the edit it reverts. The patch applies
  Git's racy-index rule before the native context reads the archive: when the archive holds an mtime from ten seconds
  before its own write (the longest a hashing pass is assumed to run) or later, it is removed and every file is hashed
  again. A full pass over 6,370 files measured 288–753 ms at load average 70–110. The check reads each entry's mtime
  from the archive's rkyv layout as Nx 23.2.1's binary writes it (the root's entry count and entry pointer in the last
  12 bytes, 24-byte entries with the i64 mtime at +8), in under 1 ms for 6,396 files. In 134 synthetic workspaces of 1
  to 400 files dated 2001 it kept every archive, and it removed every one once a file was written in the pass. An
  archive whose root does not fit is removed. A file dated ahead of the archive keeps it distrusted, so every run hashes
  everything until the clock passes that date. The check cannot see a pass longer than ten seconds, nor an archive
  another Nx process rewrites between the check and the native read; on Windows, whose native mtime is a FILETIME, it
  does nothing. The native fix records every entry dated from the second its pass started on as racy and has none of
  these limits. Not yet proposed upstream.
- **A failed run exits 1, not 130, when it skips the tasks behind a failure.** Nx skips every task that depends on a
  failed one, and tells no life cycle about a skipped task, so the results `nx` reads its exit status from had none for
  it; `didCommandComplete` takes a discrete task without a result for one the run never finished, and `nx` exited 130,
  the status of an interrupted run. A gate then reported `Failed to run "nx" due to exit code 130` for an ordinary test
  failure. The patch has `didCommandComplete` also accept a task the runner reports skipped, so the exit status is 1.
  With `--nx-bail=true`, pending tasks have no status and active tasks can be stopped by bailout cleanup; the CLI uses
  the orchestrator's actual bailout and interruption flags, so a known failure still exits 1, but a signal or forced TUI
  stop wins and exits 130. The terminal output's "Tasks not run" list is unchanged. Only that check sees the runner's
  skipped tasks: `invokeTasksRunner` and the plugins' `postTasksExecution` hooks still get Nx's own results, with none
  for a skipped task, so a caller that replays Nx's skip rule over them counts it once. (An earlier version of this hunk
  added a skipped result to those results, and such a caller counted no task as skipped.) Not yet proposed upstream: Nx
  `master` reports skipped tasks the same way.
- **A workspace has one daemon, and a second one leaves it alone.** Every daemon overwrote `server-process.json` with
  its own pid, so two clients that each found no daemon started two, and the later one displaced the earlier. A client
  of the displaced daemon lost its socket mid-request (`EPIPE` on `RECORD_OUTPUTS_HASH_BATCH`) after the run had printed
  its summary. Under a configured `NX_SOCKET_DIR`, every daemon binds the same path, and closing a listening Unix socket
  unlinks its path. The displaced daemon's shutdown therefore removed its successor's socket, the next client found no
  daemon and started another, and one workspace saw a new daemon every five seconds. Before it does anything else, the
  patched daemon creates the record with link(2), which never replaces a file. If a live daemon of the same Nx version
  that serves the same root holds the record, the new one waits for that daemon to answer and then exits. A record whose
  holder is dead, runs another Nx version, or serves another root (a copied workspace) is replaced. A daemon whose
  socket, at the path it bound, has been removed or rebound exits within 20 ms, since no client can reach it. It leaves
  a path another daemon has bound alone, so that daemon's socket is not unlinked. A holder starts when it claims, so a
  live one that has not answered by the time its record is a minute old (a pid reused after a crash) is stale and
  replaced. On a filesystem without hard links the record is created exclusively instead. Not yet proposed upstream.
- **A run that throws part way through does not report success.** The default tasks runner ends its life cycle in a
  `finally`, so the static summary also prints when the orchestrator throws mid-run. It printed "Successfully ran"
  whenever no task had failed or stopped, and counted each task the run never reported as skipped: a focused `run-many`
  whose daemon socket died while recording output hashes printed success with "10 skipped", then `Error: write EPIPE`.
  The orchestrator reports every task it finishes except one it skips, and it skips a task only behind a failed or
  stopped one, so the patch prints success only when every task was reported. Otherwise the summary says the run did not
  complete and names the unreported tasks: under "Tasks not run" after a failure, as before, and under "Tasks the run
  ended without finishing" when none failed. Nx `master` prints the same summary. Not yet proposed upstream.
- **Nx refuses an environment set up for another workspace.** A shell that moves into another checkout keeps the
  `NX_WORKSPACE_ROOT_PATH`, `NX_WORKSPACE_DATA_DIRECTORY`, `NX_CACHE_DIRECTORY` and `NX_SOCKET_DIR` of the workspace it
  was set up for, and Nx takes `NX_WORKSPACE_ROOT_PATH` verbatim as its root: `nx daemon --stop` run in the second
  checkout stopped the first one's daemon and deleted its socket directory. The `nx` CLI now finds the current
  directory's workspace by its `nx.json`, `nx` or `nx.bat` alone, ignoring the override, and refuses any command, before
  it reaches a daemon, socket or file, when one of those variables, `NX_DAEMON_SOCKET_DIR` or
  `NX_PROJECT_GRAPH_CACHE_DIRECTORY` names another workspace. The error names each such variable with its value and both
  workspaces, and says to run under the current workspace's own environment (`direnv exec <workspace> nx ...`).
  `NX_WORKSPACE_ROOT_PATH` names the directory it holds; a data, cache or socket directory belongs to the workspace
  whose root contains it, found by walking the directory's ancestors to a workspace marker (`nx.json`, `nx` or
  `nx.bat`). Workspaces are compared by the device and inode of their root directory, not by path, so every path to the
  current workspace is that workspace: a symlink, a bind mount or a firmlink. CI depends on it: `setup-devenv`
  bind-mounts the checkout at `/work/...` for devenv's evaluation cache, the environment names that mount, and the steps
  run in the checkout. Compared as resolved paths the two were different workspaces, and every `nx` of the job refused.
  A current directory inside no workspace is not refused. A directory outside every workspace, such as a socket
  directory under `/tmp`, is left unchecked: only the daemon behind it could say whose it is, and reaching that daemon
  is what the check prevents. The daemon, plugin workers and tasks do not start through the CLI and are not checked; an
  `nx` a task runs from a project directory resolves to the task's own workspace and passes. A cache or data directory
  that several checkouts share on purpose has to lie outside every workspace, or it is refused. Not yet proposed
  upstream.
- **An idle daemon releases its workers after five minutes.** The patched `SERVER_INACTIVITY_TIMEOUT_MS` is 300000 ms,
  not stock Nx's three hours. Normal requests and watched source changes reset that timer; an open request connection,
  file watcher or graph listener retains the daemon even when silent. The last request connection's close starts a fresh
  five-minute idle interval. Cowshed's existing usage-aware keeper remains authoritative for sheds whose tree changes
  without a job, session or task-database holder. No daemon is disabled and the next normal client starts one again. The
  non-default, uncached host proof is `bun nx run tooling:nx-daemon-idle-proof` from the checkout root. Its declared
  compile dependency emits the Typia-transformed product before Bun runs it. The proof holds a real silent client beyond
  five minutes, disconnects, and records the kernel and daemon-log evidence of the next five-minute departure. It stays
  outside the bounded Bun shards; their fast regression checks the actual installed policy export.
- **The daemon stops for an ignore file only when its watcher's rules may be stale.** Nx's native watcher reads its
  ignore files once, in `watch()`, and the daemon stopped itself on every watcher event for a `.gitignore` or
  `.nxignore`, whatever the file then held. A rewrite with the same bytes (a checkout, a formatter) reports one, and so
  does FSEvents for a write made shortly before the daemon's stream started, when fseventsd numbers it after that start.
  The stop destroys every client socket at once, so a file-watcher client lost the notification of the batch that
  carried the event, and a watcher waiting to judge an edit in that batch waited forever. The patch keeps the content
  hash of each ignore file the workspace context read, but only where neither the file nor its directory changed since
  just before the watcher's read (judged on the filesystem's own clock, by stamping a probe in the workspace data
  directory): those are the bytes the watcher's rules came from. An event stops the daemon unless its file is one of
  those and still hashes the same; a file changed between the watcher's read and the context's, or one the context never
  held, stops it as before.
- **A failure before execution does not crash a completion lifecycle.** The
  [run-information repair](../../patches/README.md#run-information-after-a-pre-start-failure) owns the completion,
  history and timing contract.

Publishing or installing `@smoothbricks/nx-plugin` alone does **not** change a consumer's Nx. `smoo monorepo update`
distributes the managed `patches/nx@23.2.1.patch` and maintains the root registry override `nx = "23.2.1"` plus its
matching `patchedDependencies` entry. Run ordinary `bun install`, commit its lock changes, then prove a frozen install
and the affected normal Nx gates. Do not replace the registry dependency with a local link or hide a failure by
resetting the database or disabling the daemon.

The patch is keyed to `nx@23.2.1`; a different Nx version needs its own patch file. On an Nx upgrade, remove each hunk
only when the installed upstream release contains that repair and the task-history namespace, cache-bound,
resident-worker, store-resolution, task-graph, restore-time, racy-archive, skipped-task exit, daemon-claim,
incomplete-run summary, foreign-environment, ignore-file and run-information regressions pass; preserve any repair not
yet released. The restore-time regression fails without its hunk only on macOS, where the copy clones (on Linux
`std::fs::copy` writes a fresh mtime), so run it on macOS before dropping the hunk. When every hunk is upstream, remove
the `nx@23.2.1` and `@nx/js@23.2.1` entries from `patchedDependencies` and delete their patch files.

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

`@smoothbricks/nx-plugin:bounded-exec` runs a shell command under a no-progress bound or a wall-clock ceiling, with a
force-kill grace period. Test targets use this executor so hung test processes fail predictably instead of blocking Nx
indefinitely.

A test target is bounded by progress alone: `idleTimeoutMs`, 180 s (`BOUNDED_TEST_IDLE_TIMEOUT_MS`, the hang backstop of
`specs/cowshed/08_testing.md`), with no `timeoutMs`. The bound re-arms whenever the runner reports a finished test: a
nextest status line carrying its run counter (`PASS [   0.438s] (1/4) …`), a libtest `test … ... ok` line, or a bun
result line or dot. Output that finishes no test is not progress: nextest's `SLOW` warning, repeated every slow-timeout
period for a test still running, and a hung test's heartbeat both keep printing. A suite that keeps finishing tests
therefore runs as long as it needs, however slow the host, and a test that never returns ends the run 180 s after the
last test that finished. bun prints nothing per passing test when it believes an agent runs it, so the executor asks a
lone `bun test` for `--dots`, one dot per finished test. A command that reports no tests makes progress only by exiting.
nextest's slow-timeout and the serial groups' code-time classes only warn; no per-test bound kills a test or fails one
that passed. `timeoutMs`, an absolute ceiling, is for commands that report no tests; a target names at least one of the
two.

On a macOS host, every `bounded-exec` task runs with its own `TMPDIR` lease: a directory on one RAM-backed APFS volume
per user, so test temp files never reach the SSD. The volume is created lazily by the first task that needs it and
detached when the last live lease ends, since its pages return only on detach. It is a legacy DiskImages RAM disk, so it
spends none of the AppleDiskImages2 attach budget cowshed counts (`specs/cowshed/01a_apfs.md`, "How the APFS host
degrades"). A lease whose task died is reclaimed by the next task, unless an image is still attached from a file below
it or a mount sits below it: those belong to whoever attached them, so the lease and the volume are kept and every new
task names them. A target's own `env.TMPDIR` wins over the lease. Inside a cowshed sandbox, which cannot write the lock
in `/private/tmp`, the task keeps its inherited `TMPDIR` and says so once. A failed task that left the volume nearly
full names the volume.

Outside macOS, the executor never provisions an APFS RAM disk: it keeps the inherited `TMPDIR`, or uses the target's
`env.TMPDIR` override, without reclaiming either directory. The host temp-directory proof runs the real default executor
for that contract on Linux; on macOS it exercises RAM-volume recovery, shared leases and final detach.

The volume's kernel `O_EXLOCK` admits ledger mutations and releases on process death. It is held only for cheap lease
and operation claims and their publication: host-wide process and disk-inventory probes, directory cleanup, provisioning
and detach run outside it. A caller re-takes the lock and revalidates the volume, lease identity and its owned claim
before publishing a mutation; another caller can take a lease while a dead lease is being inspected. An acquire that
fails after taking its lease directory gives it back, and detaches the volume if that was the last lease, as the last
release would have.

Checkouts of this plugin from before the ledger lease under the kernel lock alone and never read the claims, so the two
protocols must never share a volume. The protocol's revision (2) names the volume, its lock and its ledger, and adds as
many sectors to the RAM disk: the creator's stray reclaim matches user and size, so neither revision detaches the
other's disk, mounted or still attaching. On a host running both, each revision has its own volume.

A creation or detach claim is void once its owner's pid is gone. A crashed owner whose pid was reused holds every caller
waiting on the volume, and macOS gives no cheap way to tell the new process from the old without running `ps` under the
lock. So every 15 s that a caller waits on the same claim, stderr names its kind, owner pid, token, ledger and the wait
so far.

Each reclaim pass reads at most one I/O Registry and mount-table snapshot, not one per lease. A dead lease held only by
mounted filesystems keeps a positive verdict with stable mount and lease identities. A valid positive still names and
retains it without another host-wide probe; an unmount exposes the underlying RAM device and triggers a fresh probe.
Image-backed and otherwise unkeyable holds stay uncached: device-node reuse has no proven cheap incarnation key. Every
pass that claims such a lease probes it again, with a process census and the pass's inventory, which take seconds on a
busy host; that latency falls on the claiming task, outside the lock. A negative verdict is never cached, probe errors
never stand in for an empty holder set, and held directories stay unchanged.

Contended lock opens also use `O_NONBLOCK` and yield for 50 ms, avoiding a separate latent filesystem-worker deadlock
when one Node process makes five concurrent requests with four workers. The observed gate failures instead came from
cross-process waits behind long host-wide probes under the RAM lock. Real nested and sibling default-executor proofs run
with background single-lease processes to cover that shape. No case or target timeout is raised.

Once the process and holder checks permit removal, cleanup restores owner read/write/search permission on directories
through no-follow directory handles before deleting the lease. Read-only fixture directories cannot strand a dead lease
and block later tasks. File modes and symlink targets stay untouched; a directory on another device refuses cleanup
instead of changing an attached volume. Held leases undergo no permission changes.

The real RAM-volume regression walks one volume's lifecycle in a single test: unmarked debris and a live-lease refusal,
recovery over the debris, concurrent leases, failed-acquire lease/claim rollback, read-only cleanup, a held dead lease,
owner detach and reclaim, and the final detach. External symlink targets and held directory modes stay unchanged,
without duplicate provision and teardown; its 60 s case bound is unchanged and its target has the progress bound.

Whatever still works in a lease when its task ends is a leak, and is stopped before the lease goes. The usual one is a
fixture's Nx daemon: Nx detaches it into a session of its own, so the process-group kill that ends a timed-out command
never reaches it, and a test process that is killed runs no teardown. Deleting its root asks the daemon to leave only by
deleting its record, which an idle daemon notices within a second and a wedged one never does, and a process left on the
volume keeps it from detaching. So the task's own release, and the next task's reclaim of a dead lease, first hold
discovered processes with SIGSTOP and rescan their descendants and lease working directories until a stopped census is
stable. A process can fork after a table read but before its signal; only a table read after it is stopped closes that
gap. Discovery failures attempt to resume every held process; one denied SIGCONT cannot strand the rest. Errors retain
the original discovery failure and name any denied continuation with its PID and command. The settled census gets
SIGTERM then SIGCONT, SIGKILL if anything has not exited five seconds later, and names each PID once on stderr
(`stopped <pid> (<command>), which the command left working in its lease`). A lease with nothing in it costs no scan.
The lease's working directories are read again only after the same census is stopped in consecutive process tables,
including an empty one, which recovers children reparented out of the tree. This avoids host-wide cwd scans while
stopped-table quiescence is still being established. The line is the leak check: a suite that prints it has a fixture to
fix.

The volume mounts at `/Volumes/smoo-ram2-<uid>`, where DiskArbitration puts it: asking for any other mountpoint
escalates to an administrator dialog that blocks every `diskutil` on the host. That mount is `noowners`, and launchd
refuses a plist from it, so a test that bootstraps a launchd job keeps the plist outside `TMPDIR`.

The volume is created under its final name, `smoo-ram2-<uid>`, and is never renamed. It is mounted and marked at a
private staging directory, then unmounted and placed at `/Volumes/smoo-ram2-<uid>` by DiskArbitration, so a creator that
dies part way never leaves an unmarked volume at that mountpoint for the next one to dodge as `smoo-ram2-<uid> 1`.
Before creating, a task detaches every RAM disk of the same user and size that is mounted nowhere but at those two
paths: the debris of a creator that was killed or ran into its deadline. One that still carries a lease directory, or
has something attached below it, is named in the task's error instead.

Every `hdiutil` and `diskutil` execution goes through `cowshed disk-command <tool> -- <args>`, including host-test
inventory and cleanup. The cowshed is the installed one on `PATH`, which must provide this command, unless
`COWSHED_EXECUTABLE` names another. A target may set it in its `env`, workspace-relative or absolute: the task's own RAM
volume work and its command then both run that cowshed, given to the command as an absolute path. This repository sets
it to `packages/cowshed/bin/cowshed`, after `cowshed:build`, on the targets that exercise the facade (both real-APFS
groups and `nx-plugin:test-host`), so they test this checkout's cowshed, not an installed one that may predate a command
the checkout added. Cowshed owns disk-lifecycle leasing and starts image commands detached from the caller's signals,
with file-backed output. A caller deadline stops waiting; it never kills the disk command, because killing an attach or
detach can leak its kernel device until reboot. A still-running command retains its lease and is named in the timeout
error. Other helpers retain their process-group deadline. A process whose lease waits are recorded
(`COWSHED_LEASE_WAIT_RECORD`) asks for its phase `alone`; a gateway from before `alone` refuses the field at once and is
asked shared instead, for a minute. Cowshed's runner refuses `diskutil rename` and `renameVolume` for every caller; the
RAM volume is created under its final name and never renamed.

`bun node_modules/@smoothbricks/nx-plugin/src/disk-command-policy.ts <repository...>` checks tracked and nonignored
source, tests and scripts for direct disk-tool process calls. SmoothBricks runs it from `tooling:lint-disk-commands`;
consumers wire the same guard into their root lint. Rust callers use `SystemCommandRunner` and `CommandRequest`; script
callers use the CLI facade rather than introducing another disk-command runner.

Without an inherited parent pool, a `bounded-exec` command takes CPU tokens from cowshed's host budget before it starts,
when the gateway answers on the same socket (`specs/cowshed/05_gateway.md`, "Host CPU budget"), so concurrent gates on
one host keep their runnable work near the core count instead of each sizing its runners to the whole machine. It asks
for the runner's requested parallelism — nextest's `--test-threads` or one per core, `bun test --parallel=N` N, a cargo
build one per core, anything else one, or the target's `parallelism` option — bounded by positive inherited
`NEXTEST_TEST_THREADS`/`CARGO_BUILD_JOBS` ceilings. A configured count can lower an inherited ceiling, never raise it.
The runner uses `min(bounded request, granted tokens)`: nextest's environment and named flag, Bun's parallel flag, and
Cargo's jobs are sized to that limit. Cargo's `RUST_TEST_THREADS` retains its independent inherited/configured ceiling.
`BOUNDED_EXEC_CPU_TOKENS`, the sub-ledger and telemetry still name the actual held grant. The gateway shares tokens
fairly between checkouts (Nx workspace roots). `timeoutMs` and `idleTimeoutMs` start at the grant. The tokens go back
when the command exits, or with the executor's socket when it is killed. The wait prints one
`cowshed: cpu-tokens wait done elapsed=… tokens=<granted>/<asked>` line. With no gateway listening, the command runs
exactly as configured and nothing is said; a gateway from before the budget runs it unbudgeted and stderr says so.

A granted executor serves a private `BOUNDED_EXEC_CPU_POOL` with its actual token count. A nested default executor takes
from that immediate parent pool, capped at `min(asked, inherited, ceil(cores/2))`, instead of taking from the host while
the parent holds its grant. The pool accounts for siblings together; each admitted child serves only its own allocation
to grandchildren. Closing a child's connection returns its local allocation, never the parent's upstream grant. Upstream
loss revokes the downstream pool; losing a borrowed grant stops the command and records an ordinary failure, not a
timeout. A bare count or absent endpoint follows normal host admission. A connected parent pool's refusal or protocol
failure instead fails admission: taking from the host then could wait on the parent's own grant. `options.env` cannot
inject either marker. Parent-pool grants record `cpu.inherited: true` and their actual local wait; the span also says
`inherited=true`. Pool setup, listener and cleanup errors fail rather than bypassing accounting. Command bounds stay
unchanged.

The absent-parent fixture uses the executor's injectable budget with a real, controlled outer pool as its backing. It
proves a positive allocation and re-acquires it after the command ends, rather than depending on free tokens in the
host-wide gateway while the test runner itself holds an allocation. A bare count or unbudgeted success is not proof.

Every `bounded-exec` task records why it ended in `.nx/workspace-data/bounded-exec/<task>/verdict.json`, keyed by the
task id and the hash it ran at:

- `bound`: nothing failed but a wall-clock bound. Either the command outlived `timeoutMs`, or every failing test in the
  runner's JUnit report failed on its runner's per-test timeout (`cargo-nextest` writes `type="test timeout"` where a
  configuration still sets `terminate-after`, `bun test` writes `type="TimeoutError"`). A loaded host produces exactly
  these. When `timeoutMs` fires, the tests `cargo-nextest` was still running are reported as failures of
  `type="test abort"`, `process aborted with signal 15 (SIGTERM)` (SIGKILL once the force-kill follows); they are the
  bound's casualties and do not make the run `failed`. A test that dies of any other signal, or that is aborted with no
  bound to explain it, does.
- `wedged`: `idleTimeoutMs` fired: no test finished for that long. A host that is only slow still finishes tests, so
  this is a hang, never load. Its `tests` name the tests still running: those the runner said were running (nextest's
  `SLOW`, libtest's `has been running for over`) and never said finished, and those the JUnit report says it aborted
  when the bound stopped it.
- `failed`: a test failed on its own, or the command exited non-zero with no report naming a timeout.
- `passed`.

The report needs no target configuration. When the command runs exactly one `bun test` or `nextest run`, the executor
asks that runner for a JUnit report in the task's directory: `bun test` through `--reporter=junit --reporter-outfile`,
nextest through a one-key tool config (nextest takes the report path only from configuration). A `bun` is any unquoted
command word whose basename is `bun`, so `../bun-runtime/.runtime/bun test` is the runner too, and the rewrite keeps the
binary the target named. A script that spawns the runner itself passes
`--reporter=junit --reporter-outfile="$BOUNDED_EXEC_JUNIT"` on; the variable is set for every task. A command with no
runner, or with two (they would write one report), is judged by its exit alone.

`smoo-nx-bound-failures` reads the workspace's last Nx run (`run.json` in Nx's `cacheDir`, the directory
`StoreRunInformationLifeCycle` writes: the checkout's own `.nx/cache` under smoo's workspace policy) against those
records and prints one line per failed task. It exits 0 only when the run failed and every failed task's record, for
that task's hash, is a `bound`; a failed task without one is not. This diagnostic is not land acceptance. The land
wrapper, `tooling/land-host.sh`, requires `nx run-many -t gate` to exit zero: bounds, assertions and infrastructure
failures all stop the land without a retry or waiver. `smoo-nx-failures OUTPUT` reports unique failing tasks and
deduplicated exact diagnostics from that invocation's retained output, not a shared last-run record. The portable
`@smoothbricks/nx-plugin/failure-summary` export owns `summarizeFailures(output)` and `runFailureSummary(args)`;
repository entrypoints such as `tooling/land-summary.ts` call that owner without copying the parser. Its zero exit means
reporting succeeded, never that the failed gate may land. Known diagnostic sections omit fixture progress and daemon
debug dumps while preserving multiline assertions and source locations; unknown formats retain their output.

The shared policy API is exported from `@smoothbricks/nx-plugin/bounded-test-policy` for generators or other workspace
tools that need to normalize package JSON consistently.

```bash
nx generate ./packages/nx-plugin:bounded-test-targets --project @smoothbricks/my-package
```

The generator rewrites `package.json` so `nx.targets.test` uses:

- executor `@smoothbricks/nx-plugin:bounded-exec`
- command preserved from an existing `nx:run-commands` test target or direct `scripts.test`
- `cwd: "{projectRoot}"`
- `idleTimeoutMs: 180000`
- `killAfterMs: 10000`
- package script alias `nx run <project>:test --outputStyle=stream`

Workspace updates preserve no-op test targets rather than adding a second test runner. A no-op `test` aggregate passes
the check when it reaches at least one local test execution leg and every such leg is bounded, including through nested
no-op aggregates: `bounded-exec` with a command, a `cwd`, a positive `idleTimeoutMs` and `killAfterMs`, and no
`timeoutMs`. Upstream and non-test prerequisites such as `^build`, `build`, `typecheck-tests`, and Cargo
compilation/archive targets are ignored by this test-leg check. Each bounded Bun leg must declare a positive per-test
`--timeout` no greater than its own `idleTimeoutMs`; the migration's 30-second default is not a limit on explicitly
bounded lanes. Only a leg that runs `bun test` must start in the project root or its `src/`, where Bun's test-file
discovery is cheap; any other command (a cargo workspace's per-crate nextest legs run from the workspace root, a gate
may run a script that lives in another project) may use any `cwd`.

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

- `ownedFixtureRoot(parentDirectory, suite, prefix)` creates a root in the run directory of the test process that owns
  it, `<parentDirectory>/smoothbricks-fixtures/<suite>/run-<pid>`. The caller always names the parent: `tmpdir()`, or
  the directory of the volume a fixture has to lie on. The parent is resolved through every link first, so one spelled
  two ways is one run, and a process holds one run for each parent and suite. The first root a process creates for a
  suite under a parent first reclaims every run of that suite there whose owner pid is gone: each process working in it
  gets SIGTERM (SIGKILL if it outlives 10 s); once all have exited, every socket alias naming a workspace of that run
  (see `publishFixtureSocketDirectory`) is unlinked, whether that workspace still exists or not, then the run is
  deleted. A run whose owner is alive is never touched by another process, and other entries of the socket namespace are
  left alone. A killed process therefore leaves what it owned in a `run-<pid>` of its own, inside that parent's
  `smoothbricks-fixtures` directory, never as a bare prefix among whatever else the parent holds, and the next process
  to use that parent and suite reclaims it. The owner itself, on SIGTERM, SIGINT or SIGHUP (a bounded test leg's timeout
  sends SIGTERM, then SIGKILL 10 s later), first stops whatever still works in every run it holds, all at once, and
  makes no new root meanwhile; a repeated signal waits for that. Then it dies of the first signal it got.
- `reclaimDeadFixtureRuns(parentDirectory, suite)` runs that sweep on demand, for a test that has just watched a
  fixture-owning process die.
- `fixtureSocketDirectory(workspace)` names, without creating anything, the `nx` socket leaf below a short parent alias
  in the sandbox's socket namespace (`socketNamespace`). The alias is named for a hash of the workspace's physical path,
  so every client and daemon stop of a workspace names the same leaf, and no two fixtures share one. A namespace too
  small for Nx's longest socket name is refused here, before anything is published.
- `publishFixtureSocketDirectory(workspace)` makes that alias and returns the leaf. The workspace must be a physical
  directory in a run this process holds (`ownedFixtureRoot`), so the run's sweep can reclaim the alias if the process
  dies; anything else is refused. The alias points at the fixture's own `.nx/sockets` (mode 0700, never through a
  symlinked `.nx`); Nx makes the real `nx` leaf. This package's `withNxFixture` publishes its workspace and passes the
  directory to the body, and the CLI's `runFixtureNx` publishes each root it runs in. Both `fixtureNxEnv`s take the
  directory as an argument and publish nothing.
- `ownedWorkspaceSocketDirectory(workspace, slot)` creates the real leaf `a` or `b` beside `nx` below a published alias,
  for tests of multiple daemon socket directories. Its caller stops the daemon before removing the leaf.
- `retireFixtureSocketDirectory(workspace)` unlinks only the verified parent alias after the daemon exits, named from
  the workspace's path alone, so a deleted workspace retires too. A colliding non-symlink or retargeted alias refuses
  instead of removing another owner's entry; removing the fixture root reclaims its physical socket directories.
- `stopNxDaemon(workspace, stop)` runs the caller's `nx daemon --stop` and waits until the daemon recorded in
  `<workspace>/.nx/workspace-data/d/server-process.json`, and its descendants, has exited, then ends every process still
  working in the workspace. That final census also catches editor probes reparented when the daemon exits.

A fixture retires on every exit path, a throwing body included: stop its daemon, unlink its socket alias, then delete
its root. A daemon that outlives the stop keeps the alias and the root, so the next run's sweep still finds both.

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

### Workflow security and bounded execution

Managed action references use full commit SHAs with their release tags in comments. Read-only jobs explicitly set
`persist-credentials: false`; checkout includes the required history and blobs so later private historical reads do not
need credentials left in Git configuration. Jobs that push release or managed-file refs retain credentials explicitly,
with their reason beside checkout. A platform producer that fetches a private release-candidate ref also requires
authenticated Git transport; its checkout names that reason.

| Job                                                               | Persist checkout credentials | Reason                                                                                                                                                                                  |
| ----------------------------------------------------------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Validate, E2E, cross-test execution, deploy-only, preview cleanup | No                           | Complete history and blobs make historical reads local; these jobs do not push or fetch later private refs.                                                                             |
| Linux release candidate                                           | No                           | The candidate is versioned locally and transferred as a Git bundle, not pushed.                                                                                                         |
| macOS / cross-platform release producer                           | Yes                          | `releaseCollectPlatformOutputs` fetches private candidate/repair refs after checkout (`packages/cli/src/release/index.ts:335`); a full initial clone cannot contain refs created later. |
| Release publisher and pending-release healer                      | Yes                          | Publishing and repair fetch and push release refs.                                                                                                                                      |
| Managed-file healer                                               | Yes                          | It pushes the regenerated review branch.                                                                                                                                                |

Job timeouts remain generous and finite: Validate and compact release jobs use 45 minutes; Apple/cross-platform
production and combined platform releases use 90 minutes to accommodate cold native builds; production deployment and
managed-file healing use 30 minutes; deployed E2E and preview cleanup use 15 minutes. Cross-target test execution uses
30 minutes. These are job hang bounds, not replacements for package test deadlines.

Forgejo artifact steps use managed local JavaScript actions, not GitHub clients or absolute `uses:` URLs. They fetch and
verify the pinned GHES-gate-free upstream commit before running its original bundled entry point under the runner's Node
runtime. This preserves artifact inputs, outputs and execution conditions while allowing the unchanged jactionlint
default rules to validate the same workflow grammar for both providers.

A configured flattened producer runs its preflight even when no platform family matches. Its declared environment is
merged directly into every emitted step of its producer job; step-local fields take precedence. Producer names are
encoded as YAML strings so identifiers such as `TRUE` and `NULL` retain their exact spelling, not scalar coercions.
Runner-dependent values are evaluated in the valid step-level context, not job env. No producer value is exported
through `GITHUB_ENV`: GitHub
[forbids setting `NODE_OPTIONS` through that environment file](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands#setting-an-environment-variable).
If a declaration name cannot be carried, generation fails with that name in the diagnostic instead of silently dropping
it. Conditional preflight and platform-build steps are numbered from the actual emitted job sequence, so every following
step anchor remains exact.
