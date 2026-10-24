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
- an uncached `shard-timings` check, which `lint` depends on, beside `bun test` targets sharded with `--shard` and
  balanced with `--timings`
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

The `lint` target runs its commands with this plugin's `all-verdicts` executor, not Nx's `run-commands`, which in
parallel terminates the other commands once one fails and serially never starts the ones after a failure: one tool's
errors would hide another's until a later run. `all-verdicts` starts every command at once, and none ends another. Each
command's output prints as one block when it closes, then every command's verdict: `PASS`, `FAIL (exit n)`, or
`UNJUDGED` with its cause when a signal ended it, it never started, or the run itself was signalled. A signal to the run
ends each command's process group, and the run then ends by the same signal. The target passes only when every command
passed; an empty command list is refused, since it could never fail. The options keep `commands` and `cwd`, so each
tool's command, the inputs that key it and `metadata.eslintFileSet` are what they were.

Lint inputs include dependency production inputs because type-aware ESLint reads dependencies' declarations. This uses
`^production` unless the workspace opts into the versionless production input described below. Keep
`targetDefaults.lint` limited to shared cache policy or additive inputs such as `["...", "^production"]`. Smoo's
workspace policy removes static lint executors, commands, dependencies, outputs and replacement input lists because they
override source-aware inference for every project. Project-local `nx.targets.lint` declarations retain normal Nx
override precedence.

The workspace ESLint configuration keys every lint target with the config files and exactly the workspace files they
load, read the way ESLint loads them (`src/eslint-config-closure.ts`). Under Node, ESLint loads a TypeScript config
through jiti and any other config with a native `import()`, and every module a config reaches loads the same way. For a
jiti config the walk runs jiti's own transform, so an import that only types use is erased exactly as jiti erases it,
and jiti's own resolver picks each file under the `require` or `import` conditions of the call. The walk runs in
whichever process builds the graph (the Nx CLI, its daemon, smoo), so it restores `Error.prepareStackTrace` and
`Error.stackTraceLimit` after each transform: jiti's Babel replaces both for the life of the process, and under Bun the
wrapper it leaves makes every later `Error.captureStackTrace` onto a non-Error object throw (oven-sh/bun#46393), as
follow-redirects' error types do when axios loads. For a native config `import type` and `export type` are erased, and
the `node` that runs ESLint resolves each load with `createRequire(module).resolve` or
`import.meta.resolve(specifier, module)`, one process per walk layer. A builtin, or a package installed outside the
workspace's own files, adds nothing: `bun.lock` and the root manifest among the check inputs already key what is
installed. A package that resolves into the workspace (a member linked into `node_modules`) is keyed and walked like a
relative module. Every loaded file also keys the package.json files that decide how it loads: the nearest one (its
module format), a member's own (its entry) and that of a directory a relative specifier enters (its `main`). A load
whose specifier the source does not spell (`require(name)`, `import(path)`, `module.require`, `module['require']`,
`require.call`/`apply`/`bind`, an alias of `require`, `createRequire`), a specifier that is neither relative nor a
package name, one the loader cannot resolve, and a workspace file outside Nx's index fail graph construction with the
config, the loading module and the load: lint cannot cache a verdict its key does not cover. A file beside the config's
modules that no config loads keys no lint target.

Smoo's workspace policy also sets `"cacheDirectory": ".nx/cache"` in the root `nx.json`, and `smoo monorepo check` fails
without it. With no `NX_*_DIRECTORY` variable set, as in a host shell, the pinned Nx otherwise moves the task cache and
its database to a per-user `~/.nx/<id>` keyed by the repository's remote, which every checkout of the repository shares.
A configured `cacheDirectory` keeps the cache in the checkout's `.nx/cache` and the task database in its
`.nx/workspace-data` (nx.json has no key for the database's directory). Cowshed links both into the checkout's build
volume, and `src/nx-state-location.test.ts` checks where this repository's host Nx resolves them.

The plugin's own `nx run nx-plugin:test` aggregates four bounded Bun targets. Each uses Bun's native
`--timings=test-timings.json --shard=N/4` partition to balance measured per-file work, while normal discovery still runs
every test file (including newly added files) exactly once. A failed shard fails the aggregate. Each shard, and
`test-host`, is cached the way the unsharded `test` was: `targetDefaults.test` keys only the target named `test`, so a
target split from it states `"cache": true` and its inputs itself (`test-host` also hashes the cowshed build it runs).
`smoo monorepo validate` refuses a member of a cached aggregate that states no `cache`, since Nx then never caches it:
the shards ran on every gate from 10-02 to 10-22 for that reason. The shard count and standard 120-second target
deadline stay unchanged. Bun cuts the path-sorted files into four contiguous runs and costs a file the manifest does not
name at the manifest's median, so files added without a refresh cluster into one shard and can push it past its
deadline. The inferred `shard-timings` target makes that loud instead: for any project whose `bun test` targets carry
both `--shard=N/M` and `--timings=<file>`, it replays Bun's discovery rules without running a test and fails naming
every file the shards would run that the manifest does not name, with the refresh command
`smoo nx refresh-test-timings <project>`. That command runs the suite once without `--shard`, adding `--update-timings`,
and then repeats the check. Refresh after adding or slowing a test file; the manifest is scheduling data, not a test
allowlist. The shards of one suite must share one cwd and every argument but `--shard`, pass flag values as
`--flag=value`, and use no shell syntax beyond quoting; a shard command that breaks this fails graph construction,
because the files it runs could only be guessed.

Real Nx fixtures own their cache and workspace-data directories and stop their daemon before removing the fixture. The
daemon is the process its workspace record names, and its descendants in the process table; those get SIGTERM and are
awaited. Nothing is signalled because it works in the fixture: a host census of working directories and `TMPDIR` cannot
tell a fixture's orphan from an editor, an indexer or another job's process there, so it decides nothing
(`specs/cowshed/11_shell.md`, "Process identity and signal ownership"). A process that left the daemon's tree (a child
its last act started, reparented when it exited) stays in the tree the job's supervisor observes, and is retired with
the test command's subtree when that command ends (`bounded-exec`, below). The census only names what still works in a
fixture on stderr (`censusDiagnostic`): after a daemon stop, when a dead run is reclaimed, and when a signalled test
process deletes its own runs. Fixtures that intentionally strip caller directory overrides declare
`cacheDirectory: ".nx/cache"` in their own `nx.json`; this keeps the native task database local too, rather than leaving
a new `~/.nx/<repoKey>` behind. Offline, path-only Cargo fixtures likewise own a temporary `CARGO_HOME`; production
commands still keep the caller's Cargo home.

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
process connects to (the counted Cargo broker, a remote-cache server) binds through `bindListener`: an odd-offset
service port of the workspace's `COWSHED_PORT_BASE`/`COWSHED_PORT_BLOCK_SIZE` block, lowest first, skipping only ports
in use, and port 0 outside a block. A client that must be refused gets `claimRefusingPort`: the even session port of a
service port the test holds, which nothing binds (`specs/cowshed/08_testing.md`), never a port bound and closed, which
the next allocator in the block binds.

On macOS the working-directory census invokes `/usr/sbin/lsof` explicitly, independent of a test or devenv `PATH`. A
census that cannot be read says so in its diagnostic instead of naming no process; it never fails cleanup, since it
decides nothing. Only lsof's exit-1 process-disappearance race retains the partial inventory it produced.

The provider-real-Nx snapshot walk uses asynchronous children so its case timer can run while a producer or capture is
active. Its 60 s case guard covers one provider, daemon lifecycle and all provenance/refusal checks; it does not split
those checks into separately rebuilt fixtures.

Ignored-lockfile validity uses one daemon-successor walk: first refusal at the request boundary, then the successor's
request-free safety-poll invalidation. Both boundaries keep their original observations without a second fixture start.

Daemon-validity readiness waits for an actual successful PING under its existing 30 s guard, probing every 50 ms;
socket-directory events are not readiness because a bind/listen or closed-connection transition need not emit another
one. Expiry reports the expected and current daemon PID, observed process state and descendants, and the daemon log's
last 60 lines before fixture retirement removes them. The record-change and restart oracles remain event-driven.

## Cargo Workspace Layouts

The plugin discovers Cargo workspaces beside Nx project manifests at any depth:

- **Workspace owner:** the project beside a workspace `Cargo.toml` owns one `cargo-fetch`, `cargo-test-archive`,
  `cargo-lint`, cross-check, mutation, bench, and `cargo-retention` target. Its `cargo-lint` and `cargo-test` aggregates
  reach all workspace crates, including a root `[package]`; `cargo-test` is the ordinary suite, and the project's `test`
  also reaches the serial test groups (below).
- **Crate owner:** the deepest Nx project directory containing a crate owns `cargo-test-<crate>` targets. Its
  `cargo-lint` delegates to the workspace owner's one verdict, and `lint` never depends on the workspace test suite.
  Projects can own multiple crates. A nested independent workspace has its own prerequisites and does not inherit those
  of an outer workspace.
- **Commands:** validation runs from the governing Cargo workspace directory. One `cargo fmt --all --check` and one
  `cargo --frozen clippy --workspace --all-targets --target-dir target/cargo-lint --config 'build.warnings="deny"'`
  cover every member. Clippy of a crate is a check build of its whole closure, so a target directory per crate rebuilt
  every shared dependency once per crate — 49 closures and 11 GiB on one repository — and no per-crate cache hit repaid
  it. Clippy writes its own `target/cargo-lint`, so the archive, the Wasm and N-API builds and every other dev build no
  longer queue behind a workspace-wide check on its lock. Measured in a land gate, clippy held `target/debug`'s build
  lock for about 257 s while `cargo-test-archive` waited about 236 s with 0 crates to compile and `columine:cargo-wasm`
  about 264 s. The price is that clippy compiles its own host units (build scripts, proc-macros) once per directory.
  `cargo-lint-cross` keeps `target/cargo-lint-cross` for the same lock reason: a foreign triple's units are distinct
  anyway, and its own lock keeps the cross check from queueing behind host builds. Warnings are denied by Cargo's
  `build.warnings`, not `-- -D warnings`: arguments after `--` are clippy-driver's `CLIPPY_ARGS`, which enter every
  member's fingerprint, so a `-- -D warnings` gate would fingerprint every member differently from a plain
  `cargo clippy --workspace --all-targets`. `build.warnings` also fails on warnings replayed from Fresh units, so the
  gate still fails when a warning replays instead of recompiling. Cargo's own lock serializes the invocations that share
  a directory.
- **Retention:** Cargo never deletes a unit generation a later build superseded, so a busy checkout's target directory
  only grows. After each Nx run that executed a Cargo producer (an inferred target that builds, checks or tests into the
  target directory, marked by its `cargoProducer` metadata: `cargo-test-archive`, `cargo-test`, `cargo-lint`,
  `cargo-lint-cross`, mutation, bench, and the napi and wasm builds), the plugin's `postTasksExecution` checks whether
  each Cargo workspace is due for maintenance. Automatic passes run at most once per 24 hours after a successful
  completed pass; its existing receipt records completion time and configuration/workspace/target identity. A not-due
  hook reports that scheduling decision without locks, a fingerprint scan, cleanup or receipt replacement. It does not
  claim newer outputs were inspected or the graph is unchanged. Busy, refused, invalid, foreign or backward-clock
  receipt evidence never postpones the next attempt. The first executed producer after the interval runs a current pass;
  cache replays, fetch and noop tasks start none. Manual preview and apply always run fresh. Due passes reclaim
  superseded generations under `<profile>/build/<package>/<META>/`. They keep the newest generation of each unit
  identity of a workspace member (identity is every fingerprint field but its edges and source state, integers read
  exactly), every unit used in the last 24 hours (`-Zmtime-on-use` stamps each use), and the transitive dependency
  closure of those across every profile, host and cross alike. A pass holds every profile's Cargo locks without waiting:
  one held lock skips the pass as `busy`, and a profile it cannot read (legacy layout, unreadable fingerprint, missing
  `build/` or lock) refuses the whole pass. It moves each reclaimed generation into the profile's
  `.smoo-retention-trash/` under that hold, recording its inode first, and unlinks it after letting go; it writes its
  last report to `<target>/.smoo-cargo-retention.json`. Cargo's root-relative fingerprint paths distinguish cross
  profiles from nested target roots under the lock; mixed, foreign or unprovable cross-profile roots refuse the pass
  rather than silently dropping their dependency edges. A pre-created target need not carry Cargo's optional backup tag;
  a present but invalid tag still refuses the pass. Every pass still reads the current fingerprint graph. The locked
  scan reuses directory-entry types and reads its small metadata files synchronously, avoiding duplicate stats and one
  asynchronous dispatch per tiny file; it does not walk retained artifact contents or reuse an earlier graph that could
  miss in-place recompiles. `cargo-retention` previews the same selection without changing anything. `incremental/`,
  nested target directories (`cargo-lint`, `cargo-lint-cross`) and the split-debuginfo objects rustc keeps inside
  current generations (rust-lang/rust#161824) are not reclaimed, so the target directory is not bounded. Empty
  fingerprint hashes are kept as unfinished compilations. The preservation proof covers Cargo's current unit and
  dependency records, not every debug reference in copied binaries or other checkouts. In particular, a binary copied up
  before a failed link can outlive the generation its debug map names; this pass does not establish that machine-wide
  debug closure.
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

The default configuration of every cargo target compiles cargo's `dev` profile: every local build and test, CI
validation and `nx build` share one `target/` and one set of units, so cargo recompiles only what changed. The clippy
gate and the cross check keep their own `target/cargo-lint` and `target/cargo-lint-cross` (see Cargo Workspace Layouts).
The `production` configuration adds `--release` to the targets that build shipped artifacts (`cargo-napi`,
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

`cargo-fetch` is uncached because the registry lives outside Nx outputs. `cargo-test-archive` is the one cached cargo
build, and it is the step that compiles the test binaries ahead of the bounded runners; there is no separate uncached
`cargo test --no-run` warm-up, since it compiled the same units the archive does. Cargo's own incremental cache avoids
recompiling unchanged code. Nx never restores or collects shared Cargo build directories; lint and test **results**, and
dedicated Wasm/N-API **outputs**, remain cacheable.

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
in that declaration, or in the named input it uses, or a toolchain bump will not invalidate it. The replaced list also
held the target's `externalDependencies` entry described below; restate it (`[]` for a target that runs only cargo), or
every lockfile change re-keys the target.

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

Policy keeps Rust crate sources out of `production`: `smoo monorepo validate` refuses a positive entry beneath
`{projectRoot}/crates`, ending in `.rs` or naming a `Cargo.toml`, and `smoo monorepo update` removes it. TypeScript
targets see Rust only as the producer target's outputs, through `dependsOn` and `dependentTasksOutputFiles`; a target
that reads crate source as text names it in its own inputs; cargo targets are keyed by `cargoClosure`. Update removes
the entry rather than keeping it, as it rewrites a broad `production` to the default: whatever reads crate source
declares it on its own target.

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

A cache-publication refusal retains the original filesystem cause, logical directory/lock/temporary paths, and parent
device/inode observations immediately before lock construction, at the temporary open, and after failure (or the exact
stat error). This distinguishes a missing namespace from a rebound one without recreating directories, retrying
publication or changing warm-cache behavior.

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
fixture delegates to the command selected before its shim was installed, with that original search path, so nothing it
starts rediscovers the counted shim and recursively enters the broker. The real-producer regression covers both counted
metadata and forwarded workspace discovery, with one metadata child.

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
(`invalid utf-8 sequence of 1 bytes from index N`, exit 1). Nx drops a runtime input's stderr, so a failure that only
wrote to stderr would be invisible. The wrapper retains each failure under the checkout's
`.runtime-input-failures/failure.XXXXXXXXXX/`, outside Nx's state and the temporary directory. These private records
survive `nx reset`, task-cache cleanup and temporary-directory retirement; they remain until the operator removes them.
The reliable diagnostic after a daemon or headless gate's decoder error is:

```sh
cat .runtime-input-failures/*/report
```

`smoo github-ci nx-smart`, `nx-run-many` and `nx-deploy` print every report retained during a failed Nx run
(`runtimeInputFailureReports(root, sinceMs)`, `@smoothbricks/nx-plugin/runtime-input-policy`), so a CI log names the
command and its cause beside Nx's decoder error, which names neither.

Each readable `report` names the command, its indexed arguments, working directory, exit status and original stderr.
`argv` retains the exact NUL-delimited argument vector (including empty arguments), `stderr` retains the original bytes
(including trailing newlines), and `exit-status` retains the decimal status. The wrapper also attempts a stderr summary
naming the command and report path, and displays the report on `/dev/tty` when it can open a controlling terminal.
Neither display is guaranteed through Nx's daemon or a headless harness: rely on the retained report, not on Nx
forwarding stderr. A failed terminal write is retained in the record's `terminal-error`. Records can contain arguments
or stderr with credentials; keep them private and inspect before sharing. Evidence-storage errors name the problem and
the surviving stderr capture on stderr; they never remove the refusal byte or replace the command's status.

Successful commands still emit only their original stdout bytes, discard stderr and retain no failure record: no
evidence path, timestamp or diagnostic enters a successful key. `runtime-input.test.ts` compares that byte stream and
its hash with the previous success path, and plants a failure through real Nx (daemon and daemonless) to prove both
refusal and retained evidence.

A runtime input runs only programs the workspace declares: devenv's Nix profile, the checkout's `node_modules/.bin` and
its own scripts. Nx hashes on a developer's Mac, on a NixOS CI runner and inside cowshed's sandbox, and only those are
the same in all three. A program the host's operating system ships is not: `shasum` is a Perl script that macOS ships in
`/usr/bin`, not coreutils, and no devenv profile provides it, so an input that named it hashed on every Mac and exited
127 on a NixOS runner. Hash a file with Bun, as the plugin's own file inputs do (`Bun.CryptoHasher`); devenv's coreutils
provide `sha256sum` where a pipeline needs one. The only host paths admitted are `/bin/sh` and `/usr/bin/env`, which
NixOS keeps too. `@smoothbricks/nx-plugin/runtime-input-policy` exports `hostPrograms(command, { path, root, home })`:
each program the command runs (through the wrapper, `env`, `exec`, `command`, `sh -c` and command substitutions; a
script file it runs is that script's own concern) that PATH finds outside `/nix`, the checkout and the home, that
nothing on PATH provides, or that only a link in the checkout's `node_modules/.bin` to a build output of one of its own
packages provides. Bun links a workspace package's bin only when its target exists at install time, and a fresh checkout
installs before it builds, so such a name is absent on a fresh runner and present in every checkout that built first;
run the file instead (`node node_modules/@smoothbricks/nx-plugin/dist/bin/smoo-nx-cargo-hash.js`, as the plugin's own
Cargo input does). A consumer's installed package ships its build, so its bins are not refused. Each refusal is a tagged
`HostProgram`, and `hostProgramReason` says it. `runtime-input.test.ts` judges this repository's declared inputs on
`tooling/direnv/repo-path --print`, the PATH entries the devenv shell and CI give Nx (which hands a runtime input its
own environment): the devenv profile and the checkout's directories, with no `/usr/bin` or `/bin`, so a program only the
host provides fails on macOS as it does on the NixOS runner. A consumer judges its own declarations with the same
export.

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

Each Cargo child and each acquisition of the cross-process resolve lock has a five-minute `CARGO_RESOLVE_BOUND_MS`
deadline. A warm content-keyed cache read takes no lock and spawns no child. A cold flight acquires the pinned Nx
binding's blocking kernel lock in one owned child; its parent kills and reaps that child if acquisition exceeds the
deadline. The named `CargoResolveLockError` reports the lock path, elapsed wait, bound and waiter pid (the holder pid is
unknown). A successful flight holds the lease until closing the child's stdin releases the lock and the child is reaped.
A timed-out Cargo command is killed, and resolution refuses instead of hanging graph construction or returning a stale
closure.

Resolution expands shared ancestors once and reads current file bytes directly, without per-file asynchronous dispatch
or canonical-path probes for missing files. Standalone calls retain a fresh native manifest census; graph callers use
the graph's existing index. Canonical paths and contents still key metadata, including ignored and external packages;
source bytes are read on every hash call, not kept in a runtime-result cache.

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

Fileset reduction works from the set of package directories: a path is covered exactly when it or one of its ancestors
is a member. Nested packages therefore need no extra subtree pattern, and shared ancestors are visited once. Sibling
names accumulate into the same brace pattern without copying each growing prefix. The ordered filesets are unchanged; no
new cache or retained index is introduced. For `P` packages and `F` distinct source paths of bounded depth, with `L`
loose files, reduction costs `O(P log P + F + L log L)` rather than enumerating package pairs and rebuilding sibling
prefixes.

`bun packages/nx-plugin/benchmarks/cargo-closure-input.bench.ts` measures the real async inference entry with cached
Cargo resolution/source scans at 1–4,096 packages, including a one-package overhead row. Its expansion oracle checks
complete ordered input equality before timing; filesystem scanning and Cargo resolution are not part of this
measurement. `bun packages/nx-plugin/benchmarks/compact-candidates.bench.ts` measures the unchanged output-directory
reduction and converging dependency-graph traversal separately, without invoking an executor or gate.

Literal `include_str!` and `include_bytes!` files also belong to the closure, including data inside a crate and tracked
files outside its directory (such as a shipped Markdown skill). One Rust lexer excludes comments and example strings;
computed paths and arbitrary build-script reads still need explicit inputs. A compiled-in file is named only when Nx's
file index holds both it and the source including it: an ignored one is a build output, whose bytes no fileset can hash
and which reaches consumers through its producer's dependency outputs. Naming it would only key the project graph on the
names the last producer run left on disk, so a run regenerating content-named files would move every key it was hashed
under. External path-package digests include the same literal data bytes. Inferred `napi-*` and repository-root
`cargo-wasm` builds use this resolved closure rather than every Rust file in the workspace, while retaining their
toolchain pins, wrapper scripts, N-API configuration and CLI version.

Runtime file identities are rooted in Cargo package name/version and the declared input role, not in the consuming
checkout's absolute path or depth. Source paths are package-relative; compiled-in data keeps its declaring source and
literal path as its identity. Identical main/shed inputs therefore share a key, while changed bytes, module names and
symlink targets still move it. File identities retain their globally sorted `cargo-path-inputs-v2` transcript, including
the original insertion order for equal identities. The Cargo resolution memo schema retains package names/versions;
refreshing that memo does not reset the Nx task database.

Runtime include discovery is a bounded fuse, not a source-byte cache. Ordinary scheduled Rust sources are read once:
their buffer supplies both the existing literal scanner and SHA-256. `file` identities precede the same package's
`include` identities; packages sharing an identity are processed together. An earlier package's include can consume a
later package's source, but only its discovered references survive until that later identity's boundary. Retention is
one live file buffer plus the current UTF-8 decode/tokens and path metadata, including across package boundaries.
Literal spellings are copied into path-sized storage with the same UTF-8 encoding, without Unicode normalization, so
substring views cannot pin decoded source files.

Discovery still scans ignored sources, just as the graph scanner does. Discovery-only sources that are later included
(for example through an unignored symlink that resolves to an ignored `.rs`) are read twice: serving those from one read
would require retaining bytes across their discovery and sorted hash positions. This exception preserves the old digest
rather than introducing a workspace-sized cache. No source bytes or runtime discovery memo survive an invocation.
Read-count, timing, corpus-equivalence and retention evidence is recorded in
`benchmarks/cargo-source-read.results.json`; `benchmarks/cargo-source-read.bench.ts` measures real filesystem reads in a
Node bundle (Bun does not refresh named builtin imports when the benchmark installs its read observers).

Members the file index cannot hold, outside the Nx workspace or under `node_modules`, are hashed by one runtime
`smoo-nx-cargo-hash --closure <projectRoot> <Cargo.toml>` entry, present only while such members exist. Its cached
resolution checks external manifests too, so a dependency introduced by an external manifest edit is still covered.

A runtime input that cannot be computed, whether from a manifest or locked dependency it cannot resolve or from
arguments it refuses, prints one fixed line (`cargo-input-unavailable`, `cargo-input-invalid-arguments`) and exits
nonzero, and `runtime-input.sh` turns that into an Nx error. Its stderr names the cause, Cargo's own message included,
with the checkout's absolute path written as a relative one and Cargo's timing-dependent lock-wait lines dropped;
refused arguments write nothing. The Cargo target, run by hand, still refuses the missing dependency or manifest with
Cargo's own cause. Only stdout supplies the cache-key result. Stderr retains the operational cause and may also contain
native-loader warnings; those diagnostics are not required to be byte-identical across checkouts.

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

The fetch is bounded, because it holds Cargo's global package-cache lock (`$CARGO_HOME/.package-cache`) while it runs
and every other cargo that shares the `CARGO_HOME` waits behind it. Each `cargo fetch --locked` runs in a process group
of its own under a 300 s deadline (`CARGO_FETCH_DEADLINE_MS` in `tooling/direnv/cargo-fetch-bound.ts`, the bound the
project graph puts on each of its own Cargo children). At the deadline the whole group is killed and the task fails,
naming the command and the lock it held and printing Cargo's output up to the kill; `SMOO_CARGO_FETCH_DEADLINE_MS` gives
a shorter deadline in whole milliseconds and never a longer one. A signal that ends the step (SIGINT, SIGTERM, SIGHUP)
or the step's exit ends the group with it, since the group no longer shares the terminal's signals. A group signal
reaches only the members that exist when it goes out, and macOS does not hold a fork back while it does, so the kill is
repeated while the kernel still lists members of the group (`judgeReap` in `tooling/direnv/cargo-fetch-bound.ts`): every
5 ms for up to two seconds at the deadline, and every millisecond for up to thirty when the step is ending, which can
wait for nothing. If members are left at the deadline, the failure names the group and says so. When the step is ending,
a line on stderr says the group was still listed and that whether anything in it still runs is unconfirmed, because its
leader is listed until the step has gone. Neither claims the kill reached every process.

The step also gives the fetch `GIT_HTTP_LOW_SPEED_LIMIT=1` and `GIT_HTTP_LOW_SPEED_TIME=60`, over any value the shell
already holds. Git has no low-speed limit unless one is set, and Cargo runs the `git` executable for git dependencies
unless `net.git-fetch-with-cli = false` (its default detects `git`), so a `git fetch` over HTTP(S) whose connection
stalls fails 60 to 67 s after its last byte instead of holding the lock until the deadline. The limit reaches only those
transfers: a git dependency over ssh or the git protocol, and Cargo's own git library
(`net.git-fetch-with-cli = false`), are bounded by the deadline alone. Cargo's registry downloads carry limits of their
own (`http.timeout`, `http.low-speed-limit`).

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

`refuseOverlappingOutputs(graph, workspaceRoot)` from the same module gives each output path one producing target. It
refuses any target whose output is at, under or above another target's output, in any configuration: Nx restores a
directory output as one tree, so a second target writing inside it leaves the first target's tree unlike its cache
artifact after every build, and the two restore over each other without either ever being a cache hit. Each declaration
states its kind: one ending in `/` names a directory, any other a file (`{projectRoot}/dist/`, `{options.out}/`). A
pattern conflicts with a path at or above its static prefix (it writes beneath that path, so a file there collides by
kind), a path it matches, or a directory it can reach inside under Nx's own minimatch, which expands brace alternatives
before splitting segments; a file below its prefix that it does not match is not written by it. Two patterns are not
judged, since only a file both match collides. `OutputOverlapError.overlaps` names both targets and outputs. Both
validators interpolate the declarations themselves, so a declaration Nx refuses to interpolate raises
`OutputDeclarationError` naming its target.

Raw project output declarations, including collect/apply fixtures, retain the directory suffix. Already-resolved Nx
`Task.outputs` paths are normalized paths, not declarations, and need no trailing `/`.

`refuseMisdeclaredOutputKinds(graph, workspaceRoot)` holds those declarations to the built tree: a literal output
declared a file that is a directory on disk, or declared a directory that is a file, raises `OutputKindError`, whose
`findings` name the target, output, declared kind and on-disk kind. Call it where the outputs exist after a build; an
output absent on disk is not judged, and patterns state no kind. The overlap policy reads only the declarations, so its
verdict is the same in every checkout.

## Test Walk Policy

A test pays its setup once and asserts after each step; two cases that test one path are one test. Consuming
repositories enforce this in their root Nx lint with the `smoo-nx-test-walks` bin. The bin judges only pairs with a case
changed since the upstream merge base. Among those pairs it reports each case's nearest refusing partner and its nearest
advisory partner separately, so an advisory pair never hides a refusal. Existing pairs therefore need no allowance
table. The verdict depends on that base and no file hash names it, so the plugin resolves it as a run precondition
before Nx hashes any task (`assertUpstreamBasePrecondition`, from its pre-tasks hook): a checkout without a main ref
refuses the run with the check's own reason rather than carrying the base through a runtime input. `upstreamBase` picks
the upstream in this order: `NX_BASE` (refused when it names no commit), `refs/remotes/main/main`, the local main's
upstream branch, `refs/remotes/origin/main`, then `main`. `findDuplicateScenarios(root, files, changes)`,
`formatDuplicateScenario`, `diffScope` and `upstreamBase` are exported from `@smoothbricks/nx-plugin/test-walk-policy`,
and git failures come back as `GitAnswer` values.

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

## Test Clock Policy

A test's duration is a property of its work, not of the host ("Bounded Test Targets"). Consuming repositories enforce
this in their root Nx lint with the `smoo-nx-test-clocks` bin, which refuses four kinds of site in test code with
`error[TEST-CLOCK-ORACLE]`:

- **runner-deadline:** a deadline handed to the test runner: a number or `{ timeout }` after a `test`/`it`/`test.each`
  body, or `setDefaultTimeout`. Delete it and put nothing in its place.
- **test-deadline:** a Rust test's own deadline: `recv_timeout`, `park_timeout` or `timeout` bounded by a literal
  `Duration` below the no-progress backstop, whose expiry fails the case (`.expect`, `.unwrap`). A quiet window
  (`timeout(d, f).await.is_err()`) is not a deadline.
- **clock-delta:** an assertion that elapsed host time stays under a bound, such as
  `expect(Date.now() - started).toBeLessThan(5_000)` or `assert!(started.elapsed() < d)`. Lower bounds, ordering against
  a product timestamp, epoch stamps, CPU time and injected clocks pass.
- **clocked-bound:** a literal product bound below the backstop in a case whose stand-in a timer or sleep ends
  (`{ command: 'node -e "setTimeout(() => {}, 5000)"', timeoutMs: 50 }`). The bound races the stand-in's clock. A bound
  that is the case's subject, with the event that would beat it held off by construction (a child that never answers),
  passes. So does one that cannot decide: `0` (disabled) or at `BOUNDED_TEST_IDLE_TIMEOUT_MS`.

A Rust fn on paused tokio time (`tokio::time::pause()`, `start_paused = true`) runs on virtual time and is not examined.
Like `smoo-nx-test-walks`, the bin judges only what changed since `upstreamBase`, the same base the lint target's
runtime input already prints. A site in a case that overlaps a change is judged; the whole case is read, so an edit
cannot keep an old site. A site outside any case is judged when its own line changed. The check stops new sites and
removes none, so existing sites need no allowance table. It cannot see: a bound passed positionally or held outside the
case; a stand-in in another file; a bound below the backstop beside untimed real work; a clock read through a helper;
`if (…) throw` and `while` deadline guards; Rust socket-option deadlines; timer waits themselves.
`findClockOracles(root, files, changes)` and `formatClockOracle` are exported from
`@smoothbricks/nx-plugin/test-clock-policy`.

## Nx Target Naming

Target names are `{tool}-{output}` names. Use names like `tsc-js`, `tsdown-js`, and `cargo-wasm`; `build` and `lint` are
aggregates.

Concrete targets come from concrete files:

- `tsc-js` emits transformed JavaScript through a temporary JS-only `ttsc` config, restores execute bits on outputs
  declared by `package.json#bin`, then emits declarations and declaration maps from the original project with native
  `tsc`. Direct project references and every cached output lane are preserved. It writes `.build-inputs.json` into the
  `outDir`: the SHA-256 of every file the config's `include` matches less its `exclude`, taken before the emit, and
  those globs as Git pathspecs. The macOS napi targets write the same record from rustc's dep-info file through
  `smoo-nx-build-inputs --manifest-path <Cargo.toml> --dep-info <path under the target directory> --output <dir>`. The
  record is inside the target's outputs, so a cache hit restores it with them; a live developer link
  (`@smoothbricks/cli`, "Declared Links") checks it to tell a current build from a stale one by content.
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
- A workspace-root `Cargo.toml` provides `cargo-test`, `test`, `cargo-lint`, `mutation`, and `bench`. A project's
  `build` depends on its cargo workspace's `cargo-test-archive` (for a nested package, the workspace root project's), so
  `nx build` leaves the test binaries compiled. `cargo-test-archive` runs one
  `cargo --frozen nextest archive --workspace` into `.cache/nextest/archive.tar.zst` and is the only cached cargo BUILD:
  it produces a file outside the build-volume links rather than a mutable build tree. Nx cache restoration must never
  replace Cargo's `target` link. Its inputs, like every cargo-derived fileset, are anchored at `{workspaceRoot}`: Nx
  resolves a `{projectRoot}` glob against only the files its project owns, and a nested package owns the crates under
  it, so a root workspace's archive spelled that way hashes none of its members and replays stale binaries against
  edited crates. Each member crate gets a cached `cargo-test-<package>` run (30s slow-test warning, no per-test kill)
  whose inputs include that crate and its path dependencies, and whose command runs
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
  output such as `.cache/nextest`. `cargo clean` removes it. A repository's own target that runs tests from the
  workspace archive calls the same helper and passes the same reuse flags: `nextest run --archive-file` unpacks the
  whole archive into a fresh directory under its `TMPDIR`, which for a `bounded-exec` task on macOS is a lease on the
  shared RAM volume (measured: a 548 MB archive unpacks to 1.97 GiB, and four such tasks filled the 8 GiB volume). The
  RAM lease holds what tests write, never the binaries they run. `--workspace-remap` is required — an archive records
  the producing tree's absolute paths, and without it a restored archive hands tests another checkout's
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
- Canonical `napi` package metadata provides a host `cargo-napi` target (only when no configured triple is the host's
  own platform) and named release targets for each configured triple. macOS triples run `napi build` with the host's
  toolchain, because a macOS addon needs Apple's SDK, which zig does not carry; the napi CLI also writes the platform
  `.d.ts`.
- Linux artifacts reference no build-host path. N-API addons link glibc 2.17 symbol versions and carry no RUNPATH;
  binaries a package builds for Linux, such as a CLI, build the same way. Both are built by devenv's zig through
  cargo-zigbuild, with the same command on every host. Each Linux triple's `napi-<arch>-linux` target runs the
  `@smoothbricks/nx-plugin:cargo-zigbuild` executor with `--target <triple>.2.17`, and the executor copies the cdylib to
  `dist/native/linux-<arch>-gnu/<binaryName>.linux-<arch>-gnu.node` itself: the napi CLI cannot carry the glibc suffix,
  since it looks for the artifact under `target/<triple>.2.17/`. A package-local `cli-<arch>-linux` uses the same
  executor with `bin` set; a static CLI targets `<arch>-unknown-linux-musl`. The executor runs `cargo-zigbuild`
  directly, removes any compiler or linker the shell names for its triple (a Linux shell pins its host compiler for
  `x86_64-unknown-linux-gnu`), and reads the artifact path from cargo's JSON messages. zig and cargo-zigbuild come from
  the managed devenv module: a Linux shell carries them, a macOS shell only inside the `linux-cross` profile
  (`tooling/devenv -P linux-cross shell -- …`).
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

A local graph plugin can declare `options.graphInputContract` with `version: 1`, one explicit `kind` (`native-only`,
`tsconfig-extends` or `reference-scan`), `modules: [{ path, sha256 }]` and `packages: [name]`. Module paths are
workspace-relative and the first module is the configured entry. The literal SHA-256 values bind the audited
entry/helper import closure to that input contract; changing code cannot inherit its former proof, and checking a
declaration never rewrites its approval hashes. The installed `@smoothbricks/nx-plugin/graph-input-contract` factory
collects a read witness from the owning reader, not a second parser or a guessed file list. Pure native-only callbacks
need no outside-read trace, but still require their exact audited module and package-resolution closure.

The other kinds publish complete or unproved evidence in the existing Nx workspace-data directory as a byproduct of the
actual callback. A complete witness binds its contract, native file/hash/namespace identity before and after the
callback, read file/directory stamps, missing paths, physical package resolutions, tools and environment. TypeScript
extends and reference scans retain their existing results while recording the outside inputs they really read, including
Git configuration origins and relevant namespace inventories. Installed Nx owns daemon/worker environment normalization;
loader/preload settings are not silently dropped. A preload's own bytes are not its whole input, since it can load more
code and read more files, so only an audited preload is bounded. A loader environment made only of preload clauses,
`--require <file>` or `--require=<file>` in `NODE_OPTIONS` and `--preload <file>` or `--preload=<file>` in
`BUN_OPTIONS`, whose every file's SHA-256 is in `AUDITED_PRELOADS`, is bounded, and each file is a witnessed read like
any other input. That set holds cowshed's port-0 preload alone, which every cowshed job loads. Any other preload, any
other text in those variables, and any `LD_PRELOAD` or `DYLD_INSERT_LIBRARIES` is an unknown loader. Missing or changed
declarations, missing traces, stale read evidence and unknown loaders, dynamic configuration or unbounded reads make the
proof unproved and require the ordinary authoritative graph RPC. A declared kind alone is never admission, and this
witness never replaces current Nx task hashing or artifact comparison.

The installed-package census expands each declared `files` set and relative export targets through that workspace's Nx
native output authority, always retaining the manifest, directory/alias namespace evidence and every resolution edge;
without a file declaration it conservatively retains the whole package tree.

**Every call censuses the installed closure again; only file bytes are reused.** Each census resolves every edge, lists
every directory and stats every node. A file takes its SHA-256 from the census the witness records (or, after the graph
RPC, from the same call's earlier census) only when its stamp (device, inode, mode, size, and mtime and ctime in
nanoseconds) is the one that census read and that census did not read it in the tick it changed in; the witness records
those unproven files beside the identity, which binds what was read and never what was reused. Any write moves a file's
ctime to the tick it happens in, so a file written since, one the recorded census lacks, and one it recorded unproven
are read again. The key is the stamp and not a cheaper installation generation (a resolution, a manifest, an install
marker): a rebuild of a developer-linked package, this plugin's own `dist`, rewrites its files in place and moves none
of those. Measured on a 23,789-node closure (21,069 files): a census that reads every file takes 5–9 s on a loaded host,
its lstat pass alone 1.3–1.8 s.

Ancestor directories bind device/inode/type/mode path identity; files and symlinks retain their full stamps, and a
directory is upgraded to a full nanosecond node/namespace stamp when its membership is actually read.

**A stamp read in the tick its node changed in is unproven, not unchanged.** A filesystem dates a change by the kernel's
clock. On Linux, on a filesystem without fine-grained timestamps such as ZFS, that clock moves once a jiffy: 1 ms at
`CONFIG_HZ=1000`, as on CI's runner, and 10 ms at `CONFIG_HZ=100`, the slowest. A same-size rewrite in the tick a stamp
was read in leaves every field of that stamp as it was. So a census reads the clock before its first stat, and records a
node whose later of ctime and mtime is within 10 ms of that time with its ctime `unproven`, which no later read equals.
This is git's racily clean rule (`Documentation/technical/racy-git.txt`), and the Nx archive repair below applies it to
Nx's own file hashes. Node can read neither the jiffy nor a filesystem's granularity, and a shorter window reopens the
gap on a slower kernel, so the window is the slowest tick and is not an option. A reader that meets an unproven stamp
re-reads the node against its recorded bytes, link text or membership, as it does a node whose ctime alone moved; one
that holds nothing to re-read it against counts it moved. The local plugin modules, which hash every byte on every read,
record their stamps as read. So does a workspace capture's own stability check, which compares the stamps it read: only
the capture after it distrusts them. The installed-package census records its stamps as read too, since its identity
binds what it read, and names beside them the files it read in their change tick, which the census that takes its hashes
reads again.

A physically missing required factory after the clean cutover is an invalid installation and fails loudly; it is not a
proof miss that can fall back to the graph RPC. Consumers adopt the normal static import only after the public producer
is available.

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
for all those nodes, and a node the comparison read in the tick it changed in is recorded unproven, so the next
comparison is a full one. It is evidence of the same previously compared bytes, never source identity or an mtime
freshness rule. Missing/unreadable evidence or moved nodes require a full comparison; mutations to either the cache or
working tree, parent symlink replacement, and directory membership changes cannot borrow an old equality verdict.

`probeBuilt({ target, cwd })` is the distinct typed read-only product operation. Its `hit` is an actual published
`EnsureBuiltReceipt`, not a cache hit re-probed under the consumer's environment. The producer opts into publication in
its own Nx plugin options as
`producerReceipts: { targets: ["lmao:rust-dist", "lmao:cargo-wasm", "lmao:cargo-thread-ffi"] }`, naming every target a
consumer's snapshot declaration names. The pre-task hook captures the committed HEAD/tree and tracked-source/tool basis;
the post-task hook uses Nx's actual successful `TaskResults`, hashes/hash details, argv and start/end times, including
successful uncacheable prerequisites, and atomically publishes the receipt in that checkout's
`.cache/producer-receipts`. That is its source tree, not Nx's workspace-data: under cowshed workspace-data is build
state on the checkout's build volume, which a consumer's read grant of the provider checkout does not reach, so the
consumer could not read the receipt it validates. The directory ignores itself with a `.gitignore` of `*`, so no
checkout tracks a record and Nx never hashes one. A missing receipt refuses naming the path it was looked for at; a
receipt the consumer cannot read refuses with the read error (EPERM under a sandbox). Disabled publication performs no
source prepass, so an ordinary consumer CLI pays none of that producer work. No file permission, path, daemon state or
cached row can invent a completion receipt. Nx takes one pre/post task-hook pair per plugin, so the plugin's pair runs
the controller's `COWSHED_NX_TASK_RECEIPT_ID` task-results hint first and these producer hooks second, each with the
same options and run context and neither reading the other's output.

The receipt binds the selected target, basis commit SHA and committed tree SHA, actual argv, declared producer
environment identity and bound tool/module inputs, independent product output SHA-256, measured wall time, and an
explicit clean tracked-tree assertion. It includes every completed required task's Nx identity and every Nx-expanded
public output. Files carry SHA-256; symlinks carry text and mode without following them; directories carry mode, ordered
membership and child-census digest, including empty directories. The producer source/tool/output witnesses must stay
stable through publication, and every refusal that one moved names the first nodes that did. A tool or installed-module
node whose device, inode, mode, size and mtime are unchanged and only its ctime moved is re-read and compared with its
recorded bytes, link text and membership, as any moved stamp requires a full comparison: under `globalStore` the closure
lives in Bun's install cache, which every checkout on the machine shares, and linking a package's bin on any of their
installs re-chmods the store file to the mode it already has. A source, tool or output node the census read in the tick
it changed in, as a task's last writes are, is recorded unproven and re-read the same way. The command's stdout/stderr
digest is not a product digest. The bound tool/module inputs are the installed closures of Nx, this plugin, TypeScript
and local graph plugins, and the producer executables. A task's npm external inputs (`AllExternalDependencies` and the
npm nodes of its project's dependency closure) are Nx's lockfile identities: the task's hash details and the committed
`bun.lock` bind them, and no installed closure is resolved for them, since under Bun's isolated linker most of them do
not resolve from the task's project.

The pre-task hook stats every node of that installed closure on every run, and that is what lets it skip reading bytes.
The census is a function of the nodes' stamps (device, inode, mode, size, and mtime and ctime in nanoseconds), not of
the moment it runs: the hook records the census it took in Nx's workspace-data (`producer-installation-census.json`, a
hash source, never a proof), and the next run takes the hash of every file whose stamp has not moved and that the record
did not read in the tick it changed in. An unchanged closure reads no file bytes; a developer-linked package whose
`dist` a rebuild rewrites in place, with no lockfile or `node_modules` change, moves those files' ctimes and they are
read again. No cheaper generation (a lockfile, a manifest, an install marker) keys it, since such a rebuild moves none
of those. The tool's nodes take the hash the census read instead of reading each file a second time: a write landing
between two reads could make them disagree. Nor does the census take a stat pass of its own to prove itself: the tool's
nodes stat each of its nodes again, a file whose stamp moved since the census read it is read again, and the capture's
stability check refuses a tool node that moves before publication. For a consumer that links this plugin from a checkout
(about 28,000 closure nodes) the hook took 9-17 s of synchronous I/O on the plugin worker before, with every file read
twice, and 6-9 s after, almost all of it stats; on a host whose disk-image layer is saturated the reads ran for minutes,
while the worker's event loop could neither answer Nx nor reap its exited children. The hook's phases are performance
measures (`producerReceipts:graph`, `producerReceipts:source`, `producerReceipts:tools`, `producerReceipts:census`),
which Nx reports under `NX_PERF_LOGGING=true`.

Validity compares the committed tree, not the current HEAD spelling. An empty commit with an identical tree keeps the
receipt valid and its original basis SHA unchanged; the consumer never rewrites that provenance. A genuinely changed
tree invalidates the receipt. Multi-target consumers require one tree and retain each producer's actual basis.

The consumer validates this stored receipt against its declared target, committed-tree/configuration identity, declared
environment/tool inputs and actual output census. It never re-hashes the provider's task graph in its own environment.
Its read of the recorded tools must stay stable through that census on the same terms as the producer's: a tool node
whose ctime alone moved is re-read and compared with the receipt, and a refusal names the nodes that moved. A valid
receipt is the hit. A missing, mismatched, stale or unbound receipt requires the authoritative graph RPC, then a loud
miss/refusal without running a target or writing provider outputs. That RPC goes only to a daemon already serving the
provider: the probe never starts one, since a start writes the provider's workspace data and a sandboxed consumer may
only read its provider, so with none serving the refusal names `no-daemon`. A managed consumer requires the installed
`probeBuilt` operation and the complete producer-receipt contract; an old receipt lacking it refuses, never falls back
to `ensureBuilt` or a producer writer. `ensureBuilt` remains the normal build/stream operation with its existing
hit/built/failed results and Nx's current source/cache/artifact verdict.

**A refusal never replaces a receipt.** A run whose products the hooks cannot prove publishes a refusal instead: its
detail, in its own `producer-refusal-<sha>.json` beside `producer-receipt-<sha>.json`. A refusal says only that this run
could not prove something; it is no evidence about the products an earlier run proved, and overwriting that run's
receipt would destroy the evidence a consumer can still check. So a receipt stays until a later receipt replaces it, and
the consumer validates it on its own evidence as above: one that still validates is the hit, whatever a later run
refused. A miss names the latest refusal in its detail, and a published receipt removes the refusal it answers. The
hooks read git under the repository's own configuration (`GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_NOSYSTEM=1`): Nx
runs them in the workspace's Nx daemon, which runs inside the cowshed sandbox under a host client's `HOME`, and the
sandbox denies the operator's global git configuration (specs/cowshed/04_sandbox.md, the runtime-input contract).

A FILE-only source plan does not perform a redundant installation census for absent runtime recipes; the complete
producer tool/module and output SHA evidence is still captured and independently revalidated on every product probe.

Git source provenance is the committed tree and the producer's clean tracked source basis, not an unrelated untracked
user file in its working directory. An actual FILE input outside HEAD, a modified tracked input, changed membership,
opaque custom/native input proof or genuinely unbounded runtime source cannot be signed. Bounded runtime/env identity is
explicitly labelled `nx-task-hash-only`; the existing generator-owned version recipes still require their declared
cwd/toolchain FILE pins and proved installed closure. Public outputs are excluded precisely from source stability, never
with broad exclusions that can hide manifests or configuration. A caller probing multiple targets requires the same
provider committed-tree identity, retaining each original producer basis, before publishing its sealed snapshot. A
caller from another workspace never lends its socket, database or cache overrides to the provider. Inside a managed
shell (`DEVENV_RUNTIME`), `ensureBuilt` asks the packaged `nx-socket-dir.sh` owner for the provider's binding. A cowshed
workspace root gets its verified runtime link; the owner refuses one its `.cowshed/env` names no link for, or whose link
leads elsewhere. Any other root gets its canonical checkout spelling under that shell runtime. Outside a managed shell,
Nx retains its normal defaults. A socket-owner refusal returns `disposition: 'failed'` with
`reason.kind: 'workspace-socket'`, the selected `taskId`, `workspaceRoot` and operational `detail`, before any task or
daemon client starts. The owner's actual exit or signal is preserved; the wrapper must not report that the target ran.

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
payloads receive additional bounded store grants. Grants are derived from the fixture's installed package namespace, not
the repository's different private patched namespace. Installation runs once per suite template, which fixtures clone.
Installer-restoration witnesses clone that real installed product into `packages/nx` of a distinct private developer
checkout, outside every consumer `node_modules`. Its root declares `workspaces: ["packages/nx"]` and the dependency
`nx: "workspace:*"`, following the ordinary in-repo workspace binding pattern. Pinned offline Bun produces that root's
lock and production namespace; no hand-made self alias substitutes for the install. The unchanged Nx CLI's global
resolution must resolve `nx/bin/nx.js` from that producer-owned checkout, not from `/private/tmp`. Ordinary pinned Bun
first generates this package's own lock offline with the existing `--lockfile-only` producer, then installs its
manifest-owned production dependencies offline into its own `node_modules`, without changing its manifest or maintained
program bytes. The fixture records the actual install, lock and product digests. The process launcher keeps `PWD` equal
to its actual `current_dir`, as the supervisor launcher does; the fixture observes Bun's real root before installation
and requires the lock at that root. Production mode implies a frozen lock and does not write a missing one, so a
production-only run is not lock-production evidence. Private lexical dependency aliases therefore stay inside the owned
developer checkout; reached namespaces and canonical shared targets use the same single closure traversal, bounded by
the exact caller-owned developer checkout and the actual shared store. Only reached package and namespace paths become
grants, never the whole checkout or `.bun` root. Traversal deduplicates exact package identities, not read-grant
prefixes: a covered local `@nx` scope can still contain a binding to the pinned native platform package in the shared
store. A normal Bun store entry is never presented as a developer binding. Local file providers are checkout-relative
directories that travel with the clone. Foreign-daemon retirement witnesses retain the daemon's own standalone root,
state and short socket throughout the landing; its actual produced record is only a nomination, and its owner performs
cleanup after the identity proof.

The Nx patch repairs upstream Nx runtime behavior, separately from this plugin's workspace-owner checks:

- **Task history uses the client's native database connection.** Task details and history must share that connection; a
  daemon can have frozen a different database namespace before a later client supplies workspace-data overrides. The
  patch removes the obsolete history RPCs and retains native errors, including post-run failures. See
  [upstream Nx #37268](https://github.com/nrwl/nx/pull/37268).
- **A raw-input query resolves only its requested task's plan.** Nx still builds the full dependency graph to plan
  dependent outputs, but does not walk every producer's own inputs and then discard those results. The plural inspector
  keeps its full-closure default. The installed-product regression compares all root input buckets and configurations
  against closure inspection and rejects the old all-plan selection. See
  [root-only raw-input inspection](../../patches/README.md#root-only-raw-input-inspection).
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
- **A target's output has one owner across clients of the checkout.** A per-project/target lease spans cache lookup and
  restore, execution, output publication, output-hash recording and cache capture. A producer replacing and retiring a
  directory cannot invalidate a concurrent cache walk, and same-hash clients cannot remove one another's artifact while
  publishing it. Waiting clients recheck the cache under ownership; independent targets retain concurrency. See
  [`patches/README.md` §Cross-client task output ownership](../../patches/README.md#cross-client-task-output-ownership)
  for the kernel lock, worker lifecycle and real-client regression.
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
  compile dependency emits the Typia-transformed product before Bun runs it. The proof gives the daemon a resident
  plugin worker that asks it for files, holds a real silent client beyond five minutes, disconnects, and records the
  kernel and daemon-log evidence of the next five-minute departure, workers included. It stays outside the bounded Bun
  shards; their fast regression checks the actual installed policy export.
- **A plugin worker is its daemon's client only while it waits on a reply.** A plugin's `globWithWorkspaceContext`,
  `hashWithWorkspaceContext` and the other workspace-context reads run through the worker's own daemon client, which
  kept its connection open after the reply. The daemon keeps a worker with graph hooks for as long as it lives (above),
  and that open connection retained the daemon, so neither ever reached the idle stop: on 2026-10-17 the 72 shed daemon
  logs held 573 stops and not one `Server stopped because: "300000ms of inactivity"`, and every daemon a host client
  started stayed behind as a `ppid 1` process until something signalled it. In a plugin worker the client now drops its
  connection once its request queue drains, and the daemon is unchanged. The client also ignores the close of a
  connection it already dropped, which stock Nx let reset whichever request was current. A bounded regression runs a
  real daemon whose resident worker globs through it and requires zero open connections after the only client exits.
  Stock Nx keeps a worker until its last registered phase, so a plugin with task hooks keeps the same connection open
  there after a graph-only request; this hunk does not retire with the worker-retention hunk.
- **The daemon is its own client only while it waits on a reply.** Code the daemon runs, such as a sync generator, that
  reaches Nx through another copy of the nx package finds no graph in that copy's project graph module, so it asks the
  daemon over the daemon's own socket, and that client kept the connection after the reply. Measured 2026-10-22: two
  shed daemons that ran `@smoothbricks/nx-plugin:managed-files` from a plugin linked outside the shed each held one
  socket pair to itself, logged one open connection with no client left, reset their idle timer every five minutes for
  half an hour until they were signalled by hand, and kept seven plugin workers each. The rule that releases a worker's
  connection once its request queue drains now covers the daemon process too. A bounded regression runs a real daemon
  whose global sync generator globs through the daemon's own client and requires zero open connections once the
  generator finishes.
- **A daemon whose workspace has gone stops instead of reading it.** Nx 23.2.1's native file worker reads an empty file
  map as one it has not filled yet and waits on its condition variable for files (`FilesWorker::get_files`,
  `while guard.len() == 0`). Once every file of a workspace is deleted under a running daemon, as when a checkout is
  removed or unmounted before its daemon is stopped, the next glob blocks the daemon's only thread for good. It sits at
  0% CPU in `WorkspaceContext::multi_glob`, no timer or request runs again, and SIGTERM does nothing because its handler
  is JavaScript; only SIGKILL ends it. The daemon now stops itself when the workspace's `nx.json` is gone. It checks on
  every watcher batch before applying it, on every request and on the 5 s safety poll, so the batch that would empty the
  file map is never applied. A workspace without `nx.json` is no Nx workspace, and its client already runs without a
  daemon. A bounded regression deletes every tracked file under a real daemon and requires it to exit with that reason;
  against the unpatched daemon it hangs in that frame and survives its fixture's SIGTERM.
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

A run that ends at its bound ends with its whole process group, not its leader: SIGTERM, then SIGKILL after the grace. A
group signal reaches only the members that exist when it goes out, and macOS does not hold a fork back while it does, so
each force-kill is repeated while the kernel still lists members of the group. One module, `src/process-group.ts`,
decides that (`judgeReap`) and does it for this executor, the RAM temp volume's deadline, the hang-diagnostics probes
and the Cargo children of `cargo-source-hash`: every 5 ms for up to two seconds at a bound, and every millisecond for up
to thirty where a process is ending and can wait for nothing. What a kill that waits on the event loop cannot end is
named with its process group on stderr, or in the capture or error that reports the bound, and never called killed. A
kill that blocks (the Cargo children's, the probe stop's) cannot reap the group's leader, which the kernel lists as a
member until it is reaped, so what it leaves is told as listed and unconfirmed, not as running. A signal or probe that
fails is told too, and never thrown past the cleanup that follows it. A process that left the group is beyond a group
kill, and one that exited and has not been reaped yet is listed like a live one. The managed script
`tooling/direnv/cargo-fetch-run.ts` makes the same decision with its own copy, since it runs before anything is
installed.

A test target is bounded by progress alone: `idleTimeoutMs`, 180 s (`BOUNDED_TEST_IDLE_TIMEOUT_MS`, the hang backstop of
`specs/cowshed/08_testing.md`), with no `timeoutMs`. The bound re-arms whenever the runner reports a finished test: a
nextest status line carrying its run counter (`PASS [   0.438s] (1/4) …`), a libtest `test … ... ok` line, or a bun
result line or dot. Output that finishes no test is not progress: nextest's `SLOW` warning, repeated every slow-timeout
period for a test still running, and a hung test's heartbeat both keep printing. A suite that keeps finishing tests
therefore runs as long as it needs, however slow the host, and a test that never returns ends the run 180 s after the
last test that finished. bun prints nothing per passing test when it believes an agent runs it, so the executor asks a
lone `bun test` for `--dots`, one dot per finished test. Without a recognised execution lifecycle, the clock starts at
command spawn; a command that reports no tests makes progress only by exiting. nextest's slow-timeout and the serial
groups' code-time classes only warn; no per-test bound kills a test or fails one that passed. Bun has a per-test
deadline of its own, 5000 ms unless the command sets one, and it reads no bunfig for it, so every bun leg deletes it
with `bun test --timeout=0` (`BUN_TEST_TIMEOUT_FLAG`; `0` means no deadline, measured on the pinned Bun 1.4.2). A test
waits for the event it is waiting for, however long a loaded host makes the work take; one that never returns still ends
the run through `idleTimeoutMs`. `timeoutMs`, an absolute ceiling, is for commands that report no tests; a target names
at least one of the two.

On POSIX hosts, direct Go and Cargo test commands (including chains joined by `&&`, `||`, or `;`) share one private
runner lifecycle: `build`, `active`, and `terminal`. A per-command Node supervisor reports phases over an acknowledged
descriptor that its actual tool child and nested fixtures do not inherit. Shell/supervisor startup is bounded; the tool
starts only after its build phase is accepted, and native lifecycle-bearing stdout is forwarded only after its phase
owns the clock. A malformed or prematurely closed lifecycle channel fails the run rather than silently disabling the
watchdog.

| Runner                                                          | Authoritative end of build                                                 | End of active execution                                                                                                                |
| --------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Go (`go test -json`)                                            | Native package `Action:start`, after that package's build dependencies     | Native package `pass`, `fail`, or `skip`; the clock runs while any package executes                                                    |
| Cargo (`cargo test --message-format=json-render-diagnostics`)   | Native `build-finished`, after `compile_tests` and before `run_unit_tests` | The owned Cargo child's close **and drained output**, never a libtest summary; custom harnesses, doctests, and teardown remain bounded |
| Bun                                                             | No separate authoritative build event; active from command spawn           | Command exit                                                                                                                           |
| Unknown shell wrappers, mixed unsupported commands, and nextest | No inferred build exemption; active from command spawn                     | Command exit                                                                                                                           |

The unchanged 180 s interval re-arms on finished tests/packages. Initial Go linking, inter-package Go compilation, and
initial or inter-command Cargo compilation spend no test idle budget. **Compiler-process activity is deliberately not a
signal:** Go linking and Rust compilation before execution are build work; Bun's cold-compiler fixture (and a Rust test
compiling a fixture) is active-test work and must still expire. Build output and heartbeats never reset the bound. Cargo
uses its native build declaration without replacing configured target runners; a nested `test result` or
`build-finished` line cannot retire the owned process or exempt its teardown. Absolute `timeoutMs`, when supplied, is
unchanged.

Separately building test binaries and then executing them under the unchanged bound is a **workaround** for callers
without a native lifecycle hook, not evidence that their combined cold-build command is safe. Direct Go/Cargo commands
use the lifecycle channel instead; unknown wrappers retain conservative spawn accounting.

Process-group timeout fixtures retain cleanup authority only until the command leader exits and every descendant closes
its inherited lifetime FIFO. EOF is the terminal ownership event; a later `afterEach` must not signal the retired PGID
again. Darwin can answer `EPERM` for an unreaped zombie-only group, and an ambient `ps` scan is not an ownership proof.
Failed fixtures retain their cleanup obligation rather than suppressing signal errors.

A job waiting for a CPU token is queued, not idle. A descendant whose own work waits in the host's CPU queue (a test's
scratch cowshed project, say) writes `{"op":"host-wait"}` on the `BOUNDED_EXEC_CPU_POOL` it inherited and holds the
connection while the work waits; while any such wait is held, the idle clock is stopped and keeps the time it had left,
and it runs on from there when the last wait closes. stderr says `bounded-exec: idle clock stopped` and `… restarted`,
and an expiry names the queued time it did not count (`specs/cowshed/05_gateway.md`, "A queued descendant is not idle").
`timeoutMs` is never stopped.

The final-ten-second pre-signal snapshot attributes the runner's process tree, its leader and leaf call stacks and
observed resources, with explicit incomplete/raced results and no delay to termination. Its timing, visibility and
evidence contract is defined in `specs/cowshed/08_testing.md`, "Integration tests (real substrate)".

The snapshot-reset regression orders capture, test completion, and expiry through IPC, not heartbeat cadence. It
deliberately holds the first capture's progress publication while another heartbeat arrives; only the observed PASS
advances the next capture interval. Its virtual idle bound is twice the capture window.

A leg that runs `bun` runs the Bun its workspace declares. Before it asks for a RAM lease or CPU tokens, the executor
reads `packageManager` in the workspace root's `package.json` and, when that names `bun@<version>`, finds the first
`bun` on the PATH the command runs with (the target's `env.PATH`, else the executor's own) and asks it `--version`, in
the command's own environment, which is what a wrapper that routes `bun` reads. A command with no `bun` word in it
(`bun test`, `cd x && bun run y`, `sh -c 'bun test'`), a workspace that declares no Bun and a PATH that holds none are
not checked: the first runs none, the second declares none and the third leaves the shell to say so. A path-qualified
`../runtime/bun test` names its binary itself, so PATH decides nothing for it. When the printed version is not the
declared one the run is refused with a `failed` verdict, like every refusal before its command starts, and stderr names
both versions, the binary and the setting each came from: `packageManager "bun@1.4.3"` in the manifest against
`PATH from the target's env`, whose first `bun` prints 1.4.2. A `packageManager` that names `bun` and no exact version,
or a `bun` that gives no answer to `--version` within 120 s, is refused the same way: a guard that skipped what it
cannot read would be silent exactly where the declaration or the binary is wrong. The refusal replaces a failure far
from its cause. A Bun other than the declared one passes startup, and on Bun 1.4.2 `console.log` drops what a full pipe
refuses, so a child's JSON document reaches the parent that reads it cut mid-string while the child exits 0: the leg
fails three layers from the cause as `JSON Parse error: Unterminated string`. The check is not made on Windows.

On a macOS host, a `bounded-exec` task whose processes a cowshed supervisor owns attempts its own `TMPDIR` lease on a
RAM-backed APFS volume per user. RAM is acceleration, not a guarantee that temporary bytes never reach ordinary storage.
The volume is created lazily by the first task that needs it and detached when the last live lease ends, since its pages
return only on detach. It is a legacy DiskImages RAM disk, so it spends none of the AppleDiskImages2 attach budget
cowshed counts (`specs/cowshed/01a_apfs.md`, "How the APFS host degrades"). The task never attaches the disk itself: the
cowshed gateway does (`ram-attach`, `specs/cowshed/05_gateway.md`, "RAM disks"). The disk lives in a `diskimages-helper`
process, and a helper the task's own job forked would be retired when that job ends, taking the volume from under every
other job's lease, whose open files then read EIO. The helper belongs to the gateway; no job's retirement reaches it. A
target's own `env.TMPDIR` wins over the lease. Inside a cowshed sandbox, which cannot write the lock in `/private/tmp`,
the task keeps its inherited `TMPDIR` and says so once.

Every owned `bounded-exec` target attempts the lease, test targets included. Explicit RAM unavailability uses a unique
owned ordinary directory beside the RAM lock that answer was taken under, in the system temp directory `/private/tmp`.
It lies outside every checkout, so no Git or Cargo walk from the command's temp files finds the workspace it builds; the
workspace's runtime alias is not, since it resolves into `<mount>/.cowshed/run` inside the checkout. It never reuses
inherited RAM as its fallback. The report names the actual medium/path and typed reason once; an explicit target
`env.TMPDIR` wins, is labelled target-provided, and is never cleaned by the executor. Successful ordinary scratch is
removed only after subtree retirement; failed or unretired scratch remains at its reported path. Reclamation never
deletes the scratch of a run that did not pass: a lease whose task died, or whose own run failed, timed out or was
signalled, is renamed whole into `/private/var/folders/cowshed/<uid>/ram3/.archive/<archived at, epoch ms>-<lease>`, on
the same volume, so the move is one `rename(2)` and copies nothing (the build-volume trash does the same,
`specs/cowshed/16_build_volumes.md`, "Garbage collection"). The task that reclaims it says where on stderr
(`RAM temp volume: dead lease <lease> archived at <archive>: kept until …`). A passing run's lease is deleted. The
volume keeps the newest two archives, the failing run under triage and the one before it, which a triage compares to
tell a flake from a regression; that bound is a stated reason, not a measurement. It keeps none when less than half the
volume is free as an archive arrives: half of 8 GiB is 2.5 times the 1.61 GiB a full parallel gate peaked at, and live
leases need the space more than evidence does. Every other archive is renamed into
`.archive/.deleting/<deleter pid>-<archive>` at once and deleted by that task in the background, one at a time, with
nothing waiting on it; a dead deleter's entries are claimed and deleted by the next retention, and a failed deletion is
said on stderr and retried then. What is retained is therefore bounded by the volume's life: the volume detaches with
its last live lease, and its archives go with it, which that release says. A test whose value depends on its raw traces
after the run must name a store-backed `env.TMPDIR`.

A failed task whose output said ENOSPC (`ENOSPC`, or `strerror`'s `No space left on device`) names it, the writer's task
and lease, and the volume's free space read when that line arrived, and records the `no-space` verdict: the writer
usually removes what it half-wrote before it exits, so the volume no longer looks full by then. A failed task that
printed no such line but left the volume nearly full names the volume.

A lease names the attach of the volume it was taken on, and its space is read only while the mountpoint still holds that
attach and the lease is still on it. Once the attach is gone the mountpoint is a bare directory on the host's Data
volume, where a write through `TMPDIR` can make the lease path again, and that free space is not the lease's: the ENOSPC
line then says the lease was lost and why, or that the volume could not be read. Its release says it the same way
(`RAM temp volume: lease <lease> was lost while its task held it: …`) and is never taken as a release of a lease that
was there.

What a command leaves running is ended by its owner, never chosen by where it works (`specs/cowshed/11_shell.md`,
"Process identity and signal ownership"; `specs/cowshed/07_api.md`, `JobSubtreeHandle`). When the job's supervisor
serves `COWSHED_SUPERVISOR_SOCKET`, the command runs as `cowshed subtree-root --handle-fd 4 -- /bin/sh -c <command>`:
the wrapper asks the supervisor for a handle on itself while it is alive, writes the answer to descriptor 4, and execs
the shell in the same process, so the command's pid, process group and life are unchanged; descriptor 3, the runner
lifecycle's socket, reaches the shell as it would without the wrapper. When the command ends, however it ended, the
executor pipes the handle to `cowshed retire-descendants --grace-ms 5000`: the supervisor sends TERM to every live
member its observed tree records under that root (a fixture's Nx daemon in a session of its own and a ttsc build writing
through `TMPDIR` included), KILL after the grace, and joins their exits. Each is named on stderr
(`cowshed: the supervisor stopped <pid> (<image>), which the command left running`), as are the supervisor's coverage
gaps (`… has a gap: <kind> (pid <pid>)`) and a receipt that may have ended processes it could not name: the line is the
leak check, and a suite that prints it has a fixture to fix. A process that only works in the command's directory or
lease, and that the command did not start, is in no such tree and is never signalled. A typed refusal of the admission
(`foldUnavailable`, `notMember`, `foreignHandle`, `jobEnded`, `spentHandle`) still runs the command, unowned, and says
nothing will retire what it leaves; a client that cannot reach the supervisor at retirement fails the run, naming why,
and its lease is archived. Only then does the lease go. Without a supervisor socket nothing could end what the command
leaves in a lease before it is reclaimed, so no lease is taken and the run says so
(`RAM temp volume not leased: no cowshed supervisor owns this command's processes`). The host census of working
directories and `TMPDIR` survives only as a diagnostic: when a lease cannot be released, it names what still works there
and signals nothing.

A task records its handle beside its lease (`<lease>.subtree`, on the volume, outside the command's reach) once the
supervisor admits the command. A lease whose task died is reclaimed by the next task: it hands the recorded handle back
to its own supervisor first and archives the lease once that retirement is joined. An ended job's supervisor retired the
whole job as it ended (`jobEnded`), and a spent handle was already retired: either way the lease is reclaimed. A handle
of a job that is still another task's (`foreignHandle`, `notMember`, `foldUnavailable`) keeps the lease, named on every
new task's stderr, until that job ends. A lease with no readable handle recorded none: its task died before its command
was admitted, or the admission was refused. An image still attached from a file below a dead lease, or a mount below it,
belongs to whoever attached it, so the lease and the volume are kept and every new task names them.

Outside macOS, the executor never provisions an APFS RAM disk: it keeps the inherited `TMPDIR`, or uses the target's
`env.TMPDIR` override, without reclaiming either directory; a supervisor that serves the socket still owns and retires
the command's subtree. On Linux the supervisor admits a subtree only with an observed fold, and refuses with
`foldUnavailable` otherwise. In CI the same rule decides: a job that runs `nx` outside a cowshed job (`ubuntu-latest`
for a fork's pull request, a GitHub macOS runner) has no `COWSHED_SUPERVISOR_SOCKET`, so its commands run directly,
nothing retires what they leave beyond the runner's own job teardown, and on macOS no RAM lease is taken, which every
run says. The self-hosted NixOS runner gets ownership exactly when its `nx` runs inside a cowshed job whose supervisor
serves the socket. The host temp-directory proof runs the real default executor for that contract on Linux; on macOS it
exercises RAM-volume recovery, shared leases and final detach. No test leases, creates or detaches the user's volume:
every macOS host proof runs the executor on a fixture volume of its own (`runBoundedExecWith` with `fixtureRamVolume`),
whose mountpoint, lock and size no other process uses. Its real fixture gateway attaches the disk; every facade and
non-facade disk command uses that gateway's own socket. Teardown detaches only exact registry identities its attaches
recorded, never disks selected by UID or size alone. `fixtureRamVolume` refuses the user's own paths, size and gateway.
The host target depends on `cowshed:ram-fixture`, which builds the native helper before any test; no test compiles or
installs a gateway.

RAM is optional acceleration for the production executor, not a correctness prerequisite. Gateway absence, an
unsupported RAM operation, or unproven legacy ownership yields a typed unavailable reason and one report of the actual
owned ordinary temporary medium used instead. It never falls back to a job-owned attach or inherits a potentially unsafe
shared RAM directory. Required fixture RAM remains required. I/O errors, malformed new markers, unknown ledgers and
unresolved mutation custody fail closed.

The volume marker is version 1 JSON containing `volumeName`, `owner: "gateway"` and the exact `registryEntry` captured
immediately after this gateway attach. Before leasing, a fresh registry/mount snapshot proves that entry still names
this mounted volume; the root and marker identities are revalidated under the ledger lock. A changed identity refuses
instead of silently following a replacement. A legacy name-only marker or an unmarked disk with no exact creation
receipt is unproven: it is not leased, rewritten, reclaimed or detached. Fixture debris reclamation consumes only the
exact identities its own real attaches recorded, never UID or size alone.

The volume's kernel `O_EXLOCK` admits ledger mutations and releases on process death. It is held only for cheap lease
and operation claims and their publication: host-wide process and disk-inventory probes, directory cleanup, provisioning
and detach run outside it. A caller re-takes the lock and revalidates the volume, lease identity and its owned claim
before publishing a mutation; another caller can take a lease while a dead lease is being inspected. An acquire that
fails after taking its lease directory gives it back, and detaches the volume if that was the last lease, as the last
release would have.

Checkouts of this plugin from before the ledger lease under the kernel lock alone and never read the claims, and
checkouts from before the move below the cowshed root mount the volume at `/Volumes`, so no two protocols may share a
volume. The protocol's revision (3) names the volume, its mountpoint, its lock and its ledger, and adds as many sectors
to the RAM disk: inventory distinguishes revisions by uid and size, while exact creation identity alone authorizes
reclamation, so no revision detaches another's disk, mounted or still attaching. On a host running several, each
revision has its own volume; a revision-2 volume, still under `/Volumes`, is detached by revision 2's own last release,
never by revision 3.

An ordinary operation claim is void once its owner's pid is gone. A pending protected detach differs: caller timeout or
death does not finish the child or release its authority over the volume. Its custody is published before the spawn, as
`waiting` while its caller still awaits the facade: other callers queue behind it as behind any claim. Once that caller
stops waiting, or its run throws without an answer, the custody is `reported`: the existing ledger retains that exact
operation token and disk identity, and refuses every other caller by name, until the gateway proves its native child
reaped, proves nothing started, or a fresh registry read proves the exact entry gone. A broken requester connection or
an unknown native outcome proves none of these and retains authority. A dead caller's `waiting` custody is reconciled
the same way; its liveness only chooses queueing over reporting. A PID is diagnostic, never the proof that a pending
detach ended. While a pending detach exists, ledger version 2 makes older readers refuse rather than discard the claim;
normal version 1 returns only after the pending authority clears, preserving every lease entry. Unknown versions refuse
by name without rewriting them. Every 15 s a caller waiting on a claim names its kind, owner pid, token, ledger and wait
so far; another volume's ledger remains independent.

Each reclaim pass reads at most one I/O Registry and mount-table snapshot, not one per lease. A dead lease held only by
mounted filesystems keeps a positive verdict with stable mount and lease identities. A valid positive still names and
retains it without another host-wide probe; an unmount exposes the underlying RAM device and triggers a fresh probe.
Image-backed and otherwise unkeyable holds stay uncached: device-node reuse has no proven cheap incarnation key. Every
pass that claims such a lease probes it again, with the pass's inventory, which takes seconds on a busy host; that
latency falls on the claiming task, outside the lock. A negative verdict is never cached, probe errors never stand in
for an empty holder set, and held directories stay unchanged.

Contended lock opens also use `O_NONBLOCK` and yield for 50 ms, avoiding a separate latent filesystem-worker deadlock
when one Node process makes five concurrent requests with four workers. The observed gate failures instead came from
cross-process waits behind long host-wide probes under the RAM lock. Real nested and sibling default-executor proofs run
with background single-lease processes to cover that shape. No case or target timeout is raised.

Once the retirement and holder checks permit removal, a lease to delete, and an archive being deleted, has owner
read/write/search permission restored on its directories through no-follow directory handles first. Read-only fixture
directories cannot strand a dead lease and block later tasks. File modes and symlink targets stay untouched; a directory
on another device refuses cleanup instead of changing an attached volume. Held leases undergo no permission changes.

The real RAM-volume regression walks one volume's lifecycle in a single test: unmarked debris and a live-lease refusal,
recovery over the debris, concurrent leases, failed-acquire lease/claim rollback, read-only cleanup, a held dead lease,
owner detach and reclaim of that lease while a writer the dead task left still writes into it through `TMPDIR` (retired
through its recorded handle, then archived whole, readonly tree and modes included) and a bystander works in it (named
by the census, never signalled), beside a dead lease that recorded no handle and one whose handle another live job holds
(kept, then reclaimed once that job ends), a later dead lease whose archive rotates the oldest out and is deleted in the
background, a failed run's own lease archived below the half-volume floor and so deleted with every other archive, and
the final detach. Its supervisor is a stand-in speaking the subtree verbs' wire shapes, whose tree is the root's process
group (`src/__tests__/stand-in-cowshed.ts`). Its volume uses a real private gateway served by `ram_fixture`, with
`recordedRamAttach` capturing every attached identity and `disk-command --gateway-socket` routing native admission. The
helper is closed only after exact-owned teardown; no test asks the installed gateway. External symlink targets and held
directory modes stay unchanged, without duplicate provision and teardown; its 60 s case bound is unchanged and its
target has the progress bound.

The host target names both owned files with `./` paths: Bun otherwise interprets a bare path as a filter over recursive
test discovery. This changes no selected case or timeout. The failed-provisioning regression keeps its exact injected
refusal and rollback exit-status assertions, then checks the named volume and both mountpoints before test teardown. Its
trace records command start/end times, refusal, inventory and separate teardown. Commands, refusal and inventory are
also written to stderr immediately, because a runner timeout can end the process before the harness flushes its trace. A
reported rollback failure must not be mistaken for a successful detach that left a volume behind.

A refused rollback also records the kernel mount and read-only filesystem/device `lsof` observations before teardown.
Each observation has a one-second wait bound and retains its status and stderr; an incomplete probe is not an empty
holder set. These observations do not prove which holder caused `EBUSY`, and change no case or target bound.

The native lifecycle walk's real host-command delegate records start/end timestamps from case entry, retaining the
actual argv, requested deadline and result without changing them. Observation before delegation is timed separately from
the delegate's wait, so logging cost is never presented as child execution time.

The volume mounts at `/private/var/folders/cowshed/<uid>/ram3`. The parent is the per-user root `cowshed setup` makes,
`/private/var/folders/cowshed/<uid>`, a 0700 directory of the user's; the leaf carries the protocol's revision, so two
revisions never mount on one directory. Spotlight's `mds` admits no volume whose kernel-resolved mount path contains
`/var/folders/`, and keeps one IOKit right for every volume it admits for as long as it runs, with no way to release it
(`specs/cowshed/01a_apfs.md`, "Spotlight media references"). Measured 2026-10-22: a RAM volume mounted at `/Volumes` or
`/private/tmp` left `mds` one right; the same mount under `/private/var/folders` left none. A task refuses to create the
volume while that root is missing, is a link, or is not a 0700 directory the user owns, and its error says to run
`cowshed setup`. The kernel mounts the volume there: `mount_apfs` needs no administrator, where
`diskutil mount -mountPoint` escalated to an administrator dialog that blocked every `diskutil` on the host. The mount
is `noowners`, and launchd refuses a plist from it, so a test that bootstraps a launchd job keeps the plist outside
`TMPDIR`.

The volume is created under its final name, `smoo-ram3-<uid>`, and is never renamed. It is mounted and marked at a
private staging directory, `/private/var/folders/cowshed/<uid>/ram3-new`, then unmounted and mounted again at its
mountpoint, so a creator that dies part way never leaves an unmarked volume there. No DiskArbitration call makes either
directory, so none removes them: the task that detaches the volume, or whose provisioning step failed, removes both.
Before creating, a task inventories RAM disks of the same uid and size, but those fields authorize no detach. Only an
exact creation receipt owned by this volume permits debris reclamation; unknown legacy or unmarked disks make the RAM
optimization unavailable and are left untouched. One that still carries a lease directory, or has something attached
below it, is named in the task's error instead. The task finds these disks, and the one it detaches with the last lease,
in the I/O Registry (an `IOHDIXHDDriveOutKernel` entry per RAM disk, with its size and owner uid) and their mounts in
the mount table, never in `hdiutil info`: measured 2026-10-22, `hdiutil info` left an attached RAM disk out of 2 of 10
listings while other images attached and detached, and a disk left out would read as absent. A registry read that does
not parse whole fails the task instead. A BSD name passes to the next disk once its disk goes, so every RAM detach,
including provisioning rollback, is the typed gateway-owned `ram-detach` of an exact device and registry entry
(specs/cowshed/05_gateway.md, "RAM disks"). The gateway revalidates that identity inside its own lease before spawning
the native tool. It uses no force; a busy owned staging volume stays in place with its named refusal. A requester or its
job ending cannot kill this protected mutation. Authoritative terminal, proven not-started and unknown outcomes remain
distinct; only the first two or exact entry disappearance release pending authority. A gateway that predates
`ram-attach`, or has no control listener, makes optional RAM unavailable with its typed reason, not a task failure.
Required RAM fixtures still fail. No task ever attaches the disk itself.

RAM attach and detach go through their typed gateway operations; every other `hdiutil` and `diskutil` execution goes
through `cowshed disk-command`, including host-test inventory. The cowshed is the installed one on `PATH`, which must
provide this command, unless `COWSHED_EXECUTABLE` names another. Where `PATH` provides no cowshed, a workspace that
itself provides a launcher — its own cowshed package's `packages/cowshed/bin/cowshed`, or a linked one's
`node_modules/@smoothbricks/cowshed/bin/cowshed` — names that launcher for the run and its command instead; an
environment with no cowshed anywhere keeps the bare name, so its spawn failure names the missing installed tool. A
target may set it in its `env`, workspace-relative or absolute: the task's own RAM volume work and its command then both
run that cowshed, given to the command as an absolute path. This repository sets it to `packages/cowshed/bin/cowshed`,
after `cowshed:build`, on the targets that exercise the facade (both real-APFS groups and `nx-plugin:test-host`), so
they test this checkout's cowshed, not an installed one that may predate a command the checkout added. Cowshed alone
owns disk-lifecycle leasing. Safe disk commands use
`cowshed disk-command diskutil --deadline-ms <positive integer> -- <args>`: the CLI owns that total monotonic budget
across lease admission and child execution, never starts a child after it expires, and kills and reaps an overrun safe
child before returning. Node waits for the facade's actual terminal exit without a second timer or lease. Only protected
image commands (any `hdiutil`, `diskutil image`, and `diskutil eject`) use `cowshed disk-command <tool> -- <args>` and a
caller-bounded stop-wait: cowshed starts them detached from caller signals, with file-backed output, and never kills
them, because killing an attach or detach can leak its kernel device until reboot. A still-running protected command
retains its lease and is named in the timeout error. That timeout status is the wrapper's stop-wait outcome, not an
actual disk-child exit or signal; native termination diagnostics come from the CLI's owned child wait. Other helpers
retain their process-group deadline. The shared default remains 120 s. A process whose lease waits are recorded
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
for the runner's requested parallelism, read from the runner's own flag rather than the core count — nextest's
`--test-threads` or one per core (one for `--no-capture`, which runs tests serially), `bun test --parallel=N` N,
Stryker's `--concurrency` N or N% (default one per core less one past four cores), Vitest's `--maxWorkers` N or N%
(default one per core less one), `go test -p` (default `GOMAXPROCS`), cargo-mutants' `--jobserver-tasks` (default one
per core, whatever `--jobs` says), a cargo build one per core, anything else one, or the target's `parallelism` option —
bounded by positive inherited `NEXTEST_TEST_THREADS`/`CARGO_BUILD_JOBS` ceilings. A configured count can lower an
inherited ceiling, never raise it. The runner uses `min(bounded request, granted tokens)`: nextest's environment and
named flag, Bun's parallel flag, Stryker's, Vitest's and go's worker flag (added when the command names none),
cargo-mutants' jobserver tasks, `--jobs` and per-process test threads, and a cargo build's libtest and nextest test
threads are sized to that limit. Cargo's rustc count needs no flag: the run's sub-pool is a GNU make jobserver fifo that
`CARGO_MAKEFLAGS` and `MAKEFLAGS` name, so Cargo, its build scripts and every nested `cargo` share the grant's count
with the sub-pool's socket children, and no grant sets `CARGO_BUILD_JOBS`. A command whose runner the executor cannot
see (a script) gets `RUST_TEST_THREADS` and `NEXTEST_TEST_THREADS` set to its whole held grant, since test threads take
no jobserver token. A runner that starts its own workers or jobserver (cargo-mutants, Stryker, Vitest, `go test`)
ignores that fifo, so it must run under `bounded-exec`, whose runner kinds size its own flag; the inferred `mutation`
target does. Test threads retain their independent inherited/configured ceilings. `BOUNDED_EXEC_CPU_TOKENS`, the
sub-ledger and telemetry still name the actual held grant. The gateway shares tokens fairly between checkouts (Nx
workspace roots). `timeoutMs` and `idleTimeoutMs` start at the grant. The tokens go back when the command exits, or with
the executor's socket when it is killed. The wait prints one
`cowshed: cpu-tokens wait done elapsed=… tokens=<granted>/<asked>` line. On a host whose gateway socket does not exist
(a CI runner, a machine without cowshed), the plugin's `preTasksExecution` serves each Nx run that inherits no pool its
own `BOUNDED_EXEC_CPU_POOL` of one token per core, with its jobserver fifo in `CARGO_MAKEFLAGS` for the Cargo that
`nx:run-commands` starts, and `postTasksExecution` closes it: the run's targets, started side by side under
`NX_PARALLEL=100%`, queue for its cores instead of each sizing its runner to the whole host
(`specs/cowshed/05_gateway.md`, "A run without a gateway budgets itself"). Its scope is one run. A command no pool and
no gateway answers runs exactly as configured and nothing is said; a gateway from before the budget runs it unbudgeted
and stderr says so.

A granted executor serves a private `BOUNDED_EXEC_CPU_POOL` with its actual token count, and the same count as a make
jobserver fifo beside it: `g − 1` token bytes, the command holding the implicit first, which the pool lends a socket
child only once the fifo is empty. A socket grant takes fifo bytes first and that slot last, and its release writes the
bytes back; the pool waits for a fifo byte on the event loop, so a token Cargo writes back wakes the queued child. Its
close drains and removes the fifo. A nested default executor asks that immediate parent pool for its own demand, instead
of taking from the host while the parent holds its grant; the pool grants at most what it can. A lent pool accounts for
siblings together; each admitted child serves only its own allocation to grandchildren. Closing a child's connection
returns its local allocation, never the parent's upstream grant. Upstream loss revokes the downstream pool, so a dead
parent grants nothing more; a command whose grant closes while it runs (a gateway restart, or the lending parent's exit)
runs on over budget to its own exit, and stderr says `cowshed: cpu-tokens grant ended`. A bare count or absent endpoint
follows normal host admission. A connected parent pool's refusal or protocol failure instead fails admission: taking
from the host then could wait on the parent's own grant. `options.env` cannot inject either marker, nor a jobserver
(`CARGO_MAKEFLAGS`, `MAKEFLAGS`, `MFLAGS`); a parent make's jobserver the executor inherits still reaches an ungranted
command. Parent-pool grants record `cpu.inherited: true` and their actual local wait; the span also says
`inherited=true`. Pool setup, listener and cleanup errors fail rather than bypassing accounting. Command bounds stay
unchanged.

The absent-parent fixture uses the executor's injectable budget with a real, controlled outer pool as its backing. It
proves a positive allocation and re-acquires it after the command ends, rather than depending on free tokens in the
host-wide gateway while the test runner itself holds an allocation. A bare count or unbudgeted success is not proof. A
land gate (`tooling/land-host.sh`) enters the existing task server with
`cowshed exec <workspace> --class gate --host [--want N] -- COMMAND [ARGS...]` after daemon-stop. It requires a real
gateway jobserver `class: "gate"` grant for its own orchestrator; its live pool lends nothing of its own and relays
every child's ask to the host as a gate-class request, so each task the gate starts is served FIFO ahead of every other
exec's queued work. No environment-only class or unbudgeted fallback is accepted. The gate waits for its grant with no
deadline, ahead of queued background work. Running background tests/FIFOs are never shrunk, frozen or killed to
manufacture tokens. Requested/granted count and actual queue wait are logged, and a child's nonzero exit is preserved
exactly. Compiling Cargo under the live pool takes a local allocation and receives its own private child pool, so
grandchildren never compete with their occupied ancestor. See `specs/cowshed/05_gateway.md`, "Gate admission: FIFO ahead
of background, never starving it", for the guarantee and its deliberate limits.

Every `bounded-exec` task records why it ended in `.nx/workspace-data/bounded-exec/<task>/verdict.json`, keyed by the
task id and the hash it ran at:

- `bound`: nothing failed but a wall-clock bound. Either the command outlived `timeoutMs`, or every failing test in the
  runner's JUnit report failed on its runner's per-test timeout (`cargo-nextest` writes `type="test timeout"` where a
  configuration still sets `terminate-after`, `bun test` writes `type="TimeoutError"`). A loaded host produces exactly
  these. When `timeoutMs` fires, the tests `cargo-nextest` was still running are reported as failures of
  `type="test abort"`, `process aborted with signal 15 (SIGTERM)` (SIGKILL once the force-kill follows); they are the
  bound's casualties and do not make the run `failed`. A test that dies of any other signal, or that is aborted with no
  bound to explain it, does.
- `wedged`: `idleTimeoutMs` fired: no test finished during that much active test time. This proves the completion
  interval was exceeded, not that every process stopped: an oversized fixture can exceed it too. Its `tests` name those
  the runner reported running (nextest's `SLOW`, libtest's `has been running for over`) and never finished, and those
  the JUnit report says it aborted when the bound stopped it. Its `lockWaits` lists only regular files the run holds on
  numbered descriptors that processes outside the run also hold open, naming those holders' pids and commands. This is
  open-handle evidence, not kernel lock-owner proof. The open-file census runs only on macOS, so on every other platform
  `lockWaits` is always empty. An incomplete or raced capture records none.
- `no-space`: the command failed after its output said a write found no space (`ENOSPC` or `No space left on device`).
  Its tests failed on the disk, not on their own, whatever bound or hang followed: it records that line and, when the
  task held a RAM lease, the lease, the volume and its free bytes when the line arrived, or why none were read: the
  lease was no longer on its volume (`lost`) or the volume could not be read (`unread`). A passing run that printed such
  a line passed.
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
compilation/archive targets are ignored by this test-leg check. Each bounded Bun leg must delete Bun's per-test deadline
with `BUN_TEST_TIMEOUT_FLAG` (`--timeout=0`) and set no other; a number is refused, since it bounds a test's wall-clock
time, which is the host's, rather than its progress. Only a leg that runs `bun test` must start in the project root or
its `src/`, where Bun's test-file discovery is cheap; any other command (a cargo workspace's per-crate nextest legs run
from the workspace root, a gate may run a script that lives in another project) may use any `cwd`.

## Test Fixtures That Run Nx

The executor's real-process tests acknowledge handler installation and process-group membership before delivering a
timeout signal. Cooperative termination requires its handler receipt and no force kill; descendant lifetime pipes prove
actual closure. The abort-report walk requires the runner's named cancellation record as well as total-bound
classification, so an executor-only expiry cannot stand in for a report the runner never wrote.

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
  `<workspace>/.nx/workspace-data/d/server-process.json`, and its descendants, has exited, then names on stderr every
  process the host census still finds working in the workspace. That final census also catches editor probes reparented
  when the daemon exits. It runs only where a daemon was started: Nx opens `d/daemon.log` before it spawns one, so a
  workspace without that log, such as a daemonless fixture's, skips the host process scan.

A fixture retires on every exit path, a throwing body included: stop its daemon, unlink its socket alias, then delete
its root. A daemon that outlives the stop keeps the alias and the root, so the next run's sweep still finds both.

A fixture workspace that runs real Nx with this plugin links an installation it owns, never the host's.
`@smoothbricks/nx-plugin/owned-installation`'s `ownedInstallation(censusRoot, name)` copies the closure the graph
witness's installation census resolves from `censusRoot` (Nx, this plugin and TypeScript, each package with its
dependency edges relinked beside it and the top-level bins in `.bin`), and keeps the copy as `name` in `censusRoot`'s
tool cache, `node_modules/.cache/smoothbricks-nx-plugin/owned-installations/<name>`. Nothing sweeps that directory: a
cowshed fork clones it with the build volume and main adopts it at land (specs/cowshed/16_build_volumes.md), so it is
cold only after `node_modules` is reinstalled or the build volume reseeded. Making the copy is most of what a test file
costs (about 21,000 files: a 59 s copy and a 31–119 s removal per run on a loaded host), and clonefile cannot share it,
since Bun's global store and every directory a cowshed job may write lie on different volumes. On the first
`nodeModules()` of a run the census of the host closure and the proven census of the copy are compared package by
package, by what each holds: resolution edges, node types and modes, and file bytes. A package whose copy no longer
holds what the host's holds is copied again, and every package when the edges moved or the copy is unproved; a copy that
was there and failed its proof is named on stderr with the reason. Both censuses take unmoved files' hashes from the
ones recorded beside the copy, so a run that changes nothing reads no file bytes. `retire()` fails, naming the changed
nodes, if a fixture wrote into the copy, and leaves it for the next run to prove. The host's closure moves under a
fixture that reads it: a shared Bun store relinked by another install, or, in a workspace that developer-links this
plugin, the checkout its author is editing and rebuilding. Either fails the producer receipt's installation proof for
work that is not the test's.

Test files that name one copy share it. Bun runs many test files in one process, which takes the copy once, on the first
`nodeModules()` of any of them, and each file's `retire()` proves that no fixture wrote into it since that take.
Concurrent test processes, such as the shards of one run, share the copy too: an exclusive flock(2) on `lock` beside the
record admits one take at a time, held by a perl child the kernel releases however its holder ends, and each taker names
the copy it took in `holders/<pid>`. A copy that another live process holds is never changed in place: one that no
longer holds what the host closure holds is made again, whole, in a new `installation-<uuid>`, and a copy that neither
the record nor a live holder names is removed by the next take. A changed host closure is therefore copied once per run,
not once per test file. This package's tests share one copy, `nx-plugin-tests`, and bind `censusRoot` to this
repository; a consumer binds it to its own workspace root.

The copy lies beneath `censusRoot`'s `node_modules`. Where `node_modules/.cache` is a real directory, as on a fresh CI
checkout (a cowshed build volume links it elsewhere), Node's ancestor lookup from the copy reaches the host's own
packages, so a census of a missing or incomplete copy would resolve the host's. A census of the copy that resolves any
package outside the copy therefore proves nothing about it: the copy is unproved and made again, and stderr names the
resolution. Node keeps each resolution for the life of a process, so a copy made whole goes to a new directory,
`installation-<uuid>`, which the record beside it names. The same lookup lets a package in the copy that requires a
dependency it does not declare load the host's top-level one; the census binds declared edges only and cannot see that.

## Managed workspace files

`nx generate @smoothbricks/nx-plugin:managed-files` stages the same managed files as `smoo monorepo update`, without
installing packages or resolving secrets. Use Nx `--dry-run` to inspect the changes. The workspace defaults register
this generator for `nx sync` and `nx sync:check`.

The plugin owns the packaged templates, pure rendering and content-preservation functions. The generator reads workspace
files through Nx `Tree` and uses Nx's resolved project graph for inferred targets. The CLI only supplies the filesystem
and process boundary. There is no separate serialized change plan.

The idempotence witness compares the second pass's managed-target roster with the populated first pass before checking
unchanged actions, an empty staged diff and retained bytes. An empty result is not evidence that generation inspected
the managed files.

Files with a `# smoo-local` tail or `# smoo-local-begin` / `# smoo-local-end` blocks retain those sections. Matching
source symlinks are preserved; conflicting local blocks and broken or external links are reported instead of
overwritten. The generator does not remove a repository-owned file when a capability is disabled.

`tooling/direnv/repo-path` writes the `PATH` every managed shell entry and git hook uses. In a cowshed workspace whose
`.cowshed/env` lacks `TMPDIR` or `COWSHED_RUNTIME_LINK`, a fork interrupted before its first supervisor start, it says
so on stderr, runs `cowshed exec <ws> -- true` once through the installed cowshed, and continues on the file that start
served. It refuses, naming the workspace and the start's exit status and error, only when that start fails or leaves the
file unserved (cowshed `specs/cowshed/02_workspaces.md`, "Workspace environment").

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
