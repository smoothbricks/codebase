# Patches

Patches this repository applies to its dependencies. `bun install` applies the root `package.json`'s
`patchedDependencies`, including `nx@23.2.1.patch`. The root `overrides.nx` pins the registry version `23.2.1`;
`bun.lock` is written by Bun and records that version, registry integrity and patch path. The root `overrides["@nx/js"]`
likewise pins `23.2.1`, the version its patch applies to.

## Flechette factory declarations

All Flechette consumers use the 2.7 series. `@uwdata/flechette@2.7.0.patch` is the reviewed declaration repair shared
with strict external consumers: the upstream source declares the generic row factory as private and readonly, and its
owner-generated `table.d.ts` preserves that field. Runtime factory logic is unchanged. Do not cast tables or relax a
consumer's `skipLibCheck` to hide a factory declaration mismatch. Apply this patch through root `patchedDependencies`
and ordinary `bun install`; rebuild lmao's declarations from the installed product. Retire the patch only when the
upstream declaration preserves the same contract.

Flechette 2.7 builds Date64 metadata from numeric millisecond inputs. The Compact fixture seeds only that schema with a
number; its physical payload remains `BigInt64Array`, with the same i64 round-trip assertions.

## scriptc's installed toolchain

`scriptc@0.4.4.patch` records the toolchain sidecar's payload paths from the platform package's canonical absolute
source directory, not relative to the checkout's installed compiler. Bun's isolated packages live in its shared
link-target cache (`specs/cowshed/03_caches.md`, "The discriminator"); a warm checkout at another depth must still reach
that same payload. Bare linker commands stay on PATH, and the upstream installer's copy and executable-mode
normalization stay unchanged. No consumer overrides the compiler or rewrites its sidecar.

The installer regression in `tooling/direnv/scriptc-install.test.ts` exercises the actual pinned relocator through a
symlinked cache and two checkout depths. The native targets described in `packages/nx-plugin/README.md` exercise the
installed compiler. Sidecars remain installed dependencies, never committed source or Nx outputs. Retire this patch when
the upstream installer publishes depth-independent payload paths.

## typia output scriptc compiles

scriptc compiles a package's ttsc-emitted JavaScript to a native executable (`scriptc-<arch>-<family>`,
`packages/nx-plugin/README.md`), and two facts of typia 15.2.1 stop it. Both patches change no validator's behaviour.

- `typia@15.2.1.patch`: `FeatureProgrammer.WriteDecomposed` (`native/core/programmers/internal/FeatureProgrammer.go`)
  appended the uninitialized `let errors; let _report;` (validate) and `let _errorFactory;` (assert) after the functor
  helpers and checker closures that read them. That is valid JavaScript, but scriptc 0.4.4 refuses a closure over a
  binding declared after it ("a binding form with no lowering"), so the program built and died on its first invalid
  input. The patch declares those slots first. ttsc builds typia's transformer from `node_modules/typia/native`, so the
  next ttsc run rebuilds it from the patched source.
- `@typia/interface@15.2.1.patch`: ttsc keeps `import typia from 'typia'` after transforming every call away, and
  `typia/lib/index.mjs` re-exports `tags` from `@typia/interface`, which ships only its tsc CommonJS output (top-level
  `this`, `exports` as a value), which scriptc refuses. The patch adds `lib/index.mjs` (the package's whole runtime
  surface: the `ProtobufWire` enum and an empty `tags` namespace) and an `import` export condition naming it.

Retire each when typia or scriptc removes its cause; bump both with typia.

## Activating a patch change

Prepare Nx changes with `bun patch nx@23.2.1`, then use the registry-index regeneration flow:

```sh
bun patch nx@23.2.1
python3 tooling/nx_patch.py --check
# Edit the Bun-prepared node_modules/nx package only after the no-op check passes.
python3 tooling/nx_patch.py
rm -rf node_modules/nx # Retire Bun's prepared copy before restoring its isolated dependency links.
bun install
```

The no-op check must reproduce the current patch byte-identically before editing. The script verifies the pristine
registry archive against `bun.lock`, indexes that package in an isolated temporary Git root, and diffs the prepared
package against that index. It preserves full deletion hunks and package-relative paths, excludes only Bun's install
markers and dependency directory, and writes the root patch and its byte-identical managed consumer copy. Its temporary
root is retired on success or failure. `tooling/test_nx_patch.py` guards deletions, additions, installer metadata and
archive/package identity. The generator supports Python 3.9 and newer. Extraction explicitly uses `filter="data"` on
Python 3.12+; older interpreters omit only that unsupported keyword. Every version verifies the archive's lock-selected
SHA-512 before extracting it.

If an inherited patch differs only in diff-header spelling or hunk ordering, first apply it to the verified pristine
archive and compare every resulting package file with the prepared package. Only an identical file set and content allow
one generator normalization of both carried copies; then require a byte-identical `--check` before editing.

Do not use `bun patch --commit` for this carried patch with Bun 1.4.2: it emits absolute registry-cache paths and
discards the bodies of deleted-file hunks, and includes a `.bun-tag-*` installer marker. Nor hand-edit the generated
patch. Retire the Bun-prepared `node_modules/nx` copy after regeneration: even `bun install --force` leaves that
dependency-less copy in place. Ordinary `bun install` then restores the isolated package and owns activation and all
lock changes; commit them with the patch. Nx is never developer-linked and needs no release publication, generated
tarball or package-activation target. Bun's isolated global store deliberately keeps patched entries project-local under
`node_modules/.bun`; their installed patch marker and cached identity distinguish the patched bytes. Do not replace that
package with a writable generated-product link or edit an installed package without first preparing it. The
installed-product regression runs `node --check` on every patched JavaScript file as well as checking the five-minute
inactivity policy.

The nx-plugin managed files distribute the same Nx patch to consumers. The root monorepo policy maintains
`overrides.nx = "23.2.1"` and `patchedDependencies["nx@23.2.1"] = "patches/nx@23.2.1.patch"`. Run `smoo monorepo update`
and ordinary `bun install` in a consumer to adopt it. Tests import the actual installed Nx package, so a source gate
covers the currently maintained patch without publishing anything. Drop a carried hunk only when the installed upstream
version contains its repair, retaining its regression test.

### Known Bun 1.4.2 deletion-patch bug

Bun 1.4.2 (`744846f84`) generates a malformed deletion section: only a `diff --git` header naming an absolute cache
path, `deleted file mode` and `index`, without `---`, `+++ /dev/null`, the deletion hunk or the removed lines. Full
regeneration therefore corrupts the existing task-history deletions. A fresh registry cache reproduces the same defect;
it is not repaired by clearing the cache.

Minimal reproduction, in a disposable directory:

```sh
mkdir nx-delete-repro
cd nx-delete-repro
printf '{"private":true,"dependencies":{"nx":"23.2.1"}}\n' > package.json
bun install --cache-dir ../nx-registry-cache
bun patch nx@23.2.1 --cache-dir ../nx-registry-cache
rm node_modules/nx/dist/src/daemon/message-types/task-history.js
bun patch --commit nx@23.2.1 --cache-dir ../nx-registry-cache
cat patches/nx@23.2.1.patch
```

The resulting patch has only three lines and cannot carry the deletion's contents. Repair and maintain the carried patch
with the verified registry-index generator in [Activating a patch change](#activating-a-patch-change): prepare Nx,
require `python3 tooling/nx_patch.py --check` to reproduce the current patch byte-identically before editing, then
regenerate both patch copies with `python3 tooling/nx_patch.py`. Retire the prepared copy and install normally as
specified there. Do not splice Bun-generated sections or hand-edit the generated patch.

## Pre-task producer selection

The pre-task context carries the project graph already loaded by Nx and a task graph expanded by Nx from the
invocation's normalized projects, targets, configuration, overrides and dependency-selection option. Producer hooks
select configured canonical roots from those task identities instead of parsing argv, asking for another project graph,
or capturing every configured producer before an unrelated command. Configuration and dependency expansion for that
membership test remain Nx's authority. Hooks still run before task hashing and sync processing; a later graph change
cannot publish without matching actual POST results and the existing source/configuration checks. A wrapper probe that
executes no tasks supplies an empty task graph.

The producer source tests cover shared acquisition with independent output exclusions; the linked-snapshot smoke uses
real Nx pre/post hooks and validates the published completion through the consumer operation. Retire this patch when
upstream supplies the same pre-task selection context.

## Root-only raw-input inspection

`getTaskRawInputs` returns only the requested canonical task's inputs. Upstream nevertheless asks the native inspector
to resolve every plan in its dependency graph, including output-inventory walks whose results the caller discards. Each
parallel output walk creates its own filesystem workers, so this wasted work can consume a test's unchanged bound.

The raw-input call now supplies only the canonical task ID at the plan-selection boundary. The complete task graph still
reaches the planner: dependency edges and generated-output inputs are not excluded. `inspectTaskInputs` keeps its
full-closure default; its optional selected-ID list limits resolution, not graph construction. No persistent inventory
cache, thread-environment override, input narrowing or timeout change is involved.

`nx-raw-inputs.test.ts` runs the actual installed Nx planner and inspector against a diamond producer graph. It compares
every root input bucket with full-closure inspection, retains the shared transitive producer's declarations,
distinguishes configurations and unrelated files, and preserves refusal errors. A delegating control drops the
selected-ID argument; the same oracle must then reject the old all-plan behavior without depending on a timeout.

## A hash failure names its tasks and runtime inputs

Nx 23.2.1's native hasher reads a runtime input's stdout and stderr with `std::str::from_utf8(…)?` (`hash_runtime.rs`),
and the task hasher passes that error up with no context, so the client printed the bare decoder message:
`invalid utf-8 sequence of 1 bytes from index 0`. Every failed input `runtime-input.sh` wraps ends its stdout with that
byte (`packages/nx-plugin/README.md`, "Runtime inputs"), so that one line was all Nx said about any failed input of any
task in the batch.

`NativeTaskHasherImpl.hashTasks` (`hasher/native-task-hasher-impl.js`) now rethrows a native failure as
`Could not hash <task ids>: <native message>`, then each `runtime:` instruction of those tasks' plans with the tasks
that hash it, read from the same planner (`HashPlanner.getPlans`). A list names ten task ids and counts the rest
(`…, and 2 more`), as cowshed's refusals do. Nothing runs again, and the native error is the `cause`. A planner that
cannot answer costs the listing, never the native error: its message still leads, and the error says why the runtime
inputs could not be listed. A native value that is not an `Error` is named by `String`. The hunk runs where the hashing
runs, in the daemon or in the client, so both report the same lines. It names the candidates, not the input that failed:
the native error does not say which, and finding out would run the inputs again. The wrapper's retained report names the
failed command and its cause.

`packages/nx-plugin/src/runtime-input.test.ts` drives the installed `hashTasks` over stand-ins for the native planner
and hasher, faked at their published methods: a listed planner, twelve tasks, a planner whose `getPlans` throws, and a
native value that is not an `Error`. It also plants a failing input through real Nx, with and without the daemon,
requiring the error to name the task and its runtime input. Drop the hunk, and keep the tests, once the installed Nx's
hasher names the input it failed on.

## Cross-client task output ownership

Clients of one checkout share a target's output tree and cache entry. Each target with declared outputs holds one
kernel-backed lease from cache lookup and restore through execution, publication, output-hash recording and cache
capture. The lease is keyed by project and target in the checkout's workspace-data directory, not by task hash or
configuration: different inputs and configurations can publish the same directory. Independent targets and checkouts
remain concurrent. A waiting client rechecks the cache after acquiring ownership.

The native cache recursively copies live directories. Replacing a complete output tree and retiring the previous
directory during that walk can return success with only a tail of its files; two same-hash writers can also remove each
other's cache destination. A lock inside the producer command ends too soon to protect either capture or restore.
Discrete, batch and continuous task lifetimes therefore own the lease at the orchestrator boundary. Batch keys are
acquired in sorted order; a continuous task retains ownership until its exit is handled, while clients sharing the
already-running task do not acquire another writer lease.

A continuous target's short startup guard covers both the `RunningTasksService` check and the winner's registration. Two
cold clients therefore cannot both observe "absent" before one takes lifetime ownership. The loser joins
`SharedRunningTask`, consumes the winner's output and waits for that producer to finish, not for its lifetime lock.
Startup and output locks use separate purpose namespaces and bounded pools: a one-worker output pool cannot deadlock by
holding its startup slot while requesting another slot for lifetime ownership.

Nx's native `FileLock.lock()` blocks synchronously, so a bounded, reusable helper-process pool holds leases without
blocking the client's event loop. A helper can be cancelled even while blocked inside the native call; a worker thread
cannot. Lock files are never unlinked or replaced; their contents name the holder and client PIDs and target names, and
helper death releases kernel ownership. Runner cleanup cancels pending acquisitions before stopping owned producers,
then releases held leases. The `nx:task-output-lease` diagnostics channel reports waiting, acquired and released
transitions with the lock purpose, target names and lock paths. Cancellation uses `SIGKILL` only on the client's owned
lock helpers, which execute no task work, to interrupt a blocking native wait without depending on a JavaScript signal
handler. Producers retain their existing shutdown protocol.

A cancelled acquisition fails with its cancellation, never with the exit of the helper its own client killed: the error
names the targets, the cleanup's cause (the signal the client received, another stop, or the run's end) and each lock
file's holder record, so a client stopped while it waits says which helper and client held the lease. Reported as the
helper's exit, a post-land install stopped by `SIGTERM` while it waited for an operator's build of the same target read
`Task output lease worker exited (code null, signal SIGKILL)`, as if something had killed the helper. The suite cancels
a waiter in-process and stops a real waiting client with `SIGTERM`; both must name the cancellation and the holder. A
waiter for a pool slot waits for exactly two events: a release hands it the released helper, or a helper leaves the pool
and its slot is handed on to fork a successor. A helper leaves the pool on every way it can stop serving: its exit, a
failed spawn, a failed send, or an error reply, after which the client kills it (its reply came after it unlocked what
it held). A helper that dies holding a lease fails only that holder's release, with the exit named; a dead idle helper
is never handed out. Without this, one failed acquire in a one-worker pool (`--parallel=1`) left a live, idle helper
holding the only slot and every later acquire waiting forever, the client and its helper both idle with no lock wait. No
deadline stands in for these events.

A released lock does not imply a finished writer. A helper that dies holding a lease releases the kernel lock while the
task may still be writing its outputs, so another client can take the target then; the task fails only at release time,
with the helper's exit named.

A helper keeps one `FileLock` per lock file for its lifetime and locks and unlocks it again on each acquire. Nx's
`FileLock` has no close, so opening one per acquire kept every descriptor until garbage collection: one helper held 27
descriptors for 14 lock files, growing with every task until `EMFILE`.

`packages/nx-plugin/src/nx-task-output-lease.test.ts` exercises two actual Nx clients against one output tree, pauses
the real cache capture while another producer attempts to publish, checks both complete byte-exact artifacts, then
restores one through a dependent runtime-input hash. Binary file content is hashed as bytes by its producer; the
runtime-input wrapper's deliberate non-UTF-8 failure sentinel is unchanged. The suite also starts two cold continuous
clients at the same barrier, requires one real producer and one shared waiter while that producer is alive, and requires
both clients to complete after it exits. Its pool cases kill a helper holding a lease, make a helper reply with an
error, and kill an idle helper; each next acquire must take the freed slot. A helper that acquires one target five times
must hold one descriptor on its lock file. Drop these hunks only when the installed upstream orchestrator protects the
same complete lifetime and atomic startup, retaining the regressions.

## A remote cache outage is a miss

Nx 23.2.1's native self-hosted cache client (`HttpRemoteCache`) sets no timeout and rejects every transport failure with
`Failed to send request: …`; the task runner awaits it unguarded, so an unreachable cache failed the whole run and a
server that accepts and never answers held it forever. Measured 2026-10-22: every Nx run in the affected sheds exited 1
with `Failed to send request … http://127.0.0.1:<port>/nx/v1/cache/<hash>` while a host-wide APFS stall held the
gateway's cache server. Cache downtime must never stop work (the owner's ruling, 2026-10-17;
`specs/cowshed/05_gateway.md` "`/nx/v1/cache/{hash}`", `specs/cowshed/03_caches.md` "Nx accepted-task artifact
service").

`getHttpCache` now wraps the native client in `RemoteCacheOutageGuard` (`tasks-runner/remote-cache-outage.js`). Its pure
`settleRemoteCacheRequest` decides each settled request: an answer is used; a send or body-read failure, a 5xx status,
or no result within `REMOTE_CACHE_ANSWER_WITHIN_MS` (90 s) is an outage; every other rejection (401, any other
unexpected status, a damaged or unsafe archive, local disk) is rethrown unchanged and fails the run as before. The first
outage prints one line naming the server, the request and the cause, and that process asks the server nothing more:
every lookup misses and every store is skipped, so tasks run exactly as with no remote cache. The bound covers the whole
native call (a store builds its archive inside it) and is longer than the gateway's own outage answer (connect 5 s plus
response head 60 s), so it decides only when the server itself cannot answer. The guard takes its clock as an optional
third constructor argument, a function returning the deadline for a bound: a promise that elapses and its release.
`getHttpCache` passes none, so a run keeps its 90 s timer; a test passes a deadline it elapses itself.

The native call the deadline overtook keeps running, unread, and a late lookup would still unpack its artifact into the
directory it was given: over the entry the task wrote meanwhile, or while the task writes it. So a lookup never gets the
cache directory. It unpacks into its own staging directory under `<cacheDirectory>/remote-retrieve/`, and only an
artifact answered in time is moved to `<cacheDirectory>/<hash>`. A miss or an outage removes the staging directory once
its native call settles, and one still unsettled when the process exits is removed then; a removal that fails is said. A
store's late native call only reads the entry. Nx Cloud and the plugin caches are not wrapped.

`packages/nx-plugin/src/nx-remote-cache-outage.test.ts` holds property tests of the decision and the line, and runs the
actual installed Nx against real local servers that refuse, hang up after reading the request, answer 503, and answer a
lookup 404 then hang up on the store: each task runs and passes with exactly one diagnostic, and the server is asked
nothing after the outage. A 400 still fails the run. A server that accepts and never answers is the guard's own
deadline, so that case runs in process: a guard whose deadline has already elapsed, over a native stand-in whose calls
never settle, misses, says one line naming the bound, and asks nothing more. A real native lookup held past an elapsed
deadline and then answered with a real native artifact leaves the task's own entry intact, while the same native call
given the cache directory overwrites it. Drop the hunk, and keep the test, once the pinned upstream version treats an
unreachable cache as a miss ([nrwl/nx#36651](https://github.com/nrwl/nx/pull/36651) proposes it, unmerged).

## A shed reads the remote cache and never stores

Stock Nx 23.2.1 has no read-only self-hosted cache: its `store` builds the gzip archive before the PUT. Every cowshed
workspace's environment sets `NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY=true` beside `NX_SELF_HOSTED_REMOTE_CACHE_SERVER`
(`specs/cowshed/05_gateway.md` "`/nx/v1/cache/{hash}`"; the owner's rulings, 2026-10-10 and 2026-10-25). `getHttpCache`
then wraps the guard in `ReadOnlyRemoteCache` (`tasks-runner/remote-cache-outage.js`), whose `store` answers false
before building anything. Only a land publishes, with the native client directly. The test "a run that reads the remote
cache and never stores" in `packages/nx-plugin/src/nx-remote-cache-outage.test.ts` runs a cacheable task against a
server that answers 404 and requires only GETs, no PUT. Drop the hunk once upstream Nx has a read-only self-hosted
option.

## Non-workspace file packages

Bun's `file:` protocol identifies an installation source, not workspace membership. The pinned Nx parser keeps a
non-workspace file package as an external node, with its consumer dependency edges. A resolved file path is a workspace
only when its normalized lockfile-relative target names a declared workspace root; ordinary `workspace:` resolutions
remain workspace packages. This keeps copied local packages visible without inventing registry packages or converting
their consumers to workspace dependencies.

`packages/nx-plugin/src/bun-file-packages.test.ts` runs the real installed parser and native hasher against a locked
file package, its consumer and a distinct actual workspace. It requires the external file node and consumer edge, while
the workspace stays out of the external-node set. Normal nx-plugin test shards enroll the control. Regenerate and
activate the carried patch through the owning flow above; do not repair graph coverage by weakening a scope check or
deleting a runtime input.

## Dependency-graph resolution cache identity

The cached dependency edges of an unchanged file are reusable only while its resolution context is unchanged.
`createProjectFileMapCache` records project roots and JavaScript package-resolution metadata: package name, version,
exports, main entry and package-manager workspace membership. `shouldRecomputeWholeGraph` compares that snapshot,
including added or removed projects. Enumeration order is retained because Nx's package-name map is last-writer-wins and
wildcard export lookup uses the first matching entry. Unrelated target and tag changes do not invalidate this dependency
cache. Added TypeScript path aliases also invalidate it, not only changes to previously cached aliases.

A legacy file-map cache without the snapshot is recomputed once through normal graph construction. Nothing deletes,
resets or redirects the task database or cache entries. This fixes a missing-input class where a package's inferred
identity changed while an importing manifest's bytes stayed identical, so live resolution found a dependency but the
persisted graph continued to omit it.

`packages/nx-plugin/src/nx-project-resolution-cache.test.ts` uses the installed Nx cache writer, invalidation decision
and target locator. It proves the missing-edge transition with unchanged file hashes, every resolution field, membership
and collision order, legacy recovery, alias additions, and retention across unrelated target/tag edits. Drop these
carried hunks when the pinned upstream version includes the repair; retain the regression.

## Plugin-load error causes

Both configured and default plugin-load aggregation include each error's message, its complete `cause` chain and every
nested `errors` entry. Local-plugin resolution first reads project configurations, so a duplicate project name can fail
before any plugin runs. The non-verbose diagnostic must retain the duplicate name and both project roots through
`LoadPluginError` and `ProjectConfigurationsError`, not stop at their wrapper messages.

Serialized worker errors retain their `cause` and `errors` when normalized to an `Error`. Formatting follows those same
fields on native and serialized errors; it reports a circular reference without dropping sibling errors, and shared
errors in separate branches remain visible in each branch. It does not dump project configuration objects or require a
stack trace to explain a configuration failure.

`packages/nx-plugin/src/nx-plugin-load-errors.test.ts` drives the actual installed Nx in a fixture containing two
same-named projects and a local plugin, then native, serialized and circular plugin-load failures. Drop this hunk only
when the installed upstream version preserves these diagnostics, retaining the regressions.

## Project-graph error causes

The project-graph error hunks carry [nrwl/nx PR #37103](https://github.com/nrwl/nx/pull/37103) (`a935bb7d0af8`, not in
23.2.1) to the pinned distribution. Stock 23.2.1 `handleProjectGraphError` printed only
`Failed to process project graph.` and `Pass --verbose to see the stacktraces.` for every command that exits on a graph
error (`run-many`, `affected`, `exec`, `release` …): a consumer's CI failed that way for days, and only a
`NX_VERBOSE_LOGGING=true` reproduction named the createNodes refusal. `formatProjectGraphError` now serves both that
handler and `handleErrors`: without `--verbose` the body names every underlying error, then the hint.

Beyond upstream, the body uses the plugin-load formatter, moved to `error-types.js` as `formatErrorMessages` and shared
by `get-plugins.js`: each error's message, its complete `cause` chain and its nested `errors`, circular references
named. An `AggregateCreateNodesError` message already lists its entries (`formatAggregateCreateNodesError`, which now
formats each entry the same way), so they are not repeated. Verbose stack lines include each entry's cause.

`packages/nx-plugin/src/nx-project-graph-errors.test.ts` drives the actual installed Nx: a local plugin whose
createNodes throws an error with a cause, under `run-many` and `show projects`, in-process and isolated. The non-verbose
output must name the message and its cause without stack frames; verbose keeps frames and the chain. Drop the carried
hunks when the installed upstream version contains #37103, retaining the regression and the cause chain.

## Event-driven daemon validity

The daemon-validity hunks adapt [nrwl/nx PR #37113](https://github.com/nrwl/nx/pull/37113)
(`5812d81d9b00818c00b4a2022f04c998bd1fa381`, addressing [#37112](https://github.com/nrwl/nx/issues/37112)) to the pinned
Nx 23.2.1 distribution. Root lockfile changes restart the daemon only after the native workspace context has hashed the
watcher batch and dropped same-byte rewrites. The startup lockfile baseline is captured before watch/scan. Nested
lockfiles are not workspace lockfiles. The owner-record watcher retires a displaced daemon without waiting for a poll.

Nx 23.2.1 has neither the upstream `ChangeBatch` API nor its `rescan` event. This adaptation checks validity once per
request, including requests on an existing connection, and retains a 5000ms unref'd safety interval for missed events
and socket loss. There is no 20ms validity loop. Each check preserves fresh installed-version resolution and the
daemon's captured socket inode/path: a package-link swap, replaced ownership record or replaced socket cannot leave the
old daemon serving requests or deleting its successor's socket. Root lockfile names as well as bytes enter the fallback
hash, so a missed rename between lockfile formats is visible. A termination latch prevents duplicate replacement spawns
and serving queued requests during asynchronous restart.

The installed-version owner reads the literal `node_modules/<package>/package.json` through each `getNxRequirePaths()`
directory, in order, so the filesystem follows the current link on every read. Node's module resolver can retain stale
realpaths across package-link swaps even after `Module._pathCache` is cleared. Only non-node_modules/PnP layouts fall
back to the existing detached, cache-shielded resolver. Invalid metadata does not fall through to a different installed
copy.

A plugin `nx.json` names whose package resolves outside the workspace, a developer link into another checkout, changes
behind that link with no workspace lockfile change. The daemon then kept serving the graph the old plugin code inferred,
so every task it hashed afterwards was keyed on a project configuration no fresh process computes: a consumer's land
gate keyed `cargo-lint-cross` on the configuration this plugin inferred before a mid-gate land, and the push that
followed missed. The same baseline captures, before watch/scan, every file npm would publish of such a plugin and the
lockfiles of the nearest directory above it that holds one, keyed by plugin and package-relative path. npm's own reading
decides what is published: npm-packlist, resolved from that checkout, reads `files` with its negations and globs as
`npm pack` does, so a test the package keeps beside its sources but does not publish never restarts a consumer's daemon.
A package whose manifest does not parse, or whose checkout installs no npm-packlist, is judged whole, and the daemon log
says so once. A request resolves each plugin's link, stats the files already known at the link's current target and
re-hashes only one whose stat moved; a moved link, and at most once per safety interval anyway, starts the full census
in the background, which finds a file added beside them and restarts the daemon when a file changed. Only changed bytes
restart the daemon (`LINKED_PLUGIN_CHANGED`), so a rebuild that rewrites identical files, or a link moved to a copy with
the same bytes, keeps it. A plugin package installed inside the workspace changes only with its lockfile and is not
watched this way. A local plugin — one whose entry resolves to source inside the checkout, outside `node_modules` —
reloads its worker instead of the daemon:

A local plugin is always loaded in a plugin worker, even under `NX_ISOLATE_PLUGINS=false`, because a module loaded into
the daemon's own process cannot be evicted: purging `require.cache` does not unload an ESM module, so an in-process
local plugin would keep serving the code it first loaded after its source changed. Installed plugins keep whatever the
isolation setting already gives them. For a local plugin there is no in-process fallback: a worker that cannot start or
be reached fails the load with the existing worker-startup error, because an in-process copy could never be evicted and
would silently break the reload this section promises. An installed plugin keeps its existing fallback. Before it loads
the plugin, the worker registers a Node load hook and records every file under the checkout the loader actually loads
for it — the entry and its transitive static, lazy and computed dynamic imports, CJS and ESM alike, `node_modules`
excluded — with a digest of each source as loaded. That record is the plugin's closure: no static import walk re-derives
it, and nothing hashes the repository. When a closure is first registered, the daemon compares each digest against the
file's current bytes once, so a file changed between the worker loading it and the registration is already stale instead
of being served until its next change. From then on, every watcher batch reaches the decision already content-filtered,
the native workspace context having dropped same-byte rewrites when it hashed the batch. The changed paths are
intersected with each local plugin's closure — a pure decision, which changed paths reload which plugin — and only a
plugin whose closure they touch has its worker retired and reloaded and the cached plugin set's identity generation
advanced, so the recompute that batch already schedules builds the graph from the new code. A closure member under an
ignored path never reaches that filtered batch, so an observed member like it is judged on the raw watcher stream
instead: the event's file is compared with the digest recorded when the worker loaded it, the same bytes keep the
worker, and changed bytes retire it and invalidate the graph. Only ignored closure members are judged this way, each
against its own recorded digest; no other file is hashed for it. The daemon itself is not restarted and keeps its
process; every other plugin keeps the worker and state it had. Bun has no module customization hooks, so a daemon whose
CLI runs under Bun spawns these workers with the `node` on PATH rather than its own executable.
`nx-daemon-validity.test.ts` walks the reload end to end in one daemon: a CJS and an ESM local plugin whose helpers
change, and a third local plugin that must not reload.

A `REQUEST_PROJECT_GRAPH` failure, including a plugin that fails to load, is a nonterminal error value: the daemon
answers it with the shared `respondWithError` helper and keeps serving. A developer correcting a local plugin must not
pay a daemon restart or discard peers' graphs. A later source change retries graph preparation; the failed load never
substitutes the previous plugin's graph. All other daemon and protocol errors retain their existing exit policy. The
same regression refuses a local helper, repairs it, and observes its new target from the same daemon pid while an
unaffected plugin's worker remains loaded.

Measured on one unchanged consumer with three local graph plugins, three cold-daemon requests and their following warm
recomputes per arm: median `loadSpecifiedNxPlugins` is 315.9 ms in-process and 662.6 ms in workers (346.7 ms more cold
setup). The largest logged `total for creating and serializing project graph` per sample has cold medians 3389.1 →
6281.6 ms and warm medians 7477.1 → 14147.7 ms. End-to-end client wall medians are cold 10274.1 → 7325.3 ms and warm
17060.2 → 15452.7 ms. These are busy-host observations, not a speedup claim: overlapping graph requests share fixed
performance marks, so the daemon's numbers are not isolated build clocks. No warm sample starts another plugin worker or
reloads the specified plugin set; the added worker-loading cost is paid at cold load or source invalidation.

Each restart (`LOCK_FILES_CHANGED`, `LINKED_PLUGIN_CHANGED`) belongs to the process that started the daemon. A daemon
started as `start.js --kept`, cowshed's keeper job (`specs/cowshed/04_sandbox.md`), stops instead of spawning its own
successor, and the keeper's next probe starts one inside its job; Nx clients waiting on it after a lockfile change
reconnect within their 60 s poll and resend, while a linked plugin's change ends their commands
([A lost daemon ends the command](#a-lost-daemon-ends-the-command)). Read once at boot from argv, which no client's
environment can rewrite. Any other daemon restarts itself as before.

The existing ignore-file content snapshot, watch-before-scan ordering, exclusive daemon claim and five-minute inactivity
policy remain unchanged. This follows `specs/cowshed/04_sandbox.md`'s fixture ownership and one-state-per- checkout
contract and `packages/nx-plugin/README.md`'s runtime-state exclusions and daemon policy. `nx-daemon-validity.test.ts`
exercises the installed product's lockfiles, metadata, ownership and socket lifecycle; the existing claim, ignore-file
and idle-policy regressions remain required. The opt-in `nx-daemon-idle-cpu.test.ts` measures an owned idle daemon using
the kernel's cumulative CPU time, without a profiler or writes during the 30-second window. Run it through the host-only
process-census lane:

```sh
direnv exec <shed> nx run nx-plugin:test-shard1 \
  --command="NX_DAEMON_CPU_PROOF=true bun test --timeout=120000 src/nx-daemon-idle-cpu.test.ts" \
  --forwardAllArgs=false
```

Drop these hunks only when the installed upstream version has the event-driven checks, retaining behavioral and idle CPU
evidence. This is a carry of the existing upstream PR, not a new upstream issue.

## A job's client never starts a daemon

A stock client that finds no live daemon starts one, detached into a session of its own (`setsid`), and the record
shares it with every client of the checkout. Cowshed ends a job's whole process tree when the job ends, so a daemon one
job's client started would end with that job while another job's `nx run` still used it. Cowshed therefore gives every
job `NX_DAEMON_START=never` (`specs/cowshed/04_sandbox.md`). Under it, `enabled()` uses the daemon only while the
process its record names runs. Otherwise the command runs without one, as `NX_DAEMON=false` would. The daemon can stop
after the client chose it, or restart itself under a request and end its socket; its successor belongs to its starter
(cowshed's keeper, or the restarting daemon). A request in flight first waits the client's reconnect window for a
successor and resumes on it. When there is none, or when a request finds the daemon gone, the client turns the daemon
off for its own process alone (`disableDaemonForThisProcess`, never the `disabled` marker that only `nx reset` clears),
and fails the request with an error tagged `daemonStartForbidden` that names the variable, the connection failure as its
cause. Every daemon request of a command continues from that error in its own process through
`DaemonClient.orWithoutDaemon`, which notes the stop once per process and lets the connection go, so the caller's own
in-process path answers: the project graph, task hashing (`DaemonBasedTaskHasher` builds the graph in this process once,
for the in-process hasher's file data), output-hash record and compare (the outputs stamps below), the pre/post
tasks-execution hooks, sync generators and the workspace-context file queries. A run whose daemon stops under it
therefore finishes. The client never calls `startInBackground` implicitly, and an explicit `nx daemon --start`
(`command-line/daemon/daemon.js`), which would call it directly, is refused as policy: exit 1 with a reason naming the
variable and the test fixture escape. Without the variable nothing changes.

A daemon that restarts itself opens the log file twice for its successor's stdout and stderr. The restart closes both
descriptors right after the spawn, which keeps duplicates for the child, as the client's own start already did: left to
garbage collection, Node >=26 turns a collected `FileHandle` into a fatal `ERR_INVALID_STATE`, which ended the
restarting daemon with the socket its clients were still writing to.

`nx-daemon-start-never.test.ts` runs the installed Nx against a fixture with `NX_DAEMON=true`:

- with no daemon, the command succeeds and leaves no record;
- the same client without the variable starts one (the control);
- a running daemon is used, and the record still names it;
- a killed daemon's stale record starts nothing;
- a record naming a live process that serves no socket: the command notes the variable and continues without a daemon,
  the record is untouched, and no `disabled` marker is written;
- a daemon a task kills mid-run: the run finishes, notes the variable once, starts no successor and writes no `disabled`
  marker;
- a daemon that restarts itself mid-run (a task writes a lockfile): the run finishes on its successor;
- the restart's successor spawn, run under Node with garbage collected right after it (`startNewDaemonInBackground`,
  exported for this), exits cleanly with no `ERR_INVALID_STATE`;
- `nx daemon --start` under the variable exits 1 naming it and the fixture escape, and leaves no record; without the
  variable it starts a daemon.

Drop the hunk only when cowshed no longer ends a job's tree around a shared daemon, or upstream Nx has an equivalent
setting; retain the regression. Stock Nx starting the daemon is not enough; see
[Can the daemon hunks be dropped?](#can-the-daemon-hunks-be-dropped).

## A cache hit keeps the outputs a run left, without a daemon

Stock Nx keeps a cache hit's on-disk outputs only when its daemon recorded them (`outputsHashesMatchBatch`); a client
with no daemon "can't verify on-disk outputs, always copy". A job's client starts none (above) and main keeps none, so
every cached task with outputs was restored on every run, and the restore-time stamp re-dated each restored file. A
producer that runs another task's output as its tool (`build-inputs:scriptc-*`, `build-inputs:tsc-js`) then saw its tool
rewritten between capture and the end of every run, and refused its receipt (`producer-receipts.ts`), so no checkout
could capture a snapshot from a producer basis.

`tasks-runner/outputs-stamp.js` records, per task hash, the files a run or restore left in its outputs: path, inode,
size, mtime and ctime, in `<workspace data>/outputs-stamps/<hash>.json`, written whole by a rename. It is recorded with
or without a daemon (`recordOutputsHashBatch`), and verifies a hit whenever no daemon answers
(`shouldCopyOutputsFromCacheBatch`, its daemon-gone fallback included). A hit keeps outputs that are still exactly those
files; any write since changes the ctime, which no tool sets back, a replacement changes the inode, and an added or
removed file changes the set, and each of those restores.

`nx-cache-kept-outputs.test.ts` runs the installed Nx without a daemon: a second run keeps the very files the first
left, a third keeps them again, an edited output restores and the restore is kept next, and a removed output restores.
Drop the hunk, and keep the test, once the Nx in use verifies on-disk outputs without a daemon.

## The longest chain starts first, past a full slot count

Stock Nx orders ready tasks by how many tasks depend on them directly, then by project dependencies and history, and
starts nothing while `--parallel` workers run. A gate's critical chain therefore started behind leaves with more direct
dependents, and its next task waited for a slot while tests running at 0.1–0.3 cores held all of them: 119–320 s per
gate, measured on the 2026-10-27 land gates.

`tasks-schedule.js` computes each task's critical path once per run: its estimated duration from Nx's task history (plus
one, so a task with no history still counts as a step) and the longest critical path among the tasks that depend on it.
Ready tasks are sorted by it, Nx's order breaking ties. `nextCriticalTask` answers the ready task heading the run's
longest remaining chain, or nothing while a running task or one not yet ready heads a longer one.

`task-orchestrator.js` dispatches that task as one extra worker when every slot is held, never more than one beyond
`--parallel`. The extra worker exists because the jobserver is the CPU admission: cowshed gives every job a fifo with as
many slots as the host has cores, served by its supervisor, and the kernel schedules what it admits (05_gateway.md,
"Parallelism inside a job", "Host scheduling"). An Nx slot is a second admission, and holding the chain behind it buys
no CPU anyone else uses.

`nx-critical-path-schedule.test.ts`: with `--parallel=1`, the head of a four-task chain starts before a task with two
direct dependents that heads a chain of two; and `nextCriticalTask` offers nothing while the running head of the longest
chain runs, and offers the next head once that chain's predecessor ended without freeing a slot. Drop the hunks, and
keep the test, once the Nx in use schedules by critical path.

## A lost daemon ends the command

A command's project graph, and every task key it computes from it, comes from the plugin code its daemon loaded. When a
plugin linked from another checkout changes its bytes under a run, the daemon restarts (`LINKED_PLUGIN_CHANGED`, above),
and its successor, or the client's own process, would load the new code: the rest of the run would be keyed by a graph
it was not planned by, the "same tree, two keys" defect the restart exists to prevent. That happens in a consumer's land
gate whenever a land in the plugin's checkout rebuilds it mid-gate. The run cannot keep the plugin it started with,
since no process still runs that code, so it fails and names the change. Before it stops for that reason, the daemon
answers every connection a command holds with an error tagged `daemonGraphChanged` that names the daemon's pid,
`LINKED_PLUGIN_CHANGED` and the changed file, idle connections included. File-watcher and project-graph-listener streams
are excepted: their clients follow the successor and see the change there. The client fails the request waiting on it
and every later request of the command at once with that error. This holds under `NX_DAEMON_START=never` as well,
because a job's in-process continuation would load the changed plugin too. A lockfile restart still resumes on its
successor.

A daemon can also be lost under a request with nothing to resume on, for example when it is stopped by SIGTERM and no
keeper or restart starts a successor. The client waits its reconnect window (6000 polls 10 ms apart; a refusal ends the
wait early) and then fails that request. Stock 23.2.1 left the client `CONNECTING` with its ready signal never given, so
every later request waited for a reconnect that had already failed. pid 23683, a consumer's land gate on 2026-10-25, sat
for 2 h 48 min after its daemon's SIGTERM with no task running, while its output-lease helpers kept the process alive.
`DaemonClient.loseDaemon` now ends this process's use of the daemon in one place. The waiting request, any request
parked on the reconnect, and every later request fail at once with the loss, including a version mismatch found on
reconnect. A job's client gets the loss tagged `daemonStartForbidden` and continues in its own process, as the previous
section specifies; a host shell's client fails.

`nx-daemon-validity.test.ts` runs the installed Nx against a fixture whose plugin is linked from outside it. `p:build`
rewrites the plugin with other bytes, and `run-many` must exit nonzero, naming `LINKED_PLUGIN_CHANGED`, without building
`q` and within half the reconnect window. It runs once on a self-restarting daemon under a host client and once on a
kept daemon, whose keeper is not running, under a job client. Before this hunk the first run built `q` on the new plugin
and exited 0. A second case stands a socket server in for a daemon that ends the connection of the client's first
request, then a successor that refuses the client. All three of the client's sequential requests must fail, the third
with the loss. Before, the request after the failed reconnect never settled, and the client exited with it pending.

A daemon can be gone while its socket still takes requests: a process that outlived it, such as a child that inherited
the listener, accepts every connection and answers none. Its socket never closes, so no loss arrives, and stock 23.2.1
waits out its 20-minute keep-alive. A client on 2026-10-25 waited at no CPU for a daemon whose process no longer
existed. While a request waits, the client now checks every 5 s (`DAEMON_LIVENESS_MS`) that the pid its record named
when the request began still runs. A pid that does not run loses the daemon with an error naming that pid and the socket
(`daemonGone`). Under `NX_DAEMON_START=never` the error is tagged `daemonStartForbidden`, and the request continues in
its own process. The file-watcher and project-graph-listener streams take the same check from when they connect. Once
their daemon is gone, each callback gets that error, and the stream is let go. A file watcher then gets `closed`, which
`nx watch` exits on. A project-graph listener gets the error alone, because its consumer debounces its callback and
keeps only the last call. A record that names no pid arms no check (smoothbricks `specs/cowshed/05_gateway.md`,
"Deadlines"). A third case of `nx-daemon-validity.test.ts` binds a listener that never answers on the recorded socket,
under a record naming a pid that has ended. The client's request must fail with that pid and socket named; before this
hunk it waited out the keep-alive.

## Package-manager runs keep the daemon's graph

A client a package manager runs (`bun nx`, `bun run`, `npm run` …) sends the daemon a PATH with `node_modules/.bin`
directories in front, for the run's directory and each ancestor, and from Bun a NODE naming the `node` it found on PATH
(or, with none there, a temporary directory whose `node` is Bun, also put first on PATH). A plain `nx` client of the
same shell sends neither. Stock 23.2.1 counted both as project-graph identity, so every switch between the two kinds of
client discarded the daemon's graph and computed it again: on a consumer's dev boot about 2.6 s and a fresh task hasher
per switch, with all 47 task hashes the same under both environments.

`normalizeDaemonEnvironmentForGraph` now leaves that scaffolding out, beside Yarn Berry's per-invocation bin folder.
Where an absolute PATH entry lies is judged on its spelling and on its real path, so a link inside the workspace (its
own `node_modules` linked to an install elsewhere) is the workspace's. A `node_modules/.bin` entry goes when it lies in
the workspace, whose lockfile identifies the installed packages it holds (the daemon already restarts when that
changes), or when it lies in or above the workspace and is proved absent (ENOENT or ENOTDIR), so it resolves nothing.
NODE goes only when it is the `node` a lookup of that PATH resolves (the first PATH directory holding an executable
`node`), so PATH already carries the choice; Bun's `bun-node-<hash>` shim directory leaves PATH with it only when its
`node` is that one. Another checkout's `node_modules/.bin` (present or absent), an entry with `.` or `..` segments (the
kernel resolves `..` after a link, which a runtime's realpath may collapse lexically first, so no lookup can place it;
package managers write normal form), an existing one above the workspace, one whose lookup fails otherwise (EACCES,
say), a relative one, any other PATH entry, the order of the rest, and a NODE that names another binary or directory
stay identity. The filesystem is read once per env and the decision is a function of those facts. The daemon still
applies the whole env, so workers run with the scaffolding a client sent. `hashDaemonClientEnv`, which keys a plugin's
cache of what it inferred, hashes the same normalized env. From a directory without its own `package.json`, Bun also
puts that package's root on PATH; that entry is no package-manager directory by these rules, so such a client still
recomputes once.

`packages/nx-plugin/src/nx-daemon-env-identity.test.ts` alternates `bun nx` and plain clients against a real fixture
daemon and requires its log to keep the graph while naming PATH among the runtime keys it applied; a PATH directory no
package manager added must still recompute. It also checks the installed normalization against real directories and
`node` binaries: Bun's PATH shapes (nested runs, the shim), an alternate or off-PATH NODE, another checkout's package
bin directory present, absent or spelled through the workspace with `..`, an unsearchable one, and the plugin digest in
a child process. In a child whose workspace is a fixture, a bin directory reached through a workspace link and `..` must
stay, one above the workspace must go while proved absent and stay once a lookup into it is refused (EACCES).

## A refused daemon stop is a failure

Stock `DaemonClient.stop` sent SIGTERM to the pid the daemon record names and, whatever the kernel answered other than
ESRCH, printed the error, deleted the daemon's socket directory and let `nx daemon --stop` print
`Daemon Server - Stopped` and exit 0. A cowshed job may signal only its own sandbox's processes
(`specs/cowshed/04_sandbox.md`, `(allow signal (target same-sandbox))`), so its stop of the shed's kept daemon got
EPERM, the daemon kept running, and the job was told it had stopped. Deleting the socket of a daemon that still runs
hides it from the checkout's clients, and the next one starts a second daemon for the checkout's one Nx state
(`specs/cowshed/16_build_volumes.md`, "Every checkout has exactly one Nx state"). That daemon is its keeper's to stop.

The stop now throws for any refusal other than ESRCH, naming the pid and the errno, and keeps the socket directory.
`nx daemon --stop` exits 1 through `handleErrors` without the success line; `nx reset` reports it among its errors,
`nx add` fails. Cowshed stops the recorded daemon from its host controller before admitting the task-hash probe and
skips that root's selection if the native stop fails. The sandboxed probe computes locally and sends no signal,
including when the keeper starts a replacement during admission. A recorded pid that has exited is still a stopped
daemon.

`packages/nx-plugin/src/nx-daemon-stop-host.test.ts` runs in `nx-plugin:test-host`: on macOS it starts a real fixture
daemon and stops it from a Seatbelt profile with the job sandbox's signal rule, which `sandbox_apply` refuses inside
another sandbox. The daemon must keep running and answering on the same record and socket. On every platform it stops a
record naming pid 1, a live process of another user, and one naming an exited process, which still stops.

## A stopped watcher stream is never served from

A daemon's file watcher can stop delivering without a word. Nx 23.2.1's native watcher drops FSEvents' rescan and
dropped-event flags, and an FSEventStream fseventsd stopped serving (a restarted fseventsd, a stream refused at start)
reports nothing. The daemon then hashed tasks, graphs and output verdicts from the files it last saw: a shed's daemon
received no event for 33 minutes after a host-side stop, and `nx run-many` called a changed input's target
`[existing outputs match the cache, left as is]`, so the tests ran the old build.

The daemon now watches the checkout with one native watcher (`daemon/server/watcher.js`), not one for sources and one
for outputs: FSEvents orders events within one stream only. The native filter keeps Nx's hard-coded ignores; the
workspace view applies the ignore files Nx's native workspace watcher read, in its order (`watch_filterer.rs`): the root
`.nxignore` first, whose ignore or re-include decides; then the `.gitignore` files from the event's directory up to the
workspace root, then those between a nested workspace and its git root, the nearest opinion deciding. A path under a
directory those rules exclude is excluded, as the walker never enters it. A root `.nxignore` `!p/keep.txt` therefore
keeps a file `p/.gitignore` ignores, and a nested workspace's `!keep.txt` keeps a file its parent repository's `*.txt`
ignores, as watcher and walker do. A nested `.nxignore`, which only the walker reads, is not read, as Nx's watcher never
read it. A change to one of the files read stops the daemon, as before. The daemon record and the outputs tracking take
every event, as the outputs watcher did.

On macOS the first request of a connection that is answered from the workspace's files (everything but liveness,
shutdown, subscriptions and the Nx Console and AI agent status) writes an empty probe, `nx-watch-probe-<pid>-<n>`, and
drains the stream until the probe comes back. The probe lives in the first of the workspace data directory, `.git` and
`node_modules` that lies physically inside the checkout (a cowshed shed links its workspace data onto another volume)
and whose probe path the root `.nxignore` does not ignore: the native watcher applies that file to every event, and a
workspace that ignores every dot path (`.*`, as this repository does) hides `.nx` and `.git`. Where none qualifies (a
linked `node_modules` under such an `.nxignore`, say), nothing can prove the stream current: the daemon answers the
first file-state request with the reason, tagged as below, and stops. Every write made before the nx command reached the
daemon by then. Requests that arrive while a probe is out share the next one. A plugin worker's request, which its
daemon client tags `fromPluginWorker`, makes no probe: a worker is no command, and its request nests in a daemon
operation already current, the watcher's own recompute, which must read that recompute's files, or a command whose
connection made its probe. Each worker connection's probe cost an isolated graph plugin a round trip through the stream
(56 ms in the regression's fixture). The nx-plugin's producer census records `node_modules` membership without the probe
(`packages/nx-plugin/README.md`, producer receipts), so a probe during a producer run is no tool change. A probe that
has not come back after `DAEMON_VALIDITY_SAFETY_INTERVAL_MS` means the stream stopped: the daemon replies with an error
tagged `daemonStartForbidden` (the client's "continue without this daemon in this process" tag), so the command goes on
without a daemon at once (the client turns the daemon off for its own process, `disableDaemonForThisProcess`, so no
later request of the command dials the stopping daemon), then logs `watcher stream stopped` and stops. It stops rather
than restarts: a successor it spawned would inherit the cause. The next client, or the shed's keeper, starts one with a
fresh stream. Later requests on the same connection are served as Nx always served them, so a write another process
makes while a command runs is as visible as it was. Linux inotify has no daemon between the kernel and the watcher that
can stop serving it, and Nx registers no watch under the hard-coded ignores where a probe could live; there the daemon
only drains its watcher, as Nx did before serving a graph, and the inotify overflow (Rescan) path stays unproven.

Measured on macOS, a 47-task chain of cache hits: stock 0.51–0.82 s per run; patched 0.79–0.96 s, one probe of 63–103 ms
per command. A probe before every file-state request cost 51 probes of 65–78 ms, 3.3–3.9 s per run, and was dropped. A
burst of 20000 gitignored and 2000 tracked file writes cost the daemon 810–910 ms of CPU against stock's 1040–1260 ms:
one native stream instead of two.

`packages/nx-plugin/src/nx-daemon-watch-stream-host.test.ts` runs in `nx-plugin:test-host`. On every platform a real
fixture daemon rebuilds a target whose input was replaced by rename. On macOS a daemon started under a Seatbelt profile
that denies FSEvents (which `sandbox_apply` refuses inside another sandbox) must tell the command
`watcher stream stopped`, which continues without it, log it, and never call the changed input's target cached; a
healthy daemon whose `.nxignore` hides every dot path must see its probe come back from a real `node_modules`; and one
with nowhere to put a probe must answer `cannot be proven current` and never serve a stale verdict. An isolated graph
plugin that asks the daemon for files must leave one probe in the daemon's log, its command's.
`nx-daemon-watch-filter.test.ts` (an ordinary shard) holds a nested workspace whose `.gitignore` re-includes `keep.txt`
under its parent repository's `*.txt`: a fresh walk indexes it, and the daemon must rebuild the target whose input it is
once it changes. Drop these hunks only when the installed upstream watcher reports a stopped stream, retaining the
regression.

## Can the daemon hunks be dropped?

No. Whether stock Nx could start the daemon itself, with cowshed only stopping the recorded pid at release, is answered
in `specs/cowshed/04_sandbox.md`, "Why cowshed keeps the daemon, not the first client". Its seven reasons are
containment (macOS and Linux), the untrusted record, land adoption, the sandbox profile, held-lock commands, racing
starters and the idle stop. Each hunk below serves some of them or is independent of who starts the daemon. Only
`--kept`, 11 of the patch's 3,809 changed lines, goes with the keeper. Were every kind of client allowed to start a
daemon, about 78 lines (2%) could go: the never-start policy (65), `--kept` (11) and the `never` tag on two lost-daemon
lines (`P:727`, `P:766`). A job whose daemon stops would then fail its command, as a host shell's does today
(`loseDaemon`, `P:476-492`): a choice, not a free deletion.

Every changed line (+3,133 −676, in 79 files) is counted once, under the purpose that decides whether it can go; a hunk
that mixes purposes is split by function. `P:n` is line n of `patches/nx@23.2.1.patch` as last changed by `446e01de66`,
and moves when the patch is regenerated.

| Purpose                           | Changed     | Serves                                                    |
| --------------------------------- | ----------- | --------------------------------------------------------- |
| Never-start policy                | +63 −2      | containment, record, land, profile, held-lock commands    |
| In-process continuation           | +99 −20     | the same, and the stopped watcher's reply to every client |
| Lost-daemon handling              | +130 −7     | none: defects of stock 23.2.1 that any client has         |
| Record claim and socket ownership | +326 −18    | racing starters                                           |
| `LINKED_PLUGIN_CHANGED`           | +206 −6     | none: a graph-identity defect, whoever started the daemon |
| Idle, refused stop and validity   | +741 −179   | idle stop, profile (refused stop)                         |
| Unrelated                         | +1,568 −444 | none                                                      |

- **Never-start policy** (`1b7475de52`, `9d44a660ef`): `nx daemon --start` refused (`P:78-92`); `enabled()` uses the
  daemon only while the process its record names runs (`P:420-428`, `recordedDaemonRuns` `P:858-866`);
  `startDaemonIfNecessary` never starts one (`P:609-619`); the tagged error `daemonStartForbidden()` and the tags on
  lost connections (`P:903-917`, `P:688-693`).
- **In-process continuation** (`cbf0b42671`): `orWithoutDaemon` and `continueWithoutDaemon` (`P:365-372`, `P:435-468`)
  and their callers: task hashing (`P:2941-2949`, `P:2999-3007`, `P:3019-3039`), the pre and post tasks-execution hooks
  (`P:3542-3556`), the project graph (`P:3583-3589`), output-hash record and compare (`P:4975-4987`), sync generators
  (`P:5425-5444`) and workspace-context file queries (`P:5604-5665`). The stopped watcher's reply carries the same tag
  to every client, host shells included (`P:1876`), so these stay whoever starts the daemon.
- **Lost-daemon handling** (`90dd74cfb7`, `2ed1e18cf5`, `8151e1c16d`, `446e01de66`): `loseDaemon` and the gates that
  read it (`P:411-412`, `P:476-492`, `P:627-629`, `P:700-703`, `P:735-737`); a failed reconnect or a version mismatch
  ends the use (`P:711-727`); the per-request pid check and `daemonGone` (`P:402-403`, `P:745-779`, `P:867-895`); the
  same check for watcher and graph-listener streams (`P:500`, `P:508-573`). They fix defects of stock 23.2.1 that any
  client has: a client left `CONNECTING` for good after a failed reconnect, and a 20-minute keep-alive behind a listener
  that outlived its daemon.
- **Record claim and socket ownership** (`8b1716430f`): the record as an exclusive claim (`daemon/cache.{d.ts,js}`,
  `P:124-149`, `P:159-344`; `claimDaemonOrDefer` and the reordered `startServer`, `P:1992-2025`, `P:2040`,
  `P:2046-2051`); the pid that answered, reported to the client (`P:802-808`); the daemon's own socket and inode
  (`P:2105`, `P:2836-2850`, `P:2862-2910`); a displaced or socketless daemon stopping itself (`P:1697-1706`,
  `P:1728-1731`, `P:1755-1757`); a shutdown that leaves a socket another daemon rebound (`P:2236-2242`, `P:2263`,
  `P:2268-2287`).
- **`LINKED_PLUGIN_CHANGED`** (`31dfd0f614`, `a19f960832`, `90dd74cfb7`): the census and hash of a plugin linked from
  outside the workspace (`P:1529-1687`) and its restart decision (`P:1741-1752`); the `daemonGraphChanged` notice to
  every connected command (`P:2221-2235`, `P:2243-2262`, `P:2265-2267`); the client that fails at once on it
  (`P:787-794`).
- **Idle, refused stop and validity**:
  - Idle (`a97f25840d`, `3becb87c55`): five minutes, and an open connection counts as activity (`P:1836-1838`,
    `P:1929-1934`, `P:2118-2119`, `P:2162-2163`; +8 −4). A plugin worker, or the daemon as its own client, lets go of
    its connection once its queue drains (`9646926b2c`, `40df71236d`; `P:636-680`, +30 −8). Both matter more once Nx's
    own timer is the only idle stop.
  - Refused stop (`3a4abc1969`): `DaemonClient.stop` throws on any refusal but `ESRCH` and keeps the socket, because
    another job's stop gets `EPERM` (`P:816-850`, +22 −10).
  - Event-driven validity (`90a278508e`): `restart-checks.{d.ts,js}` (`P:1443-1454`, `P:1461-1484`, `P:1491-1528`,
    `P:1688-1696`, `P:1720-1727`, `P:1732-1740`, `P:1753-1754`, `P:1758-1762`, `P:1771-1774`, `P:1780-1785`), its
    watcher-path check (`P:1412-1433`), its callers in `server.js` (`P:1794-1804`, `P:1828`, `P:1853-1857`,
    `P:1942-1972`, `P:2026-2028`, `P:2056-2101`) and the installed-version owner (`P:5382-5413`); +157 −75.
  - A workspace that has gone stops the daemon (`ee9c35a9fc`; `P:1707-1719`, `P:1980-1984`, +18).
  - A restarting daemon's successor keeps its own copies of the log descriptors (`cbf0b42671`; `P:2131-2132`, `P:2152`,
    `P:2160`, `P:2169-2177`, `P:2185-2207`, +25 −11): once the keeper is gone, Nx's own restart is the only restart.
  - A stopped watcher stream is never served from (`b84e602d05`): one native watcher, probed before the daemon answers
    from files (`watcher.{d.ts,js}`, `P:2308-2824`; `P:1763-1770`, `P:1844-1845`, `P:1861-1882`, `P:1906-1921`,
    `P:2033-2034`, `P:2102-2104`, `P:2106-2107`, `P:2122-2123`, `P:2144-2145`, `P:2215-2220`, `P:2291-2296`); +470 −71.
  - `--kept` (`822134f5b0`): `keptByLauncher` and its branch in `restartDaemon` (`P:1485-1490`, `P:1775-1779`, +11) make
    a keeper-started daemon stop instead of spawning a successor no ledger can prove. It is the one hunk that exists for
    the keeper, and serves containment and land adoption.
- **Unrelated**: everything else, none of it about who starts the daemon: the output lease, the remote cache hunks,
  environment graph identity, exit codes, error causes, the foreign workspace environment, task history on the client's
  connection (`cea3107da8`: the daemon picks its database scope once, at startup), the daemon's resident graph plugin
  workers (`2e204ae77c`), task-graph closure, the racy file-hash archive, dependency-cache identity, run information
  after a pre-start failure, resolution from the workspace, root normalization, raw-input inspection, `NX_USE_LOCAL` and
  Bun file packages.

## Canonical daemon workspace roots

The daemon and plugin-worker message guard compares canonical path strings. A redundant trailing separator does not make
a workspace foreign to itself, but a different workspace remains refused. The normalizer trims only past the path's
root: POSIX `/`, Windows drive roots and UNC share roots remain absolute, Windows case/separator rules remain intact,
and POSIX case and backslash filename components retain their meaning.

`nx-daemon-root-normalization.test.ts` consumes the actual Bun-installed patched Nx. It checks both platform rules and
drives an owned daemon RPC with the same root's trailing separator, then a genuinely foreign root, and verifies the
daemon remains available to its own workspace. No caller root override, shared-socket allowance or namespace workaround
replaces the guard. Drop this hunk only when upstream contains the same normalization, retaining the regression.

## The installed Nx answers, never `nx@latest`

Stock Nx answers the Nx Console and AI-agent status every `nx run` asks its daemon for by installing `nx@latest` from
the registry into a temporary directory, unless the daemon's environment says `NX_USE_LOCAL=true`; the daemon takes that
environment from whichever client spoke last. Agents, cowshed jobs, test fixtures and library clients (smoo, cowshed's
task hashing, `ensureBuilt`) run without the variable. The `latest-nx` hunk makes the daemon resolve its own installed
Nx instead, whatever a client sends, so it starts no package manager and asks no registry. The `bin/nx` hunk defaults
`NX_USE_LOCAL` to `true` for every command and the processes it starts, so `nx configure-ai-agents`, `nx init` and
`nx migrate` use the installed CLI; `NX_USE_LOCAL=false` restores the latest one for one command.

`nx-use-local.test.ts` starts a fixture daemon from a library client without the variable and then runs the bare
`node_modules/.bin/nx` entry against it, with every package manager on `PATH` replaced by one that records its run. The
daemon must take the branch that pulled `nx@latest`, pull nothing, and start no package manager.

## Editor probes only for a person at a terminal

Stock Nx's Nx Console check and its AI-agent configuration check run each editor's CLI from the daemon
(`code --version`, `code --list-extensions`, the same for `cursor`). The daemon starts both as it starts, every client
asks for the Nx Console status before its command and every `nx run` for the AI-agent status after its tasks, though
only a client on a terminal shows what either finds. So a daemon that only ever served cowshed jobs, gates or tests
forked editors, and one outlived the daemon it came from in its process group: a land gate's test shard held
`code --version` past its daemon's stop. The `server.js` hunk starts neither check at daemon start, and the
`init-local.js` and `run-command.js` hunks ask the daemon for them only when stdin and stdout (Nx Console) or stdout (AI
agents) is a terminal.

`nx-daemon-idle-policy.test.ts` starts a fixture daemon with `code` and `cursor` stand-ins on `PATH` that record any
run, serves `nx show projects` and `nx run` from non-terminal clients, stops it, and requires no `[NX-CONSOLE]` daemon
log line and no editor run. Stock Nx logs the check and runs all four probes.

## Run information after a pre-start failure

Stock Nx's discrete worker reports a hash failure through `endTasks` even when `startTasks` never ran. Its
`StoreRunInformationLifeCycle` dereferenced the absent timing entry and threw a `TypeError` over the original error. The
run-information hunk creates that entry only when absent, retaining the failed result and its completion time. An
execution start stays absent unless `startTasks` or the task's own `startTime` supplied one; no start is fabricated.

The composed lifecycle must also complete `endCommand`: native task history requires a hash and an execution start;
legacy history stringifies the start. Both exclude results lacking either, rather than inventing a history row or
crashing after the run information was saved. Genuine database failures for valid execution rows still propagate.
Optional timing diagnostics retain an unstarted result with a `null` duration; profiling emits no executed interval
without a real start. Supplied timestamps, including zero, and normal started tasks retain their timing.

`packages/nx-plugin/src/nx-run-information.test.ts` exercises the actual installed lifecycle with pre-start failures,
absent timestamps, supplied task timestamps and normal started/cache-hit controls.
`packages/nx-plugin/src/nx-failure-life-cycles.test.ts` composes it with native/legacy history and both optional timing
reporters, checks actual SQLite/CSV/profile output and includes a genuine native database-error control. Drop these
hunks only when the installed upstream package handles the same ordering, retaining the regressions.

## Requested task closure after dummy normalization

The `create-task-graph` hunk retains only the caller's initial tasks and the tasks reachable from them through the
normalized regular or continuous dependency edges. Expansion may discover a real producer behind a dummy cycle that
normalization later removes. Leaving that producer in `tasks` makes it an unrelated new root, which Nx executes even
though no requested task depends on it. On a model-command graph, fourteen such tasks pulled in complete role and native
builds; no dependency of the nineteen requested-closure tasks was changed.

The selection happens at Nx's graph-construction owner, not in a consumer cache probe or launcher. Explicitly requested
tasks, genuine producer edges, continuous-only dependencies and real task cycles remain intact. The existing
`nx-task-graph-cycles.test.ts` guards both this closure rule and cycle membership against the actual Bun-installed
patched package. Ordinary installation applies the current patch before test typechecking or execution; no publication
or generated product substitutes for the lock-selected implementation. Drop each carried hunk only when the installed
upstream version contains its respective repair, retaining the regressions.

## Upstream PR draft: Nx `findCycles`

Status: drafted, not opened. Written against nrwl/nx `master` at `a37b5ca4c6630bb9ea5df9ac0546edc3a186bc2f`, whose
`findCycles`, `filterDummyTasks` and `getNonDummyDeps` have the logic of Nx 23.2.1's. This repository carries the fix as
the `task-graph-utils` hunk of `nx@23.2.1.patch`; `packages/nx-plugin/src/nx-task-graph-cycles.test.ts` guards it here.
Drop that patch hunk once the installed Nx contains the fix, retaining the regression.

### Title

```text
fix(core): give a task the same dependencies whichever other tasks a run asks for
```

### Description

Follows the template of `.github/PULL_REQUEST_TEMPLATE.md`.

```markdown
## Current Behavior

When a target depends on `^build` through a project that has no `build` target, Nx makes a dummy task for that project
and later flattens every dummy into the real tasks behind it (`filterDummyTasks`), except a dummy that `findCycles`
reports as being in a cycle, which contributes nothing. #28793 added `findCycles` so the flattening ends when there are
several cycles. It has two defects, and both make its answer depend on the order of the graph's keys:

- it reports every task on the depth-first path that reached a cycle, including tasks that only _lead to_ the cycle and
  are not on it, so a dummy task in front of a cycle loses its dependencies;
- a traversal returns at the first cycle it finds, before it has looked at the rest of that task's dependencies, and
  everything it entered stays marked visited, so a later traversal stops at those tasks and a cycle through them is
  never reported (`a -> b`, `b -> a, e`, `e -> b` reports `a` and `b`, not `e`).

So the same task gets different dependencies depending on which other tasks the run asks for. In the reproduction below,
`app:inner` depends on nothing under `nx run app:inner` and on `core:build` under `nx run app:agg`, where `app:agg` is
an aggregate that also runs `app:inner`; and `app:agg` itself lacks the `core:build` it reaches through the same
project. Dependencies feed `dependentTasksOutputFiles`, so such runs also hash a task differently: measured on a
workspace of 7 projects that import each other, one task hashed 6788173338685328009 requested alone and
16936296670297497910 through `run-many`, the only difference being two `dependentTasksOutputFiles` nodes for a
dependency the lone request had lost, and a pre-run of one never hit the cache of the other. A dropped edge is also a
missing ordering: in the existing spec "app1:build -> app2 <-> app3:build" `app1:compile` depends on `app3` through
`app2`, yet runs with no dependency on `app3:compile`.

## Expected Behavior

`findCycles` returns the tasks that lie on a cycle: every task of a strongly connected component of more than one task,
and every task that depends on itself. That is a property of the graph, so a dummy task that merely leads to a cycle is
still flattened, and the answer no longer depends on the order of the keys or on the other tasks of the run. It is
Tarjan's algorithm: linear in the graph, iterative (no recursion, no copied paths), and `findCycle`, which wants one
cycle as a path, is unchanged.

One existing expectation changes: in "app1:build -> app2 <-> app3:build" `app1:compile` now depends on `app3:compile`,
as `lib2:build` depends on `lib4:build` through `lib3` in the neighbouring specs. The cycle there is `app3:compile` <->
its own dummy for `app2`; the dummy `app1:compile` reaches is in front of it, not on it.

## Related Issue(s)

Follow-up to #28793 (fixes #28788). No issue filed for this.
```

### Minimal reproduction

Three projects: `app` imports `mid`, `mid` and `core` import each other, `mid` has no `build` target. `app` has an
`inner` target and an `agg` target that depends on `^build` and on `inner`; `inner` depends on `^build`.

```ts
import { createTaskGraph } from 'nx/src/tasks-runner/create-task-graph';

const project = (name: string, targets: Record<string, string[]>) => ({
  name,
  type: 'lib' as const,
  data: {
    root: name,
    targets: Object.fromEntries(
      Object.entries(targets).map(([target, dependsOn]) => [target, { executor: 'nx:noop', dependsOn }])
    ),
  },
});

const graph = {
  nodes: {
    app: project('app', { agg: ['^build', 'inner'], inner: ['^build'] }),
    mid: project('mid', {}),
    core: project('core', { build: ['^build'] }),
  },
  dependencies: {
    app: [{ source: 'app', target: 'mid', type: 'static' }],
    mid: [{ source: 'mid', target: 'core', type: 'static' }],
    core: [{ source: 'core', target: 'mid', type: 'static' }],
  },
} as any;

const alone = createTaskGraph(graph, {}, ['app'], ['inner'], undefined, {});
const through = createTaskGraph(graph, {}, ['app'], ['agg'], undefined, {});
console.log('nx run app:inner  app:inner ->', JSON.stringify(alone.dependencies['app:inner']));
console.log('nx run app:agg    app:inner ->', JSON.stringify(through.dependencies['app:inner']));
console.log('nx run app:agg    app:agg   ->', JSON.stringify(through.dependencies['app:agg']));
```

Run against the registry's `nx@23.2.1` and against a build of `master` at the commit above, both print:

```text
nx run app:inner  app:inner -> []
nx run app:agg    app:inner -> ["core:build"]
nx run app:agg    app:agg   -> ["app:inner"]
```

and with the patch below, all three print `["core:build"]`, `["core:build"]` and `["core:build", "app:inner"]`.

What `filterDummyTasks` is handed (`dummy(x)` is `mid`'s dummy `build` task made for `x`, such as
`mid:appinner__build__nx_dummy_task__` for `app:inner`):

```text
nx run app:inner   app:inner -> dummy(app:inner) -> core:build -> dummy(core:build) -> core:build
nx run app:agg     app:agg -> dummy(app:agg), app:inner;  dummy(app:agg) -> core:build, app:inner
                   core:build -> dummy(core:build) -> core:build;  app:inner -> dummy(app:inner) -> core:build
```

The only cycle is `core:build` <-> `dummy(core:build)`. Starting from `app:inner`, the depth-first path is the whole
chain, so `findCycles` also reports `app:inner` and `dummy(app:inner)`, and `dummy(app:inner)` is dropped with
`core:build` behind it. Starting from `app:agg`, the traversal returns at the cycle through `dummy(app:agg)`, so
`app:agg` loses `core:build` through that dummy; it searches `app:inner` only afterwards, finds `core:build` visited,
and never flags `dummy(app:inner)`, which is flattened. Fixed, `findCycles` reports only `core:build` and
`dummy(core:build)`, in both.

### Patch

Against `packages/nx/src/tasks-runner/task-graph-utils.ts` (formatted with Prettier 3,
`--single-quote --trailing-comma es5`, which the unchanged files satisfy):

```diff
diff --git a/packages/nx/src/tasks-runner/task-graph-utils.ts b/packages/nx/src/tasks-runner/task-graph-utils.ts
index 51d1a22..26b859f 100644
--- a/packages/nx/src/tasks-runner/task-graph-utils.ts
+++ b/packages/nx/src/tasks-runner/task-graph-utils.ts
@@ -48,22 +48,76 @@ export function findCycle(graph: {

 /**
  * This function finds all cycles in the graph.
- * @returns a list of unique task ids in all cycles found, or null if no cycle is found.
+ * @returns the unique task ids that lie on a cycle (every task of a strongly
+ * connected component of more than one task, and every task that depends on
+ * itself), or null if there is none. A task that merely leads to a cycle is not
+ * on it. The result is a property of the graph, not of the order of its keys.
  */
 export function findCycles(graph: {
   dependencies: Record<string, string[]>;
   continuousDependencies?: Record<string, string[]>;
 }): Set<string> | null {
-  const visited = {};
+  const edges = (id: string): string[] => [
+    ...(graph.dependencies[id] ?? []),
+    ...(graph.continuousDependencies?.[id] ?? []),
+  ];
+
+  // Tarjan's strongly connected components, iterative so that a long chain of
+  // tasks cannot overflow the call stack.
+  const visits = new Map<
+    string,
+    { index: number; lowlink: number; onStack: boolean }
+  >();
+  const stack: string[] = [];
   const cycles = new Set<string>();
-  for (const t of Object.keys(graph.dependencies)) {
-    visited[t] = false;
-  }

-  for (const t of Object.keys(graph.dependencies)) {
-    const cycle = _findCycle(graph, t, visited, [t]);
-    if (cycle) {
-      cycle.forEach((t) => cycles.add(t));
+  const enter = (id: string) => {
+    const visit = { index: visits.size, lowlink: visits.size, onStack: true };
+    visits.set(id, visit);
+    stack.push(id);
+    return { id, visit, edges: edges(id), next: 0 };
+  };
+
+  for (const root of Object.keys(graph.dependencies)) {
+    if (visits.has(root)) {
+      continue;
+    }
+    const frames = [enter(root)];
+    while (frames.length > 0) {
+      const frame = frames[frames.length - 1];
+      if (frame.next < frame.edges.length) {
+        const dep = frame.edges[frame.next++];
+        const seen = visits.get(dep);
+        if (dep === frame.id) {
+          cycles.add(dep);
+        } else if (!seen) {
+          frames.push(enter(dep));
+        } else if (seen.onStack) {
+          frame.visit.lowlink = Math.min(frame.visit.lowlink, seen.index);
+        }
+        continue;
+      }
+
+      frames.pop();
+      const parent = frames[frames.length - 1];
+      if (parent) {
+        parent.visit.lowlink = Math.min(
+          parent.visit.lowlink,
+          frame.visit.lowlink
+        );
+      }
+      if (frame.visit.lowlink === frame.visit.index) {
+        const component: string[] = [];
+        let member: string;
+        do {
+          member = stack.pop();
+          visits.get(member).onStack = false;
+          component.push(member);
+        } while (member !== frame.id);
+        if (component.length > 1) {
+          component.forEach((id) => cycles.add(id));
+        }
+      }
     }
   }

```

Tests, with the one changed expectation:

```diff
diff --git a/packages/nx/src/tasks-runner/create-task-graph.spec.ts b/packages/nx/src/tasks-runner/create-task-graph.spec.ts
index 12f3abd..2333f75 100644
--- a/packages/nx/src/tasks-runner/create-task-graph.spec.ts
+++ b/packages/nx/src/tasks-runner/create-task-graph.spec.ts
@@ -2496,7 +2496,7 @@ describe('createTaskGraph', () => {
       }
     );
     expect(taskGraph).toEqual({
-      roots: ['app1:compile', 'app3:compile'],
+      roots: ['app3:compile'],
       tasks: {
         'app1:compile': {
           id: 'app1:compile',
@@ -2530,7 +2530,7 @@ describe('createTaskGraph', () => {
         },
       },
       dependencies: {
-        'app1:compile': [],
+        'app1:compile': ['app3:compile'],
         'app3:compile': [],
       },
       continuousDependencies: {
@@ -2540,6 +2540,79 @@ describe('createTaskGraph', () => {
     });
   });

+  it('should give a task the same dependencies whichever other tasks are requested (app:inner -> mid <-> core:build)', () => {
+    projectGraph = {
+      nodes: {
+        app: {
+          name: 'app',
+          type: 'app',
+          data: {
+            root: 'app-root',
+            targets: {
+              aggregate: {
+                executor: 'nx:run-commands',
+              },
+              inner: {
+                executor: 'nx:run-commands',
+              },
+            },
+          },
+        },
+        mid: {
+          name: 'mid',
+          type: 'lib',
+          data: {
+            root: 'mid-root',
+            targets: {},
+          },
+        },
+        core: {
+          name: 'core',
+          type: 'lib',
+          data: {
+            root: 'core-root',
+            targets: {
+              build: {
+                executor: 'nx:run-commands',
+              },
+            },
+          },
+        },
+      },
+      dependencies: {
+        app: [{ source: 'app', target: 'mid', type: 'static' }],
+        mid: [{ source: 'mid', target: 'core', type: 'static' }],
+        core: [{ source: 'core', target: 'mid', type: 'static' }],
+      },
+    };
+    const targetDefaults = {
+      aggregate: [{ target: 'build', dependencies: true }, { target: 'inner' }],
+      inner: [{ target: 'build', dependencies: true }],
+      build: [{ target: 'build', dependencies: true }],
+    };
+
+    const alone = createTaskGraph(
+      projectGraph,
+      targetDefaults,
+      ['app'],
+      ['inner'],
+      undefined,
+      { __overrides_unparsed__: [] }
+    );
+    const throughAggregate = createTaskGraph(
+      projectGraph,
+      targetDefaults,
+      ['app'],
+      ['aggregate'],
+      undefined,
+      { __overrides_unparsed__: [] }
+    );
+
+    // `app` reaches `core:build` through `mid`, which has no `build` target.
+    expect(alone.dependencies['app:inner']).toEqual(['core:build']);
+    expect(throughAggregate.dependencies['app:inner']).toEqual(['core:build']);
+  });
+
   it('should not conflate dependencies of dummy tasks', () => {
     projectGraph = {
       nodes: {
diff --git a/packages/nx/src/tasks-runner/task-graph-utils.spec.ts b/packages/nx/src/tasks-runner/task-graph-utils.spec.ts
index accf2bc..c893d2a 100644
--- a/packages/nx/src/tasks-runner/task-graph-utils.spec.ts
+++ b/packages/nx/src/tasks-runner/task-graph-utils.spec.ts
@@ -160,6 +160,81 @@ describe('task graph utils', () => {
         })
       ).toEqual(null);
     });
+
+    it('should not return tasks that only lead to a cycle', () => {
+      expect(
+        findCycles({
+          dependencies: {
+            top: ['x'],
+            x: ['y'],
+            y: ['z'],
+            z: ['y'],
+          },
+        })
+      ).toEqual(new Set(['y', 'z']));
+    });
+
+    it('should return a task that depends on itself', () => {
+      expect(findCycles({ dependencies: { a: ['a'], b: [] } })).toEqual(
+        new Set(['a'])
+      );
+    });
+
+    it('should return a cycle behind a task that was visited before', () => {
+      // The search from `a` stops at the cycle `a` <-> `b` before it looks at
+      // `b`'s other dependency `e`, and the search from `e` finds `b` visited.
+      // `e` is on a cycle with `b` too.
+      expect(
+        findCycles({
+          dependencies: {
+            a: ['b'],
+            b: ['a', 'e'],
+            e: ['b'],
+          },
+        })
+      ).toEqual(new Set(['a', 'b', 'e']));
+    });
+
+    it('should return a continuous cycle', () => {
+      expect(
+        findCycles({
+          dependencies: { a: [], b: [] },
+          continuousDependencies: { a: ['b'], b: ['a'] },
+        })
+      ).toEqual(new Set(['a', 'b']));
+    });
+
+    it('should return the same tasks whatever the order of the graph', () => {
+      const edges: Record<string, string[]> = {
+        a: ['b'],
+        b: ['c'],
+        c: ['b'],
+        d: ['c'],
+      };
+      for (const keys of [
+        ['a', 'b', 'c', 'd'],
+        ['d', 'c', 'b', 'a'],
+        ['c', 'a', 'd', 'b'],
+        ['b', 'd', 'a', 'c'],
+      ]) {
+        const dependencies = Object.fromEntries(
+          keys.map((key) => [key, edges[key]])
+        );
+        expect(findCycles({ dependencies })).toEqual(new Set(['b', 'c']));
+      }
+    });
+
+    it('should not overflow the stack on a long chain of tasks', () => {
+      const dependencies: Record<string, string[]> = {};
+      const length = 50_000;
+      for (let i = 0; i < length; i++) {
+        dependencies[`t${i}`] = [`t${i + 1}`];
+      }
+      dependencies[`t${length}`] = [`t${length - 1}`];
+      expect(findCycles({ dependencies })).toEqual(
+        new Set([`t${length - 1}`, `t${length}`])
+      );
+    });
   });

   describe('makeAcyclic', () => {
```

### How it was checked

Not under Nx's own vitest run: the two spec files and their sources were compiled with `tsc` from a sparse checkout of
`master`, with the native binding stubbed (`validateOutputs` a no-op), and run with `bun test`.

- Without the patch, six tests fail: the four `findCycles` tests that are not guards (`only lead to a cycle`,
  `behind a task that was visited before`, `whatever the order of the graph`, `long chain`), the new `createTaskGraph`
  test, and the changed `app1:build -> app2 <-> app3:build` expectation.
- With it, 72 tests pass and two fail, `assertTaskGraphDoesNotContainInvalidTargets`'s inline snapshots of a thrown
  error, which fail the same way without the patch (Bun and vitest serialise an `Error` differently).
- Every other `createTaskGraph` cycle spec, and the `findCycles` specs that were already there, pass unchanged.

Run `nx test nx --testFile=packages/nx/src/tasks-runner/create-task-graph.spec.ts` and
`--testFile=packages/nx/src/tasks-runner/task-graph-utils.spec.ts` on a real checkout before opening the PR.

### After the PR is opened

Update the draft's status with the upstream PR number. Retain the carried patch and its regressions until the installed
registry version contains the repair.
