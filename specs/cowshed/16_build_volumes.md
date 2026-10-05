# Build Volumes: Millisecond Forks and Lands

A workspace is two volumes: the **source volume** (the workspace image of 01_storage.md: the git tree, installed
dependencies, every build tool's declared outputs) and a **build volume** (the build tools' own incremental state: Cargo
target directories and Nx's task cache with its database). A fork clones both and is warm for every task main built. A
land moves main to the build volume the landing workspace already built, by renaming one link: main builds nothing that
the workspace built, and nothing is copied. This spec states that model, every rule that keeps it warm, why each rule
exists, and how each is enforced. It is the one place these rules live; other specs cite it.

## Goal

- **Fork in milliseconds, warm.** `cowshed new` clones images (milliseconds; 01_storage.md "Images") and attaches them
  (≈0.2 s each, measured independent of how many images the host has attached: 0.17–0.39 s at 91–254 attached images).
  Every Nx task main ran is a cache hit in the fork and every Cargo unit is `Fresh`. Only the fork's own edits
  invalidate anything, and exactly what the same edit would invalidate on main.
- **Land in milliseconds.** A land runs no build on main. The landing workspace has already built and checked the exact
  tree main moves to; main takes that workspace's build volume by one `rename(2)`.
- **Nothing copied, nothing patched, nothing extra in the environment.** No cache entries are copied between checkouts.
  Nx, Cargo and sccache run unmodified. A process learns nothing about cowshed: it reads the same paths it always reads.
- **Generic.** Build volumes come from capability detection (15_capabilities.md), not from a project's configuration. A
  project that uses Nx, Cargo, Go, ttsc, zig or none of them gets exactly the build state its tools have.
- **Bounded.** Every build volume and every cache inside one has an owner that deletes it. Nothing grows without bound.

## Why two volumes

One image per workspace carried everything until now: source, dependencies and build state. Three measured problems
follow from that, and a second volume removes each of them.

1. **Main fragments, and every fork pays for it.** Fragmentation comes from writing an image while a clone shares its
   blocks (01_storage.md "Images", "Format measurements": a fresh image under fixed churn goes from 385 to 8,871 extents
   with four clones held, but stays at 468 → 655 with none; the first write to a clone costs about 3.5–12 µs per extent,
   25.7 s at 2.1M extents). Build state is where nearly all of main's churn comes from: every build of main rewrites
   gigabytes under `target/` and `.nx/cache` while sessions hold clones of main's image. With build state on its own
   volume, main's source image changes only when its tree changes (a land's fast-forward, an edit), and build churn
   lands in build volumes that are replaced at every land instead of accumulating extents. Verify item: measure main's
   source image extent count across a week of lands with sessions held, against the single-image baseline; if it stays
   within the no-clone curve, `cowshed defrag main` becomes a repair for imports, not a routine operation.
2. **A land rebuilt main from scratch, or main was never warm.** A workspace builds and checks its work, lands, and then
   main had to build the same tree again before the next fork could start warm. Main's warm step was a background job
   that could fail, queue, or be skipped; while it lagged, every fork started cold. Measured over 48 hours of real use:
   15,137 Nx cache hits (2.9 h of task time) against 11,403 executions (131 h), with load averages of 70–154 on an
   18-core host from concurrent workspace checks that each re-ran nearly everything. The build volume removes the
   rebuild: main adopts what the workspace built.
3. **The inode problem stays solved.** Build state is the largest inode count a checkout has (a Rust `target/` holds
   hundreds of thousands of files). It lives inside a volume, never on the Data volume or the store volume's directory
   tree, so it stays out of the host's inode namespace exactly as before (00_overview.md "Problem").

## Model

### What lives where

| State                                                                                                | Volume                            | Shared how                                                |
| ---------------------------------------------------------------------------------------------------- | --------------------------------- | --------------------------------------------------------- |
| Git tree and `.git`, installed dependencies (`node_modules`), every Nx task's declared outputs       | Source volume                     | Cloned with the workspace image                           |
| Cargo target directories; Nx `.nx/cache` and `.nx/workspace-data` (task database, project graph)     | Build volume                      | Cloned at fork; adopted by main at land                   |
| Nx daemon record and sockets (`.nx/workspace-data/d`, the socket directory)                          | Per checkout, never travels       | Deleted from a build volume before main adopts it (below) |
| Content-addressed tool caches (Cargo registry/git, Go `GOMODCACHE`/`GOCACHE`, zig, bun, uv, sccache) | Shared cache paths (03_caches.md) | Written once, read by every checkout; never in any image  |

The build volume holds exactly the state a build tool keeps **for one tree and keys by its own fingerprints**: state
that is correct to reuse for any tree the tool checks against, but costly to rebuild and useless to share between two
writers at once. Content-addressed caches, whose entries are immutable and valid for every checkout, stay shared
(03_caches.md). Declared task outputs stay on the source volume (rule "Nx outputs never live in the build volume").

### Substrate

- **APFS**: a build volume is an ASIF image of its own, created exactly as a workspace image is (01_storage.md "Images":
  case-sensitive APFS), stored beside the workspace's image as `<owner>/<repo>/build/<id>.asif` with its sidecar
  `<id>.asif.json`. It is attached `nobrowse` at a store-side mountpoint, `<mount-root>/.build/<owner>/<repo>/<id>`,
  never inside another volume.
- **Capacity**: 100 GiB by default, and resizable exactly as a workspace image is (01_storage.md "Detached growth";
  override per project via `.cowshed.toml`). A sparse image costs only its written blocks, so the capacity is not an
  allocation: it is a deliberate cap on build-cache growth. A build that fills its volume fails loudly with the volume
  named, and the remedy is a resize or a prune, never an unbounded cache. A clone and a seed inherit their source's
  capacity.
- **ZFS**: a build volume is a dataset, `<pool>/cowshed/<owner>/<repo>/build/<id>`; a fork is `snapshot` + `clone`, and
  a seed is a snapshot (09_substrates.md).
- A directory is never a build volume. Copying a Rust `target/` tree file by file with `clonefile`-backed `cp -c -p -R`
  took 370–568 s for one 319-unit workspace on the same volume; cloning the image that holds it takes milliseconds.
  Minutes per land is unacceptable.

The sidecar records: the git tree (`git rev-parse HEAD^{tree}`) the volume was last built at, the checkout that links
it, whether it is a **seed** (immutable, see below), and its creation time. Nothing else is derived from it.

### One link per checkout

A checkout reaches its build volume through exactly one symlink, `<checkout>/.cowshed/build`, which points at the
volume's mountpoint. `.cowshed/` is already excluded from git in every checkout (02_workspaces.md). The tools' own paths
are fixed relative symlinks through it, created by cowshed when it mints the checkout and never changed afterwards:

```
<checkout>/target            -> .cowshed/build/target               # one per Cargo target dir capability detection finds
<checkout>/.nx/cache         -> ../.cowshed/build/nx/cache
<checkout>/.nx/workspace-data -> ../.cowshed/build/nx/workspace-data
```

A land changes exactly one name, `.cowshed/build`, with one `rename(2)` of a new symlink over it. That is atomic: every
path a tool opens afterwards resolves into the new volume. The fixed links mean no tool, no environment variable and no
configuration file ever names a build volume directly.

Mounting a checkout mounts the volume its link names only when that volume's sidecar records the checkout as its linker,
or the volume has no sidecar yet (a creation in progress, whose sidecar is written last). Any other link is stale and is
re-pointed at the one volume recorded as the checkout's; none or several refuses. Why: the link lives inside the source
image, so a restored checkpoint carries the link it had when taken, and a land interrupted between renaming a target's
link and updating the sidecars leaves the target naming the landing volume. Mounting such a link as found would let the
checkout write a volume a target or another checkout owns, or one already collected. A restore therefore never rewinds
the build volume.

Capability detection names the build-state paths (15_capabilities.md, one contribution contract): the Cargo capability
contributes each `cargo metadata` `target_directory` inside the checkout, the Nx capability contributes `.nx/cache` and
`.nx/workspace-data`, and the code-graph indexer (detected by its `.codegraph/` directory) contributes `.codegraph/`
whole. A capability that keeps no per-tree incremental state contributes none. A project with no build-state capability
gets no build volume and pays nothing.

### Targets and seeds

A **target** is any workspace that other workspaces fork from and land into: main, and every integration workspace built
on top of it — a lane base, the base of a stack of changes (a PR stack), a merge-queue head (02_workspaces.md "Lanes").
Everything below holds for every target alike; main is the target at the root.

A **seed** is a build volume nobody writes: a target's frozen build state at one landed tree. **Each target has its own
latest seed.** Every fork of a target clones that target's seed, never the target's live build volume. A target's live
build volume is written by whatever runs there (a developer's build, a reload, the target's own check); cloning it
mid-write would copy a Cargo unit or an Nx database half-written. A seed has no writer by construction, so its clone is
consistent without any quiescence protocol.

A target's seed is made when the target is created (a clone of the seed it was forked from) and again during every land
into it (below), by cloning the landing workspace's build volume after that workspace has been quiesced and before the
target adopts it, so neither side can be writing it. Each target keeps only its latest seed; a target's seed is deleted
when the target retires.

## Fork: `cowshed new` / `cowshed fork`

A fork names its target: `cowshed new <ws>` forks main, `cowshed fork <target> <ws>` forks an integration workspace (a
lane base, a stack base).

1. Clone the target's source image as today (02_workspaces.md).
2. Clone **the target's** latest seed image to a new build volume owned by the new workspace, attach it, and point the
   workspace's `.cowshed/build` at it. Clones preserve every file's bytes and mtime; Cargo freshness depends on that
   (rule "Clones preserve mtimes").
3. Delete `nx/workspace-data/d` in the new build volume: a daemon record names another checkout's process and socket
   (rule "One Nx state per checkout").

The fork is warm for the seed's tree. If the target's tree has moved past its seed's (a land skipped its swap, below),
the fork is warm for the seed's tree and builds the difference incrementally, as an edit would.

## Land: `cowshed land`

Land keeps its contract (02_workspaces.md "`cowshed land`"): it lands the head the caller validated into its target
(main by default, an integration workspace with `--into <target>`), under the target's repository lock, and never
rebases on its own. Building on the target is replaced by adopting the workspace's build volume. Every step reads the
same for main and for an integration workspace; "the target" is whichever one it is.

1. **Refuse a dirty tree** (as today).
2. **Validate in the workspace**: the caller's check (`--check`, for an Nx project `nx run-many -t lint test build` over
   what the workspace changed) runs in the sandbox against the workspace's own build volume. Only the delta builds.
3. **Fast-forward the target** under its repository lock (as today).
4. **Quiesce the landing workspace.** Its supervisor stops the workspace's jobs and its sandboxed Nx daemon, and the
   landing build volume's Nx task database must have no open file descriptors. If it still does, adoption is **skipped**
   and reported, as for the target below. The landing volume now has no writer.
5. **Freeze the seed.** Clone the landing build volume's image as the target's new seed and delete the target's previous
   seed. Nothing writes the volume while it is cloned, so the seed is consistent, and the next fork of this target
   starts from exactly what is landing.
6. **Adopt the build volume.** Under the same lock (rule "The adoption needs the target's Nx database closed"):
   1. query the open file descriptors of the target's current Nx task database (one query on one file). If any holder is
      not the daemon named by the target's `nx/workspace-data/d` record, skip;
   2. stop that daemon as stock `nx daemon --stop` does, a SIGTERM to the pid taken from the same record read that
      verified it live, and wait for its exit on a process-exit event (it restarts on the next client). A test with a
      real stock daemon pins the equivalence: after this stop, `nx daemon` starts cleanly;
   3. query again; any holder at all, including a daemon a host client started in between, means skip;
   4. delete `nx/workspace-data/d` in the landing build volume;
   5. `rename(2)` a new `.cowshed/build` symlink over the target's, naming the landing workspace's build volume;
   6. hand ownership in the sidecars: the target owns the adopted volume; its previous volume becomes unlinked (GC
      below).

   A skipped swap is reported in the land report with each holder's pid and command. The target keeps its build volume
   and builds the landed delta incrementally the next time anything builds there; forks still start from the new seed. A
   skipped swap is never wrong, only slower.

7. **Check the adoption (2b).** Re-run the landed check in the target, now on the adopted build volume. For Nx it must
   be **100% cache hits**. Every miss is a defect in the project's build configuration, not a reason to build: an input
   that differs between checkouts, an output that is not declared, a nondeterministic step. The land report lists each
   missed task with its hash inputs as a typed finding (13_telemetry.md records it), and the land still succeeds,
   because the landed code was already checked. This is a free, continuous lint of the Nx configuration: a coordinator
   turns findings into fix work.

   A check's counts come only from the run summary stock Nx writes into the target's cache (`cache/run.json`) when that
   summary is the check's own: Nx's `run.startTime` is not before the check was spawned, its `run.endTime` is not after
   the check exited, its command is one the check spells, and it has a task for every target that command names. Any
   other summary leaves the check **unattributed**, with the reason, and counts neither a hit nor a miss. Why: stock Nx
   records no pid or invocation in the summary and writes it once, at the end of a run, so the summary a check leaves is
   the last one any Nx process in the target wrote. Step 7 runs under the target's repository lock right after the
   target's daemon was stopped, so only a host shell in the target can run Nx then. A run that ended inside the window
   before the check's own was overwritten by it, and every other interleaving fails one of the four conditions, except
   one: a run of the same command that began after the check and ended between the check's Nx writing its summary and
   the check exiting. That run hashes the same tree with the same tasks, so the hashes it reports are the check's.

8. **Retire the workspace** (as today). Its build volume is now the target's and does not retire with it.

### Stacks and merge queues

A stack of changes on top of main (a PR stack, a merge queue, a lane) is a chain of targets, and the same three moves
apply at every level:

- Units fork the stack's head target and clone **its** seed, so they start warm with every change already landed into
  the stack, not just main's.
- A unit lands into the stack's head target: the head adopts the unit's build volume and reseeds. Without this, every
  fast-forward into an integration workspace would leave it cold and the next unit would rebuild what the previous one
  built — the same problem build volumes remove from main.
- When the stack itself lands into main (its base workspace's close-out, 02_workspaces.md "Lanes"), main adopts the
  stack base's build volume and reseeds. The stack's own seeds retire with its workspaces.

A stack of depth `n` therefore pays one incremental build per landed change, at the level where it lands, and nothing
again at any level above it.

## Garbage collection

- A build volume that no checkout links, that is not a seed, and that is **not busy** is detached and deleted. "Not
  busy" is the kernel's answer: a non-forced detach that the image driver refuses while any process has a file or
  working directory in the volume (01_storage.md detach). Cowshed never forces it and never infers idleness from a
  process scan. A refused detach leaves the volume for the next GC pass. A process that still runs on main's previous
  build volume after a swap therefore keeps it alive until it exits.
- Each target keeps only its latest seed; a target's seed is deleted when the target retires.
- Inside a build volume, Nx's own cache eviction runs unchanged (age and size bounds, configured in `nx.json` as Nx
  documents). Its database and its cache directory are always the same pair, so its eviction never deletes what another
  database indexes. Cargo's target directory is bounded by the tree it builds; cowshed does not prune it.
- A workspace's build volume retires with the workspace (`cowshed rm`), unless main adopted it.

## Rules that keep it warm

Each rule states what is enforced, why, and the incident that taught it. Each has an enforcing test or check; a rule
without one is not done.

### Every checkout has exactly one Nx state

Host shells, sandboxed jobs, land checks and the daemon of one checkout use one `.nx/cache`, one task database and one
daemon (04_sandbox.md states the socket and daemon placement). There is never a second, sandbox-private Nx cache, not
even for a read-only job: build state is not source, so a read-only job keeps the source tree read-only but reads and
writes its checkout's build volume like any other job.

- **Why**: Nx's task database indexes exactly one cache directory, and setting any of `NX_WORKSPACE_DATA_DIRECTORY`,
  `NX_CACHE_DIRECTORY` or `NX_PROJECT_GRAPH_CACHE_DIRECTORY` to something else moves the database with it. A second
  cache means a build fills one while a check reads the other, and every clone inherits both half-warm.
- **Incidents**: a per-job cache dir made every new workspace miss everything (fixed once, then reintroduced by a
  capability refactor that set `NX_CACHE_DIRECTORY` to a per-workspace `.cowshed/cache/nx`, which made every shed's
  cache private and cold). Danny, 2026-09-29: "isolated nx cache ARE YOU FUCKING KIDDING ME"; 2026-09-26: "why are you
  unsetting all the NX variables, why not configure nx correctly? Why are you scared of the cache?"
- **Enforced by**: a capability test asserting every job environment of an Nx project names the checkout's own
  `.nx/cache` and `.nx/workspace-data` and nothing else.

### The Nx daemon stays on

Cowshed never sets `NX_DAEMON=false` for any process, and never withholds the daemon to work around state problems.

- **Why**: the daemon is how Nx keeps the project graph and file hashes warm between commands; disabling it recomputes
  them on every run.
- **Incident**: Danny, 2026-10-03: "NEVER FUCKING USE NX_DAEMON=false … USE THE NX CACHES PROPERLY".
- **Enforced by**: the same capability test; a lint over cowshed and managed shell files refuses the string.
- The daemon's record lives in `.nx/workspace-data/d`, the directory that also holds the task database (pinned Nx
  23.2.1, `daemon/tmp-dir.js`), so the record would travel with a build volume. Fork and land delete it from the volume
  (above); a daemon is per checkout.
- **Accepted property**: an Nx client sends its whole environment to the daemon in every message
  (`daemon/client/client.js` `getDaemonEnv`), and the daemon applies it to its own `process.env`
  (`daemon/server/handle-client-env.js`). In a workspace whose daemon runs inside the sandbox, a host client's
  environment therefore reaches code the workspace controls. This is accepted: no checkout holds real secrets in its
  environment, the ones present are temporary, and host-side Nx in an agent's workspace runs with a pure environment.
  Revisit when real secrets exist.

### One Cargo environment on every path

Every Cargo invocation of a checkout (host shell, sandboxed job, Nx task, land check, a long-running dev process) sees
the same unit-identity environment. Concretely:

- **No `CI` in a dev environment.** Cargo turns incremental compilation off for workspace crates whenever `CI` is set,
  which changes their `-C metadata` and writes a second set of units into the same target directory (measured: 57
  workspace units hash differently; a target built without `CI` is 269/319 Fresh under `CI=true` and 319/319 without).
  Agent harnesses export `CI=true` on every command. The workspace environment cowshed provides unsets `CI` unless the
  runner identifies itself as real CI (`GITHUB_ACTIONS`, or the runner's own marker), and a repository's dev shell does
  the same. Danny, 2026-09-30: "why the fuck is CI set on our dev env? Who sets that"; "you can just prefix unset CI
  when you call things".
- **The toolchain's compiler variables.** `CC_<target>`, `CXX_<target>` and `AR_<target>` must be the dev shell's. Build
  scripts using the `cc` crate declare `rerun-if-env-changed` on them; one missing variable rebuilt four `-sys` crates
  and 42 dependents.
- **No absolute checkout paths in compiler flags.** `--remap-path-prefix=<absolute checkout>=…` puts the checkout path
  into `RUSTFLAGS`, which is part of every unit's identity, so every checkout builds its own units. Use
  `-Zremap-cwd-prefix` (relative), or no remap.
- **Two profiles.** `dev`, which includes unit tests, and `release`, for a released binary. `test` inherits `dev`.
  Danny, 2026-09-30: "BUILD EVERYTHING ONCE. CARGO ALREADY TRACKS CHANGES AND REBUILDS WHAT HAS CHANGED"; "You have
  realistically 2 profiles: dev (which includes unit testing) and release". 03_caches.md states the profile rule.
- **sccache is for dependencies and CI, never dev targets.** Incremental workspace units are not cacheable by sccache;
  the wrapper does not change fingerprints either way. Danny, 2026-09-30: "sccache is for CI and stable crates, not for
  dev targets".
- **Enforced by**: a test that builds a fixture workspace in one checkout, clones its build volume to a second checkout
  at another path, runs the same build from a host shell with `CI=true` exported and from a sandboxed job, and requires
  every unit `Fresh`.

### Clones preserve mtimes; directories are never copied

Cargo's freshness is mtime-based: a unit is stale when any dependency output is newer than its own. Image clones keep
every mtime. A file-by-file copy that restamps files in walk order (`cp -c -R`) made 214 of 319 units rebuild, and even
`cp -c -p -R` took minutes. Build volumes are only ever image clones or dataset clones.

- **Enforced by**: the fork test above runs on a cloned build volume and requires every unit `Fresh`.
- Content-based fingerprints (`CARGO_UNSTABLE_CHECKSUM_FRESHNESS` with `build.fingerprint = "content"`) are not used. A
  land rewrites only the files it changes, so mtime freshness rebuilds only those crates, and switching modes rebuilds
  every unit once and makes the two modes overwrite each other.

### Nx outputs never live in the build volume

No Nx target declares an output at or under a build-volume path (`target/…`, `.nx/…`).

- **Why**, measured on stock Nx: restoring a cached output beneath a symlinked directory replaces the symlink with a
  real directory, which silently detaches the checkout from its build volume; declaring the link itself as an output
  caches the symlink, not the bytes, so a hit restores nothing. Task outputs belong on the source volume (`dist/`,
  `.cache/<tool>/`), where the source image carries them to forks.
- **Enforced by**: a check in the Nx capability (and `cowshed doctor`) that refuses a project whose resolved target
  outputs fall under a build-volume path, naming the target and the output.

### Hash inputs are the same in every checkout

An Nx task hash must not change between checkouts of the same tree.

- No absolute paths, workspace names, user names, mount points or per-checkout values in any hashed input.
- A runtime input (a command whose output Nx hashes) prints only its value or a fixed sentinel. Nx hashes the command's
  stdout and stderr and ignores its exit code (pinned Nx 23.2.1, `hash_runtime`), so an error message that names an
  absolute path becomes a checkout-specific key that silently splits the cache. The input's consumer fails instead.
- Nx hashes the whole target configuration of the owning project into every task of that project (`hash_planner.rs`
  `gather_self_inputs` adds the project configuration; `hash_project_config.rs` hashes every target's executor, options,
  outputs and configurations). Input declarations cannot remove it; a project whose target definitions change often
  re-keys all of its tasks.
- A target that runs no package from `node_modules` declares `externalDependencies: []`; otherwise a lockfile edit
  re-keys it.
- **Enforced by**: step 2b of every land (100% hits on main) and the fork test (100% hits in a fresh fork).

### The adoption needs the target's Nx database closed

A process that opened the target's task database before a swap keeps that file open but resolves cache paths through the
links afterwards, so it reads rows from the old volume and files from the new one. For a hash the old database lists and
the new cache lacks, stock Nx reports a hit and restores nothing (measured): the task "succeeds" with its outputs
missing. A run that started on the old tree and stays entirely on the old volume is correct; only the mix is wrong. The
land therefore swaps only when no process holds the target's current task database open, and otherwise skips the swap
(Land step 6). The daemon is stopped at the swap because it also keeps the database open (it records task history). The
landing workspace is quiesced first (Land step 4) for the same reason on its side.

A run that started on the old tree and stays wholly on the old volume is correct even when it hits; the hazard is only
the mix.

- **Enforced by**: a test that holds main's database open in a running Nx client, lands, and requires the swap to be
  skipped and reported; and one with the database closed that requires the swap.

### Content-addressed tool caches are shared and writable

Read-at-build and link-target caches (03_caches.md layer 3: Cargo registry and git, Go module and build caches, zig,
bun, uv, sccache through its daemon) are one shared copy per host, written by whichever checkout fetches or compiles
first. Every sandbox that uses the tool must be able to write them; a cache a sandbox can only read fails the tool
(measured: Go's build cache unwritable from the sandbox failed every sandboxed Go build with `operation not permitted`).

- **Enforced by**: a sandboxed `go build`, `cargo fetch` and `bun install` in the capability test suite.

## Process lifetime across a swap

Nothing pins anything. A process resolves paths through the links when it opens them:

- A command started after a swap uses the new build volume entirely.
- A process that started before the swap and finishes without opening the Nx database again stays correct on the old
  volume; the old volume stays attached until it exits (GC).
- A long-running process that starts builds (a development server, a file watcher) starts each build as a new process,
  which resolves the new volume. Such a process re-subscribes to Nx file events when the daemon it watched is stopped at
  a swap (Nx reports `reconnecting`; the watcher reconnects through a new client, which starts a daemon).

## Stacks and fragmentation

Stacks also limit fragmentation (02_workspaces.md "Lanes"): units clone the stack's head, not main, and only the stack's
close-out writes main. With build volumes, each level adopts its units' build volumes and reseeds ("Stacks and merge
queues" above), and main's source image is written once per stack close-out. Stacks remain a coordinator choice; build
volumes do not require them.

## History

The decisions this spec records, in the order they were taken. It is kept so they are not re-litigated.

- 2026-09-26/27: Nx caching must be configured, not avoided ("why are you unsetting all the NX variables").
- 2026-09-29: a workspace is an APFS volume clone that replaces a git worktree with a warm, fully built sandbox ("DO YOU
  NOT UNDERSTAND THE CONCEPT OF AN APFS VOLUME CLONE REPLACING A git worktree WITH A WARM FULLY CACHED FULLY BUILT
  WORKING SANDBOX"); never an isolated Nx cache; stray directories inside `target/` made builds uncached; sccache for
  stable crates everywhere, incremental for dev.
- 2026-09-30: `CI` must not be set in dev environments; fragmentation comes from churn on main while clones exist ("so
  as long as any clone exists it grows much faster"), which motivated lanes; cowshed exists to keep build state off the
  Data volume ("We started cowshed because regular git worktrees exploded our inode usage"); two Cargo profiles only;
  sccache is not for dev targets.
- 2026-10-02: main must be fully built (`lint test build`) after landings so new workspaces start warm.
- 2026-10-03: never `NX_DAEMON=false`; the warm step runs before new workspaces are made.
- 2026-10-04: cowshed provides every setting out of the box by capability detection; the warm step is replaced by build
  volumes: a land adopts the landing workspace's build volume by renaming one link ("Millisecond forks of workspaces.
  Millisecond lands."); after the swap, main re-runs the check and asserts 100% Nx hits; minutes per land is
  unacceptable, so build volumes are images or datasets, never directories; Nx, Cargo and sccache stay unpatched; no
  extra environment variables; the daemon environment forwarding is accepted.

## Open questions

- **rust-analyzer and editors with `target` as a symlink.** Not yet verified.
- **Daemon keeper probing.** 04_sandbox.md's workspace supervisor probes the sandboxed daemon every 5 s; an event-driven
  alternative (the daemon's exit) is preferred if Nx exposes one.
- **ZFS mapping.** Dataset layout and `zfs promote` for adoption are specified with the ZFS substrate
  (09_substrates.md).
- **Fragmentation measurement** (Why two volumes, item 1).
