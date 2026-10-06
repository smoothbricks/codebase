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
  Every Nx task main ran is a cache hit in the fork and every Cargo unit is `Fresh`: the fork reseeds main first when
  main's volume holds work its seed does not (Targets and seeds). Only the fork's own edits invalidate anything, and
  exactly what the same edit would invalidate on main.
- **Land in milliseconds.** A land runs no build on main. The landing workspace has already built and checked the exact
  tree main moves to; main takes that workspace's build volume by one `rename(2)`.
- **Nothing copied but what a land would lose, nothing patched, nothing extra in the environment.** No cache entries are
  copied between checkouts, with one exception: a land copies into the landing volume the target's Nx cache entries it
  lacks (Land, "Carry"), because adopting the volume would otherwise discard them. Nx, Cargo and sccache run unmodified.
  A process learns nothing about cowshed: it reads the same paths it always reads.
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

| State                                                                                                                                                       | Volume                            | Shared how                                                |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- | --------------------------------------------------------- |
| Git tree and `.git`, installed dependencies (`node_modules`), every Nx task's declared outputs                                                              | Source volume                     | Cloned with the workspace image                           |
| Cargo target directories; Nx `.nx/cache` and `.nx/workspace-data` (task database, project graph); each installed JavaScript package's `node_modules/.cache` | Build volume                      | Cloned at fork; adopted by main at land                   |
| Declared build state (`.cowshed.toml` `[build] state`), under the volume's `declared/`                                                                      | Build volume                      | Cloned at fork; adopted by main at land                   |
| Nx daemon record and sockets (`.nx/workspace-data/d`, the socket directory)                                                                                 | Per checkout, never travels       | Deleted from a build volume before main adopts it (below) |
| Content-addressed tool caches (Cargo registry/git, Go `GOMODCACHE`/`GOCACHE`, zig, bun, uv, sccache)                                                        | Shared cache paths (03_caches.md) | Written once, read by every checkout; never in any image  |

The build volume holds exactly the state a build tool keeps **for one tree and keys by its own fingerprints**: state
that is correct to reuse for any tree the tool checks against, but costly to rebuild and useless to share between two
writers at once. Content-addressed caches, whose entries are immutable and valid for every checkout, stay shared
(03_caches.md). Declared task outputs stay on the source volume (rule "Nx outputs never live in the build volume").

### Substrate

- **APFS**: a build volume is an ASIF image of its own, created exactly as a workspace image is (01_storage.md "Images":
  case-sensitive APFS), stored beside the workspace's image as `<owner>/<repo>/build/<id>.asif` with its sidecar
  `<id>.asif.json`. It is attached `nobrowse` at a store-side mountpoint, `<mount-root>/.build/<owner>/<repo>/<id>`,
  never inside another volume.
- **Capacity**: 100 GiB by default for a build volume created from nothing; `.cowshed.toml`
  `[build] capacity = "<size>"` overrides it per project. A sparse image costs only its written blocks, so the capacity
  is not an allocation: it is a deliberate cap on build-cache growth. A build that fills its volume fails loudly with
  the volume named, and the remedy is a resize or a prune, never an unbounded cache. A clone and a seed inherit their
  source's capacity: an image clone carries its capacity, and configuration only ever names the capacity of a volume
  nothing was cloned from.
- **Resize**: `cowshed resize <ws|main> --build <size>` grows the workspace's build volume exactly as a workspace image
  grows (01_storage.md "Detached growth"), and its seed with it, so every later fork of the workspace inherits the new
  capacity. The workspace's jobs and Nx daemon stop first, because the image has to leave the kernel; any other holder
  of the volume refuses the resize before anything changes. A land never shrinks its target: while the landing volume is
  quiet (Land step 4) it grows to the target's capacity when it is smaller, before the seed is frozen from it, so the
  target and every later fork end at the larger of the two capacities.
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

To Git each of these links is a file. A repository's own directory pattern (`target/`) matches the directory a link
replaced and never the link itself. Before cowshed creates any link, it therefore adds an anchored, exact pattern for
the build link and for every build-state link to one managed block in the repository's `info/exclude` (the common
directory's, for a linked worktree). Lines outside the block are left exactly as they were, and entries in the block are
only ever added. A migrated checkout's `git status` shows none of its links, and the repository's `.gitignore` stays the
repository's own.

Mounting a checkout mounts the volume its link names only when that volume's sidecar records the checkout as its linker,
or the volume has no sidecar yet (a creation in progress, whose sidecar is written last). Any other link is stale and is
re-pointed at the one volume recorded as the checkout's. When the checkout owns none and the link names another
checkout's live volume, a target adopted it from this checkout and a `land --no-retire` stopped before reforking it: the
checkout takes a fresh clone of that target's latest seed, exactly as the refork would have. Anything else (several
volumes, or none and no adopter's seed) refuses. Why: the link lives inside the source image, so a restored checkpoint
carries the link it had when taken, and a land interrupted between renaming a target's link and updating the sidecars
leaves the target naming the landing volume. Mounting such a link as found would let the checkout write a volume a
target or another checkout owns, or one already collected. A restore therefore never rewinds the build volume.

**Refresh.** Before a job of a checkout is admitted (exec, a land check, the adoption check), at adoption for main, and
from `cowshed setup` for every mounted workspace, cowshed refreshes the checkout's build state: it fingerprints the
tracked build inputs (Cargo manifests, `.cowshed.toml`, the build-state capabilities' markers) and, while the
fingerprint matches the one the volume's state records, only restores a fixed link a tool displaced (a real directory
where the link belongs is discarded and relinked, never copied, and reported). When it moved, capability detection runs
again in the canonical job environment (the caller's `CARGO_TARGET_DIR` and the like never name a checkout's build
state): new paths join the volume, held ones never move. A checkout with build state and no volume gets its first one
(its first touch) at the project's `[build] capacity`, and the checkout's seed with it, so it is a target from then on.
A build-state path that holds tracked source refuses before anything is deleted.

A discarded directory is first renamed into `<checkout>/.cowshed/discard/`. That is the same volume, in cowshed's
excluded namespace, and never a sibling in the source tree. The link then takes the path, and the refresh returns
without waiting for the delete, which runs in the background. A target directory can be tens of GiB, and no job waits on
deleting it. A process that ends first, a crash included, leaves the rest pending. Every later refresh of the checkout
resumes the delete, and `cowshed gc` finishes it, naming each directory it deletes. A discovery job that has not
answered within 10 seconds says on stderr every 10 seconds what it is waiting on: the processes holding the host Cargo
home's package-cache locks, or none, in which case it is the shell's activation or Cargo itself.

Capability detection names the build-state paths (15_capabilities.md, one contribution contract): the Cargo capability
contributes each `cargo metadata` `target_directory` inside the checkout, the Nx capability contributes `.nx/cache` and
`.nx/workspace-data`, a JavaScript package manager contributes the `node_modules/.cache` of every package it installed,
and the code-graph indexer (detected by its `.codegraph/` directory) contributes `.codegraph/` whole. A capability that
keeps no per-tree incremental state contributes none. A project with no build-state capability and no declared build
state gets no build volume and pays nothing.

A tool's own per-checkout state that no capability names belongs inside one that is named, never in the source tree
beside an output. Measured on a consumer repository over three hours, with sessions holding clones of main: nextest's
extracted test archive (2 GB, rewritten for every new archive) sat beside its archive under `.cache/nextest`, and lmao's
SQLite trace sink (716 MB in one package, rewritten page by page on every test run) sat in each package's `.cache/`,
together 2.7 GB of the 5 GB written into main's source image. Both are content-keyed or per-run tool state, not outputs:
the nextest extraction now lives under the Cargo target directory (`<target_directory>/nextest-extracted`), and the
trace sink in `.cache/lmao`, a directory of its own that the project declares as build state (`packages/*/.cache/lmao`,
below). A package's `.cache/` itself cannot be build state, because build tools declare their outputs there.

### Declared build state

`.cowshed.toml` `[build] state = ["<checkout-relative path>", ...]` declares build state no capability detects. The
motivating case is a patch-development checkout of an upstream project plus its build tree, kept gitignored inside the
repository: many GiB of incremental build output that no convention file names, so no detector can recognize it. Left in
the source image, it is the largest thing a fork clones and the main source of main's fragmentation (Why two volumes);
on the build volume it travels exactly as a Cargo target directory does.

An entry may be a pattern, `packages/*/.cache/lmao`: its leading components hold `*`, `?` or `[...]`, each matching one
directory level as Git's glob pathspec reads them (`**` is refused), and its last components are a literal name. At
discovery the pattern selects every directory the checkout tracks files under (`git ls-files` of `<selector>/**`), never
an untracked one, and declares the literal name beneath each: one declared path per match. **Why**: per-package tool
state, a trace store every package's tests write under their own working directory, belongs on the volume for every
package, and listing each package by hand goes stale the day one is added. The expansion, not only the spelling, is a
fingerprint input, so a newly tracked package joins at the next refresh. A pattern whose last component is a glob is
refused: it would select tracked directories, which are never build state.

Each declared path is one more `BuildStatePath` contribution, `<path> -> declared/<path>` on the volume, merged into
discovery after the capabilities'. The `declared/` namespace keeps it apart from every tool's own volume names, and
tells cowshed it is declared rather than a tool's. The list is part of `.cowshed.toml`, which the discovery fingerprint
already covers, so changing it rediscovers at the next refresh: a new path joins the volume, and a held path keeps its
link. Validation refuses, before anything is deleted and with the remedy named:

- an entry that is not a normalized checkout-relative name or pattern, one naming `.git` or inside `.cowshed/`, and one
  spelled inside another (when `.cowshed.toml` is parsed); a pattern match inside another declared path, or around one
  (at discovery);
- a path reached through a symlinked parent that resolves outside the checkout;
- a path that overlaps a capability's build state: the capability already links it;
- a path holding tracked source: the same `git ls-files` guard migration applies.

A real directory at a declared path is treated like any build state a capability contributes: moved aside, linked,
deleted in the background, never copied (Refresh). For the upstream checkout that means the first touch after the path
is declared discards it. That is deliberate: build state is rebuild-only, never migrated, and declared state is
rebuildable by definition; copying many GiB out of the source image would cost the minutes this design exists to avoid.
The author re-runs the checkout's reconstruct script once; from then on it lives on the volume, every fork inherits it
warm, and a land carries it into main. The refresh reports the discard on stderr with that instruction.

**A declared path is linked when it appears, never before.** A capability's path is linked at once, and its tool finds
an empty directory it fills. A declared path's existence carries meaning to tools cowshed does not know: a build script
that builds the upstream checkout when it is there, and takes a prebuilt artifact when it is not, would find an empty
directory and try to build nothing. So a refresh makes nothing for a declared path nothing occupies, neither the link,
the volume directory nor a missing parent; the volume's state still holds the path. Once a tool has made the directory,
the next refresh adopts it as above. A reconstruct script that wants to fill the volume on its first run makes the empty
directory, lets a refresh link it (any `cowshed exec`), and only then fills it. The rule holds per pattern match: a
package whose tests never wrote a trace store gets no link.

A tool whose own state records absolute paths sees the volume's: the declared path is a link, and a process that
resolves its working directory (`getcwd`, `pwd -P`) gets `<mount-root>/.build/<owner>/<repo>/<id>/declared/<path>`. That
path changes when a land swaps the checkout's volume and differs in every fork, so state keyed on its own absolute
location must compare against the resolved path, not the checkout spelling, and treat a different one as moved.

**A path-baking build is warm only within the checkout that built it.** Bun's CMake/Ninja tree records the resolved
path, so a fork of main and main after a land each reconfigure it from scratch. Cargo's target directory is relocatable
and is not affected. Declaring such a tree is still worth it: its multi-GiB churn stays off the source image, and a
patch-development workspace stays warm for its own life. A stable per-checkout mountpoint would keep only main warm
across lands, and was rejected. An atomic swap at one path would need mount stacking, and measured on APFS, a process
whose working directory was entered under the covered volume reads the new one, and `diskutil unmount` of the covered
device removed the top mount instead. The swap would therefore be an unmount and a mount, which must skip adoption
whenever any process (rust-analyzer included) holds main's build volume, on every land.

- **Enforced by**: a real-APFS test in which main's first touch discards a declared nested checkout and links the path,
  the reconstructed state is written through the link, and a fork of main reads it warm; discovery tests refusing a
  tracked, overlapping or escaping declaration with its remedy; migration tests linking a declared checkout and leaving
  an absent declared path absent until a tool makes it; a discovery test expanding a pattern over tracked packages only,
  rediscovering when one is added; and configuration parse tests.

### Targets and seeds

A **target** is any workspace that other workspaces fork from and land into: main, and every integration workspace built
on top of it — a lane base, the base of a stack of changes (a PR stack), a merge-queue head (02_workspaces.md "Lanes").
Everything below holds for every target alike; main is the target at the root.

A **seed** is a build volume nobody writes: a target's frozen build state at one landed tree. **Each target has its own
latest seed.** Every fork of a target clones that target's seed, never the target's live build volume. A target's live
build volume is written by whatever runs there (a developer's build, a reload, the target's own check); cloning it
mid-write would copy a Cargo unit or an Nx database half-written. A seed has no writer by construction, so its clone is
consistent without any quiescence protocol.

A target's seed is made when the target is created (a clone of the seed it was forked from, or of its first build volume
at its first touch), during every land into it (below), by cloning the landing workspace's build volume after that
workspace has been quiesced and before the target adopts it, so neither side can be writing it, and again by a
**reseed** whenever the target's live volume holds a write its seed does not. Each target keeps only its latest seed; a
target's seed is deleted when the target retires.

**Why a reseed.** A land freezes the seed before the target runs anything on the adopted volume. Everything the target
runs afterwards (its own builds and gates, the land's adoption check, a developer's build in main) is written to its
live volume and never reaches that seed. Measured: a fork made 18 minutes after a land missed 122 Nx tasks whose hashes
equalled main's; the seed had no row for any of them, and main had executed each one after the seed was frozen. A fork
of a target is meant to be warm for what the target ran, not only for what last landed in it, so the seed follows the
live volume. Refreezing once more at the end of the land would not do it: it would capture only the adoption check,
which hits by construction, and miss everything the target runs later.

**Behind is one comparison of two modification times.** An image's modification time is the instant of the last write it
holds: the image driver writes the image file as its volume is written, and a clone keeps its source's modification time
(cowshed restores it after the clone's own first write). A seed is behind when the live volume's image, its volume
flushed first, was modified after the seed's image. A fork sets its own seed's time to its new live volume's, so a new
workspace starts with a fresh seed rather than one behind by its own mount.

**A reseed clones the live volume only while it has no writer**, because a clone of a volume being written can hold a
half-written Cargo unit or Nx database. It runs under the target's image lock, which every fork of the target holds
while it clones the seed, so a reseed never deletes a seed a fork is cloning:

1. the target's Nx task database may have no holder but the target's daemon, which is stopped as at an adoption (Land
   step 5); any other holder, or a daemon that outlives its stop, skips the reseed;
2. every Cargo build lock in the volume (`.cargo-lock` in each profile directory of a Cargo target directory, which a
   running Cargo holds for its whole build) is taken without waiting; a held one skips the reseed, and holding them all
   keeps a Cargo build from starting until the clone is cut (it waits on the lock as for any concurrent build);
3. the live volume is cloned as the new seed, recording the live volume's tree;
4. the task database is looked at once more: a process that opened it during the clone may have written it mid-clone, so
   that clone is deleted and the reseed skipped; otherwise the previous seed is deleted.

Only the two tools whose state a half-written copy corrupts are asked. A process that writes a build-state directory
outside their protocols (a script appending to a file under `target/`) is not looked for: the seed holds its files as
they were at the clone, as after a crash. The kernel's own idea of idle, no process with a file or working directory in
the volume, would be stricter and would never come: an editor's rust-analyzer keeps proc-macro libraries from the target
directory open for as long as it runs, and the code-graph indexer keeps its database open.

Every fork reseeds its target first (Fork step 2). A skip names each holder and leaves the seed as it was: the fork is
colder, never wrong, and the next fork tries again. `cowshed reseed <ws>` does the same on its own, and `cowshed doctor`
reports each target whose seed is behind its live volume, with both instants (`seed-age`).

- **Enforced by**: a real-APFS test in which main runs an Nx task after adopting a landed volume, `cowshed reseed main`
  refreezes its seed, and a fork of main hits that task; and unit tests of the Cargo lock walk and hold.

## Fork: `cowshed new` / `cowshed fork`

A fork names its target: `cowshed new <ws>` forks main, `cowshed fork <target> <ws>` forks an integration workspace (a
lane base, a stack base).

1. Clone the target's source image as today (02_workspaces.md). The fork holds the target's image lock from here to the
   end of step 3.
2. **Reseed the target** when its seed is behind its live volume and the volume has no writer (Targets and seeds).
3. Clone **the target's** latest seed image to a new build volume owned by the new workspace, attach it, and point the
   workspace's `.cowshed/build` at it. Clones preserve every file's bytes and mtime; Cargo freshness depends on that
   (rule "Clones preserve mtimes").
4. Delete `nx/workspace-data/d` in the new build volume: a daemon record names another checkout's process and socket
   (rule "One Nx state per checkout").

The fork is warm for everything the target's volume held when the seed was last frozen: at the latest land, or at the
latest reseed, which is this fork's own step 2 unless the target's volume had a writer. If the target's tree has moved
past its seed's (a land skipped its swap, below), the fork builds the difference incrementally, as an edit would.

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
5. **Close the target, carry, and freeze the seed.** Under the same lock (rule "The adoption needs the target's Nx
   database closed"):
   1. **stage the carry** (below) while the target still runs: copy into the landing volume the target's Nx cache
      entries it lacks, indexing none of them yet;
   2. query the open file descriptors of the target's current Nx task database (one query on one file). If any holder is
      not the daemon named by the target's `nx/workspace-data/d` record, skip;
   3. stop that daemon as stock `nx daemon --stop` does, a SIGTERM to the pid taken from the same record read that
      verified it live, and wait for its exit on a process-exit event (it restarts on the next client). A test with a
      real stock daemon pins the equivalence: after this stop, `nx daemon` starts cleanly;
   4. query again; any holder at all, including a daemon a host client started in between, means skip;
   5. **commit the carry**: index what was staged, and copy what the target indexed since;
   6. clone the landing build volume's image as the target's new seed and delete the target's previous seed. Nothing
      writes the volume while it is cloned, so the seed is consistent, and the next fork of this target starts from at
      least what is landing and what the target held. What the target itself runs on the adopted volume afterwards
      reaches the seed by a reseed (Targets and seeds).

   A skip at 2 or 4 deletes what was staged, still freezes the seed (5.6), and skips the swap.

6. **Adopt the build volume.** Still under the lock:
   1. query the target's task database once more: a holder that opened it since 5.4 means skip;
   2. delete `nx/workspace-data/d` in the landing build volume;
   3. `rename(2)` a new `.cowshed/build` symlink over the target's, naming the landing workspace's build volume;
   4. hand ownership in the sidecars: the target owns the adopted volume; its previous volume becomes unlinked (GC
      below).

   A skipped swap is reported in the land report with each holder's pid and command. The target keeps its build volume
   and builds the landed delta incrementally the next time anything builds there; forks start from the new seed until
   the target's own volume is written again, and then from a reseed of that volume, which holds the target's own work
   and builds the landed delta as the target does. A skipped swap is never wrong, only slower.

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

### Carry

A landing volume holds what its workspace forked from and what it ran since. The target's volume also holds what other
lands and the target itself ran in the meantime. Adopting the landing volume as it is discards all of that, though Nx's
entries are content-addressed and every one is a correct hit for any tree that computes its hash. Measured on a consumer
repository: a fork made right after a land hit 78 of 108 tasks, and among the misses were tasks main's previous volume
held at that very tree. So before the target adopts the landing volume, every Nx cache entry the target's task database
indexes and the landing volume's does not is copied into the landing volume and indexed there. Only Nx's state is
carried: a Cargo target directory is per tree and mtime-fresh, so the landing volume's is the one that matches the
landed tree.

Stock Nx (pinned 23.2.1, `native/cache/cache.rs`) keeps an entry in three places: the directory `<cache>/<hash>` with
the task's outputs, the file `<cache>/terminalOutputs/<hash>`, and a `cache_outputs` row in the task database
`<workspace-data>/<machine>-v<schema>.db`, which references a `task_details` row. `put` writes the files first and the
row last, and `get` answers a hit only for a row. The carry keeps that order in two phases, so the target's database is
closed only for as long as it was before:

- **Stage**, while the target runs (5.1): one SQLite read of the target's database (its write-ahead log included) names
  the rows the landing database lacks, most recently used first; each entry's files are copied with `copyfile(3)`
  (bytes, mode and times) into the landing volume's `.carry/` staging directory, outside every tool's namespace. Nothing
  indexes them, so nothing reads them.
- **Commit**, once the target is closed (5.5): a staged entry whose row (code, size, creation time) is unchanged moves
  into the cache by `rename(2)`. Nx rewrites an entry only by deleting its directory, rewriting it and re-stamping its
  row, and evicts one by deleting its row first, so an unchanged row proves the staged copy whole; any other staged
  entry is dropped. Rows the target indexed after the stage are copied now, nothing writing either side. Every placed
  entry's `task_details` and `cache_outputs` rows are inserted in one transaction, as the target holds them, its
  `accessed_at` included, so Nx's own age and size eviction treats them as it would have in the target. `.carry/` is
  then deleted.

Entries cross from one image to another, so they are copied, never cloned. Their volume is the delta of what other lands
and the target ran since the landing workspace forked, which is what a fork would otherwise rebuild. Copies run before
the target's database is closed and cost no window; a failed copy (a full volume) stops the carry, keeps every entry
carried until then, and is reported (`carried.stopped`); the land still succeeds. A landing Nx state with no task
database never ran Nx, and first takes a copy of the target's database without its cache rows (`VACUUM INTO`, one
consistent snapshot), so the carry has a schema Nx wrote to index into; one whose database has another name runs an Nx
of another schema and gets nothing. A carry that crashes leaves only `.carry/`, which the next stage on that volume
deletes before it starts.

- **Enforced by**: a real-APFS test in which one workspace lands and warms an Nx task in main, a second, forked before
  that land and changing nothing the task hashes, lands without running it, and a fork of main hits the task.

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
- A fork's volume and seed exist before its workspace does: `cowshed new` and `cowshed fork` clone them into the staged
  checkout, which no other process can read, and publish the workspace afterwards. While the create or fork is past its
  mutation fence and unfinished in the lifecycle intent journal, collection in any process defers the volumes recorded
  as that workspace's or as its seed, and every image without a record, naming the workspace. Without this, an `rm` in
  one process deleted the volume and seed of a `new` running in another, and the new workspace's mount refused its link
  to a volume nobody owned.
- Inside a build volume, Nx's own cache eviction runs unchanged (age and size bounds, configured in `nx.json` as Nx
  documents). Its database and its cache directory are always the same pair, so its eviction never deletes what another
  database indexes. A carried entry keeps the row the target held, last use included, so it ages out as it would have in
  the target, and the carry indexes every entry it places, so nothing it writes escapes Nx's eviction; its staging
  directory is its own to delete (Land, "Carry"). Gc never opens a volume's contents. Cargo's target directory is
  bounded by the tree it builds; cowshed does not prune it.
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

No Nx target declares an output that can cover a contributed build-volume path: at it, under it, or an ancestor of it.
Globs whose static prefix overlaps one of those paths are also refused; absolute outputs are resolved against the
workspace root.

- **Why**, measured on stock Nx: restoring a cached output beneath a symlinked directory replaces the symlink with a
  real directory, which silently detaches the checkout from its build volume; declaring the link itself as an output
  caches the symlink, not the bytes, so a hit restores nothing. Task outputs belong on the source volume (`dist/`,
  `.cache/<tool>/`), where the source image carries them to forks.
- **Enforced by**: the consuming repository's Nx lint calls the plugin's `refuseOutputsUnderBuildState` validator with
  its already resolved graph (including project overrides), `cowshed build-state --json` path records, and its workspace
  root. A refusal names the target, output, and build-state path. Job admission and `cowshed doctor` never construct an
  Nx graph or execute repository plugins for this rule.
- `cowshed build-state --json` reads the selected checkout's volume-state record and prints
  `{paths:[{checkout,volume}]}` without opening the controller or store. It never rediscovers tools. A missing link or
  state record refuses with `environment-missing` (exit 5): "this checkout has no build volume yet; run `cowshed setup`
  (host) or any `cowshed exec` in it to migrate".

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
(Land steps 5 and 6). The daemon is stopped before the swap because it also keeps the database open (it records task
history), and the carry commits while the database is closed. The landing workspace is quiesced first (Land step 4) for
the same reason on its side.

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

Nothing pins a path. A process resolves paths through the links when it opens them, inside the sandbox grant of the job
it belongs to. Each job's sandbox grants exactly the build volume the controller resolved when the job was admitted
(04_sandbox.md); the supervisor is never relaunched for a swap:

- A job admitted after a swap, and the Nx daemon the supervisor restarts after it, use the new build volume entirely.
  The keeper resolves the current pointer through the same controller-owned layout as admission on every restart, even
  when no user job has been admitted since the swap. Repository links alone never authorize a mount.
- A job admitted before the swap keeps its grant and stays on the old volume. The target's previous volume is released
  without force, so it stays attached, busy, until the last such job exits; GC then deletes it. Nothing is killed.
- A long-running job that starts builds (a development server, a file watcher) keeps the grant it was admitted with:
  builds it starts after a swap resolve the links into the new volume, which that grant does not name, and the sandbox
  denies them. Restarting the job admits it on the adopted volume. Its Nx client re-subscribes to file events when the
  daemon it watched is stopped at a swap (Nx reports `reconnecting`).

- **Enforced by**: a paused-clock supervisor test pivots the build link while its first keeper job runs, admits no
  intervening user job, and requires the restarted daemon's grant to name only the adopted volume while the old job's
  profile still names only the previous volume.

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
- 2026-10-05: a fork made after a land missed 122 Nx tasks main had run after the seed was frozen, so a target's seed
  follows its live volume: every fork reseeds its target first when the target's volume was written after its seed and
  has no writer (Nx database closed, Cargo build locks free), `cowshed reseed` does the same on demand, and doctor
  reports a seed that is behind.
- 2026-10-06: a fork made right after a land hit 78 of 108 Nx tasks: adopting the landing volume had discarded the
  entries main's previous volume held, though they were content-addressed. A land now carries the target's Nx cache
  entries the landing volume lacks into it before the swap, copying while the target runs and indexing while its
  database is closed, so the swap's window stays as short as before.

## Open questions

- **rust-analyzer and editors with `target` as a symlink.** Not yet verified.
- **Daemon keeper probing.** 04_sandbox.md's workspace supervisor probes the sandboxed daemon every 5 s; an event-driven
  alternative (the daemon's exit) is preferred if Nx exposes one.
- **ZFS mapping.** Dataset layout and `zfs promote` for adoption are specified with the ZFS substrate
  (09_substrates.md).
- **Fragmentation measurement** (Why two volumes, item 1).
