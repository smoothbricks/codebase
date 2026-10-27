# Build Volumes: Millisecond Forks and Lands

A workspace is two volumes: the **source volume** (the workspace image of 01a_apfs.md: the git tree, installed
dependencies, every build tool's declared outputs) and a **build volume** (the build tools' own incremental state: Nx's
task cache with its database, package caches, the code index, declared build state, and the Cargo target directories of
a project without mbx). A Cargo project with the mbx capability keeps its compiler work out of both: mr boxington (mbx)
holds it in one content-addressed store per host and one managed target directory per checkout, on the host's
build-cache volume (01a_apfs.md, "Dedicated build-cache volume"). A fork clones both volumes and is warm for every task
main built, and restores every Cargo unit any checkout stored. A land moves main to the build volume the landing
workspace already built, by renaming one link: main builds nothing that the workspace built, and nothing is copied. This
spec states that model, every rule that keeps it warm, why each rule exists, and how each is enforced. It is the one
place these rules live; other specs cite it.

## Goal

- **Fork in milliseconds, warm.** `cowshed fork` clones images (milliseconds; 01a_apfs.md "Images") and attaches them (a
  fraction of a second each, independent of how many images the host has attached). Every Nx task main ran is a cache
  hit in the fork: the fork reseeds main first when main's volume holds work its seed does not (Targets and seeds).
  Every Cargo unit is restored from the mbx store instead of compiled, or, in a project without mbx, is `Fresh` in the
  cloned volume. Only the fork's own edits invalidate anything, and exactly what the same edit would invalidate on main.
- **Land in milliseconds.** A land runs no build on main. The landing workspace has already built and checked the exact
  tree main moves to; main takes that workspace's build volume by one `rename(2)`.
- **Nothing copied, nothing patched, nothing extra in the environment.** No cache entries are copied between checkouts:
  a land adopts the landing volume as it is, and what other lands ran reaches a fork through the project's Nx cache
  (05_gateway.md, "/nx/v1/cache/{hash}") and the mbx store. Nx and Cargo run unmodified, Cargo through mbx exactly as
  mbx documents. A process learns nothing about cowshed: it reads the same paths it always reads.
- **Generic.** Build volumes come from capability detection (15_capabilities.md), not from a project's configuration. A
  project that uses Nx, Cargo, Go, ttsc, zig or none of them gets exactly the build state its tools have.
- **Bounded.** Every build volume and every cache inside one has an owner that deletes it. mbx owns its store and its
  managed targets and bounds both with its own collection (03_caches.md). Nothing grows without bound.

## Why two volumes

Carrying source, dependencies and build state in one image per workspace has three problems, and a second volume removes
each of them.

1. **Main fragments, and every fork pays for it.** Fragmentation comes from writing an image while a clone shares its
   blocks (01a_apfs.md "Images", its clone-cost bullet): extents multiply under churn while clones are held, and the
   first write to a clone costs time per extent. Build state is where nearly all of main's churn comes from: every build
   of main rewrites gigabytes under `target/` and `.nx/cache` while sessions hold clones of main's image. With build
   state on its own volume, main's source image changes only when its tree changes (a land's fast-forward, an edit).
   Main's source image therefore fragments no faster than an image that has no clone, and `cowshed defrag main` is a
   repair for imports, not a routine operation. Build volumes are not replaced by anything that sheds extents: a fork
   clones its target's seed, a land adopts the landing volume and a seed is captured from it, so every build volume
   inherits its parent's extent map and adds its own churn. SmoothBricks' main seed went from 434,000 to 881,000 extents
   in four days, and reached 1.2 million extents and 397.7 GB allocated by 2026-10-27, when its `mount_apfs` outran the
   120 s deadline. Nearly all of that churn was Cargo's `target/`, which an mbx project keeps on the build-cache volume
   instead (item 4). Seed compaction (Targets and seeds) bounds what lineage remains.
2. **Without build volumes, a land rebuilds main from scratch, or main is never warm.** A workspace builds and checks
   its work, lands, and then main has to build the same tree again before the next fork can start warm. A warm step for
   main would be a background job that can fail, queue, or be skipped; while it lags, every fork starts cold, and
   concurrent workspace checks that each re-run nearly everything load the host. The build volume removes the rebuild:
   main adopts what the workspace built.
3. **The inode problem stays solved.** Build state is the largest inode count a checkout has (a Rust `target/` holds
   hundreds of thousands of files). It lives inside a volume, never on the Data volume or the store volume's directory
   tree, so it stays out of the host's inode namespace (00_overview.md "Problem"). mbx's managed targets live on the
   build-cache volume for the same reason.
4. **Cargo's state is shared, not per tree.** A target directory per build volume means two sheds compile the same
   change twice, main adopts only one lander's units, and a build volume carries the churn of every Cargo build. The mbx
   store holds each compiled action once per host, addressed by content with checkout paths replaced by placeholders.
   Every checkout restores what any other compiled, including work in flight, through one scheduler (05_gateway.md,
   "Host scheduling"), and build-script runs and host links are cached with it. A plain volume, not an image, holds it:
   the store is shared by every checkout, so no fork clones it and no land adopts it, and a volume has no extent
   lineage, attach or compaction.

## Model

### What lives where

| State                                                                                                                                                                                               | Volume                                                                                  | Shared how                                                                             |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Git tree and `.git`, installed dependencies (`node_modules`), every Nx task's declared outputs                                                                                                      | Source volume                                                                           | Cloned with the workspace image                                                        |
| Cargo target directories of a project without the mbx capability; Nx `.nx/cache` and `.nx/workspace-data` (task database, project graph); each installed JavaScript package's `node_modules/.cache` | Build volume                                                                            | Cloned at fork; adopted by main at land                                                |
| Cargo target directory of a project with the mbx capability                                                                                                                                         | mbx's managed target for the checkout, `<build-cache>/mbx/targets/v1/<checkout digest>` | Per checkout, never travels; restored from the mbx store by content                    |
| Cargo compiled actions, build-script runs, host links, learned incremental state                                                                                                                    | The mbx store, `<build-cache>/mbx`                                                      | One per host, written and read by every checkout (03_caches.md)                        |
| Codegraph's per-tree `.codegraph/` index                                                                                                                                                            | Build volume                                                                            | Cloned at fork; adopted by main at land                                                |
| Node/Bun module compile cache (`NODE_COMPILE_CACHE`)                                                                                                                                                | Workspace's private XDG cache (03_caches.md)                                            | Read-write/host cache cloned with source; read-only cache retired with owned exec-temp |
| Declared build state (`.cowshed.toml` `[build] state`), under the volume's `declared/`                                                                                                              | Build volume                                                                            | Cloned at fork; adopted by main at land                                                |
| Nx daemon record and sockets (`.nx/workspace-data/d`, the socket directory)                                                                                                                         | Per checkout, never travels                                                             | Deleted from a build volume before main adopts it (below)                              |
| Content-addressed tool caches (Cargo registry/git, Go `GOMODCACHE`/`GOCACHE`, zig, bun, uv)                                                                                                         | Shared cache paths (03_caches.md)                                                       | Written once, read by every checkout; never in any image                               |

The build volume holds exactly the state a build tool keeps **for one tree and keys by its own fingerprints**: state
that is correct to reuse for any tree the tool checks against, but costly to rebuild and useless to share between two
writers at once. Content-addressed caches, whose entries are immutable and valid for every checkout, stay shared
(03_caches.md). Declared task outputs stay on the source volume (rule "Nx outputs never live in the build volume").

Module compilation uses the already-owned private environment cache, not a new build-state volume or a loose directory
under `TMPDIR`. Its runtime-versioned bytes remain reconstructible and retain that cache root's clone and retirement
rules. Source-read-only jobs write only their existing exec-temp cache. Clean retirement reclaims that environment
through the owned scratch lifetime. A retained recovery image keeps its authority until deliberate export: after
transfer preflight, under the name's lifecycle lock, the storage owner reclaims its exec-temp environment before any
image or companion moves (02_workspaces.md, Recovering a recovery image). A live or newer namesake retains its own
environment. A mount at exec-temp itself or beneath it refuses cleanup before any entry is removed, including a Linux
bind mount on the same backing device; the recovery image and sidecar stay in place. Restoration's refusal of
unexplained project contents is unchanged: no blanket temporary-tree sweep or restore exception stands in for producer
ownership.

### Substrate

- **APFS**: a build volume is a sparse RAW image of its own, created exactly as a workspace image is (01a_apfs.md
  "Images": case-sensitive APFS), stored beside the workspace's image as `<owner>/<repo>/build/<id>.dmg` with its
  sidecar `<id>.dmg.json`. It is attached `nobrowse` at a store-side mountpoint,
  `<mount-root>/.build/<owner>/<repo>/<id>`, never inside another volume.
- **Names**: every mounted live build volume is named `[cowshed] <project> — build <checkout>` (the project is displayed
  as `<owner> · <repo>`); main uses `build main`, and a session uses `build <ws>`. A mounted live volume is never left
  named only `[cowshed]`. Its supervisor queues the APFS rename off the provisioning path, checks the name again on
  supervisor start, and queues a new name whenever land or adopt changes the checkout that owns it. Seeds are never
  mounted and are never named: mounting one merely to rename it would write its image and advance the seed-freshness
  clock (Targets and seeds). Resize may attach a seed without mounting it; no label operation accompanies that attach.
  Provisioning, quiescence and retirement never wait for a rename. Before naming a build volume, the background task
  checks its sidecar-backed checkout grant, not merely its link; an old owner cannot authorize the adopted volume.
  Labels are cosmetic, never authority. A supervisor start reconciles a name changed in the check-to-rename window.
- **Capacity**: 100 GiB by default for a build volume created from nothing; `.cowshed.toml`
  `[build] capacity = "<size>"` overrides it per project. A sparse image costs only its written blocks, so the capacity
  is not an allocation: it is a deliberate cap on build-cache growth. A build that fills its volume fails loudly with
  the volume named, and the remedy is a resize or a prune, never an unbounded cache. A clone and a seed inherit their
  source's capacity: an image clone carries its capacity, and configuration only ever names the capacity of a volume
  nothing was cloned from.
- **Resize**: `cowshed resize <ws|main> --build <size>` grows the workspace's build volume exactly as a workspace image
  grows (01a_apfs.md "Detached growth"), and its seed with it, so every later fork of the workspace inherits the new
  capacity. The workspace's jobs and Nx daemon stop first, because the image has to leave the kernel; any other holder
  of the volume refuses the resize before anything changes. A land never shrinks its target: while the landing volume is
  quiet (Land step 4) it grows to the target's capacity when it is smaller, before the seed is frozen from it, so the
  target and every later fork end at the larger of the two capacities. A filesystem holder that refuses that growth is a
  `landingVolumeBusy` skip, because adopting the smaller volume would shrink the target.
- **ZFS**: a build volume is a dataset, `<pool>/cowshed/projects/<owner>/<repo>/build/<id>`; a fork is `snapshot` +
  `clone`, a seed is a snapshot, and land adoption promotes the adopted dataset (01b_zfs.md, 09_substrates.md).
- A directory is never a build volume. Copying a Rust `target/` tree file by file, even with `clonefile`-backed
  `cp -c -p -R`, takes minutes for a large workspace; cloning the image that holds it takes milliseconds. Minutes per
  land is unacceptable.

The sidecar records: the git tree (`git rev-parse HEAD^{tree}`) the volume was last built at, the workspace that owns it
(the checkout that links it, a target whose seed it is, or the workspace whose link an adoption or compaction moved off
it, **superseded** at that workspace's incarnation), whether it is a **seed** (immutable, see below), its creation time,
and its **stage**. Every operation that writes a build image (an empty mint, a seed capture, a fork's or refork's clone,
a compaction's replacement, a re-adoption's replacement) writes the sidecar **registered** before the image's first byte
exists, naming the owner and the registrant that answers for it: the existing workspace at its incarnation, or the
unfinished create, fork, adopt or re-adopt the lifecycle intent journal records for a workspace that has none yet. It
writes the sidecar **published** only once the image holds what the role promises. A registered seed is nobody's to fork
from. So no build image ever exists that its sidecar does not name an owner for, and a crash leaves the exact id of what
it was creating in that sidecar. Nothing else is derived from it.

### One link per checkout

A checkout reaches its build volume through exactly one symlink, `<checkout>/.cowshed/build`, which points at the
volume's mountpoint. `.cowshed/` is already excluded from git in every checkout (02_workspaces.md). The tools' own paths
are fixed relative symlinks through it, created by cowshed when it mints the checkout and never changed afterwards:

```
<checkout>/target            -> .cowshed/build/target               # one per Cargo target dir capability detection finds, without mbx
<checkout>/.nx/cache         -> ../.cowshed/build/nx/cache
<checkout>/.nx/workspace-data -> ../.cowshed/build/nx/workspace-data
<checkout>/.codegraph        -> .cowshed/build/codegraph
```

In a project with the mbx capability, `target` is not a build-volume link. mbx creates and owns it at the checkout's
first Cargo build, `target -> <build-cache>/mbx/targets/v1/<checkout digest>`, and adds its exact path to the
repository's `info/exclude` itself (mbx docs, "Managed target directories"). Cowshed never creates, discards or relinks
it, and a refresh that finds it leaves it. The source image carries the link to every fork, where it names the source
checkout's managed target, so a fork deletes it (Fork step 4) and the fork's first build places its own. The managed
target never travels: a land leaves the target's managed target where it is, and its next build restores from the store
what the landing workspace compiled.

A land changes exactly one name, `.cowshed/build`, with one `rename(2)` of a new symlink over it. That is atomic: every
path a tool opens afterwards resolves into the new volume. The fixed links mean no tool, no environment variable and no
configuration file ever names a build volume directly.

Membership is physical, not lexical: the controller resolves the link target's parent and the project's build-mount
directory before comparing them. A preferred-path symlink to the fixed mount root names the same directory and is
accepted without rewriting the checkout's build link. A target resolving to another directory, including another
project's build namespace, is refused; the volume id and sidecar ownership rules still apply.

To Git each of these links is a file. A repository's own directory pattern (`target/`) matches the directory a link
replaced and never the link itself. Before cowshed creates any link, it therefore adds an anchored, exact pattern for
the build link and for every build-state link to one managed block in the repository's `info/exclude` (the common
directory's, for a linked worktree). Lines outside the block are left exactly as they were, and entries in the block are
only ever added. A migrated checkout's `git status` shows none of its links, and the repository's `.gitignore` stays the
repository's own.

Mounting a checkout mounts the volume its link names only when that volume's sidecar records the checkout as its owner,
published or still registered (a first touch the link already names, which the next refresh finishes). An image with no
sidecar predates owner registration: the mount refuses and names it. Any other link is stale and is re-pointed at the
one volume recorded as the checkout's. When the checkout owns none and the link names another checkout's live volume, a
target adopted it from this checkout and a `land --no-retire` stopped before reforking it: the checkout takes a fresh
clone of that target's latest seed, exactly as the refork would have. When the checkout owns none and the link names a
volume whose image is gone, whatever sidecar it left, nobody owns what the link names: the checkout takes a fresh clone
of its own latest seed, or, with no seed, loses the link, so its next build-state refresh makes its first volume; either
is said on stderr with the lost volume's id. Every opening's mount settles this, `cowshed doctor --repair` and
`cowshed setup` included. Anything else (several volumes, or none and no adopter's seed for a volume that exists)
refuses. Why: the link lives inside the source image, so a fork carries the link it had when cloned, and a land
interrupted between renaming a target's link and updating the sidecars leaves the target naming the landing volume.
Mounting such a link as found would let the checkout write a volume a target or another checkout owns, or one already
collected.

**Refresh.** Before a job of a checkout is admitted (exec, a land check, Land step 7 output restoration), at adoption
for main, and from `cowshed setup` for every mounted workspace, cowshed refreshes the checkout's build state: it
fingerprints the tracked build inputs (Cargo manifests, `.cowshed.toml`, the build-state capabilities' markers) and,
while the fingerprint matches the one the volume's state records, only restores a fixed link a tool displaced (a real
directory where the link belongs is discarded and relinked, never copied, and reported). When it moved, capability
detection runs again in the canonical job environment (the caller's `CARGO_TARGET_DIR` and the like never name a
checkout's build state): new paths join the volume, held ones never move. A refresh never treats mbx's managed-target
link as a displaced fixed link. For a Cargo project that gains the mbx capability, the refresh removes cowshed's own
fixed `target` link (which names `.cowshed/build/target`), so that mbx's first build places the managed target: mbx
leaves a `target` that is already a symlink alone (mbx docs, "Eligibility and recovery"). The volume's old `target`
directory leaves the volume's state and is deleted in the background like a discarded directory (below), so no fork or
seed carries it again. A checkout with build state and no volume gets its first one (its first touch) at the project's
`[build] capacity`, and the checkout's seed with it, so it is a target from then on. Main's first volume is minted while
`cowshed adopt` stages main's image, at the capacity the adopted checkout's `.cowshed.toml` asks, so the two attaches
wait on `storagekitd` once (01a_apfs.md, "How the APFS host degrades"), unless nothing in the checkout can name build
state (no tracked Cargo or package manifest, no declared `[build] state`, no enabled Nx detector match), whose first
touch makes no volume. Main's first touch links it once main is mounted at the checkout, when main links no volume yet
and asks the capacity it was minted at (the checkout's `.cowshed.toml` may change while main is copied; the first touch
then mints its own at the capacity asked now). A mint that failed fails only a first touch that needs a volume, with the
mint's own error, and is not retried. An adoption releases the volume before it completes whenever its first touch does
not link it (nothing to hold, a refusal, a failure, a volume already linked, another capacity), and an adoption dropped
before that (a cancelled request) releases it as it is dropped, the mint itself when it was still running. A build-state
path that holds tracked source refuses before anything is deleted.

**Owned index links.** `.codegraph/` is build-volume state and follows the same fixed-link contract as other contributed
paths. At the checkout root, `.codegraph -> .cowshed/build/codegraph`; a nested project's contribution preserves its
project-relative path on the build volume. A fork clones the index with the build state, and a land adopts it with that
volume. Refresh keeps the owned fixed link; it does not migrate the index to a real directory on the source volume. Each
workspace indexes its own tree, never another checkout's live index.

The seed-capture writer exclusion is a named list of build-state holders whose on-disk state is a non-transactional tree
of files being written:

- **Nx state** (`.nx/cache`, `.nx/workspace-data`): a task updates database, metadata and output files separately.
- **Cargo build output** (`target/`, including discovered custom target directories), in a project without the mbx
  capability: a build writes its units and fingerprints separately. Under mbx no Cargo state is in the build volume, and
  capture takes no Cargo lock.

Every other contributed path is exempt by default: it is a rebuildable cache with a consistent read, such as a
code-intelligence SQLite index or a lint/type cache, or state a capture cannot tear. Adding a contributed path does not
add it to the exclusion; extending this named list requires its own stated reason. The block-atomic image clone is a
consistent point-in-time snapshot, and SQLite WAL recovery handles an interrupted writer. Capture keeps index bytes on
the build volume and never copies a live database onto the source volume to evade a holder. No holder is killed and no
unmount is forced.

A link displaced between refreshes detaches the checkout from its volume until the next one: what a tool writes there
reaches no fork, land or seed. A tool that removes the path itself (`nx reset`, an `rm -rf` of the path) leaves its next
run to make a real directory there, and a clone of main copies that directory. So `nx reset` clears nothing: it removes
the links and leaves every entry on the volume, and `cowshed cache clear` is the clear ("Clearing a checkout's Nx
state"). `cowshed doctor` reports each build-state path that is not what a refresh would leave (`build-link`: missing, a
real directory, or something else) without changing it, and names `cowshed exec <ws> -- true`, whose refresh relinks it.

A discarded directory is first renamed into `<checkout>/.cowshed/discard/`. That is the same volume, in cowshed's
excluded namespace, and never a sibling in the source tree. The link then takes the path, and the refresh returns
without waiting for the delete, which runs in the background. A target directory can be tens of GiB, and no job waits on
deleting it. A background delete that finds another delete of the checkout running (a refresh's or `gc`'s) waits it out
and then takes whatever is still pending, so what its own refresh moved aside never waits for a later one. A process
that ends first, a crash included, leaves the rest pending. Every later refresh of the checkout resumes the delete, and
`cowshed gc` finishes it, naming each directory it deletes. A refresh whose fingerprint moved first probes the hold its
job must take on the linked volume: an exclusive claim (a release, a seed capture, a compaction move, a cache clear)
refuses there, before discovery's shell activation is paid for an answer the refusal would discard. A discovery job that
has not answered within 10 seconds says on stderr every 10 seconds what phase it is in: still preparing its sandbox
environment, or running as a named process in a named kernel state. It names nothing inferred from other processes: a
guessed cause printed beside a wait is read as fact. If the caller that asked for discovery ends first -- an interrupted
command -- the discovery job's process group is ended and its leader reaped before the caller answers -- or, past the
reap bound, collected later with that said -- so no activation tree outlives it.

Cargo discovery runs at most four queries concurrently within one activation of each selected workspace shell. Answers
and failures retain manifest order; a failed manifest does not hide independent workspaces. The existing tracked-input
fingerprint caches the complete discovered path set, so unchanged refreshes run no Cargo query. This bound changes only
discovery scheduling, not journal identity or failure fences.

Capability detection names the build-state paths (15_capabilities.md, one contribution contract): the Nx capability
contributes `.nx/cache` and `.nx/workspace-data`, and a JavaScript package manager contributes the `node_modules/.cache`
of every package it installed. The Codegraph capability contributes each detected project's `.codegraph/` index, held at
that project's `codegraph` path on the build volume. The Cargo capability contributes each `cargo metadata`
`target_directory` inside the checkout only in a project without the mbx capability. With it, the target directory is
mbx's, and the Cargo capability contributes nothing (15_capabilities.md, "mbx detection", which also refuses a tracked
target-directory override mbx would not manage). A project with no build-state capability and no declared build state
gets no build volume and pays nothing.

A tool's own per-checkout state that no capability names belongs inside one that is named, never in the source tree
beside an output. Content-keyed or per-run tool state is not an output, and written into the source tree it churns the
source image that every fork clones: nextest's extracted test archive is rewritten for every new archive, and lmao's
SQLite trace sink is rewritten page by page on every test run. The nextest extraction therefore lives under the Cargo
target directory (`<target_directory>/nextest-extracted`), and the trace sink in `.cache/lmao`, a directory of its own
that the project declares as build state (`packages/*/.cache/lmao`, below). A package's `.cache/` itself cannot be build
state, because build tools declare their outputs there.

### Declared build state

`.cowshed.toml` `[build] state = ["<checkout-relative path>", ...]` declares build state no capability detects. The
motivating case is a patch-development checkout of an upstream project plus its build tree, kept gitignored inside the
repository: many GiB of incremental build output that no convention file names, so no detector can recognize it. Left in
the source image, it is the largest thing a fork clones and the main source of main's fragmentation (Why two volumes);
on the build volume it is cloned at fork and adopted at land, as a Cargo target directory is only in a project without
the mbx capability.

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
warm, and main adopts it at a land. The refresh reports the discard on stderr with that instruction.

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
across lands, and is rejected. An atomic swap at one path would need mount stacking, and on APFS a process whose working
directory was entered under the covered volume reads the new one, and `diskutil unmount` of the covered device removes
the top mount instead. The swap would therefore be an unmount and a mount, which must skip adoption whenever any process
(rust-analyzer included) holds main's build volume, on every land.

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
latest seed.** Every fork of a target clones that target's seed, never an unguarded live build volume. Capture retains
closed supervised admission and the Nx and existing Cargo-profile protocol guards while descriptor-cloning the mounted
image through the same block-atomic live-capture path as source images. The resulting APFS seed stays detached.

A target's seed is made when the target is created (a clone of the seed it was forked from, or of its first build volume
at its first touch), and **every gated land into the target refreshes it from the landing workspace's build volume** (an
ungated fold into a lane base leaves it as it was, "Ungated folds"): `cowshed land` captures the guarded landing volume
before the target adopts it (Land step 5.4). A named Nx or Cargo holder refuses capture without publishing a replacement
seed. A land made outside cowshed, a plain fast-forward of the target's branch to a tree a live workspace has checked
out (an **external land**), makes that workspace's volume the seed at the target's next fork (Which volume a seed holds,
below). Between lands a **reseed**, the first step of every fork of the target (Fork step 2), refreezes the seed from
the target's live volume when that volume holds a write the seed does not and is not farther from the fork's tree.
Stopping supervised jobs alone is not writer exclusion: the named Nx and Cargo protocols must also admit capture. Each
target keeps only its latest seed; a target's seed is deleted when the target retires.

**No manual seed refresh or warm step exists.** Apart from the explicit re-adoption rule below, every seed refresh is a
step of a land or of a fork: there is no command that refreezes one, no background warm build in the target, and no land
script step. A land script that fast-forwards the target's branch itself is an external land, and the next fork takes
care of it.

Re-adoption preserves the live BuildVolumeId, its owner record, tree and creation time, and carries its warm filesystem
into RAW at the capacity recorded from the kernel before detach (02_workspaces.md §Re-adoption). It leaves every
old-incarnation seed unchanged as retained evidence, rather than relabeling it as a seed of the new incarnation. The new
main receives one fresh RAW seed captured from its verified warm replacement under the same writer exclusion as every
seed. This explicit retained evidence is not an active latest seed and is never silently collected as one. Re-adoption
carries its existing exclusive source `ReleaseClaim` into build publication. A RAW source and its RAW replacement share
the canonical hold resource, so registration and an empty replacement's mint borrow that claim instead of taking a
conflicting shared creation hold. Capture borrows it through seal, restore and the immutable seed mark instead of taking
a second exclusive claim.

**Why a reseed.** A land freezes the seed before the target runs anything on the adopted volume. Everything the target
runs afterwards (its own builds and gates, the land's output restoration, a developer's build in main) is written to its
live volume and never reaches that seed. A fork made afterwards would miss every task the target executed after the seed
was frozen, though its hash equals the target's: the seed has no row for it. A fork of a target is meant to be warm for
what the target ran, not only for what last landed in it, so the seed follows the live volume. Refreezing once more at
the end of the land would not do it: it would capture only output restoration and miss everything the target runs later.

**Behind is one comparison of two modification times.** An image's modification time is the instant of the last write it
holds: the image driver writes the image file as its volume is written, and a clone keeps its source's modification
time. Capture fixes the cloned image's clock as the immutable seed freshness mark before the clone's own extent-map
write. It does not sample a later live-source clock and claim that a cache write after the snapshot was captured.
Supervised admission remains closed through publication: a direct capture retains its exclusive build hold, and a land
retains its stopped supervisor socket lease and shared GC hold. A seed is behind when the live volume's image, its
volume flushed first, was modified after the seed's mark. A fork's own seed keeps its clone's clock, taken before the
live volume mounts; the mount's own writes can leave it reading behind, and the next fork from that workspace reseeds it
as any behind seed — no flush on the fork path samples a live clock to pre-pay that. Output restoration and later work
on an adopted volume reach the seed through the next fork's reseed.

**A reseed excludes the named non-transactional writers, not every open file.** It runs under the target's image lock,
which every fork of the target holds while it clones the seed, so a reseed never deletes a seed a fork is cloning:

1. the source's build hold (`<id>.dmg.hold`, outside its mounted filesystem) is taken exclusively without waiting,
   before anything else; an admitted job holding it skips the capture before any image mutation, retaining the old seed,
   and the skip names each process holding the hold. From here the hold refuses every job admission, as Land's quiet
   capture refuses replacement admission from before its jobs stop: a shed's daemon keeper (04_sandbox.md) restarts the
   daemon step 2 stops as a read-write job, and taken after that stop the hold would lose to the restart;
2. the target's Nx task database may have no holder but the target's daemon, which is stopped as at an adoption (Land
   step 5); any other holder, or a daemon that outlives its stop, skips the reseed. The Nx database open locks remain
   held through capture so another client cannot open a known task database between the holder check and the clone;
3. in a project without the mbx capability, every existing Cargo profile's `.cargo-lock` is taken exclusively without
   waiting; Cargo holds that lock shared for its whole build, so a held one skips the reseed with its lock path and
   processes. The admitted profile locks remain held through capture. Under mbx this step is empty: no Cargo state is in
   the build volume;
4. the live volume and its backing image are flushed, then the existing mounted-source descriptor-clone captures the
   image atomically. Capture does not require unmount or detach; open files on exempt contributed paths do not refuse
   it. The Nx and Cargo guards remain held until the cloned bytes and immutable freshness mark are fixed;
5. the clone's extent proof and owner-record publication complete before supervised admission reopens. The proof counts
   the clone's extents and writes nothing: the copy of the extent map it shares with its source waits for the first
   mutation of either (01a_apfs.md, "Clone cost follows extents, not size"). A job admitted afterwards cannot invalidate
   the snapshot; its later writes make the seed behind again. Dropping the capture claim only unlocks the source's hold;
   it never retires or deletes the source.

Native capture, clone and release operations record each image's allocated bytes before and after, their signed delta,
operation and measurement time, and explicitly process-window disk-write bytes. Telemetry walks no extent map: nothing
consumed those counts, and their three walks cost about 85 seconds around an 81 ms seed clone under concurrent forks.
Compaction's foreground advice reuses the necessary operation's live duration receipt, never diagnostic rows or a new
extent query. Only its child counts the refreshed immutable source before copying; doctor and forkability keep their
required authority counts. See 13_telemetry.md, "Native image observations", for the audit-only measurements.

Land already owns a shared GC hold on its quiet source. Its typed quiet capture retains the stopped supervisor socket
lease that refuses replacement admission, from before the jobs stop through capture and adoption. It does not take a
second exclusive claim, nor upgrade the shared hold: Darwin flock upgrades release and retake the lock and would reopen
the gap. The socket lease is released only before replacement supervisors start, after the immutable seed mark and
adoption (and a kept workspace's refork). The shared GC hold remains through the rest of the build-volume steps. The
quiet capability and the capture method that consumes it are compiled only with the macOS runtime; portable APFS backend
operations do not depend on that platform-only capability. Re-adoption's retained-image runtime operations likewise
compile with macOS, while its durable record data, Git evidence schema and pure command-contract and Git-preservation
tests remain portable.

The guarantee is no capture mid-write by an admitted Nx state holder or, in a project without mbx, by a holder of a
known Cargo profile. It is not a kernel-wide filesystem-writer barrier: an open index reader, editor or other exempt
cache holder does not refuse capture. Neither those holders nor the named writers are force-unmounted or killed.

**Known gap, without mbx only: a host process can create a new Cargo profile after enumeration.** The current profile
locks do not fence that new directory, and mounted capture does not fence a profile created after enumeration. Such a
host build can leave intermediate build-output bytes in a seed. A producer-side capture lease, acquired by every Cargo
invocation before it creates or writes a profile and held for the build, is required to close that gap; no protection
for that case is claimed by the current capture contract. Under mbx the gap does not exist: the managed target is never
in a build volume.

**Which volume a seed holds.** Every build volume's sidecar records the tree it was built at (01_storage.md "Layout"): a
seed the tree it was frozen at, a land's adopted volume the landed tree, a fork's live volume its seed's. A linked
volume's sidecar also records `checkedOut`, the tree its checkout had when cowshed placed the volume there: the fork's
tree at a fork, the landed tree at an adoption. `tree` says what was built into the volume; `checkedOut` says where it
was put. A record without `checkedOut` (one that predates it, or a placement at no tree cowshed knows) never reads as
moved. A fork names the tree it checks out: the target's `HEAD^{tree}`, or under `fork --ref <rev>` the tree of
`<rev>`'s commit. A volume's **nearness** to that tree is _equal_; else the number of paths `git diff-tree -r` reports
between the two trees in the target's repository; else _unknown_ (no recorded tree, or one the repository cannot
compare, said on stderr), farther than any count. Fork step 2 decides, under the target's image lock:

1. **An external land**, into main only. When neither the seed nor the target's live volume is equal, the fork's tree is
   the target's `HEAD^{tree}`, and that tree differs from the live volume's `checkedOut`, the target's branch moved past
   every volume cowshed knows, and a live workspace whose `HEAD^{tree}` is that tree ran its gates there. The seed
   becomes a capture of that workspace's build volume, recorded at the tree. Candidates are tried warmest first (latest
   written). Each one's image lock is taken without waiting (a held one is a skip naming it: its own fork or land is
   running), its `HEAD^{tree}` is read again under that lock, and a candidate whose tree moved is refused: a seed is
   never recorded at a tree its volume was not built at. It is then captured under the same writer exclusion as every
   seed, above, with its own Nx daemon stopped as at a land. When no candidate is captured, each skip is said on stderr
   and the decision falls to the reseed.
2. **A reseed** refreezes the seed from the target's live volume when the live volume is nearer the fork's tree, or as
   near and written after the seed (behind, above). A seed nearer than the live volume is kept however much later the
   live volume was written, and the live volume is not sealed at all: a landed seed beats a live volume built at an
   older tree, and a long-lived reader of the target's volume (an editor, an indexing server) costs such a fork nothing.
   Both at the same tree: the target ran tasks on the adopted volume after its land, or the volume's own mount wrote it
   after its seed was cloned.

A skip names each holder and leaves the seed as it was: the fork is colder, never wrong, and the next fork tries again.

A fork of a workspace whose checkout has not moved since cowshed placed its volume is therefore no external land, even
when its live volume records an older seed tree (forked while its target's seed was behind) and another workspace has
its tree checked out: the reseed takes the workspace's own live volume, and the fork inherits what it built. Step 1 is
main's alone. Plain Git outside cowshed fast-forwards main to a tree some workspace built, and only that workspace's
volume holds it. A session's checkout moves only by work in the session itself (a commit, a rebase, a merge an operator
runs there), so the session's own live volume is what built its tree, and another workspace at that tree, a fork of it
say, holds less. A fork of a session, a lane base included, therefore skips step 1 and takes the reseed: every fork of a
shed inherits that shed's build state, whether it moved since its own fork or not. `target-moved-outside-land` is
likewise reported for main only.

A foreground keeper is an admitted holder. While it holds the source volume, fork takes this busy-skip cold-seed path;
it does not stop the keeper, bypass its hold, or wait synchronously for its release. The warm path becomes reachable
after the keeper's ordinary release, and a subsequent fork can then refresh the seed and hit the latest run. Waiting
outside fork for normal idle release can cost the keeper's remaining five-minute idle lifetime; continued use can extend
it. A bounded test cannot replace this distinction with a real five-minute wait under the unchanged 180s no-progress
limit (08_testing.md).

`cowshed doctor` reports seed age only for main and live fork sources (workspaces with dependants), not every leaf. It
compares the image modification times, never tool-file statistics. `seed-tree-behind` names the seed's tree and the
target's `HEAD^{tree}` (and the number of commits since a matching tree when available); `target-moved-outside-land`
names both recorded volume trees when neither equals HEAD. A next fork tries tree-matched capture; a plain Git
fast-forward performs no output restoration or adoption. Doctor shares one operation-local, target-indexed observation
of build records across these seed probes, retaining per-target claim checks, image modification times and failure
precedence; lifecycle seed lookups remain fresh.

- **Enforced by**: a real-APFS test in which main runs an Nx task after adopting a landed volume and a fork reseeds main
  and hits that task; a keeper-held fork that reports a busy skip, keeps the existing seed and takes the cold path,
  paired with the no-keeper warm-fork hit oracle; a real-APFS test in which main's branch moves externally to the tree
  of a workspace whose volume holds a marker output, a fork of main at that tree inherits the marker without building,
  the same fork with no candidate lacks it, and a candidate whose tree moved is refused with the old seed kept; a
  real-APFS land whose landing volume a host process holds a file of open, which keeps main's previous seed, still
  adopts with every checked Nx output restored from cache, and whose next fork after the holder exits refreezes the seed
  at the landed tree; unit tests of the nearness order; real Cargo build-script rendezvous tests for both an existing
  profile and a profile created after quiescence and lock enumeration; and the sealed-source holder and
  attachment-exclusion tests.
- **Freshness boundary enforced by**: real APFS capture without a job write remains Fresh without tolerance; actual job
  admission is refused during canonical restore, both by a direct capture's exclusive claim and a land's retained
  stopped socket lease; an admitted shell job after admission reopens writes bytes absent from the seed and makes it
  behind; an already-held admission skips direct capture without mutating images or replacing the old seed, and the skip
  names the process holding the hold.

### Seed compaction

A seed is detached and immutable, so rewriting it into contiguous space stops no live shell. Necessary main-seed capture
or attach/mount work **strictly over 10 seconds** advises the existing background compactor, at a foreground fork of
main or a land that froze main's seed. A capture receipt includes the existing source sync and descriptor clone, with
source-bound image-lease admission completed before that work is timed. The attach/mount receipt sums only the actual
attach and mount command work inside their granted disk leases; it excludes lease waits, fsck, metadata,
destination-seed capture and the rest of build-ready. A fork of a non-main source never advises main's compaction from
its own slow work. No timer, rolling window, advice cache, Arrow query or foreground extent walk decides it. A
successful compaction leaves no recent-success guard or success marker: an adopted lineage cannot inherit another
lineage's old suppression. The same-build failure hold remains (Failures). The compactor alone verifies the existing
**500,000-extent** bound on its current immutable source immediately before copying. A seed replaced during the copy is
caught up to (Catch-up); a later qualifying foreground operation can advise another run if one stopped.

- **Why 500,000.** A fork's first write costs 0.1 to 0.2 ms per extent of its seed's map, and every fork pays it twice
  (its volume, then its own seed's capture), so 500,000 extents cost a fork about 140 s. A compaction writes the whole
  seed once and leaves about as many extents as the image has data regions, the floor of a copy that keeps holes (near
  100,000 for a 209 GB seed); larger write chunks and `F_PREALLOCATE` do not lower it. It pays for itself within four
  forks, and a seed growing about 110,000 extents a day is due again after a few days. Compacting at every land would
  rewrite the seed every time and hold main's image lock for minutes.
- **Off the foreground path.** Advice starts `cowshed __compact-seed <project-root>` in a session of its own, holding no
  descriptor of the fork or land but its stdio, its stderr appended to `build/.compacting/log` (the foreground creates
  `build/.compacting` first). The foreground answers without waiting for the child or counting any extents for advice.
  The operation receipt already proves a seed was captured or prepared: advice enumerates no build records and makes no
  seed-existence probe. Only the child's refreshed source determines whether a seed still exists and reaches the bound.
- **The land reports it.** The frozen seed carries `notMain`, `notAdvised` (no usable qualifying live operation),
  `started` (the actual process and log, never an unobserved extent count), `held` (the same-build failure and its age),
  or `failed` (deciding or starting failed, and why). None fails the land. A start whose reaper cannot be created still
  reports started; the reaper failure is recorded as the compaction failure.
- **Failures.** A foreground decision or start that fails, and a compaction ending in an error, write
  `build/.compacting/failed`: the cowshed build that failed (its `BuildId`, 11_shell.md "Draining a supervisor of
  another build") and the error, written aside and renamed into place. Advice by that same build starts nothing and
  reports `held`, so an unchanged cause is not met at every fork or land. Another build can start as usual, since a new
  build is how a fix ships. `cowshed __compact-seed <project-root>` run by hand does not read the record, and any
  success removes it. A compaction that cannot tell whether a gate or a land runs, gives up catching up, or finds
  another one running has not failed.
- **One at a time.** The process takes the project's compaction lock (`build/.compacting/lock`) without waiting and
  exits if another process holds it. It refreshes main's seed exactly as a fork of main does (Fork step 2), under main's
  image lock, without recursively advising another compaction. The copy then holds what main's live volume holds, so the
  next fork's reseed keeps the compacted seed instead of replacing it with a later capture.
- **Only the refreshes, the clones and the publication hold main's image lock.** Still under the lock that the refresh
  took, the seed is cloned to `build/.compacting/<seed>.dmg` (milliseconds; the clone is never written, so a land or
  reseed that retires the seed meanwhile changes nothing it reads). With no lock held, one full extent count of this
  refreshed immutable clone is taken immediately before copying. Below 500,000 it reports the actual count, creates no
  copy and hands the staged clone to cleanup. At the bound it copies the clone's data with plain `pread`/`pwrite` into
  `build/.compacting/copy.dmg`, keeping holes, as `defrag` copies (01a_apfs.md, "Images"); the same count becomes the
  report's `before`, with no separate threshold walk. After-copy, catch-up, forkability and doctor counts remain real.
  The copy runs at normal disk priority and past the cache (`F_NOCACHE` on both descriptors): it holds for every gate
  and land (below), so between them the disk is its to use, and it passes each byte once, so caching it would only evict
  the working set of the rest of the host. Measured 2026-10-25 on this host: throttled (`IOPOL_THROTTLE`), a 367 GB seed
  copied at 26 MB/s, about four hours; at normal priority one copied 209 GB in 440 s. The clone stays until the copy is
  published, for a catch-up to read which blocks a newer seed still shares with it. Forks and lands of main proceed
  throughout the copy and clone the seed as it stands.
- **It holds for lands.** After its first chunk and every GiB after that, the copy, and a catch-up's writes and its
  punches of at most 8 MiB alike, ask the gateway whether a land holds or waits for any repository's turn
  (05_gateway.md, "Land turns"); a land that starts the compaction still holds its own turn. A gate-class job outside a
  land does not hold it: a class is a nice, not a lock (05_gateway.md, "Classes"). While a land runs, the writer holds
  where it is, says once in its log what it waits for, and checks again every **15 s**; it says how long it waited when
  it goes on. If the gateway's answer cannot be read, it stops, and the copy is removed. The seed is left as it was. A
  gateway that is not running holds no land turn: the turns are its own state.
- **Publication.** Under main's image lock again, main's seed is refreshed once more exactly as a fork of main refreshes
  it, so a write main's live volume took during the copy reaches the seed now rather than at the next fork. The copy is
  published only if the seed it holds is still main's latest. It is registered as a new seed of main at that seed's tree
  before its image exists, renamed into place, and given that seed's freshness mark (its bytes are that seed's, so they
  are no newer). It is proven forkable and published, as a captured seed is. Every older seed is then claimed under the
  lock and committed to `build/.deleting` once the compaction lets the lock go, as a fork's seed refresh retires the
  seeds it replaces (Garbage collection). A successful publication removes its prior failure record.
- **Catch-up.** When a land, a fork's reseed or the publication's own refresh replaced the seed the copy holds, the new
  latest seed is cloned aside under the same lock, and the lock is let go. With no lock held, the copy is made to hold
  the new seed's bytes: wherever the new seed's data sits on the same device blocks as the old seed's clone at the same
  offset (`F_LOG2PHYS_EXT`; a clone shares its source's blocks until either is written), the copy is left as it is;
  every other data range of the new seed is written into the copy in place, past the cache, and whatever data the copy
  holds where the new seed has a hole is punched. The copy takes the new seed's length. Writing in place keeps the
  copy's runs (measured 2026-10-26 on the store volume: sixteen 1 MiB overwrites of a contiguous 64 MiB file left its 8
  runs, the same writes into a clone of it made 87). The old clone is then handed to the gateway's cleanup (Leftovers),
  and publication is tried again under the lock, refresh first. Each catch-up writes only what main's seed changed since
  the last, so it takes the time of one land's delta, not of the whole seed; each says on stderr the seeds, the bytes
  written and punched, and the time taken. After **8** catch-ups the seed is replaced faster than the copy follows it,
  and the compaction stops. A copy already under way finishes the seed it started rather than switching to the new one:
  it reads one immutable clone in one pass and takes no lock, a switch would still need the catch-up for the part
  already copied, and finishing writes at most the new seed's changes to the remaining part twice. Physical-layout
  comparisons retain each file's current logical data region across physical extents, as the byte comparison does:
  SEEK_DATA/SEEK_HOLE probes advance only past that region's end, never once per extent. A pure counting test covers
  leading, interior and trailing holes and many extents per region; probe count follows regions, not extents.
- **Main's live volume moves onto the compacted seed.** After a publication, with main's image lock let go, main's live
  build volume is replaced by a clone of its own compacted seed under main's image lock and lifecycle lease. This
  internal move has no public target-only replacement path. It moves only when the live volume holds no write the seed
  lacks: under its exclusive build claim, with main's Nx daemon stopped, its volume is flushed and its image's
  modification time compared with the seed's mark (Targets and seeds, "Behind is one comparison of two modification
  times"), before the replacement is cloned and again once it is cloned, immediately before main's volume is sealed,
  since a host process the claim does not fence can write while the clone is made. The comparison cannot follow the
  seal: the unmount writes the image itself (measured 2026-10-26: a volume nothing wrote read 5,188 ms behind its mark
  once sealed), so it would refuse every move. Between the last comparison and the unmount only a host process outside
  any cowshed job can write, since the exclusive build hold taken before the first comparison and held through the seal
  refuses every job admission and the daemon keeper's restart, and that is acceptable because it is the writer no
  capture fences either (Targets and seeds: the guarantee "is not a kernel-wide filesystem-writer barrier"); one still
  holding a file open refuses the unforced seal. A later write refuses the move and releases the replacement. A job's
  hold, an Nx holder, a file open on the volume (a Cargo build included), a detached main, a later write, a seed
  replaced in between, or a failure of the move leaves main's volume as it is, and the report says why; the compaction
  has still succeeded, and nothing is killed. Main then writes on in contiguous space, so the seeds later captured from
  it stay compact; without the move, the next fork's reseed would replace the compacted seed with a capture of main's
  fragmented volume as soon as main is written. Forks made after the compacted seed is published start compact either
  way, and their lands carry the compact lineage into main.
- **Leftovers.** `build/.compacting` is read by no enumeration of images, records or `build/.deleting`. Every `.dmg` in
  it, the clones and the copy, is handed to the gateway's cleanup by the compaction itself before it answers, and an
  interrupted compaction's by the next one once it holds the compaction lock and before it takes main's image lock: one
  rename into `build/.deleting` under a fresh id, made durable, and a nudge (05_gateway.md, "Deleting released images").
  The compaction cuts nothing: the gateway's cleanup is the only cutter on the store's container. Measured 2026-10-26, a
  compaction that freed its old seed's clone itself beside the cleanup took 67.98 s for its first cut and 1 to 14 s for
  later ones, the cleanup's cuts beside it 14 and 17 s, and unrelated opens on the host waited up to 49 s.
- The process reports one line on stderr: running elsewhere, below the bound with the child's actual count, no seed,
  stopped and why, or compacted with both extent counts, the bytes written, the time taken, each catch-up, and whether
  main's volume moved or why it was kept.
- **Enforced by**: pure injected-duration advice tables below, at and strictly above ten seconds, main and non-main,
  absent observations, and actual attach-plus-mount composition; an injected-clock adapter witness excludes admission
  queueing from command work; the production count seam counts once, makes no copy below 500,000 and reuses its count at
  the bound; the failure hold admits only another build. A test in which the first start, with no `build/.compacting`,
  starts a process that holds none of the land's descriptors; a held copy asks again until it may go on; a copy that a
  stop interrupts removes its copy and leaves the source unchanged; a real-APFS test of the catch-up writing nothing for
  a plain clone of the copied seed and, for a clone with a range rewritten, data written into a hole and a range
  punched, exactly its unshared data, admitting the last write and every punch, after which the copy holds the new
  seed's bytes and holes; a real-APFS test in which a fragmented seed is cloned, copied and published as main's latest
  seed, with at least ten times fewer extents, the same data and holes, and the copied seed's freshness mark, while the
  old seed is committed to `build/.deleting`, and in which a seed a land publishes between the copy and its publication,
  or during the copy, is caught up to, writing only what the land changed, and published with that seed's bytes and
  mark; and a real-APFS test in which main moves onto a clone of its seed only once its volume holds no write after the
  seed's mark, and a write made while the replacement is cloned refuses the move, is kept, and leaves no replacement
  behind.

### Source and build state arrive together

A workspace keeps its own build volume through rebase and gate. A newer warm baseline arrives through a fresh fork,
which carries the source tree and its build state together; there is no target-only build-volume replacement command and
no land-time build-volume picker. This preserves ordinary mtime freshness without touching source metadata, with one
exception: a land harmonizes the target checkout's source mtimes with the landing checkout's before it freezes the
landing volume as the target's seed, the one place cowshed writes a source file's metadata (Land step 5.3). Compaction's
internal move onto the workspace's own seed is different: it proves that the old build volume holds no write the seed
lacks, before cloning and immediately before sealing (Seed compaction).

### Clearing a checkout's Nx state

`cowshed cache clear [<ws>]` (06_cli.md, "Cache clear") deletes every entry directly in each `.nx/cache` and
`.nx/workspace-data` directory the workspace's build volume holds: cached outputs, terminal outputs and the run summary
together with the task database that indexes them, so no row outlives its artifacts and no artifact outlives its row.
The directories stay, because they are what the build links name. `nx reset` is not this operation: it removes
`.nx/cache` and `.nx/workspace-data` themselves, which here are the links (`rmSync` of a link removes only the link,
measured), so every entry stays on the volume and the next refresh links it back.

It fences admissions and refuses instead of stopping work. The workspace must be mounted. Its image lock is taken
without waiting, so a fork, land or compaction move of it refuses the clear. Its `<id>.dmg.hold` is claimed exclusively
without waiting: an admitted job's shared hold or a land's refuses, naming the hold path and each visible holder's PID
and command. The Nx-close protocol then stops only the checkout's own daemon and refuses another database holder by PID
and command. Nx's open locks (`<database>.lock`) are held for the whole deletion, with a second look at the databases
under them, so no Nx client opens a task database meanwhile; those lock files stay. A checkout that links no build
volume is `not-found` (`cowshed doctor` reports its `build-link`). Seeds are untouched: a fork from a seed captured
before the clear still carries its entries until a land refreshes that seed.

- **Enforced by**: a directory-host test in which a job's shared hold, an open task database and a held image lock each
  refuse by name and delete nothing; the clear then empties both directories, counts their entries, keeps Nx's open lock
  and lets go of the claim.

## Fork: `cowshed fork [<src>] <ws>`

A fork names its source: absent `<src>` defaults to main; an explicit source may be any session or integration
workspace. All sources use one mint path, carrying edits and untracked files warm by default, or cleaning with `--clean`
/ `--ref` (02).

1. Flush and descriptor-clone the target's live RAW source image without detaching it (02_workspaces.md). Source
   filesystem crash consistency does not replace build-state writer exclusion. The fork holds the target's image lock
   from here until step 3's capture.
2. **Refresh the target's seed** (Targets and seeds, "Which volume a seed holds"): from a landed workspace's volume
   after an external land, else by a reseed from the target's live volume when that is nearer or as near and newer and
   has no writer.
3. Capture **the target's** latest seed image into a new build volume owned by the new workspace: register it and
   `clonefile` the seed, nothing more, and let go of the target's image lock. Then, reading only the clone, prove it
   forkable, publish and attach it, and point the workspace's `.cowshed/build` at it. Clones preserve every file's bytes
   and mtime; Cargo freshness depends on that (rule "Clones preserve mtimes").
4. Delete `nx/workspace-data/d` in the new build volume: a daemon record names another checkout's process and socket
   (rule "One Nx state per checkout"). Delete the checkout's inherited `target` link when it names an mbx managed
   target: it names the source checkout's managed target, and the fork's first build places its own.

The lock covers what reads the target: the clone of its image, its seed refresh and the capture of its seed, all one
hold, so the clone's checkout and its build volume come from one state of the target that no land or reseed changes
between them. Nothing under the lock writes a clone of the fork's own: a clone's first mutation, whatever and wherever
it is, makes APFS copy the whole extent list it shares with its source, and only that first mutation pays; it is the
clone's read-write mount, after the lock is let go, so the hold is the `clonefile`s'. A step 2 reseed captures, proves
and publishes the target's new seed under the lock, which a fork does only when the target's live volume is nearer its
tree than the seed. The hold is said as `cowshed: apfs source-lock held=<duration>`. A git-worktree fork keeps the lock
to the end: its initializer registers it in main's repository.

After the lock is let go, the build volume's attach and mount run beside the cloned image's attach and mount: every
attach waits in the host's one `storagekitd` queue (01a_apfs.md "How the APFS host degrades"), and the two in flight
together wait in it once. The new workspace's own seed is a clone of its live volume made before anything mounts it, so
it holds exactly what the target's seed held. The checkout's link is written only once its checkout is staged; a fork
that never stages, or whose mount fails after publication, releases the volumes made for it.

The fork is warm for everything the seed's volume held when it was last frozen: at the latest land, at the next fork
after an external land, or at the latest reseed, which is this fork's own step 2 unless the target's volume had a writer
or is farther from the fork's tree than the seed. If the fork's tree has moved past its seed's (a land skipped its
freeze, or `--ref` names a tree no volume was built at), the fork builds the difference incrementally, as an edit would.

## Land: `cowshed land`

Land's contract is 02_workspaces.md "`cowshed land`": a gated land lands the head the caller validated into its target
(main by default, an integration workspace with `--into <target>`), fencing the target's head under its repository lock,
and never rebases on its own. Its check and output restoration build nothing on the target: it adopts the workspace's
build volume. Every step reads the same for main and for an integration workspace; "the target" is whichever one it is.
An ungated fold into a lane base rebases the unit onto the lane base's branch by Git alone and runs none of steps 2 and
4 to 7 ("Ungated folds", below). Every delivered target also runs its ordinary canonical shell refresh before retirement
(02_workspaces.md, land); that entry may run its normal shell tasks, but never reruns a check or adopts the landing
unit's export.

1. **Refuse a dirty tree**.
2. **Validate in the workspace**: the caller's check (`--check`, for an Nx project `nx run-many -t lint test build` over
   what the workspace changed) runs in the sandbox against the workspace's own build volume. Only the delta builds. In a
   project with the mbx capability the check runs at normal mbx priority (05_gateway.md, "Classes").
3. **Update the target** under its repository lock (02_workspaces.md "`cowshed land`" step 3), which the land holds for
   this step alone: every step after it runs with the lock released. When the target's checkout has the target branch
   checked out, the branch fast-forwards through that checkout and the land continues at step 4. When nothing has the
   target branch checked out, the land compares-and-swaps the branch ref and **skips steps 4 to 7**: the target's
   checkout holds another branch, so its build volume and seed are not the landed branch's. The target keeps its build
   volume and its seed; nothing is quiesced, frozen or adopted.
4. **Quiesce the landing workspace.** The land holds the landing build volume (the shared `<id>.dmg.hold` a job holds,
   GC below) from before the workspace's jobs stop until its build-volume steps end (after step 7), so no collection in
   any process releases it while step 6 renames the target's link and records the target as its owner. Its supervisor
   stops the workspace's jobs, and the stopped supervisor socket lease stays with the quiet volume through capture and
   adoption, refusing a replacement supervisor's job admission. Its private retire call carries `ControllerOwned` Nx
   policy: the actor gives up keeper-record cleanup, not admitted-job retirement. Land's final retirement carries the
   same policy; ordinary lifecycle callers explicitly select `Recorded`. An old supervisor that cannot accept the
   controller handoff is refused rather than generically drained. Before that retirement the controller reads each
   landing Nx daemon's plan from the job group ledger: a record only nominates a pid, and only a process the ledger
   proves is the job's own is signalled, with its exit and the children it forks watched first (04_sandbox.md). After
   retirement it attempts `SIGTERM` through that identity, reports whether the signal was delivered, runs a one-shot
   `nx daemon --stop` only to remove the daemon's socket (its failure is reported and never aborts the land), waits up
   to the shared 10 s grace, and then ends the plugin workers the daemon forked. A daemon the ledger does not prove the
   job's, or one that outlives the grace, skips adoption as `LandingDaemonStayed`; an unproven daemon is never
   signalled. A daemon started detached outside the ledger is that skip. An unreadable or malformed daemon record or job
   ledger instead skips as `LandingDaemonUnverifiable`, carrying the record path and reason, before quiesce or any
   signal. Nothing is signalled through a record alone. The landing build volume's Nx task database must then have no
   open file descriptors, read without signalling. If it still does, adoption is **skipped** and reported, as for the
   target below. This settles supervised jobs and Nx; the later seed capture retains Nx open locks and, in a project
   without the mbx capability, existing Cargo-profile guards through capture and publication. Other contributed paths
   remain exempt under the named-list rule (Owned index links). Mounted capture replaces the former detach-and-seal
   barrier; the new-host-profile gap remains as documented in Targets and seeds.
5. **Close the target and freeze the seed.** Outside the repository lock, which fenced only the head: what keeps these
   steps correct is the target's Nx open locks and image lock below (rule "The adoption needs the target's Nx database
   closed"):
   1. query the open file descriptors of the target's current Nx task database (one query on one file). If any holder is
      not the daemon named by the target's `nx/workspace-data/d` record, skip;
   2. stop that daemon as stock `nx daemon --stop` does, a SIGTERM to the pid taken from the same record read that
      verified it live, and wait for its exit on a process-exit event (it restarts on the next client). A test with a
      real stock daemon pins the equivalence: after this stop, `nx daemon` starts cleanly. Query again; any holder at
      all, including a daemon a host client started in between, means skip;
   3. take the target's image lock and **harmonize the target's source mtimes** with the landing checkout's. A
      fast-forward rewrites only the files it changes, so a file the landing checkout rewrote on its rebase — after its
      volume had built a unit from the bytes that file held before the rewrite — holds the same bytes in the target with
      an older mtime than that unit: paired with the landing volume by the freeze below and the adoption at step 6,
      Cargo's mtime freshness would judge the unit fresh and reuse it over the landed bytes, and an ignored generated
      output in the target can hold other bytes altogether. The paths reconciled were enumerated before the
      fast-forward: the index's tracked files (`git ls-files --cached`) and every regular file on disk that a declared
      Nx output names, for every target and configuration of the landing checkout's resolved graph, fallback outputs
      included, whether or not a run selected the task (the plugin's `existingOutputFiles`,
      `packages/nx-plugin/README.md`, "Cowshed Build-State Output Policy"). A root whose package graph does not resolve
      `@smoothbricks/nx-plugin` has no policy naming its outputs, so only its tracked files are reconciled and stderr
      says so, while any other failure to enumerate them, a plugin present but throwing or missing a module of its own
      included, refuses the land before the fast-forward, with the target unmoved. The enumeration runs as a read-write
      job of the landing workspace, in the shell its other jobs activated (`11_shell.md`, "Shell activation and process
      reuse"): a read-only job keeps its environment in exec temp, so its port-0 loader options name another preload and
      that activation misses, and the devenv evaluation that follows cannot write the checkout's `.devenv`. A generated
      build input that no task declares as an output and the index does not track is outside this contract: nothing
      enumerates it and nothing reconciles it. Both checkouts are read through held directories and no-follow opens, so
      a planted link redirects neither a read nor a timestamp write outside either checkout; a path that is not plainly
      relative is refused, and one that is not a regular file in both checkouts — absent, a link, a directory or beneath
      a link in either — is skipped. Files whose mtimes are already equal are skipped without reading a byte. Where they
      differ, a tracked file, whose bytes the fast-forward made equal, takes the newer of the two mtimes. A declared
      output — generated and ignored, so its bytes can differ, and reconciled as an output even when the index also
      tracks it — has its bytes compared: equal bytes take the newer mtime in the same way; different bytes give the
      target file the time of this pass, one time taken when the pass starts, unless it is already newer, so those bytes
      are newer than every unit the landing volume built from the landing checkout's bytes. No byte of either checkout
      changes, only a target file's mtime, and no mtime ever moves back. The pass is reported on stderr with the tracked
      and output path counts, the regular files examined, the files advanced to the landing checkout's newer mtime, the
      outputs whose bytes were compared, the outputs republished at the pass's time, and the time the pass took;
   4. admit the landing volume's Nx state and, in a project without the mbx capability, existing Cargo profile locks
      immediately before capture. A live task-database holder is `landingHeld`, a process opening that database is
      `landingOpening`, a daemon that outlives its stop is `landingDaemonStayed`, and a held Cargo profile is
      `landingBuilding`, with the resource and holder named. Retain the Nx open locks and any admitted Cargo locks
      through the mounted-source capture. Every other contributed path is exempt under the named-list rule above.
      Capture does not unmount the live volume or kill an index reader. On success, publish the target's new seed and
      claim its previous seeds, released once the land lets go of the target's image lock (Garbage collection). Writes
      after the snapshot reach the seed through a reseed (Targets and seeds); the documented new-host-profile gap is not
      claimed closed.

   Resolving either build volume for mounting takes its per-image lease **before** reading the attachment inventory. A
   recovered attachment owns a raw IOMedia pin across the subsequent kernel mount-table read and mount, just as a
   freshly verified attachment does (01a_apfs.md, "Mounts"). A concurrent image replacement may finish while that lease
   is awaited: the lookup then resolves its restored device, not the earlier device name. Once pinned, a replacement
   cannot detach that attachment before the mount decision completes. Reading the attachment before awaiting the lease
   would let a land's `close-target` pin open a device a concurrent operation has already detached, after Git delivery
   but before the build-link swap. No retry or wider target-lock interval substitutes for this identity handoff.

   The mount-table decision is made under the image lease reacquired for mounting, not in the gap after attachment
   recovery releases its lease. A second mounter can win that gap without changing the pinned device. The image lease
   and raw pin remain held through the decision and command. If `mount_apfs` reports `Operation already in progress`,
   cowshed observes the mount table once more under that same authority: only the expected volume at its canonical
   mountpoint is adopted. An absent mount, a different device or mountpoint, or an unreadable table refuses with the
   image, expected device and mountpoint and the original command failure. There is no blind mount retry; unrelated
   command failures are never converted into success by a subsequent mount observation.

   The land holds the target's image lock from 5.3 until 6.4 has renamed the target's link and a kept workspace has
   captured its fresh clone of the target's latest seed (the new one, or the previous one retained by a skipped
   capture); the clone's mount and link run after the land lets go. Every fork of the target holds the same lock from
   before it reads the target's seed and link until it has captured its own (Fork steps 1 to 3), and so does every
   reseed, so neither reads a seed or a volume the land deletes. Without it, a land that freezes main's seed and
   releases main's previous volume while a fork of main is under way deletes the seed the fork has read, and the fork
   fails (`sync source image …: No such file or directory`). A land that finds the lock held says so on stderr and waits
   for it. It waits with the target's daemon already stopped (5.2), so the longer it waits, the likelier an Nx client
   restarts it and 6.2 skips the swap, reported as any skip is.

   A skip at 1 or 2 still freezes the seed (5.4) and skips the swap. A skipped capture at 5.4 keeps the previous seed
   and still swaps.

6. **Adopt the build volume.** Under the Nx open locks and the target's image lock:
   1. take, without waiting, stock Nx's open lock of each of the target's task databases (the database's path with the
      `lock` extension; every Nx process locks it exclusively before it opens the database and lets go once it has,
      pinned Nx 23.2.1 `native/db/mod.rs`). A lock some process holds means an Nx process is opening the database: skip,
      and name it. Held, the locks keep every Nx process from opening a database until the rename is done; one that
      starts meanwhile waits, then opens the database by its path, so through the moved link;
   2. query the target's task database once more: a holder that opened it since 5.2 means skip;
   3. delete `nx/workspace-data/d` in the landing build volume, then copy into it the target's record of a live daemon
      that runs outside every sandbox (the host daemon a client started since 5.2);
   4. `rename(2)` a new `.cowshed/build` symlink over the target's, naming the landing workspace's build volume, and let
      go of the locks;
   5. hand ownership in the sidecars: the target owns the adopted volume, and its background label becomes
      `build <target>`; its previous volume becomes superseded under the target at its incarnation and is released
      before land returns (GC below), unless a running job's hold still owns it, in which case it stays the target's and
      that job's end releases it. This applies with `--no-retire` too. Nothing links it any more, so its release starts
      once the land lets go of the target's image lock, with the release of the seeds its freeze replaced, and runs
      beside what the land does in the target next (its supervisor, a kept workspace's fresh clone, the adoption check
      of step 7), so the release's unmount, detach and commit rename can overlap them. It frees no byte before land
      returns: the gateway's cleanup frees the committed image off the land's path, and the land never waits for it (GC
      below). It needs no image lock: a fork or reseed that takes the lock after the rename reads the adopted volume,
      and none that held it before the land took it still runs.

   A skipped swap is reported in the land report with each holder's pid and command. The target keeps its build volume
   and builds the landed delta incrementally the next time anything builds there; forks start from the new seed until
   the target's own volume is written again, and then from a reseed of that volume, which holds the target's own work
   and builds the landed delta as the target does. A skipped swap is never wrong, only slower.

   Every seeded/unseeded and adopted/skipped result is also a durable `LandOutcome` commitment, including every
   `AdoptionSkip` and its holders (13_telemetry.md §"Controller audit record schema"), not just cache misses.

   Cargo state takes no part in adoption. In a project with the mbx capability the target keeps its own managed target,
   and its next build restores from the mbx store what the landing workspace compiled [INFERENCE: until the A/B
   gate-after-land cell measures it].

7. **Restore the checked outputs (2b).** Checks run **once**, in the landing workspace before delivery. Capture each
   check's attributed stock Nx run summary immediately, in memory for this land only. After adoption, a cowshed-owned
   cache-only helper reconstructs that run's declared task graph through stock Nx's argument parsers and graph builder.
   Explicit `run`, `run-many` and infix task commands use the same stock parser paths. Graph-query selections
   (`affected`) are not reconstructed; restoration reports them unsupported. Its task IDs must equal the captured IDs; a
   different graph refuses restoration and is reported, never expanded. Hash and restore dependencies before their
   consumers, using only successful entries in the adopted local Nx cache.

   Each recomputed hash must equal the source check's captured hash; a different input identity is a miss, never a
   different cached result silently substituted for the validated one. Declared outputs live on the source volume, so
   adoption of the build volume alone does not materialize them.

   No check shell, task executor or task-execution hook runs in the target. A missing result, an uncacheable task or an
   unrestored dependency is a finding, **not permission to execute it**; non-Nx or unattributed checks are not replayed.
   The report counts only actual cache restorations as hits, lists misses with their inputs/reason, and reports helper
   failures honestly. The land still succeeds because validation already passed. Restoration performs hashing and
   filesystem work; it is not a promise of a free configuration lint. Cross-checkout cache identity and output
   availability remain enforced by the real restoration and warm-fork tests.

A source check's run summary (`cache/run.json`) is attributed only when Nx's `run.startTime` is not before the check was
spawned, its `run.endTime` is not after the check exited, its command is one the check spells, and it has a task for
every target that command names. An ambiguous or foreign summary leaves the check **unattributed**, with the reason, and
supplies no restoration authority. Stock Nx writes a summary once at run completion and records no pid or invocation;
capturing it immediately avoids treating a later check's summary as the earlier check's. These summaries are transient
byproducts of the completed checks, not another validation ledger.

The same attributed runs are what the land publishes to the project's Nx cache once every check has passed
(05_gateway.md, "/nx/v1/cache/{hash}"): each run's task hashes, local hits included, from the landing checkout's own
cache, as a gate-class job of the landing workspace. An unattributed check publishes nothing, and a failed publication
is said without failing the land.

8. **Retire the workspace**. An adopted build volume is now the target's and does not retire with it. After a
   compare-and-swap at step 3 nothing adopted it, so it is still the workspace's.

A land into main whose capture published a seed uses that capture's necessary-work receipt to advise its background
compactor (Seed compaction); the child, not the land, decides whether its refreshed immutable source reaches the bound.

### Adoption copies nothing

The target adopts the landing volume exactly as the land quiesced it. Nothing of the target's previous volume is copied
into it: what the target and other lands ran since the landing workspace forked stays in the previous volume, which is
released (step 6.5). Those results reach later forks and sheds through the project's Nx cache instead: every gated land
publishes its checks' attributed runs to it, and every shed reads it (05_gateway.md, "/nx/v1/cache/{hash}"). A rebase is
Git alone (02_workspaces.md, "`cowshed rebase`"): it computes no task hash, stops no Nx daemon and copies no cache
entry, and the workspace's next gate reads what its target ran from the same cache.

**Why the local Nx cache is not shared between checkouts.** Pinned Nx 23.2.1 finds a cached task only through a row in
the checkout's own task database (`NxCache::get` is `UPDATE cache_outputs … WHERE hash = ?1 RETURNING`), so an entry
another checkout wrote into a shared directory, with no row here, is a miss. Its `put` removes the task's directory
before writing it again, so two checkouts writing one hash, or one writing while another restores it, destroy each
other's payload (`packages/nx/src/native/cache/cache.rs`). The cache and its database are therefore one pair per
checkout, and sharing between checkouts is the remote-cache path, the gateway's accepted-task service.

- **Enforced by**: a directory-host test in which the target's volume indexes an Nx entry the landing volume lacks, and
  after quiesce, seed freeze, close and adoption the adopted volume indexes only its own entry and holds none of the
  target's files.

### Stacks and merge queues

A stack of changes on top of main (a PR stack, a merge queue, a lane) is a chain of targets, and the same three moves
apply at every level:

- Units fork the stack's head target and clone **its** seed, so they start warm with every change already landed into
  the stack, not just main's.
- A gated unit lands into the stack's head target: the head adopts the unit's build volume and reseeds. Without this,
  every fast-forward into an integration workspace would leave it cold and the next unit would rebuild what the previous
  one built — the same problem build volumes remove from main.
- When the stack itself lands into main (its base workspace's close-out, 02_workspaces.md "Lanes"), main adopts the
  stack base's build volume and reseeds. The stack's own seeds retire with its workspaces.

A stack of depth `n` therefore pays one incremental build per landed change, at the level where it lands, and nothing
again at any level above it. A lane whose units fold ungated pays one build in all, at its base's close-out into main.

### Ungated folds

A lane unit's ungated fold into its lane base (`land --into <lane> --ungated`, 02_workspaces.md Lanes) moves Git state
without a check or unit-volume adoption. Its rebase is the ordinary rebase, Git alone. Its land skips Land steps 4 to 7:
it quiesces no landing volume, closes no target Nx state, freezes no seed, adopts nothing and restores no output. It
then refreshes the target's shell through its canonical supervisor; ordinary entry tasks are not a gate or output
restoration. No landing unit's volume replaces the lane base's own volume or seed. The unit's volume is still the
unit's, as after a compare-and-swap at Land step 3, so step 8 retires it with the unit and collection releases it like
any retired checkout's volume (Garbage collection): nothing is adopted and nothing is left behind. The land report's
seed is `unchanged` and its adoption is `skipped` with `reason:{kind:"ungated"}`, which its `LandOutcome` records
(13_telemetry.md).

Main refuses `--ungated` by name: a land into main is where a tree is gated and its built volume adopted, so main and
every fork of it stay warm. A lane base whose units fold ungated builds the lane's whole delta once, incrementally on
its own volume, at its own gated close-out into main, and main adopts that volume. A fold does not build or restore its
delta on the lane base; the target's ordinary shell-entry tasks may write their normal state. A fork meanwhile takes the
seed the lane base already had and builds the folded delta incrementally, as an edit would.

- **Enforced by**: unit tests of the land plan, in which an ungated land into a lane base runs no check or volume
  adoption and retires as `rm` does, a gated plan is unchanged into main and into a lane base, and main, `--check` and
  `--push-only` refuse an ungated land; unit tests of an ungated land's rebase, in which a unit behind its lane base is
  replayed onto the lane base's tip and fast-forwards it, a unit already based lands its own head with no fetch and no
  replay, a conflicting replay is rolled back, names its conflicted paths and moves nothing, and a fast-forward refused
  after that rebase names the ungated land's retry while a gated land's names `cowshed rebase`; and a directory-host
  test in which main refuses `--ungated` by name, a rebase of the unit into its lane base replays by Git alone, an
  ungated land of a unit a lane-mate's commit left behind rebases it and fast-forwards the lane base in one call, only
  the lane base's branch moves, the land reports the `ungated` skip, every build volume record but the unit's is
  unchanged, and the unit's volume is released with the unit.

## Garbage collection

- The last checkout link is the reclaim moment. Removing or retiring a checkout, adopting a replacement, failing a fork,
  or superseding a seed releases every volume that no checkout links and no target keeps as its latest seed. `rm` and
  `land` (including `--no-retire`) run collection before returning, not only on a later explicit `gc`.
- Ownership is a job hold, a land's hold on its landing volume, an unfinished create/fork/adopt in the lifecycle intent
  journal, or a creation's own hold on the volume it registered, not the kernel's refusal to unmount. Every admitted
  cowshed job holds a shared flock on the build volume's `<id>.dmg.hold` for its lifetime; release must claim it
  exclusively. A running job therefore keeps main's previous volume across a swap, and that volume stays recorded as
  main's (superseded, at main's incarnation) while it does: when the job ends, its supervisor has the controller release
  a superseded volume then and waits for that release before it reports the job ended; a release that cannot run or
  fails is the job's terminal failure and leaves the record naming its owner, never a later collection's work. Job
  admission takes its hold without waiting and refuses while any exclusive claim has the volume (a release, a capture, a
  compaction move, a cache clear, a dry-run probe), naming it claimed, not released. A land's hold waits out an
  exclusive claim instead, and then refuses only when the image is gone: only a release deletes it, so a capture, a
  probe or a release that declines under its claim delays such a hold and never refuses it.
- A held lifecycle or image lock likewise defers release. A superseded seed is claimed under its target's image lock,
  which the land and reseed paths hold while they replace it, and released once that lock is let go: the claim keeps
  collection, and a later land or reseed of the target, off it, so none releases a seed another is retiring, and no
  seed's rename, sync or gateway nudge runs under the lock. A claimed seed is never its target's latest seed, so no fork
  clones it: on a tie of creation times (recorded to the second) its unclaimed successor is the newer, and a newest seed
  a release claims (its target retiring) leaves the target no seed rather than an older one. A seed already claimed when
  a land or reseed reads the target's seeds is one an earlier one is retiring, and is left to it, as is one a retiring
  workspace's own release finds claimed. A land retires its seeds once it lets the target's lock go, and a fork once its
  create has let every lock go (a git-worktree fork holds its source's until its initializer ends), whether or not
  either succeeds; only a crash, or a retirement that fails and says so on stderr, leaves a superseded seed recorded
  under its target, for the next seed's retirement or collection.
- Collection decides from a snapshot, and a land can move a target's link onto a volume after it: an `rm` that retires
  the landing workspace while that workspace's land still runs sees the landing volume linked by nothing, and once it
  releases it, main's link names a volume that no longer exists. So each release claims the volume first and reads again
  under the claim: a volume a checkout's link names now is that checkout's and stays, and one whose sidecar is no longer
  the one the collection judged it by is left to the next pass. Whatever makes a volume someone's holds it while it
  does: a land from before its landing workspace's jobs stop until its build-volume steps end, a creation (an empty
  mint, a seed, a fork's volume) from before its sidecar is registered, ahead of its image, until it is published.
- A creation that fails after registering releases what it registered, image and sidecar, before it answers. A creation
  a crash interrupts leaves its registered sidecar: the next open of the project, after it recovers the lifecycle
  intents and before it answers, reads every registered sidecar and releases each whose registrant is gone, by that
  exact id, image and sidecar, as `interruptedBuildVolume`. A registered sidecar's authority is exact, never a workspace
  name a later verb may reuse: its live creator's hold on that id (a creation holds the volume from before its
  registration until it publishes it, and an adopt's mint is held by the adopt until its first touch publishes it or it
  releases the mint), an unfinished re-adoption's journaled record naming that id, or a detached checkout's unreadable
  link. One such authority still owns stays with it; one whose release fails, or whose sidecar or image cannot be read,
  refuses the open, naming each, with its sidecar still naming its owner. Every collection decides registered sidecars
  the same way. No pass looks for images nobody owns: an image with no sidecar predates owner registration, is named
  with its path on every pass, and is never released on a guess.
- Release retains the volume's owner record and exclusive claim while it requests a non-forced unmount and allows a
  bounded wall-clock grace for unmanaged holders to release it. Time spent waiting on command execution or a disk lease
  counts, not only requested poll sleeps. If the kernel still refuses, cowshed forces the unmount and detaches the
  image. At the initial unforced refusal it records the holders and names each pid and command, distinguishing release
  within the grace from a forced unmount. Editors or indexers without a cowshed job hold cannot retain a retiring build
  volume indefinitely. Force revokes their access to the volume; it does not kill them.
- A release commits and frees nothing. The operation that retires the owner or replaces the volume (`rm`'s retirement,
  `gc`, a land's release of its target's previous volume and of the seed its seal replaced, a compaction move's release
  of the volume it replaced, a job's end releasing a superseded volume, a failed creation's release of what it
  registered, a fork's seed refresh) detaches the image and commits its release with one `rename(2)` of it to
  `build/.deleting/<id>.dmg`, inside the images directory and so on the store volume, and syncs both directories: before
  the rename the volume is its owner's, after it no hold, mount or fork reaches it, and a hold taken after it finds no
  image, as before. A rename the kernel refuses across volumes is the release's error, never a copy. The rename never
  replaces a file: an id is never reused, so an entry already under its name is the same image's own committed release,
  and a rename over a file would free the file it replaces in one go. The commit never opens the image: it reads none of
  its bytes and needs no access to it, and every deletion syncs the `.deleting` directory and its parent before its
  first cut, so none cuts the entry before the rename is durable. Once the rename is durable, the operation nudges the
  gateway (below), removes the owner record, hold file and mountpoint, appends its deletion-log line and answers, none
  of it waiting for the free. No verb frees a byte of an image and none finishes another operation's entry. Nothing
  depends on those bytes being freed: no record, link, hold or mount names the entry, and nothing mounts, forks or reads
  it. Only space lags: until the gateway's cleanup finishes the entry the store's container still holds the image's
  allocation, so free-space figures and the container's extent count lag the commit, and an entry no gateway was nudged
  for waits for the next listing. A failure refuses with its path and cause: before the rename the owner record stays
  for the same operation's retry, after it the entry stays for its deletion and the retry finds the commit already made,
  makes its rename durable, nudges the gateway and answers. A nudge the gateway does not take is no failure of the
  release (below). A held job retains its registered volume ownership; a later sweep is not a substitute for completing
  the owning lifecycle operation.
- The free is not a verb's work, because the verb waits for it and keeps its turn and locks meanwhile. Cutting at a
  bound (below) bounds what the container's other writers wait for, not what the verb that cuts waits for. So the free
  runs in the gateway, which holds no turn of any verb and no lock but the entry's own, and the verb only nudges the
  gateway about the entry (05_gateway.md, "Deleting released images").
- `build/.deleting/` holds the committed releases of build volumes and seeds: an entry exists from its operation's
  rename to its unlink. Every other image of the project (a removed workspace's, a staged clone's, an orphan `gc`
  collects) commits the same way into `sessions/.deleting/`, a removed workspace's as `<ws>-<incarnation>.dmg` and any
  other as `reclaimed-<fresh 32-hex>.dmg` (05_gateway.md, "Deleting released images"). Right after the rename is durable
  the releasing process nudges the gateway of its host namespace with one one-shot `released` control request and
  returns without waiting, truncating nothing and starting no process (05_gateway.md, "Deleting released images", for
  the request and its answer). A gateway that is absent, does not answer or refuses is said once on stderr, and the
  entry waits in `.deleting` for the gateway's next listing; the release stands, its rename being the commit. Nothing
  mounts, forks or reads an entry, and nothing is opened through a link: the images directory and the deleting directory
  are held by descriptor, a link or any other non-regular file at the image's name or an entry's refuses or is named and
  left, untouched.
- The gateway's `image-cleanup` thread lists every `.deleting` directory on each pass and frees the entries it lists,
  oldest first, one entry at a time. Each pass lists every entry in each project's `build/.deleting` and
  `sessions/.deleting`, the same two directories of every archived project (`.retired-projects/<operation-id>/`) and the
  store's own `<store>/.deleting`. So a gateway restart or a reboot frees every entry left, an image cut short mid-free
  included. The order it frees in and what it names and leaves are the gateway's (05_gateway.md, "Deleting released
  images"). Before an entry's first cut it syncs the directory and its parent, because the operation may have died
  before syncing its rename; listing syncs nothing.
- In a directory store (the test substrate, 08_testing.md "directory_*") a release only renames the payload tree into a
  `.deleting` directory (a workspace-side payload into `<project>/sessions/.deleting/<name>`, a build volume into
  `build/.deleting/<id>.dmg`) and refuses no holder: a process holding anything inside keeps what it has open. The verb
  deletes nothing and nudges nothing (it says so once per process), because a tree can be freed only with the directory
  host's holder census, which runs in-process. A released payload stays in `.deleting` until its test store goes, and
  `DirectoryExecutionHost::finish_build_releases` can finish build-volume trees, which names and leaves a tree a process
  still holds anything inside, never deleting it from under its holder. For this substrate, retry confirmation and the
  finisher's listing, accounting and recursive deletion resolve each child against its held parent descriptor, never the
  deleting directory's replaceable pathname. Holder discovery uses the held directory's current kernel name with
  entry-identity checks around the census; a changed name refuses the pass. The payload is opened only after that census
  (so the observer is not itself a holder), checked against the observed identity, and both the deleting directory and
  the source directory are synced for that entry before its first deletion.
- Freeing an image never takes one go. One `unlink` of a written image is one free of its whole extent tree, and every
  writer of the store's APFS container waits behind it: a long free hangs every `cowshed fork` and `ls` on the host. So
  a deletion cuts the image from its end with `ftruncate(2)`, 1,024 physical runs (`F_LOG2PHYS_EXT`) in an entry's first
  cut, and unlinks it once it is empty; the table is `STEP_EXTENTS` in `storage/apfs/release.rs`. A cut's length grows
  with its extents, so the gateway's cleanup starts each entry at 1,024 runs and sizes each next cut from the rate the
  one before it took, toward 250 ms under the 500 ms a fork may wait (at most twice the last cut, at least 64 runs;
  `next_cut`; 01a_apfs.md, "Freeing an image"). The bound is on runs reclaimed, not time inside a kernel call. A fresh
  clone's first cut does whole-map first-mutation work inside one syscall, which a step limit or yield cannot split. An
  observed shared-map flag does not retain its other owner, so no flag-based shortcut bypasses the cuts. See
  01a_apfs.md, "Freeing an image", for the reasons and limitations. The image's length is the free's journal: a cut ends
  on a run boundary, so a free that did not finish (a crash, a gateway restart) leaves a shorter image that the next
  gateway listing frees, and nothing else records progress. The gateway frees one entry at a time, cutting each at once
  (05_gateway.md, "Deleting released images"); it holds the entry's `flock`, and no image lock, for the whole free. The
  free runs at normal disk priority: a thread throttled to background priority (`IOPOL_THROTTLE`) holds the container's
  work while its own metadata reads wait, so the same free takes longer and the writers beside it wait longer.
- Workspace images containing the only copy of dirty or unlanded content keep their separate recovery rules; rebuildable
  build images do not use that preservation mechanism.
- Every release that commits an image, a seed's or another volume's, whatever asked for it (collection, a land's release
  of the target's previous volume or superseded seed, a compaction move, a fork's seed refresh, a fork that never
  staged), appends one line for the image to the project's `deletion-log.jsonl` (01_storage.md) once its rename is
  durable: op `release-build-volume`, kind `build-seed` when its sidecar recorded it as a seed and `build-volume`
  otherwise, and the workspace that sidecar named, read before the owner record is removed. A target's previous volume
  is recorded superseded under the target before its release, so its line names that target. A held volume is not
  released and leaves no line; a release that finds neither the image nor its committed entry commits none and leaves
  none. The record such a release retires is journaled by a line of its own, `release-imageless-build-volume`: its
  artifact is the sidecar record it deletes (which names the volume's id) and its image the missing path, so the journal
  says the record went because its image was absent, not because collection reclaimed it. Without this journal, an `rm`
  that releases the volume a land has just moved main onto leaves no trace of it.
- Each target keeps only its latest seed; a superseded seed is released when replaced (by a land's seal or a fork's seed
  refresh, committed to `build/.deleting` like every release, above), and a target's seed is released when the target
  retires. Detached seeds need no attach or label rename to be released. Main retires last, in `rm main --restore` (and
  `--restore --purge`): its unbinding collects once main's image is retired, so main's linked volume and seed are
  released with every volume an earlier collection left, and a volume that stays refuses the unbinding while the project
  is still bound (02_workspaces.md, "Checkout layouts"). The unbinding is the project's last act and its store tree goes
  with it, so before it plans what it deletes it moves each released image of its project, from `build/.deleting` and
  `sessions/.deleting`, into `<store>/.deleting/<fresh id>.dmg` and nudges the gateway for each one; it frees nothing
  itself, and a cut the gateway is making moves with its entry, which the cleanup holds open. Anything there that is not
  a released image stays, and the terminal plan then refuses, naming it. With no volume left, the unbinding deletes
  `build/` (then only empty directories and zero-length holds) and the project's `.build/<owner>/<repo>` mount tree.
- GC also enumerates sidecars whose images are already gone: every seed's, and every other record collection would
  release with its image: one recorded superseded, registered and interrupted, or naming a checkout that no longer links
  it. An image that does not exist cannot be waiting for anything, so deferring its record would defer it on every pass
  with nothing that ever ends the wait. Only absence authorizes this: an image that exists but whose size cannot be read
  is deferred with the error, never released on a guess. Under the ordinary exclusive release claim it rechecks image
  absence, the sidecar and live links, then deletes the orphan sidecar with zero image bytes reclaimed. An image
  appearing or a record/link changing defers it; a record a live checkout's link names and retained existing seed images
  are not relabeled or collected by this rule.
- A fork's volume and seed exist before its workspace does: `cowshed fork` registers and clones them into the staged
  checkout, which no other process can read, and publishes the workspace afterwards. Main's first volume likewise exists
  before main's first touch links it: `cowshed adopt` registers it to main by the adopt's intent and mints it beside
  main's image, and completes its intent only once the first touch linked and published it or the adoption released it.
  While the create, fork or adopt is past its mutation fence and unfinished in the lifecycle intent journal, collection
  in any process defers the published volumes recorded as that workspace's or as its seed, naming the workspace; what it
  has registered and not yet published, it holds by id meanwhile. Without this, an `rm` in one process deletes the
  volume and seed of a fork running in another, and the fork's mount refuses its link to a volume nobody owns.
  Collection asks that hold before it reads the image at all: a volume whose hold is taken is deferred as held without a
  stat, because its creator may sit in the first write with the image's vnode held, and a stat waits as long (measured
  2026-10-25: an `rm`'s open spent 37 s in one `lstat` of a concurrent fork's volume); the hold is a file of its own. A
  verb crosses its fence before it makes a volume, so collection lists the images first and then reads the journal and
  the workspaces, and decides only the images it listed: one made after the listing is left to a later pass, never
  decided from links read before it existed. An adoption killed between the mint and the link leaves main's registered
  sidecar, which no process holds any more, and the next open releases it by its id once the adoption's intent is
  recovered.
- Collection says why it skips: a job hold, an unfinished create/fork/adopt, an unreadable detached checkout or record,
  or a failed release names the volume and reason. Opportunistic collection counts routine detached/still-forming
  deferrals on one line. One volume's release failure does not hide later candidates. `--dry-run` changes nothing and
  reports the same ownership reasons.
- Inside a build volume, Nx's own cache eviction runs unchanged (age and size bounds, configured in `nx.json` as Nx
  documents). Its database and its cache directory are always the same pair, so its eviction never deletes what another
  database indexes. Gc never opens a volume's contents. Under mbx, mbx bounds each managed target and the store by its
  own collection (checkout gone, `target.max_age`, `target.max_size`, store budget; 03_caches.md, "mbx"). A detached
  shed whose mount path is absent may look gone to mbx, so its managed target can be collected while the shed exists;
  its next build restores from the store [INFERENCE: mbx's "checkout is gone" test is path existence; Open questions].
  The Nx plugin's Cargo retention and its post-run purge (`packages/nx-plugin/README.md`, "Cargo Workspace Layouts") do
  not run in a project with the mbx capability: mbx collects unused units and whole managed targets itself (unused units
  only where the volume records access times; Open questions, "`noatime`"), and a second collector over the same
  directory would only add work (measured: a post-run purge of 73 superseded generations outran a host gate's 10-minute
  bound). Without mbx, Cargo does not bound its target directory: it keeps every unit generation a later build
  superseded, and rustc keeps every split-debuginfo object set it writes (rust-lang/rust#161824), so a Rust project
  without mbx keeps the Nx plugin's Cargo retention, which reclaims superseded generations after each run that built;
  cowshed does not prune target directories.
- A workspace's build volume retires with the workspace (`cowshed rm`, which collects before it answers, an unfinished
  clone's removal included), unless main adopted it.

## Rules that keep it warm

Each rule states what is enforced and why. Each has an enforcing test or check; a rule without one is not done.

### Read-only cache-reuse advisor

`node packages/cowshed/bin/cache-advisor.cjs --workspace <checkout> --main <main-checkout> -- [bun] nx run-many <args>`
advises an operator before a gate; it never rebases, restores outputs, replaces or deletes an image. It accepts an
explicit `run-many` argv, including project selection, configuration and task overrides, not shell text or
package-script indirection. It runs in the workspace's ordinary activated toolchain environment. Its one JSON report is
stdout; Nx diagnostics and failures are stderr.

The standalone script ships beside the cowshed launcher. A new cowshed verb would add supervisor/controller/API
lifecycle plumbing to a read-only calculation. The standalone surface can run from a workspace's existing read grant; it
needs no new lifecycle operation or candidate write grant.

On macOS, the advisor starts one confined child with kernel denial of persistent writes except its private scratch
directory. The nonpersistent `/dev/null` device is explicitly writable because Git opens it O_RDWR. All process signals
and network access are denied. Graph plugins and runtime-input descendants inherit that denial. `cargo metadata` asks
its compiler for `-vV`; under mbx that query runs through mbx's wrapper, a read-only consultation of the toolchain, not
a build [INFERENCE: whether mbx's wrapper answers `-vV` without its scheduler socket is unverified; the advisor's test
must pin it]. A wrapper that refuses fails loudly, never unsets `RUSTC_WRAPPER` or silently probes a different
toolchain. Unsupported platforms refuse rather than run unconfined. Node 22.13 or newer supplies SQLite.

SQLite never opens a live candidate database, even read-only: a read-only WAL connection can still require SHM writes.
Instead each database and its present `-wal`/`-journal` files are copied once into scratch, without its `-shm`. The
entire set's device, inode, size, mtimeNs and ctimeNs are captured before any copy and checked after all copies;
appearance/disappearance is a change too. A changed file refuses the observation with `busy` and its path, without a
retry or live-SHM fallback. The candidate database list and resolved state links are checked for change too. Private
SQLite recovery and SHM bookkeeping then occur only in scratch; `query_only` and a read transaction cover the queries.
No `immutable` shortcut may omit committed WAL rows. The report includes copied bytes and snapshot/query seconds per
candidate; the typical approximately 1 MiB database copy is not free. A refused copy gives an explicit
`decision.basis: "space"`, `timeDecisionAvailable: false` and null work score, alongside the loud diagnostic. Candidates
are advisory observations, not a cross-volume atomic snapshot or a reservation against concurrent eviction.

The workspace's current graph and hashes are computed once; main's source tree is never hashed. Disposable Cargo
resolution and parsed-lockfile caches can seed scratch, but neither old native file hashes nor an old project graph can
replace the fresh tree walk. Cacheable non-continuous tasks retain their id, project, target, configuration and current
hash. Custom hashers and tasks that depend on dependency outputs are unresolved, with the reason, never keyed from stale
outputs. Noncacheable/continuous tasks are listed separately and never called cache hits.

Execution hooks never run. A workspace declaring pre/post-task hooks reports its cacheable tasks unresolved because the
omitted pre-hook may supply hashed environment inputs. Runtime inputs default to denied; their exact per-task commands
are still reported. An operator may opt into `--runtime=read-only`: runtime commands run under the same write/network
denial, but only through the Nx plugin's fail-closed `runtime-input.sh` wrapper (Nx otherwise hashes a failed command's
error text). A refused runtime/hash pass reports unresolved tasks and its diagnostic, never guessed keys. The report
distinguishes runtime commands admitted to the hash pass from tasks excluded before it.

Each current hash is looked up in both candidates' successful `cache_outputs` rows, with its cache directory present.
The report distinguishes `HIT_BOTH`, `HIT_OWN`, `HIT_MAIN`, `MISS_BOTH` and `UNRESOLVED`. A current key is
tree/output-dependent, not candidate-dependent: a historical cached identity is not proof of a current HIT. The
dependency-output rule in Nx's `hash-task.js` defers hashing until dependency outputs are current. Exact deferred HIT
assessment would require materializing the current dependency artifacts in scratch, a build-output operation rather than
a database lookup. The advisor deliberately does not perform that materialization or hash stale outputs.

Each candidate is assessed separately. A keyless stock-Nx task with no successful cached identity entry is a definite
MISS in that candidate, whatever its eventual current key is. A keyless task with historical successful entries is
UNKNOWN in that candidate, never an invented HIT. Missing identity metadata also prevents an absence proof.
`assessment.own` and `assessment.main` give each verdict, basis and reason; an UNKNOWN is priced as `ASSUMED_MISS` only
on that side. The task's key remains unresolved, with no invented hash. When both candidates are UNKNOWN, the explicit
label is "unpriced key, worst case: misses in both". Custom hashers are the narrow qualification: Nx accepts their
returned value directly, without requiring task identity in the key, so identity absence proves nothing and they stay
UNKNOWN unless an exact current key is available. The gate's `@smoothbricks/codebase:cargo-test-archive` and
`cowshed:declaration-model-test` use stock Nx hashing, so the identity-absence proof applies to both; their
dependency-output deferral does not make them custom hashers.

A task's weight is the arithmetic mean of successful uncached `task_history` execution seconds joined to `task_details`
by hash and grouped by task identity, not exact current hash. Cache replays and failed/stopped attempts are excluded. A
UNION-equivalent tuple of identity, hash, start and end de-duplicates histories inherited by cloning; sample count and
min/max accompany every mean.

For each candidate, rebuild work is the sum of each determined or conservatively assumed miss times that task's mean
duration. Counts do not decide: two short misses can cost less than one long miss. Report both totals and `own - main`,
in serial task-seconds, not future gate wall time. Each candidate's `MeasuredMissWorkSeconds` term contains determined
misses (a current-key lookup or an identity-absence proof); `AssumedMissWorkSeconds` contains its UNKNOWN weights.
`assumedDifferenceSeconds` reports the assumed portion of the difference. `keylessWorstCaseSeconds` separately gives the
common term for tasks UNKNOWN in both candidates, included in each total; `weightedKeylessTasks` counts those tasks.

Holding the historical mean weights fixed, the cache-uncertainty interval for the difference is
`[determinedDifference - mainUnknownWeight, determinedDifference + ownUnknownWeight]`. Report both directional swings
and their maximum as `totals.differenceRange`, not a falsely precise point verdict. Even a common worst-case term that
cancels in the point estimate can swing the difference in either direction: UNKNOWN does not mean the actual outcomes
must match. The interval covers weighted tasks only when duration coverage is incomplete; it does not bound execution
variance. An interval spanning zero is still a usable time comparison, not a reason to fall back to space.

Tasks without a duration remain explicitly unpriced, never silently free; `totals.complete` records whether every
selected task has a usable weight, including worst-case weights for keyless tasks. Coverage reports all cacheable
identities with a duration and the resolved subset with one. A time decision is available whenever the candidate
observations are usable, at least one candidate holds an Nx cache entry, and at least one selected task has a duration
sample. Partial duration coverage does not veto that decision: report the gaps alongside the usable score. Space is the
fallback for genuine incapacity (no usable Nx cache, no weightable selected task, or a refused candidate observation),
not merely an unresolved key. The advisor does not implement the space policy or adopt an image. Probe wall time and
graph/hash-pass wall time are reported separately.

- **Enforced by**: `packages/cowshed/scripts/cache-advisor.test.cjs`, using real stock Nx and a running fixture daemon,
  asymmetric cache entries, cloned histories and a deferred dependency-output task. Candidate state bytes and
  `task_details` counts stay unchanged, the daemon's pid and record survive, the mean and totals match known samples,
  and a runtime descendant cannot write the fixture source.

### Every checkout has exactly one Nx state

Host shells, sandboxed jobs, land checks and the daemon of one checkout use one `.nx/cache`, one task database and one
daemon (04_sandbox.md states the socket and daemon placement). There is never a second, sandbox-private Nx cache, not
even for a read-only job: build state is not source, so a read-only job keeps the source tree read-only but reads and
writes its checkout's build volume like any other job. Nx is configured to use its cache, never to avoid it: cowshed
does not unset Nx's variables to sidestep a problem. Clearing Nx's graph state (a shell entry that rebuilt a stale Nx
plugin) keeps the task database: it is the cache's only index, so the cache without it is unreachable and never evicted.

The read-only cache-reuse advisor is the sole exception: its disposable graph, file-map and native-hash bookkeeping
lives in a scratch directory outside both candidate images. It writes no task cache entry or task database, never calls
`getTaskDetails`, and passes no `taskDetails` to `hashTasks`. The checkout's own Nx state stays untouched. This is not a
second build cache: no task executes and scratch is removed after success or failure.

- **Why**: Nx's task database indexes exactly one cache directory, and setting any of `NX_WORKSPACE_DATA_DIRECTORY`,
  `NX_CACHE_DIRECTORY` or `NX_PROJECT_GRAPH_CACHE_DIRECTORY` to something else moves the database with it. A second
  cache means a build fills one while a check reads the other, and every clone inherits both half-warm. A per-job or
  per-workspace cache directory (`NX_CACHE_DIRECTORY` set to a private `.cowshed/cache/nx`, say) makes every workspace's
  cache private and cold, so every new workspace misses everything.
- **Enforced by**: a capability test asserting every job environment of an Nx project names the checkout's own
  `.nx/cache` and `.nx/workspace-data` and nothing else, and a check that a host shell's Nx, with no `NX_*` variable
  set, resolves its cache to `<checkout>/.nx/cache` and its task database to `<checkout>/.nx/workspace-data`: the pinned
  Nx 23.2.1 otherwise moves both to a per-user `~/.nx/<id>` unless `nx.json` names `cacheDirectory`. Smoo's workspace
  policy writes `"cacheDirectory": ".nx/cache"` into every managed `nx.json` (`smoo monorepo update`), and
  `smoo monorepo check` and `smoo monorepo validate` fail on an `nx.json` without it; and a shell-entry test in which a
  stale plugin rebuild removes Nx's daemon record and persisted project graph through the link and keeps every other
  entry, including a Cargo resolution a concurrent Nx process holds mid-flight, which then publishes.
- The capability publishes `NX_MAX_CACHE_SIZE` as the caller-overridable default defined in
  [03_caches.md §Wiring](./03_caches.md#wiring), independent of whichever filesystem a client happens to discover. The
  hosted CI action's separate bound is `20GB`.

### The Nx daemon stays on

For a normal run, cowshed never sets `NX_DAEMON=false`, and never withholds the daemon to work around state problems.
The read-only cache-reuse advisor bypasses the daemon and computes in-process, without stopping, restarting or
signalling the checkout's existing daemon. A fresh walk of the rebased source tree must not use a watcher's lagging file
map, and the probe's bookkeeping must not reach either candidate image.

- **Why**: the daemon is how Nx keeps the project graph and file hashes warm between commands; disabling it recomputes
  them on every run.
- **Enforced by**: the same capability test; a lint over cowshed and managed shell files refuses the string.
- The daemon's record lives in `.nx/workspace-data/d`, the directory that also holds the task database (pinned Nx
  23.2.1, `daemon/tmp-dir.js`), so the record would travel with a build volume. Fork and land delete it from the volume
  (above); a daemon is per checkout. A land then gives the adopted volume the target's own record of a live host daemon
  (Land 6.3), so the swap does not end it: the daemon exits as soon as it finds no record that names it, and a client
  writing to it then fails with `write EPIPE`. A sandboxed daemon's grant names the previous volume alone, so its record
  is left behind; it exits and its keeper starts the next one on the adopted volume.
- The keeper runs the checkout's installed Nx foreground server as one admitted background job. Its original job
  birth/group is the only ledger ownership proof; a daemon record never enrolls a new process. That maintenance job is
  excluded from user-activity and idle checks, while quiescence and retirement stop and wait for it through the ordinary
  job lifecycle.
- An idle shed's daemon is stopped, never disabled. After the configured idle interval (five minutes by default) without
  developer activity, its keeper stops its daemon and starts none until the shed is in use again (04_sandbox.md), and
  the next Nx command starts one. A named session's opening is one use; its idle handle is not a continuing hold on Nx.
  Real job admission and conclusion restart the interval; probes and maintenance do not. What the rule protects is the
  warm state of a shed in use; a daemon nothing uses is a resident process, its plugin workers and a file watcher
  recomputing the project graph for every change in a tree nobody builds.
- **A daemon answers from the files as they are.** A daemon whose file watcher stops delivering is blind to every later
  change and would hash tasks from the files it last saw: a shed's daemon received no event for 33 minutes (its FSEvents
  stream stopped with no error; Nx 23.2.1 drops FSEvents' rescan flags) and called a changed input's target "[existing
  outputs match the cache, left as is]". The carried Nx patch watches the checkout with one stream, and on macOS an nx
  command's first request answered from file state writes a probe the daemon must see come back through that stream; the
  stream delivers in order, so every write made before the command reached the daemon. A probe lost for
  `DAEMON_VALIDITY_SAFETY_INTERVAL_MS` tells the client "watcher stream stopped", which continues without a daemon, and
  stops the daemon; the keeper (or the next host client) starts one with a fresh stream. A checkout with no directory
  that takes a probe the watcher reports fails closed the same way. A write another process makes while a command runs
  is as visible as Nx makes it. Linux inotify has no daemon between the kernel and the watcher that can stop serving it,
  so there the daemon only drains its watcher; its overflow (Rescan) path stays unproven. (`patches/README.md`, "A
  stopped watcher stream is never served from".)
  - **Enforced by**: `packages/nx-plugin/src/nx-daemon-watch-stream-host.test.ts` (`nx-plugin:test-host`).
- **Accepted property**: an Nx client sends its whole environment to the daemon in every message
  (`daemon/client/client.js` `getDaemonEnv`), and the daemon applies it to its own `process.env`
  (`daemon/server/handle-client-env.js`). In a workspace whose daemon runs inside the sandbox, a host client's
  environment therefore reaches code the workspace controls. This is accepted: no checkout holds real secrets in its
  environment, the ones present are temporary, and host-side Nx in an agent's workspace runs with a pure environment.
  The acceptance ends where a checkout holds real secrets in its environment.
- **Runtime inputs read only the checkout.** The same adoption runs a host client's runtime hash inputs inside the
  sandbox with the host's `HOME` and without the job's git isolation. An input that reads the operator's home (git's
  global configuration, for one) then fails there, and Nx 23.2.1 would hash the failure's text as the key. 04_sandbox.md
  states the contract that keeps one tree's key identical from every client: inputs are hermetic, and every input runs
  through `@smoothbricks/nx-plugin`'s `runtime-input.sh`, which makes a failed input an Nx error.
  - **Enforced by**: `packages/nx-plugin/src/runtime-input.test.ts`, which plants a failing input through real Nx, with
    and without the daemon, and refuses a runtime input in this repository that bypasses the wrapper.
    - **Runtime inputs run only declared programs.** Nx hashes on a developer's Mac, on a NixOS runner and in the
      sandbox; only devenv's Nix profile and the checkout's own programs are the same in all three, with `/bin/sh` and
      `/usr/bin/env`, which NixOS keeps. A host-shipped program such as macOS's `shasum` (Perl, not coreutils) hashes on
      a Mac and exits 127 on NixOS, which Nx 23.2.1 reports only as `invalid utf-8 sequence of 1 bytes from index 0`.
      `smoo github-ci` prints the reports runtime-input.sh retained after a failed Nx run.
  - **Enforced by**: `packages/nx-plugin/src/runtime-input.test.ts`, which judges this repository's declared runtime
    inputs on the PATH `tooling/direnv/repo-path` declares (no `/usr/bin` or `/bin`) and refuses a program found outside
    `/nix`, the checkout and the home, one nothing provides, and one only a workspace package's build output provides
    through `node_modules/.bin` (`hostPrograms`, `@smoothbricks/nx-plugin/runtime-input-policy`), and reads a planted
    failure's report back after real Nx refuses it.

### One Cargo environment on every path

Every Cargo invocation of a checkout (host shell, sandboxed job, Nx task, land check, a long-running dev process) sees
the same unit-identity environment, and in a project with the mbx capability the same mbx route. 11_shell.md, "Canonical
mbx environment", owns the exact mbx variables every path carries. This rule owns what must hold around them.
Concretely:

- **Cargo runs through mbx, unwrapped by anything else.** mbx defers to a preset `RUSTC_WRAPPER`, warns and caches
  nothing, and treats a preset `RUSTC_WORKSPACE_WRAPPER` other than Clippy's `clippy-driver` the same way for workspace
  crates (mbx FAQ, "Can I use mbx together with sccache"). In an mbx project the canonical job environment therefore
  carries neither. A wrapper a devenv activation sets is a typed finding that refuses job admission, naming the variable
  and its source, with the remedy to remove it or disable the mbx capability; a caller's value never reaches an owned
  job. A tracked `build.target-dir` or `build.build-dir`, or a `CARGO_TARGET_DIR` in the canonical environment, is
  refused the same way, because mbx leaves such a target unmanaged (15_capabilities.md, "mbx detection").
- **Incremental compilation is mbx's.** mbx sets `CARGO_INCREMENTAL=0` and gives a workspace crate private incremental
  state on its first source edit, seeded from the most recently used checkout's state for the same crate and flags on
  rustc 1.98 or later (mbx docs, "Incremental"). A caller's `CARGO_INCREMENTAL=0` already wins and changes nothing; no
  path sets `CARGO_INCREMENTAL=1` or `MBX_INCREMENTAL`, which would hand workspace crates to Cargo's checkout-specific
  incremental state, outside the shared store.
- **No `CI` in a dev environment.** Cargo turns incremental compilation off for workspace crates whenever `CI` is set,
  which changes their `-C metadata` and writes a second set of units into the same target directory: a target built
  without `CI` is not wholly `Fresh` under `CI=true`. Agent harnesses export `CI=true` on every command. The workspace
  environment cowshed provides unsets `CI` unless the runner identifies itself as real CI (`GITHUB_ACTIONS`, or the
  runner's own marker), and a repository's dev shell does the same.
- **The toolchain's compiler variables.** cc-rs's compiler variables are the dev shell's: the ones its `env` declares
  and no others. They are `CC_<target>`, `CXX_<target>`, `AR_<target>` and `RANLIB_<target>`, the `HOST_` and `TARGET_`
  forms of each, and `CFLAGS`, `CXXFLAGS`, `ARFLAGS` and `RANLIBFLAGS` in each of those forms and generic; the generic
  tool names are the stdenv's on Linux and Xcode's on Darwin (`apple-developer.sh` drops them). cc-rs declares
  `rerun-if-env-changed` on each name it looks a compiler up through, so a value a caller's environment carries in (an
  agent started in another checkout's devenv shell exports Apple's clang as `CC_aarch64_apple_darwin`) reruns every
  build script that looked it up, and rebuilds every unit that depends on those packages, against a seed the land built
  without it. The dev shell's prologue drops each inherited one it does not declare and announces the drop
  (`tooling/direnv/compiler-variables.sh`), so a host shell, `direnv exec`, a sandboxed job's shell and the land's pure
  shell agree. A host-gate job runs its argv in exactly its caller's environment, so its Cargo is the checkout's only
  through that shell: its caller is the shell, or its argv enters it, as the land's `direnv exec <checkout>` does.
  - **Enforced by**: `packages/cli/src/monorepo/compiler-variables.test.ts`; and the land. Before gating,
    `tooling/land-host.sh` compares the shed's dev-profile build-script records with what Cargo sees in the gate's
    shell, the shell main's seed is built in, and names each variable they disagree on, its recorded and current values
    and the packages whose scripts read it (`@smoothbricks/nx-plugin/cargo-build-script-env`, which a real Cargo build
    holds to Cargo's own rerun decision). It is said, never a reason to stop: drift costs the gate a rebuild, not
    correctness.
- **No absolute checkout paths in compiler flags.** `--remap-path-prefix=<absolute checkout>=…` puts the checkout path
  into `RUSTFLAGS`, which is part of every unit's identity, so every checkout builds its own units. Use
  `-Zremap-cwd-prefix` (relative), or no remap.
- **No checkout-resolved linker paths.** `target.<triple>.linker` and `host.linker` are a bare PATH program or an
  immutable store path, never a checkout-relative path: Cargo hashes the resolved linker into every unit. On macOS the
  repository's store-installed linker wrapper uses exported `SDKROOT` without discovery and executes `/usr/bin/clang`
  with `-fuse-ld=/usr/bin/ld`, selecting Apple's linker independently of PATH; only an absent SDK invokes `xcrun`.
  **Under mbx the wrapper links but is never cached.** mbx identifies a `-C linker` only when its file name is `clang`
  or `clang++`, and any other bypasses the link cache while the link still runs (mbx limits, "Linker overrides and link
  arguments"); it models `-C link-arg=-fuse-ld=<absolute path>` and keys that linker's path and version. The form mbx
  caches is `linker = "/usr/bin/clang"` for the macOS target in `.cargo/config.toml`, `MBX_LINKER=path:/usr/bin/ld`
  exported by the dev shell, and the dev shell's `SDKROOT` selecting the SDK. Measured 2026-10-27: the wrapper bypasses
  as `unportable-native-link`, and in the cached form a native test link restores from the store with no compiler run.
  An mbx project switches to the cached form only on the link measurement in Migration. The Darwin dev shell disables
  devenv's C tools, uses a compiler-free stdenv, and removes only the Rust toolchain's propagated Nix compiler while
  retaining the pinned components and their runtime dependencies. Linux retains its existing C tools, stdenv and
  toolchain propagation. Its Rust linker, devenv's clang driver, links with a patched LLD 21 that sizes ELF relocation
  shards from the actual LLVM executor, whose worker indices address them. The driver's earlier CPU-affinity snapshot
  cannot size those shards: an Incus cpuset can grow before the pool starts. The serial path keeps one shard without
  creating a pool. Only that linker is patched, as local derivations in `tooling/direnv/devenv.nix` rather than an
  overlay on `llvmPackages`, which would rebuild every package that links with the LLVM stdenv (Chromium, which no
  binary cache holds): `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER` names devenv's driver over the patched wrapper.
  LLVM 23 removes implicit thread-index shards in
  [905f654bb721](https://github.com/llvm/llvm-project/commit/905f654bb721f1df119bd2cfaa23736e30838e90); the local patch
  retires when the pinned linker contains that change. The Linux regression (`tooling/direnv/lld-affinity-test.py`)
  grows affinity from eight CPUs to twelve at the first parallel task, requires a successful link, checks every relative
  relocation remains in the output, and fails when the first parallel task never runs.
- **Two profiles.** `dev`, which includes unit tests, and `release`, for a released binary. `test` inherits `dev`. The
  owner's ruling, 2026-09-30: "BUILD EVERYTHING ONCE. CARGO ALREADY TRACKS CHANGES AND REBUILDS WHAT HAS CHANGED"; "You
  have realistically 2 profiles: dev (which includes unit testing) and release". 03_caches.md states the profile rule.
- **One compiler cache.** mbx is the only compiler cache in a project with the mbx capability: registry crates and
  workspace crates alike are restored from the mbx store by content. A project without mbx compiles every crate in its
  own target directory.
- **Every use of a unit is recorded.** `CARGO_UNSTABLE_MTIME_ON_USE=true` (`-Zmtime-on-use`) makes a build stamp the
  fingerprint hash file of every unit it uses, `Fresh` ones included. Cargo output retention
  (`packages/nx-plugin/README.md`, "Cargo Workspace Layouts") reads that stamp as the unit's last use; without it a
  unit's last use is its last compile, and a unit that a branch switch keeps reusing looks as old as its build. The
  Cargo capability owns it for every job of a Cargo project, so a caller's value never turns it off, and the workspace
  environment cowshed provides exports it for host shells. Measured on a two-crate workspace: the unit directory names
  and every fingerprint byte are those of a build without it, a build with it and one without are each `Fresh` over the
  other's units, and a no-op `cargo build -p a` stamps `a`'s hash file and not an unrelated unit's. Stable Cargo (1.95.0
  and 1.97.0) ignores the variable without a warning, so it is exported unconditionally.
- **Enforced by**: a test that builds a fixture workspace in one checkout and the same tree in a second checkout at
  another path, from a host shell with `CI=true` exported and from a sandboxed job, and requires every compilation of
  the second build to be an mbx store hit, and without mbx, requires every unit `Fresh` in a cloned build volume. The
  repository's own Cargo configuration also receives a real-repository two-checkout build requiring the same, not just a
  fixture. A detection test refuses an activation-set `RUSTC_WRAPPER`, a non-Clippy `RUSTC_WORKSPACE_WRAPPER` and a
  tracked `build.target-dir` in an mbx project by name, and a link test requires mbx to report host links of the
  repository's test binaries as cached, not bypassed.
- **Enforced by**: `directory_a_noop_cargo_build_stamps_the_units_it_uses_and_no_others_substrate_lifecycle`
  (`crates/cowshed-cli/tests/command_dispatch/macos.rs`): a no-op `cargo build -p` from a sandboxed job and from a host
  shell that sources `.cowshed/env` stamps the used unit's hash file and no other's, and the same host build without the
  file stamps nothing. `a_detected_project_gets_its_tools_wiring_and_a_trust_bundle` and
  `a_plain_repository_gets_port_loaders_but_no_tool_wiring` hold the job's variable to Cargo projects, and
  `a_cargo_projects_published_environment_records_each_unit_use` holds a served file to the same split.

### Clones preserve mtimes; directories are never copied

Cargo's freshness is mtime-based: a unit is stale when any dependency output is newer than its own. Image clones keep
every mtime. A file-by-file copy that restamps files in walk order (`cp -c -R`) makes units rebuild, and even
`cp -c -p -R` takes minutes. Build volumes are only ever image clones or dataset clones.

- **Enforced by**: the fork test above runs on a cloned build volume and requires every unit `Fresh`.
- Cargo keeps its default mtime freshness in host shells, jobs and every checkout. That is sound because every producer
  of a Cargo-read input gives changed bytes a fresh mtime: an Nx cache restore stamps what it writes
  (`packages/nx-plugin/README.md`, "A restored output is newer than the output it replaces"); a fork clones its sources
  and build state from one tree ("Source and build state arrive together"); and a land harmonizes the target's source
  mtimes with the landing checkout's under the target's image lock, before the landing volume is frozen as its seed or
  adopted as its build volume (Land step 5.3). A Cargo run whose directory has none of the checkout's
  `.cargo/config.toml` above it reads them through `--config`, shallowest first: the cowshed-core capability probe
  builds its workspace under `CARGO_TARGET_TMPDIR`, which resolves into the build volume, and without them it built
  cowshed-core's tree without the workspace's linker or `[env]`.
- **Enforced by**: `source_mtimes_make_cargo_rebuild_a_unit_the_fast_forward_left_stale`
  (`crates/cowshed-core/src/runtime/build_volumes/source_mtimes.rs`), in which real Cargo in its default mtime freshness
  reuses a unit built from pre-rebase bytes beside the target's older landed source and, after harmonization, rebuilds
  it from the landed bytes; and that module's unit tests —
  `source_mtimes_take_only_a_newer_landing_mtime_for_tracked_files`,
  `source_mtimes_republish_a_differing_output_and_advance_an_equal_one`,
  `source_mtimes_skip_absent_files_and_never_follow_a_link` and
  `source_mtimes_refuse_a_path_that_is_not_plainly_relative`.

### Nx outputs never live in the build volume

No Nx target declares an output that can cover a contributed build-volume path: at it, under it, or an ancestor of it.
Globs whose static prefix overlaps one of those paths are also refused; absolute outputs are resolved against the
workspace root.

- **Why**: on stock Nx, restoring a cached output beneath a symlinked directory replaces the symlink with a real
  directory, which silently detaches the checkout from its build volume; declaring the link itself as an output caches
  the symlink, not the bytes, so a hit restores nothing. Task outputs belong on the source volume (`dist/`,
  `.cache/<tool>/`), where the source image carries them to forks.
- The API generator is published directly by Cargo's artifact directory into `packages/cowshed/.cache/api-generator`. Nx
  caches that source-owned directory; generation, drift checks and the executed DTO/validator regression invoke that
  published binary, never a borrowed executable under `target/`.
- **Enforced by**: the consuming repository's Nx lint calls the plugin's `refuseOutputsUnderBuildState` validator with
  its already resolved graph (including project overrides), `cowshed build-state --json` path records, and its workspace
  root. A refusal names the target, output, and build-state path. The plugin's final-graph `createMetadata` hook calls
  `refuseOverlappingOutputs` for every Nx command: an output at, under or above another producing task's output is
  refused, including two configurations of one target, because a restore of either replaces the other's tree. A
  declaration states its output's kind: one ending in `/` names a directory, any other a file. Below a pattern's static
  prefix, a pattern overlaps a file output only by matching it; a file at or above the prefix collides by kind, since
  the pattern writes beneath it. Identical patterns are one output claim and conflict; different patterns are not judged
  without the tree. The verdict is a function of the declarations alone. Where outputs exist after a build, the lint
  also calls `refuseMisdeclaredOutputKinds`, which refuses a literal output whose on-disk kind contradicts its
  declaration. Job admission and `cowshed doctor` never construct an Nx graph or execute repository plugins for this
  rule. Sandboxed lint resolves the installed `cowshed` through the job's private `tools/bin` (15_capabilities.md,
  "Bootstrap executables"); its executable-only grant admits no controller data or socket.
- `cowshed build-state --json` reads the selected checkout's volume-state record and prints
  `{paths:[{checkout,volume}]}` without opening the controller or store. It never rediscovers tools. A missing link or
  state record refuses with `environment-missing` (exit 5): "this checkout has no build volume yet; run `cowshed setup`
  (host) or any `cowshed exec` in it to migrate". It selects the nearest `.git` marker from the canonicalized cwd or
  `--project` path using the existing filesystem-only root walk, stopping at a filesystem boundary. It executes no Git
  discovery subprocess: a controller's nested Git profile is neither required nor authority for a checkout-local state
  read.

### Hash inputs are the same in every checkout

An Nx task hash must not change between checkouts of the same tree.

- No absolute paths, workspace names, user names, mount points or per-checkout values in any hashed input.
- A runtime input's command text is part of the owning target configuration: spell installed files through
  workspace-relative `node_modules` paths, never the plugin's physical checkout.
- Commit-keyed inputs are allowed only for outputs that deliberately embed commit provenance. Rust LMAO provenance uses
  the explicit `LMAO_GIT_REVISION` environment input
  ([01f](../lmao/01f_arrow_table_structure.md#smoo/lmao!n/arrow-table-module-id.not-git)); default dev and test builds
  leave it unset, while publishing jobs supply the revision.
- Workflow-only edits are not default task inputs. A host browser location hashes a fixed configured/managed sentinel,
  not the executable's absolute path. `default` holds no CI-only `sharedGlobals` reference; the managed workspace policy
  must not re-inject a CI global. Custom non-CI globals and named inputs used explicitly by workflow targets are
  preserved.
- A runtime input (a command whose output Nx hashes) prints only its value or a fixed sentinel. Nx hashes the command's
  stdout and stderr and ignores its exit code (pinned Nx 23.2.1, `hash_runtime`), so an error message that names an
  absolute path becomes a checkout-specific key that silently splits the cache. The input's consumer fails instead.
- Nx hashes the whole target configuration of the owning project into every task of that project (`hash_planner.rs`
  `gather_self_inputs` adds the project configuration; `hash_project_config.rs` hashes every target's executor, options,
  outputs and configurations). Input declarations cannot remove it; a project whose target definitions change often
  re-keys all of its tasks.
- A file list that inference writes into a target (a command, its metadata or its inputs) names only files in Nx's
  workspace file index: .gitignore and .nxignore applied. A walk of the tree also finds ignored generated modules that a
  build left on disk and that a fresh checkout lacks. Through the project configuration, such a list re-keys every task
  that reaches the project, depending on whether a build ran in that checkout.
- A target that runs no package from `node_modules` declares `externalDependencies: []`; otherwise a lockfile edit
  re-keys it.
- **Enforced by**: step 2b of every land (100% hits on main) and the fork test (100% hits in a fresh fork).

A project-contained developer-link declaration selects its directory in the current checkout and refuses physical
symlink escapes, including an escaping existing ancestor of a not-yet-existing declared directory. Sibling declarations
resolve beside the current checkout; inherited absolute sibling bindings retain their existing targets. Neither the
containment nor the sibling rule grants the main checkout or an ancestor dependency store to a cowshed sandbox.

A project-contained developer alias is a relative link within its checkout. A source-image fork therefore selects its
own copy without borrowing its donor's tree. Genuine sibling links retain their distinct host-local authority. The
checkout's own build volume is within the checkout: an alias that reaches build state through the build link, such as a
linked-package snapshot in declared `.cache/linked-snapshots`, is owned, and a fork's volume clone carries it. Only a
contained alias whose physical target lies outside both the checkout and the volume its build link names escapes: mint
and job admission refuse it.

### The adoption needs the target's Nx database closed

A process that opened the target's task database before a swap keeps that file open but resolves cache paths through the
links afterwards, so it reads rows from the old volume and files from the new one. For a hash the old database lists and
the new cache lacks, stock Nx reports a hit and restores nothing: the task "succeeds" with its outputs missing. A run
that started on the old tree and stays entirely on the old volume is correct; only the mix is wrong. The land therefore
swaps only when no process holds the target's current task database open, and otherwise skips the swap (Land steps 5 and
6). The daemon is stopped before the swap because it also keeps the database open (it records task history). The landing
workspace is quiesced first (Land step 4) for the same reason on its side.

A run that started on the old tree and stays wholly on the old volume is correct even when it hits; the hazard is only
the mix.

The look is not enough by itself. It lists processes and then their open files, which on a loaded host can take seconds.
An Nx client that opens the database after the list was taken and before the rename keeps the old volume's database,
while the daemon it restarts after the swap opens the adopted one. The client then records a task's details in one
database, the daemon records the task's history in the other, and the daemon's write fails with
`FOREIGN KEY constraint failed`. So the look and the rename both happen under Nx's own open locks (Land 6.1): every Nx
process opens the database only while it holds the database's lock, so one that began opening before the locks were
taken holds the database at the look, and one that begins later waits and opens the adopted database.

- **Enforced by**: a test that holds main's database open in a running Nx client, lands, and requires the swap to be
  skipped and reported; one with the database closed that requires the swap; the same land test holding main's open
  lock, which requires the skip to name the process opening; a test in which a stock Nx process starts opening while the
  locks are held and the link then moves, and must open the database the moved link names; and one in which a stock Nx
  daemon whose record was handed over still serves a client after the move, and exits without the record.

### Content-addressed tool caches are shared and writable

Read-at-build and link-target caches (03_caches.md layer 3: Cargo registry and git, Go module and build caches, zig,
bun, uv) are one shared copy per host, written by whichever checkout fetches or compiles first. The mbx store is shared
and writable the same way (03_caches.md, "mbx"). Every sandbox that uses the tool must be able to write them; a cache a
sandbox can only read fails the tool (Go's build cache, unwritable from the sandbox, fails every sandboxed Go build with
`operation not permitted`).

- **Enforced by**: a sandboxed `go build`, `cargo fetch` and `bun install` in the capability test suite.

## Process lifetime across a swap

Nothing pins a path. A process resolves paths through the links when it opens them, inside the sandbox grant of the job
it belongs to. Each job's sandbox grants exactly the build volume the controller resolved when the job was admitted,
and, in a project with the mbx capability, `<build-cache>/mbx` (04_sandbox.md); the supervisor is never relaunched for a
swap:

- A job admitted after a swap, and the Nx daemon the supervisor restarts after it, use the new build volume entirely.
  The keeper resolves the current pointer through the same controller-owned layout as admission on every restart, even
  when no user job has been admitted since the swap. Repository links alone never authorize a mount.
- A job admitted before the swap keeps its grant and a shared build-volume hold, so the target's previous volume stays
  attached, recorded as the target's (superseded, at its incarnation), until the last such job exits. Its hold prevents
  even a forced collection; each such job's end releases the volume once no hold is left. A host process with no job
  hold does not own the superseded volume (Garbage collection).
- A long-running job that starts builds (a development server, a file watcher) keeps the grant it was admitted with:
  builds it starts after a swap resolve the links into the new volume, which that grant does not name, and the sandbox
  denies them. Restarting the job admits it on the adopted volume. Its Nx client re-subscribes to file events when the
  daemon it watched is stopped at a swap (Nx reports `reconnecting`); main's host daemon is not stopped by the swap
  itself (rule "The Nx daemon stays on").
- A coordinator that reads a workspace's files itself, outside any sandbox, follows the build link only into the volume
  the controller answers for it (`WorkspaceRef::build_volume`, `workspace.buildVolume`): the grant a job admitted now
  would get, fenced on the incarnation the coordinator holds. It compares what the link names against that answer, so a
  link a job repointed never widens what the coordinator reads.

- **Enforced by**: a paused-clock supervisor test pivots the build link while its first keeper job runs, admits no
  intervening user job, and requires the restarted daemon's grant to name only the adopted volume while the old job's
  profile still names only the previous volume; a router test answers `workspace.buildVolume` with the host's grant only
  for an attached workspace at the incarnation the coordinator holds.

## Stacks and fragmentation

Stacks also limit fragmentation (02_workspaces.md "Lanes"): units clone the stack's head, not main, and only the stack's
close-out writes main. With build volumes, each level adopts its units' build volumes and reseeds ("Stacks and merge
queues" above), and main's source image is written once per stack close-out. Stacks remain a coordinator choice; build
volumes do not require them.

## Migration

A host moves to this model in one release, in this order:

1. **The release installs mbx first.** The cowshed release that carries this model installs its pinned mbx beside the
   cowshed binary, and `cowshed setup` provisions the build-cache volume (01a_apfs.md, "Dedicated build-cache volume"),
   adopting an existing volume at its mountpoint by UUID. Only then does a project's devenv pin the same mbx. A project
   that pins mbx on a host without it is refused admission as `mbx-host-missing` (05_gateway.md, "Pool selection and
   revision").
2. **Each checkout moves at its first refresh.** For a Cargo project that gains the mbx capability, the checkout's first
   refresh removes cowshed's fixed `target` link and drops the old target directory from the build image (Refresh), so
   mbx's first build places its managed target. Build images, seeds and adoption are otherwise unchanged, and nothing is
   copied out of the old target directory: build state is rebuild-only.
3. **The store fills on first builds.** The mbx store starts empty and fills with the first build of each crate, after
   which every checkout restores it. A checkout's first Cargo build under mbx is cold, once.
4. **What is deleted with it.** sccache, with its four patches, its daemon and its wiring, and the smoo sccache-patches
   check. For projects with the mbx capability, the Nx plugin's Cargo retention, its purge, and the `postTasksExecution`
   hook step that runs it after each Nx run (Garbage collection; `packages/nx-plugin/README.md`, "Cargo Workspace
   Layouts"); Rust projects without mbx keep all three.
5. **The linker wrapper goes only if linking through mbx is faster.** After a project's mbx cutover, on one host, it
   links the same fixtures (two test binaries of the project's largest dependency graphs) both ways: through the
   repository's wrapper, and through `/usr/bin/clang` with `MBX_LINKER` ("One Cargo environment on every path"). Each
   way is timed on a link-cache miss and on a hit, the hit being an edit of one leaf crate and the relink. Both sides
   are measured in the same session, never against a recorded baseline, because the expected gain is under tenfold. The
   wrapper is removed only when the mbx form is faster on the miss, or faster on the hit at no greater cost on the miss.
   The measured link times are recorded here, with the decision they gave.

**Overlap with the cowshed-core crate split.** The split moves the code this model changes (00_overview.md, "Crate
map"). The model's code lands in the existing module at each item's planned crate, so the split later moves it
unchanged: Cargo/mbx detection in `capabilities/` (cowshed-environment), the build-volume record, layout and link model
in `build_volume/` (cowshed-storage), fork, land, reseed and clear coordination in `runtime/build_volumes`
(cowshed-workspaces), mbx reservation in `runtime/job_cpu.rs` (cowshed-runtime) and build-cache provisioning in
`storage/bootstrap/native` (cowshed-host).

## Open questions

- **rust-analyzer and editors with `target` as a symlink.** Not yet verified for mbx's managed-target link.
- **Daemon keeper probing.** 04_sandbox.md's workspace supervisor probes the sandboxed daemon every 5 s; an event-driven
  alternative (the daemon's exit) is preferred if Nx exposes one.
- **`noatime` on the build-cache volume.** mbx reads access times to collect unused build units inside a managed target
  and to seed a new checkout with only the units the other checkout last read; under `noatime` it skips the first and
  copies every unit for the second (mbx docs, "Managed target directories"). Whether the volume keeps `noatime` is the
  owner's decision, informed by the A/B's `mbx doctor` reading.
- **A detached shed's managed target.** Whether mbx's "checkout is gone" test treats an unmounted shed's path as gone,
  so its target is collected while the shed exists. Its next build then restores from the store.
- **Seed growth without Cargo churn.** With Cargo's `target/` out of the build image, whether a seed's extents still
  grow to the compaction trigger, and how fast: the A/B reports a day's extents (Why two volumes, item 1).
