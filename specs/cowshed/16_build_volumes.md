# Build Volumes: Millisecond Forks and Lands

A workspace is two volumes: the **source volume** (the workspace image of 01a_apfs.md: the git tree, installed
dependencies, every build tool's declared outputs) and a **build volume** (the build tools' own incremental state: Cargo
target directories and Nx's task cache with its database). A fork clones both and is warm for every task main built. A
land moves main to the build volume the landing workspace already built, by renaming one link: main builds nothing that
the workspace built, and nothing is copied. This spec states that model, every rule that keeps it warm, why each rule
exists, and how each is enforced. It is the one place these rules live; other specs cite it.

## Goal

- **Fork in milliseconds, warm.** `cowshed fork` clones images (milliseconds; 01a_apfs.md "Images") and attaches them (a
  fraction of a second each, independent of how many images the host has attached). Every Nx task main ran is a cache
  hit in the fork and every Cargo unit is `Fresh`: the fork reseeds main first when main's volume holds work its seed
  does not (Targets and seeds). Only the fork's own edits invalidate anything, and exactly what the same edit would
  invalidate on main.
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

Carrying source, dependencies and build state in one image per workspace has three problems, and a second volume removes
each of them.

1. **Main fragments, and every fork pays for it.** Fragmentation comes from writing an image while a clone shares its
   blocks (01a_apfs.md "Images", its clone-cost bullet): extents multiply under churn while clones are held, and the
   first write to a clone costs time per extent. Build state is where nearly all of main's churn comes from: every build
   of main rewrites gigabytes under `target/` and `.nx/cache` while sessions hold clones of main's image. With build
   state on its own volume, main's source image changes only when its tree changes (a land's fast-forward, an edit), and
   build churn lands in build volumes that are replaced at every land instead of accumulating extents. Main's source
   image therefore fragments no faster than an image that has no clone, and `cowshed defrag main` is a repair for
   imports, not a routine operation.
2. **Without build volumes, a land rebuilds main from scratch, or main is never warm.** A workspace builds and checks
   its work, lands, and then main has to build the same tree again before the next fork can start warm. A warm step for
   main would be a background job that can fail, queue, or be skipped; while it lags, every fork starts cold, and
   concurrent workspace checks that each re-run nearly everything load the host. The build volume removes the rebuild:
   main adopts what the workspace built.
3. **The inode problem stays solved.** Build state is the largest inode count a checkout has (a Rust `target/` holds
   hundreds of thousands of files). It lives inside a volume, never on the Data volume or the store volume's directory
   tree, so it stays out of the host's inode namespace (00_overview.md "Problem").

## Model

### What lives where

| State                                                                                                                                                       | Volume                            | Shared how                                                |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- | --------------------------------------------------------- |
| Git tree and `.git`, installed dependencies (`node_modules`), every Nx task's declared outputs                                                              | Source volume                     | Cloned with the workspace image                           |
| Cargo target directories; Nx `.nx/cache` and `.nx/workspace-data` (task database, project graph); each installed JavaScript package's `node_modules/.cache` | Build volume                      | Cloned at fork; adopted by main at land                   |
| Codegraph's per-tree `.codegraph/` index                                                                                                                    | Build volume                      | Cloned at fork; adopted by main at land                   |
| Declared build state (`.cowshed.toml` `[build] state`), under the volume's `declared/`                                                                      | Build volume                      | Cloned at fork; adopted by main at land                   |
| Nx daemon record and sockets (`.nx/workspace-data/d`, the socket directory)                                                                                 | Per checkout, never travels       | Deleted from a build volume before main adopts it (below) |
| Content-addressed tool caches (Cargo registry/git, Go `GOMODCACHE`/`GOCACHE`, zig, bun, uv, sccache)                                                        | Shared cache paths (03_caches.md) | Written once, read by every checkout; never in any image  |

The build volume holds exactly the state a build tool keeps **for one tree and keys by its own fingerprints**: state
that is correct to reuse for any tree the tool checks against, but costly to rebuild and useless to share between two
writers at once. Content-addressed caches, whose entries are immutable and valid for every checkout, stay shared
(03_caches.md). Declared task outputs stay on the source volume (rule "Nx outputs never live in the build volume").

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
- **ZFS**: a build volume is a dataset, `<pool>/cowshed/<owner>/<repo>/build/<id>`; a fork is `snapshot` + `clone`, and
  a seed is a snapshot (09_substrates.md).
- A directory is never a build volume. Copying a Rust `target/` tree file by file, even with `clonefile`-backed
  `cp -c -p -R`, takes minutes for a large workspace; cloning the image that holds it takes milliseconds. Minutes per
  land is unacceptable.

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
<checkout>/.codegraph        -> .cowshed/build/codegraph
```

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

Mounting a checkout mounts the volume its link names only when that volume's sidecar records the checkout as its linker,
or the volume has no sidecar yet (a creation in progress, whose sidecar is written last). Any other link is stale and is
re-pointed at the one volume recorded as the checkout's. When the checkout owns none and the link names another
checkout's live volume, a target adopted it from this checkout and a `land --no-retire` stopped before reforking it: the
checkout takes a fresh clone of that target's latest seed, exactly as the refork would have. When the checkout owns none
and the link names a volume whose image is gone, whatever sidecar it left, nobody owns what the link names: the checkout
takes a fresh clone of its own latest seed, or, with no seed, loses the link, so its next build-state refresh makes its
first volume; either is said on stderr with the lost volume's id. Every opening's mount settles this,
`cowshed doctor --repair` and `cowshed setup` included. Anything else (several volumes, or none and no adopter's seed
for a volume that exists) refuses. Why: the link lives inside the source image, so a restored checkpoint carries the
link it had when taken, and a land interrupted between renaming a target's link and updating the sidecars leaves the
target naming the landing volume. Mounting such a link as found would let the checkout write a volume a target or
another checkout owns, or one already collected. A restore therefore never rewinds the build volume.

**Refresh.** Before a job of a checkout is admitted (exec, a land check, Land step 7 output restoration), at adoption
for main, and from `cowshed setup` for every mounted workspace, cowshed refreshes the checkout's build state: it
fingerprints the tracked build inputs (Cargo manifests, `.cowshed.toml`, the build-state capabilities' markers) and,
while the fingerprint matches the one the volume's state records, only restores a fixed link a tool displaced (a real
directory where the link belongs is discarded and relinked, never copied, and reported). When it moved, capability
detection runs again in the canonical job environment (the caller's `CARGO_TARGET_DIR` and the like never name a
checkout's build state): new paths join the volume, held ones never move. A checkout with build state and no volume gets
its first one (its first touch) at the project's `[build] capacity`, and the checkout's seed with it, so it is a target
from then on. Main's first volume is minted while `cowshed adopt` stages main's image, at the capacity the adopted
checkout's `.cowshed.toml` asks, so the two attaches wait on `storagekitd` once (01a_apfs.md, "How the APFS host
degrades"), unless nothing in the checkout can name build state (no tracked Cargo or package manifest, no declared
`[build] state`, no enabled Nx detector match), whose first touch makes no volume. Main's first touch links it once main
is mounted at the checkout, when main links no volume yet and asks the capacity it was minted at (the checkout's
`.cowshed.toml` may change while main is copied; the first touch then mints its own at the capacity asked now). A mint
that failed fails only a first touch that needs a volume, with the mint's own error, and is not retried. An adoption
releases the volume before it completes whenever its first touch does not link it (nothing to hold, a refusal, a
failure, a volume already linked, another capacity), and an adoption dropped before that (a cancelled request) releases
it as it is dropped, the mint itself when it was still running. A build-state path that holds tracked source refuses
before anything is deleted.

**Owned index links.** `.codegraph/` is build-volume state and follows the same fixed-link contract as other contributed
paths. At the checkout root, `.codegraph -> .cowshed/build/codegraph`; a nested project's contribution preserves its
project-relative path on the build volume. A fork clones the index with the build state, and a land adopts it with that
volume. Refresh keeps the owned fixed link; it does not migrate the index to a real directory on the source volume. Each
workspace indexes its own tree, never another checkout's live index.

The seed-capture writer exclusion is a named list of build-state holders whose on-disk state is a non-transactional tree
of files being written:

- **Nx state** (`.nx/cache`, `.nx/workspace-data`): a task updates database, metadata and output files separately.
- **Cargo build output** (`target/`, including discovered custom target directories): a build writes its units and
  fingerprints separately.

Every other contributed path is exempt by default: it is a rebuildable cache with a consistent read, such as a
code-intelligence SQLite index or a lint/type cache, or state a capture cannot tear. Adding a contributed path does not
add it to the exclusion; extending this named list requires its own stated reason. The block-atomic image clone is a
consistent point-in-time snapshot, and SQLite WAL recovery handles an interrupted writer. Capture keeps index bytes on
the build volume and never copies a live database onto the source volume to evade a holder. No holder is killed and no
unmount is forced.

A link displaced between refreshes detaches the checkout from its volume until the next one: what a tool writes there
reaches no fork, land or seed. A tool that removes the path itself (`nx reset`, an `rm -rf` of the path) leaves its next
run to make a real directory there, and a clone of main copies that directory. `cowshed doctor` reports each build-state
path that is not what a refresh would leave (`build-link`: missing, a real directory, or something else) without
changing it, and names `cowshed exec <ws> -- true`, whose refresh relinks it.

A discarded directory is first renamed into `<checkout>/.cowshed/discard/`. That is the same volume, in cowshed's
excluded namespace, and never a sibling in the source tree. The link then takes the path, and the refresh returns
without waiting for the delete, which runs in the background. A target directory can be tens of GiB, and no job waits on
deleting it. A background delete that finds another delete of the checkout running (a refresh's or `gc`'s) waits it out
and then takes whatever is still pending, so what its own refresh moved aside never waits for a later one. A process
that ends first, a crash included, leaves the rest pending. Every later refresh of the checkout resumes the delete, and
`cowshed gc` finishes it, naming each directory it deletes. A discovery job that has not answered within 10 seconds says
on stderr every 10 seconds what it is waiting on: the processes holding the host Cargo home's package-cache locks, or
none, in which case it is the shell's activation or Cargo itself.

Cargo discovery runs at most four queries concurrently within one activation of each selected workspace shell. Answers
and failures retain manifest order; a failed manifest does not hide independent workspaces. The existing tracked-input
fingerprint caches the complete discovered path set, so unchanged refreshes run no Cargo query. This bound changes only
discovery scheduling, not carry holds, journal identity or failure fences.

Capability detection names the build-state paths (15_capabilities.md, one contribution contract): the Cargo capability
contributes each `cargo metadata` `target_directory` inside the checkout, the Nx capability contributes `.nx/cache` and
`.nx/workspace-data`, and a JavaScript package manager contributes the `node_modules/.cache` of every package it
installed. The Codegraph capability contributes each detected project's `.codegraph/` index, held at that project's
`codegraph` path on the build volume. A project with no build-state capability and no declared build state gets no build
volume and pays nothing.

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
at its first touch), and **every land into the target refreshes it from the landing workspace's build volume**:
`cowshed land` captures the guarded landing volume before the target adopts it (Land step 5.6). A named Nx or Cargo
holder refuses capture without publishing a replacement seed. A land made outside cowshed, a plain fast-forward of the
target's branch to a tree a live workspace has checked out (an **external land**), makes that workspace's volume the
seed at the target's next fork (Which volume a seed holds, below). Between lands a **reseed**, the first step of every
fork of the target (Fork step 2), refreezes the seed from the target's live volume when that volume holds a write the
seed does not and is not farther from the fork's tree. Stopping supervised jobs alone is not writer exclusion: the named
Nx and Cargo protocols must also admit capture. Each target keeps only its latest seed; a target's seed is deleted when
the target retires.

**No manual seed refresh or warm step exists.** Apart from the explicit re-adoption rule below, every seed refresh is a
step of a land or of a fork: there is no command that refreezes one, no background warm build in the target, and no land
script step. A land script that fast-forwards the target's branch itself is an external land, and the next fork takes
care of it. `cowshed reseed` replaces a workspace's live build volume from an existing seed; it never refreshes a seed.

Re-adoption preserves the live BuildVolumeId, its owner record, tree and creation time, and carries its warm filesystem
into RAW at the capacity recorded from the kernel before detach (02_workspaces.md §Re-adoption). It leaves every
old-incarnation seed unchanged as retained evidence, rather than relabeling it as a seed of the new incarnation. The new
main receives one fresh RAW seed captured from its verified warm replacement under the same writer exclusion as every
seed. This explicit retained evidence is not an active latest seed and is never silently collected as one. Re-adoption
carries its existing exclusive source `ReleaseClaim` into build publication. A RAW source and its RAW replacement share
the canonical hold resource, so capture borrows that claim through seal, restore and the immutable seed mark instead of
taking a conflicting second claim. A legacy source's `<id>.asif.hold` is a different resource: its retained claim stays
alive while capture takes the replacement's `<id>.dmg.hold` normally.

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
volume flushed first, was modified after the seed's mark. A fork sets its own seed's time to its new live volume's, so a
new workspace starts with a fresh seed rather than one behind by its own mount. Output restoration and later work on an
adopted volume reach the seed through the next fork's reseed.

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
3. every existing Cargo profile's `.cargo-lock` is taken exclusively without waiting; Cargo holds that lock shared for
   its whole build, so a held one skips the reseed with its lock path and processes. The admitted profile locks remain
   held through capture;
4. the live volume and its backing image are flushed, then the existing mounted-source descriptor-clone captures the
   image atomically. Capture does not require unmount or detach; open files on exempt contributed paths do not refuse
   it. The Nx and Cargo guards remain held until the cloned bytes and immutable freshness mark are fixed;
5. the clone's extent-map write and owner-record publication complete before supervised admission reopens. A job
   admitted afterwards cannot invalidate the snapshot; its later writes make the seed behind again. Dropping the capture
   claim only unlocks the source's hold; it never retires or deletes the source.

Land already owns a shared GC hold on its quiet source. Its typed quiet capture retains the stopped supervisor socket
lease that refuses replacement admission, from before the jobs stop through capture and adoption. It does not take a
second exclusive claim, nor upgrade the shared hold: Darwin flock upgrades release and retake the lock and would reopen
the gap. The socket lease is released only before replacement supervisors start, after the immutable seed mark and
adoption (and a kept workspace's refork). The shared GC hold remains through the rest of the build-volume steps. The
quiet capability and the capture method that consumes it are compiled only with the macOS runtime; portable APFS backend
operations do not depend on that platform-only capability. Re-adoption's retained-image runtime operations likewise
compile with macOS, while its durable record data, Git evidence schema and pure command-contract and Git-preservation
tests remain portable.

The guarantee is no capture mid-write by a holder of a known Cargo profile or by an admitted Nx state holder. It is not
a kernel-wide filesystem-writer barrier: an open index reader, editor or other exempt cache holder does not refuse
capture. Neither those holders nor the named writers are force-unmounted or killed.

**Known gap: a host process can create a new Cargo profile after enumeration.** The current profile locks do not fence
that new directory, and mounted capture no longer has the detached-volume barrier that previously covered it. Such a
host build can leave intermediate build-output bytes in a seed. A producer-side capture lease, acquired by every Cargo
invocation before it creates or writes a profile and held for the build, is required to close that gap. This is queued
in `agent-todo/cowshed-small-spec-gaps.md`; no protection for that case is claimed by the current capture contract.

**Which volume a seed holds.** Every build volume's sidecar records the tree it was built at (01_storage.md "Layout"): a
seed the tree it was frozen at, a land's adopted volume the landed tree, a fork's live volume its seed's. A fork names
the tree it checks out: the target's `HEAD^{tree}`, or under `fork --ref <rev>` the tree of `<rev>`'s commit. A volume's
**nearness** to that tree is _equal_; else the number of paths `git diff-tree -r` reports between the two trees in the
target's repository; else _unknown_ (no recorded tree, or one the repository cannot compare, said on stderr), farther
than any count. Fork step 2 decides, under the target's image lock:

1. **An external land.** When neither the seed nor the target's live volume is equal and the fork's tree is the target's
   `HEAD^{tree}`, the target's branch moved past every volume cowshed knows, and a live workspace whose `HEAD^{tree}` is
   that tree ran its gates there. The seed becomes a capture of that workspace's build volume, recorded at the tree.
   Candidates are tried warmest first (latest written). Each one's image lock is taken without waiting (a held one is a
   skip naming it: its own fork or land is running), its `HEAD^{tree}` is read again under that lock, and a candidate
   whose tree moved is refused: a seed is never recorded at a tree its volume was not built at. It is then captured
   under the same writer exclusion as every seed, above, with its own Nx daemon stopped as at a land. When no candidate
   is captured, each skip is said on stderr and the decision falls to the reseed.
2. **A reseed** refreezes the seed from the target's live volume when the live volume is nearer the fork's tree, or as
   near and written after the seed (behind, above). A seed nearer than the live volume is kept however much later the
   live volume was written, and the live volume is not sealed at all: a landed seed beats a live volume built at an
   older tree, and a long-lived reader of the target's volume (an editor, an indexing server) costs such a fork nothing.
   Both at the same tree: the target ran tasks on the adopted volume after its land.

A skip names each holder and leaves the seed as it was: the fork is colder, never wrong, and the next fork tries again.

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
fast-forward performs no carry, output restoration or adoption.

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

### Workspace reseed

`cowshed reseed <ws> [--from <target>] [--dry-run]` discards a workspace's accumulated incremental build state and gives
it a fresh clone of the named target's **existing latest seed at its current incarnation**; `--from` defaults to `main`.
This is a maintenance verb like resize, not a `gc` option: garbage collection removes images nobody owns, whereas reseed
explicitly discards state an active workspace owns. The workspace must already be mounted, so its authoritative build
link can be read. The target need not be mounted. No source image, Git ref, HEAD, index, tracked edit, untracked file or
declared task output is copied, reset or deleted. Only the excluded `.cowshed/build` link changes.

When selecting whether to reseed, cheaply priced Nx rebuild time decides; when no usable Nx work score exists
(Cargo-only projects included), space decides and the default is the project's main seed, because unpriced
workspace-only build state retains private blocks.

The seed is cloned as it stands, even when it is behind the target's live build volume or built at another tree.
Workspace-specific output is lost: the next build pays a cold rebuild for incremental units or cache entries the seed
does not contain, with shared content-addressed caches still available. A missing current-incarnation seed refuses
before replacement; the operation neither creates an empty fallback nor captures the target's live volume. It never
writes, refreshes, retires or deletes any seed, including the workspace's own seed, and never promotes retained
old-incarnation evidence into a current seed.

Preview is read-only: no attach, flush, daemon stop, hold-file creation, lifecycle recovery or link mutation. It records
the workspace and target source-image identities, current build link and record, current seed identity and record, and
the build images' identities, modification times and allocated bytes. Execution acquires the existing workspace and
target lifecycle-intent leases and image locks without waiting, re-observes that plan, and refuses a stale observation
before replacing anything. The target image lock remains held while the seed is cloned, exactly as in a fork, excluding
a concurrent land or fork's seed replacement. An unfinished create/fork/adopt or re-adopt refuses with its workspace,
journal path and recorded operation rather than replaying it.

Before stopping the old build volume's Nx daemon, execution takes its `<id>.dmg.hold` **exclusively without waiting**.
An admitted job's shared hold, a land's hold, or another exclusive claimant refuses and names the hold path and each
visible holder's PID and command. Held lifecycle/image locks likewise name their paths and holders. No jobs are killed.
With admission fenced, the existing Nx-close protocol stops only its own daemon; another database holder refuses by PID
and command. The old build volume then seals without force: a filesystem holder or late writer refuses and is named,
retaining the original build link. A prepublication failure restores the old canonical build mount.

The replacement has a new BuildVolumeId and is held from before cloning through publication. Its `Linked <ws>` record
precedes the atomic build-link rename; the old record becomes `Unlinked` and the old image is released under the same
exclusive claim. A crash on either side of publication therefore leaves the link naming a correctly owned image and at
most an unreachable image for ordinary collection, using the existing link-reconciliation rules. An already-running
supervisor grants the replacement at its next job admission, as after a land; it is not restarted to terminate holders.

Byte accounting is the same as build-volume garbage collection: each RAW image contributes `st_blocks * 512`, a
directory backend its allocated tree bytes. Dry-run reports the old image's allocated bytes and the seed clone's
allocation estimate; execution reports the actual replacement allocation and the old image's allocation when deleted as
`freedBytes`. A seal may flush more blocks than preview observed. These are **per-image allocated bytes**, not a promise
of that much unique physical free space: APFS clones and retained seeds can share extents. Subtracting the replacement
allocation gives a per-image net estimate, not a container-free-space measurement.

- **Enforced by**: directory-host and real-APFS tests that refuse and name shared job holds, exclusive land claims and
  filesystem readers; then replace the volume, report deleted allocation and verify dirty tracked source, untracked
  files, Git state and index bytes unchanged. A stale source seed is used without refreshing or mounting it. Focused
  directory tests refuse absent seeds, unfinished lifecycle journal entries, held image locks and stale plans.

## Fork: `cowshed fork [<src>] <ws>`

A fork names its source: absent `<src>` defaults to main; an explicit source may be any session or integration
workspace. All sources use one mint path, carrying edits and untracked files warm by default, or cleaning with `--clean`
/ `--ref` (02).

1. Flush and descriptor-clone the target's live RAW source image without detaching it (02_workspaces.md). Source
   filesystem crash consistency does not replace build-state writer exclusion. The fork holds the target's image lock
   from here to the end of step 3.
2. **Refresh the target's seed** (Targets and seeds, "Which volume a seed holds"): from a landed workspace's volume
   after an external land, else by a reseed from the target's live volume when that is nearer or as near and newer and
   has no writer.
3. Clone **the target's** latest seed image to a new build volume owned by the new workspace, attach it, and point the
   workspace's `.cowshed/build` at it. Clones preserve every file's bytes and mtime; Cargo freshness depends on that
   (rule "Clones preserve mtimes").
4. Delete `nx/workspace-data/d` in the new build volume: a daemon record names another checkout's process and socket
   (rule "One Nx state per checkout").

Steps 2 and 3 up to the attach run beside step 1's attach of the cloned image, under the same lock: every attach waits
in the host's one `storagekitd` queue (01a_apfs.md "How the APFS host degrades"), and the two in flight together wait in
it once. The new workspace's own seed is cloned, and its link written, only once its checkout is staged; a clone that
never stages releases the volume made for it.

The fork is warm for everything the seed's volume held when it was last frozen: at the latest land, at the next fork
after an external land, or at the latest reseed, which is this fork's own step 2 unless the target's volume had a writer
or is farther from the fork's tree than the seed. If the fork's tree has moved past its seed's (a land skipped its
freeze, or `--ref` names a tree no volume was built at), the fork builds the difference incrementally, as an edit would.

## Land: `cowshed land`

Land's contract is 02_workspaces.md "`cowshed land`": it lands the head the caller validated into its target (main by
default, an integration workspace with `--into <target>`), under the target's repository lock, and never rebases on its
own. It builds nothing on the target: it adopts the workspace's build volume. Every step reads the same for main and for
an integration workspace; "the target" is whichever one it is.

1. **Refuse a dirty tree**.
2. **Validate in the workspace**: the caller's check (`--check`, for an Nx project `nx run-many -t lint test build` over
   what the workspace changed) runs in the sandbox against the workspace's own build volume. Only the delta builds.
3. **Update the target** under its repository lock (02_workspaces.md "`cowshed land`" step 3). When the target's
   checkout has the target branch checked out, the branch fast-forwards through that checkout and the land continues at
   step 4. When nothing has the target branch checked out, the land compares-and-swaps the branch ref and **skips steps
   4 to 7**: the target's checkout holds another branch, so its build volume and seed are not the landed branch's. The
   target keeps its build volume and its seed; nothing is quiesced, carried, frozen or adopted.
4. **Quiesce the landing workspace.** The land holds the landing build volume (the shared `<id>.dmg.hold` a job holds,
   GC below) from before the workspace's jobs stop until its build-volume steps end (after step 7), so no collection in
   any process releases it while step 6 renames the target's link and records the target as its owner. Its supervisor
   stops the workspace's jobs, and the stopped supervisor socket lease stays with the quiet volume through capture and
   adoption, refusing a replacement supervisor's job admission. Its private retire call carries `ControllerOwned` Nx
   policy: the actor gives up keeper-record cleanup, not admitted-job retirement. Land's final retirement carries the
   same policy; ordinary lifecycle callers explicitly select `Recorded`. An old supervisor that cannot accept the
   controller handoff is refused rather than generically drained. Before that retirement the controller reads each
   landing Nx daemon's plan from the job group ledger: a record only nominates a pid, and only a process the ledger
   proves is the job's own is signalled, with its exit watch registered first. After retirement it attempts `SIGTERM`
   through that identity, reports whether the signal was delivered, runs a one-shot `nx daemon --stop` only to remove
   the daemon's socket (its failure is reported and never aborts the land), and waits up to the shared 10 s grace. A
   daemon the ledger does not prove the job's, or one that outlives the grace, skips adoption as `LandingDaemonStayed`;
   an unproven daemon is never signalled. A daemon started detached outside the ledger is that skip. An unreadable or
   malformed daemon record or job ledger instead skips as `LandingDaemonUnverifiable`, carrying the record path and
   reason, before quiesce or any signal. Nothing is signalled through a record alone. The landing build volume's Nx task
   database must then have no open file descriptors, read without signalling. If it still does, adoption is **skipped**
   and reported, as for the target below. This settles supervised jobs and Nx; the later seed capture retains Nx open
   locks and existing Cargo-profile guards through capture and publication. Other contributed paths remain exempt under
   the named-list rule (Owned index links). Mounted capture replaces the former detach-and-seal barrier; the
   new-host-profile gap remains as documented in Targets and seeds.
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
   6. take the target's image lock and admit the landing volume's Nx state and existing Cargo profile locks immediately
      before capture. A live task-database holder is `landingHeld`, a process opening that database is `landingOpening`,
      a daemon that outlives its stop is `landingDaemonStayed`, and a held Cargo profile is `landingBuilding`, with the
      resource and holder named. Retain the Nx open locks and admitted Cargo locks through the mounted-source capture.
      Every other contributed path is exempt under the named-list rule above. Capture does not unmount the live volume
      or kill an index reader. On success, publish the target's new seed and retire its previous seed. Writes after the
      snapshot reach the seed through a reseed (Targets and seeds); the documented new-host-profile gap is not claimed
      closed.

   Resolving either build volume for mounting takes its per-image lease **before** reading the attachment inventory. A
   recovered attachment owns a raw IOMedia pin across the subsequent kernel mount-table read and mount, just as a
   freshly verified attachment does (01a_apfs.md, "Mounts"). A concurrent fork's reseed may finish while that lease is
   awaited: the lookup then resolves its restored device, not the earlier device name. Once pinned, a reseed cannot
   detach that attachment before the mount decision completes. Reading the attachment before awaiting the lease would
   let a land's `close-target` pin open a device a concurrent `cowshed new` has already detached, after Git delivery but
   before the build-link swap. No retry or wider target-lock interval substitutes for this identity handoff.

   The mount-table decision is made under the image lease reacquired for mounting, not in the gap after attachment
   recovery releases its lease. A second mounter can win that gap without changing the pinned device. The image lease
   and raw pin remain held through the decision and command. If `mount_apfs` reports `Operation already in progress`,
   cowshed observes the mount table once more under that same authority: only the expected volume at its canonical
   mountpoint is adopted. An absent mount, a different device or mountpoint, or an unreadable table refuses with the
   image, expected device and mountpoint and the original command failure. There is no blind mount retry; unrelated
   command failures are never converted into success by a subsequent mount observation.

   The land holds the target's image lock from 5.6 until 6.4 has renamed the target's link and a kept workspace has its
   fresh clone of the target's latest seed (the new one, or the previous one retained by a skipped capture). Every fork
   of the target holds the same lock from before it reads the target's seed and link until it links its own (Fork steps
   1 to 3), and so does every reseed, so neither reads a seed or a volume the land deletes. Without it, a land that
   freezes main's seed and releases main's previous volume while a fork of main is under way deletes the seed the fork
   has read, and the fork fails (`sync source image …: No such file or directory`). A land that finds the lock held says
   so on stderr and waits for it. It waits with the target's daemon already stopped (5.3), so the longer it waits, the
   likelier an Nx client restarts it and 6.2 skips the swap, reported as any skip is.

   A skip at 2 or 4 deletes what was staged, still freezes the seed (5.6), and skips the swap. A skipped capture at 5.6
   keeps the previous seed and still swaps.

6. **Adopt the build volume.** Still under the lock:
   1. take, without waiting, stock Nx's open lock of each of the target's task databases (the database's path with the
      `lock` extension; every Nx process locks it exclusively before it opens the database and lets go once it has,
      pinned Nx 23.2.1 `native/db/mod.rs`). A lock some process holds means an Nx process is opening the database: skip,
      and name it. Held, the locks keep every Nx process from opening a database until the rename is done; one that
      starts meanwhile waits, then opens the database by its path, so through the moved link;
   2. query the target's task database once more: a holder that opened it since 5.4 means skip;
   3. delete `nx/workspace-data/d` in the landing build volume, then copy into it the target's record of a live daemon
      that runs outside every sandbox (the host daemon a client started since 5.3);
   4. `rename(2)` a new `.cowshed/build` symlink over the target's, naming the landing workspace's build volume, and let
      go of the locks;
   5. hand ownership in the sidecars: the target owns the adopted volume, and its background label becomes
      `build <target>`; its previous volume becomes unlinked and is released before land returns (GC below), unless a
      running job's hold still owns it. This applies with `--no-retire` too. Nothing links it any more, so its release
      runs beside what the land does in the target next (its supervisor, a kept workspace's fresh clone, the adoption
      check of step 7), so the release's unmount, detach and delete can overlap them. It needs no image lock: a fork or
      reseed that takes the lock after the rename reads the adopted volume, and none that held it before the land took
      it still runs.

   A skipped swap is reported in the land report with each holder's pid and command. The target keeps its build volume
   and builds the landed delta incrementally the next time anything builds there; forks start from the new seed until
   the target's own volume is written again, and then from a reseed of that volume, which holds the target's own work
   and builds the landed delta as the target does. A skipped swap is never wrong, only slower.

   Every seeded/unseeded and adopted/skipped result is also a durable `LandOutcome` commitment, including every
   `AdoptionSkip` and its holders (13_telemetry.md §"Controller audit record schema"), not just cache misses.

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

8. **Retire the workspace**. An adopted build volume is now the target's and does not retire with it. After a
   compare-and-swap at step 3 nothing adopted it, so it is still the workspace's.

### Carry

A landing volume holds what its workspace forked from and what it ran since. The target's volume also holds what other
lands and the target itself ran in the meantime. Adopting the landing volume as it is discards all of that, though Nx's
entries are content-addressed and every one is a correct hit for any tree that computes its hash: a fork made right
after such a land would miss tasks main's previous volume held at that very tree. So before the target adopts the
landing volume, every Nx cache entry the target's task database indexes and the landing volume's does not is copied into
the landing volume and indexed there. Only Nx state is carried; codegraph is cloned/adopted per Owned index links. A
Cargo target directory is per tree and mtime-fresh, so the landing volume's is the one that matches the landed tree.

The staged carry owns a shared hold on the source build volume until commit finishes or staging is abandoned; the quiet
landing volume already owns the destination hold. These holds protect both filesystems across the daemon-close interval,
before the later target image lock is acquired. A lifecycle regression proves the source cannot be exclusively claimed
while staging lives and is released after commit.

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
  `accessed_at` included, so Nx's own age and size eviction treats them as it would have in the target. The transaction
  runs with foreign keys enforced, so an index Nx could not have written fails the carry instead. `.carry/` is then
  deleted.

Entries cross from one image to another, so they are copied, never cloned. Their volume is the delta of what other lands
and the target ran since the landing workspace forked, which is what a fork would otherwise rebuild. Copies run before
the target's database is closed and cost no window; a failed copy (a full volume) stops the carry, keeps every entry
carried until then, and is reported (`carried.stopped`); the land still succeeds.

A landing Nx root with no task database never ran Nx. After build-state admission and before checks or fast-forward,
each detected Nx root is initialized through its checkout's own pinned Nx 23.2.1 `DbCache`, constructed with
`{nxCloudRemoteCache: null, skipRemoteCache: true}`. Nx owns the complete schema initialization; the adapter never
invokes remote initialization. An initialization failure or a different Nx version stops only Nx-entry carry, naming the
failure or found version in `carried.stopped`; validation, fast-forward and adoption policy are unchanged.

Carry requires an existing Nx-initialized v3 database and refuses one it cannot recognize. It writes no DDL or `VACUUM`,
and copies no schema, header, journal, version or task-history metadata from the target. Initialization is Nx-owned, not
a `VACUUM INTO` copy of another checkout's schema. A carry that crashes leaves only `.carry/`, which the next stage on
that volume deletes before it starts.

- **Enforced by**: a real-APFS test in which one workspace lands and warms an Nx task in main, a second, forked before
  that land and changing nothing the task hashes, lands without running it, and a fork of main hits the task.

### Rebase carry

A workspace forked before other lands holds none of what they ran. `cowshed rebase` moves its tree onto the target's,
where the target's volume already holds those tasks' entries, so the workspace's next gate would re-run every task the
target ran at that tree. So once a rebase has not conflicted, stock Nx computes task hashes in the rebased checkout.
Only target entries with one of those current-tree hashes that the workspace lacks are eligible to carry; historical
hashes are not copied merely because main still indexes them. Each Nx root is selected independently. An unavailable or
failed hash computation skips the carry with the root and reason; it never falls back to copying all rows. The target's
volume is the source and the workspace's is the destination.

Both volumes' shared build holds are acquired before staging and retained through commit, including while their Nx
daemons stop. A reseed or release must not withdraw either filesystem in that interval: Nx open locks fence database
openers, not volume capture or collection. A refused build hold fails the carry with its side and hold path; it does not
clear cache state or retry without the hold.

Selection covers cacheable, non-continuous tasks in each root, with no configuration and with each declared
configuration, using Nx's own task graph, per-task environment and hasher. CLI overrides are not guessed: an invocation
whose forwarded arguments produce another hash misses normally.

Stock Nx hashes a task before any task runs only when its hash cannot depend on another task's run. A task with a custom
hasher, or with dependencies and a `dependentTasksOutputFiles` input, is deferred: `nx run-many` hashes it only after
its dependencies have run and written or restored their outputs (pinned Nx 23.2.1, `hash-task.js`
`hashTasksThatDoNotDependOnOutputsOfOtherTasks`, then `hashTask` per task). In a rebased checkout the dependency outputs
on disk are stale: outputs live on the workspace's image, and neither a rebase nor a land refreshes them. A deferred
task hashed up front would name a key no run looks up, so the probe applies Nx's own rule and never hashes one.
Selection therefore has two classes, and each names only current tasks:

- **By hash**: each task Nx hashes up front contributes its hash. The requested hashes form the outer SQLite loop and
  probe the source cache's primary key, so this class does not scan the source's historical cache.
- **By task**: each deferred task contributes its identity (project, target and configuration, as `task_details` records
  them) and selects the one source entry of that identity with the latest `cache_outputs.accessed_at`. That is the entry
  the target's last run of the task read or wrote, at the target's head tree. The rebased tree is that head plus the
  workspace's diff, so for every deferred task the diff does not reach it is the entry the gate looks up once the task's
  dependencies have run; a deferred task the diff does reach costs one entry that misses, never more. The latest entry
  is chosen before the destination is consulted: a destination that already holds it takes nothing for that task, never
  an older entry. `task_details` has no index by identity and the carry never writes the target's database, so this
  class reads the source's `cache_outputs` once, a table Nx's own eviction bounds, and probes `task_details` by its
  primary key and the identities by theirs. The `task_details` history of evicted entries is never read.

No entry is copied merely because the target still indexes it: one entry per current hash, at most one per current
deferred task.

1. **Select and stage** while both sides run: temporary SQLite tables index the current hashes and the deferred task
   identities, joined to the target's missing rows. Only these entries are copied into the workspace volume's `.carry/`,
   as at Land 5.1. The hash probe establishes the destination's own Nx task database. If it is absent, the carry stops
   rather than copying the source's potentially unbounded task-history database just to initialize a cache.
2. **Close each side**, the workspace first, as Land 5.2–5.4 close the target: when nothing but the side's daemon holds
   its task database, stop that daemon (it restarts on its next client); any other holder skips.
3. **Take both sides' open locks and look again**, as Land 6.1–6.2 do for the target: a lock some process holds, or a
   database some process opened since the close, skips. The workspace's jobs keep running through a rebase, unlike a
   landing workspace's, so this look is what proves nothing in the workspace writes its database during the commit; an
   Nx process that starts meanwhile waits on the lock.
4. **Commit**, as Land 5.5, retaining the same selection of both classes for entries indexed after stage (a deferred
   task the target ran again since then selects its new latest entry, and the staged one is dropped), then let go of the
   locks.

One budget spans both phases and every Nx state. The row count is a defensive cap of the selected hash and deferred task
counts across database pairs, including multiple machine databases that might index the same hash. Recorded entry bytes
and block-rounded copied bytes are capped at a tenth of both the destination image's capacity and its backing store's.
Both filesystems retain a tenth of their capacity free, and the allowance is reduced to the currently available space
above either reserve. A sparse RAW image's virtual free space does not prove its backing store has space. Every
filesystem object is charged and both free-space values checked before its write. A failed store-space query stops the
carry, never falls back to image space alone. A cache row understating a file's size cannot hide an unbounded data copy.
Files are walked once, not sized in a preliminary second traversal; regular-file reads are pinned and length-limited,
growth or shrinkage refuses the partial entry, and directory metadata is restored after children. Hitting a bound stops
with `carried.stopped`, keeps whole entries already carried, removes the partial entry, and leaves the rebase
successful. Moving staged entries at commit consumes no second copy allowance.

A skip deletes what was staged and names the side, the database and each process with its pid and command; the rebase
itself has happened and stands. A copy that fails stops the carry and keeps what was carried until then, as at a land.
The report is `RebaseReport { oid, buildVolume }`, where `buildVolume` is `carried` with the `NxCarry` counts and
`deferred`, how many of `carried.entries` the by-task class selected (the rest were selected by hash), or `skipped` with
the reason. A rebase moves no seed: the workspace's seed catches up with its volume at its next fork (Targets and
seeds).

The hash-probe skip is `buildVolume: {kind:"skipped", reason:{kind:"taskHashes", state, reason}}`, where `state` is the
checkout-relative Nx state directory (`<root>/.nx`) and `reason` explains the failed process or unreadable/invalid
output. This is a successful Git rebase with no carried entries, not a refused rebase.

A target with nothing to select from is not a workspace missing nothing. When none of the target's task databases the
carry reached indexes a cache entry (no `cache_outputs` rows, or no database) at any Nx root with a selection, the carry
skips before either side is closed, with `reason:{kind:"unindexed", states}` naming those roots' checkout-relative Nx
state directories: no Nx run cached a task into the target's volume, so a `carried` with 0 entries would hide it.

- **Enforced by**: a real-APFS test in which a workspace forked before a land rebases after it, its report counts the
  land's entries carried, and its run of the landed check then hits every task; a real-APFS test in which a task hashes
  its dependency's outputs (`dependentTasksOutputFiles`), the rebased workspace's copy of those outputs on disk is
  stale, and the task is still carried by task and hits; a carry regression with a historical cache larger than the
  rebased tree's selected entries that asserts only selected files are written; a by-task selection test that carries
  one entry per deferred task out of several of its historical entries, and none when the destination holds the latest;
  row, recorded byte, actual copied byte, backing-store reserve/query failure, indexed-query plan (both classes) and
  late-entry selection tests that preserve the completed prefix without a partial entry; and a unit test in which a
  target database without `cache_outputs`, with an empty one, or absent reports `unindexed` while one indexing only
  other hashes carries 0.

A directory-host regression attempts both exclusive build claims during the recorded Nx daemon's shutdown. Both are
refused, and carry still places and indexes the selected output. SQLite refusals retain the failed statement and
extended result code; I/O/open failures also retain SQLite's OS errno, never a stale errno from a later lock refusal.

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

- The last checkout link is the reclaim moment. Removing or retiring a checkout, adopting a replacement, failing a fork,
  or superseding a seed releases every volume that no checkout links and no target keeps as its latest seed. `rm` and
  `land` (including `--no-retire`) run collection before returning, not only on a later explicit `gc`.
- Ownership is a job hold, a land's hold on its landing volume, or an unfinished create/fork/adopt in the lifecycle
  intent journal, not the kernel's refusal to unmount. Every admitted cowshed job holds a shared flock on the build
  volume's `<id>.dmg.hold` for its lifetime; release must claim it exclusively. A running job therefore keeps main's
  previous volume across a swap.
- A held lifecycle or image lock likewise defers release. A superseded seed is retired only under its target's image
  lock, which the land and reseed paths hold while they replace it, and seed retirement takes that lock as a typed
  witness, so collection never releases a seed that a land or reseed is replacing.
- Collection decides from a snapshot, and a land can move a target's link onto a volume after it: an `rm` that retires
  the landing workspace while that workspace's land still runs sees the landing volume linked by nothing, and once it
  releases it, main's link names a volume that no longer exists. So each release claims the volume first and reads again
  under the claim: a volume a checkout's link names now is that checkout's and stays, and one whose sidecar is no longer
  the one the collection judged it by is left to the next pass. Whatever makes a volume someone's holds it while it
  does: a land from before its landing workspace's jobs stop until its build-volume steps end, a clone (a seed, a fork's
  volume) from before its image exists until its sidecar is written.
- Once no owner remains, release first requests a non-forced unmount and allows a bounded wall-clock grace for holders
  to release it: time spent waiting on command execution or a disk lease counts, not only the requested poll sleeps. If
  the kernel still refuses, cowshed forces the unmount, detaches the image and deletes its image, sidecar, hold file and
  mountpoint. At the initial unforced refusal, it records the holders and names each pid and command on stderr,
  distinguishing release within the grace from a forced unmount. Editors or indexers holding files or a working
  directory without a cowshed job hold cannot keep an unlinked volume attached indefinitely. Force revokes their access
  to the volume; it does not kill them.
- Every release that deletes an image, a seed's or another volume's, whatever asked for it (collection, a land's release
  of the target's previous volume or superseded seed, a reseed, a fork that never staged), appends one line for the
  image to the project's `deletion-log.jsonl` (01_storage.md): op `release-build-volume`, kind `build-seed` when its
  sidecar recorded it as a seed and `build-volume` otherwise, and the workspace that sidecar named, read before anything
  is deleted. A target's previous volume is recorded unlinked before its release, so its line names no workspace. A held
  volume is not released and leaves no line; a release that finds the image already gone deletes none and leaves none.
  Without this journal, an `rm` that releases the volume a land has just moved main onto leaves no trace of it.
- Each target keeps only its latest seed; a superseded seed is released when replaced, and a target's seed is released
  when the target retires. Detached seeds do not need an attach or a rename to be deleted.
- GC also enumerates seed sidecars whose images are already gone. Under the ordinary exclusive release claim it rechecks
  image absence, the sidecar and live links, then deletes the orphan sidecar with zero image bytes reclaimed. An image
  appearing or a record/link changing defers it; linked records and retained existing seed images are not relabeled or
  collected by this rule. This includes payload-absent legacy `<id>.asif.json` seed sidecars as the explicit
  metadata-only exception to RAW-only enumeration (01a_apfs.md §Images). A strict Seed record is required. Reuse
  `claim_re_adopt_source` to claim the legacy image's actual hold file, then re-read record, image absence, live links
  and forming lifecycle work under that same claim; then remove the sidecar and its obsolete hold file, and sync the
  images directory. No legacy payload is attached, converted, relabeled or released, and an existing legacy seed image
  remains retained evidence. Preview creates no hold file.
- A fork's volume and seed exist before its workspace does: `cowshed fork` clones them into the staged checkout, which
  no other process can read, and publish the workspace afterwards. Main's first volume likewise exists before main's
  first touch links it: `cowshed adopt` mints it beside main's image, and completes its intent only once the first touch
  linked it or the adoption released it. While the create, fork or adopt is past its mutation fence and unfinished in
  the lifecycle intent journal, collection in any process defers the volumes recorded as that workspace's or as its
  seed, and every image without a record, naming the workspace. Without this, an `rm` in one process deletes the volume
  and seed of a fork running in another, and the fork's mount refuses its link to a volume nobody owns. A verb crosses
  its fence before it makes a volume, so collection lists the images first and then reads the journal and the
  workspaces, and decides only the images it listed: one made after the listing is left to a later pass, never decided
  from links read before it existed. An adoption killed between the mint and the link leaves an image without a record,
  deferred until the replayed adoption completes and collected after.
- Collection says why it skips: a job hold, an unfinished create/fork/adopt, an unreadable detached checkout or record,
  or a failed release names the volume and reason. Opportunistic collection counts routine detached/still-forming
  deferrals on one line. One volume's release failure does not hide later candidates. `--dry-run` changes nothing and
  reports the same ownership reasons.
- Inside a build volume, Nx's own cache eviction runs unchanged (age and size bounds, configured in `nx.json` as Nx
  documents). Its database and its cache directory are always the same pair, so its eviction never deletes what another
  database indexes. A carried entry keeps the row the target held, last use included, so it ages out as it would have in
  the target, and the carry indexes every entry it places, so nothing it writes escapes Nx's eviction; its staging
  directory is its own to delete (Land, "Carry"). Gc never opens a volume's contents. Cargo's target directory is
  bounded by the tree it builds; cowshed does not prune it.
- A workspace's build volume retires with the workspace (`cowshed rm`), unless main adopted it.

## Rules that keep it warm

Each rule states what is enforced and why. Each has an enforcing test or check; a rule without one is not done.

### Read-only cache-reuse advisor

`node packages/cowshed/bin/cache-advisor.cjs --workspace <checkout> --main <main-checkout> -- [bun] nx run-many <args>`
advises an operator before a gate; it never rebases, restores outputs, carries entries, reseeds, replaces or deletes an
image. It accepts an explicit `run-many` argv, including project selection, configuration and task overrides, not shell
text or package-script indirection. It runs in the workspace's ordinary activated toolchain environment. Its one JSON
report is stdout; Nx diagnostics and failures are stderr.

The standalone script ships beside the cowshed launcher. The embedded `build_volume/task_hashes.js` remains the carry
helper: its all-target/all-configuration selection, database writes and daemon stop are deliberately different from an
advisor for one next command. A new cowshed verb would add supervisor/controller/API lifecycle plumbing to a read-only
calculation, and an advisory branch in carry would mix these incompatible authority contracts. The standalone surface
can run from a workspace's existing read grant; it needs no new lifecycle operation or candidate write grant.

On macOS, the advisor starts one confined child with kernel denial of persistent writes except its private scratch
directory. The nonpersistent `/dev/null` device is explicitly writable because Git opens it O_RDWR. All process signals
and network access are denied except outbound IPC to the configured `SCCACHE_SERVER_UDS` Unix socket. Graph plugins and
runtime-input descendants inherit that denial. The probe does talk to sccache for metadata/toolchain queries
(`cargo metadata` asks its compiler for `-vV`); this is a read-only consultation of a toolchain service, not a build or
target invocation. A missing/unavailable wrapper or service fails loudly, never unsets `RUSTC_WRAPPER` or silently
probes a different toolchain. Unsupported platforms refuse rather than run unconfined. Node 22.13 or newer supplies
SQLite.

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
The report distinguishes `HIT_BOTH`, `HIT_OWN`, `HIT_MAIN`, `MISS_BOTH` and `UNRESOLVED`. A keyless task retains
`UNRESOLVED` classification and its reason, but its candidate costs use explicit `ASSUMED_MISS` statuses, labelled
"unpriced key, worst case: misses in both". No hash or actual cache result is invented. A task's weight is the
arithmetic mean of successful uncached `task_history` execution seconds joined to `task_details` by hash and grouped by
task identity, not exact current hash. Cache replays and failed/stopped attempts are excluded. A UNION-equivalent tuple
of identity, hash, start and end de-duplicates histories inherited by cloning; sample count and min/max accompany every
mean.

Totals sum weighted misses for own and main and report `own - main` in serial task-seconds, not future gate wall time.
Every keyless task with a duration contributes its own historical mean as an assumed miss in both candidates, including
deferred tasks, omitted hooks and refused runtime inputs. `totals.keylessWorstCaseSeconds` gives that common term once
(it is included in each candidate's total); `weightedKeylessTasks` counts its tasks. Each candidate's
`MeasuredMissWorkSeconds` term reports only weighted misses established by current-key lookups. The difference is
computed from those measured terms so the common assumption cancels without floating-point loss.

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
The read-only cache-reuse advisor alone bypasses the daemon and computes in-process, without stopping, restarting or
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
- An idle shed's daemon is stopped, never disabled. Five minutes after a shed was last in use, its keeper stops its
  daemon and starts none until the shed is in use again (04_sandbox.md), and the next Nx command starts one. What the
  rule protects is the warm state of a shed in use; a daemon nothing uses is a resident process, its plugin workers and
  a file watcher recomputing the project graph for every change in a tree nobody builds.
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

### One Cargo environment on every path

Every Cargo invocation of a checkout (host shell, sandboxed job, Nx task, land check, a long-running dev process) sees
the same unit-identity environment. Concretely:

- **No `CI` in a dev environment.** Cargo turns incremental compilation off for workspace crates whenever `CI` is set,
  which changes their `-C metadata` and writes a second set of units into the same target directory: a target built
  without `CI` is not wholly `Fresh` under `CI=true`. Agent harnesses export `CI=true` on every command. The workspace
  environment cowshed provides unsets `CI` unless the runner identifies itself as real CI (`GITHUB_ACTIONS`, or the
  runner's own marker), and a repository's dev shell does the same.
- **The toolchain's compiler variables.** `CC_<target>`, `CXX_<target>` and `AR_<target>` must be the dev shell's. Build
  scripts using the `cc` crate declare `rerun-if-env-changed` on them; one missing variable rebuilds every `-sys` crate
  that reads it, and all of their dependents.
- **No absolute checkout paths in compiler flags.** `--remap-path-prefix=<absolute checkout>=…` puts the checkout path
  into `RUSTFLAGS`, which is part of every unit's identity, so every checkout builds its own units. Use
  `-Zremap-cwd-prefix` (relative), or no remap.
- **No checkout-resolved linker paths.** `target.<triple>.linker` and `host.linker` are a bare PATH program or an
  immutable store path, never a checkout-relative path: Cargo hashes the resolved linker into every unit. On macOS the
  repository's store-installed linker uses exported `SDKROOT` without discovery and executes `/usr/bin/clang` directly;
  only an absent SDK invokes `xcrun`.
- **Two profiles.** `dev`, which includes unit tests, and `release`, for a released binary. `test` inherits `dev`.
  Danny, 2026-09-30: "BUILD EVERYTHING ONCE. CARGO ALREADY TRACKS CHANGES AND REBUILDS WHAT HAS CHANGED"; "You have
  realistically 2 profiles: dev (which includes unit testing) and release". 03_caches.md states the profile rule.
- **sccache is for dependencies and CI, never dev targets.** Incremental workspace units are not cacheable by sccache;
  the wrapper does not change fingerprints either way. Danny, 2026-09-30: "sccache is for CI and stable crates, not for
  dev targets". Host shells receive the detector's exact pinned client, endpoints and request bases through
  `.cowshed/env`, matching supervisor children; absent pinned clients unset stale wrapper settings.
- **Enforced by**: a test that builds a fixture workspace in one checkout, clones its build volume to a second checkout
  at another path, runs the same build from a host shell with `CI=true` exported and from a sandboxed job, and requires
  every unit `Fresh`. The repository's own Cargo configuration also receives a real-repository two-checkout build
  requiring every inherited unit `Fresh`, not just a fixture.

### Clones preserve mtimes; directories are never copied

Cargo's freshness is mtime-based: a unit is stale when any dependency output is newer than its own. Image clones keep
every mtime. A file-by-file copy that restamps files in walk order (`cp -c -R`) makes units rebuild, and even
`cp -c -p -R` takes minutes. Build volumes are only ever image clones or dataset clones.

- **Enforced by**: the fork test above runs on a cloned build volume and requires every unit `Fresh`.
- The pinned nightly uses checksum freshness (`[unstable] checksum-freshness = true` and
  `[build] fingerprint = "content"`) consistently in host shells, jobs and every checkout. Content identity survives
  source checkout timestamp changes; mtimes are still preserved for other tools. Never mix modes in one build volume:
  switching modes rebuilds every unit once.

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
  root. A refusal names the target, output, and build-state path. Job admission and `cowshed doctor` never construct an
  Nx graph or execute repository plugins for this rule. Sandboxed lint resolves the installed `cowshed` through the
  job's private `tools/bin` (15_capabilities.md, "Bootstrap executables"); its executable-only grant admits no
  controller data or socket.
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
own copy without borrowing its donor's tree. Genuine sibling links retain their distinct host-local authority.

### The adoption needs the target's Nx database closed

A process that opened the target's task database before a swap keeps that file open but resolves cache paths through the
links afterwards, so it reads rows from the old volume and files from the new one. For a hash the old database lists and
the new cache lacks, stock Nx reports a hit and restores nothing: the task "succeeds" with its outputs missing. A run
that started on the old tree and stays entirely on the old volume is correct; only the mix is wrong. The land therefore
swaps only when no process holds the target's current task database open, and otherwise skips the swap (Land steps 5 and
6). The daemon is stopped before the swap because it also keeps the database open (it records task history), and the
carry commits while the database is closed. The landing workspace is quiesced first (Land step 4) for the same reason on
its side.

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
bun, uv, sccache through its daemon) are one shared copy per host, written by whichever checkout fetches or compiles
first. Every sandbox that uses the tool must be able to write them; a cache a sandbox can only read fails the tool (Go's
build cache, unwritable from the sandbox, fails every sandboxed Go build with `operation not permitted`).

- **Enforced by**: a sandboxed `go build`, `cargo fetch` and `bun install` in the capability test suite.

## Process lifetime across a swap

Nothing pins a path. A process resolves paths through the links when it opens them, inside the sandbox grant of the job
it belongs to. Each job's sandbox grants exactly the build volume the controller resolved when the job was admitted
(04_sandbox.md); the supervisor is never relaunched for a swap:

- A job admitted after a swap, and the Nx daemon the supervisor restarts after it, use the new build volume entirely.
  The keeper resolves the current pointer through the same controller-owned layout as admission on every restart, even
  when no user job has been admitted since the swap. Repository links alone never authorize a mount.
- A job admitted before the swap keeps its grant and a shared build-volume hold, so the target's previous volume stays
  attached until the last such job exits. Its hold prevents even a forced collection; GC releases the volume once that
  hold is gone. A host process with no job hold does not own the unlinked volume (Garbage collection).
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

## Open questions

- **rust-analyzer and editors with `target` as a symlink.** Not yet verified.
- **Daemon keeper probing.** 04_sandbox.md's workspace supervisor probes the sandboxed daemon every 5 s; an event-driven
  alternative (the daemon's exit) is preferred if Nx exposes one.
- **ZFS mapping.** Dataset layout and `zfs promote` for adoption are specified with the ZFS substrate
  (09_substrates.md).
- **Fragmentation measurement** (Why two volumes, item 1).
