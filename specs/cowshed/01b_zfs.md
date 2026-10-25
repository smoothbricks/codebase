# ZFS Storage

On a host whose cowshed project selects the ZFS substrate, cowshed keeps every workspace and build volume as a dataset
in one pool, under one root dataset, and derives all runtime state from `zfs list` and the kernel mount table. Nothing
cowshed creates there attaches a disk image, and on macOS nothing below the root publishes an IOKit disk.

Repository identity, logical layout, shared metadata and retention belong to [01_storage.md](./01_storage.md). The
substrate contract, the dataset mapping, the clone-origin rule and the restore transaction belong to
[09_substrates.md §ZFS substrate](./09_substrates.md#zfs-substrate); build volumes, seeds, land adoption and collection
belong to [16_build_volumes.md](./16_build_volumes.md). This companion owns the ZFS realization: the pool and root, the
datasets each workspace has, what land swaps and what `rm` destroys, and the macOS dataset properties. Its APFS
counterpart is [01a_apfs.md](./01a_apfs.md).

## One pool, one root

- **The pool is selected, never guessed.** On Linux, `statfs` of an existing ZFS project selects its containing dataset
  when the pool has a delegated cowshed root. Any other checkout, and every macOS checkout, names the pool with
  `.cowshed.toml` `[substrate] kind = "zfs"` and `pool = "<pool>"` (09_substrates.md, selection); cowshed never scans
  pools or picks one. One pool carries cowshed on a host. The operator creates it and chooses its features against the
  installed OpenZFS; cowshed never enables or upgrades a pool feature, since a feature upgrade is irreversible.
- **The pool's storage is not on the Data volume.** A file vdev on Data would have every rewritten block pinned by
  Data's hourly local snapshots, for the same reason the APFS store is a dedicated volume
  ([01a_apfs.md §Dedicated store volume](./01a_apfs.md#dedicated-store-volume), "Why a volume and not paths on Data").
  The pool lives on its own disk or partition, or on a file vdev on a volume that takes no local snapshots.
- **One root dataset, `<pool>/cowshed`.** `cowshed setup` creates it with the macOS properties of
  [No disk per dataset](#no-disk-per-dataset) set locally, before any dataset exists beneath it, and attests them on
  every run. Every other cowshed dataset sits below it and inherits them:

```
<pool>                               the pool's root dataset
└── cowshed                          com.apple.devdisk=off  com.apple.browse=off  com.apple.mimic=off
    │                                com.apple.ignoreowner=off                    (all local)
    ├── store                        mountpoint=/private/cowshed/store  com.apple.ignoreowner=on (local)
    └── projects                     mountpoint=none
        └── <owner>/<repo>           (inherit mountpoint=none: never mounted)
            ├── main                 mountpoint=<checkout path>
            ├── ws                   canmount=off  mountpoint=<mount-root>/<owner>/<repo>
            │   ├── <name>           clone of <src>@cowshed:fork-<name>, mounted at <mount-root>/<owner>/<repo>/<name>
            │   └── .trash           canmount=off  mountpoint=none
            └── build                canmount=off  mountpoint=<mount-root>/.build/<owner>/<repo>
                ├── <id>             one build volume, mounted at <mount-root>/.build/<owner>/<repo>/<id>;
                │                    its seeds are its snapshots @seed-<n>
                └── .trash           canmount=off  mountpoint=none
```

The two children of `<pool>/cowshed` and their roles are 09_substrates.md's fixed hierarchy. A project's build volumes
are datasets under its own container, `…/projects/<owner>/<repo>/build/<id>`, so creation, helper containment,
discovery, send/receive and collection see one tree per project. `canmount=off` makes `ws` and `build` property
containers: they pass their `mountpoint` down by inheritance and are never mounted themselves, so every workspace and
build dataset mounts at its own path and never inside another dataset's mount. `canmount` is not inherited, so the
datasets below them mount normally. Retirement does not rely on that inheritance: it unmounts a dataset, sets
`canmount=off` on it locally and only then renames it under a `.trash` container (`mountpoint=none`). `canmount=off`
holds whatever `mountpoint` the dataset carries, so main, whose `mountpoint` is set locally to its checkout path and
survives a rename, cannot remount there; `rm main --restore` retires main this way, into `ws/.trash`, before it swaps
the retained tree back (02_workspaces.md, "Checkout layouts"). Collection sets `canmount=off` on any dataset it finds
under a `.trash` without it before it decides anything else about that dataset.

## The datasets of a workspace

A workspace is a **worktree dataset** (the source volume of 16_build_volumes.md: the git tree, `.git`, installed
dependencies, declared task outputs) and, when its tree has build state, a **build dataset** (its build volume: Cargo
target directories, the Nx cache and task database, declared build state). A checkout whose tree can name no build state
(no tracked Cargo or package manifest, no declared `[build] state`, no enabled Nx detector match) has no build dataset
until its first touch needs one (16_build_volumes.md, "Refresh"). A fork clones both from snapshots, so a new workspace
starts with its source's files and its target's warm build state.

- **Worktree.** `cowshed fork [<src>] <ws>` takes `<src>@cowshed:fork-<ws>` and clones it to `ws/<ws>`; the source
  defaults to main. Each workspace has one origin snapshot, destroyed with it (09_substrates.md, "The clone-origin
  dependency"). A ZFS snapshot is transactionally atomic, so there is no flush, descriptor clone or `fsck` before the
  clone mounts.
- **Build.** The fork clones its target's latest seed, `zfs clone <target-build>@seed-<n> build/<id>`. A block-level
  clone carries every file's bytes and modification time, so every Cargo unit the seed held is Fresh in the fork and
  every Nx task the seed recorded is a cache hit.
- **Seed.** A seed is a snapshot, `build/<id>@seed-<n>`, of the build dataset it freezes (the target's live dataset at a
  reseed, the landing workspace's at Land step 5.6), taken under the writer exclusion of
  [16_build_volumes.md §Targets and seeds](./16_build_volumes.md#targets-and-seeds): the build hold, the Nx task
  database closed and every existing Cargo profile lock held. Only the clone primitive differs from APFS. A seed is
  identified by its snapshot's `guid`, which `zfs promote` and `zfs rename` keep while they change its name, so a seed
  stays the same seed when its snapshot moves to another dataset; its full name is read from the `guid` at use.
- **Behind.** `written@<snapshot>` counts the bytes a dataset wrote since a snapshot of itself or of its origin chain,
  and means nothing for a snapshot on another branch. Freshness is therefore measured against a **freshness mark** in
  the live dataset's own history, and a mark stands only for what 16_build_volumes.md compares:
  - the seed itself when it lies there: a reseed's snapshot, a land's seed once the swap's promote makes the landing
    dataset the target's, or a retained seed the adopted dataset descends from. `written@<seed>` is then exactly "the
    live dataset holds a write the seed does not";
  - after a capture at Land step 5.6 before a skipped swap, a snapshot the land takes of the target's live dataset at
    the skip, `build/<live-id>@cowshed:seed-mark-<n>`, recorded by `guid` beside the seed and destroyed when the seed is
    superseded. 16 has forks start from that new seed until the target's own volume is written again, and
    `written@<mark>` measures exactly that.

  A seed with neither is **not comparable**: a skipped capture before a successful swap retains a seed that the adopted
  dataset neither holds nor descends from, and no later snapshot can stand for the seed's contents. It is recorded as
  behind and stays behind until a reseed replaces it, the verdict APFS reaches from the adopted image's later
  modification time. Otherwise a seed is behind when `written@<mark>` is nonzero, one property read in place of the
  image modification-time comparison. Either way the reseed still applies 16's built-at tree ordering (it does not
  refreeze from a volume farther from the fork's tree).

- **Capacity.** A worktree dataset's cap is its `refquota` (09_substrates.md); a build dataset's is `refquota` set to
  `[build] capacity` (16_build_volumes.md, "Capacity"). `refquota` is not inherited, so every clone is created with
  `-o refquota=<its source's>`, which keeps 16's rule that a clone inherits its source's capacity.
- **Build-dataset properties**, set locally on `…/<repo>/build` and inherited by every build dataset:

  | Property      | Value                         | Why                                                                                                 |
  | ------------- | ----------------------------- | --------------------------------------------------------------------------------------------------- |
  | `compression` | `zstd-1` (`lz4` if CPU-bound) | Build output compresses 2.5–4.4× per file (below)                                                   |
  | `recordsize`  | `128K`                        | Nx's SQLite pages; a 1M child dataset for Cargo's target tree is adopted only on measurement, below |
  | `sync`        | `disabled`                    | Build state is derived; ZFS stays consistent at transaction-group boundaries                        |
  | `atime`       | `off`                         | —                                                                                                   |
  | `xattr`       | `sa`                          | —                                                                                                   |
  | `dnodesize`   | `auto`                        | Tens of thousands of `.o` and incremental files per target                                          |
  | `dedup`       | `off`                         | Forks already share by clone; `zdb -S <pool>` simulates dedup before it is ever considered          |

  Measured 2026-10-06 on a large Rust workspace's build volume, whole files under `zstd-1` compress 4.43× (`.rlib`),
  2.85× (`.rmeta`), 3.70× (`.o`), 3.53× (executables over 5 MB) and 2.46× (`incremental/`); ZFS compresses per record,
  so expect somewhat less.

## Land swaps the build dataset; `rm` destroys what was not adopted

The lifecycle is the APFS one (02_workspaces.md, "`cowshed land`"; 16_build_volumes.md, "Land"): a workspace builds, its
head fast-forwards the target's branch, the target adopts the workspace's build volume in place of its own, and the
target's old build volume is removed. Only the primitives change.

- **`cowshed land`.** The target's branch fast-forwards to the landed head (Land step 3). The target then adopts the
  workspace's build dataset as its build: the target's `.cowshed/build` link moves onto it (Land step 6) and
  `zfs promote build/<landing-id>` makes it the origin of its lineage. Before the link moves, the land records a pending
  promote in the landing build volume's sidecar, naming the target and the `guid` of the dataset's current origin
  snapshot, and it clears the record once the promote returns. The target's old build dataset is retired and destroyed
  ([Retiring a build dataset](#retiring-a-build-dataset)), and the workspace's worktree dataset is retired as `rm`
  retires it (Land step 8). After a land the workspace's build dataset is the target's, and nothing else of the
  workspace remains.
- **When land adopts nothing**, the workspace keeps its build dataset: adoption skipped by a holder (Land steps 5 and
  6), or a target whose checkout holds another branch, where the land compares-and-swaps the ref and skips steps 4 to 7.
  It goes with the workspace, except as the holder of a seed: a capture at Land step 5.6 before a skipped swap publishes
  the target's new latest seed on that dataset, and the dataset stays for that seed (Retiring, step 2). Likewise a
  skipped capture before a successful swap leaves the target's latest seed on the target's old build dataset.
- **`land --no-retire`** keeps the workspace and its worktree dataset. When the target adopted its build dataset, the
  workspace takes a fresh clone of the target's latest seed as its own (16_build_volumes.md, Land step 5 and "One link
  per checkout"); when adoption was skipped, it keeps its own build dataset.
- **`cowshed rm <ws>`** destroys only what no target adopted. It retires the worktree dataset by `zfs rename` into
  `ws/.trash`, then destroys the clone **and then** its origin snapshot, one idempotent logical transaction that
  `cowshed gc` completes when interrupted (09_substrates.md). A workspace with live forks refuses, naming them. Its
  build dataset, when it still owns one, is released with it (16_build_volumes.md, "Garbage collection") and retired
  below; a build dataset a target adopted is the target's and `rm` never touches it.
- **Promote is for build datasets only.** Worktree datasets never promote; their graph stays the shallow star of
  09_substrates.md.
- **A superseded seed is destroyed deferred**, `zfs destroy -d build/<id>@seed-<n>`: a seed some live fork cloned cannot
  be destroyed, and the deferred destroy removes it when its last clone goes. A target keeps only its latest seed
  (16_build_volumes.md) without a collection scan for pinned seeds.
- Every release is journaled in `deletion-log.jsonl` as 16_build_volumes.md specifies.

### Retiring a build dataset

`zfs promote` moves the origin snapshot and every older snapshot of the old dataset onto the adopted one; snapshots
taken after the origin stay where they are. The old dataset becomes a clone of the adopted one, and is destroyable at
once unless one of its remaining snapshots, a seed frozen after the landed workspace forked, is the origin of another
live fork. A dataset may also hold the target's latest seed. Retirement therefore follows the pins rather than assuming
none:

1. **Release** as 16_build_volumes.md, "Garbage collection", specifies: claim the build hold exclusively, so a running
   job's shared hold keeps the dataset until the job ends, then request an unforced unmount with the bounded grace
   before forcing, then `zfs rename` it into `build/.trash`, where it is unmounted and linked by nothing.
2. **Destroy** its snapshots with `zfs destroy -d` and then the dataset itself, except a target's latest seed. That
   snapshot is the target's, whatever dataset holds it and whatever its clones or job holds: it is never destroyed or
   marked for deferred destruction until the target supersedes it or retires (16_build_volumes.md, "Targets and seeds"),
   and its dataset stays in `build/.trash` until then. cowshed never passes `-R`, so a dataset whose snapshot still has
   a clone is refused by ZFS rather than taking the fork with it. It stays in `build/.trash`, and every collection pass
   tries again; it is destroyed on the pass after its last dependent fork lands (whose promote takes the pinned snapshot
   over) or is removed (whose deferred snapshot then goes). `cowshed doctor` names each pinned dataset in `build/.trash`
   with the forks that pin it and the bytes it holds.
3. **Recover** forward from the pending-promote record, never from the clone graph: a live fork's origin can lie on a
   dataset in `build/.trash` too, so an origin there does not identify an interrupted adoption. Before deciding any
   unlinked build dataset, collection reads the build volumes whose sidecars record a pending promote. Where the named
   target's link names the dataset, the recorded origin decides, because `zfs promote` is not idempotent: if the
   dataset's `origin` is still the snapshot with that `guid`, collection promotes it and clears the record; if that
   snapshot now belongs to the dataset itself, the promote already completed and the record is cleared without another;
   any other state is drift, which collection refuses to touch and reports with the dataset, its origin and the record.
   Otherwise (the land stopped before the link moved, or skipped the swap) the record is cleared without a promote. The
   landing workspace's own link, which names the same dataset until the swap, never authorizes it. A crash after the
   promote leaves an unlinked dataset that the next collection releases and destroys as above.

**Destroy does not stall the pool.** ZFS frees a destroyed dataset's blocks in the background over later transaction
groups (`async_destroy`), so a destroy returns without walking the dataset's blocks, and no background deletion or
chunked free (05_gateway.md, "Deleting released images"; 01a_apfs.md, "Freeing an image") is needed. The pool's
`freeing` property reports what is still being freed.

## No disk per dataset

On macOS, OpenZFS mounts a dataset either through an IOKit proxy disk it creates for the mount, or directly by dataset
name with no device at all. cowshed takes the second path for every dataset below `<pool>/cowshed`, main's included, so
creating, cloning, mounting and destroying a workspace publish no IOKit device, no `IOMedia` and no disk node. Only the
pool itself appears as a disk.

The properties are those of the installed OpenZFS for macOS (2.4.3-rc5: `/usr/local/zfs/include/libzfs/sys/fs/zfs.h`
lines 208–212 declare them, lines 702–712 their `mimic` and `devdisk` values; `zfs set` lists their accepted values).
All four are inheritable filesystem properties:

| Property                | At `<pool>/cowshed` | Default    | What it does                                                                                                                                                                                                                              |
| ----------------------- | ------------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `com.apple.devdisk`     | `off`               | `poolonly` | Read when the dataset mounts. `on`, or `poolonly` on the pool's own root dataset, asks the kernel for a proxy disk (`ZFS_IOC_PROXY_DATASET`) and mounts `/dev/diskN`; otherwise the mount source is the dataset name and no device exists |
| `com.apple.browse`      | `off`               | `on`       | `off` sets `MNT_DONTBROWSE` on the mount, the `nobrowse` of 01a_apfs.md; applied to a mounted dataset as soon as it changes                                                                                                               |
| `com.apple.mimic`       | `off`               | `off`      | The filesystem type a mount reports in `statfs`: `zfs`, or `hfs`/`apfs` when mimicking                                                                                                                                                    |
| `com.apple.ignoreowner` | `off`               | `off`      | `on` ignores ownership like `noowners`. Workspaces keep ownership, as APFS workspaces mount `owners`; the store dataset sets `on`, as the APFS store mounts `noowners` (01a_apfs.md, "`noowners` is deliberate")                          |

The behaviour of `devdisk`, `browse` and `mimic` is read from the macOS port's source: `do_mount` in
`lib/libzfs/os/macos/libzfs_mount_os.c`, and the property callbacks in `module/os/macos/zfs/zfs_vfsops.c`.

### Why no dataset gets a disk

An APFS workspace is an attached disk image, and every attach spends host resources that the APFS findings show run out
or leak:

- **The IOMedia match-notification ceiling.** Each attached image adds about three `IOMedia` (whole disk, container,
  volume). An attach waits on one `IOServiceMatched` notification over every `IOMedia` on the host, and XNU's
  `IOServiceUserNotification::kMaxOutstanding` drops matches past 1,024 without an error: past that many media an attach
  can miss its own device and wait forever (01a_apfs.md, "The IOMedia match-notification ceiling").
- **`diskimagesiod` retention and error 150.** Each attach runs a `diskimagesiod` that maps 72 MiB into the kernel.
  Helpers that outlive their image keep their mapping, image devices leak past their detach, and the attach that finds
  no room fails with error 150 (`kIOReturnNoResources`) until orphans are killed or the host reboots (01a_apfs.md, "How
  the APFS host degrades", item 2).
- **Retained media references.** mds keeps one IOKit send right for every mounted APFS volume it discovers, until the
  mds process ends (01a_apfs.md, "Spotlight media references").

A dataset mounted with `com.apple.devdisk=off` creates none of these: there is no image, no `diskimagesiod`, no
`storagekitd` call, no `IOMedia` and no `AppleAPFSVolume`. Fork, land and `rm` are ZFS operations on the pool and a
mount of a dataset by name, so they add nothing to any of those budgets, however many workspaces a host carries, and the
attach media guard of 01a_apfs.md has nothing to count for them. The pool contributes a constant that does not grow with
workspaces: its vdev's own media, and the proxy disk of the pool's root dataset when that dataset is mounted.

`poolonly` alone already gives a child dataset no disk, but it is inherited: a pool whose root says `on` would give one
to every workspace. Setting `off` locally on `<pool>/cowshed` makes the tree's value independent of the pool's, and
`com.apple.ignoreowner=off` is set locally for the same reason. Main is no exception: its worktree and build datasets
inherit `off` like every other, since main gains nothing from a disk.

`com.apple.mimic=off` keeps every dataset reporting `zfs`. A dataset reporting `apfs` would claim a type it is not, and
send it down mds's APFS branch, which asks `APFSVolumeRole` about a mount-source device the dataset does not have
(01a_apfs.md, "Spotlight media references").

### Inherited, never set per dataset

The values live on `<pool>/cowshed`, plus `com.apple.ignoreowner=on` on `store`. A clone takes inheritable properties
from its parent in the namespace, not from its origin, so every fork, seed clone and restore clone inherits them with no
step of its own, as does a received dataset whose stream carries no properties. The one other local override is
`com.apple.browse=on`, set on a workspace created with `--browse`. `devdisk`, `mimic` and `ignoreowner` are never set
below the root on a workspace or build dataset.

`cowshed setup` and `cowshed doctor` attest the tree with one read, restricted to filesystems because the properties
exist on no snapshot or bookmark:
`zfs get -H -p -r -t filesystem -o name,property,value,source com.apple.devdisk,com.apple.browse,com.apple.mimic,com.apple.ignoreowner <pool>/cowshed`.
The root holds the four values with source `local`, `store` holds `ignoreowner` `on` with source `local`, and every
other dataset holds them `inherited from <pool>/cowshed` (`browse` may be `local` `on` on a `--browse` workspace). Any
other value or source, including `received`, is a finding naming the dataset, property, value and source. `devdisk`
takes effect when a dataset mounts, so the mount table is attested too: every mounted cowshed dataset's mount source
(`f_mntfromname`) is its dataset name. A `/dev/disk…` source is a dataset mounted with a proxy disk, reported by name
with the remount that drops it. The macOS real-ZFS test lane counts the host's registered `IOMedia` before and after
adopt, fork, land and `rm`, and requires the counts equal.

## Mount root and Spotlight

- **Mount root.** Workspace and build datasets mount under the fixed per-user mount root,
  `/private/var/folders/cowshed/<uid>/mnt` on macOS and `~/.cowshed/mnt` on Linux (01_storage.md, "Directory layout";
  [01a_apfs.md §Stable workspace mount root](./01a_apfs.md#stable-workspace-mount-root)), through the `mountpoint` the
  `ws` and `build` containers pass down: `ws/<name>` at `<mount-root>/<owner>/<repo>/<name>`, `build/<id>` at
  `<mount-root>/.build/<owner>/<repo>/<id>`. Main mounts at its original checkout path (02_workspaces.md, "Checkout
  layouts"); the store at `/private/cowshed/store`.
- **Spotlight.** The same three measures as APFS, none of them a disk property. The canonical mount root contains
  `/var/folders/`, which mds's volume factory rejects before it constructs a record, whatever the filesystem type
  (01a_apfs.md, "Spotlight media references"). The root storage service merges the store and the per-user mount roots
  into the Data volume's Spotlight Search Privacy `Exclusions` at setup and boot, preserving other entries (01a_apfs.md,
  "Images", Spotlight). Adopt plants `.metadata_never_index` at main's worktree root, and every snapshot clone carries
  it. These express indexing policy. That mds takes no media reference for a dataset follows from the mechanism, not a
  measurement: its leaking lookup acquires the IOKit service of an APFS volume's media, and a dataset with `devdisk=off`
  and `mimic=off` has neither media nor the `apfs` type. `cowshed doctor`'s `departed-volume-references` remains the
  instrument.
- **Finder.** `com.apple.browse=off` keeps every dataset out of Finder, the Desktop and the sidebar, as `nobrowse` does
  for APFS, and nothing mounts under `/Volumes`.

## Open questions

- **macOS privilege boundary.** 09_substrates.md's `cowshed-helper` is the Linux root binary through which ZFS mutations
  go, `promote` among its verbs. Which of `zfs create|clone|snapshot|destroy|rename|promote|mount` an unprivileged user
  can run on macOS under `zfs allow`, and therefore whether the same fixed-verb helper is required there, is unmeasured.
- **Build recordsize.** One build dataset at `128K` against a `1M` child dataset for Cargo's target tree (which makes
  seed snapshots and fork clones recursive): cold-build wall time, CPU and allocated bytes, three interleaved rounds
  with a null arm, decide it.
