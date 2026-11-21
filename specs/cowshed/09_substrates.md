# Substrates

cowshed's workspace physics — cheap CoW clone, instant delete, own inode namespace — is provided by a substrate.
Everything above the substrate (grants, sandbox model, gateway, CLI contract, marker files, cache taxonomy) is
substrate-independent. There are two substrates: **APFS images** (macOS, 01a_apfs.md/02_workspaces.md are written
against it) and **ZFS datasets** (Linux; macOS only where OpenZFS is already installed).

## Public lifecycle and substrate API

Lifecycle is asynchronous because image tools, sync, fsck, mount, and helper RPCs can block. Pure synchronous planning
is deliberately separate from execution: planning validates identities, names, policy, topology, and intended state
transitions without touching the platform. The common public contracts are `LifecyclePlanner` and `Substrate`:

```rust
pub trait LifecyclePlanner: Send + Sync {
    fn plan_adopt(&self, request: AdoptRequest) -> Result<AdoptPlan, PlanError>;
    fn plan_create(
        &self,
        from: &LifecycleWorkspace,
        destination: Destination,
    ) -> Result<CreatePlan, PlanError>;
    fn plan_fork(
        &self,
        from: &LifecycleWorkspace,
        destination: Destination,
    ) -> Result<ForkPlan, PlanError>;
    fn plan_retire(&self, workspace: &LifecycleWorkspace) -> Result<RetirePlan, PlanError>;
}

#[async_trait]
pub trait Substrate: LifecyclePlanner {
    type Error: Send;

    async fn execute_retire(&self, plan: RetirePlan) -> Result<RetiredRef, Self::Error>;
    async fn reclaim(&self, retired: RetiredRef) -> Result<(), Self::Error>;
    async fn list(&self, repo: &RepoId) -> Result<Vec<DerivedWorkspace>, Self::Error>;
    async fn mount_state(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<MountState, Self::Error>;
    async fn ensure_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        intent: MountIntent,
    ) -> Result<PathBuf, Self::Error>;
    async fn unmount(&self, workspace: &LifecycleWorkspace) -> Result<(), Self::Error>;
    async fn stats(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<SubstrateStats, Self::Error>;
    async fn preview_gc(&self, repo: &RepoId) -> Result<StorageGcPlan, Self::Error>;
    async fn execute_gc(&self, plan: StorageGcPlan) -> Result<StorageGcReport, Self::Error>;
}
```

`preview_gc` collects immutable, exact candidates and `execute_gc` revalidates the plan before mutation. An execution
failure local to one candidate is returned in `StorageGcReport.deferred: Vec<StorageGcDeferred { path, diagnostic }>`;
later candidates still run. Only a stale plan or unsafe ownership/integrity proof refuses the sweep.

The locked revalidation's trash observation also supplies the remaining retired incarnations of each workspace name.
APFS and directory execution share that selection owner, not their payload effects. Each candidate still checks its
exact retirement record and current canonical workspace before cleanup. After a cleanup attempt, including one that
failed after unlinking, only that entry's payload and grants record are inspected: a surviving entry keeps its name,
while an absent pair removes its incarnation from the operation-local observation. A failed inspection retains the
observed owner and reports its cause alongside any cleanup failure. No candidate re-enumerates the whole trash; the
observation and these exact probes cost O(T + K) namespace observations for T trash entries and K candidates, excluding
the separately required work on each candidate's artifacts. Standalone retirement observes trash once for its own call.
Nothing persists this selection, carries it across lock admission or changes recovery-image retention.

A candidate's identity is a digest of what `stat` reports, plus the content of the small records beside an image. Each
file it names contributes its path, length, allocated bytes and modification time. A record (grants sidecar, CA key)
also contributes its content; every other file, above all an image, contributes its device and inode and is never
opened. An image is a sparse file of up to hundreds of gigabytes, `doctor` previews every retired workspace's images,
and `execute_gc` repeats the preview under the locks, so hashing contents would read the whole store twice for a fence
the inode already gives: a replacement differs by inode, a write by modification time, a resize by length.

Adopt, create, and fork are intentionally absent from `Substrate`'s execution methods. The APFS controller boundary is
staged so that controller-owned state can be initialized or fenced while the lifecycle lock remains held.
`ApfsSubstrate` exposes these methods:

```rust
impl<H, L> ApfsSubstrate<H, L>
where
    H: ApfsExecutionHost,
    L: ApfsBlockingLane,
{
    pub async fn execute_adopt_staged<W, D, DFut, F, Fut, E>(
        &self,
        plan: AdoptPlan,
        alongside: Alongside<W, D>,
        initialize: F,
    ) -> Result<(LifecycleReceipt, W::Output), AdoptExecutionError<E>>
    where
        W: Future + Send,
        W::Output: Send,
        D: FnOnce(W::Output) -> DFut + Send,
        DFut: Future<Output = ()> + Send,
        F: FnOnce(AdoptStage) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send;

    pub async fn execute_create_staged<W, D, DFut, F, Fut, E>(
        &self,
        plan: CreatePlan,
        alongside: Alongside<W, D>,
        initialize: F,
    ) -> Result<LifecycleReceipt, CreateExecutionError<E>>
    where
        W: Future + Send,
        W::Output: Send,
        D: FnOnce(W::Output) -> DFut + Send,
        DFut: Future<Output = ()> + Send,
        F: FnOnce(CreateStage, W::Output) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send + std::fmt::Display;

    pub async fn execute_fork_staged<W, D, DFut, F, Fut, E>(
        &self,
        plan: ForkPlan,
        alongside: Alongside<W, D>,
        initialize: F,
    ) -> Result<LifecycleReceipt, ForkExecutionError<E>>
    where
        W: Future + Send,
        W::Output: Send,
        D: FnOnce(W::Output) -> DFut + Send,
        DFut: Future<Output = ()> + Send,
        F: FnOnce(ForkStage, W::Output) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send + std::fmt::Display;
}
```

The executor contract is:

- **Lock, recover, reread, revalidate.** Execution acquires every affected lifecycle lock, runs pending-operation
  recovery, rereads canonical storage and kernel facts, and revalidates the immutable plan before the first new
  mutation. A stale plan is a typed conflict. Platform subprocesses and blocking filesystem work run only through
  `ApfsBlockingLane`; callbacks are async and do not block a runtime worker.
- **Adopt/create/fork initialization precedes publication.** The executor prepares a mounted `WorkspaceStage` in the
  controller-private staging namespace, then awaits `initialize`, and publishes the canonical image and metadata only
  after the callback succeeds. A callback error aborts the prepared stage and is returned as `Initializer` or, if abort
  also fails, `InitializerCleanup`.
- **Side work runs beside staging and is owned until it is answered.** `Alongside { work, abandon }` is a second image
  the operation attaches anyway (a fork's build volume, main's first one), started once the locks are held and the plan
  revalidated and run while the stage is prepared, so the two attaches wait in `storagekitd` once. A create or fork
  hands its output to `initialize` with the stage; an adopt answers it with the receipt once main is committed. A
  staging, initializer or commit failure hands it to `abandon` instead. An operation dropped before either (a cancelled
  request) drops the work and its output with it and runs nothing for them after, so the output is the resource's owner:
  dropped, it releases what it holds, and work dropped mid-flight releases what it makes once nobody is there to take
  it. A crash leaves the resource to the replayed operation's intent and to collection.
- **Cancellation follows the same transaction boundaries as errors.** Dropping a future while an adopt/create/fork
  initialization callback is pending synchronously detaches its private attachment and reclaims any cloned staging image
  before the lifecycle lock is released. Recovery and `gc` also finish any idempotent cleanup left by an interrupted
  abort.

Plans remain pure, immutable, and capability-free: they contain validated logical names, expected incarnation/revision,
and intended operations, but no open descriptors, caller-supplied mounted paths, or executed side effects. CLI commands
may synchronously wait at the outermost process boundary; library consumers await futures.

Selection is convention, not guesswork, and the host storage anchor is distinct from the selected project path. On
macOS, `statfs` of the invoking user's home directory selects the APFS container that owns `cowshed.store`; cowshed
never derives that container from the project root because an adopted root is itself mounted from a workspace image, and
an unadopted source may live on another filesystem. The project path is still validated lexically and supplies
repository identity/copy input, not host-volume authority.

On Linux a host has one store, and a store that is published decides the substrate by what answers at
`/private/cowshed/store`: a dataset named `<parent>/cowshed/store` mounted there selects ZFS with that root, and a
directory there carrying a directory-store marker selects the directory store. Only a host with no store yet selects
from an anchor: the project root for an opening, the home for `cowshed setup`. An explicit `.cowshed.toml`
`[substrate] kind = "zfs"` and `parent = "<parent>"` names the root's parent, the pool or any dataset in it, and wins;
an opening reads its project's file, and setup reads the nearest one at or above the directory it runs in. Without a
declaration, an anchor on ZFS selects the nearest of its dataset and that dataset's ancestors whose `cowshed` child
carries the caller's `cowshed:owner` set locally, read by exact name; every other anchor selects the directory store.
cowshed never scans pools or silently picks one. A declaration that names another parent than the published store's, or
any declaration on a host whose store is a directory store, is refused naming both. The root `<parent>/cowshed` exists
before setup runs: whoever administers `<parent>` creates it and sets its owner ("Privileged helper"), and setup refuses
a root that is absent with that command. `cowshed doctor` prints the selected substrate, root, and evidence.

## APFS image substrate (reference)

As specified in 01a_apfs.md and 02_workspaces.md: one RAW image (`.dmg`) per workspace, a sparse file holding a
case-sensitive APFS volume. Formatted images attach exactly once with
`hdiutil attach -kernel -nomount -noverify -nobrowse -imagekey diskimage-class=CRawDiskImage -plist <image>`, then
pinned `fsck_apfs -q` verifies the freshly probed owned APFS volume before `mount_apfs` mounts it. Re-adoption's
retained inputs are RAW and attach the same way with `-readonly`; any other format is refused by name (01a_apfs.md,
"Mounts"). The registry-proven raw pin remains held across the host lease wait and mount command, and cowshed checks the
held mount-root/source identity before publication (01a_apfs.md, "Mounts"). There is no combined mountpoint attach,
fallback or stale-family selection. Release unmounts each exact currently-owned mounted volume with `funmount` on its
held, proven root descriptor, then guarded `hdiutil detach` of the exact physical whole device, retaining the existing
grace/force/refusal contract (01a_apfs.md, "Release and device custody"). Detached resize and blank-image formatting
intentionally attach without mounting. Deletion renames one file into a `.deleting` directory, and a background deletion
frees it later (05_gateway.md, "Deleting released images"). Clones are logically independent of their source (no GC
coupling), but inherit its extent map, so a fragmented main makes every clone's first write slow until
`cowshed defrag main` (01a_apfs.md, "Clone cost follows extents, not size"), and they share its blocks until every
sharer is released (01a_apfs.md, "Space reclamation"). Timing budgets are in 08_testing.md ("Performance budgets").

Whole RAW backing images can share host APFS blocks through `clonefile`; files inside two separately mounted RAW images
cannot. Each image contains its own APFS volume, and `clonefile` does not cross those volume boundaries; a fork clones
the backing image whole, from a live capture that never seals, unmounts or detaches its source (01a_apfs.md, "Images").
A same-host store is not evidence that its guest files can be reflinked across mounts. The mbx store is the one
plain-directory tree in the APFS design: it and mbx's managed targets are directories on the build-cache volume
(01a_apfs.md, "Dedicated build-cache volume"), so `clonefile` between them works exactly as it does between any two
files on one volume, and no image is involved. A checkout with the mbx capability reaches them through the `target` link
mbx creates (16_build_volumes.md); its build volume stays an image here like every other.

## ZFS substrate

### Mapping

`<pool>` in the names below, and in 01b_zfs.md, stands for the root's parent `<parent>`: the pool itself, or any dataset
in it that `.cowshed.toml` names.

| cowshed concept                   | ZFS object                                                                                                                                                                                                                                                                             |
| --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| project                           | `<pool>/cowshed/projects/<owner>/<repo>` (primary `repo_id`; component-safe container dataset)                                                                                                                                                                                         |
| main workspace                    | `<pool>/cowshed/projects/<owner>/<repo>/main`, `mountpoint=` original checkout path                                                                                                                                                                                                    |
| session workspace                 | `<pool>/cowshed/projects/<owner>/<repo>/ws/<name>`                                                                                                                                                                                                                                     |
| workspace mounts                  | `mountpoint` inheritance on `…/ws` → `<mount-root>/<owner>/<repo>/<name>`                                                                                                                                                                                                              |
| crash-consistent snapshot of main | `zfs snapshot main@cowshed:<name>` (transactionally atomic)                                                                                                                                                                                                                            |
| `cowshed fork [<src>] <ws>`       | `zfs snapshot src@cowshed:fork-<ws>` + clone; source defaults to main                                                                                                                                                                                                                  |
| `cowshed rm`                      | retire: `zfs rename` into `…/.trash`; reclaim: `zfs destroy` the clone **then** its origin snapshot — an idempotent logical transaction (physically two commands; `cowshed gc` completes interrupted halves)                                                                           |
| capacity cap                      | `refquota` per workspace dataset (replaces sparse-image capacity)                                                                                                                                                                                                                      |
| shared caches (layers 1 and 3)    | none — layer 1 is cowshed's user cache directory and layer 3 each tool's own default in the host HOME (03_caches.md)                                                                                                                                                                   |
| mbx store and managed targets     | `<pool>/cowshed/build-cache` dataset at `/private/cowshed/build-cache`, holding only the mbx cache dir `mbx/`; store and targets together on one filesystem, so restores are clones where the pool's block cloning serves them [INFERENCE] (01b_zfs.md, "The mbx store on a ZFS host") |
| Linux gateway attachment          | no persistent port block; per-incarnation Unix socket + private netns + trusted `127.0.0.1:7644` connector, created and destroyed with attachment                                                                                                                                      |
| compaction                        | not needed — freed blocks return to the pool                                                                                                                                                                                                                                           |

Adopt on ZFS: create `…/projects/<owner>/<repo>/main`, copy the tree once (rsync-fidelity, preserving metadata), set
`mountpoint=` the original path. The original tree moves aside as `<root>.pre-cowshed` exactly as on macOS. The
self-healing stub `.envrc` is still written beneath the mountpoint — ZFS mounts also disappear when a pool isn't
imported, and healing parity keeps its `cowshed attach` identical across substrates.

Markers, tokens, grant semantics, secrets gates, and the cache taxonomy are unchanged. Detached metadata uses the same
schema but `portBlock` is optional and omitted for ZFS/Linux: a sidecar recording `"platform": "linux"` is refused if it
carries a `portBlock` or a `retainedPortBlocks`, the gateway's session inventory reserves no block for it, and nothing
refuses it for holding none. The gateway serves an attached workspace's session on that incarnation's Unix socket,
`<store>/run/gateway/<workspaceIncarnation>.sock`, binding it when the session is installed and unlinking it when the
session is removed, which the project's reconcile does after a detach (05_gateway.md, "Placement and identity"). Each
job launch has a private loopback-only netns with the trusted connector on namespace-local `127.0.0.1:7644` relaying to
that socket (04_sandbox.md, "Linux enforcement", which also states what is not yet per attached workspace); package
tools use that HTTP address, not a Unix-socket API, and the job environment names it wherever a macOS job names its
block's base, with no synthetic base and no `COWSHED_PORT_BASE`. Layer-2 extracted caches stay **in-dataset** and arrive
via the clone, same as in-image: hardlinks cannot cross datasets, so a shared extracted cache would demote installs to
copies. OpenZFS ≥ 2.2 block cloning (`copy_file_range` → BRT) makes reflink-grade copies work _across_ datasets in one
pool; cowshed treats that as an opportunistic bonus, never load-bearing (it is version- and tunable-dependent).

### The clone-origin dependency

Unlike APFS clonefile, a ZFS clone depends on its origin snapshot: `main@cowshed:fork-raven` cannot be destroyed while
`ws/raven` exists. cowshed contains this with one rule — **one origin snapshot per workspace, destroyed together with
it**:

- `cowshed fork raven` creates `main@cowshed:fork-raven`; `cowshed rm raven` destroys `ws/raven` then the origin
  snapshot named by its `origin` property, in that order — an idempotent logical transaction (physically two commands;
  `cowshed gc` completes interrupted halves).
- `cowshed gc` prunes `cowshed:*` snapshots with no surviving clone (crash between the two destroys).
- Main is never blocked: writes to main proceed freely; a workspace's origin snapshot pins only the main blocks that
  main has since overwritten. `cowshed doctor` reports that pinned space per workspace (`zfs list -H -o written,origin`)
  so long-lived workspaces show their true cost.
- **Forks bend the star.** `cowshed fork src dst` snapshots the _source session_ (`src@cowshed:fork-<dst>`), hanging a
  spoke off a session rather than main — so `src` cannot be reclaimed while `dst` lives. The containment rule extends
  rather than breaks: a workspace's origin snapshot dies with it, and **a workspace with live dependents cannot be
  reclaimed** — `cowshed rm src` exits 4 naming the dependent forks; `cowshed doctor` reports fork dependencies and
  their pinned bytes so a coordinator lands or removes forks first.
- cowshed does not use `zfs promote` to re-parent workspace datasets. It keeps their graph shallow — main at the center,
  one snapshot spoke per workspace, fork spokes off sessions — and refuses reclaim while dependents exist. Build
  datasets are the one exception: a land promotes the build dataset its target adopts, so the target's old build dataset
  can be destroyed (01b_zfs.md, "Land swaps the build dataset"). CI uses the ordinary target/build-volume lifecycle
  (10_ci.md, "Keeping the host main warm").

### Privileged helper

Every substrate mutation goes through `cowshed-helper`, a root binary that links only libc and is auditable in one
sitting (`packages/cowshed/crates/cowshed-helper`). Delegation cannot replace it: `zfs allow` covers
create/snapshot/clone/destroy but **not `mount(2)`**, which Linux gates on `CAP_SYS_ADMIN` regardless of delegation, and
in a container whose dataset tree is zoned to its user namespace (`zfs zone`) delegation grants nothing at all: measured
2026-11-21 on OpenZFS 2.4.3, a user holding `create,snapshot,clone,destroy,mount,rename,promote,userprop` on a zoned
dataset is still refused `zfs create` and `zfs snapshot` there, while the namespace's root may do all of them. Reads
(`zfs list`, `zfs get`) need no privilege and never go through the helper.

`cowshed-helper <root> <verb> …` runs at most one `zfs` command per invocation, and exactly one for every verb but
`check`:

| Verb       | Arguments                                              | `zfs` command                 |
| ---------- | ------------------------------------------------------ | ----------------------------- |
| `create`   | `<dataset> [<property>=<value>…]`                      | `create -o … <dataset>`       |
| `clone`    | `<dataset>@<snapshot> <dataset> [<property>=<value>…]` | `clone -o … <origin> <clone>` |
| `snapshot` | `<dataset>@<snapshot>`                                 | `snapshot`                    |
| `destroy`  | `<dataset>` or `<dataset>@<snapshot> [--defer]`        | `destroy`, `destroy -d`       |
| `rename`   | `<dataset> <dataset>`                                  | `rename`                      |
| `promote`  | `<build dataset>`                                      | `promote`                     |
| `mount`    | `<dataset>`                                            | `mount`                       |
| `unmount`  | `<dataset> [--force]`                                  | `unmount`, `unmount -f`       |
| `set`      | `<dataset> <property>=<value>…`                        | `set … <dataset>`             |
| `check`    | none                                                   | none                          |

`check` changes nothing and names no dataset. It answers 0 once the helper's own preconditions hold, that it runs as
root and that `<root>` names the caller (the first refusal below), and that is all it proves: it is how an install is
proven before anything depends on it ("Install and privilege").

`rename` serves retire's trash namespace. `set` carries the mountpoint, the `canmount=off` of a retirement, a resize's
`refquota` and a `--browse` workspace's property; it replaces a `set-mountpoint` verb. No verb passes `-r`, `-R`, `-p`
or a destroy's `-f`: an invocation touches the datasets it names and no other, so a dataset whose snapshot still has a
clone is refused by ZFS rather than taking the clone with it. The helper refuses, before anything runs and with exit
status 2:

- **A root that does not name the caller.** `<root>` is `<parent>/cowshed`, where `<parent>` is the pool or any dataset
  in it, and it carries the user property `cowshed:owner=<uid>` with source `local`. Whoever administers `<parent>` sets
  it once; that property, on the dataset itself, is the whole authorization, so there is no helper configuration file.
  The caller is the real user of a setuid run and the invoking user of a `sudo` run.
- **A dataset outside the root.** Every dataset argument, a snapshot's dataset included, lies strictly below `<root>`:
  the root itself, its parents and its siblings are never an argument. A name component is ASCII letters, digits and
  `._:-`, never `.`, `..` or a leading `-`.
- **A promote of anything but a build dataset** (`<root>/projects/<owner>/<repo>/build/<id>`). cowshed calls it only for
  an adoption: at land, or when collection completes an adoption whose pending promote the build volume's sidecar
  records and the link of the target it names confirms (01b_zfs.md, "Retiring a build dataset").
- **A property outside the allow-list.** `mountpoint`, `canmount`, `refquota`, `compression`, `recordsize`, `sync`,
  `atime`, `xattr`, `dnodesize`, `dedup`, the four `com.apple.*` properties of 01b_zfs.md, and user properties
  `cowshed:<name>` other than `cowshed:owner`. `zoned`, `sharenfs`, `overlay`, `setuid`, `devices`, `exec` and the
  encryption properties are never cowshed's to change.
- **A mountpoint outside the caller's tree.** A mountpoint is `none`, one of the fixed host paths below ("Linux host
  paths"), or an absolute path of normal components whose every existing component is not a symlink and whose deepest
  existing component belongs to the caller. `legacy` is refused.

A new dataset's root directory belongs to root, so `create` hands it to the caller: it opens the mountpoint once and
changes its owner only when the kernel says that descriptor is the mounted root of the dataset it just created
(`/proc/self/mountinfo` on Linux, `fstatfs` on macOS). A clone needs no handover, its root carries its origin's owner.
`zfs` runs with an empty environment and a fixed system `PATH`, never the caller's.

#### Install and privilege

The helper is part of a build's distribution on every platform that has this substrate, Linux and macOS: each platform
directory carries `cowshed-helper` beside `cowshed` (`dist/bin/<platform>/`, and `.cache/bin/<platform>/` for a
checkout's development build). The package's `helper-bin-<arch>-<os>` target builds it into a tree of its own
(`.cache/helper/<platform>/`), because an output directory holds one `.build-inputs.json` and the platform directory
holds cowshed's; `helper-<arch>-<os>` copies it beside the binary, and `cli-record-<arch>-<os>` depends on that, so
`cli-native`, the publish jobs and the land's install build both. `cowshed setup`'s storage bootstrap runs the
distribution's helper from there, before anything is installed.

`cowshed setup` and `gateway start` install it beside the installed cowshed exactly as the Nx cache server is installed
(05_gateway.md, "The installed cowshed"): a copy named for its digest and the stable name `cowshed-helper` as a link to
it, in `~/Library/Application Support/dev.cowshed/bin`. A build with no `cowshed-helper` beside it is refused by name
and installs nothing. The install is the user's own and cannot make the copy root's, so how the helper gets its
privilege is decided per invocation from the file itself:

- **A helper that is root's and setuid runs directly.** That is an administrator's install (a setuid copy in a
  root-owned directory on `PATH`, or a NixOS `security.wrappers` entry). Of the helper files beside cowshed and then in
  each absolute `PATH` directory, the first such one serves, before the installed one.
- **Any other helper runs through `sudo -n`**, the first there is in that order, which is the installed one. A sudoers
  entry must admit it without a password, because the gateway daemon has no terminal to type one at. The installed path
  lies in a directory the user can write, so an entry naming the path alone would make root of whatever file is put
  there: the entry pins the helper's bytes,
  `#<uid> ALL=(root) NOPASSWD: sha256:<digest> <installed path, spaces escaped>`, and is written again when an install
  changes those bytes.

`cowshed setup` proves this once the helper is installed, on a store that is a ZFS dataset: it runs the `check` verb of
the helper the installed cowshed reaches, as it reaches it, and through `sudo` with `-k` besides, so the credentials
setup's own `sudo -v` cached do not answer for a daemon that has none. When the proof fails setup exits
`environment-missing`, saying what the attempt answered, and its `next:` line is the one root command that mends it: for
the installed helper, the entry above written to `/etc/sudoers.d/cowshed-helper-<uid>` (staged as
`cowshed-helper-<uid>.new`, a name `sudo` passes over, checked by `visudo -cf`, then renamed), with the entry itself in
the message for a host that keeps its sudoers elsewhere; for a setuid helper that was reached instead, the
`install -o 0 -g 0 -m 4755` that replaces it with this build's; and for an installed path sudoers cannot spell, the same
setuid install into `/usr/local/bin`. `gateway stop --purge` and `setup --uninstall` remove the installed helper;
neither removes a sudoers entry, which is root's.

Everything else (Landlock sandboxing (04_sandbox.md), gateway, secrets scanner, CLI) runs unprivileged.

**Open: the mountpoint check and the mount are two steps.** Between the helper's ownership walk and ZFS's mount, a
caller who owns the parent directory can replace a component with a symlink, and the dataset then mounts over a
directory that is not the caller's. On a host where the caller is not already root-equivalent this is an escalation.
Closing it means the helper mounts by a descriptor it holds (`mount(2)` onto `/proc/self/fd/<n>` with the `zfsutil`
option on Linux) instead of by the `mountpoint` string; whether a zoned namespace's root may issue that mount, and the
macOS equivalent, are unmeasured.

### Linux host paths

The store is at `/private/cowshed/store` on both platforms; `<bc>`, the build-cache root that holds the mbx store, is
platform-specific, as the table below states — only the volume technology behind each mountpoint also differs. Shared
layer-1 and layer-3 caches are not datasets: they live in the host HOME on both platforms (03_caches.md); the mbx store
is the one shared cache that is a dataset on ZFS and a dedicated volume on APFS, because its restores must clone
(03_caches.md, "The reflink-reachability rule"). ZFS additionally keeps workspace data in the sibling `projects` dataset
tree:

| macOS                                                                                                          | Linux                                                                  |
| -------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `cowshed.store` APFS volume at `/private/cowshed/store` (images, grants, quarantine, gateway state, telemetry) | `<pool>/cowshed/store` dataset at `/private/cowshed/store`             |
| `cowshed.build-cache` APFS volume at `/private/var/folders/cowshed/shared/build-cache` (the mbx store, `mbx/`) | `<pool>/cowshed/build-cache` dataset at `/private/cowshed/build-cache` |
| project images are files on `cowshed.store`                                                                    | `<pool>/cowshed/projects/<owner>/<repo>` workspace dataset containers  |

A Linux host without a ZFS root that serves the user has a **directory store**: `/private/cowshed/store` and
`/private/cowshed/build-cache` are plain directories of whatever filesystem holds them, owned by the user, and every
workspace payload is a directory moved by rename (`storage/directory.rs`). Its markers say `"substrate": "directory"`,
so nothing reads a directory store as a dataset or a volume, and no dataset or volume clone backs a fork: the source
tree is cloned file by file. Because a rename never crosses a mount, setup and every validation refuse a directory store
whose store path and per-user mount root (`~/.cowshed/mnt`) lie in different mounts, naming both.

**Linux host setup.** `cowshed setup` reads the kernel mount table, each host path without following a link, and for ZFS
`zfs get` of the exact dataset names, plans from that alone, and announces every action before it takes one:

- **Directory store.** `/private` and `/private/cowshed` are root's and writable by root alone; the store and
  build-cache directories below them are the user's, mode 0755, each published last by its marker. Making them is the
  one step that needs root: setup runs `sudo` (which asks on the terminal where its policy wants a password) for
  `install -d` of each missing directory, and nothing else. A path that holds entries without a marker, belongs to
  another user, is a link, or is root's and empty (which is also what the mountpoint of an unmounted dataset looks like)
  is refused by name and left as found; an ancestor that is not root's alone is refused the same way.
- **ZFS.** Setup attests that the root names the caller, then creates what is missing through the helper, one run each:
  `create <root>/store mountpoint=/private/cowshed/store`, `create <root>/projects mountpoint=none`,
  `create <root>/build-cache mountpoint=/private/cowshed/build-cache`, each with its `cowshed:role` and
  `cowshed:version` properties, and writes the marker file of each mounted one as the user. A dataset that exists
  unmounted is mounted (`set mountpoint=…`, `mount`), and one mounted and still empty without its marker is completed. A
  dataset mounted anywhere but its fixed path, or a fixed path that holds anything a mount would mask, is refused by
  name. The helper is found beside the running cowshed, then on `PATH`; it is run directly when it is setuid root and
  through `sudo -n` otherwise. No directory under `/private` is made by setup: `zfs` makes each mountpoint.
- **Both.** The per-user mount root `~/.cowshed/mnt` and `~/.cowshed` are made 0700 by the user, and `host.json` is
  published into a store that has none. Nothing is pinned in `/etc/fstab` and no system service is installed: a
  directory store has no volume to mount, and ZFS mounts a root's datasets when their pool is imported.
  `setup --uninstall` therefore removes nothing of the host's storage; the store stays, as a macOS volume does.

Validation (`validate_existing_host_storage`, every command and the gateway's start) gathers the same evidence and
performs only its reads, the marker guards and the root's attestation; anything a plan would change is reported as
setup-required and never done.

### Fixed dataset hierarchy

The ZFS root has exactly three sibling children; project datasets never sit beside or beneath the store dataset, and
nothing but the mbx cache dir sits in the build-cache dataset:

```
<pool>/cowshed
├── store       mountpoint=/private/cowshed/store
├── build-cache mountpoint=/private/cowshed/build-cache
└── projects    mountpoint=none
    └── <owner>/<repo>/{main,ws/...}
```

`store` contains repository bindings, trusted `policy.json`, grants, quarantine, gateway state, and telemetry.
`build-cache` contains only `mbx/`, the one mbx cache directory (01b_zfs.md, "The mbx store on a ZFS host"); mbx owns
its contents and no cowshed collection enters it. `projects` contains only workspace datasets and their snapshots.
Creation, helper containment checks, discovery, send/receive, and GC all use these exact roots; the siblings are not
configurable independently. A host that still has the `<pool>/cowshed/caches` dataset of an earlier release keeps it
mounted, never creating one, until its retirement destroys it (03_caches.md, "Wiring").

### macOS ZFS, btrfs

OpenZFS on macOS (o3x) is a kext requiring reduced-security boot on Apple silicon; cowshed supports the ZFS substrate
there when the pool already exists but never selects it by default — APFS images remain the macOS path. btrfs occupies
the third implementation slot when needed: subvolume snapshot + clone maps one-to-one onto the trait, its clones are
independent (no origin rule), and reflink crosses subvolumes natively; it is listed only to show the trait is not
ZFS-shaped.

## Tradeoffs

**Origin dependency accepted on ZFS (where shadows were rejected on macOS).** Both pin a base; the difference is blast
radius and cost. An hdiutil shadow pins the _entire base image file_ forever, degrades writes 2.34×, and blocks template
GC. A ZFS origin snapshot pins only since-overwritten blocks of main, costs nothing at creation, never blocks main's
writes, and dies with its workspace under a mechanical GC rule. Same physics, opposite economics.

**`zfs` CLI over libzfs bindings.** libzfs is explicitly unstable ABI; the CLI is the compatibility contract OpenZFS
actually maintains. cowshed shells out with `-H -p` machine output, exactly as it shells out to hdiutil/diskutil on
macOS. The helper boundary would exist either way.

**One dataset per workspace over one dataset per project with directory clones.** `cp --reflink` directory copies inside
a single dataset would avoid the mount-helper requirement but recreate the host-inode explosion (every file a visible
inode in one namespace), lose per-workspace quotas/stats/destroy, and make instant deletion impossible. Datasets are the
point.
