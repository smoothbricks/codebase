# Substrates

cowshed's workspace physics — cheap CoW clone, instant delete, own inode namespace — is provided by a substrate.
Everything above the substrate (grants, sandbox model, gateway, CLI contract, marker files, cache taxonomy) is
substrate-independent. Two substrates ship: **APFS images** (macOS, 01_storage.md/02_workspaces.md are written against
it) and **ZFS datasets** (Linux; macOS only where OpenZFS is already installed).

## The `Substrate` trait

Lifecycle is asynchronous because image tools, `zfs`, sync, fsck, mount, and helper RPCs can block. Pure synchronous
planning is deliberately separate from execution: planning validates identities, names, policy, topology, and intended
state transitions without touching the platform; execution performs the blocking plan on an async executor. No trait
method hides a blocking subprocess or filesystem operation in a synchronous call.

```rust
pub trait Substrate: Send + Sync {
    fn plan_adopt(&self, checkout: &Path, repo: &RepoId) -> Result<AdoptPlan>;
    fn plan_create(&self, from: &WorkspaceRef, name: &WsName) -> Result<CreatePlan>;
    fn plan_checkpoint(&self, ws: &WorkspaceRef, label: &Label, pin: Pin) -> Result<CheckpointPlan>;
    fn plan_restore(&self, ws: &WorkspaceRef, cp: &CheckpointRef, mode: RestoreMode) -> Result<RestorePlan>;
    fn plan_fork(&self, src: &WorkspaceRef, dst: &WsName) -> Result<ForkPlan>;
    fn plan_retire(&self, ws: &WorkspaceRef) -> Result<RetirePlan>;

    async fn execute_adopt(&self, plan: AdoptPlan) -> Result<Main>;
    async fn execute_create(&self, plan: CreatePlan) -> Result<Workspace>;
    async fn execute_checkpoint(&self, plan: CheckpointPlan) -> Result<CheckpointRef>;
    async fn execute_restore(&self, plan: RestorePlan) -> Result<RestoreReceipt>;
    async fn execute_fork(&self, plan: ForkPlan) -> Result<Workspace>;
    async fn execute_retire(&self, plan: RetirePlan) -> Result<RetiredRef>;
    async fn reclaim(&self, retired: RetiredRef) -> Result<()>;
    async fn list(&self, repo: &RepoId) -> Result<Vec<WorkspaceRef>>;
    async fn mount_state(&self, ws: &WorkspaceRef) -> Result<MountState>;
    async fn ensure_mounted(&self, ws: &WorkspaceRef) -> Result<PathBuf>;
    async fn unmount(&self, ws: &WorkspaceRef) -> Result<()>;
    async fn caches_root(&self) -> Result<PathBuf>;
    async fn stats(&self, ws: &WorkspaceRef) -> Result<SubstrateStats>;
    async fn gc(&self) -> Result<GcReport>;
}
```

Contract notes:

- **Plans are pure, immutable, and capability-free.** They contain validated logical names, expected
  incarnation/revision, and intended operations, but no open descriptors, mounted paths supplied by callers, or executed
  side effects. Execute revalidates all preconditions under the workspace lock before the first mutation; a stale plan
  conflicts.
- **Async execution owns blocking work.** Implementations use the platform executor's blocking lane for subprocesses and
  blocking syscalls, remain cancellation-safe at documented transaction boundaries, and never block an async runtime
  worker. CLI commands may synchronously wait at the outermost process boundary; library consumers await futures.

Selection is convention, not guesswork. `statfs` of the project root selects APFS images when it reports APFS. When it
reports ZFS, cowshed uses the containing dataset only if its pool has a suitable delegated cowshed root. If `statfs`
cannot identify such a dataset (including a non-ZFS checkout whose workspace data should live on ZFS), selection
requires an explicit `.cowshed.toml` `[substrate] kind = "zfs"` and `pool = "<pool>"`; cowshed never scans pools or
silently picks one. A configured pool must contain or permit creation of the exact hierarchy below. `cowshed doctor`
prints the selected substrate, pool, and evidence.

## APFS image substrate (reference)

As specified in 01_storage.md and 02_workspaces.md: one format-specific image per workspace — `.asif` for ASIF or
`.sparseimage` for SPARSE — with clonefile ≈ 2 ms and attach ≈ 235–400 ms. The host-readable sibling metadata carries
`imageFormat`; enumeration recognizes both extensions, and cowshed refuses an extension/metadata mismatch before attach.
Dispatch is `diskutil image attach` for ASIF and `hdiutil attach` for SPARSE, followed by verification before the first
mount (attach without mounting, `fsck_apfs -q`, then mount). Deletion unlinks one file. Clones preserve their source
format and extension and are fully independent of their source — no GC coupling.

## ZFS substrate

### Mapping

| cowshed concept                   | ZFS object                                                                                                                                                                                                   |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| project                           | `<pool>/cowshed/projects/<owner>/<repo>` (primary `repo_id`; component-safe container dataset)                                                                                                               |
| main workspace                    | `<pool>/cowshed/projects/<owner>/<repo>/main`, `mountpoint=` original checkout path                                                                                                                          |
| session workspace                 | `<pool>/cowshed/projects/<owner>/<repo>/ws/<name>`                                                                                                                                                           |
| workspace mounts                  | `mountpoint` inheritance on `…/ws` → `~/.cowshed/mnt/<owner>/<repo>/<name>`                                                                                                                                  |
| crash-consistent snapshot of main | `zfs snapshot main@cowshed:<name>` (transactionally atomic)                                                                                                                                                  |
| `cowshed new`                     | snapshot + `zfs clone` — tens of ms, mounts itself, **no attach step, no fsck step**                                                                                                                         |
| `cowshed checkpoint <ws> <label>` | `zfs snapshot ws@<label>` — instant, zero-copy                                                                                                                                                               |
| `cowshed restore`                 | clone-swap (default; later checkpoints survive) or `--discard-newer` → `zfs rollback`                                                                                                                        |
| `cowshed fork <src> <dst>`        | `zfs snapshot src@cowshed:fork-<dst>` + clone                                                                                                                                                                |
| `cowshed rm`                      | retire: `zfs rename` into `…/.trash`; reclaim: `zfs destroy` the clone **then** its origin snapshot — an idempotent logical transaction (physically two commands; `cowshed gc` completes interrupted halves) |
| capacity cap                      | `refquota` per workspace dataset (replaces sparse-image capacity)                                                                                                                                            |
| shared caches (layers 1 and 3)    | `<pool>/cowshed/caches` dataset mounted at `~/.cowshed/caches`                                                                                                                                               |
| Linux gateway attachment          | no persistent port block; per-incarnation Unix socket + private netns + trusted `127.0.0.1:7644` connector, created and destroyed with attachment                                                            |
| compaction                        | not needed — freed blocks return to the pool                                                                                                                                                                 |

Adopt on ZFS: create `…/projects/<owner>/<repo>/main`, copy the tree once (rsync-fidelity, preserving metadata), set
`mountpoint=` the original path. The original tree moves aside as `<root>.pre-cowshed` exactly as on macOS. The
self-healing stub `.envrc` is still written beneath the mountpoint — ZFS mounts also disappear when a pool isn't
imported, and healing parity keeps `cowshed ensure` identical across substrates.

Markers, tokens, grant semantics, secrets gates, and the cache taxonomy are unchanged. Detached metadata uses the same
schema but `portBlock` is optional and omitted for ZFS/Linux. When a dataset is attached, the controller creates its
private loopback-only netns, bind-mounts that incarnation's Unix gateway socket, and launches the trusted connector on
namespace-local `127.0.0.1:7644`; package tools use that HTTP address, not a Unix-socket API. Detach drains and kills
the connector cgroup and unlinks the socket before releasing the netns. Restore fences and drains the old connector
before publishing the replacement incarnation, then creates a fresh socket and connector before admitting jobs. Layer-2
extracted caches stay **in-dataset** and arrive via the clone, same as in-image: hardlinks cannot cross datasets, so a
shared extracted cache would demote installs to copies. OpenZFS ≥ 2.2 block cloning (`copy_file_range` → BRT) makes
reflink-grade copies work _across_ datasets in one pool; cowshed treats that as an opportunistic bonus, never
load-bearing (it is version- and tunable-dependent).

### The clone-origin dependency

Unlike APFS clonefile, a ZFS clone depends on its origin snapshot: `main@cowshed:raven` cannot be destroyed while
`ws/raven` exists. cowshed contains this with one rule — **one origin snapshot per workspace, destroyed together with
it**:

- `cowshed new raven` creates `main@cowshed:raven`; `cowshed rm raven` destroys `ws/raven` then `main@cowshed:raven`, in
  that order — an idempotent logical transaction (physically two commands; `cowshed gc` completes interrupted halves).
- `cowshed gc` prunes `cowshed:*` snapshots with no surviving clone (crash between the two destroys).
- Main is never blocked: writes to main proceed freely; a workspace's origin snapshot pins only the main blocks that
  main has since overwritten. `cowshed doctor` reports that pinned space per workspace (`zfs list -H -o written,origin`)
  so long-lived workspaces show their true cost.
- **Forks bend the star.** `cowshed fork src dst` snapshots the _source session_ (`src@cowshed:fork-<dst>`), hanging a
  spoke off a session rather than main — so `src` cannot be reclaimed while `dst` lives. The containment rule extends
  rather than breaks: a workspace's origin snapshot dies with it, and **a workspace with live dependents cannot be
  reclaimed** — `cowshed rm src` exits 4 naming the dependent forks; `cowshed doctor` reports fork dependencies and
  their pinned bytes so a coordinator lands or removes forks first.
- `zfs promote` is not used: promotion re-parents the dependency graph and makes teardown order data-dependent. cowshed
  keeps the graph shallow — main at the center, one snapshot spoke per workspace, fork spokes off sessions — and refuses
  reclaim while dependents exist rather than re-parenting.

### Privileged helper (Linux)

`zfs allow` delegation covers create/snapshot/clone/destroy but **not `mount(2)`**, which Linux gates on `CAP_SYS_ADMIN`
regardless of delegation. Substrate mutations therefore go through `cowshed-helper`: a minimal root binary (installed by
the package, auditable in one sitting) that accepts a fixed verb set
(`create|clone|snapshot|destroy|rename|swap|mount|unmount|set-mountpoint`) — `rename` serves retire's trash namespace,
`swap` is the narrowly-constrained clone-swap that `cowshed restore` needs — and refuses, on every verb, any dataset
argument outside `<pool>/cowshed/`. Constrained sudoers entries are the supported alternative for hosts that forbid
setuid helpers. Everything else — Landlock sandboxing (04_sandbox.md), gateway, secrets scanner, CLI — runs
unprivileged.

### Linux host paths

State paths are identical across platforms — the store volume/dataset at `~/.cowshed`, caches nested at
`~/.cowshed/caches` — only the volume technology behind each mountpoint differs. ZFS additionally keeps workspace data
in the sibling `projects` dataset tree:

| macOS                                                                                              | Linux                                                                 |
| -------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `cowshed.store` APFS volume at `~/.cowshed` (images, grants, quarantine, gateway state, telemetry) | `<pool>/cowshed/store` dataset at `~/.cowshed`                        |
| `cowshed.caches` APFS volume at `~/.cowshed/caches`                                                | `<pool>/cowshed/caches` dataset at `~/.cowshed/caches`                |
| project images are files on `cowshed.store`                                                        | `<pool>/cowshed/projects/<owner>/<repo>` workspace dataset containers |

### Fixed dataset hierarchy

The ZFS root has exactly three sibling children; project datasets never sit beside or beneath the store dataset:

```
<pool>/cowshed
├── store       mountpoint=~/.cowshed
├── caches      mountpoint=~/.cowshed/caches
└── projects    mountpoint=none
    └── <owner>/<repo>/{main,ws/...}
```

`store` contains repository bindings, trusted `policy.json`, grants, quarantine, gateway state, and telemetry. `caches`
is wholly rebuildable. `projects` contains only workspace datasets and their snapshots. Creation, helper containment
checks, discovery, send/receive, and GC all use these exact roots; the three siblings are not configurable
independently.

### Restore transaction

Restore is one fenced transaction on both substrates:

1. Acquire the workspace lifecycle lock and revalidate the plan's current `workspaceIncarnation`, grant revision,
   checkpoint identity, format/dataset, and absence of a concurrent retire. Stop admitting jobs, drain the
   revision-bound supervisor, and detach/unmount the current workspace.
2. Prepare the replacement at a non-enumerated staging name. Clone the checkpoint, validate it (including fsck on APFS),
   and validate the complete protected `CheckpointManifestRecord` against the controller `CheckpointCommitment`:
   `repo_id`, `origin_incarnation`, `barrier_id`, visible stream counts/hashes/paths, `records_sha256`, and
   `manifest_batch_sha256` must agree. Missing/altered content, invalid complete frames, or digest mismatch is typed
   `Integrity` before publication; only an incomplete trailing frame may be discarded as reported recovery with its
   `batch_sha256` retained. Preserve the logical workspace name, grant binding, CA, primary `repo_id`, and macOS
   `portBlock` only when present. Linux preserves no port block.
3. Mint the fresh `workspaceIncarnation` first. Then mint a fresh gateway token bound to that incarnation and write both
   into staged marker/token files, flush, and verify while undiscoverable. Copied job records retain their producer
   incarnation; allocation in the new incarnation starts above every inherited numeric job ID.
4. Atomically swap the staged replacement into the canonical image/dataset name while moving the displaced workspace to
   the undo checkpoint. Mount at the canonical path, then validate marker, incarnation, token, policy binding, and
   grants.
5. In one publication transaction, append
   `RestoreCommitment { version,order,repo_id,source_checkpoint,source_incarnation,destination_incarnation }`, publish
   detached metadata for the new incarnation with atomic replace + parent fsync, and switch gateway acceptance from the
   old token to the new. Only then may the gateway, supervisor, or exec admit the incarnation; no interval admits both.
6. On any failure before publication, unmount and remove the staged replacement, restore the displaced workspace under
   its original name, remount it, and resume its original supervisor/token. After publication, recovery completes the
   new state and never rolls back across the incarnation fence. `gc` resumes only idempotent cleanup.

Default ZFS restore uses clone-swap so later checkpoints survive; `--discard-newer` may use rollback but follows the
same fencing, token/incarnation, publication, and supervisor ordering.

### macOS ZFS, btrfs

OpenZFS on macOS (o3x) is a kext requiring reduced-security boot on Apple silicon; cowshed supports the ZFS substrate
there when the pool already exists but never selects it by default — APFS images remain the macOS path. btrfs occupies
the third implementation slot when needed: subvolume snapshot + clone maps one-to-one onto the trait, its clones are
independent (no origin rule), and reflink crosses subvolumes natively; it is unimplemented in v1 and listed only to show
the trait is not ZFS-shaped.

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
