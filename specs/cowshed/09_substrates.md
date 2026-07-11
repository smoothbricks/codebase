# Substrates

cowshed's workspace physics — cheap CoW clone, instant delete, own inode namespace — is provided by a substrate.
Everything above the substrate (grants, sandbox model, gateway, CLI contract, marker files, cache taxonomy) is
substrate-independent. Two substrates ship: **APFS images** (macOS, 01_storage.md/02_workspaces.md are written against
it) and **ZFS datasets** (Linux; macOS only where OpenZFS is already installed).

## The `Substrate` trait

```rust
pub trait Substrate: Send + Sync {
    fn adopt(&self, checkout: &Path, project: &ProjectId) -> Result<Main>;
    fn list(&self, project: &ProjectId) -> Result<Vec<WorkspaceRef>>;   // enumeration is substrate-owned: readdir vs `zfs list`
    fn snapshot_main(&self, main: &Main) -> Result<SnapshotRef>;        // crash-consistent point; owned — see below
    fn create_workspace(&self, from: SnapshotRef, name: &WsName) -> Result<Workspace>; // mounted; consumes the snapshot
    fn checkpoint(&self, ws: &Workspace, label: &Label) -> Result<CheckpointRef>;
    fn restore(&self, ws: &Workspace, cp: CheckpointRef, mode: RestoreMode) -> Result<()>;
    fn fork(&self, src: &Workspace, dst: &WsName) -> Result<Workspace>;
    fn retire(&self, ws: &Workspace) -> Result<RetiredRef>;             // logical: fast, drops out of enumeration
    fn reclaim(&self, retired: RetiredRef) -> Result<()>;               // physical: idempotent, background, gc-resumable
    fn mount_state(&self, ws: &WorkspaceRef) -> Result<MountState>;     // derived, never cached
    fn ensure_mounted(&self, ws: &WorkspaceRef) -> Result<PathBuf>;
    fn unmount(&self, ws: &WorkspaceRef) -> Result<()>;
    fn caches_root(&self) -> Result<PathBuf>;                           // shared layer-3 area
    fn stats(&self, ws: &WorkspaceRef) -> Result<SubstrateStats>;       // space, pinned, quota, dependents
    fn gc(&self) -> Result<GcReport>;
}
```

Contract notes:

- **`SnapshotRef` is owned.** It is released by `create_workspace` (which consumes it) or by explicit cleanup/`gc` —
  never leaked. On APFS the "snapshot" _is_ the clonefile: `snapshot_main` may return a token meaning "clone the live
  image now" and the two calls collapse into one operation; the trait permits that. On ZFS it is a real
  `main@cowshed:<name>` snapshot with a lifetime the substrate must account for.
- **`retire`/`reclaim` replace one APFS-shaped `destroy`.** Retire is the perceived-instant half — APFS renames the
  image into `sessions/.trash/`, ZFS renames the dataset into a `…/.trash` namespace — after which the workspace no
  longer enumerates. Reclaim is the physical half — unlink; `zfs destroy` clone then origin snapshot — idempotent,
  allowed to fail transiently (busy mounts, dependent forks), resumed by `gc`.
- **References are opaque.** `WorkspaceRef`/`SnapshotRef`/`RetiredRef`/`CheckpointRef` never leak image paths or dataset
  names above the trait; everything above it addresses workspaces by name.

Selection is convention, not configuration: `statfs` of the project root decides — APFS → image substrate, ZFS → zfs
substrate. `.cowshed.toml` `substrate = "apfs-image" | "zfs"` overrides (the only expected use: forcing images on a mac
that also runs OpenZFS). `cowshed doctor` prints which substrate is active and why.

## APFS image substrate (reference)

As specified in 01_storage.md and 02_workspaces.md: one `.asif` per workspace, clonefile ≈ 2 ms, `hdiutil attach` ≈ 235
ms, verification before first mount (attach `-nomount`, `fsck_apfs -q`, then mount), deletion unlinks one file. Clones
are fully independent of their source — no GC coupling.

## ZFS substrate

### Mapping

| cowshed concept                   | ZFS object                                                                                                                                                                                                   |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| project                           | `<pool>/cowshed/<project_id>` (container dataset)                                                                                                                                                            |
| main workspace                    | `<pool>/cowshed/<project_id>/main`, `mountpoint=` original checkout path                                                                                                                                     |
| session workspace                 | `<pool>/cowshed/<project_id>/ws/<name>`                                                                                                                                                                      |
| workspace mounts                  | `mountpoint` inheritance on `…/ws` → `~/.cowshed/mnt/<project_id>/<name>`                                                                                                                                    |
| crash-consistent snapshot of main | `zfs snapshot main@cowshed:<name>` (transactionally atomic)                                                                                                                                                  |
| `cowshed new`                     | snapshot + `zfs clone` — tens of ms, mounts itself, **no attach step, no fsck step**                                                                                                                         |
| `cowshed checkpoint <ws> <label>` | `zfs snapshot ws@<label>` — instant, zero-copy                                                                                                                                                               |
| `cowshed restore`                 | clone-swap (default; later checkpoints survive) or `--discard-newer` → `zfs rollback`                                                                                                                        |
| `cowshed fork <src> <dst>`        | `zfs snapshot src@cowshed:fork-<dst>` + clone                                                                                                                                                                |
| `cowshed rm`                      | retire: `zfs rename` into `…/.trash`; reclaim: `zfs destroy` the clone **then** its origin snapshot — an idempotent logical transaction (physically two commands; `cowshed gc` completes interrupted halves) |
| capacity cap                      | `refquota` per workspace dataset (replaces sparse-image capacity)                                                                                                                                            |
| shared caches (layer 3)           | `<pool>/cowshed/caches` dataset mounted at `~/.cowshed/caches`                                                                                                                                               |
| compaction                        | not needed — freed blocks return to the pool                                                                                                                                                                 |

Adopt on ZFS: create `…/main`, copy the tree once (rsync-fidelity, preserving metadata), set `mountpoint=` the original
path. The original tree moves aside as `<root>.pre-cowshed` exactly as on macOS. The self-healing stub `.envrc` is still
written beneath the mountpoint — ZFS mounts also disappear when a pool isn't imported, and healing parity keeps
`cowshed ensure` identical across substrates.

Markers, tokens, grant semantics, secrets gates, and the cache taxonomy are unchanged. Layer-2 extracted caches stay
**in-dataset** and arrive via the clone, same as in-image: hardlinks cannot cross datasets, so a shared extracted cache
would demote installs to copies. OpenZFS ≥ 2.2 block cloning (`copy_file_range` → BRT) makes reflink-grade copies work
_across_ datasets in one pool; cowshed treats that as an opportunistic bonus, never load-bearing (it is version- and
tunable-dependent).

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
`~/.cowshed/caches` — only the volume technology behind each mountpoint differs (a pool necessarily exists on the ZFS
substrate, so dedicating datasets costs nothing, exactly like APFS container volumes):

| macOS                                                                                              | Linux                                                         |
| -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `cowshed.store` APFS volume at `~/.cowshed` (images, grants, quarantine, gateway state, telemetry) | `<pool>/cowshed/store` dataset at `~/.cowshed`                |
| `cowshed.caches` APFS volume at `~/.cowshed/caches`                                                | `<pool>/cowshed/caches` dataset at `~/.cowshed/caches`        |
| Keychain (gateway secrets)                                                                         | secret-service (keyring) or `systemd-creds` on headless hosts |
| launchd (gateway, login attach)                                                                    | systemd user units                                            |

### What ZFS adds

- **`zfs send -i`**: ship a warm workspace or checkpoint to another host (fleet runners cloning a shared main,
  10_ci.md), and real incremental backup of main — a durability upgrade over the APFS posture where images are excluded
  from backup and git is the only exit.
- **`refquota`/`refreservation`** per workspace; **native encryption** per project dataset.
- **No fsck, ever**: snapshots are consistent by construction; `cowshed new` drops the verify step.

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
