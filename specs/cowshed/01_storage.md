# Storage Layout

cowshed stores all durable artifacts on two dedicated APFS volumes and derives all runtime state from the filesystem and
the kernel. Nothing cowshed creates is visible in Finder, the Desktop, or the user's home directory listing — and
nothing cowshed churns lives on the Data volume, so the user's unique data keeps its metadata, fsck time, and snapshots
to itself.

## Project identity

A _project_ is a git repository, identified by its canonicalized root path:

```
project_id = "<dirname>-<hex8>"
hex8       = first 8 hex chars of SHA-256(canonical git root path, UTF-8)
```

Example: `/Users/danny/Dev/conloca` → `conloca-3f2a9c1b`. The mapping is a pure function — no registry, no lookups. It
is **not** collision-free: 32 bits of hash cannot guarantee uniqueness, so opening a project verifies that the marker's
`projectRoot` matches the canonical git root and returns a conflict (exit 4) on mismatch — cowshed never mounts the
wrong project on the strength of a short hash. Every path below uses `project_id`.

## Directory layout

```
~/.cowshed/
  store/                             # cowshed.store volume mountpoint (state; see "Dedicated volumes")
    <project_id>/
      main.asif                      # the adopted main workspace image
      main.asif.grants.json          # controller-owned grants (main is normally unsandboxed)
      sessions/
        <workspace>.asif             # one image per workspace
        <workspace>.asif.grants.json # controller-owned grant file (see 04_sandbox.md)
        <workspace>.asif.lock        # flock target for lifecycle operations
      checkpoints/
        <workspace>/<label>.asif     # clonefile snapshots (see 02_workspaces.md)
      quarantine/                    # secrets relocated by `cowshed adopt --quarantine` (02_workspaces.md)
      waivers.json                   # reasoned secret-scan waivers (02_workspaces.md)
    gateway/
      config.json                    # allowlist, upstream registries (optional; defaults apply)
      audit.ndjson                   # append-only egress audit log — protected state, never mixed with logs
    logs/                            # operational logs (per-command debug, daemon stderr), rotated
  caches/                            # cowshed.caches volume mountpoint (mirror, git mirrors, layer-3 caches)
  mnt/<project_id>/<workspace>/      # session mountpoints (empty dirs when detached)
  gateway.sock                       # gateway unix socket (control plane)

~/Library/LaunchAgents/dev.cowshed.*.plist   # the ONLY ~/Library residue — launchd requires this location
```

Workspace names match `[a-z0-9][a-z0-9-]{0,63}`; `main` is reserved.

## Images

- **Format**: ASIF via `diskutil image create blank --format ASIF`, the default on macOS 26+ (near-native I/O, sparse,
  single file). Measured, not assumed (single-run medians, substrate bench in
  `specs/cowshed/prototypes/apfs-workspace-bench/`): vs SPARSE, ASIF creates 2.1× faster, direct-writes 2.5× faster,
  direct-reads 5.6× faster, and runs metadata workloads 2.3× faster; clonefile is equal (~2 ms) and attach is ~75 ms
  slower — the one metric SPARSE wins, decisively outweighed. Sparse growth is near-identical. On hosts without ASIF
  support cowshed falls back to `hdiutil create -type SPARSE` transparently; the extension stays `.asif` and the format
  is recorded in the workspace marker. One ASIF-specific step: `diskutil` creates the inner volume root owned by root,
  so cowshed chowns the volume root to the user immediately after creation.
- **Capacity**: 100 GiB sparse. Capacity is a cap, not an allocation; images occupy only written blocks. Override per
  project via `.cowshed.toml` `capacity`.
- **Filesystem**: APFS, case sensitivity matching the volume that holds the adopted repository (queried via
  `pathconf(_PC_CASE_SENSITIVE)` at adopt time) so git behavior is identical inside and outside.
- **Volume name**: `cowshed.<project_id>.<workspace>` — unique, so mount-table rows are unambiguous.
- **Spotlight**: created with indexing disabled (`-nospotlight` / `mdutil -i off` post-attach).
- **Time Machine**: backup policy is one per-volume decision, not path exclusions. If Time Machine includes additional
  internal volumes by default (verification item, 08_testing.md), adopt excludes `cowshed.store` and `cowshed.caches`
  once, volume-level, at creation. Durability is git (`cowshed push`), never backup.

## Mounts

Attach dispatches on the marker's `imageFormat` — the two formats have disjoint attach tools (measured: `hdiutil attach`
refuses ASIF outright with _"use 'diskutil image attach'"_):

- **ASIF**: `diskutil image attach --nobrowse --mountPoint <path> <image>`. diskutil's defaults differ from hdiutil's
  (browsable, noowners), so cowshed passes the nobrowse/owners equivalents explicitly; ownership is additionally
  guaranteed by the chown-at-create step above.
- **SPARSE** (fallback): `hdiutil attach -nobrowse -owners on -mountpoint <path> <image>`.

For every attach:

- Session workspaces mount at `~/.cowshed/mnt/<project_id>/<workspace>`. `-nobrowse` keeps every cowshed volume out of
  Finder, the Desktop, and the sidebar regardless of Finder preferences.
- The **main workspace mounts at the repository's original path** (e.g. `~/Dev/conloca`), so adoption changes nothing
  about where the user works.
- Mountpoint directories are created before attach and removed by `cowshed gc`; an empty mountpoint dir is the defined
  "detached" state, and the underlying dir holds a stub `.envrc` used for self-healing (see 02_workspaces.md).
- Personal workspaces may opt into Finder visibility with `--browse` at attach time.

## Dedicated volumes

All cowshed bytes live on two dedicated APFS volumes, split by **rebuildability class**, so the Data volume carries no
cowshed churn at all:

- **`cowshed.caches`**, mounted at `~/.cowshed/caches` — everything rebuildable. Layout:
  `~/.cowshed/caches/{mirror,git,sccache,zig,gradle}` (see 03_caches.md). `git/` holds bare git mirrors
  (`<host>/<org>/<repo>.git`) — written only by the gateway via `cowshed repo mirror`, sandbox-read-only, the sole
  source of remote code inside workspaces (02_workspaces.md). Because nothing unique lives here, the nuclear recovery
  path is always safe: `diskutil apfs deleteVolume` + lazy recreate — the mirror refetches, sccache and registries
  rebuild. `cowshed doctor` offers it as the fix for cache-volume corruption; `cowshed gc` never needs more than it.
- **`cowshed.store`**, mounted at `~/.cowshed/store` — images, grant sidecars, waivers, quarantine, gateway config and
  audit, operational logs. Mostly rebuildable, with a small unique window: uncommitted work between autosaves
  (02_workspaces.md); durability is still git. Same-volume clonefile is preserved by construction — main → sessions →
  checkpoints and trash renames all stay within `cowshed.store`.

Both volumes are created lazily on first use
(`diskutil apfs addVolume <container> APFS <cowshed.caches|cowshed.store> -nomount`, then mounted `-nobrowse`) and share
the container's free-space pool — no sizing, no space cost for the split. macOS auto-mounts container volumes at boot,
but `-nobrowse` is not sticky: `cowshed ensure` re-mounts with canonical flags after reboot, and `cowshed doctor`
verifies presence and flags of both.

Why volumes and not paths on Data: Data takes hourly APFS local snapshots, and a snapshot pins every since-rewritten
block of a multi-GB churning image — ghosts that path-level `tmutil addexclusion` does **not** prevent (exclusion stops
backup, not snapshotting). Dedicated volumes get no local snapshots, collapse backup policy to one per-volume decision,
isolate fsck domains and corruption blast radius (cache loss = re-download; store loss = WIP since last autosave; Data
untouched either way), and leave the only volume with sandbox-writable subtrees — the caches volume — holding nothing
precious.

## Runtime state: derived, never stored

| Question                | Source of truth                                           |
| ----------------------- | --------------------------------------------------------- |
| Which workspaces exist? | `readdir` of `<project_id>/sessions/` (+ `main.asif`)     |
| What is attached where? | Kernel mount table (`getmntinfo`), matched by volume name |
| Workspace identity      | In-image marker `.cowshed/workspace.json`                 |
| Grants                  | Sibling file `<image>.grants.json`                        |
| Concurrency             | `flock` on `<image>.lock` per lifecycle operation         |

Marker-derived fields exist only _inside_ the image, so they are unreadable while a workspace is detached. `cowshed ls`
never attaches to answer: for detached workspaces it reports name, image mtime, and mount state from the host, and fills
`baseCommit`-class fields from a cached info snapshot written into the grants sidecar at detach time — stale-marked,
refreshed on the next attach.

### In-image marker: `.cowshed/workspace.json`

Written at adopt/new/fork/restore, at the volume root; travels with every clone:

```json
{
  "version": 1,
  "project": "conloca-3f2a9c1b",
  "projectRoot": "/Users/danny/Dev/conloca",
  "workspace": "raven",
  "role": "workspace", // "main" | "workspace"
  "imageFormat": "asif", // "asif" | "sparse"
  "baseCommit": "8f31c2d…", // main's HEAD at creation
  "createdAt": "2026-07-11T12:00:00Z",
  "forkedFrom": null // workspace name when created by `cowshed fork`
}
```

The in-image `.cowshed/` directory also holds `token` (the gateway identity, 0600, rewritten on new/fork/restore so
identities never duplicate), the in-image cache roots (03_caches.md), and `jobs/<id>/` — the per-exec output spool and
NDJSON exec records written by the shell supervisor (11_shell.md). Because job output lives inside the volume, a
checkpoint captures the execution history alongside the filesystem state it produced: snapshot-as-evidence.

### Grant files live outside the volume

`<image>.grants.json` sits next to the image on the host filesystem, owned by the invoking user, mode 0600. It is
**never** granted into any sandbox — a sandboxed process that could edit its own grant file could escalate itself. Only
cowshed-core (running unsandboxed as the controller) reads and writes it. Besides grants it carries the workspace's
`portBlock` binding (`{base, size}`; base = the gateway's per-workspace data-plane listener, base+1..15 = the
workspace's own bindable dev-server ports) and the detach-time info snapshot described above. Schema in 04_sandbox.md.

## Retention

Copy-on-write divergence only ever accumulates — checkpoints, idle workspaces, ZFS origin pins (09_substrates.md), and
APFS image-size ratchet all grow silently. cowshed keeps "storage efficient" from becoming a burden with retention
_conventions_, enforced by `cowshed gc`, never by a background daemon deleting work unasked:

| Object                             | Default retention                                                             | Exempt / override                                                                         |
| ---------------------------------- | ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Checkpoints                        | 14 days                                                                       | `cowshed checkpoint --keep` (named, never expires); `.cowshed.toml` `checkpoints.max_age` |
| CI failure checkpoints             | 7 days                                                                        | tagged `ci-fail`; `10_ci.md`                                                              |
| Idle workspaces                    | _flagged_ by `cowshed doctor` after 14 days no exec, **never auto-destroyed** | landing/removal is always explicit                                                        |
| Trash images (`sessions/.trash/…`) | drained on next `gc`                                                          | —                                                                                         |

- `cowshed gc` is the single enforcement point: drain trash, prune expired checkpoints, remove orphan mountpoints,
  `hdiutil compact` detached images whose written size exceeds referenced by a threshold, and (on ZFS) prune orphaned
  `cowshed:*` origin snapshots (09_substrates.md). It reports freed bytes on stdout; `--dry-run` lists what it would
  remove on stderr.
- `cowshed du` reports **written vs referenced** bytes per workspace and per checkpoint (the number that matters for CoW
  substrates — referenced is shared with the base, written is the true cost). `--json` for fleet dashboards. This is how
  a coordinator decides which long-lived workspaces to `cowshed rebase --fresh` (02_workspaces.md) to shed accumulated
  divergence.

## Tradeoffs

**No SQLite / state store.** Any database row describing mounts or workspaces is a cache of kernel or filesystem state
that drifts on reboot and Finder ejects, and drift demands reconciliation machinery. Deriving state makes "what cowshed
believes" and "what is on disk" the same thing by construction; `cowshed doctor` shrinks to invariant checks. The cost —
a few `readdir`/`getmntinfo` calls per command — is microseconds.

**Sparseimage/sparsebundle rejected.** Sparsebundle band files reintroduce thousands of host inodes per workspace for no
benefit (network-volume support is irrelevant here). Legacy `SPARSE` single files remain only as the pre-ASIF fallback:
ASIF measures decisively better on I/O and metadata (2–5.6×) at the cost of ~75 ms on attach and a second attach code
path (`diskutil image attach`).

**`/Volumes` mount root rejected.** DiskArbitration-managed mountpoints save a mkdir/rmdir pair but put cowshed paths in
a shared namespace where Finder surfaces them and name collisions get renamed (`conloca 1`). `~/.cowshed/mnt` gives
short, stable, hidden paths cowshed fully owns.

**`~/Library/Application Support` rejected.** The Apple-idiomatic location puts multi-GB churning images on the Data
volume, where local snapshots pin their rewritten blocks and backup policy needs per-path exclusion machinery. Two
dedicated volumes cost nothing (container space-sharing) and reduce cowshed's `~/Library` footprint to the launchd
plists that must live there.

**Images on the caches volume rejected.** Co-locating images with caches unlocks no additional sharing: container
volumes already pool free space, and the reflink boundary that matters is the image's _inner_ filesystem — clonefile
cannot cross it regardless of where the image file sits (the same wall that keeps bun's cache in-image, 03_caches.md).
The only same-volume relationship images need is with each other. Merging would also destroy the caches volume's
defining property — that deleting it is always safe.
