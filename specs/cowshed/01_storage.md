# Storage Layout

cowshed stores all durable artifacts on two dedicated APFS volumes and derives all runtime state from the filesystem and
the kernel. Nothing cowshed creates is visible in Finder, the Desktop, or the user's home directory listing — and
nothing cowshed churns lives on the Data volume, so the user's unique data keeps its metadata, fsck time, and snapshots
to itself.

## Repository identity

A _project_ is a checkout bound to one or more stable repository identities. A `repo_id` is machine-independent and has
exactly two validated components:

```
repo_id = lowercase(owner) + "/" + lowercase(repo)
```

For a repository with remotes, discovery proposes candidates from configured remote URLs and the user or trusted policy
selects one. URL normalization removes transport syntax, credentials, query/fragment, a trailing `.git`, and redundant
slashes before extracting `owner/repo`; it never guesses across ambiguous remotes and never silently mints an identity.
The binding records the chosen remote name and normalized URL and every open validates that its normalized `owner/repo`
still equals `repo_id`. Multiple identities may be bound to one checkout (for example upstream and fork), with exactly
one primary identity used for storage paths. A local-only repository requires an explicit `repo_id` at adopt time.
Moving or recloning a checkout does not change its identity.

Each `owner` and `repo` component is encoded independently as one filesystem component: lowercase ASCII
`[a-z0-9][a-z0-9._-]*`, with percent-encoding for every byte outside that set and for literal `%`, `.` and `..`. At the
layout root, an owner component equal to a host namespace (`gateway`, `telemetry`, `caches`, `mnt`, or
`.cowshed-volume.json`) is also percent-escaped, so a valid remote identity can never alias controller infrastructure.
The `/` in `repo_id` is the one layout separator, never untrusted path text. Thus `acme/widget` maps to
`~/.cowshed/acme/widget/`; containment is checked after joining and symlinks are refused. The repository binding is
`~/.cowshed/acme/widget/repository.json`; trusted project policy is `~/.cowshed/acme/widget/policy.json`. Both are
controller-owned, mode 0600, and never visible to a sandbox. Every path below uses the primary `repo_id` through this
component-safe mapping.

## Directory layout

`~/.cowshed` **is** the `cowshed.store` volume — the dotdir is a mountpoint, not a directory tree on Data. There is no
intermediate `store/` level; the volume root is the layout root:

```
~/.cowshed/                          # ← the cowshed.store volume, mounted here (see "Dedicated volumes")
  .cowshed-volume.json               # volume marker: its ABSENCE means "not mounted" (see mount ordering below)
  <owner>/<repo>/                    # primary repo_id, encoded one component at a time
    repository.json                  # chosen remote binding, alternate identities, and primary designation
    policy.json                      # trusted project policy; controller-owned, mode 0600
    main{.asif|.sparseimage}          # adopted main image; exactly one format-specific extension exists
    main{.asif|.sparseimage}.grants.json  # controller-owned grants + detached metadata
    sessions/
      <workspace>{.asif|.sparseimage}     # one image per workspace
      <workspace>{.asif|.sparseimage}.grants.json  # grants + detached metadata (see 04_sandbox.md)
      <workspace>{.asif|.sparseimage}.lock         # flock target for lifecycle operations
    checkpoints/
      <workspace>/<label>{.asif|.sparseimage}      # clonefile snapshot; extension is preserved
    quarantine/                      # secrets relocated by `cowshed adopt --quarantine` (02_workspaces.md)
    waivers.json                     # reasoned secret-scan waivers (02_workspaces.md)
  gateway/
    config.json                      # allowlist, upstream registries (optional; defaults apply)
  gateway.sock                       # gateway unix socket (control plane; root-level keeps sun_path short)
  run/gateway/                       # Linux only, 0700: per-incarnation data-plane Unix sockets, each mode 0600
    <workspaceIncarnation>.sock      # bind-mounted into exactly one attached workspace; absent after detach/restore fence
  telemetry/                         # ALL telemetry: Arrow IPC segments, day-partitioned (13_telemetry.md)
    <yyyy-mm-dd>/*.arrow             #   lifecycle spans, gateway audit, grant mutations, command debug
    daemon-stderr.log                #   the one text file: launchd stderr for pre-tracer-init crashes only
  caches/                            # ← cowshed.caches volume, NESTED mount (mirror, git mirrors, layer-3 caches)
  mnt/<owner>/<repo>/<workspace>/    # ← session workspace mounts, nested (empty dirs when detached)

~/Library/LaunchAgents/dev.cowshed.*.plist   # the ONLY ~/Library residue — launchd requires this location;
                                             # home-manager-owned on nix hosts (14_nix.md)
```

Workspace names match `[a-z0-9][a-z0-9-]{0,63}`; `main` is reserved. Repository paths are safe because both `repo_id`
components are validated and encoded independently; the slash between them is structural and percent-decoding is never
performed during path lookup.

After this layout, `{.asif|.sparseimage}` denotes exactly one format-selected extension: `.asif` for ASIF or
`.sparseimage` for SPARSE. Sidecar suffixes append to the complete image filename. `portBlock` in detached sidecar
metadata is optional and platform-specific: it is present only for macOS workspaces and is omitted on Linux; Linux does
not synthesize a base port in persistent metadata.

After this layout, cowshed's entire Data-volume home footprint is **one empty mountpoint directory** (plus the
home-manager-managed items above): everything real lives on the two cowshed volumes, outside Data's snapshots, backups,
and fsck domain.

### Two `.cowshed` namespaces

The name appears in two namespaces that must not be confused. **Host-absolute `~/.cowshed`** exists once — it is the
herd's: images, grants, caches, telemetry, mounts. **In-image relative `.cowshed/`** exists once _per workspace_, at
each volume root — the marker, token, CA certificate, in-image cache roots, job spools — and travels with every clone.
The wiring rule follows the split: shared/rebuildable state resolves to host-absolute `~/.cowshed/...` paths (the same
string in every context), workspace-keyed state resolves to in-image relative `.cowshed/...` paths (correct in every
clone automatically). Main and sessions use identical wiring; only the sandbox's permission mask differs.

## Images

- **Format and extension**: ASIF via `diskutil image create blank --format ASIF`, the default on macOS 26+ (near-native
  I/O, sparse, single file), uses `.asif`. Measured, not assumed (single-run medians, substrate bench in
  `specs/cowshed/prototypes/apfs-workspace-bench/`): vs SPARSE, ASIF creates 2.1× faster, direct-writes 2.5× faster,
  direct-reads 5.6× faster, and runs metadata workloads 2.3× faster; clonefile is equal (~2 ms) and attach is ~75 ms
  slower — the one metric SPARSE wins, decisively outweighed. Sparse growth is near-identical. On hosts without ASIF
  support cowshed falls back to `hdiutil create -type SPARSE` transparently and uses `.sparseimage`. The sibling
  detached metadata records `imageFormat: "asif" | "sparse"`; its value and the filename extension MUST agree, or
  cowshed refuses before dispatching an attach tool. After mounting, the in-image marker MUST also agree; a mismatch
  fails the attach and cowshed detaches immediately. One ASIF-specific step: `diskutil` creates the inner volume root
  owned by root, so cowshed chowns the volume root to the user immediately after creation.
- **Capacity**: 100 GiB sparse. Capacity is a cap, not an allocation; images occupy only written blocks. Override per
  project via `.cowshed.toml` `capacity`.
- **Filesystem**: APFS, case sensitivity matching the volume that holds the adopted repository (queried via
  `pathconf(_PC_CASE_SENSITIVE)` at adopt time) so git behavior is identical inside and outside.
- **Volume name**: `cowshed.<repo_id-encoded>.<workspace>` — `repo_id-encoded` is the two safe components joined with
  `--`, so volume names are unique without embedding `/` and mount-table rows are unambiguous.
- **Spotlight**: created with indexing disabled (`-nospotlight` / `mdutil -i off` post-attach).
- **Time Machine**: backup policy is one per-volume decision, not path exclusions. If Time Machine includes additional
  internal volumes by default (verification item, 08_testing.md), adopt excludes `cowshed.store` and `cowshed.caches`
  once, volume-level, at creation. Durability is git (`cowshed push`), never backup.

## Mounts

Attach resolves the format without mounting: workspace enumeration accepts only `.asif` and `.sparseimage` image names
and treats both extensions for the same workspace stem as a conflict, then reads `imageFormat` from the sibling
host-side metadata. The extension and metadata must map to the same format or attach refuses with a conflict; this
avoids depending on the inaccessible in-image marker to choose an attach tool. After mounting, cowshed also requires the
in-image marker's `imageFormat` to match. The two formats have disjoint attach tools (measured: `hdiutil attach` refuses
ASIF outright with _"use 'diskutil image attach'"_):

- **ASIF (`.asif`)**: `diskutil image attach --nobrowse --noMount --plist <image>`.
- **SPARSE (`.sparseimage`, fallback)**: `hdiutil attach -nobrowse -owners on -nomount -plist <image>`.

Both commands return machine-readable attachment data from which cowshed selects the APFS volume device. Before the
first mount, cowshed runs `fsck_apfs -q <device>`; any non-zero result detaches the image and fails without exposing a
workspace mount. It then mounts explicitly with `diskutil mount nobrowse -mountPoint <path> <device>` (`--browse` omits
`nobrowse`). ASIF and SPARSE therefore share the same verify-before-mount safety boundary even though their image
attachment tools remain disjoint. Ownership is also guaranteed by the ASIF chown-at-create step above.

For every mounted attachment:

- Session workspaces mount at `~/.cowshed/mnt/<owner>/<repo>/<workspace>`. `-nobrowse` keeps every cowshed volume out of
  Finder, the Desktop, and the sidebar regardless of Finder preferences.
- The **main workspace mounts at the repository's original path** (written `<project-root>` below), so adoption changes
  nothing about where the user works.
- Mountpoint directories are created before attach and removed by `cowshed gc`; an empty mountpoint dir is the defined
  "detached" state, and the underlying dir holds a stub `.envrc` used for self-healing (see 02_workspaces.md).
- Personal workspaces may opt into Finder visibility with `--browse` at attach time.

## Dedicated volumes

All cowshed bytes live on two dedicated APFS volumes, split by **rebuildability class**, so the Data volume carries no
cowshed churn at all:

- **`cowshed.caches`**, mounted at `~/.cowshed/caches` — everything rebuildable. Layout:
  `~/.cowshed/caches/{mirror,repo-mirrors,cargo,sccache,zig,gradle,go/{mod,build},nix/{cache,state}}` (see
  03_caches.md). `repo-mirrors/` holds bare git mirrors (`<host>/<org>/<repo>.git`) — written only by the gateway via
  `cowshed repo mirror`, sandbox-read-only, and distinct from Cargo's shared writable `cargo/git` cache. Because nothing
  unique lives here, the nuclear recovery path is always safe: `diskutil apfs deleteVolume` + lazy recreate — the mirror
  refetches, sccache and registries rebuild. `cowshed doctor` offers it as the fix for cache-volume corruption;
  `cowshed gc` never needs more than it.
- **`cowshed.store`**, mounted at `~/.cowshed` — images, grant sidecars, waivers, quarantine, gateway config and audit,
  telemetry. Mostly rebuildable, with a small unique window: uncommitted work between autosaves (02_workspaces.md);
  durability is still git. Same-volume clonefile is preserved by construction — main → sessions → checkpoints and trash
  renames all stay within `cowshed.store`.

Both volumes are created lazily on first use
(`diskutil apfs addVolume <container> APFS <cowshed.caches|cowshed.store> -nomount`, then mounted `-nobrowse`) and share
the container's free-space pool — no sizing, no space cost for the split. macOS auto-mounts container volumes at boot,
but `-nobrowse` is not sticky: `cowshed ensure` re-mounts with canonical flags after reboot, and `cowshed doctor`
verifies presence and flags of both.

**Mount ordering and the unmounted-masking guard.** The layout nests: the caches volume and every workspace mount at
directories that live _on_ the store volume, so `cowshed.store` mounts first — the login agent and `cowshed ensure`
sequence store → caches → workspaces. When the store volume is not mounted, `~/.cowshed` is a bare, empty directory on
Data; the volume-root marker `.cowshed-volume.json` is how every cowshed command tells the difference — marker absent ⇒
treat as unmounted and heal before doing anything (the same shape as the workspace stub `.envrc`). The underlying Data
directory is kept empty by construction so nothing ever silently lands on Data behind an unmounted volume.

Why volumes and not paths on Data: Data takes hourly APFS local snapshots, and a snapshot pins every since-rewritten
block of a multi-GB churning image — ghosts that path-level `tmutil addexclusion` does **not** prevent (exclusion stops
backup, not snapshotting). Dedicated volumes get no local snapshots, collapse backup policy to one per-volume decision,
isolate fsck domains and corruption blast radius (cache loss = re-download; store loss = WIP since last autosave; Data
untouched either way), and leave the only volume with sandbox-writable subtrees — the caches volume — holding nothing
precious.

## Runtime state: derived, never stored

| Question                | Source of truth                                                                 |
| ----------------------- | ------------------------------------------------------------------------------- |
| Which workspaces exist? | `readdir` for `.asif`/`.sparseimage` images in `sessions/` (+ exactly one main) |
| Image format            | Sibling metadata `imageFormat`, validated against the image extension           |
| What is attached where? | Kernel mount table (`getmntinfo`), matched by volume name                       |
| Workspace identity      | In-image marker `.cowshed/workspace.json`                                       |
| Grants                  | Sibling file `<image>.grants.json`                                              |
| Concurrency             | `flock` on `<image>.lock` per lifecycle operation                               |

The detached metadata required for discovery and attach lives in `<image>.grants.json`; at minimum it contains the
workspace identity and `imageFormat`, in addition to the grant schema in 04_sandbox.md. Thus `cowshed ls` and attach do
not need to mount an image to discover its format. Marker-derived fields that exist only _inside_ the image remain
unreadable while detached. `cowshed ls` reports name, format, image mtime, and mount state from host data, and fills
`baseCommit`-class fields from a cached info snapshot in the same sidecar — stale-marked, refreshed on the next attach.

### In-image marker: `.cowshed/workspace.json`

Written at adopt/new/fork/restore, at the volume root; travels with every clone:

```json
{
  "version": 1,
  "repoId": "acme/widget",
  "projectRoot": "<project-root>",
  "workspace": "raven",
  "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80", // fresh controller-minted 128-bit id on create/fork/restore
  "role": "workspace", // "main" | "workspace"
  "imageFormat": "asif", // "asif" | "sparse"
  "baseCommit": "8f31c2d…", // main's HEAD at creation
  "createdAt": "2026-07-11T12:00:00Z",
  "forkedFrom": null, // workspace name when created by `cowshed fork`
  "createdTrace": "4bf92f…" // trace id of the new/fork/restore that created this image (13_telemetry.md)
}
```

`workspaceIncarnation` identifies one mutable workspace timeline. Checkpoints retain the incarnation in their copied job
records; a fork destination and every restore result mint a fresh incarnation, so a reused numeric job id cannot collide
with controller telemetry from a discarded or sibling timeline. It is public identity, not a credential. The sibling
host sidecar carries the current incarnation for detached discovery; each job record carries the incarnation that
actually produced it.

`createdTrace` is the CoW-lineage anchor: `fork`/`restore`/`checkpoint` link the new trace to it, so the clone graph is
a queryable provenance graph (13_telemetry.md). The in-image `.cowshed/` directory also holds `token` (the gateway
identity, 0600, rewritten on new/fork/restore so identities never duplicate), the workspace CA **certificate** (the
public trust anchor for egress interception — the private key stays controller-side, 04_sandbox.md/05_gateway.md), the
in-image cache roots (03_caches.md), and `.cowshed/job/` — each exec receives a workspace-local monotonic numeric ID and
stores its full raw streams at `.cowshed/job/<numeric-id>/out` and `.cowshed/job/<numeric-id>/err`; Arrow exec records
live at `.cowshed/job/records.arrow` (11_shell.md/13_telemetry.md). Because job output lives inside the volume, a
checkpoint captures the execution history alongside the filesystem state it produced: snapshot-as-evidence.

### Grant files live outside the volume

`<image>.grants.json` sits next to the image on the host filesystem, owned by the invoking user, mode 0600. It is
**never** granted into any sandbox — a sandboxed process that could edit its own grant file could escalate itself. Only
cowshed-core (running unsandboxed as the controller) reads and writes it. Besides grants it carries the workspace's
identity, `workspaceIncarnation`, and `imageFormat` needed while detached, the optional macOS-only `portBlock` binding
(`{base, size}`; base = the gateway's per-workspace data-plane listener, base+1..15 = the workspace's own bindable
dev-server ports), and the detach-time info snapshot described above. Linux sidecars omit `portBlock`. The workspace's
CA **private key** sits alongside it (`<image>.ca.key`, 0600, same controller-only, sandbox-denied treatment) — the
gateway signs per-host interception leaves with it; only the public CA cert ever enters the image
(04_sandbox.md/05_gateway.md). Schema in 04_sandbox.md.

## Retention

Copy-on-write divergence only ever accumulates — checkpoints, idle workspaces, ZFS origin pins (09_substrates.md), and
APFS image-size ratchet all grow silently. cowshed keeps "storage efficient" from becoming a burden with retention
_conventions_, enforced by `cowshed gc`, never by a background daemon deleting work unasked:

| Object                             | Default retention                                                                     | Exempt / override                                                                          |
| ---------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Checkpoints                        | all younger than 14 days **plus the newest five per workspace, whichever keeps more** | explicit pin from a label or `cowshed checkpoint --keep`; pins never expire until unpinned |
| CI failure checkpoints             | same checkpoint rule as above                                                         | labeled `ci-fail`, therefore explicitly pinned until unpinned; `10_ci.md`                  |
| Idle workspaces                    | _flagged_ by `cowshed doctor` after 14 days no exec, **never auto-destroyed**         | landing/removal is always explicit                                                         |
| Trash images (`sessions/.trash/…`) | drained on next `gc`                                                                  | —                                                                                          |

- `cowshed gc` evaluates both checkpoint floors independently: it deletes only an unpinned checkpoint that is at least
  14 days old **and** is not among the newest five for that workspace. A user-supplied label creates an explicit pin;
  `--keep` pins the generated or supplied label. Pin state is authoritative detached metadata, not inferred from a
  filename, and an explicit unpin is required before such a checkpoint is eligible.
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
a shared namespace where Finder surfaces them and name collisions get renamed (`widget 1`). `~/.cowshed/mnt` gives
short, stable, hidden paths cowshed fully owns.

**`~/Library/Application Support` rejected.** The Apple-idiomatic location puts multi-GB churning images on the Data
volume, where local snapshots pin their rewritten blocks and backup policy needs per-path exclusion machinery. Two
dedicated volumes cost nothing (container space-sharing) and reduce cowshed's `~/Library` footprint to the launchd
plists that must live there.

**A `store/` sub-mountpoint rejected.** Mounting the store volume at `~/.cowshed/store` left `~/.cowshed` as a real
Data-volume directory holding one wrapper level that meant nothing to users ("store with nested telemetry"). Mounting
the volume at `~/.cowshed` itself makes the dotdir a door, not a room: one empty mountpoint inode on Data, every other
path one level shorter, and "cowshed stuff lives in the cowshed" is literally true. The costs — mount ordering and the
bare-directory guard above — are machinery `ensure` already had for the workspaces.

**Images on the caches volume rejected.** Co-locating images with caches unlocks no additional sharing: container
volumes already pool free space, and the reflink boundary that matters is the image's _inner_ filesystem — clonefile
cannot cross it regardless of where the image file sits (the same wall that keeps bun's cache in-image, 03_caches.md).
The only same-volume relationship images need is with each other. Merging would also destroy the caches volume's
defining property — that deleting it is always safe.
