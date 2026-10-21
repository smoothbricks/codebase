# Storage Layout

cowshed derives workspace state from controller-owned filesystem facts and the kernel, not a second mutable registry.
This spec owns repository identity, the logical store layout, shared metadata and retention policy.

[APFS storage](./01a_apfs.md) owns the macOS image format, Disk Arbitration lifecycle, device custody, mount policy,
dedicated volume provisioning and host failure mechanisms. Other substrates implement the same logical contract through
[09_substrates.md](./09_substrates.md).

## Repository identity

A _project_ is a checkout bound to one or more stable repository identities. A `repo_id` is machine-independent and has
exactly two validated components. A Cowshed installation may adopt any number of projects. Each project's primary
`repo_id` owns exactly one warm `main` workspace plus its own session/checkpoint namespace; `main` is never a
machine-global singleton.

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
`/private/cowshed/store/acme/widget/`; containment is checked after joining and symlinks are refused. The repository
binding is `/private/cowshed/store/acme/widget/repository.json`; trusted project policy is
`/private/cowshed/store/acme/widget/policy.json`. Both are controller-owned, mode 0600, and never visible to a sandbox.
Every path below uses the primary `repo_id` through this component-safe mapping.

## Directory layout

The store root owns the logical namespaces below. This tree shows the macOS/APFS realization at
`/private/cowshed/store`; its physical mount, marker and boot ordering are specified in
[Dedicated store volume](./01a_apfs.md#dedicated-store-volume). There is no intermediate `store/` level:

```
/private/cowshed/store/                          # ← the cowshed.store volume, fstab-pinned here (01a_apfs.md, "Dedicated store volume")
  .cowshed-volume.json               # volume marker: its ABSENCE means "not mounted" (01a_apfs.md, mount ordering); a store's is at layout version 2
  host.json                          # host configuration: credential routes; mount root is derived, never stored
  .staging/                          # port-block reservations held while a workspace's grants are minted
  .blank/                            # APFS blank templates (01a_apfs.md, "Images")
    <bytes>-<uid>-<gid>.dmg          # one formatted, verified, detached image per capacity and owner
    <bytes>-<uid>-<gid>.lock         # flock held only while that template is minted
    <bytes>-<uid>-<gid>-minting-<nonce>.dmg # a template being minted, under a name no minter used before
  .retirements/<owner>/               # explicit whole-project retirement authority, outside the tree it moves
    <repo>.json                      # version, operationId, repoId, authorization, phase, registrationSha256
    <repo>.lock                      # serializes record publication and phase transitions
    <repo>.lease                     # shared by project actors/healers; exclusive for archival; removed by an unbinding
                                     #   that upgrades to exclusive with no record beside it (02_workspaces.md)
    <repo>.run                       # one retirement execution, fenced to the exact recorded generation
  .retired-projects/                  # preserved, detached evidence; never discovered as live projects
    <operation-id>/                  # the complete former project store tree, renamed without copying or deleting
    <operation-id>.json              # completed retirement receipt
  <owner>/                           # a project owner, and the only store-root entry discovery reads
    .cowshed-owner                   # empty: makes this directory an owner (written when its first project is provisioned)
  <owner>/<repo>/                    # primary repo_id, encoded one component at a time
    repository.json                  # chosen remote binding, alternate identities, and primary designation
    checkout-root.json               # where the checkout was when main was last removed (written then, read to reopen)
    slot-bindings.json               # build slot → workspace bindings (`fork --slot`, 06_cli.md)
    policy.json                      # trusted project policy (checkpoint quotas, standing grants); controller-owned, 0600
    waivers.json                     # reasoned secret-scan waivers (02_workspaces.md)
    lifecycle-intents.json           # bounded persist-before-mutate create/fork/remove recovery journal, mode 0600
    lifecycle-intents.json.lock      # flock held for each read-modify-write of the journal
    deletion-log.jsonl               # one line per artifact the controller unlinked, one per build volume released, one per workspace retired (naming the process that retired it and what a force overrode) and per abandon bundle kept and per abandoned ref retired, with Git tips (storage/deletion_log.rs)
    main.dmg                         # adopted main image (created here behind a PendingFence sidecar)
    main.dmg.grants.json             # controller-owned grants + detached metadata
    main.dmg.ca.key                  # main's workspace CA private key, 0600
    main.dmg.lock                    # flock target for lifecycle operations
    .staging/                        # restore stages, never enumerated
    sessions/
      <workspace>.dmg                # one image per workspace
      <workspace>.dmg.grants.json    # grants + detached metadata (see 04_sandbox.md)
      <workspace>.dmg.ca.key         # the workspace's CA private key, 0600
      <workspace>.dmg.lock           # flock target for lifecycle operations
      <workspace>.intent.lock        # flock the process executing <workspace>'s lifecycle intent holds
      <workspace>.land.lock          # flock a land or an rm of <workspace> holds for its run (02 "A land and an rm of one workspace exclude each other")
      .trash/                        # removed images awaiting reclamation (`gc`)
    checkpoints/
      <workspace>/<label>.dmg        # clonefile snapshot
    build/
      <id>.dmg                       # one build volume: Cargo target dirs + Nx cache and task DB (16_build_volumes.md)
      <id>.dmg.json                  # its sidecar: built-at tree, linking checkout, checked-out tree when placed, seed flag, created
      <id>.dmg.hold                  # flock each job on the volume holds shared; a release claims it exclusive, then deletes it
      .trash/<id>.dmg                # released images; the daemon deletes them within a minute (16_build_volumes.md)
    tmp/
      <workspace>/                   # its TMPDIR, 0700: writable by its own sandbox only; reclaimed with its retired image,
                                     #   read-only directories its tools sealed made owner-writable first (`go clean -modcache`)
    quarantine/                      # secrets relocated by `cowshed adopt --quarantine`, and keyless sidecars awaiting `rekey`
  gateway.sock                       # gateway unix socket (control plane; root-level keeps sun_path short)
  sccache.sock                       # the host sccache daemon's socket (03_caches.md)
  run/
    manager.sock                     # the daemon's supervisor manager (11_shell.md)
    <digest>.sock                    # one workspace supervisor's socket
    <digest>.groups                  # that supervisor's process-group ledger
    gateway/                         # Linux only, 0700: per-incarnation data-plane Unix sockets, each mode 0600
      <workspaceIncarnation>.sock    # bind-mounted into exactly one attached workspace; absent after detach/restore fence
  telemetry/                         # ALL telemetry: Arrow IPC segments, day-partitioned (13_telemetry.md)
    <yyyy-mm-dd>/*.arrow             #   lifecycle spans, gateway audit, grant mutations, command debug
  (rebuildable caches are not beneath this tree: each tool keeps its own in the host HOME; see 03_caches.md)
  (workspaces mount under one fixed per-user root; macOS path and provisioning: 01a_apfs.md, "Stable workspace mount root")
```

**Owners are marked, never inferred.** Every walker of the store root reads only the directories that hold
`.cowshed-owner` and are not reserved names (dotted names and the controller's own namespaces above): one `lstat` passes
over anything else, however much it holds. Nothing is ever read as a repository under a reserved name. A project's
provisioning writes its owner's marker. A store whose volume marker is at layout version 1 predates the markers: every
enumeration of it refuses, naming the store and `cowshed setup`, and setup migrates it. Setup alone walks the store root
by what it finds: it marks each directory that holds a `<repo>/repository.json`, and only those, then records layout
version 2 in the store's volume marker, last and atomically, so a migration cut short is redone by the next setup.
Measured 2026-10-20, before the markers: six telemetry archives moved into the store root, one holding 3,070,019 files,
made every command that discovers projects probe each entry of each for a volume marker and a binding.
`cowshed path main` sat in that probe for more than 18 minutes, and in the same window a land's Nx cache initialization
took 2685 s. With the archives renamed under a dotted name, `cowshed path main` took 0.19 s and the same initialization
0.3 s.

Workspace mountpoints live under a fixed **mount root**, laid out `<mount-root>/<owner>/<repo>/<workspace>`. On macOS
the canonical root is `/private/var/folders/cowshed/<uid>/mnt`, derived from the invoking numeric UID; Linux uses
`~/.cowshed/mnt`. There is no mount-path setting in `host.json` or `cowshed setup`. Provisioning, the Spotlight
admission rationale and the existing-host refusal belong to
[Stable workspace mount root](./01a_apfs.md#stable-workspace-mount-root). For a preferred access path, the operator
creates a symlink **at that preferred path pointing to the canonical root**, for example
`ln -s /private/var/folders/cowshed/$(id -u)/mnt ~/workspaces`. The canonical root remains a real directory, never a
symlink to another location; mounting through an indexed physical path would defeat the policy. The root is a plain
directory on Data: no volume mounts there, so the masked-mountpoint failure class cannot exist. Git identity rules must
cover the canonical mount path, not rely on a symlink alias. `cowshed setup`/`doctor` detect divergent
`includeIf gitdir:` rules empirically by diffing `git config --show-origin` between each adopted checkout and a probe
repository under the mount root. This directory is distinct from the in-image `.cowshed/` namespace inside every clone.

<!-- prettier-ignore -->
```
~/Library/LaunchAgents/dev.cowshed.*.plist   # launchd: gateway is a user agent; boot mounts are system/dev.cowshed.storage
~/Library/Logs/cowshed/*.log                 # launchd stderr for pre-tracer-init crashes; kept off /private/cowshed/store
```

Workspace names match `[a-z0-9][a-z0-9-]{0,63}`; `main` is reserved. Repository paths are safe because both `repo_id`
components are validated and encoded independently; the slash between them is structural and percent-decoding is never
performed during path lookup.

Sidecar suffixes append to the complete image filename (`<workspace>.dmg.grants.json`). `portBlock` in detached sidecar
metadata is optional and platform-specific: it is present only for macOS workspaces and is omitted on Linux; Linux does
not synthesize a base port in persistent metadata.

The Data-volume footprint is limited to the empty `/private/cowshed/store` mountpoint directory, the per-user workspace
mount root (on macOS, the fixed `/private/var/folders/cowshed/<uid>/mnt`), and cowshed's user cache directory
`~/Library/Caches/dev.cowshed`, which holds the gateway's registry and repository mirrors. Durable evidence lives on the
dedicated store volume, outside Data's snapshots, backups, and fsck domain. Layer-3 caches belong to the tools and stay
where the tools keep them in HOME (03_caches.md).

### Two `.cowshed` namespaces

The name appears in two namespaces that must not be confused. **Host-absolute `/private/cowshed/store`** exists once —
the herd's store volume holds images, grants, and telemetry. **In-image relative `.cowshed/`** exists once _per
workspace_, at each workspace volume root — the marker, token, CA certificate, in-image cache roots, and job spools —
and travels with every clone. The wiring rule follows the split: shared state resolves to a host-absolute path — under
`/private/cowshed/store/...`, or a shared cache's own directory in the host HOME (03_caches.md) — that is the same
string in every context, while workspace-keyed state resolves to in-image relative `.cowshed/...` paths (correct in
every clone automatically). Main and sessions use identical wiring; only the sandbox's permission mask differs.

## Runtime state: derived, never stored

| Question                | Source of truth                                                                                                                              |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Which workspaces exist? | For the selected primary `repo_id`, `readdir` its `sessions/` images plus that project's exactly one `main`                                  |
| What is attached where? | Kernel mount table (`getfsstat` into an owned buffer), matched by mount point, identity confirmed by the in-image marker                     |
| Workspace identity      | In-image marker `.cowshed/workspace.json`                                                                                                    |
| Grants                  | Sibling file `<image>.grants.json`                                                                                                           |
| Concurrency             | `flock` on `<image>.lock` per lifecycle operation; `<workspace>.intent.lock` held by the process executing that workspace's lifecycle intent |

The detached metadata required for discovery and attach lives in `<image>.grants.json`; at minimum it contains the
workspace identity, in addition to the grant schema in 04_sandbox.md, so `cowshed ls` and attach never mount an image to
learn what it is. Marker-derived fields that exist only _inside_ the image remain unreadable while detached.
`cowshed ls` reports name, image mtime, and mount state from host data, and fills `baseCommit`-class fields from a
cached info snapshot in the same sidecar — stale-marked, refreshed on the next attach.

### In-image marker: `.cowshed/workspace.json`

Written at adopt/fork/restore, at the volume root; travels with every clone:

```json
{
  "version": 1,
  "repoId": "acme/widget",
  "projectRoot": "<project-root>",
  "workspace": "raven",
  "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80", // fresh controller-minted 128-bit id on create/fork/restore
  "role": "workspace", // "main" | "workspace"
  "baseCommit": "8f31c2d…", // main's HEAD at creation
  "createdAt": "2026-07-11T12:00:00Z",
  "forkedFrom": null, // workspace name when created by `cowshed fork`
  "createdTrace": "4bf92f…" // trace id of the fork/restore that created this image (13_telemetry.md)
}
```

`workspaceIncarnation` identifies one mutable workspace timeline. Checkpoints retain the incarnation in their copied job
records; a fork destination and every restore result mint a fresh incarnation, so a reused numeric job id cannot collide
with controller telemetry from a discarded or sibling timeline. It is public identity, not a credential. The sibling
host sidecar carries the current incarnation for detached discovery; each job record carries the incarnation that
actually produced it.

`createdTrace` is the CoW-lineage anchor: `fork`/`restore`/`checkpoint` link the new trace to it, so the clone graph is
a queryable provenance graph (13_telemetry.md). The in-image `.cowshed/` directory also holds `token` (the gateway
identity, 0600, rewritten on fork/restore so identities never duplicate), the workspace CA **certificate** (the public
trust anchor for egress interception — the private key stays controller-side, 04_sandbox.md/05_gateway.md), the in-image
cache roots (03_caches.md), and the protected `.cowshed/job/` authority domain.

The trusted supervisor is the only live writer beneath `.cowshed/job/**`; every executed shell, named session, startup
hook, and descendant receives the mandatory child restriction before repository-controlled code runs (04_sandbox.md).
`.cowshed/job/records.arrow` is the framed protected `ProtectedRecord` stream: `Job(JobArtifactRecord)` or
`CheckpointManifest(CheckpointManifestRecord)`. Small terminal stdout/stderr may live as Arrow Binary in a Job row. A
stream promotes lazily to `.cowshed/job/<numeric-id>/out` or `err` only when it exceeds the bounded inline limit or a
checkpoint/background/replay requirement forces residency; no per-job path is promised. Each stream records byte count,
SHA-256, bounded summary, and exact captured/redirect plus inline/file discriminants. Checkpoint manifests carry
`version, repo_id, origin_incarnation, barrier_id, visible_jobs, records_sha256`; the visible stream commitment is
`storage_kind, bytes, sha256, protected_path` with path present iff file. Complete Arrow batches and sealed spills are
immutable. A records file legitimately holds frames its ancestor incarnations wrote, because fork and restore clone the
source image; the image's marker lineage (02_workspaces.md) is what authorizes them, and a frame from any other
incarnation is an integrity fault. The controller additionally emits compact Admission/Terminal/Checkpoint/Fork/Restore
audit records — existence/status/order/lineage and expected hashes/digests, never raw payload or artifact paths — to an
optional sink that nothing reads for a decision (07_api.md/13_telemetry.md).

### Grant files live outside the volume

`<image>.grants.json` sits next to the image on the host filesystem, owned by the invoking user, mode 0600. It is
**never** granted into any sandbox — a sandboxed process that could edit its own grant file could escalate itself. Only
cowshed-core (running unsandboxed as the controller) reads and writes it. Besides grants it carries the workspace's
identity and `workspaceIncarnation` needed while detached, the optional macOS-only `portBlock` binding (`{base, size}`;
base = the gateway's per-workspace data-plane listener, `base+1 … base+size-1` = the workspace's own bindable dev-server
ports; a new block is 64 ports, and a live block keeps its size until a `service_ports` grant grows it — a grant that
moves it keeps the old block in `retainedPortBlocks`, reserved to the workspace until it retires), and the detach-time
info snapshot described above. Linux sidecars omit `portBlock`. The workspace's CA **private key** sits alongside it
(`<image>.ca.key`, 0600, same controller-only, sandbox-denied treatment) — the gateway signs per-host interception
leaves with it; only the public CA cert ever enters the image (04_sandbox.md/05_gateway.md). Schema in 04_sandbox.md.

The APFS adapter's interrupted image/metadata publication protocol is specified in
[Restore publication custody](./01a_apfs.md#restore-publication-custody).

## Retention

Copy-on-write divergence only ever accumulates — checkpoints, idle workspaces, abandon bundles, ZFS origin pins
(09_substrates.md), and APFS image-size ratchet all grow silently. `cowshed gc` enforces retention conventions.
Background reclamation handles only explicitly released build images and provably dead scratch, never idle workspaces:

| Object                                       | Default retention                                                                     | Exempt / override                                                                          |
| -------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Checkpoints                                  | all younger than 14 days **plus the newest five per workspace, whichever keeps more** | explicit pin from a label or `cowshed checkpoint --keep`; pins never expire until unpinned |
| CI failure checkpoints                       | same checkpoint rule as above                                                         | labeled `ci-fail`, therefore explicitly pinned until unpinned; `10_ci.md`                  |
| Idle workspaces                              | _flagged_ by `cowshed doctor` after 14 days no exec, **never auto-destroyed**         | landing/removal is always explicit                                                         |
| Trash images (`sessions/.trash/…`)           | drained on next `gc`                                                                  | —                                                                                          |
| Released build images (`build/.trash/…`)     | deleted by the daemon within a minute of release, at background disk priority         | `cowshed gc` empties its project's at once                                                 |
| Abandon bundles (`sessions/.trash/*.bundle`) | never reclaimed by default; their tips are also kept as refs in main's repository     | explicit `cowshed gc --abandoned`                                                          |
| Abandoned refs (in main's repository)        | retired by every `gc` once main holds each tip, by ancestry or content                | `cowshed gc --supersede <ws>/<head> --reason <one line>`                                   |
| Dead shared-temp scratch                     | sockets/empty locks deleted; all other contents renamed to `<store>/.trash/scratch/`  | unknown/live owners kept; archived evidence kept for 24 hours before background deletion   |

- `cowshed gc` evaluates both checkpoint floors independently: it deletes only an unpinned checkpoint that is at least
  14 days old **and** is not among the newest five for that workspace. A user-supplied label creates an explicit pin;
  `--keep` pins the generated or supplied label. Pin state is authoritative detached metadata, not inferred from a
  filename, and an explicit unpin is required before such a checkpoint is eligible.
- Checkpoint quota accounting is per workspace. One authoritative `SubstrateStats` read reports the active image's
  logical and allocated bytes, all published checkpoint allocated bytes/count, and the pinned checkpoint byte subset.
  Admission projects `existing checkpoint bytes + active allocated bytes` and `existing count + 1`; pinned and automatic
  checkpoints both consume the cap. Exact `<=` boundaries are admitted, while `>` returns `Conflict` before cloning or
  publishing any checkpoint image, fact, or detached metadata. A sibling workspace never consumes this workspace's
  coordinator-owned quota.
- Garbage collection is two-phase. `preview_gc(repo, options)` is a read-only enumeration that returns an immutable plan
  of exact candidates with stable SHA-256 identity, host path, allocated bytes, and closed reason. The plan records its
  sweep options. Pinned checkpoints are retained and never become candidates. `execute_gc(plan)` acquires all plan locks
  without waiting, re-enumerates at the plan's observation time under the same options, and rejects a
  pin/incarnation/path/byte/concurrency change as stale before any mutation. Only an unchanged plan drains trash, orphan
  staging/session images and mountpoints, prunes expired checkpoints, and, only under `--abandoned`, reclaims abandon
  bundles; execution reports actual freed bytes. Nothing compacts an image: guest-freed space stays allocated, so an
  image's allocation tracks its high-water mark ([APFS Images](./01a_apfs.md#images), "Space reclamation"). A candidate
  whose own cleanup cannot finish is deferred by path and diagnostic while the sweep attempts the others. Uncertain
  ownership, invalid identity, or a stale whole plan still refuse: proceeding through those could delete data that does
  not belong to the candidate.
- Abandon bundles are regular `*.bundle` files directly in `sessions/.trash`; staged `.partial` siblings and symlinks
  are not bundle candidates. Plain `gc`, opportunistic collection, and `doctor --repair` never authorize their
  reclamation. Only explicit `--abandoned` (`GcOptions.abandoned: true`) adds the `abandonedBundle` category to the
  exact plan. `gc --dry-run --abandoned` lists those candidates and their allocated bytes without deleting them.
  Reclamation never loses the commits: before executing the plan, every bundle candidate's tips are fetched into main's
  repository under the namespace its removal used, `refs/cowshed/<ws>/abandoned/<head>/` (02_workspaces.md, `rm` step 1;
  the file name `<ws>-<head>.bundle` names both), and journaled as a `keep-abandoned-tips` deletion-log line on the
  bundle's path naming that repository and each ref; each kept tip is also said on stderr. A removal by this build
  already kept them; a bundle an earlier build wrote, whose removal record says `predatesPreservation`, gets them here.
  A purge keeps them the same way before its main fetch fail refuses the whole sweep as a conflict naming each, before
  anything is deleted. Keeping them is never hiding them: a plain `gc` reports how many the trash keeps and their
  allocated bytes, `doctor` reports them as an `abandon-bundles` info finding, and `ls --all` names each project's after
  its table. Naming them authorizes nothing; the plain sweep's own plan still never holds one.
- Abandoned refs are retired by every `gc` once main holds them, so main's repository keeps no ref for work it already
  has. Each `refs/cowshed/<ws>/abandoned/<head>/` namespace's tips are measured as the `rm` gate measures a branch
  (02_workspaces.md, `rm` step 1): an ancestor of main's branch, or every file its commits change already as main has
  it, whatever history, patch identity or conflict resolution got it there. A namespace main holds every tip of is
  deleted ref by ref (`update-ref -d <ref> <tip>`, so a ref moved since it was measured is refused, never lost) and
  journaled as one `retire-abandoned-ref` deletion-log line naming each tip and the branch and head that hold it
  (`{"by":"target",…}`). Every other namespace is kept: `gc` reports it, `doctor` names it as an `abandoned-refs` info
  finding with each tip's count of commits main does not hold, and `ls --all` names it after its table. Its owner lets
  one go with `cowshed gc --supersede <ws>/<head> --reason "<one line>"`, which retires that namespace whatever main
  holds and journals the reason (`{"by":"superseded","reason":…}`); a blank, whitespace-only or multi-line reason, or a
  namespace main's repository does not keep, is refused. `--dry-run` reports both and deletes nothing. When main's
  repository cannot be measured, nothing is retired and the report says why; a supersession then refuses. A ref under an
  `abandoned/` namespace that no abandonment writes is reported and never touched.
- Explicit workspace retirement is the sole exception to live checkpoint retention: its exact trash metadata authorizes
  one workspace-scoped cleanup plan that includes pinned checkpoints and pre-restore undo generations as well as the
  trash image and empty mountpoint. Preview and execution validate the repository, workspace, incarnation, checkpoint
  facts, and every associated artifact under the workspace lock; any change makes the plan stale before mutation.
  Cleanup deletes the retirement trash metadata last so an interrupted pass retains authority for the next idempotent GC
  pass. A missing canonical image or orphan checkpoint fact alone never authorizes deletion. APFS-specific handling of
  late root-owned filesystem event logs is specified in
  [Retired mountpoint debris](./01a_apfs.md#retired-mountpoint-debris).
- `cowshed du` reports **written vs referenced** bytes per workspace and per checkpoint (the number that matters for CoW
  substrates — referenced is shared with the base, written is the true cost). `--json` for fleet dashboards. This is how
  a coordinator decides which long-lived workspaces to `cowshed rebase --fresh` (02_workspaces.md) to shed accumulated
  divergence.

### Shared temporary scratch

The daemon sweeps at startup and every **60 seconds**, independently of build-image deletion. `gc` runs the same sweep
before answering, including a read-only preview for `--dry-run`. Each pass examines at most **256 directory entries**,
not 256 successful deletions; a retained directory cannot make the pass unbounded. The daemon keeps its directory
cursors between passes, so live entries at the front do not starve dead entries behind them. Each root gets a turn, and
a complete scan restarts on the next pass. Sixty seconds amortizes idle polling; 256 bounds filesystem and process
probes even when the shared temp root contains thousands of other tools' entries. Background archive deletion has its
own 256-entry budget and never synchronously walks an entire output tree.

Multiwriter runtime ownership is the private `.cowshed-owner` record in the scratch directory, containing
`cowshed-scratch-v1`, one positive pid per line, and a final `end` line. The terminator makes an interrupted publication
unknown, not a shorter complete lease. Single-owner fixtures instead publish the creator atomically in the private
directory's name: `<cowshed-/cs-prefix>-p<8-hex-PID><7-hex-nonce>`. The suffix is exactly the 17 bytes of the unowned
`<prefix>-<16-hex>` name it replaces, the length fixtures under the workspace's runtime link are sized for
(08_testing.md, "Fixtures"), so nested Unix socket paths still fit `sockaddr_un`. `p` is not a hex digit, so no unowned
name parses as an owner. Creation takes a fresh nonce when the name exists. A prefix outside `cowshed-`/`cs-` gets the
same name but no claim: its fixture's own cleanup remains its only owner. This keeps payload roots pristine: the fixture
may itself be a Git worktree or a directory containing only telemetry date partitions. Before an evidence rename changes
that name, the reaper carries the creator into the record used by archive cleanup. An existing malformed record never
falls back to the name.

Every recorded process must be dead according to the existing OS process probe. EPERM, unreadable or malformed records,
missing ownership evidence, foreign ownership, and symlinks are kept with a reason. PID reuse keeps the directory
conservatively. Neither creation time nor mtime proves death; an old live directory survives. Legacy `cowshed-sock-*`
and `cowshed-mgr-*` roots without a record or the explicit pid grammar remain unknown, even when all they hold is
`s.sock.lock`. A held socket kernel lease keeps a directory after its recorded creator has died.

The shared test `TempRoot` uses the named single-owner protocol for socket and manager roots and its other private
fixtures. The runtime write-site audit also covers Git's `cowshed-ignore-rules-*`, detached disk-command
`cowshed-disk-image-*` stdout/stderr (both caller and detached child are owners), and the cache advisor's
`cowshed-advisor-*` (parent and probe child). Existing unmarked test scratch is reported but not guessed dead. Workspace
runtime `cs-*` **symlinks** retain their existing `point_runtime_link` lifecycle; this directory reaper never follows or
removes them. Workspace exec temp and sandbox profiles belong to the store's existing workspace and profile reclamation,
not the shared-temp sweep. Real-image `cowshed-itest-*` roots and their sweep lock/record remain with
`cowshed-apfs/tests/support/scratch_apfs.rs`: that owner stops workers and releases attached images under its own lock
before reclaiming a root. The generic reaper explicitly excludes and reports that domain, even if a fixture acquires a
marker; moving an attached backing image here would race its existing recovery protocol.

Only a bounded set of the marker, `s`/`s.sock` Unix sockets and an empty `s.sock.lock` can be deleted directly.
Unexpected files, subdirectories, nonempty locks, stdout, and Arrow traces are evidence: one rename moves the whole
directory into the store's private `.trash/scratch/<archive-seconds>-<uuid>`. A cross-filesystem rename failure keeps
the source and reports the OS error; there is no copy/delete fallback. The archive's 24-hour recovery window starts at
the rename, not at scratch creation. `gc` does not delete those archives. The daemon later removes them incrementally;
it never follows symlinks or deletes through a mounted descendant (including same-device Linux bind mounts). Its
original ownership record stays until a fresh listing shows nothing else in the archive, and its exact bytes go back if
the root then cannot be removed (an unwritable trash directory, an entry created after that listing), so an interrupted
cleanup can resume; a timestamp-shaped name alone never authorizes deletion. A walk that fails (an unreadable subtree, a
mounted descendant, an entry it cannot remove) keeps the archive and its record, reports both the archive and the error,
and gives up that walk so the archives behind it still take their turn; the next full listing walks it again from its
root.

The roots are canonical `/tmp` and the daemon/CLI's non-RAM temp root, deduplicated. Foreign `node_*`, `bash_*`,
`cargo_*`, `bun_*`, `devenv-*`, and `sccache_*` entries are excluded, even if they contain a marker. Bounded-exec
remains the sole owner of RAM lease reclamation: `ram-temp.ts::settle` runs under its kernel lock, claim ledger and
identity recheck during acquire/release. This sweep never enters `smoo-ram*` volumes, leases, staging, `.archive`,
`.archive/.deleting`, locks or ledgers, and reports that delegation. Reclamation and retention decisions are printed by
path and reason; the summary states the examined count and bound.

## Tradeoffs

**No SQLite / state store.** Any database row describing mounts or workspaces is a cache of kernel or filesystem state
that drifts on reboot and Finder ejects, and drift demands reconciliation machinery. Deriving state makes "what cowshed
believes" and "what is on disk" the same thing by construction; `cowshed doctor` shrinks to invariant checks. Commands
read the source directories and owned kernel mount snapshots rather than maintaining a mutable cache of them.
