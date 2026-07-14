# Workspace Lifecycle

The central convention: **main is the base**. There are no templates, no refresh pipelines, and no registration steps.
The **main workspace** is the adopted image-backed workspace and its standalone Git repository/object store. It is not
the checked-out Git `main` branch, and it is not shorthand for that branch's working tree or index. The main workspace
is always warm because the user works in its currently checked-out working tree and merges land there; every new
workspace is a copy-on-write clone of the main workspace's live image. Operations that update only a non-checked-out ref
in its repository do not change its checked-out branch, index, or working tree.

Every use of `main` in this specification is project-scoped. A host may adopt any number of repositories, each with its
own warm `main`, sessions, checkpoints, grants, gateway identity, and standalone Git object store under its primary
`repo_id`. Commands select the project explicitly with global `--project <git-root>` or derive it from cwd; `new`,
`fork`, `checkpoint`, `restore`, `push`, and `land` never select a machine-global `main` or cross repository boundaries.

## The safe-to-clone-main invariant

Cloning main's live image for any consumer — including sandboxed agents — is safe because secrets never live in a
worktree:

- No `.env` files. Credentials exist only inside cowshed-gateway (Keychain-backed, 05_gateway.md).
- Environment wiring (`cowshed ensure`) exports build-configuration variables only, never secrets.

Everything else in main — uncommitted source changes, installed dependencies, warm build state — is exactly what makes a
clone useful.

### Secrets enforcement

The invariant is checked, not assumed. One built-in scanner (`cowshed-core::secrets`), three call sites:

- **`cowshed adopt` — blocking gate.** Full-tree scan before the image is created (skipping cache roots and `.git`
  objects). Findings refuse the adopt (exit 4) with per-file stderr guidance: migrate the value to the gateway Keychain,
  delete the file, or `cowshed adopt --quarantine`, which moves findings to `~/.cowshed/<owner>/<repo>/quarantine/` (the
  primary component-safe `repo_id` path; mode 0600, original paths preserved) so dependent tooling fails loudly instead
  of leaking silently. There is no "adopt anyway" flag — main's image is cloned to every future workspace; adopt is the
  one moment the invariant is cheap to establish.
- **`cowshed push` and autosave — delta gate.** Scans only content new relative to the base commit (cheap). Push refuses
  with exit 4 and names the offending hunks; autosave skips the snapshot and emits one `cowshed:` warning — it never
  blocks work, but it never propagates a finding to the host repo either. This closes the write side: an agent that
  pastes a token into a file cannot export it through cowshed.
- **`cowshed doctor` — recurring audit.** Re-scans every workspace incrementally (mtime-bounded) and reports findings
  with `fix:` hints; `--json` for fleet automation.

Detection is convention-tiered, no configuration: filename rules (`.env*`, `*.pem`, `id_*`, `.netrc`, `.npmrc`/`.pypirc`
with auth keys, cloud credential dirs) plus content rules for well-known token shapes (`ghp_…`, `xoxb-…`, `sk-…`,
`AKIA…`, PEM blocks) and `.envrc`/shell exports of `*_TOKEN|*_SECRET|*_KEY` variables. No entropy heuristics — cowshed
prefers zero false positives over exhaustiveness, and says so; `cowshed doctor --secrets-deep` shells out to `gitleaks`
when installed for the paranoid pass. Waivers are explicit and controller-owned: per-path entries in the project's
waivers file next to the grant files (never inside the image), each with a required `reason`, shown by `cowshed doctor`
so waived paths stay visible.

## `cowshed adopt` — create the main workspace

Converts an existing checkout into that project's image-backed `main` workspace mounted at its original path. One-time
per project, copy-bound (clonefile cannot cross volumes). The source is a _live_ tree — editors, watchers, and build
daemons mutate it during the copy — so adopt is an explicit transaction with defined crash points, not a best-effort
script:

1. Verify: git root, clean-enough state (adopt refuses mid-merge/rebase), free space, the secrets gate, and repository
   identity. Remote discovery may propose normalized lowercase `owner/repo` candidates but never silently chooses or
   mints one. Adoption selects a remote, records its normalized URL and `repo_id`, validates they agree, designates one
   primary identity if several are bound, and requires `--repo-id owner/repo` for a local-only repository. Programmatic
   callers supply the same explicit identity as `AdoptOptions.repo_id`; omission is valid only when remote selection
   produces the trusted binding. Load trusted policy only from `~/.cowshed/<owner>/<repo>/policy.json`. Also require
   that `<root>.pre-cowshed` does **not** already exist (exit 4 — a previous adopt left state behind; resolve it first).
   Ensure host setup is present — declaratively validated when home-manager/nix-darwin owns it
   (`programs.cowshed`/`services.cowshed`, 14_nix.md), imperatively applied otherwise — and both dedicated volumes
   exist: lazily create and mount `cowshed.store` (at `~/.cowshed`) then `cowshed.caches` (nested; ordering and the
   volume marker in 01_storage.md) before any image is created.
2. Select the supported format, then create the image under a staged, non-enumerated, format-specific name:
   `<owner>/<repo>/.staging/main.asif` for ASIF or `<owner>/<repo>/.staging/main.sparseimage` for SPARSE. Both
   components come from the validated primary `repo_id` and are encoded independently as specified in 01_storage.md.
   Create its complete sibling host sidecar with the matching `imageFormat` before the first attach; the readdir
   registry never sees staged objects. Attach at a staging mountpoint, refusing any extension/metadata mismatch as
   specified in 01_storage.md.
3. Copy the full tree (including `.git`), preserving metadata, in delta passes until quiescent: re-run rsync-style delta
   copies while the source keeps changing; refuse (exit 4) if the tree fails to quiesce within the pass budget, naming
   the churning paths.
4. Write `.cowshed/workspace.json` (`role: "main"`), mint `.cowshed/token`, mint main's per-workspace CA (private key
   controller-side next to the grant file; CA cert placed in-image as a trust anchor with the tool anchors wired —
   04_sandbox.md/05_gateway.md), create in-image cache roots, and write platform endpoint plus shared-cache wiring into
   tool config files (03_caches.md). The main sidecar contains `portBlock` only on macOS; Linux omits it and creates its
   per-incarnation socket/netns connector when the published workspace is attached. Verify the copied tree against the
   source before publication.
5. Publish: move the original tree aside (`<root>.pre-cowshed`), recreate the emptied directory as the mountpoint with
   the self-healing stub `.envrc` inside it, rename the staged image and each sibling sidecar into place without
   changing the format extension, `fsync` the parent directories around each rename, attach.
6. Print the mount path on stdout.

Every step is idempotent to re-run. `cowshed doctor`/`cowshed gc` recognize each crash point — staged image present,
tree moved but unattached, marker unpublished — and resume or roll back. `<root>.pre-cowshed` is retained until the user
deletes it; cowshed never auto-deletes it.

Adopting is reversible: `cowshed rm main --restore` detaches and moves `<root>.pre-cowshed` back.

## `cowshed new <name>` — create a session workspace

Budget: ≤ 1 s cold. No pool, no pre-warming.

1. `flock` main's image lock; `fsync`/`sync` the main volume. Measured: the sync is for **freshness, not consistency** —
   a live clone is always crash-consistent, but without a sync it can miss the last writes (a just-written file was
   absent from a non-synced clone).
2. Preserve the source format and extension: `clonefile(main.<ext>, sessions/<name>.<ext>)`, where `<ext>` is `asif` or
   `sparseimage` according to main's validated detached `imageFormat` — ~2 ms regardless of content size. Create the
   complete closed-baseline sibling sidecar before attach; allocate `portBlock` only on macOS and omit it on Linux.
3. Attach without mounting, run `fsck_apfs -q` against the clone's APFS volume device, then mount at
   `~/.cowshed/mnt/<owner>/<repo>/<name>` — extension and detached `imageFormat` must agree, then attach dispatches to
   `diskutil image attach --noMount` for ASIF or `hdiutil attach -nomount` for SPARSE (flags per 01_storage.md) —
   ~235–400 ms typical. Verification precedes the first mount; a clone never mounts unchecked.
4. On fsck failure, delete the clone and retry once from a fresh sync. (Measured: 10/10 clonefiles taken under a
   continuous writer plus a streaming 128 MiB dd passed both `fsck_apfs -q` and a full `-n` check, mountable and
   readable, on both formats — this path is a safety net that is expected to essentially never fire; the fork-mid-write
   integration test pins it, 08_testing.md.)
5. Rewrite `.cowshed/workspace.json` (`role: "workspace"`, `baseCommit` = main's HEAD), mint a fresh `.cowshed/token`,
   mint a fresh per-workspace CA (private key controller-side next to the grant file; CA cert placed in-image as a trust
   anchor with the tool anchors wired — 04_sandbox.md/05_gateway.md), and complete platform wiring. On macOS, use the
   sidecar's allocated contiguous 16-port `portBlock` from the reserved range (default `40960–49151`): the gateway binds
   `base`, and `base+1 … base+15` are workspace service ports. On Linux, omit `portBlock`; after the dataset is mounted,
   create and bind-mount the per-incarnation Unix gateway socket and launch the trusted connector inside the workspace's
   private netns on `127.0.0.1:7644` (04_sandbox.md/05_gateway.md). Mark `<mount>/.envrc` direnv-trusted. In-image Bun,
   Cargo, Go, and proxy wiring uses the platform endpoint, and tool shims are placed at `.cowshed/bin/`; there is no git
   network wiring — workspace git is local-only (see "Remote code ingress").
6. Inside the mount, under the workspace's closed sandbox: `git remote add host <project-root>` (idempotent),
   `git switch -c cowshed/<name>` from the checked-out state. The `.git` directory arrived complete via CoW — the
   workspace is a standalone repository with **no linked-worktree registration and no back-references** into the host
   checkout.
7. Publish
   `ControllerCommitment::Fork(ForkCommitment { version, order, repo_id, source_incarnation, destination_incarnation })`
   before the new workspace becomes discoverable.
8. Print the mount path on stdout; guidance and `next:` hints on stderr.

Flags: `--ref <rev>` (after branching, `git switch -c cowshed/<name> <rev>` instead of main's state),
`--from <workspace>` (clone a session instead of main — sugar over `cowshed fork`), `--browse`.

## `cowshed fork <src> <dst>`

Clones a _session_ mid-flight with the same barrier/fencing as `cowshed new`, preserving the source image's validated
format and extension (`sessions/<src>.asif` → `sessions/<dst>.asif`, or `.sparseimage` → `.sparseimage`). The marker
records `forkedFrom`, and before destination publication the controller appends
`ControllerCommitment::Fork(ForkCommitment { version, order, repo_id, source_incarnation, destination_incarnation })`.
Grants do **not** carry over: the fork starts closed, with a fresh CA and platform endpoint identity. macOS allocates a
new `portBlock`; Linux leaves it absent and creates a new per-incarnation socket/netns/ connector. Neither inherits the
source endpoint or CA.

## `cowshed checkpoint <ws> [label]` / `cowshed restore <ws> <label>`

- Before pausing the supervisor or cloning, cowshed reads the target workspace's coordinator-owned `CheckpointQuota` and
  authoritative substrate stats. The projected count is its existing published checkpoint count plus one. The projected
  bytes are its existing published checkpoint allocated bytes (pinned and automatic) plus the active image's
  conservative allocated-byte charge. Other workspaces are isolated from this cap. Exact boundaries are allowed; an
  over-boundary projection is `Conflict` with no checkpoint image, fact, detached-metadata, or barrier publication.
- Checkpoint is a supervisor/filesystem barrier. Under the lifecycle lock, cowshed pauses admissions and artifact
  writes, promotes every running memory-only stream prefix, and fsyncs protected files. It then appends and fsyncs
  exactly
  `ProtectedRecord::CheckpointManifest(CheckpointManifestRecord { version, repo_id, origin_incarnation, barrier_id, visible_jobs, records_sha256 })`.
  `barrier_id` is positive and monotonic within `origin_incarnation`; `records_sha256` hashes the complete
  protected-record stream prefix immediately before the manifest batch. `visible_jobs` contains
  `VisibleJobCommitment { workspace_incarnation, job_id, state, stdout, stderr }`; each stream commitment is
  `{storage_kind, bytes, sha256, protected_path}` with `storage_kind` exactly
  `captured-inline|captured-file|redirect-inline|redirect-file` and `protected_path` present iff file.
- With writes still quiesced, cowshed syncs for filesystem freshness and clones the image. The controller atomically
  publishes
  `ControllerCommitment::Checkpoint(CheckpointCommitment { version, order, repo_id, origin_incarnation, checkpoint_id, barrier_id, manifest_batch_sha256 })`,
  then resumes. `manifest_batch_sha256` is the recovery frame's digest for the complete manifest batch. The manifest
  defines every checkpoint-resident byte: terminal inline data is in a prior protected Job record covered by
  `records_sha256`; every running prefix is an fsynced protected file. Complete batches/sealed files are immutable;
  recovery may discard only an incomplete trailing frame while retaining and reporting its `batch_sha256`. Committed
  digest/count mismatch is `Integrity`.
- Restore follows the substrate transaction in 09_substrates.md: lock and revalidate the old incarnation/revision; stop
  admissions and drain the supervisor and, on Linux, the connector; detach; prepare and verify a non-enumerated
  same-format clone; validate its protected checkpoint manifest against the controller checkpoint commitment; mint and
  write the fresh `workspaceIncarnation` **before** minting its fresh token; atomically swap the clone into the
  canonical name; attach and validate; on Linux create the fresh per-incarnation socket and connector. At the same
  atomic publication boundary, append
  `ControllerCommitment::Restore(RestoreCommitment { version, order, repo_id, source_checkpoint, source_incarnation, destination_incarnation })`,
  publish detached metadata, and switch gateway acceptance from the old endpoint/token to the new; only then admit the
  new supervisor. A missing commitment/artifact, hash/count/batch-digest mismatch, or malformed complete frame fails
  restore as `Integrity`; cowshed never chooses the newer-looking side. The logical workspace identity, primary
  `repo_id`, CA, and grant binding carry through; a macOS `portBlock` carries through only when present, while Linux has
  none. The displaced image becomes `checkpoints/<ws>/pre-restore-<destination_incarnation>.<ext>` with its original
  extension, so restore is undoable. The checkpoint source incarnation, the replaced active incarnation, and the fresh
  destination incarnation are distinct roles; on repeated restore the checkpoint source can differ from the replaced
  active generation. Before publication, failure restores the displaced workspace and old token; after the incarnation
  fence is published, recovery completes forward and never exposes both tokens. Copied job records retain the
  incarnation that produced them, while the new incarnation allocates workspace-local monotonic numeric job IDs above
  every inherited allocation. Restore restart recovery reads only substrate facts: the canonical/pending detached
  metadata, the destination-keyed displaced image/grant/CA sidecars, and the exact sibling `.restore.json` recovery fact
  described in 01_storage.md. Before durable pending-metadata publication it restores the old generation; afterward it
  leaves the new generation pending for idempotent controller-commitment publication and activation. Repeating startup
  recovery and activation cleanup has the same result. The project runtime persists no restore fence, journal, or
  `.restore-fences` path.

- `cowshed gc` retains every pinned checkpoint, every checkpoint younger than 14 days, and always the newest five per
  workspace. A supplied label and `--keep` both create explicit pins; only an explicit unpin makes them eligible.
  `CheckpointOptions.keep` carries the same pin request for programmatic callers. `WorkspaceInfo.checkpoints` always
  projects the canonical `{label, revision, pinned}` facts (an empty array when none exist), so detached workspaces do
  not need a mount or cached marker to report their retry points.

### Checkpoint before writer handoff

Before write responsibility passes from one writer to another, the current writer creates a checkpoint after stopping or
quiescing its workspace writes. The checkpoint barrier also makes every supervisor-resident captured byte durable and
publishes the protected manifest/controller commitment pair described above. That checkpoint is the recovery boundary
for the handoff and includes the complete live workspace state, including committed and uncommitted files, plus exactly
the job evidence committed by its manifest. The next writer may receive a fresh capability for the same attached,
writable workspace, or the handoff may create a live copy-on-write fork from that workspace. A fork receives its own
identity and closed-baseline grants as described above; the source and its checkpoint remain recoverable until the fork
is accepted or independently preserved. A failed writer can therefore be replaced on the same workspace, or its attempt
can be abandoned in favor of the validated checkpoint or an unaffected fork, without conflating handoff with publication
or retirement.

## `cowshed push <ws> [--branch <name>]`

Preserves a completed workspace branch in the **main workspace's standalone repository/object store**. This is a local,
pull-based Git operation; it does not switch or update the checked-out Git `main` branch and does not modify the main
workspace's index or working tree. The default source is the workspace's `refs/heads/cowshed/<ws>` branch. A validated
`--branch` selects another local source branch. The destination is the non-checked-out namespaced ref
`refs/cowshed/<ws>/heads/<branch>` in the main workspace repository.

The controller reads from the source workspace using host-side Git with the main workspace repository as the receiving
repository, `--no-write-fetch-head`, and no checkout or merge. It fetches the selected source into a temporary ref,
verifies the fetched object ID, then atomically installs that exact object ID at the destination ref and removes the
temporary ref. The receiving command never uses the source workspace's Git configuration or hooks. Updating an existing
destination is deliberate preservation of a newer snapshot, not a working-tree operation.

`push` accepts three independent optional compare-and-swap preconditions: the expected source workspace incarnation, the
expected source branch head object ID, and the expected destination ref head (either a specific object ID or missing).
Under the destination-ref lock, cowshed validates every supplied expectation against authoritative host metadata and the
fetched source. Any mismatch returns `Conflict`, reports which expectation moved, leaves the destination ref unchanged,
and retains the source workspace. Omitting an expectation requests the corresponding unconditional behavior; callers
that may race or retry should supply all three.

Success returns the source object ID and destination ref and guarantees that the destination resolves to that exact
object ID. Once every unique commit that must survive is reachable from this durable namespaced ref (or another
non-temporary ref in the main workspace repository), the source workspace may be retired. Merely transferring objects,
leaving them dangling, or updating `FETCH_HEAD` is not preservation. Remote forge publication, pull-request mutation,
workflow policy, and scheduling are outside Cowshed; another trusted host-side component may publish the preserved ref
without changing these local semantics.

## Autosave

A per-project launchd user agent (installed at adopt; systemd user timer on Linux) snapshots every _attached_ workspace
into the host repository on a 10-minute cadence (`.cowshed.toml` `[autosave] interval`). The mechanism is a host-side
fetch — the workspace branch plus `+HEAD:refs/cowshed/<ws>/wip` — the same pull-based direction as `cowshed push`, for
the same trust reason. `refs/cowshed/…` (singular) is the canonical namespace; `land` prunes a workspace's refs at
retire. Each snapshot is gated by the secrets delta-scan (see "Secrets enforcement"): a finding skips that workspace's
snapshot with one `cowshed:` warning and never blocks work. Uncommitted work between autosaves is the only loss window —
images are excluded from backup by design (01_storage.md).

## Remote code ingress: `cowshed repo`

Sandboxed git speaks only local paths. A workspace's remotes are the `host` remote (main's mount) and read-only bare
mirrors on the caches volume — never a network URL. There is no git endpoint on the gateway data plane, no credential
helper, and no `insteadOf` rewriting inside any workspace.

- **`cowshed repo mirror <url>`** — a control-plane RPC, not workspace git. The gateway checks the workspace's repo
  grants (`cowshed grant <ws> --repo github.com/org/*` — repo-scoped, finer than host egress), executes the fetch itself
  with Keychain-held credentials into a bare mirror it owns at `~/.cowshed/caches/repo-mirrors/<host>/<org>/<repo>.git`,
  writes one audit line, and returns the mirror path on stdout. Mirrors are created by the gateway with its own config —
  no agent-writable git config is ever in the loop — and are fetch-only, sandbox-read-only (01_storage.md,
  05_gateway.md).
- **`cowshed repo clone <url> [dir]`** — sugar: mirror, then a local `git clone --dissociate <mirror>` run inside the
  sandbox.

The project's own origin is just another mirror — mirroring it covers "the agent needs `refs/pull/123/head`". GitHub API
operations (creating PRs, issues) are coordinator work in v1: they require credentials and therefore live outside the
sandbox by construction.

## Artifact egress: `cowshed sim export` / `cowshed app export`

The outbound sibling of `repo` ingress, for posture B (14_nix.md). `cowshed sim export <ws> [artifact]` copies a built
iOS `.app` to the one-way drop dir (`<shared-drop-root>/<owner>/<repo>/`, using the primary path-safe `repo_id`), where
the personal session installs it into the human's simulator; `cowshed app export <ws> [artifact]` is the Mac-target
sibling. Either way an immutable artifact crosses the uid boundary, never a live tree; semantics, gating, and the
`--sim` grant class live in 14_nix.md / 04_sandbox.md / 05_gateway.md. The lane-3 counterpart `cowshed app promote` is a
**personal-session human verb** (it installs a promoted build into `~/Applications`), documented in 14_nix.md — not a
workspace or sandbox operation and not agent-invokable.

## Return to an integration branch: `cowshed rebase` and `cowshed land`

Workspaces are born from the main workspace's Git state and return to a controller-selected integration branch.
`new`/`fork` are the first half of the lifecycle; `rebase` and `land` are the second. Between them, autosave refs
(fetched host-side to `refs/cowshed/<ws>/wip` — see "Autosave") remain the crash-safety net. Cowshed provides the
primitives; retry, scheduling, review, remote publication, and branch-selection policy are coordinator work (12_mcp.md),
deliberately not baked in.

### `cowshed rebase <ws> [--onto <branch-or-ref>]`

Brings a workspace's branch up to the selected host revision. The default is `main`; `--onto` accepts a validated local
branch or fully qualified ref in the main workspace repository. An object ID may be used as an immutable replay base,
but `land` still requires a named branch as its fast-forward target.

- Default mode: fetch the selected host revision inside the workspace, then rebase the `cowshed/<name>` branch onto that
  exact object ID. The whole step runs **inside the workspace sandbox** — the fetch from the main workspace mount is a
  read (covered by the baseline), and every write lands in the workspace's own repository. Conflicts abort the rebase
  (`git rebase --abort`), leave the workspace exactly as it was, and exit 4 with `next:` hints naming the conflicted
  paths.
- `--fresh`: divergence-shedding rebase. Create a new clone from the selected current host revision, replay the branch
  onto it (`git format-patch`/`am`, or cherry-pick range), then **transplant identity** — the new clone inherits the
  workspace's name, canonical mount path, token, and grant-file binding; the old clone is destroyed (11_shell.md
  teardown ordering). This drops accumulated substrate divergence (ZFS origin-snapshot pin, APFS image growth —
  01_storage.md, 09_substrates.md) and re-warms in-image caches to the selected base. A replay conflict leaves the
  **original** workspace untouched and discards the half-built fresh clone (exit 4). Refuses (exit 4) when the workspace
  has uncommitted changes — replay carries commits only; commit or discard first. `--fresh` is what a coordinator runs
  against a long-lived workspace that `cowshed du` shows has drifted far from its integration base.

### `cowshed land <ws> [--target <branch>] [--check <cmd>]`

The full born-from-host-return-to-host close-out, as one primitive. The target defaults to `main`:

1. **Rebase** onto the current target branch object ID (the `cowshed rebase --onto <target>` step above; conflict → exit
   4, workspace intact).
2. **Validate**: run the check _inside the sandbox_ — `--check <cmd>` if given, else `.cowshed.toml` `[land] check`,
   else no validation with one honest `cowshed:` stderr line saying so. Non-zero check → exit 4, workspace intact,
   output captured as a job (11_shell.md) for diagnosis.
3. **Fast-forward the target branch under its repository lock**: fetch the exact validated source head from the
   workspace mount, revalidate the source workspace incarnation, source head, and expected target head, then require a
   fast-forward. How the update is materialized depends only on checkout state:
   - If the target branch is checked out in the main workspace, run the fast-forward **through that checked-out
     workspace**. Its branch ref, `HEAD`, index, and working tree all advance to the validated source tree. This is the
     normal `main` case; Cowshed must not update only a hidden or non-checked-out ref while leaving the visible main
     workspace stale. The main workspace must be clean enough for the update or `land` refuses with exit 4 and a `next:`
     hint to commit or stash.
   - If the target branch is not checked out, atomically compare-and-swap `refs/heads/<target>` to the validated source
     head without changing the main workspace's currently checked-out branch, `HEAD`, index, or working tree.
   - If the target is checked out by any other linked worktree unknown to Cowshed, refuse rather than leave that
     worktree's index and files stale. If the target advanced during validation, any expected head changed, or the
     update is not a fast-forward, return `Conflict`, leave both source and target intact, and report the moved value.
     Cowshed never retries against a new base internally; the coordinator decides whether to rebase and re-run checks.
4. **Retire**: only after the target branch and, when checked out, its visible working state resolve to the validated
   source head, destroy the workspace (supervisor tree first — 11_shell.md) and prune its `refs/cowshed/<ws>/*`
   preservation refs on the host.

`--no-retire` keeps the workspace after a successful land (for a coordinator that wants to reuse it via
`rebase --fresh`). `--push-only` performs steps 1–2 and installs the validated head in the workspace's durable
`refs/cowshed/<ws>/heads/<branch>` preservation ref without advancing the target branch, for review-gated flows.

## `cowshed rm <ws>` — perceived-instant deletion

1. Refuse (exit 4) when the branch has commits absent from the host repository, unless `--force` — the unsaved-work
   safety net.
2. Stop the supervisor: TERM → grace → KILL across the whole descendant tree (11_shell.md). Teardown precedes retirement
   — live children would otherwise hold the mount busy and keep enforcing stale launch-time authority after the grants
   disappear.
3. Logically retire: preserve the image extension while renaming it to `sessions/.trash/<ws>.<ts>.asif` for ASIF or
   `sessions/.trash/<ws>.<ts>.sparseimage` for SPARSE, and remove the grant file and the controller-side CA private key
   — the workspace disappears from enumeration; the command returns here (typically well under a second; a stubborn
   process tree delays it by at most the kill grace).
4. Background (spawned detached): detach the mount (escalating to `-force` after a 10 s grace), unlink the trashed image
   and any checkpoints, remove the mountpoint dir. Interrupted cleanup is resumed by `cowshed gc` (idempotent).

`cowshed rm main --restore` is the adoption rollback described above and maps to `RemoveOptions.restore`; plain
`cowshed rm main` requires `--force` and a clean `git status` (exit 4 otherwise).

## `cowshed attach` / `cowshed detach`

Explicit attachment lifecycle for long-lived workspaces: `detach` first stops admissions and drains the supervisor and
Linux connector, unlinks the Linux per-incarnation socket, then frees the attachment; the image/dataset and grants
persist. `attach` mounts at the canonical path with canonical flags and, on Linux, creates the per-workspace
socket/netns and starts exactly one connector before admitting execs. Personal macOS workspaces are typically attached
at login by a launchd agent; Linux uses its platform service/controller lifecycle.

## `cowshed ensure` — the .envrc fast path

Contract: **silent exit 0 in ≤ 25 ms when healthy**. Resolution asks the bound controller to re-read canonical storage,
detached metadata, and kernel mount facts. The canonical cwd must be contained in exactly one active mount for that
project; nested paths resolve, while detached, ambiguous, cross-project, and marker-only paths fail closed. The marker
is validated only after mount authority is established and never grants a capability by itself.

When something is wrong, `ensure` fixes what is safe to fix synchronously:

- image exists but not mounted (reboot, eject) → re-attach and continue;
- mounted with wrong flags or at the wrong path → re-mount;
- `cowshed.store` or `cowshed.caches` volume missing or mounted with wrong flags → recreate (lazy, idempotent) and
  re-mount with canonical flags, in order: store at `~/.cowshed` first — the other mountpoints live on it, and the
  volume-root marker `.cowshed-volume.json` absent means "unmounted", never "empty" (01_storage.md);
- broken in-image cache config → rewrite (03_caches.md).

It warns (one stderr line each, never blocking) when the gateway is unreachable or the workspace has old unpushed work.
It never does slow or surprising work — no fetch, no compaction, no refresh.

`cowshed ensure --envrc` additionally prints exactly the load-bearing exports represented by the authoritative
`EnsureReport`: `GOENV` points at the mounted workspace's `.cowshed/cache/go/env`, `COWSHED_WORKSPACE_TOKEN` contains
the value read from the mounted workspace's controller-minted `.cowshed/token`, and macOS additionally exports
`COWSHED_PORT_BASE` from the exact `portBlock` in detached metadata. Linux metadata has no port block and emits no
sentinel value. The CLI never derives these paths from its cwd, guesses a port from a slot, or trusts a marker to choose
a workspace. Usage in a repo's `.envrc`:

```bash
eval "$(cowshed ensure --envrc)"
```

**Self-healing stub**: the underlying (shadowed-when-mounted) mountpoint directory contains a one-line `.envrc` —
`cowshed ensure --attach` — so `cd` into an unmounted workspace triggers direnv, cowshed re-attaches, and the real
`.envrc` shadows the stub on the next prompt. Every `cd` is a repair opportunity.

## Tradeoffs

**Templates rejected.** A template registry (SHA-keyed images, refresh pipelines, promotion, rollback) reproduces CI
inside a workspace manager and demands configuration. Main-as-base gives a fresher starting state for free: main is warm
because it is used. The secrets invariant is what makes this safe; it is enforced, not assumed.

**Linked git worktrees rejected.** They couple every workspace to the host `.git/worktrees/<id>` (sandbox holes, stale
registrations, invalid when cloned). A standalone `.git` arrives via CoW at zero marginal cost, and the pull-based
`cowshed push` replaces shared-state semantics with an explicit, auditable hand-back.

**Gatewaying git rejected — workspace git is local-only.** A smart-HTTP credential broker on the gateway would let
sandboxed git reach real remotes, reintroducing in-sandbox push, a git wire protocol to proxy faithfully, and
host-granularity policy that git defeats: Anthropic's sandbox-runtime README warns that allowlisting github.com lets a
process push to any repository (https://github.com/anthropic-experimental/sandbox-runtime), and yolo-cage had to build
an entire git-command classifier to police in-sandbox remote git (https://github.com/borenstein/yolo-cage). Local-only
git removes the class: ingress is a gateway-executed mirror fetch (`cowshed repo`), egress is a controller
fetch-from-workspace (`cowshed push`), and no credential or remote URL ever exists inside a sandbox.

**hdiutil shadow files rejected.** Shadows share only a frozen base (identical to what clonefile already provides),
measure 2.3× slower on synchronous writes, and pin their base image against deletion. Clones have no runtime dependency
on their source — main's image can be replaced or compacted freely.
