# Single-mount restructure: crash-window analysis (resume vs rollback)

Goal: eliminate the staging-detach → canonical-attach DA churn behind the mount hang by
mounting once, while preserving crash-atomicity. Two candidate shapes evaluated against the
existing recovery machinery (staging-orphan GC in `preview_gc_project`, sidecar-first
`recover_pending`, mount-on-demand healing in `ensure_mounted`):

- **(a) rollback**: clone to canonical, mount once at canonical, run init; on init
  *failure* detach + delete the image. Prove there is no corpse window.
- **(b) resume**: clone to canonical, mount once at canonical, run init as individually
  re-runnable steps guarded by marker expectations + the lifecycle-intent state machine; a
  killed `new` leaves a marked-incomplete workspace that the next touch completes (or GC
  reclaims). Prove every step re-runnable.

Verdict up front: **(b) wins; (a) is rejected.** Neither is implemented in this branch —
see sequencing at the end.

## 1. Current two-mount crash windows (the bar to preserve)

Create/fork today: clone S → metadata → attach S → mount sM → init (callback, lock held) →
validate → detach S → publish S→C (sidecar-first renames) → attach C → mount cM →
validate → retain. SIGKILL at each point:

| Window | State at kill | Coverage |
|---|---|---|
| K1: before `publish_metadata` | S partial, or whole w/o sidecar | Staging-orphan GC (`OrphanStagingImage`, lock free) |
| K2: after MD(S), during attach/mount/init-steps or init callback | S+MD(S), maybe sM mounted | Same GC + `retire_staging_mount` detaches sM by mountpoint |
| K3: commit, during staging detach | S+MD(S), attached or not | Same as K2 |
| K4a: after detach, before canonical-sidecar rename | S+MD(S) | Orphan GC |
| K4b: after sidecar rename, before image rename | MD(C) w/o C | `recover_pending` completes from S (retained via `recoverable_staged_stems`) |
| K4c: after image rename | C+MD(C), unmounted, init complete | Claimed by `list`; healed onto a mount by `ensure_mounted` on next touch |
| K5: during `mount_canonical` | C mounted at cM, registry lost | Kernel mount re-derived by `mounts()` + `validate_kernel_mount`; `detach_mounted` fallback detaches by mountpoint |
| K6: after retain | Live workspace | Re-issue → plan conflict (correct: it exists) |

Unclaimed-image invariant (current): a canonical image exists ⟹ its sidecar exists
(sidecar-first order), and GC never names canonical paths. No corpse window. Staging
exists precisely so that *every* pre-publication crash is namespaced garbage.

Verb-level resume already exists (`recover_lifecycle_intents`: pending Create/Fork intent
+ workspace NotFound → re-run `create()`/`fork()`), and it is safe today *because* the
crashed attempt touched only staging. Any single-mount shape must preserve that property
with canonical state instead.

## 2. Shape (a) rollback — REJECTED

Crash-during-init leaves a canonical corpse (C+MD(C), maybe mounted, init half-done).
Covering it needs: (i) a fence so the corpse is not listed as live (else a visible ghost
the user can `cd` into), (ii) a corpse-reclaim rule in recovery (canonical PendingFence
create-image + uncompleted intent → detach + reclaim), and (iii) compensation for init's
**effects outside the image**, which image deletion cannot undo:

- `prepare_workspace` / `adopt_as_linked_worktree` create branch `cowshed/<name>` in
  **main's** ref namespace and register the worktree in **main's** admin dir
  (`src/git.rs`). Deleting the image orphans those registrations.
- Compensating (delete branch, unregister worktree) is itself crashable → a saga, more
  machinery than the alternative — and deleting a branch that acquired user commits
  between crash and reclaim is data loss. Resume never deletes user-reachable state.

(a) also has a good-workspace-deletion window: crash after init success but before intent
completion → reclaim deletes complete work. Verdict: (a) cannot close its windows without
a fence + corpse rule + compensation saga. Rejected.

## 3. Shape (b) resume — VIABLE, with named guards

Resume re-runs init to completion instead of deleting. Main-namespace effects become
continue-in-place rather than compensate. Audit of every init step for re-runnability
(create/fork initialize closures in `src/runtime/project.rs`, substrate prepare steps for
a canonical-path flow, post-commit steps):

Re-runnable as written:

| Step | Why re-running is safe |
|---|---|
| `discard_in` (daemon state) | NotFound → Ok; deterministic on content |
| `ensure_workspace_environment_wiring` | check-first, plus re-check inside the append open |
| `configure_main_remote` | documented idempotent (ownership-record based) |
| remote-strip loop, `restore_inherited_links` | empty loop / deterministic rewrite → no-op |
| `register_workspace_remote` (`set_remote`) | add-vs-set-url check-first |
| `rename_volume` to same label | idempotent |
| `validate_marker`, `validate_staged_companion` | read-only |
| `mint_workspace_credentials` (`publish_asset` atomic overwrite) | overwrite safe pre-publication (nothing external holds the crashed run's keys) |
| `commitments.record` | telemetry; replay duplicates gate nothing |
| `complete_lifecycle_intent` | completion overwrite |
| `ensure_supervisor` | start-or-reuse |
| port-block claims (`claim_port_block`) | stale (dead-pid) markers reaped on next claim |
| `release_slot` on error paths | idempotent |

NOT safely repeatable as written — the deciding list (each needs a guard):

1. **clone to existing destination** — `clonefile` fails if the target exists. Needs:
   destination-exists check-first (skip when a prior clone completed; reclaim-then-clone
   when torn — torn detection needs a rule, e.g. sidecar presence + fsck).
2. **attach of an already-attached image** — a second attach mints a duplicate device
   instead of reusing. Needs: inventory check-first reusing the live attachment.
3. **mount onto a busy mountpoint** — crashed run may still hold cM. Needs: kernel-mount
   check-first (`heal_mount` shape: mount present + marker valid → skip).
4. **branch already exists** — `ensure_workspace_branch_absent` (`src/git.rs`)
   conflicts when `switch -c` already ran. Needs: branch-present → adopt-and-continue
   (verify it names our start point, then skip creation).
5. **worktree admin already registered** — `adopt_as_linked_worktree` conflicts on
   `admin.exists()`, and a stale staging dir vs git's registration table needs
   prune/repair-first. Needs: registered-to-self → `repair_linked_worktree` + continue.
6. **marker lineage self-duplication** — `write_marker` recomputes lineage from the
   existing marker; a resume read of the first run's marker prepends our own incarnation
   again (`clone_lineage` pushes self). Needs: skip-if-marker-already-current guard.
7. **`bind_slot` on re-run** — verify same-owner bind is a no-op (likely needs a
   bound-to-self check-first; not yet confirmed in `SlotBindings::bind`).

Plus one new signal (small, principled): an **init-completion marker** (extension of the
`freshly_stamped` pattern) recording that init finished inside the image, checked by the
resume entry point — because `recover_lifecycle_intents` currently completes a pending
intent as soon as `current()` finds the workspace, which would bless a half-initialized
tree. With the signal: intent pending + workspace found + init-incomplete → re-run init,
not complete.

## 4. Implementation plan for (b) (when sequenced)

1. Canonical-path prepare: clone-to-canonical (guard 1), metadata, attach (guard 2),
   mount-at-canonical (guard 3), rename, creds, marker (guard 6), validate.
2. Init closure with guards 4–5 (git worktree/branch continue-in-place), slot guard 7.
3. Init-completion marker written + fsynced as the last init step; resume entry in
   `recover_lifecycle_intents` re-runs init when the signal is absent.
4. `retain_mounted` + receipt unchanged; post-commit steps (`register_workspace_in_main`,
   commitments, intent completion, supervisor) already re-runnable.
5. Lifecycle unit tests per window (FakeHost + real git in temp dirs, no DA): kill-points
   K1–K6 equivalents, each asserting resume converges; lineage/marker version handling.

## 5. Sequencing recommendation (why no code in this branch)

- The premise (DA churn causes the hang) still needs live confirmation; the
  detach-settle measurements in this branch are the instrument. Restructuring before the
  trigger is confirmed risks a large, load-bearing diff against a misdiagnosed cause.
- The change touches the most critical path plus a marker-schema-adjacent signal; it
  deserves its own review, not a ride-along.
- Live proof required before landing (b): real-APFS crash injection at each window
  (the CI-excluded `real_apfs_*` suites are the vehicle), duplicate-UUID DA behavior
  under the single-attach flow, and quiet-box vs load hang comparison.

No volume-UUID changes in any shape: there is no public verb to regenerate one, and the
fix must not go near them.

## 6. Considered and rejected: retain-attached-staging through canonical rename

A third shape — attach and mount the *staging* image once, init, then rename S→C files
under the live mount, never detaching — was evaluated and rejected:

- It rests on an untestable-in-this-environment macOS lemma: renaming an attached image's
  backing file while its volume is mounted. DA/FSEvents tolerance is unknown, and the
  `hdiutil info` inventory is keyed by open path, so post-rename the attachment inventory
  goes stale (`attached_capacity` would misread a mounted image as detached).
- Crash-during-init leaves a *staging* image mounted at the *canonical* mountpoint,
  breaking the namespace invariant the staging-mount GC depends on:
  `retire_staging_mount` only retires mounts under the staging dir, so a stale kernel
  mount at cM blocks the retry (busy mountpoint) while the workspace is still unpublished.
  Fixing that needs either renaming the mountpoint itself (another untestable lemma) or
  new GC rules — new machinery either way, unlike publish-before-mount.

Publish-before-mount (shape b) is the recommendation: no mounted file is ever renamed,
and every crash state is isomorphic to a state today's machinery already covers.
