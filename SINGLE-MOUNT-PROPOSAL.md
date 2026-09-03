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

Verdict up front: **(b) wins; (a) is rejected.** The implementation now uses a canonical
`PendingFence` clone and a single normal-path attach/mount.

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

## 3. Shape (b) resume — IMPLEMENTED

The lifecycle intent is durable before storage mutation. Clone metadata is written at the
canonical sidecar before the payload appears, with `PendingFence` keeping the destination out of
ordinary enumeration until initialization and post-callback validation finish. Activation is one
atomic sidecar rewrite from `PendingFence` to `Active`; the mounted attachment is then retained.

| Kill window | Durable state | Recovery action and guard |
|---|---|---|
| Before pending metadata | Intent only | Re-run create/fork normally. |
| After sidecar, before complete clone | Sidecar-only, or failed clone | Clone failure reclaims both artifacts; startup recovery removes a sidecar-only record. No partial payload is admitted as resumable. |
| After clone, before attach | C + PendingFence | Reuse the metadata incarnation and original operation identity; never clone over C. |
| After attach, before mount | C + PendingFence + unmounted attachment | Exact inventory match is detached and settled, then the ordinary verified attach/fsck path is repeated before mounting. |
| After mount, during image-local preparation | C + PendingFence + canonical kernel mount | Exact source-device and canonical mount flags reuse the existing attachment; ambiguous or foreign mounts fail closed. Rename and credential publication are idempotent. |
| During initializer | Same pending mounted C, possibly with external Git effects | Cancellation and initializer errors preserve C and the mount. Retry receives explicit `stage.resuming` authority. Git branch/worktree state machines continue only their exact cowshed-owned branch and registration. |
| After marker write | Same pending mounted C | An already-current marker is validated rather than rewritten, preventing lineage self-duplication. Environment wiring, inherited-link repair, daemon discard, and remote configuration are check-first or deterministic. |
| After callback, before/during activation | Pending or Active sidecar (atomic rewrite) | Pending reruns callback and validation; Active is listed as current. No intermediate publication state exists. |
| After activation, before downstream effects or intent completion | Active C + pending lifecycle intent | Startup recovery reruns optional main registration and append-safe commitments, then records exact completion. Slot binding is retained across errors and exact-owner rebinding is a no-op. |
| After completion | Active C + completed intent | Re-issue returns the recorded incarnation; supervisor startup remains start-or-reuse. |

The normal path has no staging mount, detach, image rename, canonical reattach, or remount. The
detach/settle leg exists only when recovery finds a crash-left *unmounted* attachment whose fsck
completion cannot be proved. A surviving exact canonical kernel mount is reused without churn.

Initializer rerunnability audit:

| Step | Replay rule |
|---|---|
| `discard_in` | NotFound is success; deletion is deterministic. |
| environment hook | check-first, with an append-open recheck. |
| inherited remotes/links and local `main` remote | empty-loop/deterministic rewrite plus ownership-checked configuration. |
| standalone branch | fresh calls reject collisions; storage-authorized resume reuses and checks out only `cowshed/<name>`. |
| linked-worktree registration | exact admin/pointer states continue through staging-pointer relocation and `worktree repair`; mixed or foreign states fail closed. |
| marker | exact current marker validates and skips rewriting. |
| credentials | atomically overwritten while still unpublished. |
| slot | exact same workspace/slot bind is idempotent and the binding remains owned by the pending intent. |
| optional registration, commitments, intent completion, supervisor | set/check-first, append-safe telemetry, overwrite-safe completion, start-or-reuse. |

The remaining live-system proof is intentionally deferred by the no-disk-experiments order:
confirm `hdiutil info` reports the image path with the same exact spelling supplied to attach, and
run real-APFS crash injection when storage experiments are permitted. RecordingRunner and
filesystem-only Git tests cover the implemented state transitions without touching disk images.

No volume-UUID changes are involved: there is no public verb to regenerate one, and this change
does not introduce one.

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
