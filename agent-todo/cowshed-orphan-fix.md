# Cowshed orphan fix: parked NACK

Review custody: coder shed `cowshed-orphan-fix`, fix `f1691b0fb4940dd46cbf9ed9d8f7b3301db85789` on original `54a42b051`,
base `65597b49a`. Main parks this unit; it is not accepted for landing. No repair is made by this note.

1. **Known non-APFS volumes are missed.** `packages/cowshed/crates/cowshed-core/src/orphaned_project.rs:49–55`
   implements `SystemVolumes::registered` using only `registered_apfs_containers`. A system-known but unmounted
   HFS+/ExFAT volume is outside that inventory, so this probe can authorize abandonment despite a way back. This is
   source-traced, not a real-volume reproduction. Governing contract: `specs/cowshed/02_workspaces.md`
   §`cowshed rm <ws>`, lines964–971: `--abandon` is the caller's declaration, but any still-registered named volume or
   unreadable registry refuses.

2. **A missing attachment alias permits purge.** `packages/cowshed/crates/cowshed-core/src/orphaned_project.rs:745–761`
   converts ENOENT/ENOTDIR for an attached backing path into `identity = None`. If its symlink alias was removed after
   attachment, the registered path differs from the surviving canonical image and no inode comparison succeeds;
   lines767–768 allow deletion. This is source-traced, not an executed APFS reproduction. The existing alias regression
   at1372–1394 keeps the alias present and does not cover this case. Governing contracts:
   `specs/cowshed/02_workspaces.md` §`cowshed rm <ws>`, lines977–979 (attachment under any alias refuses), and
   `specs/cowshed/01_storage.md` §Images, lines399–410 (exact image identity and leases).

3. **`project_runtime` behavioral proof is outstanding.** The fixer's report (`history://OrphanFix2Opus`) records all
   nine selected tests failing before test code because the exec sandbox denies `/private/tmp`.
   `packages/cowshed/crates/cowshed-core/tests/support/temp_root.rs:5–24` fixes the parent to `/tmp`; changing TMPDIR
   cannot redirect it. The known failure is not rerun. Per Main's ruling, this proof runs host-side through its owning
   Nx bounded-exec target, not raw host Cargo or another sandbox-denied attempt. Governing authority:
   `specs/cowshed/08_testing.md` §Host-controller authority, lines94–136; `specs/cowshed/04_sandbox.md` §Layered
   Sandbox, lines3–9 (an inner profile cannot widen the enclosing supervisor). No host-side run is performed in this
   review.

Measured on the reviewed fix through `cowshed exec cowshed-orphan-fix`: core orphan/inventory/terminal-cleanup suite
**48 passed**; CLI setup-service suite **57 passed**; CLI args/runtime suite **89 passed**. N-API parity is not rerun
before parking. `retained/` remains outside the purge plan, as required by `specs/cowshed/02_workspaces.md`
§`cowshed rm <ws>`, lines980–987. Green suites do not discharge the two source-traced safety findings above.
