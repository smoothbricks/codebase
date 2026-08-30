# cowshed-core/storage/lifecycle+recovery+audit

Scope: `packages/cowshed/crates/cowshed-core/src/storage/mod.rs` (1040), `recovery.rs` (1297), `lifecycle.rs` (968),
`audit.rs` (637), `audit/macos.rs` (24), `audit/linux.rs` (26), `audit/unsupported.rs` (14), `host_config.rs` (619),
`fstab.rs` (115). Doctrine: `BYPRODUCT-ENGINEERING.md`, `docs/handbook/04-mechanisms.md`, `05-memory-toolkit.md`,
`02-measurement.md` §4.1. Manifest: `packages/cowshed/crates/cowshed-core/Cargo.toml`.

## Summary

- HIGH SSOT: three incomplete verb enums (`Operation` / `LifecycleIntent` / `TransactionKind`) plus two independent
  conflict checkers over the same on-disk facts; `RecoveryModel` is not on the production path.
- HIGH SSOT: `ExpectedState` and `ObservedState` are identical types; `revalidate` is a field-by-field copy compare.
- HIGH SSOT: `rename_noreplace` is copied verbatim in `storage/audit/{macos,linux}.rs` and
  `storage/job_artifact/publication/{macos,linux}.rs`.
- HIGH SSOT: `host_config.rs` restates `/private/cowshed/store` instead of `bootstrap::STORE_ROOT`; the
  default-mount-root branch is keyed on that string.
- MEDIUM SSOT: `CHECKPOINT_NAMESPACE = ".checkpoints"` disagrees with on-disk `checkpoints/` (the other two namespace
  constants match disk).
- MEDIUM SSOT: `RepositoryIdentityIntent` deserializes without `deny_unknown_fields` while every sibling journal type
  requires it.
- MEDIUM SSOT: two UTC calendars in this slice (`civil_from_days` vs `gmtime_r` + `valid_calendar_date`); `pre-restore-`
  is restated as a bare string.
- MEDIUM STRUCTURE: every `unsafe` in the audit fd path lacks a SAFETY invariant comment.
- MEDIUM DUPLICATION: `ArrowAuditSink::seal` reimplements `encode_controller_commitment`'s StreamWriter loop.
- LOW TESTS: `host_config` asserts `Error::to_string()`; `mod.rs` tautologically asserts `WORKSPACE_MARKER_PATH` equals
  its own literal.

## Findings

### F1 — HIGH — SSOT — Three verb enums and two conflict machines over one on-disk lifecycle

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/lifecycle.rs:312-349`

```
pub enum Operation {
    Adopt { … },
    Create { … },
    Fork { … },
    Checkpoint { … },
    Restore { … },
    Retire { … },
}
```

`packages/cowshed/crates/cowshed-core/src/storage/recovery.rs:23-39`

```
pub enum LifecycleIntent {
    Adopt { options: AdoptOptions },
    Create { workspace: WorkspaceName, options: CreateOptions },
    Fork { source: WorkspaceName, destination: WorkspaceName },
    Retire { workspace: WorkspaceName, options: RemoveOptions },
}
```

`packages/cowshed/crates/cowshed-core/src/storage/recovery.rs:480-485`

```
pub enum TransactionKind {
    Adopt,
    Create,
    Restore,
    Retire,
}
```

Conflict checkers: `lifecycle.rs:205-269` (`revalidate` on `ExpectedState`/`ObservedState`) vs `recovery.rs:1240-1264`
(`stale_dimensions` on `AuthoritativeObservations`). `RecoveryModel` (`recovery.rs:943-1238`) is referenced only from
this file and `packages/cowshed/crates/cowshed-core/tests/recovery_model.rs`. Production open-path recovery uses
`LifecycleIntentJournal` (`recovery.rs:103-260`) from `runtime/project.rs`. Problem: The verbs that exist on disk are
restated three times and already disagree. Coverage:

| verb       | `Operation` | `LifecycleIntent` (durable journal) | `TransactionKind` (`RecoveryModel`) |
| ---------- | ----------- | ----------------------------------- | ----------------------------------- |
| Adopt      | yes         | yes                                 | yes                                 |
| Create     | yes         | yes                                 | yes                                 |
| Fork       | yes         | yes                                 | no                                  |
| Checkpoint | yes         | no                                  | no                                  |
| Restore    | yes         | no                                  | yes                                 |
| Retire     | yes         | yes                                 | yes                                 |

`LifecycleIntentPhase` is `{Prepared, Mutating}`; `TransactionPhase` is 14 publication steps. Transition rules therefore
exist twice and cover different subsets. A crash during checkpoint/restore cannot be resumed from the intent journal
because those verbs were never written there. `RecoveryModel` tests cannot go red when the APFS executor diverges (PH
§7.10bb). Fix: One verb enum is the SSOT — `Operation`. Derive the durable journal (`LifecycleIntent`) and the
publication-protocol kind from it (or drop `TransactionKind` and drive `RecoveryModel` off `Operation`). One observation
type feeds both `revalidate` and `stale_dimensions`. Delete whichever protocol copy is not executed on the production
open path, or wire `RecoveryModel::advance` so APFS publication is a step function of that enum. Decision: keep
`LifecycleIntentJournal` as the durable user-intent fence (it is what `project.rs` actually persists); make
`TransactionKind`/`RecoveryModel` either the typed spec APFS implements or delete them. Do not keep a third list.
Cost/Risk: `runtime/project.rs` intent begin/complete, APFS `execute_checked` plans,
`tests/lifecycle_intent_recovery.rs`, `tests/recovery_model.rs`, `tests/lifecycle_contracts.rs`. Journal JSON tag names
(`kind`) must stay the on-disk schema until a single migrator rewrites `lifecycle-intents.json`.

### F2 — HIGH — SSOT — `ExpectedState` and `ObservedState` are the same type written twice

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/lifecycle.rs:134-190`

```
pub enum ExpectedState {
    Exists { repo, name, incarnation, revision, topology_revision, retired },
    Absent { repo, name, topology_revision },
    Checkpoint { repo, workspace, label, revision },
}
pub enum ObservedState {
    Exists { repo, name, incarnation, revision, topology_revision, retired },
    Absent { repo, name, topology_revision },
    Checkpoint { repo, workspace, label, revision },
}
```

`lifecycle.rs:213-258` then matches both arms field-by-field (`er == ar && en == an && …`). Out-of-slice tests already
admit the copy: `packages/cowshed/crates/cowshed-core/tests/lifecycle_contracts.rs` defines
`fn observed(expected: &ExpectedState) -> ObservedState` that clones every field. Problem: Illegal states are
representable as mixed expected/observed pairs that `revalidate` can only reject after a 40-line match. Adding a field
to one and forgetting the other is a silent stale-plan miss. This is the same fact as `AuthoritativeObservations`
(`recovery.rs:734-742`) at a different grain. Fix: One type,
`WorkspaceFact { repo, name, incarnation, revision, topology_revision, retired }` plus `CheckpointFact`.
`revalidate(expected: &[Fact], actual: &[Fact])` is `==`. `ExpectedState`/`ObservedState` go away. Fold
`AuthoritativeObservations` into the same fact or a newtype over it. Cost/Risk: every `match ExpectedState` /
`ObservedState` in `storage/apfs.rs` and `storage/apfs/native.rs` (other slices). Mechanical, compiler-driven.

### F3 — HIGH — SSOT — `rename_noreplace` exists twice with the same hardcoded Linux flag

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/audit/macos.rs:5-18`

```
pub(super) fn rename_noreplace(directory: RawFd, temporary: &CStr, sealed: &CStr) -> io::Result<()> {
    let result = unsafe {
        libc::renameatx_np(directory, temporary.as_ptr(), directory, sealed.as_ptr(), libc::RENAME_EXCL)
    };
```

`packages/cowshed/crates/cowshed-core/src/storage/audit/linux.rs:5-19`

```
const RENAME_NOREPLACE: libc::c_uint = 1;
let result = unsafe {
    libc::syscall(libc::SYS_renameat2, directory, temporary.as_ptr(), directory, sealed.as_ptr(), RENAME_NOREPLACE)
};
```

Byte-equivalent copies: `packages/cowshed/crates/cowshed-core/src/storage/job_artifact/publication/macos.rs:65-77` and
`publication/linux.rs:37-51` (`const RENAME_NOREPLACE: libc::c_uint = 1` again). `fsio.rs:1-8` already claims to be "the
one atomic private-file writer" but implements replace-over (`publish_private_file`), not create-new. Problem: Two
platform triads for one syscall. A flag or errno handling fix in one copy will not land in the other.
`RENAME_NOREPLACE = 1` is an untyped ABI constant restated by hand. Fix: One
`fsio::rename_noreplace(dir_fd, from, to) -> io::Result<()>` with the macos/linux/unsupported arms. Audit and
job-artifact publication call it. Use `libc::RENAME_NOREPLACE` if the pinned libc exports it; otherwise one named
constant next to the syscall, not two. Cost/Risk: job-artifact slice owns the other copy (`CsCoreJobArtifact`).
Signature today returns `io::Result<()>` (audit) vs `Result<c_int, ArtifactError>` (publication); normalize on
`io::Result` and map at the publication boundary.

### F4 — HIGH — SSOT — host-config default mount root is keyed on a restated store path

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/host_config.rs:54-63`

```
pub fn load_for_store(store_root: &Path) -> Result<Self, HostConfigError> {
    validate_absolute_path(store_root)?;
    let default = if store_root == Path::new("/private/cowshed/store") {
        let home = std::env::var_os("HOME").ok_or(HostConfigError::HomeUnavailable)?;
        …
        home.join(".cowshed/mnt")
    } else {
        store_root.join(RETIRED_MOUNT_DIRECTORY)
    };
```

SSOT already exists: `packages/cowshed/crates/cowshed-core/src/storage/bootstrap.rs:31`

```
pub const STORE_ROOT: &str = "/private/cowshed/store";
```

`mod.rs:551` and `audit.rs:6` restate the same literal in tests/docs. Problem: The production branch that decides "this
is the machine-global store, so default mounts under `$HOME/.cowshed/mnt`" compares against a naked string. If
`STORE_ROOT` moves, this comparison stays, and every real store silently takes the retired `<store>/mnt` default. That
is a live footgun, not yet a live mismatch (the two strings currently agree). Fix:
`if store_root == Path::new(crate::storage::bootstrap::STORE_ROOT)`. Delete the other literals in this slice (`mod.rs`
test fixtures can keep a local `const` that aliases `STORE_ROOT`). Cost/Risk: `host_config` would depend on
`bootstrap`'s constant only — no behavior change today. Cycle risk: `bootstrap.rs` already imports `fstab`; it does not
import `host_config`.

### F5 — MEDIUM — SSOT — `CHECKPOINT_NAMESPACE` does not name the on-disk checkpoint directory

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/recovery.rs:13-16`

```
pub const CHECKPOINT_NAMESPACE: &str = ".checkpoints";
pub const STAGING_NAMESPACE: &str = ".staging";
pub const TRASH_NAMESPACE: &str = ".trash";
```

`recovery.rs:702-707`

```
StoredObject::checkpoint(format!(
    "{CHECKPOINT_NAMESPACE}/{}-pre-restore-{}",
    self.logical_name, self.transaction_id
))
```

`mod.rs:189` uses `STAGING_NAMESPACE` as a real path component (`.staging`). `mod.rs:218-225` `checkpoint_image` joins
`self.project.checkpoints` — on disk that is `checkpoints/`, no leading dot (`mod.rs:718` test:
`/private/cowshed/store/acme/widget/checkpoints/raven/…`). `CHECKPOINT_NAMESPACE` is `pub` and used only inside
`recovery.rs`. Problem: Two of three namespace constants are filesystem names. The third is a `RecoveryModel` label that
would create a different directory if anyone joined it. The copies already disagree. Fix: Either (a) make
`CHECKPOINT_NAMESPACE` the on-disk name (`"checkpoints"`) and stop using it as a `StoredObject` prefix, or (b) stop
publishing it and keep a private model-only tag. Staging/trash stay the SSOT they already are. Do not invent
`.checkpoints/` on disk. Cost/Risk: `tests/recovery_model.rs` asserts `".checkpoints/topic-pre-restore-tx-fields"`. APFS
native path builders (other slice) must not be switched onto this constant until it matches disk.

### F6 — MEDIUM — SSOT — `RepositoryIdentityIntent` accepts unknown JSON fields; the sibling journal does not

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/recovery.rs:21-22` and `77-78` and `101-102`

```
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum LifecycleIntent { … }
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LifecycleIntentRecord { … }
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LifecycleIntentJournal { … }
```

`recovery.rs:275-276`

```
#[serde(rename_all = "camelCase")]
pub struct RepositoryIdentityIntent {
```

Problem: The store-root identity-change record is the one recovery file that will _not_ fail closed on a
truncated-or-extended write. `validate` (`recovery.rs:307-349`) checks paths, not schema. A future field added by one
writer is silently dropped by an older reader, then persisted back without it. Fix: Add `deny_unknown_fields`. That is
the journal contract; identity intent is the same class of crash record. Cost/Risk: any test fixture that stuffed extra
keys. Greenfield: migrate fixtures, no compat shim.

### F7 — MEDIUM — SSOT — two UTC civil-date implementations in this slice

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/mod.rs:405-423` (`civil_from_days`, Howard Hinnant,
documented as total over every `u64` second because `libc::gmtime_r` rejects out-of-range `time_t`). `audit.rs:467-485`
then calls `libc::time` + `gmtime_r` and `audit.rs:493-505` reimplements leap-year days:

```
fn valid_calendar_date(year: u16, month: u8, day: u8) -> bool {
    …
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
```

A third `gmtime_r` lives in `runtime/supervisor.rs:3319` (other slice). Problem: The comment on `civil_from_days` names
`gmtime_r` as the thing a label generator must not use. The audit date partition uses it anyway, then validates with a
second leap table. Regime is once-per-audit-record (not a hot loop) but the two calendars can disagree at the `time_t`
edge the comment already called out. Fix: `CommitmentDate` from `civil_from_days(unix_seconds / 86_400)`. Delete
`valid_calendar_date` and the `gmtime_r` path in `SystemEnvironment::utc_date`. Keep `unsafe` libc I/O; do not keep a
second calendar. Cost/Risk: audit tests that inject `AuditSinkEnvironment::utc_date` are unaffected. Production
`SystemEnvironment` needs a `SystemTime` or `time(2)` seconds source only.

### F8 — MEDIUM — SSOT — `pre-restore-` prefix is a bare string in three places

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/mod.rs:38-39`

```
let valid = (1..=128).contains(&bytes.len())
    && !value.starts_with("pre-restore-")
```

`recovery.rs:705` `"{CHECKPOINT_NAMESPACE}/{}-pre-restore-{}"`. Out of slice: `storage/apfs.rs:33`
`const PRE_RESTORE_PREFIX: &str = "pre-restore-";` and several `starts_with("pre-restore-")` in
`storage/apfs/native.rs`. Problem: `CheckpointLabel` refuses user labels that collide with controller undo images, but
the prefix it refuses is not the constant the restorer writes. A rename of the undo grammar will admit labels that later
collide, or reject labels that no longer match. Fix: One `pub const PRE_RESTORE_PREFIX: &str` next to `CheckpointLabel`
(this is the validator). APFS and `TransactionSpec::restore_checkpoint_object` import it. Delete the apfs copy.
Cost/Risk: apfs slice (`CsCoreApfsTriad`) must switch to the shared constant.

### F9 — MEDIUM — STRUCTURE — audit `unsafe` blocks have no stated invariant

Evidence: `audit.rs:470-477` (`time`, `gmtime_r`, `assume_init`), `514-526` (`open` + `File::from_raw_fd`), `535`
(`mkdirat`), `556-566` (`openat` + `from_raw_fd`), `571-595` (`openat`/`fchmod`/`close`/`unlinkat`/`from_raw_fd`),
`622-624` (`unlinkat` in `Drop`). `audit/macos.rs:10-17` (`renameatx_np`). `audit/linux.rs:11-19`
(`syscall SYS_renameat2`). Zero `// SAFETY:` comments in those files. Problem: Repo rule and PH §7.3: `unsafe` is only
legal with a structural invariant. `from_raw_fd` on a failed-close path, `assume_init` on `tm`, and `Drop` `unlinkat`
swallowing the fd lifetime are the sites that will be wrong first. Fix: One SAFETY comment per block, naming the
fd-ownership transfer (`fd >= 0`, exclusive owner, `O_CLOEXEC`), the `gmtime_r` non-null proof, and that
`TemporaryCleanup` holds a directory fd that outlives the `CString` name. Do not wrap in `unsafe fn`. Cost/Risk:
comments only, unless a review of `create_new_file_at` finds a double-close (it does not: failed `fchmod` `close`s
before returning, success path transfers to `File`).

### F10 — MEDIUM — DUPLICATION — `ArrowAuditSink::seal` reimplements the commitment encoder

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/audit.rs:386-421` builds a batch via
`controller_commitments_to_batch`, then `StreamWriter::try_new` / `write` / `finish` onto the fd.
`packages/cowshed/crates/cowshed-core/src/storage/job_artifact.rs:3415-3434` `encode_controller_commitment` is
documented as "the byte form an external AuditSink stores" and does the same three StreamWriter calls into a `Vec`.
Problem: The comment on the encoder says a host that carries its own Arrow write would fork the version. This crate then
forked it internally. Schema stays shared; the IPC envelope does not. Regime: once per controller act, not a hot loop —
this is a drift finding, not a copies finding. Fix: Point `seal` at a
`write_controller_commitment(writer: impl Write, commitment: &ControllerCommitment)` in `job_artifact.rs`.
`encode_controller_commitment` becomes that helper into a `Vec`. Audit writes the helper into the already-`openat`'d
file so fsync still happens on the fd, not on an intermediate buffer. Cost/Risk: job-artifact slice owns the encoder. No
schema change.

### F11 — LOW — TESTS — guards that assert rendered strings or the const they sit next to

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/host_config.rs:519-523`

```
assert_eq!(
    error.to_string(),
    "workspace mount root cannot change while attached: [zeta/widget/raven]"
);
```

The same test then matches `HostConfigError::WorkspacesAttached { workspaces }` on the typed value. `mod.rs:725`
`assert_eq!(WORKSPACE_MARKER_PATH, ".cowshed/workspace.json");` cannot go red unless the const on line 27 is edited in
the same file (PH §7.10bb). `recovery.rs:459-467` asserts `error.message.contains(…)` / `error.hint.contains(…)` for
`prepared_retirement_unreadable`. Problem: The typed match already defends the contract. The `Display` assert fails on
punctuation. The marker-path assert is a spelling check of a literal against itself. Fix: Drop the `to_string()` and
`WORKSPACE_MARKER_PATH` asserts. Keep the `WorkspacesAttached` workspace list. Keep the diagnostic test's `ErrorCode`
assert; drop the substring checks or replace them with a structured field. Cost/Risk: none. `fstab.rs` tests that assert
the full rewritten text are _not_ this defect — the fstab line is the observable contract of `build_fstab`.

### F12 — LOW — STRUCTURE — `WorkspaceName::main()` exists and this slice still constructs `"main"`

Evidence: `lifecycle.rs:441` `WorkspaceName::new("main").expect("fixed main name is valid")`. `mod.rs:302` same.
`recovery.rs:472-475` already does it right:

```
static MAIN_NAME: LazyLock<WorkspaceName> = LazyLock::new(WorkspaceName::main);
fn main_name() -> &'static WorkspaceName { &MAIN_NAME }
```

Problem: Two constructors for an invariant name. `expect` on a parser is the "stringly main" that `WorkspaceName::main`
was written to delete. Fix: Call `WorkspaceName::main()` (or `recovery::main_name()` if it is made `pub(crate)`). Delete
the `expect("fixed main")` sites in this slice. Cost/Risk: none.

## Cross-slice questions

- `storage/apfs.rs` `PRE_RESTORE_PREFIX` and `storage/apfs/native.rs` `starts_with("pre-restore-")` — F8. Owned by the
  APFS triad slice. Confirm they import a core constant rather than keep a third copy.
- `storage/job_artifact/publication/{macos,linux}.rs` `rename_noreplace` — F3. Owned by `CsCoreJobArtifact`. SSOT
  belongs in `fsio`.
- `storage/job_artifact.rs` `encode_controller_commitment` — F10. Same owner.
- `storage/bootstrap.rs` `STORE_ROOT` — F4. Bootstrap slice. `host_config` should import, not restated.
- `storage/bootstrap/native/macos.rs:1479` `split_once("# cowshed created volume labelled")` restates `fstab.rs:4`
  `COWSHED_FSTAB_TAG`. If that tag changes, bootstrap's reader misses every cowshed fstab line. Bootstrap slice should
  import the constant.
- `runtime/project.rs` is the only production consumer of `LifecycleIntentJournal` and does not journal
  checkpoint/restore (F1). Confirm APFS `recover_pending` is the intended sole recovery for those two verbs, or they are
  a hole.
- `runtime/supervisor.rs:3319` third `gmtime_r` (F7).
- `RecoveryModel` vs APFS publication (`CsCoreApfsTriad`): does `advance`'s 14-phase fence match the native executor, or
  is the model a discarded spec? Tests in `tests/recovery_model.rs` cannot answer that.

## Non-findings (checked, clean)

- **fstab parsing / plist / notify.** `fstab.rs` is ~45 lines of tagged-line rewrite. No `plist`, no `notify` in this
  slice. `plist` is used in APFS/bootstrap (hdiutil/diskutil XML); `notify` is used in `runtime/supervisor.rs`. Neither
  belongs here. Hand-rolled fstab is the right weight; a crate or a `plutil` shell-out would be worse. Tests pin the
  exact line grammar, which _is_ the contract.
- **uuid in `audit.rs`.** Writer id is part of the sealed segment name (`commitment-<order>-<uuid>.arrow`) and must be
  unique across concurrent controllers. `Uuid::new_v4` / `.hyphenated()` / `.simple()` (temp names via
  `fsio::temp_name`) is load-bearing. `getrandom` alone would reimplement the same identifier. Crate-wide uuid use is
  the precedent; do not shell out to `uuidgen`.
- **walkdir in `host_config::retired_layout_paths`.** Recursive scan of grant sidecars under discovered projects,
  `follow_links(false)`. Same crate already pays for `walkdir` in `secrets.rs`. A 30-line `read_dir` walker would
  duplicate that. Keep.
- **arrow-\* in `audit.rs`.** One sealed Arrow IPC segment per record is the declared telemetry format (`job_artifact`
  owns the schema so a second Arrow version cannot fork). In-process, typed, and the CLI default. Not replaceable by
  `serde_json` without breaking Containium/PTMCART. Feature flags are not over-wide for this use.
- **Copies / alloc regime.** `clone()` on `RepoId`/`WorkspaceName` in `PurePlanner` and `derive_workspaces` is
  once-per-lifecycle-plan / once-per-enumerate, not a probe loop. `ArrowAuditSink::seal` allocates a batch, two
  `CString`s, and a v4 uuid per record — once per controller act. Do not file these as copies findings (PH §4.1: name
  the regime first).
- **`LifecycleIntentPhase` shared by the journal and `RepositoryIdentityIntent`.** That part is already one type.
- **No TODO/FIXME, no `unimplemented!` in this slice.**
- **`dispatch_blocking`** is a four-line `spawn_blocking` wrapper; not a second runtime.
- **`NullAuditSink` / `ContinuityAudit`.** Thin, used.
- **Platform cfg.** macos/linux/unsupported `rename_noreplace` arms are mutually exclusive and the unsupported path
  returns `ErrorKind::Unsupported` rather than compiling a fake success.
- **`fstab` idempotence tests** (`second_run_is_byte_identical`, empty pin set uninstall) can go red.
- **God-file threshold.** Largest owned file is `recovery.rs` at 1297 lines; no 5k-line module. `RecoveryModel::advance`
  is ~86 lines. No argument-list that should have been a struct beyond what `CheckedLifecyclePlan`/`TransactionSpec`
  already are. )
