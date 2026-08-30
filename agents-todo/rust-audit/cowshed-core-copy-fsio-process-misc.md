# cowshed-core/copy+fsio+process+misc

Scope: `packages/cowshed/crates/cowshed-core/src/copy.rs` (1275), `copy/native.rs` (9), `copy/native/macos.rs` (74),
`copy/native/linux.rs` (52), `fsio.rs` (191), `process.rs` (191), `device.rs` (86), `landing.rs` (160), `error.rs`
(177), `lib.rs` (37)

## Summary

- HIGH: Linux `copy_leaf` is always `std::fs::copy` (byte shovel, no xattrs/ACLs); never FICLONE. The crate already has
  a FICLONE helper in job-artifact publication.
- MEDIUM: `ErrorCode` kebab-case is restated in `as_str`, serde, and `packages/cowshed/src/types.ts`; `docs/cli.md`
  already drifted to `env-missing`.
- MEDIUM: copy tests never observe clone vs data-copy, so deleting `COPYFILE_CLONE_FORCE` stays green.
- LOW: `fmt_command_failure` / `fmt_command_spawn` duplicate argv rendering.
- LOW: `is_temp_artifact` is not the inverse of `temp_name` for non-UTF-8 names, and is dead in production.
- macOS adopt tries `COPYFILE_CLONE_FORCE` first; EXDEV/ENOTSUP fallback is explicit, not silent. Cross-volume adopt is
  copy-bound by APFS physics.
- `copy.rs` walks with `read_dir`, not `walkdir`. `error.rs` is the one public operational taxonomy.

## Findings

### F1 — HIGH — COPIES — Linux tree copy is a byte shovel with no CoW and weaker fidelity

Evidence: `packages/cowshed/crates/cowshed-core/src/copy/native/linux.rs:37-51`

```
pub fn copy_leaf(source: &Path, destination: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        std::os::unix::fs::symlink(fs::read_link(source)?, destination)?;
    } else {
        fs::copy(source, destination)?;
        fs::set_permissions(destination, metadata.permissions())?;
    }
    set_times(destination, &metadata)
}
```

Contrast (same crate, already written):
`packages/cowshed/crates/cowshed-core/src/storage/job_artifact/publication/linux.rs:8-16`

```
pub(super) fn try_fast_clone(...) {
    const FICLONE: libc::c_ulong = 0x4004_9409;
    ...
    if unsafe { libc::ioctl(file.as_raw_fd(), FICLONE, source.as_raw_fd()) } == 0 {
        return Ok(Some(file));
    }
```

macOS counterpart tries clone first: `copy/native/macos.rs:41-60`
(`COPYFILE_ALL | COPYFILE_CLONE_FORCE | COPYFILE_NOFOLLOW`, then `COPYFILE_ALL` only on `EXDEV`/`ENOTSUP`). Problem: On
every non-macOS build, every regular file is a userspace/kernel data copy (`std::fs::copy`). No
`FICLONE`/`FICLONERANGE`. Same-volume Linux adopt (or a same-volume reconcile) therefore duplicates every byte.
`fs::copy` also does not carry xattrs/ACLs; macOS `COPYFILE_ALL` does. Regime: adopt/reconcile kernel, once per tree,
size = n files — not a startup note. Silent: `CopyReport` has no clone-vs-copy field (`copy.rs:59-64`). Fix: Lift the
job-artifact `FICLONE` ioctl into `copy/native/linux.rs` (or a shared `native::clone_regular_file`). Try clone; on
`EXDEV`/`EOPNOTSUPP`/`ENOTTY`/`EINVAL` fall back to `fs::copy` (or `copy_file_range`). Copy xattrs with
`listxattr`/`getxattr`/`setxattr` if Linux adopt is a real product path; otherwise delete the Linux module and `cfg` the
crate to macos-only. Do not leave a half-port that looks like the macOS copier. Cost/Risk: Linux adopt of large trees;
need to share the ioctl constant with job-artifact (CsCoreJobArtifact owns that file). xattr parity is a second,
separate cut.

### F2 — MEDIUM — SSOT — ErrorCode kebab-case is restated three times and already diverged in docs

Evidence: `packages/cowshed/crates/cowshed-core/src/error.rs:6-16,43-52`

```
#[serde(rename_all = "kebab-case")]
pub enum ErrorCode { Internal, Usage, NotFound, Conflict, EnvironmentMissing, SandboxDenied, Integrity }
...
pub const fn as_str(self) -> &'static str {
    match self {
        Self::Internal => "internal",
        ...
        Self::EnvironmentMissing => "environment-missing",
```

`packages/cowshed/src/types.ts:1-8`

```
export type ErrorCode =
  | 'internal'
  | 'usage'
  | 'not-found'
  | 'conflict'
  | 'environment-missing'
  | 'sandbox-denied'
  | 'integrity';
```

Live divergence: `packages/cowshed/docs/cli.md:23` table cell is `env-missing` while the JSON contract and `as_str` are
`environment-missing`. Problem: One taxonomy, three spellings to keep in lockstep. `as_str` is a handwritten copy of
`rename_all = "kebab-case"`; the TS union is a handwritten copy of the Rust enum. `cli.md` already disagrees. Adding a
variant can compile and still ship the wrong wire string if `as_str` is updated independently of serde. Fix: Rust
`ErrorCode` is the SSOT. Delete `as_str`'s string table: generate from `serde` (test-roundtrip every variant — the
existing `CODES` array in `error.rs:124-132` already lists them) or make `as_str` call a single shared `&'static str`
used by a custom serializer. Generate the TS union from the same list (or from the JSON test in `error.rs:156-162`).
Change `cli.md` line 23 to `environment-missing`. Cost/Risk: napi/TS consumers of `ErrorCode`; CLI `--json` golden tests
(`cowshed-cli/tests/output_contracts.rs` already pins `"sandbox-denied"`).

### F3 — MEDIUM — TESTS — copy tests cannot go red if clonefile is removed

Evidence: macOS clone attempt is `copy/native/macos.rs:41-60`. Tests that would have to notice: `copy.rs:883-905` (inode
of an already-mirrored file — skip-recopy, not clone), `copy.rs:911-928` (hard-link identity — `fs::hard_link`, not
clonefile), `copy.rs:974-1031` (content/mode/xattr/symlink — fidelity of whatever copier ran), `copy.rs:1226-1273`
(resume does not reclone completed leaves — again skip-recopy). No test reads `st_blocks`, `clonefile(2)` success, or
`CopyReport` clone/copy counts. `CopyReport` (`copy.rs:59-64`) only has `passes` and `changed_entries`. Problem:
PERFORMANCE-HANDBOOK §7.10bb. Substituting `copyfile(..., COPYFILE_ALL | COPYFILE_NOFOLLOW)` for the `CLONE_FORCE` path,
or replacing macOS `copy_leaf` with Linux `fs::copy`, leaves every test green. The CoW attempt is unguarded. Regime:
adopt tests run on the same APFS volume as `TMPDIR`, where `CLONE_FORCE` should succeed — the one place an oracle is
cheap. Fix: On macOS, after `run`, assert `st_blocks` of a large-enough file is far below `size/512` (or
`fclonefileat`/`getattrlist` clone-exists). Fail the test if the dest consumed a full data copy. Optionally add
`cloned: usize` / `copied: usize` to `CopyReport` so the production path cannot lie either. Cost/Risk: APFS-only
assertion; Linux stays a data copy until F1. Do not assert clone across volumes — adopt's host→image hop is supposed to
fall back.

### F4 — LOW — DUPLICATION — argv rendering is written twice

Evidence: `packages/cowshed/crates/cowshed-core/src/process.rs:109-115` and `131-137`

```
write!(f, "{operation} failed: executable {program:?}, argv [")?;
for (index, arg) in args.iter().enumerate() {
    if index != 0 { f.write_str(", ")?; }
    write!(f, "{:?}", arg.as_ref())?;
}
...
write!(f, "could not run executable {program:?}, argv [")?;
for (index, arg) in args.iter().enumerate() {
    if index != 0 { f.write_str(", ")?; }
    write!(f, "{:?}", arg.as_ref())?;
}
```

Problem: Two copies of the same argv loop. A later change to quoting or empty-argv rendering will land in one and not
the other. `apfs.rs` and `storage/bootstrap.rs` both call these (re-export), so the diagnostic is load-bearing. Fix: One
`fn fmt_argv(...)` used by both. Delete the second loop. Cost/Risk: Display of `ApfsError` / `HostCommandFailure` only;
golden strings in `apfs.rs` tests around `process.rs` diagnostics.

### F5 — LOW — STRUCTURE — `is_temp_artifact` is not the inverse of `temp_name`, and is dead outside tests

Evidence: `packages/cowshed/crates/cowshed-core/src/fsio.rs:22-38,33,131-135`

```
pub(crate) fn temp_name(final_name: &OsStr, discriminator: impl fmt::Display) -> OsString {
    let mut name = OsString::from(".");
    name.push(final_name);
    name.push(format!(".tmp.{discriminator}"));
    name
}
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn is_temp_artifact(file_name: &OsStr) -> bool {
    let Some(name) = file_name.to_str() else { return false; };
    name.starts_with('.') && name.contains(".tmp.")
}
...
if self.armed { let _ = fs::remove_file(&self.path); }
```

Problem: Comment at `fsio.rs:29-31` claims “the exact inverse”. `temp_name` accepts non-UTF-8 `final_name`;
`is_temp_artifact` returns false unless the whole `OsStr` is UTF-8. No production caller (`allow(dead_code)`).
`TempCleanup::drop` swallows `remove_file` errors (`let _ =`), so a failed publish can leave residue the recognizer
cannot see. Regime: publish path, rare. Fix: Either delete `is_temp_artifact` until a sweeper exists, or match on
`OsStr` bytes (`starts_with(b".")` + contains `b".tmp."`) and stop claiming inverse. On drop failure, at least
debug-assert in tests; do not invent a log crate for it. Cost/Risk: `metadata.rs` test already calls `is_temp_artifact`;
any future sweeper must use the same grammar as `temp_name`.

## Cross-slice questions

- `packages/cowshed/crates/cowshed-core/src/storage/bootstrap/native/macos.rs:6416` restates the temp-artifact grammar
  (`format!(".{}.tmp.{}", ...)`) instead of calling `fsio::temp_name`. CsCoreBootstrap owns that file — should it
  switch, or is the fd-based marker writer deliberately independent?
- `packages/cowshed/crates/cowshed-core/src/git.rs:523` excludes only `.fseventsd/`. `copy.rs:43-49` skips five APFS
  volume-metadata names. CsCoreGit: is the dirty-exclude list supposed to be the same table?
- `packages/cowshed/crates/cowshed-core/src/storage/job_artifact/publication/linux.rs` owns the working `FICLONE` ioctl.
  F1 wants that as the single Linux clone primitive — CsCoreJobArtifact should confirm before anyone moves it.
- `landing.rs` consumes `crate::api::dto::{GitOid, LandingCommits, WorkspaceLanding}`;
  `packages/cowshed/src/types.ts:59-68` restates `LandingCommits`. API/DTO slice owns whether TS is generated.

## Non-findings (checked, clean)

- macOS leaf copy is `copyfile(3)` with `COPYFILE_CLONE_FORCE` first; EXDEV/ENOTSUP fallback is named in
  `macos.rs:38-40`. Adopt host→image is copy-bound by volume physics (spec `02_workspaces.md`); not a silent degrade of
  the macOS path.
- `copy.rs` does not use `walkdir`. Snapshot is an explicit `read_dir` walk (`copy.rs:487-537`) so vanishing entries are
  skippable. `walkdir` in `cowshed-core/Cargo.toml` is other slices (`secrets.rs`, `host_config.rs`).
- No userspace read/write loop or tunable buffer in this slice. macOS is one syscall; Linux is `fs::copy` (F1).
- `error.rs` is the one public operational enum (`CowshedError` / `ErrorCode`). `fsio::PublishError` is a typed
  Io-vs-Write envelope, not a parallel taxonomy. `landing.rs` returns `String` / `LandingCommits::Indeterminate` by
  documented design (`landing.rs:49-51,80-83`) so a failure can never read as landed.
- `device.rs` is the SSOT for `diskN`/`diskNsM` grammar and `DISKUTIL`; `apfs.rs` and bootstrap import it. Tests pin
  typed `Option<usize>`, not rendered strings.
- `libc` is load-bearing (`copyfile`, `utimensat`, `mkfifo` in tests). Do not shell out to `cp`/`rsync` — `copy.rs:1-25`
  records why rsync failed. `tokio::task::spawn_blocking` around the copier is load-bearing. `serde` on `CowshedError`
  is the JSON CLI contract. `uuid` in `fsio.rs:84` is an `O_EXCL` discriminator; the crate already depends on `uuid` v4
  elsewhere — not a slice-local dep to delete.
- `unsafe` sites in this slice have invariant comments (`macos.rs:21-22`, `linux.rs:21`, `copy.rs:778`,
  `copy.rs:800-801`, `copy.rs:822`).
- `process.rs` `DiagnosticBytes` keeps invalid bytes (`\\xHH`) instead of U+FFFD; test pins that (`process.rs:183-190`).
- PathBuf clones in `snapshot`/`reconcile`/`copy_leaves_parallel` are adopt-once, n = tree size, dominated by the
  per-file syscall. BTreeMap order is load-bearing (shallow create, deep remove/metadata). Not a finding.
- `copy.rs` is 1275 lines, ~540 of them tests. Production functions stay near the 100-line line (`reconcile` 240-346).
  Not a god file.
- `lib.rs` re-exports only; no second copy of these modules.
