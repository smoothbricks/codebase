# cowshed-core/apfs-triad

Scope: `packages/cowshed/crates/cowshed-core/src/apfs.rs` (5169),
`packages/cowshed/crates/cowshed-core/src/storage/apfs.rs` (3217),
`packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs` (5190)

## Summary

- The triad is layered, not a superseded copy: `src/apfs.rs` is the macOS disk-image primitive (`ApfsBackend` /
  `hdiutil` / `diskutil` / `clonefile`); `src/storage/apfs.rs` is the lifecycle substrate (`ApfsSubstrate` /
  `ApfsExecutionHost`); `src/storage/apfs/native.rs` is the host that _delegates_ image ops to `MacOsApfsBackend`.
  Neither module dies.
- Public surfaces do not overlap: `ApfsError` vs `ApfsStorageError`; volume naming lives only in `storage/apfs.rs`
  (`volume_key`, `volume_label`); `crate::apfs` never invents a label format.
- HIGH: `native.rs` (~4856 production lines) and `apfs.rs` (5169, ~2086 production) are 5k-class god files;
  `recover_pending` alone is ~550 lines.
- HIGH: every `unsafe` in the slice lacks a SAFETY/invariant comment (`clonefile`,
  `open`/`openat`/`flock`/`from_raw_fd`, `getmntinfo`, `chown`, `renameatx_np`, `getuid`/`getgid`).
- HIGH SSOT: restore undo prefix `pre-restore-` is a named constant then restated as a string literal in four
  `native.rs` sites.
- MEDIUM: `Host(String)` erases typed operational errors; `VolumeNameResolutionFailure` is dead public API; `read_dir`
  entry errors are swallowed; three near-identical reclaim helpers; two `hdiutil info -plist` walkers.
- Slice deps that this code actually uses (`plist`, `libc`, `sha2`, `uuid`, `serde`/`serde_json`, `thiserror`,
  `async-trait`, `tokio`) are load-bearing. Do not shell out to `plutil` on the attach path.

## Findings

### F1 — HIGH — STRUCTURE — `native.rs` is a 5k-line god file

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs:1-5190` (file), `2386-4777`
(`impl ApfsExecutionHost`, ~2390 lines), `4054-4603` (`recover_pending`, ~550 lines)

```1085:1095:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
/// Real filesystem adapter for [`super::ApfsSubstrate`]. Native image commands are never
/// reconstructed here: every create/clone/attach/fsck/mount/detach/delete/compact operation is
/// delegated to [`MacOsApfsBackend`].
pub struct MacOsApfsExecutionHost<R> {
    backend: MacOsApfsBackend<R>,
    config: ApfsSubstrateConfig,
    mounted: MountedRegistry,
    restore_failpoint: AtomicU8,
    mount_source: Arc<dyn KernelMountSource>,
    recovery_marker_source: Option<Arc<dyn RecoveryMarkerSource>>,
}
```

Problem: One module owns flock/openat locking, getmntinfo, a mount-registry actor, adoption RENAME_SWAP, restore crash
recovery, GC preview/execute, checkpoint JSON facts, and the entire `ApfsExecutionHost` impl. `recover_pending` is a
550-line state machine with nested crash-boundary arms. A 5k-line file is itself the finding. Fix: Split along the seams
the types already name: `native/lock.rs` (openat/flock), `native/mounts.rs` (getmntinfo + `MountedRegistry`),
`native/publish.rs` (adopt/link/vacate/publish_image), `native/restore.rs` (`restore_swap` / `rollback_restore` /
`recover_pending`), `native/gc.rs` (`preview_gc_project` / `execute_gc_plan`). Keep `MacOsApfsExecutionHost` as a thin
facade. Cost/Risk: Test modules in `crates/cowshed-core/tests/apfs_native_macos.rs` import this path; split is
mechanical if `pub` surface (`MacOsApfsExecutionHost`, `KernelMountSource`, `RestoreFailpoint`) stays in
`native.rs`/`native/mod.rs`.

### F2 — HIGH — STRUCTURE — `apfs.rs` is a 5k-line god file (production + in-module tests)

Evidence: `packages/cowshed/crates/cowshed-core/src/apfs.rs:1-2086` (production), `2088-5169` (`mod tests`, ~3081 lines)

```1:4:packages/cowshed/crates/cowshed-core/src/apfs.rs
//! macOS APFS disk-image substrate.
//!
//! Every external operation crosses [`CommandRunner`]. Commands are represented
//! as an executable plus an argument vector; this module never invokes a shell.
```

```604:655:packages/cowshed/crates/cowshed-core/src/apfs.rs
pub trait ApfsBackend {
    fn create_staged_image(&self, request: &CreateImageRequest) -> Result<CreatedImage, ApfsError>;
    fn compact_image(&self, image: &Path, format: ImageFormat) -> Result<(), ApfsError>;
    // ... attach/mount/detach/resize/grow_container/attached_capacity ...
}
```

Problem: Types, command backend, eight independent plist parsers, clonefile FFI, and 3k lines of RecordingRunner tests
share one file. Integration coverage already lives in `crates/cowshed-core/tests/apfs_*.rs`. Fix: Keep `apfs.rs` as the
public types + `ApfsBackend` trait. Move `MacOsApfsBackend` to `apfs/macos.rs`, plist parsers to `apfs/plist.rs`,
clonefile to `apfs/clonefile.rs`, unit tests to `apfs/tests.rs` or the existing integration crate. Cost/Risk:
`pub mod apfs` re-exports must stay stable (`MacOsApfsBackend`, `ApfsError`, `CommandRunner`).
`src/bin/apfs_benchmark.rs` imports those names.

### F3 — HIGH — STRUCTURE — `unsafe` with no stated invariant

Evidence: `packages/cowshed/crates/cowshed-core/src/apfs.rs:2029-2050`,
`packages/cowshed/crates/cowshed-core/src/storage/apfs.rs:1902-1903`,
`packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs:114-175`, `198`, `716-724`, `2617`, `4815-4823`

```2029:2050:packages/cowshed/crates/cowshed-core/src/apfs.rs
        unsafe extern "C" {
            fn clonefile(
                src: *const std::ffi::c_char,
                dst: *const std::ffi::c_char,
                flags: u32,
            ) -> std::ffi::c_int;
        }
        // ...
        let result = unsafe { clonefile(src.as_ptr(), dst.as_ptr(), 0) };
```

```114:122:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
    let root_fd = unsafe { libc::open(root_name.as_ptr(), ROOT_OPEN_FLAGS) };
    if fd_failed(root_fd) {
        return Err(io_error(
            "open controller store without following symlinks",
            root,
            io::Error::last_os_error(),
        ));
    }
    let mut directory = unsafe { File::from_raw_fd(root_fd) };
```

```716:724:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
        let mut mounts = std::ptr::null_mut();
        let count = unsafe { libc::getmntinfo(&mut mounts, libc::MNT_NOWAIT) };
        // ...
        let entries = unsafe { std::slice::from_raw_parts(mounts, count as usize) };
```

Problem: Rubric: `unsafe` without a stated invariant comment is a finding. `from_raw_fd` requires exclusive ownership of
a live fd; `getmntinfo` returns a process-static buffer that must not be retained across a second call; `clonefile`
requires NUL-terminated paths (already `CString`) and flags 0. None of that is written down. `getuid`/`getgid` in
`prepare_adopt_stage` (`storage/apfs.rs:1902-1903`) is the same omission on a quieter path. Fix: One SAFETY comment per
block citing the structural invariant (CString lifetime, fd ownership, getmntinfo buffer lifetime, RENAME_SWAP = 0x2).
Do not wrap in extra safe APIs unless a comment cannot name the invariant. Cost/Risk: Comments only. No behavior change.

### F4 — HIGH — SSOT — `PRE_RESTORE_PREFIX` restated as literals

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs.rs:33`, `2981-2984` (the constant in use), vs
`native.rs:304-307`, `885`, `4231-4234`, `4291`

```33:33:packages/cowshed/crates/cowshed-core/src/storage/apfs.rs
const PRE_RESTORE_PREFIX: &str = "pre-restore-";
```

```304:308:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
        .join(format!(
            "pre-restore-{}.{}",
            fact.destination_incarnation,
            format.extension()
        ));
```

```885:885:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
            if name.starts_with("pre-restore-") && name.ends_with(GRANTS_SIDECAR_SUFFIX) {
```

```4231:4234:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
                        .join(format!(
                            "pre-restore-{}.{}",
                            metadata.workspace_incarnation,
                            metadata.image_format.extension()
```

Problem: The undo-image stem is the recovery join key. `undo_image` and two native sites already use
`super::PRE_RESTORE_PREFIX`; four others re-type the string. The copies currently agree — not a live bug — but this is
the exact class that becomes a restore/GC miss when one side changes. Fix: Every native site uses
`super::PRE_RESTORE_PREFIX` (or a single `fn pre_restore_image(checkpoints, workspace, incarnation, format) -> PathBuf`
next to `undo_image`). Delete the literals. Cost/Risk: `crates/cowshed-core/tests/apfs_native_macos.rs` fixtures
hardcode the same stem; they stay as fixtures. `storage/mod.rs` `CheckpointLabel` also rejects
`starts_with("pre-restore-")` — other slice, see Cross-slice.

### F5 — MEDIUM — STRUCTURE — operational failures erased to `Host(String)`

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs.rs:652-653`, call sites e.g. `native.rs:133-134`
(incarnation mint), `2569`, `2712`, `3508`, `4800-4805` (`Io` exists and is unused at those sites)

```652:653:packages/cowshed/crates/cowshed-core/src/storage/apfs.rs
    #[error("APFS host operation failed: {0}")]
    Host(String),
```

```2565:2569:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
    fn copy_tree(&self, source: &Path, destination: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(destination)?;
        copy_until_quiescent_blocking(source, destination)
            .map(|_| ())
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
```

Problem: `ApfsStorageError::Io` and `Apfs(ApfsError)` already exist. `Host(String)` is the `/dev/null` of error typing:
metadata parse failures, copy failures, and identity errors all become an unmatchable string. Callers cannot distinguish
"marker mismatch" from "copy failed" without scraping Display. Fix: Map through existing variants (`Io`, `Apfs`,
`MarkerMismatch`, `Layout`). Delete `Host` once the last site moves. `UuidIncarnationSource` should fail with a typed
incarnation error, not `Host`. Cost/Risk: `runtime/project.rs` and tests match `ApfsStorageError::Host` /
`MarkerMismatch` today; migrate those matches.

### F6 — MEDIUM — STRUCTURE — dead public `VolumeNameResolutionFailure`

Evidence: `packages/cowshed/crates/cowshed-core/src/apfs.rs:321-328`, `414-417`, Display `510-514`, tests `4492-4520`.
No production constructor anywhere in the crate (only the Display table in tests).

```321:328:packages/cowshed/crates/cowshed-core/src/apfs.rs
pub enum VolumeNameResolutionFailure {
    InvalidPlist(String),
    MissingDeviceIdentifier,
    DeviceMismatch { reported: String },
    MissingVolumeName,
    WrongTypeVolumeName,
    BlankVolumeName,
}
```

```414:417:packages/cowshed/crates/cowshed-core/src/apfs.rs
    VolumeNameResolutionFailed {
        device: String,
        reason: VolumeNameResolutionFailure,
    },
```

Problem: Volume identity is the in-image marker (`native.rs:1488-1491` says the APFS volume _label_ is not an
authority). This error type is leftover from a name-based resolver that is gone. The tests only assert Display strings,
so they cannot go red if production stops constructing the variant — it already has. Fix: Delete
`VolumeNameResolutionFailure`, `ApfsError::VolumeNameResolutionFailed`, and the Display test table. Cost/Risk: Public
`cowshed_core::apfs` surface; no in-repo constructor. Greenfield: delete.

### F7 — MEDIUM — STRUCTURE — `read_dir` entry errors swallowed

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs:1449-1452`, `3914-3917`

```1449:1452:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
        let entries = match fs::read_dir(&layout.project().sessions) {
            Ok(entries) => entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .collect::<Vec<_>>(),
```

```3914:3917:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
        let entries = match fs::read_dir(&storage.project().sessions) {
            Ok(entries) => entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .collect::<Vec<_>>(),
```

Problem: `published_facts` / `pending_publications` drop a `read_dir` entry `Err`. A unreadable session image disappears
from enumeration instead of failing closed. Adjacent helpers (`preview_gc_project`, `retired_checkpoint_artifacts`)
already propagate the same class of error via `io_error`. Fix: Same shape as `preview_gc_project` (`2005-2011`): `map` +
`collect::<Result<Vec<_>, _>>()?`. Cost/Risk: Enumeration can start failing on a damaged sessions dir. That is the
correct failure.

### F8 — MEDIUM — DUPLICATION — `directory_children` / `regular_file_children`

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs:773-841`

```773:806:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
fn directory_children(directory: &Path) -> Result<Vec<PathBuf>, ApfsStorageError> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(metadata) if metadata.file_type().is_symlink() => return Ok(Vec::new()),
        // ... NotFound => empty, else io_error ...
    }
    // filter_map: keep dirs only
}
```

```808:841:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
fn regular_file_children(directory: &Path) -> Result<Vec<PathBuf>, ApfsStorageError> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(metadata) if metadata.file_type().is_symlink() => return Ok(Vec::new()),
        // identical error arms, then keep files only
    }
```

Problem: Two functions, one predicate. Drift in the symlink/NotFound policy is a recovery hole. Fix:
`fn children(directory, pred: impl Fn(FileType) -> bool)`. Callers pass `is_dir` / `is_file`. Cost/Risk: Local to
`native.rs`. Tests `recovery_enumerators_are_no_follow_and_type_exact` stay.

### F9 — MEDIUM — DUPLICATION — three identical `detach_and_reclaim_*` functions

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs.rs:2020-2031`, `2238-2249`, `2538-2549`

```2020:2031:packages/cowshed/crates/cowshed-core/src/storage/apfs.rs
fn detach_and_reclaim_adopt<H: ApfsExecutionHost>(
    host: &H,
    attachment: H::Attachment,
    staged_image: &Path,
    format: ImageFormat,
) -> Result<(), ApfsStorageError> {
    let detached = host.detach(attachment, DetachIntent::Release);
    let reclaimed = host.reclaim_image(staged_image, format);
    match detached {
        Ok(()) => reclaimed,
        Err(primary) => combine_cleanup("adopt staging detach", primary, reclaimed),
    }
}
```

Problem: Clone and restore copies differ only in the `combine_cleanup` operation string. Three functions that must stay
identical. Fix: `fn detach_and_reclaim(..., operation: &'static str)`. Pass `"adopt staging detach"` /
`"clone staging detach"` / `"restore staging detach"`. Cost/Risk: None. Callers are the abort/commit paths in the same
file.

### F10 — MEDIUM — DUPLICATION — three image-extension validators

Evidence: `packages/cowshed/crates/cowshed-core/src/apfs.rs:1710-1729`,
`packages/cowshed/crates/cowshed-core/src/metadata.rs:206-216` (neighbour; same predicate)

```1710:1729:packages/cowshed/crates/cowshed-core/src/apfs.rs
fn validate_image_path(path: &Path, format: ImageFormat) -> Result<(), ApfsError> {
    if path.extension() == Some(OsStr::new(format.extension())) {
        Ok(())
    } else {
        Err(ApfsError::InvalidImagePath { path: path.to_owned(), format })
    }
}

fn validate_clone_path(path: &Path, format: ImageFormat) -> Result<(), CloneFileError> {
    if path.extension() == Some(OsStr::new(format.extension())) {
        Ok(())
    } else {
        Err(CloneFileError::InvalidImagePath { path: path.to_owned(), format })
    }
}
```

Problem: `ImageFormat::validate_path` already exists and is what `native.rs` calls (`2505-2510`). `apfs.rs` reimplements
the same extension check twice to pick an error type. `validate_path` compares UTF-8 `to_str()`; these compare `OsStr` —
a latent disagreement on non-UTF-8 extensions. Fix: Single predicate on `ImageFormat`. Map
`MetadataError::ImageFormatMismatch` into `ApfsError::InvalidImagePath` / `CloneFileError::InvalidImagePath` at the two
call sites. Delete the twins. Cost/Risk: `metadata.rs` is another slice's file; the mapping lives in `apfs.rs`.

### F11 — MEDIUM — DUPLICATION / COPIES — two walkers of the same `hdiutil info -plist`

Evidence: `packages/cowshed/crates/cowshed-core/src/apfs.rs:687-697` + `1621-1679` (`parse_attachment_inventory`),
`1462-1468` + `1528-1576` (`parse_attachment_capacity`)

```687:697:packages/cowshed/crates/cowshed-core/src/apfs.rs
    fn attached_whole_devices(&self, image: &Path) -> Result<BTreeSet<String>, ApfsError> {
        let image = attachment_inventory_path(image)?;
        let output = self.run_checked(
            "inventory attached disk images",
            CommandRequest::new(HDIUTIL, ["info", "-plist"]),
        )?;
        parse_attachment_inventory(&image, &output.stdout)
    }
```

```1462:1468:packages/cowshed/crates/cowshed-core/src/apfs.rs
    fn attached_capacity(&self, image: &Path) -> Result<ImageCapacity, ApfsError> {
        let image = attachment_inventory_path(image)?;
        let output = self.run_checked(
            "inventory attached disk images",
            CommandRequest::new(HDIUTIL, ["info", "-plist"]),
        )?;
        parse_attachment_capacity(&image, &output.stdout)
    }
```

Problem: Both parse `images[]` / `image-path` / `system-entities`. Resize (`native.rs:2909-2936`) can hit both on one
verb: inventory for "is it attached", then again for capacity. Regime: per attach/resize, not a byte loop — still
evaporating work (Byproduct L0) and two parsers that can disagree about "matching image". Fix: One
`struct AttachmentInventory { devices, capacity }` parsed once. `attached_whole_devices` / `attached_capacity` project
fields. Resize reuses one inventory. Cost/Risk: Parser tests in `apfs.rs` `mod tests` must assert both fields of the
fused type.

### F12 — MEDIUM — SSOT — checkpoint pin is a `String` restated at three match sites

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs:216-225`, `3527-3531`, `4010-4018`,
`4694-4702`

```216:225:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
struct CheckpointFactWire {
    version: u32,
    repo_id: RepoId,
    workspace: WorkspaceName,
    label: CheckpointLabel,
    revision: u64,
    pin: String,
}
```

```3527:3531:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
            pin: match pin {
                Pin::Pinned => "pinned",
                Pin::Automatic => "automatic",
            }
            .to_owned(),
```

Problem: `Pin` is already the domain enum. The wire format re-encodes it as `"pinned" | "automatic"` and three readers
re-parse those strings. A typo in one match is a silent invalid-pin Host error. Fix: `pin: Pin` with serde rename, or a
two-variant wire enum. Delete the string matches. Cost/Risk: On-disk `.checkpoint.json` camelCase field stays; serde
mapping is the cutover. Existing facts already use those two spellings.

### F13 — MEDIUM — SSOT — `"sessions"` / `"checkpoints"` restated beside `StorageLayout`

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs.rs:3000-3002`, `native.rs:1648-1650`, `1933`, `2004`,
`4060` vs layout accessors `1449`, `1737-1739`, `3988`

```2994:3002:packages/cowshed/crates/cowshed-core/src/storage/apfs.rs
fn retired_image_below(
    project_root: &Path,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
    format: ImageFormat,
) -> PathBuf {
    project_root
        .join("sessions")
        .join(TRASH_NAMESPACE)
```

```2004:2004:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
        let sessions = project.join("sessions");
```

Problem: `published_facts` goes through `layout.project().sessions`; trash/GC/recovery rebuild
`project.join("sessions")` / `join("checkpoints")`. `TRAGING_NAMESPACE`/`TRASH_NAMESPACE` were imported from recovery to
avoid this class of drift; the parent directory names were not. Fix: Only `StorageLayout` / `ProjectLayout` may name
`sessions` and `checkpoints`. `retired_image_below` takes a layout (or `project().sessions`) instead of a bare project
root. Cost/Risk: Layout lives in `storage/mod.rs` (other slice). This slice stops concatenating the strings.

### F14 — MEDIUM — TESTS — Display-string oracles, including for dead types

Evidence: `packages/cowshed/crates/cowshed-core/src/apfs.rs:2511-2621`, `4352-4353`, `4479-4521`, `4972-4983`

```4492:4520:packages/cowshed/crates/cowshed-core/src/apfs.rs
            (
                VolumeNameResolutionFailure::InvalidPlist("bad shape".into()),
                "invalid disk info plist: bad shape",
            ),
            // ... six more Display pairs ...
        ];
        for (failure, expected) in cases {
            assert_eq!(failure.to_string(), expected);
        }
```

```2618:2620:packages/cowshed/crates/cowshed-core/src/apfs.rs
        assert_eq!(
            ApfsError::InvalidAttachmentInventory("bad shape".into()).to_string(),
            "invalid attachment inventory: bad shape"
        );
```

Problem: These tests assert rendered strings, not typed variants. Combined with F6 they cannot go red on the production
path (PH §7.10bb). Command-argv tests in the same module _do_ assert structure (`argv(&requests[0])`) — keep those. Fix:
Delete the Display tables. Keep tests that match `ApfsError::…` / `CloneFileError::…` and the argv/plist parsers.
Cost/Risk: None. Integration tests in `tests/apfs_*.rs` already cover behavior.

### F15 — LOW — DUPLICATION — `sync_parent!` and `sync_parent_path` are the same function

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs:490-509`

```490:509:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
macro_rules! sync_parent {
    ($path:expr) => {{
        let path: &Path = $path;
        let parent = path
            .parent()
            .ok_or(ApfsStorageError::InvalidPlan("image path has no parent"))?;
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error("sync image directory", parent, error))
    }};
}

fn sync_parent_path(path: &Path) -> Result<(), ApfsStorageError> {
    // identical body
}
```

Problem: Two spellings of one fsync. The macro exists so `sync_parent!(&path)?` works in `let`/`if let` without a
statement; the function is the same bytes. Fix: Keep `sync_parent_path`. Replace the macro with that call. Or keep the
macro and delete the function. Cost/Risk: Local.

### F16 — LOW — STRUCTURE — `WorkspaceName::new("main").expect` beside `WorkspaceName::main()`

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs.rs:2881-2882`, `native.rs:1440`, `3899`

```2881:2882:packages/cowshed/crates/cowshed-core/src/storage/apfs.rs
fn main_name() -> WorkspaceName {
    WorkspaceName::main()
}
```

```1440:1440:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
        let main = WorkspaceName::new("main").expect("fixed main name is valid");
```

Problem: Native re-parses a name the type already constructs infallibly, and panics if that ever stops being true.
`storage/apfs.rs` already wraps `WorkspaceName::main()`. Fix: Call `WorkspaceName::main()` (or `super::main_name()`) at
both native sites. Delete the `expect`. Cost/Risk: None.

### F17 — LOW — COPIES — `512` restated against `SECTOR_BYTES`

Evidence: `packages/cowshed/crates/cowshed-core/src/apfs.rs:27`,
`packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs:324-328`

```26:27:packages/cowshed/crates/cowshed-core/src/apfs.rs
/// The unit `hdiutil` reports and accepts image extents in.
const SECTOR_BYTES: u64 = 512;
```

```324:328:packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
fn allocated_file_bytes(metadata: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        metadata.blocks().saturating_mul(512)
    }
```

Problem: Unix `st_blocks` is 512-byte units on Darwin; that happens to equal `hdiutil`'s sector. Two constants for one
number. Regime: GC/stats, not attach hot path — LOW. Fix: One crate-level `pub(crate) const DEV_BSIZE: u64 = 512` or
reuse `SECTOR_BYTES` via `crate::apfs` if the comment is rewritten to "Unix block accounting and hdiutil sectors, both
512 on Darwin". Cost/Risk: Comment must name both consumers so nobody "fixes" one.

## Cross-slice questions

- `packages/cowshed/crates/cowshed-core/src/metadata.rs:206-216` (`ImageFormat::validate_path`) is the SSOT F10 should
  call. Metadata slice owns the file.
- `packages/cowshed/crates/cowshed-core/src/storage/mod.rs` `CheckpointLabel::new` rejects `starts_with("pre-restore-")`
  (`storage/mod.rs:38-39`). That string must stay glued to `PRE_RESTORE_PREFIX` (F4). Storage-layout slice owns the
  label type.
- `packages/cowshed/crates/cowshed-core/src/storage/recovery.rs:14-16` defines `CHECKPOINT_NAMESPACE = ".checkpoints"`
  and a restore object named `{name}-pre-restore-{transaction_id}`. On-disk APFS checkpoints are
  `project/checkpoints/<workspace>/pre-restore-<incarnation>.<ext>`. Confirm those are different planes (logical
  recovery model vs APFS image files) and not a drifted path. Recovery/lifecycle slice owns `recovery.rs`.
- CLI copy (`packages/cowshed/crates/cowshed-cli/src/args.rs:1464-1465`, `docs/cli.md`) says displaced images are
  `pre-restore-<timestamp>`. This slice writes `pre-restore-<incarnation>`. Docs/CLI slice should take this slice's stem
  as SSOT.
- `packages/cowshed/crates/cowshed-core/src/device.rs` is the identifier grammar `apfs.rs` correctly uses (`DISKUTIL`,
  `identifier_depth`, `container_of`). No contradiction.
- `volume_key` / `volume_label` callers in `runtime/project.rs` and `gateway_inventory.rs` are consumers, not a second
  implementation.

## Non-findings (checked, clean)

- **Not two implementations of one concept.** `storage/apfs.rs:10-13` and `366-367` import and require `crate::apfs`.
  `native.rs:20-23,1085-1087` delegates create/clone/attach/fsck/mount/detach/delete/compact to `MacOsApfsBackend`.
  Callers: `runtime/project.rs` (`ApfsSubstrate<MacOsApfsExecutionHost<SystemCommandRunner>>`), `cowshed-cli` runtime,
  `gateway_inventory.rs`, `src/bin/apfs_benchmark.rs` (primitives only). Killing either module would delete a layer, not
  a duplicate.
- Volume naming is single-source: `volume_key` / `volume_label` in `storage/apfs.rs:3061-3086`. `apfs.rs` only validates
  a name (`is_valid_apfs_volume_name`); it does not format one. Comment at `native.rs:1488-1491` matches: label is not
  an authority.
- Error enums are layered, not copied: `ApfsError` (tool/plist/clonefile) ⊂ `ApfsStorageError::Apfs(#[from] ApfsError)`.
- `plist` crate is load-bearing. Attach/resize parse `diskutil`/`hdiutil` plists in-process with typed errors. `plutil`
  would add a process per attach and throw away error typing. Keep it.
- `libc` is load-bearing (`clonefile`, `flock`/`openat`/`O_NOFOLLOW`, `getmntinfo`, `renameatx_np`). No CLI equivalent
  is machine-parseable at this boundary.
- `sha2` is load-bearing for GC plan identity (`gc_candidate`). Not a hot loop; cryptographic identity of a plan is the
  point.
- `uuid` is load-bearing for `UuidIncarnationSource` (`uuid::Uuid::new_v4().simple()` is the incarnation grammar).
  Already a crate dep; `uuidgen(1)` is not acceptable in-process.
- `serde`/`serde_json` are load-bearing for checkpoint/restore fact files (`deny_unknown_fields` wire types).
- Copies on the attach path (`PathBuf::to_owned` into errors, `OsString` argv, `CommandRequest.clone` on spawn failure)
  are once-per-operation, not a hot loop. Not findings (PH §4.1 regime).
- `clonefile` FFI vs shelling out to `cp -c`: in-process, typed `EXDEV`/`EEXIST`, no shell. Keep FFI.
- `storage/apfs.rs` in-file tests cover the lock-table contract with typed `Operation` values (not Display). Broader
  substrate tests live in `tests/apfs_storage.rs` (other files).
- No `TODO`/`FIXME` in the three files. `unreachable!()` in restore commit (`storage/apfs.rs:1163-1165`, `2557-2560`) is
  after an exhaustive two-variant split — invariant, not operational.
- `cfg(target_os = "macos")` arms have `not(macos)` counterparts (`clonefile` → `UnsupportedPlatform`, `getmntinfo` →
  empty, `swap_paths` → Host error). They compile. )
