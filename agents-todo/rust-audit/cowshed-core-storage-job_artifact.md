# cowshed-core/storage/job_artifact

Scope: `packages/cowshed/crates/cowshed-core/src/storage/job_artifact.rs` (4574),
`packages/cowshed/crates/cowshed-core/src/storage/job_artifact/publication.rs` (882),
`packages/cowshed/crates/cowshed-core/src/storage/job_artifact/publication/macos.rs` (79),
`packages/cowshed/crates/cowshed-core/src/storage/job_artifact/publication/linux.rs` (53),
`packages/cowshed/crates/cowshed-core/src/storage/job_artifact/publication/unsupported.rs` (24). Doctrine:
`BYPRODUCT-ENGINEERING.md`, `docs/handbook/04-mechanisms.md`, `docs/handbook/05-memory-toolkit.md`,
`docs/handbook/02-measurement.md` §4.1.

## Summary

- HIGH SSOT: protected-path grammar (`.cowshed/job/<id>/<leaf>`) is restated in four builders inside this slice and
  again in `dto.rs`.
- HIGH COPIES: spilled-file publication hashes the source, copies or clonefiles, then hashes the destination — up to
  three full-file passes on the 1 GiB quota path.
- HIGH STRUCTURE: 4574-line god file; `recover_records_with_budget` is 212 lines; two Arrow codecs and the live store
  share one module.
- MEDIUM DUPLICATION: five near-identical 64 KiB read/hash/copy loops; `encode_batch` restated as
  `encode_controller_commitment`.
- MEDIUM COPIES: `protected_record_schema()` rebuilds 33 fields on every encode/decode compare; `append_record` clones a
  `JobArtifactRecord` (inline payload included) just to take `&`; checkpoint re-hashes `records.arrow` from byte zero.
- MEDIUM STRUCTURE: every `unsafe` in publication lacks a SAFETY invariant comment; `mod publication` is unix-only while
  `job_artifact.rs` still carries `not(unix)` arms.
- LOW SSOT/TESTS: `recover_records` hardcodes the 64 MiB budget; `IO_BUFFER_BYTES` vs `COPY_BUFFER_BYTES`; several tests
  assert `message.contains`.
- No CRITICAL live divergence found in the copies that currently agree (`out`/`err`, JobState camelCase).
- `sha2` and Arrow IPC are load-bearing; clonefile-first then 64 KiB copy is the right publication shape.
- CSARROW1/CSBATCH1 framing is not duplicated in cowshed-gateway (gateway writes unframed Arrow IPC).

## Findings

### F1 — HIGH — SSOT — Protected artifact path grammar is restated four times

Evidence: `job_artifact.rs:245-252`, `job_artifact.rs:266-275`, `job_artifact.rs:1425-1438`, `job_artifact.rs:1907-1908`

```
fn validate_protected_path(...) {
    if let ProtectedOutput::File { path } = info.storage.artifact() {
        let expected = format!(".cowshed/job/{}/{}", job_id.get(), stream.leaf());
        if path.as_path() != Path::new(&expected) {
fn validate_visible_path(...) {
    let expected = format!(".cowshed/job/{}/{}", job_id.get(), stream.leaf());
fn protected_relative(...) {
    Ok(WorkspacePath::new(format!(".cowshed/job/{}/{}", job_id.get(), stream.leaf()))?)
fn protected_absolute(...) {
    workspace_root.join(".cowshed").join("job").join(job_id.get().to_string()).join(stream.leaf())
fn records_path(...) { workspace_root.join(".cowshed/job/records.arrow") }
```

Problem: One path grammar, four string/join constructions, plus `StreamKind::leaf()` (`"out"`/`"err"`) as a fifth
implicit table. `publication.rs:33` restates the first component as `b".cowshed"`. A leaf or directory rename cannot be
a single edit. Currently the copies agree; that is luck, not a type. Fix: One function
`protected_stream_path(job_id, kind) -> WorkspacePath` used by validate, relative, absolute, and records layout.
`PROTECTED_DIRECTORY` lives next to it. Callers compare `WorkspacePath` values, not reformatted strings. Cost/Risk:
Intra-crate only if dto is left alone; dto also restates the grammar (cross-slice). Tests that hardcode
`.cowshed/job/1/out` keep working if the string is unchanged.

### F2 — HIGH — COPIES — Publication verifies by hashing the whole artifact twice (copy path: three full-file reads)

Evidence: `publication.rs:188-201`, `publication.rs:378-393`, `publication.rs:396-420`, `publication.rs:423-448`

```
ProtectedOutput::File { path } => {
    let source = open_authority_source(&source_path, stream)?;  // hashes source
    match self.try_fast_clone(&source)? {
        Some(file) => file,
        None => {
            copy_file_descriptor(&source, &mut file, ...)?;     // reads source again
            file
        }
    }
}
verify_content(&temporary, ..., stream.bytes, stream.sha256)?; // hashes dest
fn open_authority_source(...) {
    ...
    verify_content(&file, path, stream.bytes, stream.sha256)?;
}
```

Problem: Regime is once per `publish_output` of a spilled stream — the production path once output exceeds
`inline_cap_bytes` (64 KiB) up to `combined_output_quota_bytes` (1 GiB). Clonefile avoids the byte copy (good) but still
pays two sequential SHA-256 walks. The copy fallback pays hash + copy + hash: three full-file reads and one write.
Byproduct L0/L7.8: the copy pass already touches every byte; the dest digest is a byproduct of that pass. Source digest
is already in `stream.sha256` from seal (`StreamWriterState` hashed on append). Fix: Trust the sealed `stream.sha256`
after `reject_hardlink` + mode checks on the authority fd (L7: verify at seal). Fuse dest hashing into
`copy_file_descriptor`. After clonefile, either `fstat` length + one dest hash (integrity of the new inode) or skip dest
hash when clone succeeded and source identity still matches — pick dest-hash-after-clone as the conservative option;
delete the source re-hash. Cost/Risk: Security-sensitive. Dest hash after clonefile is the fill-time verify of the
published inode and should stay. Source re-hash is the evaporating copy.

### F3 — HIGH — STRUCTURE — 4574-line module owns store, fs, framed log, and two Arrow codecs

Evidence: `job_artifact.rs:1-4574` (module layout), `job_artifact.rs:2212-2424` (`recover_records_with_budget`, 212
lines), `job_artifact.rs:3458-3583` (`decode_controller_commitments`, 125 lines), `job_artifact.rs:522-634`
(`ArtifactStore::open`, 112 lines)

```
pub fn recover_records_with_budget(...) -> Result<RecoveryReport, ArtifactError> {
    // 212 lines: magic, frame walk, digest, Arrow decode, sequence, manifest prefix
}
pub fn decode_controller_commitments(...) -> Result<Vec<ControllerCommitment>, ArtifactError> {
    // 125 lines of per-kind column indexes
}
```

Problem: Natural seams already exist as comments (`controller_commitment_schema` vs protected records; `mod publication`
already extracted). The parent file still mixes live `ArtifactStore`/`StreamWriterState`, unix fd walks, CSARROW1
framing, protected-record codec, and controller-commitment codec. `recover_records_with_budget` is the recovery state
machine and should not share a file with `job_record_to_batch`. Fix: Split along the seams that already compile
independently: `job_artifact/store.rs` (ArtifactStore, StreamWriterState, quota), `job_artifact/fs.rs` (protected
create/open/mode/hardlink), `job_artifact/frame.rs` (CSARROW1 append/recover), `job_artifact/codec.rs` (protected
schema + job/manifest), `job_artifact/controller_codec.rs` (already a second schema). Keep `publication/` as-is.
Cost/Risk: `pub` exports (`ArtifactStore`, `recover_records`, `encode_controller_commitment`, schemas) stay in
`job_artifact.rs` as `pub use`. No wire change.

### F4 — MEDIUM — DUPLICATION — Five 64 KiB read/hash/copy loops

Evidence: `publication.rs:396-420` (`copy_file_descriptor`), `publication.rs:423-446` (`verify_content`),
`job_artifact.rs:1547-1577` (`read_file_verified`), `job_artifact.rs:1650-1670` (`hash_file_incrementally`),
`job_artifact.rs:1688-1733` (`VerifiedStreamReader::read_chunk`)

```
let mut buffer = [0_u8; COPY_BUFFER_BYTES]; // or IO_BUFFER_BYTES
loop {
    let read = file.read(&mut buffer)?;
    if read == 0 { break; }
    hasher.update(&buffer[..read]); // or destination.write_all
}
```

Problem: Same streaming kernel, five copies, two buffer-size constants (`IO_BUFFER_BYTES = 65_536`,
`COPY_BUFFER_BYTES = 64 * 1024`). Overflow handling already diverges: publication uses `saturating_add`,
`read_file_verified` uses `checked_add`. Fix: One helper `hash_reader(file, buf) -> (u64, Sha256Digest)` and one
`copy_and_hash(src, dst, buf)`. `VerifiedStreamReader` stays the public chunked API and calls the same hasher. Collapse
`IO_BUFFER_BYTES`/`COPY_BUFFER_BYTES` to one `const`. Cost/Risk: Publication error stage (`PublicationStage::Sync` vs
`Copy`) must remain a parameter so tests that pin the stage stay red.

### F5 — MEDIUM — COPIES — `protected_record_schema()` allocates 33 fields on every compare

Evidence: `job_artifact.rs:2493-2532`, `job_artifact.rs:2698-2702`, `job_artifact.rs:2885-2889`, vs the cached sibling
at `job_artifact.rs:3169-3173`

```
/// The controller commitment schema is compared against every decoded segment during replay, so
/// it is built once rather than allocating twenty-three fields per comparison.
pub fn controller_commitment_schema() -> Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(build_controller_commitment_schema);
    Arc::clone(&SCHEMA)
}
pub fn protected_record_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![ /* 33 fields */ ]))
}
fn batch_to_protected_record(...) {
    if batch.num_rows() != 1 || batch.schema() != protected_record_schema() {
fn batch_to_job_record(...) {
    if batch.num_rows() != 1 || batch.schema() != protected_record_schema() {
```

Problem: The file already documents why schema compares must not rebuild fields — then does exactly that for the
protected schema. Recovery walks every frame (`recover_records_with_budget` → `batch_to_protected_record` →
`batch_to_job_record`), so each job frame allocates the schema twice and walks it twice. Regime: once per workspace open
/ recovery, scaled by job count — not a per-append hot loop, but the controller codec already paid the LazyLock tax for
this exact compare. Fix: Same `LazyLock` pattern as `controller_commitment_schema`. Drop the duplicate schema check
inside `batch_to_job_record` (caller already checked). Cost/Risk: None; schema bytes are immutable.

### F6 — MEDIUM — COPIES — `append_record` clones the job record to take a reference, then clones it again into the map

Evidence: `job_artifact.rs:710-737`

```
let batch = protected_record_to_batch(&ProtectedRecord::Job(record.clone()))?;
...
if !matches!(record.state, JobState::Queued | JobState::Running) {
    self.committed_jobs.insert(record.job_id, record.clone());
}
Ok((record, digest))
```

Problem: `job_record_to_batch` takes `&JobArtifactRecord`. The first clone exists only to wrap `ProtectedRecord::Job`.
Inline stdout/stderr (up to 64 KiB each, plus argv) are copied for nothing. The second clone is for `committed_jobs`
while still returning `record`. Regime: once per admission and once per finish — not a byte loop, but it copies the
payload the rest of the file worked to avoid buffering twice. Fix: `job_record_to_batch(&record)` directly.
`insert(job_id, record.clone())` only when committing, or insert then clone out of the map for the return. Checkpoint
path (`record.clone()` at `job_artifact.rs:784`) is the same shape. Cost/Risk: None.

### F7 — MEDIUM — COPIES — Checkpoint re-hashes `records.arrow` from byte zero

Evidence: `job_artifact.rs:765-787`, `job_artifact.rs:1650-1670`, `job_artifact.rs:726-727`

```
let records_sha256 = match fs::symlink_metadata(&path) {
    Ok(metadata) if metadata.len() == 0 => Sha256Digest::compute(RECORD_MAGIC),
    Ok(_) => hash_file_incrementally(&path)?,
    ...
};
let payload = encode_batch(&batch)?;
let manifest_batch_sha256 = Sha256Digest::compute(&payload);
append_framed_batch(&path, &payload, manifest_batch_sha256)?;
```

Problem: Every `append_framed_batch` already has the payload bytes in hand and hashes them for the frame trailer.
Checkpoint then re-reads the whole file to produce `records_sha256`. Recovery must re-hash the prefix (crash path; keep
that). The live store does not need to. Grows with records file size (L0 evaporating work under load). Regime: once per
checkpoint, O(file). Fix: Keep a running `Sha256` on `ArtifactStore`, seeded with `RECORD_MAGIC`, updated with each
complete frame in `append_framed_batch`. Checkpoint reads the running digest. Recovery rebuilds it. Cost/Risk: Running
hasher must match recovery's `prefix_hasher` byte-for-byte (header + payload + digest + trailer). One differential test:
checkpoint digest == recover-and-rehash.

### F8 — MEDIUM — DUPLICATION — Arrow IPC one-batch encode/decode written twice

Evidence: `job_artifact.rs:2463-2490`, `job_artifact.rs:3419-3452`

```
fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, ArtifactError> {
    let mut payload = Vec::new();
    let mut writer = StreamWriter::try_new(&mut payload, &batch.schema())?;
    writer.write(batch)?; writer.finish()?;
    Ok(payload)
}
pub fn encode_controller_commitment(...) {
    let batch = controller_commitments_to_batch(...)?;
    let mut out = Vec::with_capacity(512);
    let mut writer = StreamWriter::try_new(&mut out, batch.schema_ref())?;
    writer.write(&batch)?; writer.finish()?;
    Ok(out)
}
```

Problem: Same StreamWriter kernel, two functions, two decode twins (`decode_single_batch` /
`decode_controller_commitment`). The comment on `encode_controller_commitment` says the codec must live beside the
schema so they drift together — that does not require a second IPC wrapper. Fix: `encode_controller_commitment` =
`encode_batch(&controller_commitments_to_batch(...)?)`. `decode_controller_commitment` = `decode_single_batch` then
`decode_controller_commitments`. Cost/Risk: None. Audit sink still writes its own StreamWriter (cross-slice).

### F9 — MEDIUM — STRUCTURE — `unsafe` in publication has no SAFETY invariant comments

Evidence: `publication/macos.rs:12-19`, `publication/macos.rs:39-62`, `publication/macos.rs:70-78`,
`publication/linux.rs:14`, `publication/linux.rs:43-51`, `publication.rs:124-138`, `publication.rs:231-247`,
`publication.rs:331-338`, `publication.rs:364-375`, `job_artifact.rs:848`

```
let result = unsafe {
    libc::fclonefileat(source.as_raw_fd(), parent.directory.as_raw_fd(), parent.temporary_leaf.as_ptr(), 0)
};
Ok(Some(unsafe { File::from_raw_fd(fd) }))
if unsafe { libc::ioctl(file.as_raw_fd(), FICLONE, source.as_raw_fd()) } == 0 {
```

Problem: `from_raw_fd` ownership, `fstat`/`fstatat` `assume_init`, `openat` flags (`O_NOFOLLOW|O_DIRECTORY|O_CLOEXEC`),
and ioctl/syscall contracts are the invariants. None are written down. `linux.rs` also hardcodes `FICLONE = 0x4004_9409`
and `RENAME_NOREPLACE = 1` next to the unsafe. Fix: One SAFETY comment per block naming fd ownership, NUL-terminated
`CStr`, and why `O_NOFOLLOW` makes the path walk TOCTOU-closed. Keep the hardcoded ioctl/syscall numbers with a
citation; do not shell out `cp`/`mv`. Cost/Risk: Comments only.

### F10 — MEDIUM — STRUCTURE — `publication` is unix-only while the parent file claims `not(unix)`

Evidence: `job_artifact.rs:33` (`mod publication;`), `publication.rs:5-7` (`std::os::unix::{ffi,fs,io}`),
`job_artifact.rs:1902-1905`, `job_artifact.rs:1638-1647`

```
mod publication;   // unconditional
use std::os::unix::ffi::OsStrExt;
#[cfg(not(unix))]
fn reject_hardlink(...) -> Result<(), ArtifactError> { Ok(()) }
#[cfg(not(unix))]
{ OpenOptions::new().read(true).open(&path) }
```

Problem: `publication.rs` will not compile off unix. The parent file still stubs hardlink rejection as a no-op and opens
artifacts with follow-able `OpenOptions`. Those stubs are unreachable as long as `mod publication` is compiled, and they
are a security hole if it is ever cfg-gated without replacing publication. Fix: `#[cfg(unix)] mod publication;` and a
`not(unix)` publication that returns `PublicationStage::Publish` unsupported (already exists as
`publication/unsupported.rs`, but that module is "not macos/linux", still unix). Delete the silent `reject_hardlink`
no-op; fail closed on platforms that cannot prove nlink==1. Cost/Risk: If Windows is not a target, delete the
`not(unix)` arms instead of pretending.

### F11 — LOW — SSOT — `recover_records` hardcodes the 64 MiB budget that `ArtifactConfig` already owns

Evidence: `job_artifact.rs:143-149`, `job_artifact.rs:2208-2210`

```
retained_recovery_budget_bytes: 64 * 1024 * 1024,
pub fn recover_records(path: &Path) -> Result<RecoveryReport, ArtifactError> {
    recover_records_with_budget(path, 64 * 1024 * 1024)
}
```

Problem: Two literals. `ArtifactStore::open` uses the config field; the public `recover_records` does not. A config
change does not move the public helper. Fix: `recover_records` calls
`recover_records_with_budget(path, ArtifactConfig::default().retained_recovery_budget_bytes)` or drop the helper and
make callers pass the budget. Cost/Risk: Tests that call `recover_records` keep the same number until default changes.

### F12 — LOW — TESTS — Integrity tests assert on message substrings

Evidence: `job_artifact.rs:3918-3921`, `job_artifact.rs:4415-4418`, `job_artifact.rs:4447-4450`,
`job_artifact.rs:4540-4542`, `publication.rs:557-574`

```
Err(ArtifactError::Integrity { message, .. }) if message.contains("duplicate terminal")
Err(ArtifactError::Integrity { message, .. }) if message.contains("prefix digest")
message.contains(expected_message)
```

Problem: The stage/variant is typed; the string is not. `assert_publication_error` can stay green under a rewritten
message that still happens to contain the needle (`""` is used as a wildcard in several publication tests). Substitution
test (§7.10bb): swapping the error variant to another `Integrity` with a similar sentence still passes. Fix: Match on a
structured code (new `IntegrityKind` or keep distinct `ArtifactError` variants). Publication tests already match
`PublicationStage`; drop empty-string `contains`. Cost/Risk: Error enum change ripples to supervisor mapping.

## Cross-slice questions

- `packages/cowshed/crates/cowshed-core/src/api/dto.rs:1103-1107` and `:1520-1521` restates `.cowshed/job/{id}/{leaf}`
  with literals `"out"`/`"err"`. This slice's `StreamKind::leaf` must stay in lockstep. dto should own the path builder;
  this slice should call it.
- `packages/cowshed/crates/cowshed-core/src/storage/audit.rs:414-421` writes Arrow IPC with its own `StreamWriter` after
  `controller_commitments_to_batch`. This slice's `encode_controller_commitment` is the documented "byte form an
  AuditSink stores" (`job_artifact.rs:3415-3418`) and is unused by the in-tree sink.
- `packages/cowshed/crates/cowshed-core/src/storage/audit/{linux,macos,unsupported}.rs` duplicate `rename_noreplace`
  with this slice's `publication/{linux,macos,unsupported}.rs` (same `RENAME_NOREPLACE = 1` / `renameatx_np` +
  `RENAME_EXCL`). One fs helper should own atomic create-new rename.
- `packages/cowshed/crates/cowshed-gateway/src/telemetry.rs:571-575` writes unframed Arrow IPC. Magic
  `CSARROW1`/`CSBATCH1`/`CSEND001` is unique to this slice; gateway is a different envelope, not a drifted copy. Confirm
  gateway does not need the CSARROW1 trailer/digest frame.
- `JobState` serde in dto is `rename_all = "camelCase"` (`dto.rs:640-650`). This slice restates the same strings in
  `state_name`/`parse_state` (`job_artifact.rs:3144-3166`). They agree today (`outputLimit`). dto serde should be the
  single table.

## Non-findings (checked, clean)

- `sha2`: load-bearing. Incremental in-process digest of live streams and sealed files, typed `Sha256Digest`, no shell
  (`shasum` is not an fd-hash API). Keep.
- Arrow (`arrow-array`/`arrow-buffer`/`arrow-ipc`/`arrow-schema`): the durable records format is Arrow IPC. Cannot shell
  out. Keep.
- Publication copy is streaming (`[u8; 64 KiB]`), not whole-artifact `read_to_end`. Clonefile/`FICLONE` is attempted
  first. That is the right shape; F2 is the extra hash passes, not the copy itself.
- `base64` is unused in this slice (crate-level, dto/credentials).
- CSARROW1 framing constants are not restated outside this file.
- `BufferBudget` admission is closed-form; spill releases the reservation. Not a grow-under-load path.
- `hasher.clone().finalize()` in `durable_prefix` is required (hasher keeps running). Not a finding. `finish` could
  consume the hasher; once-per-job, note only.
- Live `unwrap`/`expect` in non-test code are invariant claims (`"eight bytes"`, `"spill file exists"`,
  `"WorkspacePath is always UTF-8"`, `JobId::new(1)`). Operational failures go through `ArtifactError`.
- Unit tests that pin typed values (`barrier_id`, digests, `AppendOutcome` via buffer accounting, frame lengths) can go
  red. Publication tests pin `PublicationStage`.
- `libc` ioctl/syscall wrappers are load-bearing; `cp`/`clonefile(1)` cannot do `openat`+`O_NOFOLLOW`+`RENAME_EXCL`.
