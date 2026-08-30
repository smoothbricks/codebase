# cowshed-core/api

Scope: `packages/cowshed/crates/cowshed-core/src/api/capability.rs` (3023), `dto.rs` (2517), `server.rs` (1128),
`mod.rs` (10), `peer_credentials/mod.rs` (67), `peer_credentials/linux.rs` (25), `peer_credentials/macos.rs` (12).
Doctrine: BYPRODUCT-ENGINEERING.md, PERFORMANCE-HANDBOOK §4.1 / §7 / §7.10bb / §7.12. Compared against
`packages/cowshed/src/types.ts` (353) and `packages/cowshed/crates/cowshed-napi/src/lib.rs` (NapiExecRequest + stdin
seam).

## Summary

- `dto.rs` is restated by hand in `types.ts` and (for exec) in `NapiExecRequest`; `JobInfo.argv` has already diverged
  and the TS client asserts the wrong shape.
- Tagged-bytes wire grammar is implemented twice (`CommandArg` vs `BinaryData`) and the copies no longer agree.
- Durable `ExecRecord.argv` is `Vec<String>` while live `JobInfo.argv` is `Vec<CommandArg>` — non-UTF-8 argv cannot
  survive the record.
- Client and server each own `verify_peer` plus length-prefixed frame I/O; error mapping already drifted.
- `CAPABILITY_METHODS` / `WORKER_METHODS` are two string tables of one allowlist.
- `dto.rs` (2517) and `capability.rs` (3023) are god files; `spawn_controller_actor` and `serve_controller_connection`
  exceed 150 lines.
- `unsafe` libc credential calls have no SAFETY comments.
- Follow-logs `job.status` re-deserializes `JobInfo` and re-hashes inline stream bytes (L0/L7) every poll.
- Affine-ownership test `size_of::<CoordinatorToken>() > 0` cannot go red (§7.10bb).
- Slice deps that this code actually uses (`serde`, `base64`, `sha2`, `libc`, `tokio`, `bytes`, `async-trait`, `url`,
  `thiserror`) earn their weight; `uuid` here is only a 64-hex nonce.

## Findings

### F1 — HIGH — SSOT — DTO shapes exist in Rust, TypeScript, and napi; JobInfo.argv already disagrees

Evidence: `packages/cowshed/crates/cowshed-core/src/api/dto.rs:1475-1493`

```
pub struct JobInfo {
    pub repo_id: RepoId,
    ...
    pub argv: Vec<CommandArg>,
    pub cwd: Option<WorkspacePath>,
    ...
    pub exit: Option<ExitStatus>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    ...
    pub stdin: StdinInfo,
}
```

`packages/cowshed/src/types.ts:257-274`

```
export interface JobInfo {
  ...
  readonly argv: readonly string[];
  readonly cwd?: string;
  ...
  readonly exit?: unknown;
  readonly stdout: unknown;
  readonly stderr: unknown;
  ...
  readonly stdin: unknown;
}
```

`packages/cowshed/crates/cowshed-napi/src/lib.rs:108-128` restates `ExecRequest` as `NapiExecRequest`
(`argv: Vec<String>`, `stdin: Option<String>`, `stdin_workspace_path`). `packages/cowshed/src/index.ts:110,420` then
`typia.json.createAssertParse<JobInfo>()` against napi JSON produced by serde of the Rust type. Wire argv is tagged
`{encoding,data}` (`capability.rs:1933`). Problem: three sources of truth. TS `JobInfo.argv: string[]` is not the Rust
wire; nested job streams were erased to `unknown`. TS `Coordinator.remove` vs Rust `Coordinator::destroy` / RPC
`coordinator.destroy`; TS omits `changeRepoId` / `assignSlot` / `repoMirror` / `setCheckpointQuota`. `ExecRequest` stdin
dual-spelling is documented in `dto.rs:1786-1798` and pinned by `wire_stdin_spelling` / `cli_stdin_spelling` — that
adapter is the exception, not the rule. Fix: `dto.rs` is the SSOT (validation, `deny_unknown_fields`, domain newtypes).
Generate TS from those serde types (ts-rs or a schema dump). napi keeps JSON passthrough of core types; keep
`NapiExecRequest` only as the stdin adapter. Do not generate Rust from TS — TS has already lost nested types. Cost/Risk:
every JS parse of `status` / `wait` / `listJobs`; typia will reject real `JobInfo` JSON. [INFERENCE] that typia throws
at runtime; the type mismatch is read, not inferred.

### F2 — HIGH — DUPLICATION — CommandArg and BinaryData are one tagged-bytes grammar, already forked

Evidence: `dto.rs:780-850` (CommandArg: UTF-8 tag, else canonical standard base64; deserialize re-encodes and
**rejects** valid UTF-8 encoded as base64) vs `dto.rs:916-952`

```
let bytes = match wire.encoding {
    BinaryEncoding::Utf8 => wire.data.into_bytes(),
    BinaryEncoding::Base64 => base64::engine::general_purpose::STANDARD
        .decode(wire.data)
        .map_err(|_| serde::de::Error::custom(DtoError::InvalidBinaryEncoding))?,
};
Self::new(bytes).map_err(serde::de::Error::custom)
```

`CommandArgEncoding` (`dto.rs:760-765`) and `BinaryEncoding` (`dto.rs:895-900`) are identical enums.
`CommandArgRef`/`CommandArgWire` vs `BinaryDataRef`/`BinaryDataWire` are the same two-field shape. Problem: copies no
longer agree. `BinaryData` accepts non-canonical base64 and UTF-8-as-base64; `CommandArg` does not. Two JSON spellings
of the same bytes are legal on output payloads and illegal on argv. Fix: one `TaggedBytes` (or `BinaryData`) with the
strict CommandArg rules; `CommandArg` is that type plus NUL/argv limits. Delete the second encoding enum. Cost/Risk: any
stored `BinaryData` JSON that used the loose form must be rejected or migrated. Greenfield: reject.

### F3 — HIGH — SSOT — JobInfo.argv is CommandArg; ExecRecord.argv is String

Evidence: `dto.rs:1483` `pub argv: Vec<CommandArg>,` vs `dto.rs:1644` `pub argv: Vec<String>,` `JobInfo::validate` and
`ExecRecord::validate` (`dto.rs:1495-1522`, `1658-1682`) copy the same terminal-state / output-limit / exit-kind /
`validate_for` checks. Serialize/deserialize wire+ref structs are copy-pasted (`JobInfoRef`/`JobInfoWire` vs
`ExecRecordRef`/`ExecRecordWire`). Problem: the durable record cannot represent the live job's byte-exact argv.
Non-UTF-8 `CommandArg` is a legal job and an illegal record. Two projections of one process. Fix:
`ExecRecord.argv: Vec<CommandArg>`. Share one `fn validate_terminal_job(...)`. Delete the second wire/ref pair or
generate both from one schema. Cost/Risk: any reader of persisted ExecRecord JSON that assumed string argv. Cross-slice:
whoever constructs `ExecRecord` (job artifact / supervisor) must stop `.to_string_lossy()`-style conversion.

### F4 — MEDIUM — DUPLICATION — Client and server each implement peer auth and length-prefixed frames

Evidence: `capability.rs:887-905` vs `server.rs:1089-1108` — same `peer_uid` + `geteuid` check; error mapping already
drifted (`SocketTypeSizeOverflow` is "not a stream socket" on the client, "socket type size does not fit socklen_t" on
the server). `capability.rs:915-1071`
(`write_frame`/`read_frame`/`write_rpc_frame`/`read_rpc_frame`/`write_binary_frame`/`read_binary_frame`) vs
`server.rs:918-1035` (same protocol, `write_u32` vs `to_be_bytes()`). Problem: the wire is one codec (`server.rs`
`codec` module is already the JSON SSOT). Byte framing and uid check are not. A length/endian/limit drift is a handshake
break. Fix: one `frame` module next to `codec`: `write_frame`/`read_frame`/`write_binary_frame`/`read_binary_frame` +
`verify_peer`. Client and `serve_controller_connection` call it. Keep error _text_ as a parameter if handshake vs
connection hints must differ. Cost/Risk: capability tests that drive the private frame helpers; move those helpers with
the codec.

### F5 — MEDIUM — DUPLICATION — Worker allowlist is a second copy of capability method names

Evidence: `server.rs:422-485`

```
pub const CAPABILITY_METHODS: &[&str] = &[
    "project.open", ... "session.close",
];
pub const WORKER_METHODS: &[&str] = &[
    "workspace.grants", "worker.exec", ... "session.close",
];
```

`validate_request` (`server.rs:815-844`) requires membership in `CAPABILITY_METHODS` first, then `WORKER_METHODS` for
worker authority. Adding a worker method to only one table fails closed (good) or opens a coordinator-only hole (bad).
Problem: two tables of one set. The strings are also restated as literals in every `call_typed` / `json!` site in
`capability.rs` and in the runtime match (cross-slice `runtime/project.rs`). Fix: one `&[Method]` with an authority bit
(`CoordinatorOnly | Worker`). Generate the two slices. Optional: typed method enum so `capability.rs` cannot spell
`"job.logs"` twice. Cost/Risk: `controller_server.rs` tests that iterate the consts.

### F6 — MEDIUM — STRUCTURE — God files; two connection functions over 150 lines

Evidence: `dto.rs` 2517 lines, no `mod tests`, every DTO/newtype/envelope in one file. `capability.rs` 3023 lines
(production ~1-1792, tests 1793-3023). `spawn_controller_actor` `capability.rs:1074-1231` (~157).
`serve_controller_connection` `server.rs:642-793` (~151). Problem: no seams. `dto.rs` mixes identity newtypes, job
projections, controller commitments, CLI options, and `JsonEnvelope`. `capability.rs` mixes the unix actor, the public
capability types, and a 1.2k-line test module. Fix: split `dto.rs` into `id` / `job` / `commitment` / `options` /
`envelope`. Move `capability.rs` tests to `capability/tests.rs` or `tests/capability_actor.rs`. Split
`spawn_controller_actor` exchange vs spawn; split `serve_controller_connection` handshake vs request loop. Cost/Risk:
`pub use dto::*` in `mod.rs:10` can stay; module paths inside the crate move.

### F7 — MEDIUM — STRUCTURE — unsafe libc without a stated invariant

Evidence: `capability.rs:898` `let current_uid = unsafe { libc::geteuid() };` — no SAFETY comment. Same at
`server.rs:1101`. `peer_credentials/mod.rs:34-42` `getsockopt(SO_TYPE)`; `linux.rs:11-18` `getsockopt(SO_PEERCRED)`;
`macos.rs:6` `getpeereid`. None state the fd-liveness / socklen invariant. Problem: repo rule: `unsafe` without a stated
invariant comment. These are the authorization boundary for the inherited controller socket. Fix: one `verify_peer` (see
F4) with SAFETY: descriptor is a live `OwnedFd`; `geteuid` has no preconditions; `getsockopt`/`getpeereid` require a
valid socket fd and an out-buffer whose `socklen_t` matches the struct. After `SO_PEERCRED`, check returned
`credentials_len` equals `size_of::<ucred>()` (linux.rs does not). Cost/Risk: local to peer_credentials + the two call
sites.

### F8 — LOW — COPIES — StreamInfo re-hashes inline bytes on every ser/de; follow-poll pays it

Evidence: `dto.rs:1088-1098`

```
if let ProtectedOutput::Inline { data } = self.storage.artifact() {
    if self.bytes != data.as_bytes().len() as u64 { ... }
    if self.sha256 != Sha256Digest::compute(data.as_bytes()) { ... }
}
```

Called from `StreamInfo` serialize and deserialize (`dto.rs:1139,1162`) and from `JobInfo::validate`
(`dto.rs:1520-1521`). `poll_job_stream` (`capability.rs:427-447`) on follow-after-eof calls `job.status` and
`serde_json::from_value` into `JobInfo` every 50 ms (`capability.rs:461-465`). Problem: L7/§7.7 — re-validation of
immutable bytes. L0 — SHA-256 evaporates. Regime: not a per-element kernel; it is the follow-logs idle poll (up to 2×64
KiB hashed per status). Once-per-RPC on other paths: note, not a hot-loop finding. Fix: verify at construction/`new`;
ser/de trusts the type. Follow-poll should fetch a terminal flag, not the full `JobInfo` projection. Cost/Risk: any code
that mutates `StreamInfo` fields after `new` would skip the check — there is no such API today (fields are public: make
them private).

### F9 — LOW — TESTS — Affine-ownership guard cannot go red

Evidence: `capability.rs:1979-1985`

```
assert!(needs_drop::<CoordinatorToken>());
assert!(needs_drop::<Coordinator>());
assert!(needs_drop::<WorkspaceHandle>());
assert!(size_of::<CoordinatorToken>() > 0);
```

Problem: PERFORMANCE-HANDBOOK §7.10bb. `size_of::<T>() > 0` is true for every inhabited value type. `needs_drop` is true
because of `Arc` insides, not because the token is affine. Substituting `()` fails `needs_drop`; substituting `String`
passes everything. The test does not pin "consumed exactly once". Fix: delete it. Affine consumption is already
`CoordinatorToken` not `Clone` plus `Cowshed::coordinator` `Arc::ptr_eq` check (`capability.rs:657-667`). Handshake
tests (`capability.rs:2263-2282`) are the real guard. Cost/Risk: none.

### F10 — LOW — STRUCTURE — Session::background is Session::run

Evidence: `capability.rs:1759-1761`

```
pub async fn background(&self, request: ExecRequest) -> Result<JobHandle> {
    self.run(request).await
}
```

Problem: second name, same function. TS `Session` exposes `exec` only (`types.ts:327-330`). Three spellings of one call
(`run` / `background` / TS `exec`). Fix: delete `background`; one method. Update the test that sends two execs through
`run` then `background` (`capability.rs:2408-2423`) to call `run` twice. Cost/Risk: any Rust caller of `background`
(grep at cutover).

### F11 — LOW — DUPLICATION — Lowercase-hex predicates restated five times

Evidence: `GitOid::new` `dto.rs:120-123`; `hex_identifier!` `dto.rs:399-403`; `Sha256Digest::from_hex` `dto.rs:982-985`;
`RevisionTarget::parse_cli` `dto.rs:1945-1948`; `validate_hello` nonce `server.rs:801-804`. All: length +
`is_ascii_digit() || (b'a'..=b'f')`. Problem: one predicate, five copies. `GitOid` allows 40|64; nonce is 64; trace/span
reject all-zero. The _length and zero-policy_ should be parameters; the nibble test should not be rewritten. Fix:
`fn is_lowercase_hex(s: &str) -> bool` next to `hex_nibble`. Call it. Cost/Risk: none.

## Cross-slice questions

- `runtime/project.rs` restates the same RPC method strings in a match. If that slice does not treat
  `CAPABILITY_METHODS` as SSOT, F5 is incomplete.
- Who builds `ExecRecord` from a live job (`job_artifact.rs` / supervisor)? If argv is `to_string_lossy`, F3 is already
  a silent corruption, not just a type fork.
- `GrantSet` / `RepoRule` / `EgressRule` live in `metadata.rs` (CsCoreMetadata). TS `GrantDelta.repos?: string[]`
  matches `RepoRule(pub String)` on the wire only because of `#[serde(transparent)]` — confirm metadata still owns that.
- `packages/cowshed/src/index.ts` typia parsers are the operational consumer of F1; that file is not this slice.
- `cowshed-core` Cargo.toml deps this slice does **not** use (`arrow-*`, `plist`, `notify`, `rcgen`, `walkdir`,
  `x509-parser`, `zeroize`, `getrandom`): other slices. Do not drop them from here.

## Non-findings (checked, clean)

- **DEP-BLOAT (used deps):** `base64` is in-process serde of argv/output (not `base64(1)`). `sha2` is
  `Sha256Digest::compute` on DTO validate. `libc` is SO_PEERCRED/getpeereid/geteuid — the CLI cannot replace an fd-local
  credential query. `serde`/`serde_json`/`tokio`/`bytes`/`async-trait`/`url`/`thiserror` are load-bearing. `uuid` in
  `fresh_nonce` could be `getrandom` + hex, but `uuid` is crate-wide (fsio, git, apfs, supervisor); removing it here
  does not drop the crate. Lockfile: single versions of `base64` 0.22.1, `sha2` 0.10.9, `uuid` 1.23.5, `serde` 1.0.228,
  `libc` 0.2.186.
- **StdinSource frontend mapping:** documented dual spelling with exhaustive seams; not a silent third DTO.
- **peer_credentials:** fail-closed on non-macOS/Linux; uid is the only authorization boundary; gid ignored on purpose.
  `cfg` arms compile.
- **codec module:** one schema for hello/RPC; tests reject unknown fields and bound encode/decode. Client imports it.
- **WORKER_METHODS ⊆ CAPABILITY_METHODS** at the lines read (no live hole today).
- **MAX_JOB_ID = 2^53-1**, frame limits, `CONTROLLER_COMMITMENT_VERSION` are defined once in this crate and imported
  elsewhere.
- **json! / clone on RPC params:** once-per-call, not a hot loop (handbook §4.1 regime). Not a finding.
- **dto.rs has no in-file tests;** contracts live in `cowshed-core/tests/public_api_contracts.rs` (other slice).
  Actor/framing tests in `capability.rs` are behavioral (stdin frames, follow/eof, bounded poll) except F9.
- **LandingCommits as a sum** (Measured vs Indeterminate) is the right data shape; TS matches it.
