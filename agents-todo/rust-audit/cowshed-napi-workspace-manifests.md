# cowshed-napi + workspace manifests

Scope: `packages/cowshed/crates/cowshed-napi/src/lib.rs` (1036), `packages/cowshed/crates/cowshed-napi/Cargo.toml` (20),
`packages/cowshed/crates/cowshed-napi/build.rs` (4), `packages/cowshed/crates/cowshed-escape-tests/src/lib.rs` (1),
`packages/cowshed/crates/cowshed-escape-tests/Cargo.toml` (7), `packages/cowshed/Cargo.toml` (31),
`packages/cowshed/Cargo.lock` (2629), `packages/cowshed/package.json` (237), `packages/cowshed/src/index.ts` (484),
`packages/cowshed/src/types.ts` (353), `packages/cowshed/src/native.ts` (133), `packages/cowshed/src/platform.ts` (12),
`packages/cowshed/src/cli.ts` (8), `packages/cowshed/src/cli-trampoline.ts` (164), `packages/cowshed/src/native.test.ts`
(98), `packages/cowshed/src/cli-trampoline.test.ts` (254). Neighbor reads (duplication only):
`crates/cowshed-core/src/error.rs`, `crates/cowshed-core/src/api/dto.rs`, `crates/cowshed-core/Cargo.toml`,
`crates/cowshed-cli/Cargo.toml`, `crates/cowshed-cli/src/launchd.rs`, `crates/cowshed-gateway/Cargo.toml`.

## Summary

- CRITICAL: `JobInfo` in `types.ts` does not match the JSON napi emits from core (`argv: string[]` vs `{encoding,data}`;
  `exit`/`stdout`/`stderr`/`stdin`/`outputLimit` are `unknown`). `listJobs`/`status`/`wait` will fail typia parse or
  silently skip shape checks.
- HIGH: FFI types restated four times (core DTO, `NapiExecRequest`, `Native*Handle`, `types.ts`). Core serde types are
  the wire SSOT; generate TS from them; generate `native.ts` from napi-derive.
- HIGH: `cowshed-napi` cdylib depends on `cowshed-cli` and therefore the whole core/gateway graph (arrow, hyper, rustls,
  ring, notify, plist, clap). The trampoline already prefers `dist/bin`.
- MEDIUM: `\nnext: `, `MAX_JOB_ID`, platform output dirs, and `~/Library/Application Support/dev.cowshed/bin/cowshed`
  each exist in two spellings.
- MEDIUM: lockfile has 14 multi-version crates (`getrandom` 0.2/0.3/0.4); `cowshed-escape-tests` is an empty workspace
  member; napi parity tests cannot go red.
- LOW: JSON serialize/parse per call; `runCli` errors drop `ErrorCode`; npm `0.1.7-next.0` vs cargo `0.1.0`.
- tokio `full` is not a finding: fs/net/process/signal/io/sync/time/macros/rt are all used in this workspace.
- Every napi export has a TS consumer. `libc` is load-bearing for `fcntl(CLOEXEC)`.

## Findings

### F1 — CRITICAL — SSOT — JobInfo TS type disagrees with the JSON napi actually emits

Evidence: `packages/cowshed/src/types.ts:257-274`

```ts
export interface JobInfo {
  readonly repoId: string;
  readonly workspaceIncarnation: string;
  readonly jobId: number;
  readonly state: JobState;
  readonly argv: readonly string[];
  readonly exit?: unknown;
  readonly stdout: unknown;
  readonly stderr: unknown;
  readonly outputLimit?: unknown;
  readonly stdin: unknown;
}
```

Evidence: `packages/cowshed/crates/cowshed-core/src/api/dto.rs:767-800` and `1476-1492`

```rust
struct CommandArgRef<'a> { encoding: CommandArgEncoding, data: &'a str; }
// serialize: utf8 → {encoding:"utf8",data} / else {encoding:"base64",data}
pub struct JobInfo {
    pub argv: Vec<CommandArg>,
    pub exit: Option<ExitStatus>,      // tagged {kind:"exited"|"signaled", ...}
    pub stdout: StreamInfo,            // {storage, bytes, sha256, summary}
    pub stderr: StreamInfo,
    pub output_limit: Option<OutputLimitInfo>,
    pub stdin: StdinInfo,
}
```

Evidence: `packages/cowshed/src/index.ts:110-111` + `372-373` + `419-420` + `435-436`

```ts
const parseJobInfo = typia.json.createAssertParse<JobInfo>();
const parseJobInfos = typia.json.createAssertParse<JobInfo[]>();
return parseJobInfos(await callNativeAsync(() => this.#native.listJobs()));
return parseJobInfo(await callNativeAsync(() => this.#native.statusJson()));
```

Evidence: `packages/cowshed/crates/cowshed-napi/src/lib.rs:566-572` + `664-669` + `711-716`
(`canonical_json("job list"|"job status", &jobs)`). Problem: copies no longer agree. Core's job DTO serializes `argv` as
`{encoding,data}` objects (never as a JSON string). `ExitStatus`/`StreamInfo`/`StdinInfo`/`OutputLimitInfo` are real
tagged objects. TS types `argv` as `string[]` and the rest as `unknown`. `createAssertParse<JobInfo>()` will reject
every real job listing on `argv`, and cannot go red on drift of the `unknown` fields. This is the FFI seam the slice
exists to catch. Fix: delete the hand-written `JobInfo` (and `JobState`, `ExecRequest.argv`) from `types.ts`. Generate
TS from `cowshed-core::api` serde types (`CommandArg`, `ExitStatus`, `StreamInfo`, `StdinInfo`, `OutputLimitInfo`). Keep
`unknown` illegal. Core DTOs are the single source; napi only `canonical_json`s them. Cost/Risk: TS callers of
`listJobs`/`status`/`wait` must consume `{encoding,data}` argv (or a generated branded type). Any code that assumed
`string[]` was already broken against the wire.

### F2 — HIGH — SSOT — Public API, native handles, napi glue, and core DTOs are four restatements

Evidence: `packages/cowshed/crates/cowshed-napi/src/lib.rs:108-128` (`NapiExecRequest`) vs
`packages/cowshed/crates/cowshed-core/src/api/dto.rs:1841-1850` (`ExecRequest`) vs
`packages/cowshed/src/types.ts:202-213` vs `packages/cowshed/src/native.ts:45-54`.

```rust
struct NapiExecRequest {
    argv: Vec<String>,  // core is Vec<CommandArg> (lossy vs OsString)
    stdin: Option<String>,
    stdin_workspace_path: Option<WorkspacePath>,
    // ... cwd, mode, env, trace, stdout_copy, stderr_copy
}
```

Evidence: `packages/cowshed/src/types.ts:1-8` vs `packages/cowshed/crates/cowshed-core/src/error.rs:8-52` (same 7
kebab-case codes, restated). Evidence: method-name restatement, e.g. `grantsJson`/`infoJson`/`statusJson` in
`native.ts:21-23,54,64` matching `lib.rs:613,879,664`, then re-wrapped to `grants()`/`info()`/`status()` in `index.ts`.
Problem: one concept, four types. `NapiExecRequest.argv: Vec<String>` cannot carry the non-UTF-8 argv the CLI path
preserves (`CommandArg`). `ErrorCode` will drift the moment core adds a variant. `Native*Handle` is a hand-copied napi
method list. Attach is the only options object that uses `typia.createAssertEquals`; every other encoder uses
`typia.assert`, so extra JS fields can survive into `deny_unknown_fields` rust parse. Fix: generate `types.ts` from core
serde (ts-rs, or a `cargo test` that dumps JSON Schema). Generate `native.ts` `NativeModule`/`Native*Handle` from
napi-derive `.d.ts` (`napi build --dts`). Keep one hand-written exception: stdin flattening (`stdin` XOR
`stdinWorkspacePath` → `StdinSource`), next to the existing `wire_stdin_spelling` match. Delete `NapiExecRequest` if
core can deserialize the JS object; if not, make `NapiExecRequest` a `#[serde(transparent)]` newtype over the generated
wire struct plus the two stdin fields. Cost/Risk: one codegen step in the napi build; `index.ts` wrappers stay as the
capability/JSON parse layer until objects replace strings.

### F3 — HIGH — DEP-BLOAT — cowshed-napi cdylib links cowshed-cli and therefore gateway/arrow/hyper/ring

Evidence: `packages/cowshed/crates/cowshed-napi/Cargo.toml:10-18`

```toml
cowshed-cli = { path = "../cowshed-cli" }
cowshed-core.workspace = true
libc = "0.2"
napi.workspace = true
```

Evidence: `packages/cowshed/crates/cowshed-napi/src/lib.rs:924-932` (only `cowshed-cli` use is `cowshed_cli::run::run`).
Evidence: `packages/cowshed/Cargo.lock:343-444` (`cowshed-napi` → `cowshed-cli` + `cowshed-core`; `cowshed-core` →
`arrow-*`, `cowshed-gateway`, `notify`, `plist`, `rcgen`, `walkdir`; `cowshed-gateway` → `hyper`, `rustls`, `ring` via
`rcgen`/`tokio-rustls`). Evidence: `packages/cowshed/src/cli-trampoline.ts:77-89` already prefers
`dist/bin/<platform>/cowshed` and only then napi `runCli`. Problem: lockfile is 272 package entries / 256 unique names.
`cowshed-napi`'s recorded closure is 254 names (that mix includes `napi-build` and `cowshed-core`'s dev-dep `proptest`;
the linked cdylib still contains clap, toml, arrow, hyper, rustls, ring, notify, plist). The Node addon is a second copy
of the CLI+controller+gateway stack. Shelling out to the packaged binary is the path the trampoline already takes.
`libc` is not this class of bloat (see Non-findings). Fix: drop `cowshed-cli` from `cowshed-napi`. Make `runCli` spawn
`dist/bin/.../cowshed` (the trampoline already resolves that path) or a `cowshed-cli` `rlib` feature that does not pull
gateway. Separately, `cowshed-core`'s `cowshed-gateway`/`arrow-*` deps are why the _client_ addon still links TLS+Arrow
even after dropping CLI — that cut belongs to core (cross-slice). Cost/Risk: in-process `runCli` parity (stdin/stdout
inherit, no extra process) goes away unless a `cowshed-cli-dispatch` crate is split out with no gateway/arrow. The
trampoline already treats napi as last resort.

### F4 — MEDIUM — SSOT — error-hint wire delimiter restated in Rust and TS

Evidence: `packages/cowshed/crates/cowshed-napi/src/lib.rs:74-76`

```rust
// `\nnext: ` is the wire delimiter `NEXT_HINT_MARKER` in src/index.ts splits on; the two
// spellings must stay byte-identical or hints silently merge back into messages.
let reason = format!("{message}\nnext: {hint}");
```

Evidence: `packages/cowshed/src/index.ts:116-133`

```ts
const NEXT_HINT_MARKER = '\nnext: ';
const marker = error.message.lastIndexOf(NEXT_HINT_MARKER);
```

Problem: the comment admits the bug class. If either spelling changes, hints merge into messages and `CowshedError.hint`
is wrong. CLI human output uses the same `next: ` convention (`cowshed-cli` tests), so this is a third copy of one
delimiter. Fix: one `pub const NEXT_HINT_MARKER: &str = "\nnext: ";` in `cowshed-core::error`. Napi formats with it; TS
must not restated it — either napi exposes `hint` as a real property (napi `Error` + extra field) so the delimiter dies,
or generate the TS constant from the rust const in the napi build. Cost/Risk: changing the delimiter is a wire break for
already-built addons; exposing `hint` as a field is the cleaner cut and deletes the split.

### F5 — MEDIUM — SSOT — MAX_JOB_ID restated as a raw 2^53-1 literal

Evidence: `packages/cowshed/crates/cowshed-napi/src/lib.rs:576-587`

```rust
if !id.is_finite() || id.fract() != 0.0 || id < 1.0 || id > ((1_u64 << 53) - 1) as f64 {
    return Err(AddonFailure::usage(format!("invalid job id {id}"), ...));
}
let id = JobId::new(id as u64).map_err(...)?;
```

Evidence: `packages/cowshed/crates/cowshed-core/src/api/dto.rs:23` + `74-76`
(`pub const MAX_JOB_ID: u64 = (1_u64 << 53) - 1;` / `JobId::new` already checks `1..=MAX_JOB_ID`). Problem: the f64 gate
duplicates `MAX_JOB_ID`. `JobId::new` already rejects the range. Two numbers to disagree. Fix: keep the finite/integer
check (JS `number` is not a u64); compare against `MAX_JOB_ID as f64` and then `JobId::new`. Do not restate the shift.
Cost/Risk: napi-only; `JobId::new` is already the authority.

### F6 — MEDIUM — SSOT — host-stable binary path restated in the TS trampoline

Evidence: `packages/cowshed/src/cli-trampoline.ts:64-71`

```ts
const stableBinary = join(options.home ?? homedir(), 'Library', 'Application Support', 'dev.cowshed', 'bin', 'cowshed');
```

Evidence: `packages/cowshed/crates/cowshed-cli/src/launchd.rs:16-27` (`STABLE_SUPPORT_DIRECTORY = "dev.cowshed"`,
`STABLE_BINARY_DIRECTORY = "bin"`, comment names `~/Library/Application Support/dev.cowshed/bin/<name>`). Problem: the
trampoline's daemon-verb routing is load-bearing (`gateway`/`sccache` must hit the installed copy). The path is owned by
`HostStableExecutable` in CLI. A rename of the support directory will leave the trampoline starting a missing binary and
falling through. Fix: export one function from rust (`cowshed-cli` or core) that returns the stable path, and have the
trampoline call `runCli`/`a tiny napi getter` rather than restating `Library/Application Support/dev.cowshed`. Or
generate the path into `platform.ts` from the rust const at build. Cost/Risk: trampoline currently avoids loading napi
for native bins (comment at `cli-trampoline.ts:100-103`); a generated TS const keeps that property. Do not load napi
just to ask for a path.

### F7 — MEDIUM — SSOT — platform → dist directory map restated in package.json

Evidence: `packages/cowshed/src/platform.ts:1-12` (comment: "mirrored by the napi --output-dir literals in
package.json"). Evidence: `packages/cowshed/package.json:26-33` (`napi.targets`) and `69`, `102`, `125`, `151`
(`--output-dir dist/bin/darwin-arm64` / `darwin-x64` / `linux-arm64-gnu` / `linux-x64-gnu`). Problem: four rustc
targets, four output dirs, and `platformDirectory()` must stay aligned. Adding a musl or Windows target in
`napi.targets` without `platform.ts` makes `loadNativeModule` throw `Unsupported Cowshed native target` after a
successful build. Fix: `platform.ts` stays the runtime SSOT (already the stated intent). Generate the `--output-dir`
strings and `napi.targets` from the same table (a 4-row const in `platform.ts` consumed by the nx commands, or a tiny
`scripts/native-targets.ts` both import). Delete the comment about mirroring. Cost/Risk: nx command strings in
`package.json` become a loop over the table; cache inputs stay the same.

### F8 — MEDIUM — DEP-BLOAT — 14 duplicate crate versions in Cargo.lock

Evidence: `packages/cowshed/Cargo.lock` package entries (272 total, 256 unique names). Multi-version:

- `getrandom` 0.2.17 (`:640`), 0.3.4 (`:651`, cowshed-core pins `"0.3"`), 0.4.3 (`:663`)
- `bit-vec` 0.8.0 (`:194`) and 0.9.1 (`:200`)
- `hashbrown` 0.16.1 (`:705`) and 0.17.1 (`:711`)
- `r-efi` 5.3.0 (`:1449`) and 6.0.0 (`:1455`)
- `windows-sys` 0.52.0 / 0.60.2 / 0.61.2 and matching `windows-targets` / `windows_*` 0.52.6 vs 0.53.x (cross-compile
  noise) Inventory (unique names, excluding self): cowshed-napi 254, cowshed-cli 241, cowshed-core 231, cowshed-gateway
  205, tokio 33, napi 48, hyper 55, ring 22, rustls 28, arrow-array 62, arrow-ipc 68, rcgen 63,
  rustls-platform-verifier 60. No `openssl`, no `aws-lc-rs`. `openssl-probe` is present via rustls-native-certs.
  `proptest` is a cowshed-core _dev_-dep listed on the lock package (does not link into the cdylib). Problem: three
  `getrandom` majors is real compile/link cost. Windows triples are unused on the four shipped unix targets but still
  resolve. `tokio` 1.52.3 in the lock is built with `parking_lot` + `signal-hook-registry` + `mio`
  (`Cargo.lock:1987-1996`) because workspace `features = ["full"]` (`packages/cowshed/Cargo.toml:24`). Fix: pin
  `getrandom` to one major via `[workspace.dependencies]` / `[patch]` once core's `"0.3"` can move. Leave windows-*
  alone (target cfg). Do not drop tokio `full` (see Non-findings). Cost/Risk: `getrandom` 0.2 is typically `ring`;
  unifying may need a rustls/ring feature bump owned by gateway.

### F9 — MEDIUM — TESTS — napi parity tests cannot go red on a missing export

Evidence: `packages/cowshed/crates/cowshed-napi/src/lib.rs:969-1034`

```rust
fn napi_export(command: &Command) -> &'static str { match command { ... } }
assert!(!napi_export(&parsed.command).is_empty(), "CLI command {argv:?} has no N-API export");
```

Evidence: `packages/cowshed/crates/cowshed-napi/src/lib.rs:957-967` (`every_core_stdin_variant_has_a_wire_verdict`
asserts only `Empty` and `Inline`; `WorkspaceFile` and `Stream` are in the match but have no expected-string assert).
Problem: PH-7.10bb substitution test. Every `Command` arm returns a non-empty literal, so `!is_empty()` never fails for
a mapped command. Adding a CLI verb requires a match arm (compile) but not a sample in `samples`; a missing sample stays
green. The stdin test does not pin `WorkspaceFile` → `"stdinWorkspacePath"`; renaming that JS field would stay green.
Fix: make `napi_export` return `Option<&'static str>` and `assert_eq` against a table of `(argv, export)`. Assert all
four `StdinSource` spellings. Drop the `is_empty` check. Cost/Risk: napi test module only.

### F10 — MEDIUM — STRUCTURE — cowshed-escape-tests is an empty workspace member

Evidence: `packages/cowshed/crates/cowshed-escape-tests/src/lib.rs:1`
(`//! Stub: implemented per specs/cowshed (see repo root).`) Evidence:
`packages/cowshed/crates/cowshed-escape-tests/Cargo.toml:1-7` (no `[dependencies]`, no `[lib]`, no tests). Evidence:
`packages/cowshed/Cargo.toml:3-9` (listed in `members`). Evidence: `packages/cowshed/Cargo.lock:391-392`. Problem: a
workspace member with one comment and zero code. It still participates in `cargo test --workspace` / lock resolution.
Greenfield: no stub crates. Fix: delete `crates/cowshed-escape-tests` and drop it from `members`. Re-add only with a
real test binary. Cost/Risk: none if nothing imports it (lock shows no dependents).

### F11 — MEDIUM — STRUCTURE — `run-cli` nx target is hardcoded to darwin-arm64

Evidence: `packages/cowshed/package.json:72-82`

```json
"run-cli": {
  "dependsOn": ["^build", "cli-arm64-macos"],
  "command": "dist/bin/darwin-arm64/cowshed {args}"
}
```

Evidence: `packages/cowshed/package.json:6-8` (`"cowshed": "nx run cowshed:run-cli --"`). Problem: the package ships
four CLI targets. `npm run cowshed` / `nx run cowshed:run-cli` only builds and execs `darwin-arm64`. Linux/x64 hosts get
a missing binary. The trampoline (`cli.ts` → `resolveCliBackend`) already picks the right `dist/bin/<platform>/`. Fix:
delete `run-cli`'s hardcoded path; point the npm `cowshed` script at `dist/ts/cli.js` (the trampoline). Keep per-arch
`cli-*` targets as build-only. Cost/Risk: local `nx run cowshed:run-cli` on this machine still works via the trampoline
if `cli-arm64-macos` ran first; linux CI must depend on the matching `cli-*` target instead of `run-cli`.

### F12 — LOW — COPIES — JSON string is the FFI value; log drain grows unbounded

Evidence: `packages/cowshed/crates/cowshed-napi/src/lib.rs:94-106` (`canonical_json` / `parse_json`) and every
`*_json: String` argument in `Coordinator`/`WorkspaceHandle` (`lib.rs:298-482`, `538-610`). Evidence:
`packages/cowshed/src/index.ts:167-214` (typia assert → `JSON.stringify` → rust `from_str` → `to_string` → typia
`assertParse`). Evidence: `packages/cowshed/crates/cowshed-napi/src/lib.rs:158-164`

```rust
let mut output = Vec::new();
while let Some(chunk) = logs.next().await {
    output.extend_from_slice(&chunk);
}
```

Regime: per JS API call / per job-log drain, not a per-byte inner loop. Do not treat getter `to_string()`
(`lib.rs:519-520`, `861-862`) as a finding. Problem: L0 evaporating work — the typed value is serialized to a `String`
and re-parsed on the other side. `read_all_logs` has no reservation (L4) on a stream whose size is known to the job DTO
(`StreamInfo.bytes`). Fix: pass napi objects via `napi` serde (feature `serde-json`) using the same core types, deleting
the string round-trip. For logs, `Vec::with_capacity(stream.bytes)` when status is already in hand, or stream chunks to
JS without concatenating. Cost/Risk: changes the native method signatures (`optionsJson: string` → object); `native.ts`
regenerates (F2). Log capacity is a one-line follow-up.

### F13 — LOW — TESTS — native tests assert rendered English

Evidence: `packages/cowshed/src/native.test.ts:50-59`

```ts
expect(handshake.message).toContain('not a stream socket');
expect(consumed.message).toContain('already been consumed');
```

Problem: tests on rendered strings, not `ErrorCode` + a typed field.
`requireCowshedError(..., 'usage'|'environment-missing'|'conflict')` is the real oracle; the `toContain` pins copy. Fix:
keep the code asserts; drop the message substrings or assert a stable `code` + `hint` only. Cost/Risk: napi-test only.

### F14 — LOW — STRUCTURE — runCli failures bypass CowshedError

Evidence: `packages/cowshed/crates/cowshed-napi/src/lib.rs:924-936`

```rust
.map_err(|error| napi::Error::from_reason(format!("cowshed CLI task failed: {error}")))?
.map_err(|error| napi::Error::from_reason(format!("cowshed CLI runtime failed: {error}")))
```

Evidence: `packages/cowshed/src/index.ts:97-123` (`NativeError` requires `code: ErrorCode`; anything else is rethrown
raw). Problem: `spawn_blocking` join errors and `runtime::Builder` IO errors become untyped napi errors. Operational,
rare, but they skip `normalizeNativeError`. Fix: map both through `AddonFailure::internal` / `to_napi_error` so the JS
side always sees `CowshedError`. Cost/Risk: napi `runCli` only.

### F15 — LOW — SSOT — npm package version is not the cargo workspace version

Evidence: `packages/cowshed/package.json:3` (`"version": "0.1.7-next.0"`). Evidence: `packages/cowshed/Cargo.toml:11-12`
(`version = "0.1.0"`). Problem: two version numbers for one package. Not a runtime bug. Fix: one version. Either cargo
workspace version is generated from `package.json` in the napi build, or stop advertising `0.1.0` on unpublished crates
(`publish = false` already). Cost/Risk: none for unpublished crates.

## Cross-slice questions

- `cowshed-core` (`crates/cowshed-core/Cargo.toml:14-21`) depends on `cowshed-gateway` and `arrow-*`. That is why the
  napi _client_ cdylib contains hyper/rustls/ring/arrow even after F3 drops `cowshed-cli`. Core-API / gateway-inventory
  slices: can the capability client compile without gateway?
- `RunSandboxMode` exists in both `crates/cowshed-core/src/sandbox.rs:25-28` and
  `crates/cowshed-core/src/api/dto.rs:1819-1825`. Not this slice's file; flag for the DTO owner.
- Stable-path constants live in `crates/cowshed-cli/src/launchd.rs` (F6). CLI-services slice owns the rust side of that
  rename.
- `CommandArg` / `JobInfo` serde (`dto.rs`) is the SSOT F1/F2 generate from. Do not "fix" TS independently of that
  crate.

## Non-findings (checked, clean)

- **tokio `full`**: workspace `Cargo.toml:24` enables it. Across cowshed-cli/core/gateway this tree uses `tokio::fs`,
  `tokio::net::UnixStream`, `tokio::process::Command`, `tokio::signal`, `tokio::io::{stdin,AsyncReadExt,AsyncWriteExt}`,
  `tokio::sync::{mpsc,oneshot}`, `tokio::time`, `tokio::select!`, `#[tokio::main]`/`#[tokio::test]`,
  `spawn`/`spawn_blocking`, `runtime::Builder`. Every `full` feature except the impl-detail `parking_lot` is referenced.
  Not a bloat finding.
- **libc in cowshed-napi**: `fcntl(F_GETFD/F_SETFD/FD_CLOEXEC)` + `STDERR_FILENO` (`lib.rs:167-183`, `219`). Thin FFI,
  SAFETY comments present, no openssl-class drag. Keep. rustix is already in the lock via tokio but swapping would not
  delete a subtree.
- **napi features**: `default-features = false, features = ["napi6", "tokio_rt"]` (`Cargo.toml:19`). Appropriate.
- **unsafe**: `from_raw_fd` / `fcntl` sites (`lib.rs:169-178`, `202-212`, `229-230`) all have SAFETY comments citing
  ownership. No bare `unwrap`/`expect` on operational napi paths (`expect` only in `parity_tests` parse).
- **Export coverage**: every `#[napi]` item is consumed. Map:
  `coordinatorEndpoint`/`openProject`/`connectCoordinator`/`runCli` → `index.ts:457-483` + `native.ts:76-80` +
  trampoline `runNapiFallback`.
  `Coordinator.{adopt,create,fork,rename,moveCheckout,grant,revoke,rebase,land,restore,detach,resize,remove,gc,doctor,worker}`
  → `CoordinatorImpl`. `WorkspaceHandle.{name,mountPath,exec,shell,listJobs,job,checkpoint,push,grantsJson}` →
  `WorkspaceHandleImpl`. `Session.{isNamed,exec}` → `SessionImpl`.
  `JobHandle.{id,statusJson,readLogs,attach,detach,wait,kill}` → `JobHandleImpl`. `JobAttachment.detach` →
  `JobAttachmentImpl`. `Project.{repoId,gitRoot,main,workspace,workspaceAt,path,listWorkspaces}` → `ProjectImpl`.
  `WorkspaceRef.{name,mountPath,infoJson,attach,grantsJson}` → `WorkspaceRefImpl`. No unused pub napi surface.
- **ErrorCode spellings currently match**
  (`internal`/`usage`/`not-found`/`conflict`/`environment-missing`/`sandbox-denied`/`integrity`) — restated, not yet
  diverged (F2).
- **Getter `to_string`/`to_owned`**: once per JS property access, not a hot loop.
- **cowshed-escape-tests purpose**: placeholder for a future escape-hatch test crate; no implementation.
- **No openssl / aws-lc** in this lock. ring is pulled by gateway rcgen/rustls, not by napi's own manifest.
- **cli-trampoline tests** assert backend _paths and exit codes_, not rendered help text. Fine.
- **build.rs** is the required `napi_build::setup()` one-liner.
