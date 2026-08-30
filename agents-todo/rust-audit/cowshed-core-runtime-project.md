# cowshed-core/runtime/project.rs

Scope: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs` (10762 lines),
`packages/cowshed/crates/cowshed-core/src/runtime/mod.rs` (12 lines)

## Summary

- 10762-line god file: actor, wire DTOs, native host, lifecycle verbs, identity move, secrets, git, doctor, and tests in
  one translation unit.
- `DEFAULT_LANDING_BRANCH` is not the single source for rebase's default onto: `"main"` is restated in two constructions
  that will not follow a constant change.
- `SandboxConfig` is built twice (`ensure_supervisor_for` vs `supervisor_sandbox`); grant-advance and first start can
  diverge.
- Every verb re-derives the full workspace inventory (`authoritative` / `snapshots` / `current`); worker RPCs pay it
  twice.
- Retired-main path candidates hardcode `.cowshed/mnt` and `/private/cowshed/store/mnt` instead of `STORE_ROOT` / host
  mount-root.
- Stdout/stderr exists as three enums; exec mode as a third wire copy of `RunSandboxMode`.
- Seven identical `workspace_params()` clone methods; two marker readers with different error policies.
- `renameatx_np` is `unsafe` without a SAFETY comment (the neighboring `kill(0)` has one).
- `utc_timestamp` spawns `/bin/date` while checkpoint labeling already uses `SystemTime`.
- Natural seams named in F1. No crate-level dep-bloat in this slice; `url`/`bytes`/`libc`/`async-trait` earn their
  weight here.

## Findings

### F1 — HIGH — STRUCTURE — 10762-line file is the largest in the repo; split on the seams already visible in the types

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:1-10762` (whole file). Natural blocks:

| Lines                 | Seam                                                                   |
| --------------------- | ---------------------------------------------------------------------- |
| 1–384                 | public types, `ProjectRuntime` open/start                              |
| 386–993               | `ProjectActor` method router                                           |
| 1064–1403             | JSON wire DTOs + `decode_exec_request`                                 |
| 1411–3650             | `NativeProjectRuntimeHost` helpers (`open`, inventory, repair, fences) |
| 4334–6648             | `impl ProjectRuntimeHost` (all verbs)                                  |
| 3713–4228             | identity-namespace move + checkout relocate                            |
| 6650–7367             | binding derive/heal/persist                                            |
| 7370–7681             | adopt secret policy / quarantine                                       |
| 7901–8282             | git spawn, rebase rollback, sandbox construction                       |
| 8656–9002             | removal refusals + doctor finding builders                             |
| 4257–4331, 7683–10762 | in-file tests                                                          |

`ProjectRuntimeHost` itself is a 40-method trait (`84–228`). Individual verbs exceed ~100 lines: `open` `1620–1908`,
`remove` `5492–5728`, `doctor` `6216–6476`, `move_checkout` `4908–5069`, `change_repo_id` `5081–5254`, `create`
`4506–4650`, `land` `5992–6133`, `ensure_supervisor_for` `3448–3567`. Problem: one module owns the controller actor, the
macOS substrate host, JSON protocol, Git, secrets, and doctor. There is no second convention yet; every new verb lands
here. Review, bisect, and incremental compile all pay the whole file. Fix: split along the table above into
`runtime/project/{types,actor,wire,host,lifecycle,checkout,binding,secrets,git_ops,doctor,removal}.rs`. Keep `mod.rs` as
the public re-export (`ProjectRuntime`, `ProjectRuntimeHost`, `DEFAULT_LANDING_BRANCH`). Move tests next to the module
they cover. Do not introduce a facade trait; `ProjectRuntimeHost` already is the seam. Cost/Risk: mechanical `mod`
split; no behavior change if re-exports stay. Touch every in-file `super::` and `cfg(test)` module. Other slices keep
importing `cowshed_core::runtime::project::*`.

### F2 — HIGH — SSOT — rebase default-onto restates `"main"` instead of `DEFAULT_LANDING_BRANCH`

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:38-43`

```
/// One constant for two questions that must never disagree: where `land` merges by default, and
/// which branch `rm` requires to contain a workspace's head before destroying its object store.
pub const DEFAULT_LANDING_BRANCH: &str = "main";
```

`packages/cowshed/crates/cowshed-core/src/runtime/project.rs:3037-3041` (land/rm measurement uses the constant)

```
let target = crate::landing::resolve_target(&main_mount, DEFAULT_LANDING_BRANCH).await;
Ok(NativeLandedState {
    branch: DEFAULT_LANDING_BRANCH.to_owned(),
```

`packages/cowshed/crates/cowshed-core/src/runtime/project.rs:5956-5963` (rebase does not)

```
let default_onto = if git_worktree {
    "main".to_owned()
} else {
    let main_remote = crate::git::GitRepository::from_root(&root)
        .configure_main_remote(&main_mount)
        .await?;
    fetch_remote = Some(main_remote.remote_name().to_owned());
    format!("{}/main", main_remote.remote_name())
};
```

Land's default already uses the constant (`6009-6012`: `unwrap_or_else(|| DEFAULT_LANDING_BRANCH.to_owned())`). Problem:
the comment on the constant says land and rm must never disagree. Rebase's default destination is the third consumer of
the same branch name and is spelled as a bare `"main"` twice (git-worktree onto, and the `{remote}/main` tracking ref).
Changing `DEFAULT_LANDING_BRANCH` would make `land`/`rm` and `rebase` target different branches. Values agree today
(`"main"`); the copies are the bug waiting to happen. Fix: git-worktree arm `DEFAULT_LANDING_BRANCH.to_owned()`;
tracking-ref arm `format!("{}/{DEFAULT_LANDING_BRANCH}", main_remote.remote_name())`. Single source remains
`DEFAULT_LANDING_BRANCH` in this file (CLI already aliases it as `LANDING_TARGET_BRANCH`). Cost/Risk: rebase default
only. Tests that pin `main/main` as a remote-tracking spelling stay valid while the constant is `"main"`.

### F3 — HIGH — DUPLICATION — `SandboxConfig` is assembled twice and can disagree

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:3496-3537` (`ensure_supervisor_for`, first start)

```
let sandbox = crate::sandbox::SandboxConfig {
    home: self.home.clone(),
    mount_root: self.layout.project().host_mount_root.clone(),
    workspace_mount: mount.clone(),
    exec_temp_dir: self.layout.project().quarantine.join(...),
    port_block,
    mode: crate::sandbox::RunSandboxMode::ReadWrite,
    grants: crate::sandbox::SandboxGrants { read: ..., write: ..., egress: ... map ... },
    allowed_unix_sockets: crate::sandbox::nix_daemon_socket().into_iter().chain([crate::sandbox::sccache_server_socket()]).collect(),
    additional_denies: vec![self.layout.project().project_root.clone(), self.telemetry_root.clone()],
    git_worktree_repository: git_worktree_repository(&current.metadata, self.workspace_mount_path(&main_name())?),
};
```

`packages/cowshed/crates/cowshed-core/src/runtime/project.rs:8235-8281` (`supervisor_sandbox`, used only from `grant` at
`5852-5859`) Problem: grant-advance rebuilds the sandbox through `supervisor_sandbox`; every other start inlines the
same struct. A future deny, socket, or grant-field added to one site is silently absent from the other. That is a live
sandbox-policy fork, not a style issue. Fix: delete the inline block in `ensure_supervisor_for`; call
`supervisor_sandbox(&self.home, &self.layout, &self.telemetry_root, &current, mount, main_mount)`. One function, one
policy. Cost/Risk: `ensure_supervisor_for` only. Behavior unchanged if the two copies still match today (they do,
field-for-field, as read).

### F4 — HIGH — COPIES — full inventory is re-derived per verb, and worker RPCs pay it twice

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:2212-2222`

```
async fn current(&self, name: &WorkspaceName) -> Result<NativeWorkspace> {
    self.authoritative()
        .await?
        .into_iter()
        .find(|workspace| workspace.derived.workspace.name() == name)
```

`authoritative_with_project_root_validation` (`2157-2209`) lists every substrate workspace, then blocking-reads every
sidecar. `snapshots` (`4383-4389`) does the same plus a `snapshot()` map.

Worker path, `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:962-970` then `6522-6534`:

```
async fn require_scoped_workspace(...) {
    ...
    let snapshots = self.host.snapshots().await?;  // inventory #1
    let snapshot = find_workspace(&snapshots, &params.workspace)?;
    self.validate_worker_snapshot(authority, snapshot)
}
async fn exec(...) {
    let current = self.current(&workspace).await?;  // inventory #2
    ...
    self.ensure_supervisor(&workspace).await?  // current() again inside
```

Regime: per coordinator/worker RPC, not a per-byte hot loop. This is the controller's inner loop: APFS list + one
sidecar read per workspace, discarded after the name match (Byproduct L0 — evaporating work). `create` (`4524-4539`)
even calls `authoritative()` for an existence check and then `current()` for the source. Problem: the histogram of
workspaces IS the index. Paying a full scan to answer "give me `raven`" throws the scan away. Worker exec/logs/kill pay
two (sometimes three) scans before the supervisor handle is used. Fix: `current` should resolve one workspace from
layout+sidecar (the path is closed-form: `layout.canonical_image(name, format)`), not filter a full list.
`require_scoped_workspace` should call `host.workspace_snapshot(name)` once and pass that incarnation into
`exec`/`read_log` so the host does not re-list. Cache the last `authoritative()` vector on the actor-owned host only for
verbs that already need the whole set (`snapshots`, `doctor`, `gc`). Cost/Risk: every verb that calls `current`. Must
keep the marker/sidecar mismatch checks that `authoritative` currently runs; move them onto the single-workspace read.

### F5 — MEDIUM — SSOT — retired-main targets hardcode mount-root spellings the bootstrap/host-config already own

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:4056-4072`

```
let candidates = [
    current_main_mount.to_owned(),
    invoking_home
        .join(".cowshed/mnt")
        .join(repo_id.owner())
        .join(repo_id.repo())
        .join("main"),
    Path::new("/private/cowshed/store/mnt")
        .join(repo_id.owner())
        .join(repo_id.repo())
        .join("main"),
];
```

Same file, `2657-2658`, hardcodes the store root in a usage hint:
`"choose a destination outside /private/cowshed/store and outside the current checkout"`.
`packages/cowshed/crates/cowshed-core/src/storage/bootstrap.rs:31` is
`pub const STORE_ROOT: &str = "/private/cowshed/store";`. Host mount-root default is `home.join(".cowshed/mnt")` in
`storage/host_config.rs` (other slice). Problem: two historical layouts are restated as string literals, including the
workspace leaf `"main"` instead of `main_name().as_str()`. A host whose mount-root is not `~/.cowshed/mnt` still has
these candidates injected. Tests pin the literals (`9677-9695`). Fix: candidates =
`[current_main_mount, invoking_home.join(HostConfig::default_mount_relative()).join(owner).join(repo).join(main_name().as_str()), Path::new(STORE_ROOT).join("mnt").join(...)]`.
Hint at `2658` should format `self.descriptor.store_root`, not a literal. Cost/Risk: `known_retired_main_targets` + its
tests in this file; HostConfig default relative path is owned by the storage slice.

### F6 — MEDIUM — DUPLICATION — stdout/stderr and exec-mode exist as three types each

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:64-67` `RuntimeJobStream { Stdout, Stderr }`

`1274-1277` `JobStreamWire { Stdout, Stderr }` with a 1:1 match at `869-871`.

`6633-6636` maps `RuntimeJobStream` again onto `supervisor::OutputStream`.

`1319-1322` `ExecModeWire { ReadWrite, ReadOnly }` mapped at `1380-1382` onto `dto::RunSandboxMode`. A third
`sandbox::RunSandboxMode` exists (`sandbox.rs:25-28`, other slice). Problem: three identical two-variant enums for one
concept. A fourth wire spelling cannot go red against the supervisor type. `ExecModeWire` is a hand-restated JSON DTO of
`RunSandboxMode`. Fix: deserialize `job.logs` stream as `RuntimeJobStream` (serde rename lowercase) and delete
`JobStreamWire`. Deserialize exec `mode` as `dto::RunSandboxMode` and delete `ExecModeWire`. Map to `OutputStream` /
`sandbox::RunSandboxMode` in one `From` impl each — ideally one `RunSandboxMode` for the crate (cross-slice). Cost/Risk:
wire JSON is unchanged if serde renames match. Supervisor slice owns `OutputStream`.

### F7 — MEDIUM — DUPLICATION — seven identical `workspace_params()` clone methods

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:1105-1111`, `1191-1197`, `1209-1215`,
`1227-1233`, `1245-1251`, `1263-1269`, `1291-1297`. Each is:

```
fn workspace_params(&self) -> WorkspaceParams {
    WorkspaceParams {
        repo_id: self.repo_id.clone(),
        workspace: self.workspace.clone(),
    }
}
```

`WorkerScope`, `SessionParams`, `JobParams`, `WorkerCheckpointParams`, `WorkerPushParams`, `LogsParams` all carry the
same `(repo_id, workspace, workspace_incarnation)` prefix; `WorkspaceOptionsParams` clones two of the three. Problem: a
table of wire structs that should be one scoped-workspace header plus a payload. `RepoId`/`WorkspaceName` clones are
per-RPC (regime: once per method, not a hot loop) but the duplication is the finding. Fix: one
`struct WorkspaceScope { repo_id, workspace, workspace_incarnation }` flattened with `#[serde(flatten)]` into the
payloads that need extra fields. One `workspace_params()` on that type. Delete the six copies. Cost/Risk: wire JSON
field names stay camelCase via flatten. Actor methods that move `params.workspace` stay valid.

### F8 — MEDIUM — DUPLICATION — two marker readers; the portable one swallows every error

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:6808-6818`

```
async fn marker_project_root(path: &Path) -> Result<Option<PathBuf>> {
    let marker_path = path.join(crate::storage::WORKSPACE_MARKER_PATH);
    let marker = crate::storage::lifecycle::dispatch_blocking(move || {
        match crate::metadata::WorkspaceMarker::read_from(&marker_path) {
            Ok(marker) => Ok(Some(marker)),
            Err(_) => Ok::<_, CowshedError>(None),
        }
    })
```

`packages/cowshed/crates/cowshed-core/src/runtime/project.rs:6962-6973` (`workspace_origin_from_marker`) treats only
`NotFound` as `None` and maps every other `MetadataError` to integrity. Problem: same file, same path
(`WORKSPACE_MARKER_PATH`), two parsers. `project_open` (`477-479`) uses the swallowing reader: a corrupt marker is "no
marker", so a workspace cwd with a damaged `.cowshed/workspace.json` fails the belongs-to-project check instead of
naming the damage. `workspace_origin_from_marker` is the honest policy. Fix: delete `marker_project_root`.
`project_open` should call `workspace_origin_from_marker` (or a shared
`read_marker(path) -> Result<Option<WorkspaceMarker>>` that only maps NotFound to None) and then take
`origin.project_root`. Cost/Risk: `project.open` from a workspace with a corrupt marker becomes an integrity error
instead of "path does not belong". That is the correct refusal.

### F9 — MEDIUM — STRUCTURE — `renameatx_np` is unsafe with no stated invariant

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:4125-4147`

```
fn swap_checkout_paths(left: &Path, right: &Path) -> std::io::Result<()> {
    ...
    const RENAME_SWAP: u32 = 0x0000_0002;
    let result = unsafe {
        libc::renameatx_np(
            libc::AT_FDCWD,
            left.as_ptr(),
            libc::AT_FDCWD,
            right.as_ptr(),
            RENAME_SWAP,
        )
    };
```

Neighboring `process_is_alive` (`1452-1454`) documents its `unsafe { libc::kill(pid, 0) }`. This site does not. Problem:
repo rule is `unsafe` without a stated invariant comment. The CStrings are NUL-checked; `AT_FDCWD` + `RENAME_SWAP` is
the macOS swap; none of that is written down. Fix: add a SAFETY comment citing (1) both pointers are `CString` so they
are NUL-terminated and not mutated, (2) `AT_FDCWD` is the calling process cwd, (3) `0x2` is `RENAME_SWAP` and exchanges
two directory entries atomically or fails, (4) no aliasing of the buffers across the call. Optionally `const` the flag
from a named binding next to the comment. Cost/Risk: comment only.

### F10 — MEDIUM — SSOT — `WorkspaceName::main()` is wrapped, then bypassed

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:8651-8654`

```
fn main_name() -> WorkspaceName {
    WorkspaceName::main()
}
```

Callers that ignore it: `3176-3177` `WorkspaceName::new("main").expect("fixed main")` in `verify_checkout_identity`;
`4537-4538` `unwrap_or_else(|| WorkspaceName::new("main").expect("fixed main"))` in `create`.
`known_retired_main_targets` joins `"main"` as a path component (`4068`, `4072`). Problem: `main_name()` exists
specifically so the reserved name is not restated. Two production sites still parse `"main"` and can panic on a
`WorkspaceName` grammar change that `WorkspaceName::main()` would survive. Fix: replace both
`WorkspaceName::new("main").expect(...)` with `main_name()`. Path joins use `main_name().as_str()`. Cost/Risk: none;
`WorkspaceName::main()` is already the constructor.

### F11 — LOW — COPIES — `/bin/date` spawn for a timestamp `SystemTime` already produces

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:7901-7917`

```
async fn utc_timestamp() -> Result<String> {
    let output = tokio::process::Command::new("/bin/date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .await
```

Same file, checkpoint labeling (`5365-5366`): `CheckpointLabel::utc_default(std::time::SystemTime::now(), ...)`. Regime:
once per create/fork/adopt/restore (`operation_identity` at `2914`), not a hot loop. Problem: two clocks for one
ISO-8601 UTC string. The spawn is a process, a pipe, and a UTF-8 parse to format what `SystemTime` already is.
`/bin/date` is guaranteed on macOS; that does not make it the right layer. Fix: format `SystemTime::now()` as
`YYYY-MM-DDTHH:MM:SSZ` in `operation_identity` (or reuse whatever `CheckpointLabel::utc_default` already uses). Delete
`utc_timestamp`. Cost/Risk: `created_at` strings on new workspaces. Existing fixtures hardcode timestamps and do not
call this.

### F12 — LOW — COPIES — `decode_params` clones the JSON `Value` on every RPC

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:1037-1043`

```
fn decode_params<T: DeserializeOwned>(params: &Value, method: &str) -> Result<T> {
    serde_json::from_value(params.clone()).map_err(|error| {
```

Regime: once per router method. The clone is the entire params object, including exec `env` maps and argv. Problem:
`from_value` needs ownership; the `RouterRequest` still holds `params`. A borrow deserializer (`T: Deserialize<'de>`
from `&Value`) avoids the clone. Not a finding if params stay tiny; exec env maps are the case that is not tiny. Fix:
`serde_json::from_value` → `T::deserialize(params)` via `serde::Deserialize` on `&Value` (serde_json supports this).
Keep the usage error wrapping. Cost/Risk: decode_params callers unchanged.

### F13 — LOW — STRUCTURE — `ForkParams` is the rename wire type

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:1116-1120`
`struct ForkParams { repo_id, source, destination }`

Used for fork at `591` and rename at `599`: `let params: ForkParams = decode_params(...)`. Problem: the JSON happens to
share field names. The type name lies about rename and will grow a fork-only field that rename must then deny. Fix:
`struct SourceDestinationParams { repo_id, source, destination }` used by both, or two types that share a flatten header
(F7). Do not keep the fork name on the rename path. Cost/Risk: none if the JSON is unchanged.

## Cross-slice questions

- `packages/cowshed/crates/cowshed-core/src/api/server.rs` `CAPABILITY_METHODS` restates every `"project.open"` /
  `"coordinator.land"` / `"worker.exec"` string that `ProjectActor::route` (`406-450`) matches. Who owns the method
  table? This slice should deserialize an enum, not a string, if the table lives in `server.rs`.
- `dto::RunSandboxMode` (`api/dto.rs:1821`) and `sandbox::RunSandboxMode` (`sandbox.rs:25`) are two crate-level copies;
  `ExecModeWire` in this file is a third. Sandbox slice should own the type; dto should re-export or serde it.
- `supervisor::OutputStream` (`runtime/supervisor.rs:168`) is the third stdout/stderr enum (F6). Supervisor slice: can
  `RuntimeJobStream` move there, or `From` both ways?
- `storage/host_config.rs` default `~/.cowshed/mnt` and `STORE_ROOT` (`storage/bootstrap.rs:31`) are the SSOT F5 wants.
  Confirm `RETIRED_MOUNT_DIRECTORY` is `"mnt"` under the store before this slice stops spelling
  `/private/cowshed/store/mnt`.
- `crate::git::MAIN_REMOTE` (`git.rs:24`, `"main"`) is the remote _name_, distinct from `DEFAULT_LANDING_BRANCH` (the
  branch). Rebase's `{remote}/main` concatenates both; git slice should expose
  `tracking_ref(remote, DEFAULT_LANDING_BRANCH)` so this file does not assemble it.
- Crate `Cargo.toml` `walkdir` / `plist` / `notify` / `arrow-*` are unused in this file. Not a finding here; the
  crate-level bloat slice should weigh them.

## Non-findings (checked, clean)

- DEP-BLOAT in this slice: `async-trait` (40-method actor seam), `bytes` (zero-copy log frames), `url`
  (`coordinator_repo_mirror`), `libc` (`kill(0)`, `renameatx_np`), `serde`/`serde_json` (wire), `uuid` (created_trace /
  temp names via existing `fsio::temp_name`) are load-bearing. Do not shell out `uuidgen` on the adopt path. `git2` is
  already absent; git is spawned through `crate::git::git_command_at` (`7938-7942`), which is the intended posture.
- Operational failures return `Result`/`CowshedError`. Production `expect` is `WorkspaceName::new("main")` (invariant)
  and validated-destination `parent`/`file_name` in relocate (`6866-6869`). Test `unwrap`/`expect` stay in
  `#[cfg(test)]`.
- `cfg(not(target_os = "macos"))` `open_native` (`339-352`) is a real environment error, not a stub that pretends to
  work.
- `process_is_alive` unsafe has a SAFETY comment (`1452-1453`).
- `PortGrantReservation::drop` swallowing `remove_file` (`1443`) is reservation cleanup, not a hidden operational
  failure.
- `workspace_lineage` swallowing marker write (`8591`) is documented as an optimization.
- Identity change is one function for forward + recovery (`apply_identity_change`, `3697-3703` comment); that SSOT is
  already done.
- Removal refusals are enumerated and swept so they cannot name `--force` (`8656-8663`, `removal_refusal_tests`
  `7779-7791`). That test can go red. Do not "fix" it into a string-contains suite.
- `doctor_hint_tests` / `git_worktree_tests` / `adopt_secret_policy_tests` assert `ErrorCode` plus the user-facing hint
  contract; they are not substitution-dead.
- `mod.rs` is a 12-line re-export; no duplication with `project.rs`.
