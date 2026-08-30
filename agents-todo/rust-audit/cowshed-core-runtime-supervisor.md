# cowshed-core/runtime/supervisor.rs

Scope: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs` (4102). Doctrine: `BYPRODUCT-ENGINEERING.md`,
`docs/handbook/04-mechanisms.md`, `docs/handbook/05-memory-toolkit.md`, `docs/handbook/02-measurement.md` §4.1. Targeted
duplication reads (not owned): `packages/cowshed/crates/cowshed-core/src/process.rs` (191),
`packages/cowshed/crates/cowshed-core/src/exec.rs` (`plan_exec`/`SystemSpawnRunner`/`validate_argv`),
`packages/cowshed/crates/cowshed-core/src/api/dto.rs` (`ExitStatus`, `validate_command_argv`, `valid_commitment_id`),
`packages/cowshed/crates/cowshed-gateway/src/config.rs` (`WorkspaceToken::parse`),
`packages/cowshed/crates/cowshed-core/Cargo.toml`.

## Summary

- HIGH SSOT: process death is four types (`ProcessExit`, `dto::ExitStatus`, `process::ProcessStatus`,
  `DevenvCommandOutput.status: i32`); wait errors already disagree (fake SIGKILL vs `Unknown`).
- HIGH: `devenv print-dev-env` is a host `Command` with inherited env, outside `plan_exec`/Seatbelt, while every other
  child is sandboxed.
- HIGH COPIES: `SupervisorActor.jobs` never drops a `JobStateRecord`; full stdout/stderr `VecDeque`s live for the actor
  lifetime after seal.
- MEDIUM SSOT: checkpoint-id, workspace-token, and stream-kind predicates restated against dto/gateway/artifact types;
  devenv staleness is two different algorithms.
- MEDIUM STRUCTURE: 4102-line god file; `admit_exec`/`SystemSpawnSink::spawn` >200 lines; three `unsafe` sites with no
  SAFETY comment; clock failure becomes 1970.
- MEDIUM TESTS: two tests cannot go red for the property they name; one asserts on rendered error text.
- `notify` is load-bearing. `uuid` is crate-wide load-bearing; this file is not a reason to drop it.
- Spawn/wait/signal vs `process.rs`/`exec.rs`: `process.rs` has types only; `exec.rs` has a second sync spawn+wait.
  Raised as cross-slice, not silently merged.

## Findings

### F1 — HIGH — SSOT — Process death restated four times; wait-error path already diverged

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:194-197`, `1351-1366`, `1392-1395`,
`3026-3034`; `packages/cowshed/crates/cowshed-core/src/process.rs:7-10,25-38`;
`packages/cowshed/crates/cowshed-core/src/api/dto.rs:1174-1177`

```194:197:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
pub enum ProcessExit {
    Exited(i32),
    Signaled { signal: i32, core_dumped: bool },
}
```

```1351:1366:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
        tokio::spawn(async move {
            let exit = match child.wait().await {
                Ok(status) => {
                    if let Some(code) = status.code() {
                        ProcessExit::Exited(code)
                    } else {
                        ProcessExit::Signaled {
                            signal: status.signal().unwrap_or(libc::SIGKILL),
                            core_dumped: status.core_dumped(),
                        }
                    }
                }
                Err(_) => ProcessExit::Signaled {
                    signal: libc::SIGKILL,
                    core_dumped: false,
                },
            };
```

```1392:1395:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
        Ok(DevenvCommandOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: output.stdout,
            stderr: output.stderr,
```

`process.rs` `ProcessStatus` is `{ Exit(i32), Signal(i32), Unknown }` with `From<std::process::ExitStatus>` mapping
neither-code-nor-signal to `Unknown`. `dto::ExitStatus` is isomorphic to `ProcessExit`. `DevenvCommandOutput` collapses
a signal to `-1`. Problem: one concept, four types. The copies disagree: a wait that returns neither code nor signal, or
`Err`, is reported as SIGKILL here and as `Unknown` / `SpawnFailure` in `process.rs`/`exec.rs`. `Err(_)` still sends
`ProcessEvent::Exited`, so `finalize_job` will mark the job terminal while the child may still be running. That is a
live correctness bug, not a naming nit. Fix: delete `ProcessExit` and `DevenvCommandOutput`. Use `dto::ExitStatus` on
the actor/event path (it is the published job record). Use `process::ProcessStatus` + `process::CommandOutput` for
one-shot devenv. Map `child.wait()` through `ProcessStatus::from`; on `wait` `Err`, fail the job as integrity/`Failed`
and `kill(-pid, SIGKILL)` — never invent SIGKILL as a successful terminal status. Cost/Risk:
`tests/workspace_supervisor.rs` and every `ProcessEvent::Exited` match move with the type. `CsCoreGit`/`exec` slice owns
`process.rs`/`exec.rs`; conversion `From` belongs there.

### F2 — HIGH — STRUCTURE — `print_devenv_env` execs workspace-controlled Nix on the host

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1172-1280,1377-1396`

```1377:1381:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
    async fn print_devenv_env(&mut self, devenv_dir: &Path) -> Result<DevenvCommandOutput> {
        let output = tokio::process::Command::new("devenv")
            .args(["print-dev-env", "--json"])
            .current_dir(devenv_dir)
            .output()
```

Contrast the sandboxed spawn in the same impl: `plan_exec` + `env_clear` + Seatbelt profiles +
`prepare_child_descriptors` + `setpgid` (`1177-1311`). Problem: a dirty `devenv.nix` (workspace content) causes the
supervisor to run `devenv` with the daemon's full host environment, no `env_clear`, no sandbox-exec, no process group.
The nix daemon socket is already an admitted sandbox grant (`workspace_toolchain_tests` builds
`allowed_unix_sockets: nix_daemon_socket()`), so the "devenv needs the daemon" argument does not justify skipping
Seatbelt. This is the one child that can touch host credentials, `PATH`, and agent env. Fix: drive `print-dev-env`
through `plan_exec`/`SpawnSink::spawn` (or a sibling one-shot that still uses `SpawnPlan` + `env_clear` + executed-child
profile). Capture stdout as the snapshot; do not inherit host env. If evaluation truly cannot run inside Seatbelt, that
exception belongs in `exec.rs` as a named wrapper role, not a second `Command::new` here. Cost/Risk: devenv talks to the
nix daemon and writes `.devenv/`; the executed-child profile must admit those paths. Cross-slice: `exec.rs`
`SandboxProfileRole`.

### F3 — HIGH — COPIES — Job records and captured streams are never released

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1954-1978,2109-2114,2842-2851,2955-3053`

```1954:1964:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
struct JobStateRecord {
    info: JobInfo,
    started_at: Instant,
    process: Option<Box<dyn RunningProcess>>,
    artifact_live: bool,
    stdout: VecDeque<Bytes>,
    stderr: VecDeque<Bytes>,
    stdout_len: u64,
    stderr_len: u64,
```

`finalize_job` sets `terminal_committed`, copies `seal.stdout/stderr` into `job.info`, drains waiters, and removes the
job from the session's `background_jobs`. It never `jobs.remove`. `List` clones every `JobInfo` still in the map
(`2109-2114`). `process_output` keeps pushing accepted bytes into the deques after they have also been written to
`ArtifactSink`. Problem: regime is workspace lifetime, not a CPU hot loop — but it is growth under load with no closed
form (Byproduct L4, PH §7.11). Artifact store already has the sealed bytes and a byte limit; the actor retains a second
full copy of every job's stdout/stderr plus the `JobInfo` forever. A long-lived workspace that execs often will OOM
while disk quotas still hold. Fix: after `terminal_committed`, drop `stdout`/`stderr` deques (log_read for terminal jobs
reads the artifact, or keep a bounded ring). Cap or evict terminal jobs from `jobs` (keep `JobInfo` in the artifact
index, which already exists). `list` should come from that index, not an unbounded actor map. Cost/Risk:
`log_read`/`attach_read` today walk the in-memory deques (`3206-3245`). Those paths must switch to the artifact store
for terminal jobs. `CsCoreJobArtifact` owns that store.

### F4 — MEDIUM — SSOT — `validate_checkpoint_id` restates `dto::valid_commitment_id`

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:3286-3290`;
`packages/cowshed/crates/cowshed-core/src/api/dto.rs:1385-1389`

```3286:3290:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
fn validate_checkpoint_id(value: &str) -> Result<()> {
    if (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
```

The dto predicate is the same bytes, same length, same alphabet. `storage/mod.rs` checkpoint labels are a _third_
predicate (lowercase-or-digit first char, `pre-restore-` ban) — already diverged; that file is another slice. Problem:
two identical functions. The live bug is the third copy in storage, which this slice cannot see as the authority. Fix:
call `dto`'s validator (export `valid_commitment_id` or a `CheckpointId` newtype). Delete `validate_checkpoint_id`.
Cost/Risk: one call site (`checkpoint`, `2754`). Storage-label divergence is a cross-slice question.

### F5 — MEDIUM — SSOT — Workspace token charset check is a weaker restatement of `WorkspaceToken::parse`

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1084-1088,1235-1239`;
`packages/cowshed/crates/cowshed-gateway/src/config.rs:104-114`

```1084:1088:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
fn valid_workspace_token(token: &str) -> bool {
    token.len() == 43
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
```

Gateway parses unpadded base64url to exactly 32 bytes. 32 bytes → 43 chars is restated arithmetic, not imported
`TOKEN_BYTES`. A 43-char alphabet-valid string that fails strict decode is accepted here and rejected at CONNECT.
Problem: two validators, already able to disagree. The mint site (`workspace_credentials`) and the gateway parser are
the authority; this file re-approximates them so it can put the token in `HTTP_PROXY` userinfo. Fix: call
`cowshed_gateway::config::WorkspaceToken::parse` (or a cowshed-core re-export next to `WORKSPACE_TOKEN_PATH`). Delete
`valid_workspace_token`. Keep `gateway_proxy_url` as the only URL formatter, taking a parsed token. Cost/Risk:
`cowshed-core` already depends on `cowshed-gateway`. Test `proxy_url_carries_the_token_as_basic_auth_userinfo` stays,
but should use a `WorkspaceToken::encode()` fixture.

### F6 — MEDIUM — SSOT — `OutputStream` duplicates `job_artifact::StreamKind`

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:168-171,383-386`

```168:171:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
pub enum OutputStream {
    Stdout,
    Stderr,
}
```

```383:386:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
        let stream = match stream {
            OutputStream::Stdout => StreamKind::Stdout,
            OutputStream::Stderr => StreamKind::Stderr,
        };
```

Problem: identical two-variant enums with an explicit bijection at every artifact write. Nothing here is a different
domain. Fix: use `StreamKind` in `ProcessEvent`, `RunningProcess` IO, and `log_read`. Delete `OutputStream`. Cost/Risk:
public handle API (`log_read`/`attach_read`) and `tests/workspace_supervisor.rs`.

### F7 — MEDIUM — SSOT — Devenv staleness is computed two different ways

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:782-800,873-889,650-656,712-723`

```873:888:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
fn initial_devenv_snapshot_is_stale(workspace_mount: &Path, devenv_dir: &Path) -> bool {
    let snapshot = devenv_dir.join(DEVENV_SNAPSHOT_FILE);
    let Ok(snapshot_mtime) = fs::metadata(&snapshot).and_then(|metadata| metadata.modified())
    else {
        return true;
    };
    DEVENV_INPUT_FILES
        .into_iter()
        .map(|file| devenv_dir.join(file))
        .chain(std::iter::once(workspace_mount.join(COWSHED_CONFIG_FILE)))
        .any(
            |path| match fs::metadata(path).and_then(|metadata| metadata.modified()) {
                Ok(source_mtime) => source_mtime > snapshot_mtime,
```

`devenv_input_fingerprint` records `dev/ino/size/mtime/ctime` nsec. Startup uses snapshot-vs-source `modified()` only.
Runtime dirty+fingerprint can reuse a snapshot that startup would have called stale (same mtime, different ctime/inode
after atomic replace), or the reverse. Problem: two answers to "are devenv inputs the snapshot's inputs?". Copies
already disagree by construction. Fix: one function. Fingerprint is the richer byproduct — use it at start too. Compare
snapshot fingerprint (persist it next to the JSON, or hash the same struct) rather than mtime vs mtime. Delete
`initial_devenv_snapshot_is_stale`. Cost/Risk: `startup_staleness_includes_config_and_ignores_missing_optional_inputs`
and `delayed_watcher_event_does_not_refresh_unchanged_devenv_inputs` must assert on the fingerprint, not mtime sleeps.

### F8 — MEDIUM — DUPLICATION — Spawn-failure text is copied from `exec.rs`

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:3340-3347`;
`packages/cowshed/crates/cowshed-core/src/exec.rs:296-301`

```3340:3347:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
fn map_spawn_failure(failure: SpawnFailure) -> CowshedError {
    CowshedError::environment_missing(
        format!(
            "sandbox wrapper failed during {:?}: {}",
            failure.stage, failure.source
        ),
        "verify the macOS sandbox execution environment",
    )
}
```

`exec.rs` `execute_with` builds
`ExecError::WrapperFailure { message: format!("sandbox wrapper failed during {:?}: {}", failure.stage, failure.source), ... }`.
`map_exec_error` then stringifies that. Two format strings. Problem: same sentence, two authors. They will drift (one
already hard-codes "macOS" in the hint while `exec.rs` has a linux module). Fix: always `classify_spawn_error` →
`ExecError::WrapperFailure` → `map_exec_error`. Delete `map_spawn_failure`. Cost/Risk:
`prepare_child_descriptors`/`Command::spawn` error sites in `SystemSpawnSink::spawn` (`1302-1315`).

### F9 — MEDIUM — STRUCTURE — God module; spawn and admit are 200+ line functions

Evidence: file is 4102 lines. `SystemSpawnSink::spawn` `1172-1375`. `SupervisorActor::admit_exec` `2324-2572`.
`handle_command` `2043-2175`. `finalize_job` `2955-3054`. Problem: under the 5k line tripwire but the seams are already
named types sitting in one file: devenv watch/fingerprint (`569-1061`), cargo-registry/proxy env (`1084-1165`),
`SystemSpawnSink` (`1167-1535`), commitment publisher (`444-567`), artifact adapter (`319-442`), actor (`1994-3086`),
tests (`3414-4102`). `admit_exec` is the whole exec pipeline in one function (validate, session merge, devenv, artifact
admit, two seatbelt renders, spawn, stdin pump). Fix: split along those types: `runtime/devenv.rs`, `runtime/spawn.rs`
(`SystemSpawnSink`/`RunningProcess`), `runtime/supervisor/actor.rs`. `admit_exec` becomes: validate → `JobAdmission`
struct → `spawn` → `record_job`. No new abstraction beyond the types that already exist. Cost/Risk: `mod.rs` re-exports
to keep `WorkspaceSupervisorHandle` stable. Tests in this file move with devenv/spawn.

### F10 — MEDIUM — STRUCTURE — `unsafe` without a stated invariant

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1303-1311,1458-1460,3318-3323`

```1303:1311:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
```

```1458:1460:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
        let pid = i32::try_from(self.pid)
            .map_err(|_| CowshedError::internal("process id exceeds platform range"))?;
        let result = unsafe { libc::kill(-pid, raw) };
```

```3318:3323:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
    let mut broken = std::mem::MaybeUninit::<libc::tm>::uninit();
    let result = unsafe { libc::gmtime_r(&timestamp, broken.as_mut_ptr()) };
    if result.is_null() {
        return Err(CowshedError::internal("failed to convert UTC timestamp"));
    }
    let broken = unsafe { broken.assume_init() };
```

Problem: `pre_exec` is `unsafe` because the closure runs in the child between fork and exec in a multithreaded process —
only async-signal-safe calls are legal (`setpgid` is; this fact is unstated). `kill(-pid)` is only a process-group kill
if `setpgid` succeeded; pid `1` would be `kill(-1)`. `gmtime_r` requires the out-pointer live and is duplicated in
`storage/audit.rs`. Fix: SAFETY comments citing POSIX async-signal-safety, the `setpgid`/`kill(-pgid)` pair, and
`gmtime_r` init-on-non-null. Prefer `crate::storage` civil timestamp (already exists to avoid `time_t` failure) over a
third `gmtime_r`. Reject pid that does not fit a positive `pid_t` before negation. Cost/Risk: comment-only except
`utc_now` if it switches to the civil helper (audit/storage slice).

### F11 — MEDIUM — STRUCTURE — Operational clock failure becomes 1970

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:2448-2450,3311-3333`

```2448:2450:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
        let started = utc_now().unwrap_or_else(|_| {
            UtcTimestamp::new("1970-01-01T00:00:00Z").expect("static timestamp")
        });
```

`utc_now` already returns `Result` for pre-epoch clocks, `time_t` overflow, and `gmtime_r` null. The caller discards
that and stamps the job at epoch. Problem: operational failure swallowed (`/dev/null`). Job records, commitments, and
any consumer that orders by `started` lie. This is not an invariant; it is a clock `Err`. Fix: if `utc_now` fails, fail
admission (`reply.send(Err)`), same as a seatbelt-profile failure a few lines below. Do not mint a job with a fake
timestamp. Cost/Risk: rare path; tests that freeze the clock must still succeed. No API change.

### F12 — MEDIUM — TESTS — Named tests cannot go red for the named property

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:3647-3664,3958-3965,3510-3524`

```3652:3664:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
        let Ok(store_profile) = std::fs::canonicalize("/nix/var/nix/profiles/default") else {
            return;
        };
        if !store_profile.starts_with("/nix/store") {
            return;
        }
        let Some(tool) = std::fs::read_dir(store_profile.join("bin"))
            .ok()
            .and_then(|mut entries| entries.find_map(|entry| entry.ok()))
            .map(|entry| entry.path())
        else {
            return;
        };
```

```3958:3964:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
        let first_supervisor =
            WorkspaceSupervisor::start(config.clone(), publisher.clone()).unwrap();
        first_supervisor.list().await.unwrap();
        drop(first_supervisor);
        tokio::task::yield_now().await;
        let restarted = WorkspaceSupervisor::start(config, publisher.clone()).unwrap();
        restarted.list().await.unwrap();
```

```3521:3523:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
        let error = resolve_devenv_dir(&mount).unwrap_err();
        assert!(error.message.contains("tooling/devenv"));
        assert!(error.message.contains("devenv.nix"));
```

`an_evaluated_workspace_profile_leads_path_and_runs_inside_the_sandbox` returns success if the fixture host has no Nix
profile — PH §7.10bb substitution test fails. `lineage_history_opens_and_restarts_a_replacement_supervisor` names
restore/lineage and only asserts `list()` is `Ok` (empty is fine) — PH §4.2b / §7.10bb.
`configured_devenv_without_devenv_nix_is_an_error` keys off rendered `message` text. Problem: green does not mean the
mechanism works. The sandbox-exec status is asserted after extra filesystem mutations (`3713-3725`), so a later panic
loses the only runtime check. Fix: missing Nix profile → `#[ignore]` with a reason, not `return`. Lineage test: `list()`
after restart must contain job 1 (baseline) and must not contain job 2 (post-checkpoint), or fail. Error test: match a
typed field (`configured_dir == Some(tooling/devenv)`) not substrings. Assert `status.success()` immediately after
`sandbox-exec`. Cost/Risk: lineage assertion may expose that the supervisor does not actually reload job history (the
test currently cannot tell). That is the point.

### F13 — LOW — STRUCTURE — `KillReason::StdinFailure` names the wrong cause

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1928-1933,2565-2566,2815-2816,2864-2865`

```1928:1933:packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
enum KillReason {
    Requested,
    OutputLimit,
    Retire,
    StdinFailure,
}
```

Used for spawn failure (`2566`), stdin pump failure (`2816`), _and_ `artifacts.write` `Err` (`2865`). All become
`JobState::Failed`. Problem: the discriminant is a lie. Doctor/debug of a failed job cannot distinguish a stdin pump
error from a corrupt artifact append. Fix: `SpawnFailure | StdinFailure | ArtifactFailure` (or drop the enum and store
`JobState` at kill time). `begin_kill` already overwrites only on first reason / `OutputLimit`. Cost/Risk: local to the
actor; tests that inspect `KillReason` live in `tests/workspace_supervisor.rs` if at all.

## Cross-slice questions

- `packages/cowshed/crates/cowshed-core/src/process.rs`: `ProcessStatus` / `CommandOutput` are the exit/output types
  this file reimplemented. Should `ProcessExit`/`DevenvCommandOutput` die in favor of those, with `dto::ExitStatus` only
  at the API edge?
- `packages/cowshed/crates/cowshed-core/src/exec.rs`: `SystemSpawnRunner::run` (`269-285`) is a second spawn+wait
  (`std::process::Command`, inherit stdio, no `setpgid`, no kill). `SystemSpawnSink` is the async piped/process-group
  version of the same `plan_exec`+`prepare_child_descriptors`+`classify_spawn_error` sequence. Who owns the single spawn
  implementation? Signal/process-group logic exists only here, not in `process.rs`.
- `packages/cowshed/crates/cowshed-core/src/exec.rs` `validate_argv` vs `dto::validate_command_argv`: supervisor runs
  the dto check then `plan_exec` runs the exec check on `OsString`s. Duplicate or complementary?
- `packages/cowshed/crates/cowshed-core/src/storage/mod.rs` checkpoint-label predicate already disagrees with
  `dto::valid_commitment_id` / this file's `validate_checkpoint_id`.
- `packages/cowshed/crates/cowshed-core/src/storage/audit.rs` has the other `gmtime_r` UTC formatter; `storage/mod.rs`
  `civil_from_days` exists specifically because `gmtime_r` can fail. `utc_now` should not be a third clock.
- `packages/cowshed/crates/cowshed-core/src/storage/job_artifact.rs` `StreamKind`: candidate SSOT for `OutputStream`
  (F6). Terminal `log_read` should read sealed artifacts if F3 drops in-memory deques.
- `packages/cowshed/crates/cowshed-core/src/sandbox.rs` `seatbelt_profile`: `admit_exec` renders both roles, `spawn`
  renders TrustedSupervisor again and `plan_exec` renders ExecutedChild again, then byte-compares. If the profile
  function is pure this never fires (evaporating work). If it reads files, it is TOCTOU. Which?

## Non-findings (checked, clean)

- **notify (dep)**: in-process FSEvents callback with a dirty flag is load-bearing. Shelling out to `fswatch` is the
  wrong recommendation. Keep the crate. (Recursive watch of the whole mount to notice five paths is heavier than needed,
  but once-per-mount, not a finding.)
- **uuid (dep)**: this file uses v4 for traces and test temp dirs. The crate is also the incarnation/trace generator
  elsewhere. Not removable from this slice.
- **async-trait / bytes / serde_json / libc / tokio**: object-safe async sinks, 64 KiB IO frames, devenv JSON,
  process-group syscalls. Load-bearing.
- **argv / PATH / cargo registry SSOT inside this file**: `sandbox_path` is the only PATH builder;
  `SHARED_CARGO_REGISTRY_DIRECTORIES` and `host_cargo_registry` come from `sandbox.rs`; `WORKSPACE_TOKEN_PATH` from
  `workspace_credentials`. No second tables here.
- **`COWSHED_CONFIG_FILE` / `DEVENV_*` constants**: one table in this file; bootstrap comments the filename but does not
  restate the const.
- **Once-per-exec clones** (`SandboxConfig`, seatbelt profile `String`, `argv.clone`, `authority.clone` on every
  command): regime is RPC, not a per-byte loop. Noted, not filed.
- **`Bytes::copy_from_slice` in `run_system_output`/`pump_reader`**: 64 KiB reused buffer, then an owned frame for the
  channel. Inherent, not stupid.
- **No `unwrap` on the spawn/kill operational path** except the wait-error mapping in F1 and the epoch fallback in F11.
  Session `expect("validated session exists")` is an actor invariant.
- **`cfg(target_os)`**: file is unconditionally unix (`ExitStatusExt`, `setpgid`). Tests that need macOS are
  `cfg_attr(not(target_os = "macos"), ignore)`. No opposite-platform arm that cannot compile.
- **`merge_devenv_environment` drops snapshot `PATH`**: tested with typed maps (`3557-3575`).
- **`link_cargo_registry`**: keeps `src` as a real dir, replaces stale links, leaves a pre-existing real `index` alone —
  tested (`4025-4100`).
- **Default channel capacities / TERM grace**: validated `> 0` at `WorkspaceSupervisorConfig::validate`.
- **No `HashMap` on sequential `JobId`**: `BTreeMap` is fine at job-count scale; the bug is unbounded retention (F3),
  not the container.
