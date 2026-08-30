# cowshed-cli/services

Scope: `packages/cowshed/crates/cowshed-cli/src/launchd.rs` (1650),
`packages/cowshed/crates/cowshed-cli/src/setup_service.rs` (1185),
`packages/cowshed/crates/cowshed-cli/src/gateway_service.rs` (1051). Also
`packages/cowshed/crates/cowshed-cli/Cargo.toml`. Targeted greps across `packages/cowshed` for labels, modes,
`plan_install`, `classify_executable_source`, `activate_launch_agent`, `HostAction::`, `cache_entries`; neighbour reads
of `sccache_service.rs:107-173`, `storage/bootstrap/native/macos.rs:441-466`, `runtime.rs:2433-2486`,
`cowshed-core/src/api/dto.rs:2275-2288` only to close duplication questions.

## Summary

- Plist XML is one source: `LaunchAgentSpec::plist_bytes`. Gateway and sccache both go through that; not restated per
  service.
- Install/uninstall/status is not written three times. `launchd.rs` plans and executes filesystem + `launchctl`;
  `gateway_service.rs` owns activate/deactivate/remove; `setup_service.rs` only orchestrates those helpers.
- No `plist` crate in cowshed-cli. Hand-written XML plus `/bin/launchctl` is the right split; do not add `plist` or
  shell out to `plutil` for generation.
- HIGH: `classify_executable_source` (and `containing_mount_point`) are production-dead;
  `install_host_stable_executable` copies workspace/nix sources on purpose. Tests still exercise the dead gate.
- MEDIUM: user-owned-not-symlink check copied four times; directory-mode planner copied twice; `PRIVATE_DIRECTORY_MODE`
  restated; `cache_entries`/`cache_bytes` always emitted as 0.

## Findings

### F1 — HIGH — SSOT — Unstable-source classifier is dead; the live install path copies those sources

Evidence: `packages/cowshed/crates/cowshed-cli/src/launchd.rs:620-648` and
`packages/cowshed/crates/cowshed-cli/src/gateway_service.rs:586-610`

```
/// Decide whether a LaunchAgent may be installed from `source`.
///
/// The refusals are structural. Cowshed's storage is mounted *by cowshed*, and launchd starts
/// agents before any of it exists, so a binary there is a binary the agent cannot reach at boot:
pub fn classify_executable_source(
    home: &Path,
    source: ExecutableSource<'_>,
) -> Result<(), UnstableExecutableSource> {
    let store = home.join(".cowshed");
    if source.path.starts_with(&store) {
        return Err(UnstableExecutableSource::Store { store });
    }
    if source.mount_is_workspace {
        return Err(UnstableExecutableSource::Workspace {
```

```
/// The source may live in a workspace or the nix store: those paths are unreadable at boot,
/// but the copy is not.
pub fn install_host_stable_executable<F, C>(
    ...
    if source == executable.path() {
        return Ok(executable);
    }
    let state = observe_executable_install(&executable, source)?;
    executor
        .execute_install(&plan_executable_install(&executable, source, state))
```

Grep of `packages/cowshed` for `classify_executable_source` and `containing_mount_point`: definitions in `launchd.rs`
only; every other hit is `packages/cowshed/crates/cowshed-cli/tests/launchd.rs`. No `src/` caller.

Problem: two contracts for the same question. The classifier refuses store/workspace/home-volume sources. The function
that actually installs copies those sources onto `~/Library/Application Support/dev.cowshed/bin/` and names the copy in
the plist. The copy is the correct design (the plist never names the disappearing path). The classifier,
`UnstableExecutableSource`, `ExecutableSource`, and `containing_mount_point` are leftover public API. `tests/launchd.rs`
still treats the classifier as a gate, so a production install from a workspace binary cannot turn those tests red
(PERFORMANCE-HANDBOOK §7.10bb). Docs in `packages/cowshed/docs/cli.md` still say start "refuses a running executable
inside cowshed's own storage" — that sentence describes the dead function, not the live one.

Fix: delete `classify_executable_source`, `UnstableExecutableSource`, `ExecutableSource`, and `containing_mount_point`
from `launchd.rs` and delete their tests in `tests/launchd.rs`. Keep the copy-to-host-stable-path path as the single
contract. Update `docs/cli.md` / `docs/gateway.md` in the same cut (they currently document the refusal). Do not wire
the classifier into `install_host_stable_executable`: that would refuse the workspace/nix sources the copy exists to
accept.

Cost/Risk: test file `tests/launchd.rs` and the two docs pages must move with it. No production caller to migrate.
Wiring instead of deleting would break `cowshed gateway start` from a workspace build.

### F2 — MEDIUM — DUPLICATION — User-owned not-symlink check written four times

Evidence: `packages/cowshed/crates/cowshed-cli/src/gateway_service.rs:675-688`, `:765-774`, `:798-811`, `:854-862`

```
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || metadata.uid() != effective_uid()
            {
                return Err(CowshedError::integrity(
                    format!(
                        "the installed {} binary is not a user-owned regular file: {}",
```

```
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.uid() != effective_uid()
            {
                return Err(CowshedError::integrity(
                    format!("path is not a user-owned directory: {}", path.display()),
```

Same predicate again in `inspect_install_state` (plist file) and `ensure_private_directory` (telemetry/mirror dir, plus
a post-`create_dir_all` canonicalize).

Problem: the security check that a cowshed-owned path is a user-owned non-symlink file or directory is restated four
times with four error strings. A fifth site that forgets `is_symlink()` or the uid check is a TOCTOU hole against a
planted symlink. `ensure_private_directory` also follows symlinks via `fs::create_dir_all` and then checks canonicalize,
which is a different (weaker) posture than `launchd.rs`'s `O_NOFOLLOW` opens.

Fix: one helper in `launchd.rs` or `gateway_service.rs`, `fn require_user_owned(path, FileKind) -> Result<Metadata>`,
used by `observe_executable_install`, `private_directory_mode`, `inspect_install_state`. Point
`ensure_private_directory` at `LaunchdFilesystem::ensure_directory` (already `O_NOFOLLOW` + exact mode) instead of
`create_dir_all` + chmod.

Cost/Risk: four call sites in `gateway_service.rs`; sccache start uses `inspect_install_state` so it picks up the helper
for free. Telemetry/mirror dirs currently created without `O_NOFOLLOW` would start failing closed on symlink plants —
that is the point.

### F3 — MEDIUM — DUPLICATION — Directory-mode planner copied between plist install and binary install

Evidence: `packages/cowshed/crates/cowshed-cli/src/launchd.rs:426-437` and `:516-528`

```
    match state.launch_agents_directory_mode {
        None => operations.push(Mutation::EnsureDirectory {
            path: directory.clone(),
            mode: PRIVATE_DIRECTORY_MODE,
        }),
        Some(mode) if mode != PRIVATE_DIRECTORY_MODE => {
            operations.push(Mutation::SetPermissions {
                path: directory.clone(),
                mode: PRIVATE_DIRECTORY_MODE,
            });
        }
        Some(_) => {}
    }
```

```
        match mode {
            None => operations.push(Mutation::EnsureDirectory {
                path: directory.to_path_buf(),
                mode: PRIVATE_DIRECTORY_MODE,
            }),
            Some(mode) if mode != PRIVATE_DIRECTORY_MODE => {
                operations.push(Mutation::SetPermissions {
                    path: directory.to_path_buf(),
                    mode: PRIVATE_DIRECTORY_MODE,
                });
            }
            Some(_) => {}
        }
```

`plan_remove` (`:465-479`) and `plan_executable_remove` (`:563-577`) are the same shape: `RemoveFile` + `SyncDirectory`
or empty.

Problem: the "ensure this directory exists at 0o700, else chmod, else nothing" machine is one concept with two copies. A
third host-stable artifact (sccache-source record already lives beside the binaries) will grow a third copy.
`plan_remove` / `plan_executable_remove` are the teardown twin.

Fix: `fn plan_private_directory(path: &Path, mode: Option<u32>) -> Vec<Mutation>` and
`fn plan_remove_file(path: &Path, directory: &Path, installed: bool) -> Vec<Mutation>`. `plan_install` and
`plan_executable_install` only add the tempfile/rename/sync of their payload.

Cost/Risk: `tests/launchd.rs` asserts exact `operations()` vectors; those tests move with the helper but stay as the
oracle.

### F4 — MEDIUM — SSOT — `PRIVATE_DIRECTORY_MODE` restated in gateway_service

Evidence: `packages/cowshed/crates/cowshed-cli/src/launchd.rs:18` and
`packages/cowshed/crates/cowshed-cli/src/gateway_service.rs:44`

```
pub const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
```

```
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
```

`gateway_service.rs` already imports `STABLE_BINARY_MODE` from `launchd` (`:6`) and does not import
`PRIVATE_DIRECTORY_MODE`. The local const is used only by `ensure_private_directory` (`:864`).

Problem: the mode of a cowshed-owned private directory is a security constant with two spellings in one crate. Drift is
a live permissions bug (LaunchAgents at 0700, telemetry/mirror at something else, or the reverse).

Fix: delete the local const; `use crate::launchd::PRIVATE_DIRECTORY_MODE` next to `STABLE_BINARY_MODE`. Better, after
F2, this site disappears into `LaunchdFilesystem::ensure_directory`.

Cost/Risk: one file. No ABI.

### F5 — MEDIUM — SSOT — `CliGatewayStatus.cache_entries` / `cache_bytes` always 0

Evidence: `packages/cowshed/crates/cowshed-cli/src/gateway_service.rs:532-547` and
`packages/cowshed/crates/cowshed-core/src/api/dto.rs:2277-2288`

```
fn cli_status(...) -> CliGatewayStatus {
    CliGatewayStatus {
        installed,
        running,
        socket,
        cli_version: env!("CARGO_PKG_VERSION").to_owned(),
        daemon_version: status.map(|status| status.version.clone()),
        cache_entries: 0,
        cache_bytes: 0,
        active_workspaces: status.map_or(0, |status| status.sessions.len() as u64),
    }
}
```

```
    pub cache_entries: u64,
    pub cache_bytes: u64,
    pub active_workspaces: u64,
```

The DTO serializes both as required camelCase numbers (`deny_unknown_fields`).
`cowshed-core/tests/public_api_contracts.rs` plants `cache_entries: 7`, `cache_bytes: 8192`. Every live `gateway status`
/ `gateway start` JSON path emits zeros. `cowshed-gateway::GatewayStatus` as used here has no cache counters to copy.

Problem: the wire type claims cache occupancy. The only producer in this crate hardcodes 0. A client cannot tell "empty
cache" from "never measured". That is a live lie on the JSON envelope, not an unused field.

Fix: decision I would take: drop `cache_entries` / `cache_bytes` from `cowshed_core::api::GatewayStatus` (greenfield; no
compat) unless a real counter exists on the daemon status. If the fields stay, they must be `Option<u64>` with
`skip_serializing_if = "Option::is_none"` and this function must pass `None` rather than `0`. Do not invent a counter
here.

Cost/Risk: DTO + `packages/cowshed/crates/cowshed-cli/src/runtime.rs` (also constructs zeros) +
`tests/gateway_service.rs` + `public_api_contracts.rs`. Owned in part by the core-api and cli-runtime slices.

### F6 — MEDIUM — DUPLICATION — Binary-current predicate restated beside the planner that already has it

Evidence: `packages/cowshed/crates/cowshed-cli/src/gateway_service.rs:624-628` and
`packages/cowshed/crates/cowshed-cli/src/launchd.rs:531-533`

```
pub fn installed_binary_is_stale(state: &ExecutableInstallState) -> bool {
    !state
        .installed
        .is_some_and(|installed| installed.mode == STABLE_BINARY_MODE && installed.matches_source)
}
```

```
    let binary_is_current = state
        .installed
        .is_some_and(|installed| installed.mode == STABLE_BINARY_MODE && installed.matches_source);
```

Problem: "the installed binary is the source at 0755" is one predicate. `plan_executable_install` uses it to no-op.
`refresh_gateway_binary` uses the inverted copy to skip. A third check (uid, not-symlink — already done in
`observe_executable_install`) added to one side only makes `setup` refresh a binary `start` would leave, or the reverse.

Fix: put `fn is_current(&self) -> bool` on `ExecutableInstallState` in `launchd.rs`. `plan_executable_install` and
`installed_binary_is_stale` both call it. Delete the free function or make it a one-line wrapper.

Cost/Risk: `refresh_gateway_binary` and the in-module drift test (`gateway_service.rs:894-932`).

### F7 — LOW — DUPLICATION — XML escape loop copied for `<string>` and `<key>`

Evidence: `packages/cowshed/crates/cowshed-cli/src/launchd.rs:1622-1650`

```
fn push_xml_string(output: &mut String, value: &str) {
    output.push_str("<string>");
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '\'' => output.push_str("&apos;"),
            '"' => output.push_str("&quot;"),
            _ => output.push(character),
        }
    }
    output.push_str("</string>\n");
}
```

`push_xml_key` is the same loop with `<key>` / `</key>`.

Problem: two copies of the only XML escaping the crate does. A third plist key (integer, true/false are unescaped
literals today) is fine; a missed `&` in one tag is a malformed LaunchAgent.

Fix: `fn push_xml_text(output: &mut String, tag: &str, value: &str)` or escape into a shared
`fn write_escaped(output: &mut String, value: &str)`.

Cost/Risk: `plist_bytes` and `tests/launchd.rs` byte-equality oracles. Once-per-install, not a hot path.

### F8 — LOW — SSOT — `kickstart_hint` restates `gui/{uid}/{label}` beside `service_target`

Evidence: `packages/cowshed/crates/cowshed-cli/src/gateway_service.rs:874-876` and
`packages/cowshed/crates/cowshed-cli/src/launchd.rs:1084-1089`

```
fn kickstart_hint(uid: u32) -> String {
    format!("launchctl kickstart -k gui/{uid}/{GATEWAY_LABEL}")
}
```

```
fn gui_domain(uid: u32) -> String {
    format!("gui/{uid}")
}
fn service_target(uid: u32, label: &str) -> String {
    format!("{}/{label}", gui_domain(uid))
}
```

`ControlPlan::kickstart` already builds `kickstart -k` + `service_target`. The hint duplicates that argv as a string.
`sccache_service.rs:168` formats the same `launchctl kickstart -k gui/{uid}/{}` again (other slice).

Problem: if the domain spelling changes, `launchctl` argv and the user-facing hint diverge. The test
`kickstart_guidance_targets_the_per_user_domain` pins the string, not `ControlPlan`.

Fix: export `service_target` (or a `ControlPlan::hint(&self) -> String`) from `launchd.rs`. `kickstart_hint` becomes
`format!("{} {} {}", LAUNCHCTL_EXECUTABLE, "kickstart -k", service_target(uid, GATEWAY_LABEL))` or, cheaper, format from
`ControlPlan::kickstart(uid, spec).arguments()`.

Cost/Risk: one hint, one unit test, and the sccache twin.

## Cross-slice questions

- `packages/cowshed/crates/cowshed-core/src/storage/bootstrap/native/macos.rs:441-466` hand-writes a third launchd plist
  (system LaunchDaemon `dev.cowshed.storage`) with a different `KeepAlive` shape
  (`dict`/`SuccessfulExit`/`ThrottleInterval`). Do not fold it into `LaunchAgentSpec` (wrong domain, wrong keys). XML
  escaping / doctype header is the only shareable bit; owned by the core bootstrap/APFS slice.
- `packages/cowshed/crates/cowshed-cli/src/sccache_service.rs:107-173` repeats the start sequence
  `install_host_stable_executable` → `inspect_install_state` → `plan_install` → `execute_install` →
  `activate_launch_agent` → poll. Helpers are shared; the verb body is not. CsCliSccache owns whether that body
  collapses.
- `packages/cowshed/crates/cowshed-cli/src/runtime.rs:2433-2486` `host_action_evidence` is a second exhaustive
  `HostAction` renderer next to `setup_service.rs:731-789` `action_intent`. Prose already diverges on purpose (doctor
  evidence vs setup consent) and both matches are exhaustive, so a new variant still fails to compile. Not a live bug;
  flagging so CsCliRuntime does not "unify" the sentences.
- `packages/cowshed/crates/cowshed-cli/src/runtime.rs:2376-2394` `AdoptHostSetup` re-wraps `plan_host_setup` /
  `execute_host_setup` beside `NativeHostSetup`. Same two methods, second trait.
- `setup_service.rs:391-396` and `sccache_service.rs:208-212` both walk `[ImageFormat::Asif, ImageFormat::Sparse]` and
  count/stat each `main_image`. If both formats can exist for one project, the uninstall census double-counts that main.
  Storage-layout slice owns whether that pair is exclusive.
- F5's DTO lives in `cowshed-core/src/api/dto.rs`. Core-api slice owns the field deletion / Option change.
- `docs/cli.md` still documents the F1 refusal. Docs are outside this slice.

## Non-findings (checked, clean)

- Plist template: single `LaunchAgentSpec::plist_bytes` (`launchd.rs:307-349`). `::gateway` and `::sccache` only fill
  label/argv/env/`ProcessType`/stderr name. Not restated per service.
- Install/uninstall/status machine: planner (`plan_install` / `plan_remove` / `plan_executable_*`) and `LaunchdExecutor`
  in `launchd.rs`; activate/deactivate/remove in `gateway_service.rs`; `setup_service.rs` calls `gateway_launch_agent`,
  `sccache_launch_agent`, `remove_launch_agent`, `remove_host_stable_executable`. Third copy does not exist in this
  slice.
- `plist` crate: not a cowshed-cli dependency. Generation is ~40 lines of XML with control-char validation
  (`launchd.rs:1489-1650`). Adding `plist` (cowshed-core uses it to _parse_ diskutil output) would be bloat. `plutil` on
  PATH is the wrong tool for emit-and-byte-compare. `/bin/launchctl` is load-bearing for
  bootstrap/bootout/kickstart/print (`LAUNCHCTL_EXECUTABLE`, `NativeLaunchctlCommand`) — typed exit mapping, injectable,
  no shell. Keep it.
- `libc`: load-bearing. `O_NOFOLLOW` + exclusive create cannot be a `launchctl`/`plutil` shell-out (`launchd.rs:735`,
  `:869`).
- Copies / allocs: `plist_bytes` → `Vec<u8>`, `same_contents` 64 KiB pair (`gateway_service.rs:740`), `PathBuf` clones
  in plans, `format!` for launchctl targets. Regime is once per `start`/`stop`/`setup`, not a probe loop. Not findings
  (PERFORMANCE-HANDBOOK §4.1). `copy_exclusive_no_follow` streams via `io::copy` and does not slurp the binary
  (`launchd.rs:819-823`).
- `unwrap`/`expect` in production: only structural invariants after validation (`derived paths always have a parent`,
  `validated paths are UTF-8`). No operational `unwrap`. No `unsafe`. No `cfg(target_os)` arms in these files (unix
  `MetadataExt` / `launchctl` assumed; crate is macOS host tooling).
- `HostSetup` vs launchd: setup rendering is exhaustive over `HostAction` / `VolumeState` / `ConfigOutcome` with no
  default branch. Injectable trait is the test seam; `NativeHostSetup` does not reimplement launchd.
- `ServiceBinaryRefresh::Stale` is not produced by `refresh_gateway_binary` (only `None` / `Refreshed`). The variant is
  the `HostSetup` test vocabulary (`tests/setup_service.rs` FakeHost), not dead production code in this slice.
- `async-trait` on `HostSetup` / `GatewayDrain`: edition is 2024 so native async-in-traits would compile, but the crate
  uses `async-trait` in `runtime.rs` `CliService` as well. Not a this-slice cut.
- Tests in `gateway_service.rs:886-1051` assert typed `StartProgress` lines and `GATEWAY_START_HINT`; the deadline test
  (`>= 120s`) can still go red on the original 10s bug. Full planner oracles live in `tests/launchd.rs` (out of slice,
  exist).
