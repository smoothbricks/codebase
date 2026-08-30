# cowshed-cli/runtime.rs

Scope: `packages/cowshed/crates/cowshed-cli/src/runtime.rs` (4008 lines). Doctrine: BYPRODUCT-ENGINEERING.md,
PERFORMANCE-HANDBOOK §4.1 / §7 / §7.12. Targeted (not audited) reads to close SSOT questions:
`cowshed-core/src/runtime/project.rs` (`snapshot`, `doctor`), `cowshed-core/src/gateway_inventory.rs`
(`load_project_workspaces`, `read_current_metadata`, `NativeHealSource`), `cowshed-core/src/api/dto.rs`
(`WorkspaceInfo`, `DoctorReport`), `cowshed-core/src/metadata.rs` (`ImageFormat::extension`, `WorkspaceName::is_main`),
`cowshed-core/src/gateway_sessions.rs` (`reconcile_native_project`), `cowshed-cli/src/args.rs` (`ProjectDiscovery`),
`cowshed-cli/src/setup_service.rs` (`action_intent`).

## Summary

- HIGH SSOT: `store_workspace_info` restates core's `WorkspaceInfo` mapping and skips inventory's current-incarnation /
  no-symlink checks — live divergence.
- HIGH DUPLICATION: store-wide `attach --all` / `detach --all` rebuild `ApfsSubstrate` instead of delegating, and never
  call `reconcile_native_project`.
- HIGH SSOT: `cowshed doctor` merges CLI `gateway_findings` with `ProjectRuntime::doctor`'s `gateway-down` — duplicate
  Error rows, disagreeing hints.
- MEDIUM STRUCTURE: 4008-line module; `dispatch` is ~450 lines; `host_storage_findings` is ~177.
- MEDIUM SSOT: `doctor_report` re-derives `healthy` from findings; core's `doctor()` does the same formula.
- MEDIUM DUPLICATION: project listing sort and the landing/json/table cascade are written twice.
- MEDIUM TESTS: doctor tests pin rendered strings; `every_core_stdin_variant_has_a_cli_spelling` never constructs
  `WorkspaceFile`.
- LOW DUPLICATION: host-storage inventory open is copied six times; `host_action_evidence` restates
  `setup_service::action_intent`.

## Findings

### F1 — HIGH — SSOT — CLI rebuilds `WorkspaceInfo` from sidecar bytes, weaker than inventory

Evidence: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:1567-1609`

```rust
let metadata = DetachedWorkspaceMetadata::read_for_image(image.image())
    .map_err(attach_store_storage_error)?;
let snapshot = metadata.info_snapshot.as_ref();
let base_commit = snapshot
    .map(|snapshot| GitOid::new(snapshot.base_commit.clone()))
    .transpose()
    .map_err(attach_store_storage_error)?;
// ...
Ok(WorkspaceInfo {
    repo_id: derived.workspace.repo().clone(),
    workspace: derived.workspace.name().clone(),
    // ...
    state: WorkspaceState::Detached,
    landing: None,
})
```

Same field list, including `GitOid::new(clone)` / `UtcTimestamp::new(clone)` / checkpoint `Pin` map, lives at
`cowshed-core/src/runtime/project.rs:2792-2833` (`ProjectRuntime::snapshot`) and
`cowshed-core/src/gateway_inventory.rs:861-901` (`load_project_workspaces`). Inventory then wraps the read in
`read_current_metadata` (`gateway_inventory.rs:1459-1493`): `verify_no_symlinks` on image and sidecar, plus
`publication_state == Active` and exact repo/name/incarnation/format/revision match. The CLI path calls `read_for_image`
only.

Problem: three constructors of one DTO. The CLI copy already disagrees: it will accept a pending or identity-mismatched
sidecar (and a symlinked image/sidecar) that inventory refuses, then report that snapshot as `WorkspaceInfo` after
`attach --all`.

Fix: one function in core — `WorkspaceInfo` from `(layout, DerivedWorkspace, DetachedWorkspaceMetadata)` — used by
`snapshot`, `load_project_workspaces`, and the CLI. CLI attach-from-store must go through `read_current_metadata` (or an
equivalent that keeps the symlink and incarnation gates). `state: Detached` then patch-to-Attached after mount stays a
caller concern; the mapping does not.

Cost/Risk: `CsCoreProject` (`snapshot`) and `CsCoreGatewayInv` (`load_project_workspaces` / `read_current_metadata`)
must move first; CLI `store_workspace_info` becomes a call. No unit test in this file covers the mapping today (`:3870`
tests `resolve_session_project_root`, not `WorkspaceInfo`).

### F2 — HIGH — DUPLICATION — store-wide attach/detach reimplement substrate orchestration and skip gateway reconcile

Evidence: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:1524-1562` and `:1678-1705` (substrate constructed twice),
`:3118-3128` (host dispatch, no reconcile), vs `:936-941` (project attach + reconcile) and `:544-548`.

```rust
let host = MacOsApfsExecutionHost::new(SystemCommandRunner, config.clone())
    .map_err(attach_store_storage_error)?;
let substrate = ApfsSubstrate::new(config, host);
// filter sessions, substrate.ensure_mounted(...)
```

```rust
Command::Attach(args) if args.all && args.workspace.is_none() => {
    let infos = attach_store_wide(args.browse).await?;
    emit_attached(output, cli.global.json, &infos)?;
    Ok(success())
}
```

`args.rs` sets `ProjectDiscovery::NotUsed` for `attach --all` and every `detach`, so production `attach --all` /
`detach --all` never enter `dispatch` / `ActorBridge`. The same `ApfsSubstrate::new(MacOsApfsExecutionHost::new(...))`
sequence is already `NativeHealSource::open` (`gateway_inventory.rs:320-324`) and `ProjectRuntime` substrate setup.
After project-scoped attach, `ActorBridge::reconcile_gateway` calls `reconcile_native_project`
(`gateway_sessions.rs:458-464`). The store-wide arms never do.

Problem: one user command, two stacks. Store-wide mounts/unmounts volumes without telling the gateway.
`resolve_session_project_root` (`:1719-1813`) is a third store walker (`owner/repo/sessions` readdir +
`discover_session_images`) beside `discover_repositories`. Attach filters `role() != Main`; detach filters
`!name().is_main()` — equivalent only while `PlanError::RoleNameMismatch` holds.

Fix: add a store-wide attach/detach on `NativeGatewayInventory` (heal already has the substrate) that returns
`WorkspaceInfo` from F1's constructor, then from `dispatch_host_command` loop `reconcile_native_project` per
`AdoptedProject.repo_id`. Delete CLI's `ApfsSubstrate` construction, `store_workspace_info`, and the duplicated
attach/detach error wrappers. Named `detach` can keep `resolve_session_project_root` only until inventory can answer
"which checkout owns this session name" without opening git.

Cost/Risk: `CsCoreGatewayInv` owns the API. Gateway session install after a host-wide attach is the behavior change;
exec later reconciles per-project (`requires_gateway_before_dispatch` is Exec-only), so today's gap is "gateway view
stale until the next project-scoped exec or gateway restart".

### F3 — HIGH — SSOT — `doctor` emits `gateway-down` twice with different hints

Evidence: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:2863-2864` + `:2686-2736` + `:2964-2983`, vs
`cowshed-core/src/runtime/project.rs:6227-6243`.

```rust
match gateway_service::service_status().await {
    Ok(status) => diagnosis.findings.extend(gateway_findings(&status)),
    Err(error) => diagnosis.findings.push(Finding { code: "gateway-status".into(), ... }),
}
```

```rust
Ok(mut bridge) => {
    let project = bridge.doctor().await;
    // ...
    match project {
        Ok(report) => diagnosis.findings.extend(report.findings),
```

Core `ProjectRuntime::doctor` on a down socket:

```rust
findings.push(Finding {
    code: "gateway-down".into(),
    severity: FindingSeverity::Error,
    message: format!("gateway control socket does not answer at {}: {error}", ...),
    hint: "cowshed gateway start".into(),
```

CLI `gateway_findings` uses the same code `gateway-down` but a different message (launchd loaded vs not, cli version,
daemon version) and, when installed, hint `cowshed gateway stop && cowshed gateway start`.

Problem: `run_doctor_command` always runs `diagnose_host` (host-scoped gateway check) then, when cwd is an adopted
checkout, appends project doctor findings. A down gateway therefore yields two Error rows with the same code and
disagreeing recovery. `healthy` stays false either way; JSON consumers counting `findings` or unique codes do not.

Fix: gateway health is host-scoped (same argument this file already makes for unmounted mains at `:2739-2745`). Delete
the gateway socket probe from `ProjectRuntime::doctor`. CLI `gateway_findings` is the single source. Do not "dedupe by
code" on merge — that hides the double author.

Cost/Risk: `CsCoreProject` owns `doctor()`. Project-only doctor callers (N-API / tests) lose the gateway row unless they
compose with the host diagnosis the CLI already has.

### F4 — MEDIUM — STRUCTURE — one 4008-line module, `dispatch` ~450 lines, `host_storage_findings` ~177

Evidence: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:1-4008`; `dispatch` `:674-1124`; `host_storage_findings`
`:2507-2684`; tests `:3154-4008` (~855 lines).

Problem: ActorBridge, argv→DTO mapping, store-wide APFS attach/detach, `ls` landing measurement, host doctor rendering,
and the test suite share one file. `dispatch` is a 450-line command match. `host_storage_findings` is a 177-line
`VolumeState`/`HostAction` printer. Under 5k so not automatic, but the seams are already named in the comments.

Fix: split along existing types, no new abstraction: `bridge.rs` (`ActorBridge`, teardown), `dispatch.rs` (`dispatch` /
`run_bridge_command` / `run_host_command`), `listing.rs` (landing + tables), `host_doctor.rs` (`diagnose_host` +
findings), `store_sessions.rs` (attach/detach-from-store, gone after F2). Move `mod tests` with the type it pins.

Cost/Risk: `pub` surface used by `run.rs` / tests (`dispatch`, `dispatch_and_shutdown`, `run_bridge_command`,
`run_host_command`, `resolve_project_root`, `merge_primary`, `resolve_session_project_root`) must keep those names. No
behavior change.

### F5 — MEDIUM — SSOT — `DoctorReport.healthy` formula restated

Evidence: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:2922-2928`

```rust
fn doctor_report(findings: Vec<Finding>) -> DoctorReport {
    DoctorReport {
        healthy: !findings
            .iter()
            .any(|finding| finding.severity == FindingSeverity::Error),
        findings,
    }
}
```

Identical fold at `cowshed-core/src/runtime/project.rs:6470-6475`. `DoctorReport` (`dto.rs:605-608`) is a pair of fields
with no constructor, so every author re-derives "healthy means no Error".

Problem: a Warning-is-unhealthy policy change, or treating `gateway-version-skew` as Error, has two sites. The CLI tests
(`:3507-3514`, `:3806-3810`) pin exit 5 to this helper; core tests pin the other.

Fix: `DoctorReport::from_findings(Vec<Finding>)` on the DTO. Both `doctor_report` and `ProjectRuntime::doctor` call it.
Delete the CLI wrapper.

Cost/Risk: `CsCoreApi` owns `dto.rs`. One-line call-site edits.

### F6 — MEDIUM — DUPLICATION — listing sort and emit cascade written twice

Evidence: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:813-821` vs `:3130-3138`; `:1982-2020` vs `:2022-2053`.

```rust
projects.sort_by(|left, right| left.repo_id.cmp(&right.repo_id));
for project in &mut projects {
    project.workspaces.sort_by(|left, right| left.workspace.cmp(&right.workspace));
}
emit_project_listing(output, json, args, projects).await?;
```

`dispatch` `Command::List` `--all` and `dispatch_host_command` `Command::List` sort the same way then call
`emit_project_listing`. Production `ls --all` is `ProjectDiscovery::NotUsed`, so only the host arm runs; the `dispatch`
`--all` arm is for `dispatch()` with a `CliService` (tests). `emit_project_listing` and `emit_workspace_listing` then
repeat the landing → `--landed` retain → json → `--landing` table → `--landed` names → default table cascade.

Problem: listing order and `--landing`/`--landed`/`--json` precedence are user-visible contracts with two authors.

Fix: one `sort_project_listing` / `sort_workspace_listing`. Collapse emit into one function over
`repo_id: Option<&RepoId>` plus an iterator of workspaces (project listing is that iterator flattened, with per-project
`annotate_landing` first). `dispatch` List `--all` should call the same helper as the host arm, or go away if tests
construct `ProjectWorkspaces` themselves.

Cost/Risk: `command_dispatch.rs` tests that call `dispatch` with `--all` must still see the same order. Landing
annotation stays per-project so a missing main does not poison other repos.

### F7 — MEDIUM — TESTS — string oracles and a named-exhaustive test that is not exhaustive

Evidence: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:3797-3805`, `:3998-4005`, `:3174-3196`.

```rust
assert_eq!(
    finding.message,
    "acme/widget: main is not mounted at /Users/dev/src/widget \
     (image /private/cowshed/store/acme/widget/main.asif): main's volume is not mounted"
);
```

```rust
assert_eq!(
    String::from_utf8(stderr).unwrap(),
    "cowshed: [error mount] cowshed.store: present, not mounted\n\
     cowshed: [error mount] cowshed.caches: present, not mounted\n\
     cowshed: [error gateway-down] gateway: launchd loaded; control socket does not answer\n\
     next: cowshed setup\n\
     next: cowshed gateway stop && cowshed gateway start\n"
);
```

`cli_stdin_spelling` matches `WorkspaceFile => "--stdin-file"` (`:3178`) but
`every_core_stdin_variant_has_a_cli_spelling` constructs only Empty, Stream, and Inline. The match is the real
compile-fail seam (comment at `:3170-3173`); the test cannot go red on a wrong `--stdin-file` spelling. Doctor tests
assert `Output`'s `"cowshed: "` / `"next: "` prefixes (`output.rs`) and full `finding.message` prose. Typed pins already
exist beside them (`code`, `severity`, `path`, `exit.code`).

Problem: PERFORMANCE-HANDBOOK §7.10bb — a guard that cannot go red is not a guard. Rendered-string tests go red on copy
edits, not on the typed contract (`FindingSeverity::Error` ⇒ exit 5, unique hints).

Fix: construct `CoreStdinSource::WorkspaceFile(...)` in that test. For doctor, keep `code` / `severity` / `path` /
`healthy` / exit code; drop exact `message` and the full stderr blob. Unique-hints
(`doctor_prints_status_then_findings_then_unique_hints`) should assert the hint set, not `Output` framing.

Cost/Risk: none outside this file's `mod tests`. `setup_required_becomes_per_volume_findings_with_evidence` still uses
`message.contains` for uuid/path — tighten those to `path` and `code` where the DTO already carries them.

### F8 — LOW — DUPLICATION — host inventory open copied six times

Evidence: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:302-308`, `:315-321`, `:324-330`, `:1501-1507`,
`:1647-1653`, `:1913-1915`.

```rust
let home = gateway_service::canonical_home()?;
let storage = validate_existing_host_storage(&home).await?;
NativeGatewayInventory::new(storage)
```

Problem: six independent HOME→store→inventory sequences. A change to how storage is validated has six authors.
`diagnose_host` (`:2839`) opens home for `plan_host_setup` then calls `adopted_projects()` / `unmounted_mains()` which
open it again.

Fix: `async fn host_inventory() -> Result<NativeGatewayInventory>` returning inventory over one `ValidatedHostStorage`.
`adopted_projects` / `unmounted_mains` / `list_all_adopted_projects` / store-wide attach/detach / `resolve_detach_root`
take `&inventory` or call the helper. After F2, attach/detach may not live here.

Cost/Risk: trivial. `ValidatedHostStorage` is cheap to clone today (`storage.clone()` already at `:1504`, `:1650`).

### F9 — LOW — DUPLICATION — `HostAction` English restated from setup

Evidence: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:2433-2486` (`host_action_evidence`) vs
`packages/cowshed/crates/cowshed-cli/src/setup_service.rs:731-788` (`action_intent`). Both are exhaustive `match action`
arms. Wording differs (announce vs consent), so this is not a silent live bug: a new `HostAction` variant fails both
files closed.

Problem: two printers of one plan type. Size formatting already diverges (`{size_bytes} bytes` here vs `decimal_size` in
setup).

Fix: `HostAction` grows one structured description (name, uuid, paths, bytes) in core bootstrap; CLI setup and doctor
format that. Until then, do not copy more arms.

Cost/Risk: `CsCliServices` owns `setup_service.rs`; `CsCoreBootstrap` owns `HostAction`. Doctor tests that `contains`
uuid/path keep working if those fields stay on the struct.

## Cross-slice questions

- `CsCoreProject` (`runtime/project.rs`): is `snapshot` willing to become the only `WorkspaceInfo` builder? Will
  `doctor()` drop its gateway socket probe so F3 can land?
- `CsCoreGatewayInv` (`gateway_inventory.rs`): is `read_current_metadata` the required sidecar read for anyone mapping
  `DerivedWorkspace` → `WorkspaceInfo`? Can inventory grow store-wide attach/detach without opening `project_root` as
  git (the constraint written at `:1518-1523` and `:1664-1668`)?
- `CsCoreApi` (`api/dto.rs`): `DoctorReport::from_findings` — any objection?
- `CsCliServices` (`setup_service.rs`): `action_intent` vs `host_action_evidence` — one structured description, or keep
  two prose dialects on purpose?
- `CsCliArgs` (`args.rs` `ProjectDiscovery::NotUsed` for `attach --all` / `detach`): that routing is why F2's host arms
  exist; changing discovery would collide with this slice's dispatch.

## Non-findings (checked, clean)

- ActorBridge `open_for_adopt` / `open_existing` / `open_existing_for_identity_change` delegate to `ProjectRuntime`;
  `CliService` impl is a thin `Coordinator` adapter plus exec presentation/timeout. Not a second orchestrator for
  project-scoped verbs.
- `LANDING_TARGET_BRANCH` aliases `cowshed_core::runtime::project::DEFAULT_LANDING_BRANCH` (`:1974`). `STORE_ROOT` /
  `CACHES_ROOT` / `DEFAULT_IMAGE_CAPACITY` / `MOUNT_SERVICE_PLIST` / `RETIRED_LAYOUT_HINT` are imported, not recopied.
  Tests at `:3442-3475` pin that.
- `adopt_options` / `os_*` validate at the CLI boundary then fill core DTOs (`AdoptOptions`, `CreateOptions`,
  `PushOptions`, `ExecRequest`, …). Shapes are not restated as parallel structs. `ExecCommand` is CLI-only (timeout,
  session, background) wrapping `ExecRequest`.
- `env: HashMap::new()` / `trace: None` in `exec_command` (`:1347-1348`): CLI has no flags for those fields; empty is
  the DTO default, not a second schema.
- `ImageFormat` match `"asif"`/`"sparse"` beside `extension()` (`:710-718`): `extension()` is `asif`/`sparseimage` (file
  suffix). The last token is a format nickname. Two matches, two meanings; adding a variant fails both closed.
- Copies (`report.clone()` for JSON, `options.clone()` per attach, `PathBuf` clones into landing tasks,
  `GitOid::new(snapshot.base_commit.clone())`): once-per-invocation / once-per-workspace on `ls --landing` or
  `attach --all`. PERFORMANCE-HANDBOOK §4.1 regime: not a hot loop. The clone-then-reparse of sidecar strings is the F1
  SSOT bug, not a copies finding of its own.
- No `unsafe`. No production `unwrap`/`expect` on operational paths. Teardown uses `Result` / `merge_primary`.
  `cfg(target_os = "macos")` appears only in the test sidecar fixture (`:3953`).
- Dependencies this file actually uses: `cowshed-core` (load-bearing), `tokio` (load-bearing), `bytes` (`ExecRequest`
  stdin), `async-trait` (object-safe `dyn CliService` at `resolve_workspace` / `attach_scoped_sessions` / `slot_tenant`
  — keep), `base64` (in-process `--stdin-base64` decode with typed errors — keep; shell-out is not acceptable). `sha2` /
  `libc` / `toml` are crate deps unused by this file.
- `parse_duration` is local to CLI timeout flags; no second parser in this workspace.
- `child_exit_code` `128 + signal` is Unix presentation of `ExitStatus`, not a restated core field.
- `cli_stdin_spelling` match itself is a sound compile-time seam the other direction of `exec_command`; F7 is only the
  incomplete test.
