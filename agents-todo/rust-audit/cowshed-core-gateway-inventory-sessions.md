# cowshed-core/gateway-inventory+sessions

Scope: `packages/cowshed/crates/cowshed-core/src/gateway_inventory.rs` (2591),
`packages/cowshed/crates/cowshed-core/src/gateway_sessions.rs` (511). Rubric: `BYPRODUCT-ENGINEERING.md`;
`docs/handbook/04-mechanisms.md`; `docs/handbook/05-memory-toolkit.md`; `docs/handbook/02-measurement.md` §4.1. Targeted
reads: `cowshed-gateway/src/{actor,control,config}.rs`, `cowshed-core/Cargo.toml`, `cowshed-gateway/Cargo.toml`,
`repository.rs:14-20`, `storage/apfs/native.rs:851-857`, `metadata.rs:13-16,868-878`, `api/dto.rs:972-978`,
`packages/cowshed/src/types.ts:81-86`.

## Summary

- Three reserved-namespace tables for the same store-root scan disagree (`tmp`/`quarantine`/`run`); inventory will skip
  a live `tmp/*` project that `RepoId` still accepts.
- `adopted_projects_blocking` reports every `authoritative_checkout_path` error as "no adopted checkout path", so
  `unmounted_mains` never sees those projects.
- `reconcile_project` maps every control-plane `status()` failure to `gateway_absent`, so a running-but-broken gateway
  is diagnosed as missing.
- `cowshed-core` depends on the full `cowshed-gateway` daemon crate for session types + the Unix control client;
  direction is right, grain is wrong.
- `load_project` / `load_project_workspaces` re-derive the same fact pipeline; kernel mounts are snapshotted and cloned
  once per project.

## Findings

### F1 — HIGH — SSOT — Store-root reserved-namespace lists disagree

Evidence: `packages/cowshed/crates/cowshed-core/src/gateway_inventory.rs:992-1003`

```
const RESERVED_STORE_NAMESPACES: &[&str] = &[
    "caches",
    "telemetry",
    "gateway",
    "mnt",
    "run",
    "tmp",
    "quarantine",
];
fn is_reserved_store_namespace(name: &str) -> bool {
    name.starts_with('.') || RESERVED_STORE_NAMESPACES.contains(&name)
}
```

`packages/cowshed/crates/cowshed-core/src/repository.rs:14-20`

```
const RESERVED_LAYOUT_OWNERS: &[&str] = &[
    "gateway",
    "telemetry",
    "caches",
    "mnt",
    ".cowshed-volume.json",
];
```

`packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs:851-857`

```
fn is_project_owner_directory(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            !name.starts_with('.')
                && !matches!(name, "mnt" | "caches" | "gateway" | "telemetry" | "run")
        })
}
```

Problem: One concept (which store-root children are not `<owner>/<repo>` projects) is three tables. Live divergence:
inventory skips `tmp`, `quarantine`, and `run`; the APFS walker skips only `run` of those three; `RepoId::parse` /
`encode_layout_owner` encode neither `tmp` nor `quarantine` nor `run`. A project whose owner is `tmp` (legal `RepoId`)
is stored at `<store>/tmp/<repo>` and is invisible to gateway discovery while still visible to the APFS project walk.
`run` is the same class against `repository.rs`. Fix: One `&[&str]` (plus the existing volume-marker structural test in
`is_not_a_project_namespace`) owned next to `STORE_ROOT`. Inventory skip, APFS owner filter, and layout-owner encoding
all consume it. Decision: inventory's list plus the marker test is the source — it is the discovery contract — and the
other two lists delete. Cost/Risk: `repository.rs` encoding and APFS restore enumeration change together; a host that
already adopted `tmp/*` or `run/*` would flip from "invisible" to "found" or the reverse depending on which list wins.

### F2 — HIGH — STRUCTURE — `adopted_projects` swallows checkout errors as "no path"

Evidence: `packages/cowshed/crates/cowshed-core/src/gateway_inventory.rs:461-468`

```
            match authoritative_checkout_path(&layout, &repo_id) {
                Ok(Some(project_root)) => projects.push(AdoptedProject {
                    repo_id,
                    project_root,
                }),
                Ok(None) | Err(_) => {
                    eprintln!("cowshed: skipping {repo_id}: it records no adopted checkout path");
                }
            }
```

`unmounted_mains` documents the opposite (`gateway_inventory.rs:582-585`): a project whose facts cannot be read is
reported unreachable with that failure as its reason. It iterates `adopted_projects_blocking()` (`:595`), so the
swallowed `Err` never arrives. Problem: `authoritative_checkout_path` (`:1259-1307`) returns `InvalidMetadata` for
duplicate main image formats and identity mismatch. Those are cowshed's own corrupted state. Collapsing `Err(_)` into
the `Ok(None)` message hides them on stderr and drops the project from doctor/setup's adopted set — the host looks
healthy. Cantrill: the error value is discarded; the printed sentence is a lie. Fix: Split the match. `Ok(None)` may
skip (no checkout). `Err(error)` must propagate, or at minimum be pushed as
`UnreachableMain { reason: error.to_string(), ... }` on the `unmounted_mains` path. Delete the `Err(_)` arm. Cost/Risk:
`adopted_projects` callers (CLI attach/detach/doctor) start seeing `InvalidMetadata` they currently skip. That is the
point.

### F3 — HIGH — STRUCTURE — Every control `status()` error is "gateway is not available"

Evidence: `packages/cowshed/crates/cowshed-core/src/gateway_sessions.rs:304-305`

```
    let status = control.status().await.map_err(|_| gateway_absent(uid))?;
    reconcile_against_status(control, host, project_prefix, desired, status).await
```

`packages/cowshed/crates/cowshed-core/src/gateway_sessions.rs:54-69,496-499`

```
pub trait GatewayControl: Send + Sync {
    async fn status(&self) -> std::result::Result<GatewayStatus, String>;
    ...
}
        GatewayControlClient::status(self)
            .await
            .map_err(|error| error.to_string())
pub const GATEWAY_START_HINT: &str = "cowshed gateway start";
pub fn gateway_absent(_uid: u32) -> CowshedError {
    CowshedError::environment_missing("cowshed gateway is not available", GATEWAY_START_HINT)
}
```

Problem: `GatewayControlClient::status` already returns a typed `ControlError` (connect, protocol, invalid response).
The trait erases it to `String`, then `reconcile_project` throws the string away and always emits
`environment_missing` + `cowshed gateway start`. A live gateway that answers garbage, or a uid/socket-mode refusal, is
diagnosed as "not installed". `_uid` is unused. Operational failure is not a `Result` of what happened. Fix: Put
`ControlError` (or `CowshedError`) on `GatewayControl::status`. Map `ControlError` connect-to-missing-socket onto
`gateway_absent`; every other arm keeps its text. Delete `_uid` or use it in the message. Cost/Risk: Fake
`GatewayControl` impls in `cowshed-core/tests/gateway_sessions.rs` change signature. CLI copy that keys on
`environment-missing` needs the connect-only case.

### F4 — MEDIUM — DEP-BLOAT — `cowshed-core` links the full gateway daemon for types + a Unix client

Evidence: `packages/cowshed/crates/cowshed-core/Cargo.toml:21`

```
cowshed-gateway = { path = "../cowshed-gateway" }
```

`packages/cowshed/crates/cowshed-core/src/gateway_sessions.rs:20-23`

```
use cowshed_gateway::{
    EgressGrant, GatewayControlClient, GatewayHandle, GatewayStatus, MirrorProtocol, MirrorRoute,
    WorkspaceCa, WorkspaceEndpoint, WorkspacePolicy, WorkspaceSession, WorkspaceToken,
};
```

`packages/cowshed/crates/cowshed-gateway/Cargo.toml:17-24` (no feature split)

```
hyper = { version = "1", features = ["http1", "http2", "server", "client"] }
...
rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
rustls-platform-verifier = "0.7"
```

Problem: Dependency _direction_ is right: the controller (core) fills a cache whose schema (`WorkspaceSession`,
`GatewayStatus`, control client) lives in the gateway crate; inverting it would drag APFS/inventory into the daemon.
Grain is wrong. These two files need config types + `GatewayControlClient`/`GatewayHandle`. They do not need hyper,
rustls, the proxy, or the macOS `security-framework` stack, and `cowshed-gateway` exposes no features to leave them
behind. Precedent in this repo: `git2` was removed because the extra TLS/openssl graph was not earning its keep. `sha2`
(identity digest), `async_trait` (object-safe `HealSource`/`InventorySource`), and `libc` (`geteuid`, `O_NOFOLLOW`) in
this slice _are_ load-bearing — in-process, typed, not a CLI you can parse. Do not shell those out. Fix: Split
`cowshed-gateway` into a protocol crate (config types, policy types, Unix/TCP control client, `GatewayHandle` command
types) and the daemon. `cowshed-core` depends on the protocol crate only. Do not invert core ↔ gateway. Cost/Risk:
Public re-exports in `cowshed-gateway/src/lib.rs` and CLI `gateway_service.rs` move with the split. One compile-graph
cut, no behavior change.

### F5 — MEDIUM — DUPLICATION — `load_project` and `load_project_workspaces` re-run one pipeline

Evidence: `packages/cowshed/crates/cowshed-core/src/gateway_inventory.rs:829-860` and `:909-951` (same prelude, quoted
once):

```
        let authoritative = self.source.project_facts(&self.storage, repo_id)?;
        reject_duplicate_mount_facts(&authoritative.mounts)?;
        let derived = derive_workspaces(
            authoritative.storage,
            authoritative.mounts,
            authoritative.checkpoints,
        )?;
        let layout = StorageLayout::new(self.storage.store(), repo_id).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: self.storage.store().to_owned(),
                message: error.to_string(),
            }
        })?;
        ...
            let image_paths = canonical_image_paths(&layout, &workspace.workspace)?;
            let metadata = read_current_metadata(
                self.storage.store(),
                image_paths.image(),
                &workspace.workspace,
            )?;
```

Problem: Two functions independently reconstruct layout + derived workspaces + sidecar metadata, then project different
DTOs (`WorkspaceInfo` vs `GatewaySessionFact`). A third caller, `all_reserved_port_bases_blocking` (`:755-784`), reads
metadata again via `project_facts` + `canonical_image_paths` + `read_current_metadata`. The comment on
`ProjectInventoryFacts` (`:168-172`) already names this class of bug: sourcing the same field twice is how checkpoints
vanished. Fix: One `fn load_derived(repo) -> Result<(StorageLayout, Vec<DerivedWorkspace>, ...)>`. `load_project` /
`load_project_workspaces` / port-base scan map that value. No second `derive_workspaces`. Cost/Risk: Local to
`gateway_inventory.rs`. Tests already cover both projections.

### F6 — MEDIUM — SSOT — Empty egress `ports` means `[443, 80]` only in the converter

Evidence: `packages/cowshed/crates/cowshed-core/src/gateway_sessions.rs:191-196`

```
        let ports: Vec<u16> = if rule.ports.is_empty() {
            vec![443, 80]
        } else {
            rule.ports.clone()
        };
```

`packages/cowshed/crates/cowshed-core/src/metadata.rs:870-873`

```
pub struct EgressRule {
    pub host: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<u16>,
```

`packages/cowshed/src/types.ts:81-83`

```
export interface EgressRule {
  readonly host: string;
  readonly ports?: readonly number[];
```

Problem: Omitted/empty ports is a schema meaning with no owner. The TS type and the Rust DTO leave it optional/empty;
only `policy_from_grants` invents 443 and 80. A reader of `GrantSet` cannot tell "no ports" from "default http/https".
The integration test in `cowshed-core/tests/gateway_sessions.rs:417-443` pins the expansion, so the default is real
policy, not a comment. Fix: Put `const DEFAULT_EGRESS_PORTS: [u16; 2] = [443, 80];` on `EgressRule` (metadata.rs is the
grant SSOT) and a method `fn effective_ports(&self) -> Cow<'_, [u16]>`. `policy_from_grants` calls it. Document the same
default on the TS field. Delete the inline `vec![443, 80]`. Cost/Risk: Any caller that treated empty as "no listener"
starts opening 443/80 — grep `ports.is_empty` before moving the constant. Metadata slice owns the type.

### F7 — MEDIUM — COPIES — Kernel mount table is recaptured and cloned per project

Evidence: `packages/cowshed/crates/cowshed-core/src/gateway_inventory.rs:197-228`

```
impl KernelMountSource for CapturedKernelMountSource {
    fn mounts(&self) -> Result<Vec<KernelMountSnapshot>, ApfsStorageError> {
        Ok(self.mounts.clone())
    }
}
...
        let captured = SystemKernelMountSource.mounts()?;
        let host = MacOsApfsExecutionHost::with_mount_source(
            SystemCommandRunner,
            config.clone(),
            CapturedKernelMountSource {
                mounts: captured.clone(),
            },
        )?;
```

Called from `project_facts`, which `load_project`, `load_project_workspaces`, `all_reserved_port_bases_blocking`, and
`unmounted_mains_blocking` each invoke once per repo. Problem: Regime is once-per-inventory-pass (gateway `RunAtLoad`,
doctor, status), not a per-byte hot loop — do not treat this as a µs finding. It is still L0 evaporating work with a
closed-form size: the kernel mount table is one host snapshot, then cloned into every project's
`CapturedKernelMountSource` and cloned again on every `mounts()` call. N projects ⇒ N `mount(8)`-class snapshots plus N
vector clones. Fix: Snapshot `SystemKernelMountSource.mounts()` once on `NativeGatewayInventory` (or once per
`*_blocking` entry), pass `&[KernelMountSnapshot]` into `project_facts`. `CapturedKernelMountSource` should borrow or
hold `Arc<[KernelMountSnapshot]>`, not `Vec` + `clone()`. Cost/Risk: `InventorySource::project_facts` signature grows a
mounts argument, or `NativeInventorySource` holds the snapshot. Tests using `FixtureSource` are unaffected.

### F8 — LOW — STRUCTURE — `unsafe { libc::geteuid() }` has no SAFETY comment

Evidence: `packages/cowshed/crates/cowshed-core/src/gateway_sessions.rs:487-488`

```
pub fn effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}
```

`packages/cowshed/crates/cowshed-core/src/gateway_inventory.rs:1241-1244`

```
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err("typed JSON file is not controller-owned mode 0600".to_owned());
```

Problem: Rubric: `unsafe` without a stated invariant. `geteuid` is a pure libc query with no pointer; the block is still
`unsafe` in Rust and is copy-pasted across gateway/core without a one-line contract. Fix: One
`pub fn effective_uid() -> u32` (already in `gateway_sessions.rs`) with
`// SAFETY: geteuid is always defined and reads no memory.` Inventory's JSON reader calls that function. Delete the
second `unsafe`. Cost/Risk: None.

### F9 — LOW — DUPLICATION — Hex-of-N-bytes is written twice

Evidence: `packages/cowshed/crates/cowshed-core/src/gateway_sessions.rs:178-183`

```
fn hex_prefix(bytes: &[u8], count: usize) -> String {
    let mut encoded = String::with_capacity(count * 2);
    for byte in &bytes[..count] {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}
```

`packages/cowshed/crates/cowshed-core/src/api/dto.rs:972-978` (`Sha256Digest::to_hex`) is the same loop over 32 bytes.
Problem: `stable_workspace_id` / `project_session_prefix` need 16-byte prefixes of SHA-256. The encode loop is a second
copy of `Sha256Digest::to_hex`. Regime is once per session identity (startup/reconcile) — not a copies finding, a SSOT
finding. `bytes[..count]` also panics if `count > bytes.len()`; today the only callers pass 16 on a 32-byte digest. Fix:
`Sha256Digest::to_hex` (or a shared `fn hex_lower(bytes: &[u8]) -> String`) is the source. `hex_prefix` becomes
`&digest[..16]` fed to that. Do not add a `hex` crate. Cost/Risk: dto.rs is another slice; if they decline, keep the
helper but take `[u8; 16]` so the slice cannot panic.

### F10 — LOW — STRUCTURE — `all_attached` treats `DuplicatePortBlock` as fatal, but `load_project` never produces it

Evidence: `packages/cowshed/crates/cowshed-core/src/gateway_inventory.rs:792-802`

```
                    if matches!(
                        error,
                        GatewayInventoryError::DuplicateRepository(_)
                            | GatewayInventoryError::DuplicatePortBlock(_)
                            | GatewayInventoryError::AmbiguousProjectRoot(_)
                            | GatewayInventoryError::ForeignBinding { .. }
                    ) {
                        return Err(error);
                    }
```

`DuplicatePortBlock` is constructed only in `all_reserved_port_bases_blocking` (`:778-780`). `load_project` (`:909-988`)
has no port-uniqueness check. Problem: Dead match arm. Store-wide attach will skip two workspaces that share a port (as
`InvalidMetadata` or success) and still serve both; the only uniqueness gate is a different API. The arm documents an
invariant the function does not enforce. Fix: Either enforce port uniqueness inside `load_project` (then the arm is
live) or delete `DuplicatePortBlock` from this match. Decision: enforce in `load_project` / `all_attached_blocking` with
a `BTreeSet<u16>` across the store-wide scan — that is the integrity check `all_reserved_port_bases` already believed
was on this path. Cost/Risk: A host that today serves two attached workspaces on one block starts failing
`all_attached`. That failure is correct.

## Cross-slice questions

- `cowshed-gateway/src/config.rs:20-22` restates `MACOS_PORT_MIN/MAX/BLOCK_SIZE` against
  `cowshed-core/src/metadata.rs:13-16` (`MACOS_PORT_BLOCK_MIN/MAX`, `PORT_BLOCK_SIZE`). This slice does not copy them
  (it uses `PortBlock::base()`). Metadata vs gateway-config slices own the constants; they already disagree in _names_.
  One table.
- `cowshed-gateway/src/actor.rs:403-407` `SessionStatus.endpoint: String` vs `WorkspaceEndpoint` Display
  (`config.rs:33-39`) is the string this slice compares in `reconcile_against_status` (`gateway_sessions.rs:373-384`).
  Gateway-actor slice: the status type should carry `WorkspaceEndpoint`, not a rendered string.
- `cowshed-gateway/src/config.rs:504-506` restates the leaf name `"gateway.sock"` that `control_socket_path`
  (`gateway_sessions.rs:43-44`) derives from `STORE_ROOT`. After F4's protocol split, the leaf constant lives with the
  path function.
- `metadata.rs:1130` `read_json` vs `read_typed_json_nofollow` (`gateway_inventory.rs:1223-1256`): two JSON readers, the
  inventory one is the nofollow/uid/0600/size-bounded variant. Metadata slice should absorb it or call it.
- F1's other two tables are `repository.rs` (CsCoreMetadata) and `storage/apfs/native.rs` (APFS slice). F6's
  `EgressRule` default belongs in metadata.rs + `packages/cowshed/src/types.ts`.

## Non-findings (checked, clean)

- Dependency _direction_ core → gateway is the correct one: inventory/reconcile are the controller;
  `WorkspaceSession`/`GatewayStatus` are the cache schema. `GatewaySessionFact` is a host projection (mount, grants,
  credentials, incarnation), not a hand-restated `WorkspaceSession`. Conversion is `session_from_fact`. Do not invert
  the crates.
- `control_socket_path` is defined once, from `STORE_ROOT`. Comment at `gateway_sessions.rs:40-42` is accurate.
- `sha2` for `stable_workspace_id` / `project_session_prefix`: in-process, collision-resistant IDs feeding replay
  tombstones. Load-bearing. Not `shasum(1)`.
- `async_trait` on `HealSource`/`ProjectMounts`/`InventorySource`/`GatewayControl`: required for the test seams
  (`FixtureSource`, `FakeHealSource`). Load-bearing.
- `libc` `geteuid` / `O_NOFOLLOW`: in-process uid and open flags. Load-bearing.
- Production path has no operational `unwrap`. `WorkspaceName::new("main").expect("fixed main")` (`:609`) and
  `write!(..., "{byte:02x}").expect(...)` (`gateway_sessions.rs:181`) are invariants.
- `identity_owner` `cfg(any(target_os = "macos", test))` (`:1098`) is documented; Linux lib builds omit a product path
  they cannot reach.
- File size 2591 (1050 of those are tests) is under the 5k god-file bar. Natural seams already exist (discovery, fact
  load, heal, path helpers) but are not a finding at this size.
- Inventory tests assert typed values (`RepoId`, `WorkspaceState`, `GatewayInventoryError` variants, mount order). The
  `format!("{facts:?}")` check (`:1896-1898`) is the Debug-redaction contract, not a rendered-DTO oracle.
- PEM `to_owned`, `dispatch_blocking` `self.clone()`, `ports.clone()`, `config.clone()`: once-per-inventory/reconcile,
  not a hot loop. §4.1 regime: note, not a copies finding.
- `install_all_sessions` (fail-fast restore) vs `reconcile_against_status` (per-project repair, collect install
  failures) are different jobs, not two implementations of one algorithm.
- No `TODO`/`FIXME` in either file.
