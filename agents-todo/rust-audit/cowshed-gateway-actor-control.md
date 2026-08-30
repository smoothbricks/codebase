# cowshed-gateway/actor+control

Scope: `packages/cowshed/crates/cowshed-gateway/src/actor.rs` (2206),
`packages/cowshed/crates/cowshed-gateway/src/control.rs` (1168). Supporting reads (not audited): `config.rs`
(`WorkspaceSession`, `ControlTcpConfig`), `policy.rs` (`mirror_scope_matches`, `CanonicalTarget::{origin,authority}`),
`interfaces.rs` (`AuditEvent`/`AuditKind`/`AuditStatus`), `proxy.rs` (`admit` send site), `cowshed-core/src/api/dto.rs`
(`GatewayStatus`/`AuditEvent`), `cowshed-gateway/Cargo.toml`.

## Summary

- `path_prefix_matches` in actor.rs is a byte-identical copy of `policy::mirror_scope_matches`; policy is the SSOT.
- Per-request admit path allocates HashMap `(String,String)` keys and `format!` origins on every
  `can_activate`/`activate`.
- Every activation pre-builds a full cancelled `AuditDraft` (string clones + `authority()`) even when the request
  finishes normally.
- Control-request field allowlist hand-restates `ControlRequestIn`; adding a serde field without the table silently
  rejects valid ops.
- `ControlRequestOut`/`ControlRequestIn` and `SessionWire`/`PolicyWire`/`GrantWire`/`MirrorWire` restate owned
  session/policy types.
- `127.0.0.1:7644` is parsed from a string here and constructed as `SocketAddr` in `ControlTcpConfig` (config.rs).
- `BrokerAuditKind`/`BrokerAuditStatus` restate subsets of `AuditKind`/`AuditStatus` with a manual map.
- Queue abort/reply/counter dance is copy-pasted five times; `unsafe { libc::geteuid() }` has no SAFETY comment.
- Control protocol in this crate is **not** the `cowshed-core` `dto.rs` `GatewayStatus`/`AuditEvent` (different shapes,
  same names).

## Findings

### F1 — HIGH — SSOT — `path_prefix_matches` is a second copy of `mirror_scope_matches`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/actor.rs:1830-1836`

```
fn path_prefix_matches(path: &str, prefix: &str) -> bool {
    path == prefix
        || prefix == "/"
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| prefix.ends_with('/') || suffix.starts_with('/'))
}
```

`packages/cowshed/crates/cowshed-gateway/src/policy.rs:439-445` is the same function under the name
`mirror_scope_matches`. Call site: `actor.rs:1759` (`!path_prefix_matches(&normalized, &resolved.admitted_prefix)`).
Problem: One prefix-boundary rule, two functions. A later edit to only one copy is a live redirect-escape or false-deny.
They agree today; that is luck, not a SSOT. Fix: Delete `path_prefix_matches`. Export `mirror_scope_matches` from
`policy.rs` (or a shared `path_prefix_matches` next to `normalize_path`) and call it from `build_seed`. Policy is the
SSOT: it already uses this rule to pick `admitted_prefix`. Cost/Risk: One call-site change in actor.rs plus a
`pub(crate)` on the policy helper. No wire change.

### F2 — HIGH — COPIES — origin-limit map allocates two `String`s on every admit/promote

Evidence: `packages/cowshed/crates/cowshed-gateway/src/actor.rs:1261-1294`

```
    fn can_activate(&self, workspace_id: &str, origin: &str) -> bool {
        ...
            && self
                .origins
                .get(&(workspace_id.to_owned(), origin.to_owned()))
                .copied()
                .unwrap_or(0)
                < self.config.limits.origin_active
    }
    fn activate(&mut self, seed: AdmissionSeed) -> Admission {
        ...
        let origin = seed.target.origin();
        let workspace_id = seed.workspace_id.clone();
        ...
        *self
            .origins
            .entry((workspace_id.clone(), origin.clone()))
            .or_default() += 1;
```

`seed.target.origin()` is `format!("{}://{}", …, self.authority())` (`policy.rs:155-157`), and `authority()` is another
`format!` (`policy.rs:148-152`). `admit` calls `origin()` at `actor.rs:1205` before `can_activate`; `promote` clones
`workspace_id` again at `actor.rs:1335` then calls both. Problem: Regime is **per proxied request** (and O(queue) on
`promote` after every completion), not startup. `HashMap<(String, String), usize>` cannot borrow-lookup `(&str, &str)`,
so every capacity check dumps two heap keys that are thrown away on miss. `activate` then formats origin again and
clones both sides for `entry`. Evaporating work (Byproduct L0); the histogram of active origins is the index, but the
key type forces a copy to read it. Fix: Nested `HashMap<String, HashMap<String, usize>>` (or intern workspace_id to the
session's generation/`u64` and keep origin as the inner key) so `can_activate` is
`.get(workspace_id).and_then(|m| m.get(origin))`. Compute `origin` once per admit and pass it through. Stop cloning
`pending.seed.workspace_id` in `promote` — `can_activate` already takes `&str`. Cost/Risk:
`complete`/`activate`/`can_activate`/`promote` only. `PermitState.origin` can keep the owned string for the decrement
path.

### F3 — HIGH — COPIES — every activation pre-builds a cancelled `AuditDraft`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/actor.rs:1279-1306` and `1838-1864`

```
        let cancelled =
            pending_audit_draft(&seed, AuditStatus::Cancelled, None, Some("request-dropped"));
        ...
        seed.activate(
            permit_id,
            CompletionLease::new(completion, permit_id, cancelled),
        )
```

```
        workspace_id: seed.workspace_id.clone(),
        repo_id: seed.repo_id.clone(),
        ...
        host: Some(seed.target.authority()),
        method: Some(seed.method.to_string()),
        path: Some(seed.request_path.clone()),
        ...
        trace_id: seed.trace_id.clone(),
        ...
        classification: classification.map(str::to_owned),
```

`CompletionLease::finish` (`actor.rs:596-597`) sets `cancelled = None` on the success path, so the draft is allocated
and dropped unused. Problem: Regime is **every admitted request**. The drop-audit needs those fields only if the lease
is dropped without `finish`. Success path pays a second copy of workspace/repo/endpoint/path/trace plus
`Method::to_string` plus `authority()` `format!`, then throws it away (Byproduct L0 / handbook §7.2: one allocation
consumed zero times is waste). Fix: Store a compact cancel payload on `CompletionLease` (`permit_id` + the seed fields
already moving into `Admission`, or `Arc<AdmissionSeed>` shared with `Admission`) and build `AuditDraft` only in `Drop`.
Do not call `pending_audit_draft` in `activate`. Cost/Risk: `CompletionLease`, `activate`, `pending_audit_draft`. Proxy
`finish` path unchanged.

### F4 — HIGH — SSOT — control-request allowlist restates `ControlRequestIn`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/control.rs:706-727`

```
    let allowed: &[&str] = match op {
        "status" => &["op"],
        "install" => &["op", "session"],
        "remove" => &["op", "workspaceId", "expectedRevision"],
        "audit-tail" => &["op", "workspaceId", "afterSequence", "limit", "follow"],
        "repo-mirror" => &["op", "request"],
        "sim-configure" => &["op", "config"],
        "sim-approve" => &["op", "approval"],
        "sim-list" => &["op", "repoId"],
        "sim-boot" => &["op", "repoId", "device"],
        ...
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
```

`ControlRequestIn` already has `#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]`
(`control.rs:318-357`). Parse is JSON → `Value` → allowlist → `from_value` (`control.rs:692-727`). Problem: Two schemas.
A new field on the enum that is omitted from `allowed` rejects a valid coordinator. A new `op` arm in the enum that is
omitted from the match returns `"unknown gateway control operation"` even if serde would accept it. Not diverged today;
the table is a live footgun. Also re-parses every control frame (L0 / §7.7), control-plane regime. Fix: Delete
`parse_control_request_value`'s allowlist and `Value` hop. `serde_json::from_slice::<ControlRequestIn>(bytes)` with
`deny_unknown_fields` is the single schema. Keep the existing tests in `control.rs:1118-1132` — they already go red if
unknown fields are accepted. Cost/Risk: Parser only. Error strings change from the hand-written messages to serde's;
update tests that match `Encoding(...)` text if any do (these tests only assert `is_err()`).

### F5 — MEDIUM — SSOT — `ControlRequestOut` and `ControlRequestIn` are the same protocol twice

Evidence: `packages/cowshed/crates/cowshed-gateway/src/control.rs:277-357` (Out tagged `op` kebab-case with borrowed
fields; In the same variants with owned `String`/`SessionWire`/etc.). Problem: Op names, field names (`workspaceId`,
`expectedRevision`, `afterSequence`, `repoId`), and variant set are duplicated. Adding `sim-boot`'s `device` to one side
only is a silent protocol split. Fix: One enum parameterized by lifetime / `Cow<'a, str>`, or generate In from Out via a
single serde type used for both encode and decode. Out is the SSOT for the wire names (it is what the coordinator
sends); In should not restate them. Cost/Risk: `GatewayControlClient` + `dispatch`. No daemon state change.

### F6 — MEDIUM — SSOT — control TCP address restated as a parse of `"127.0.0.1:7644"`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/control.rs:65-70`

```
        if address != "127.0.0.1:7644".parse().expect("literal control address")
            || !credential_file.is_absolute()
        {
            return Err(ControlError::InvalidTcpEndpoint);
        }
```

`ControlTcpConfig::new` / `validate` use `SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7_644)` (`config.rs:238-241`,
`252-253`). Problem: Same constant, two spellings. A port change that updates only config leaves
`GatewayControlClient::new_tcp` refusing the real listener (or the reverse: client connects to a port the daemon will
not bind). Fix: One `pub const CONTROL_TCP_ADDR: SocketAddr` next to `ControlTcpConfig` (config is the SSOT: it is what
the daemon binds). Compare against that in `new_tcp`. Do not `parse` a string at runtime. Cost/Risk: `control.rs` client
constructor + `config.rs`. Tests that parse `"127.0.0.1:7644"` should use the same const.

### F7 — MEDIUM — SSOT — control wire types restate `WorkspaceSession` / policy structs

Evidence: `packages/cowshed/crates/cowshed-gateway/src/control.rs:401-410` (`SessionWire`) vs `config.rs:175-183`
(`WorkspaceSession`); `control.rs:490-495` (`PolicyWire`) / `523-532` (`GrantWire`) / `566-574` (`MirrorWire`) with
`From`/`to_*` clones at `419-431`, `534-549`, `576-597`. Problem: Field lists for the install payload live in
config/policy **and** here. `GrantWire` also round-trips `BTreeSet<String>` methods through `Vec<String>`
(`control.rs:545`, `559`). A new `WorkspaceSession` field that is not added to `SessionWire` is silently dropped on the
control socket. Fix: Serde on the owned types (`WorkspaceSession`, `WorkspacePolicy`, `EgressGrant`, `MirrorRoute`) with
the token/CA secret encodings they already have (`WorkspaceToken::encode`/`parse`, `WorkspaceCa`). If a wire facade must
exist for zeroize-on-drop of PEM/token, generate it from the owned struct or share one schema type. Owned types are the
SSOT. Cost/Risk: Control install path + any fixture JSON in gateway tests. `deny_unknown_fields` must stay on the wire.

### F8 — MEDIUM — DUPLICATION — `BrokerAuditKind`/`BrokerAuditStatus` restate audit enums

Evidence: `packages/cowshed/crates/cowshed-gateway/src/actor.rs:56-69` and the map at `1513-1522`

```
        let kind = match event.kind {
            BrokerAuditKind::RepoMirror => AuditKind::RepoMirror,
            BrokerAuditKind::Sim => AuditKind::Sim,
        };
        let status = match event.status {
            BrokerAuditStatus::Allowed => AuditStatus::Allowed,
            ...
            BrokerAuditStatus::Completed => AuditStatus::Completed,
        };
```

Problem: Parallel enums plus a total map. A new `AuditStatus` used by brokers has to be added in three places
(`interfaces.rs`, this enum, this match) or the event cannot be recorded. The subset is not encoded in the type of
`BrokerAuditEvent`. Fix: Put `AuditKind`/`AuditStatus` on `BrokerAuditEvent` (they are already `Copy`). If the subset
must be closed, a converting constructor on `AuditKind`/`AuditStatus` in `interfaces.rs`, not a second enum in actor.rs.
Cost/Risk: `repo_mirror.rs` and `sim_broker.rs` call sites (other slices) change the type name only.

### F9 — MEDIUM — DUPLICATION — queued-request teardown is written five times

Evidence: abort-timer / abort-cancellation / decrement `session.queued` / decrement `global_queued` / optional
reply+draft appears in `cancel_queued` (`actor.rs:1371-1406`), `cancel_queued_request` (`1409-1436`), `expire_queued`
(`1438-1474`), `fail_closed` (`1595-1607`), `begin_drain` (`1639-1667`). Problem: Same state machine, five copies.
`cancel_queued_request` records audit and does not send `pending.reply` (OK: `proxy.rs:2570-2576` drops the admit
future, which drops the oneshot). `expire_queued` does send. The next edit that decrements counters in four of five arms
desynchronizes `global_queued` vs `queue.len()`. Fix: One `fn take_pending(&mut self, pending: Pending) -> Pending` that
aborts tasks and decrements counters; callers only choose the audit/reply. Actor is the SSOT for queue accounting.
Cost/Risk: Local to `impl Actor`. Queue invariants are the blast radius; existing gateway tests that cover
drain/timeout/cancel must stay.

### F10 — MEDIUM — STRUCTURE — `unsafe` `geteuid` with no SAFETY comment

Evidence: `packages/cowshed/crates/cowshed-gateway/src/control.rs:1038`

```
        || metadata.uid() != unsafe { libc::geteuid() }
```

and `packages/cowshed/crates/cowshed-gateway/src/actor.rs:1958` (same pattern in `remove_stale_socket`). Problem:
Rubric: `unsafe` without a stated invariant comment. `libc::geteuid` is unsafe only because the binding is marked
unsafe; the precondition is empty, but that fact is not written down. Fix: One `fn current_euid() -> u32` with
`// SAFETY: geteuid has no preconditions` and use it from both sites. Cost/Risk: Two call sites. No behavior change.

### F11 — MEDIUM — STRUCTURE — `install` / `admit` / `build_seed` exceed ~100 lines; control accept loop lives in actor.rs

Evidence: `install` `actor.rs:929-1048` (120), `admit` `1132-1259` (128), `build_seed` `1713-1828` (116),
`ControlRuntime` + `run_unix_control` + `run_tcp_control` `1979-2162` (184 lines of control-plane accept in the actor
module). Problem: Actor file mixes mailbox types, session install, admission/queue, TLS mint, and the Unix/TCP accept
loops that belong next to `serve_control_unix`/`serve_control_tcp`. Not a 5k-line god file; the seams are already named.
Fix: Move `ControlRuntime`/`run_*_control` into `control.rs`. Split `build_seed` by `RequestTarget` arm (mirror /
redirect / generic / sim) into functions that return the seed tuple. `admit` keeps the queue/auth policy; denial replies
go through one helper (they already share `denial_draft` + `record` + `admission_error`). Cost/Risk: Module move only
for control runtime. `Gateway::start_runtime` still constructs it.

### F12 — LOW — TESTS — credential-domain test asserts on `Debug` text

Evidence: `packages/cowshed/crates/cowshed-gateway/src/control.rs:1114`

```
        assert!(!format!("{credential:?}").contains(&data_token));
```

Problem: Guard on a rendered string. A `Debug` change that still leaks the token in a different format stays green; a
harmless `Debug` wording change goes red. Handbook §7.10bb: a guard that cannot go red on the defect is not a guard. The
`matches` assertions above it are the real test. Fix: Keep `assert!(!credential.matches(&data_token))`. For redaction,
assert against a typed leak (e.g. compare `format!("{credential:?}")` to the exact `"ControllerCredential([REDACTED])"`
implemented at `control.rs:660-663`), not `contains` of the raw token. Cost/Risk: This test only.

## Cross-slice questions

- `cowshed-core/src/api/dto.rs:2277-2289` `GatewayStatus` (`installed`/`running`/`socket`/…) and `dto.rs:2336-2346`
  `AuditEvent` are **not** the control-plane types in this slice (`actor.rs:394-410`, `interfaces.rs:302-353`). Same
  names, different protocols. CLI maps them (`cowshed-cli` `gateway_service.rs`). Confirm with the dto/CLI slices that
  this split is intentional and not a drifted restatement.
- `policy.rs` grant match uses `path.starts_with(prefix)` (`policy.rs:261` [INFERENCE from grep context]) while actor
  redirect uses boundary `path_prefix_matches`. If grant matching is prefix-byte `starts_with`, `/foo` admits `/foobar`.
  CsGwPolicy owns that.
- `Command::Admit` / `MintLeaf` / `AuditDenial` payloads are built in `proxy.rs` (`2548-2554`, `1229-1232`,
  `2520-2524`). Per-request `workspace_id.clone()` + bearer `String` + `RequestIntent` path live in that slice; this
  report only counts what the actor mailbox then clones again.
- `Git2RepoTransport` is constructed in `actor.rs:180-185` / `205-210`. Whether `git2` is still load-bearing vs `git` on
  PATH is the repo_mirror / deps slice.

## Non-findings (checked, clean)

- **DEP-BLOAT (this slice):** `serde_json`, `tokio`, `thiserror`, `http`, `libc`, `subtle` (`ct_eq`), `zeroize`,
  `base64` (controller token, 32 bytes URL_SAFE_NO_PAD) are load-bearing in-process. Shelling out to `base64`/`shasum`
  would be worse. `uuid` in `control.rs` tests is the crate's existing dep (cache/telemetry); not introduced here. No
  default-features leak visible from these two files.
- **dto.rs restatement:** control ops (`status`/`install`/`remove`/`audit-tail`/`repo-mirror`/`sim-*`) are not defined
  in `dto.rs`.
- **Mailbox oneshots:** `Command` does not `Clone`; large `WorkspaceSession` (PEMs) moves through `Install` once per
  control install — control-plane regime, not a finding.
- **`cancel_queued_request` dropping `reply`:** `proxy.rs` `AdmissionCancellation` Drop fires only when the admit future
  is dropped, so the oneshot receiver is already gone.
- **`try_reserve_owned().expect` / `take_completion().expect`:** invariants (completion channel sized to
  `global_active`; lease owned once), not operational unwraps.
- **TCP credential compare:** `ct_eq` then `&& valid` (`control.rs:650`); zeros-on-decode-fail does not authenticate
  unless `valid`.
- **`read_frame` 1 MiB cap + single trailing `\n`:** bounded; client writes LF only. Empty/multi-frame mapped to
  `MessageTooLarge` is coarse but not a hot-path issue.
- **No tests in actor.rs:** tests for this behavior live in other gateway test files (not this slice). `control.rs`
  schema tests can go red on extra fields.
- Actor loop awaiting `audit.record` is fail-closed sequencing (`next_audit` assigned on the actor), not evaporating
  work.
- `revisions: HashMap<String, u64>` is a tombstone after `remove` (monotonic fence), not a duplicate of
  `SessionState.revision`.
