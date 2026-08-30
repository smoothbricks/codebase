# cowshed-gateway/policy+config+platform+tls

Scope: `packages/cowshed/crates/cowshed-gateway/src/sim_broker.rs` (1244), `policy.rs` (641), `config.rs` (676),
`interfaces.rs` (365), `platform.rs` (354), `tls.rs` (179), `lib.rs` (61). Doctrine: `BYPRODUCT-ENGINEERING.md`,
`docs/handbook/04-mechanisms.md`, `05-memory-toolkit.md`, `02-measurement.md` §4.1. Neighbouring reads (duplication
only): `cowshed-gateway/Cargo.toml`, `actor.rs:185-275`, `repo_mirror.rs:1300-1323`,
`cowshed-core/{Cargo.toml,metadata.rs:12-16,repository.rs:107-131,workspace_credentials.rs:8-26,sandbox.rs:10-16,gateway_sessions.rs:186-236}`,
`packages/cowshed/src/types.ts:25-26`.

## Summary

- `validate_repo_id` is restated three times and already disagrees: gateway `config`/`sim_broker` allow uppercase ASCII;
  core/`repo_mirror` require `[a-z0-9][a-z0-9._-]*`.
- Simulator JSON verbs are kebab-case `open-url`; core `SimVerb`, TS `SimVerb`, grant files, docs, and audit strings are
  `openurl`.
- macOS port-block bounds live in both `cowshed-core::metadata` and `cowshed-gateway::config` under different names.
- `sim_broker` is production iOS `simctl` (not a test double) and is compiled, exported, and spawned on every host
  including Linux.
- `rustls-pki-types` is a direct dep nobody imports; TLS types come from `rustls::pki_types`.
- `is_private` does not fold IPv4-mapped IPv6, so the wildcard-to-private rebinding fence is incomplete.
- Percent-decode, ALPN lists, and `TOKEN_BYTES` are copied; `CanonicalHost::as_str` allocates a `String`.
- rcgen feature split vs cowshed-core is intentional (`Issuer::from_ca_cert_pem` needs `x509-parser`; core only
  self-signs).

## Findings

### F1 — HIGH — SSOT — `repo_id` charset restated and already diverged

Evidence: `packages/cowshed/crates/cowshed-gateway/src/config.rs:210-228`

```
fn validate_identifier(field: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
```

`packages/cowshed/crates/cowshed-gateway/src/sim_broker.rs:846-866` (same charset; repo errors collapse to
`InvalidIdentifier`). `packages/cowshed/crates/cowshed-gateway/src/repo_mirror.rs:1300-1319` restates core and requires
lowercase:

```
/// Mirror of `validate_identity_component` in cowshed-core's `repository.rs`
bytes.first().is_none_or(|first| !first.is_ascii_lowercase() && !first.is_ascii_digit())
|| !bytes.iter().all(|byte| {
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(byte)
})
```

`packages/cowshed/crates/cowshed-core/src/repository.rs:121-127` is the same lowercase rule. Problem: one identity,
three predicates. `Owner/Repo` and `Foo_Bar/baz` pass session install and sim configure, fail repo-mirror/core. Live
divergence, not a future risk. Fix: one function with core's `[a-z0-9][a-z0-9._-]*` rule. Gateway cannot import core
(core depends on gateway), so put it in gateway and have core call it, or extract a types crate. Delete the two looser
copies. Map sim failures to a repo-id error, not `InvalidIdentifier`. Cost/Risk: any test/fixture using uppercase
`repo_id` on the gateway session/sim path must change. That is the point.

### F2 — HIGH — SSOT — sim verb wire name is `open-url`; every other layer says `openurl`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/sim_broker.rs:28-39`

```
#[serde(rename_all = "kebab-case")]
pub enum SimGrant {
    OpenUrl,
    Install,
}
#[serde(tag = "verb", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SimRequest {
    OpenUrl { device: String, url: String },
    Install { device: String, digest: String },
}
```

`packages/cowshed/crates/cowshed-core/src/metadata.rs:884-889` uses `#[serde(rename = "openurl")]`.
`packages/cowshed/src/types.ts:26` is `export type SimVerb = 'openurl' | 'install'`. Broker audit/result strings are
`"openurl"` (`sim_broker.rs:473,488,497`). Problem: kebab-case emits `open-url`. Grant files, TS, core, docs, and
`simctl` use `openurl`. A client that follows the grant spelling is a 400 at `POST /sim`. Internal Rust construction
hides this (tests never JSON-round-trip the happy path). Fix: `#[serde(rename = "openurl")]` on `SimGrant::OpenUrl` and
`SimRequest::OpenUrl`, matching `SimVerb`. Delete kebab-case on these two enums. Add a positive JSON parse test for
`{"verb":"openurl",...}` and a reject test for `open-url`. Cost/Risk: any already-deployed control JSON using `open-url`
breaks; nothing in-tree produces that spelling from GrantSet (core `policy_from_grants` ignores `grants.sim` entirely —
see Cross-slice).

### F3 — HIGH — SSOT — macOS port-block bounds copied under different names

Evidence: `packages/cowshed/crates/cowshed-gateway/src/config.rs:19-22`

```
pub const TOKEN_BYTES: usize = 32;
pub const MACOS_PORT_MIN: u16 = 40_960;
pub const MACOS_PORT_MAX: u16 = 49_151;
pub const MACOS_PORT_BLOCK_SIZE: u16 = 16;
```

`packages/cowshed/crates/cowshed-core/src/metadata.rs:13-16`

```
pub const PORT_BLOCK_SIZE: u16 = 16;
pub const MACOS_PORT_BLOCK_MIN: u16 = 40_960;
pub const MACOS_PORT_BLOCK_MAX: u16 = 49_151;
pub const MACOS_PORT_BLOCK_LAST_BASE: u16 = MACOS_PORT_BLOCK_MAX - PORT_BLOCK_SIZE + 1;
```

Problem: core allocates blocks from its constants; gateway `validate_macos_port_block` admits from its own. Values agree
today. If they drift, core hands sessions the gateway will refuse. Two names for one interval (`MIN` vs `BLOCK_MIN`).
Fix: single source in gateway (already `pub use`d from `lib.rs`). Core already depends on `cowshed-gateway`; delete the
metadata copies and import. Keep `LAST_BASE` as a derived `const` next to them. Cost/Risk: rename at core call sites
(`project.rs` port claim loop). No behavior change if values stay 40960/49151/16.

### F4 — HIGH — STRUCTURE — simctl broker ships and is spawned on every platform

Evidence: `packages/cowshed/crates/cowshed-gateway/src/lib.rs:19,57-59` (`mod sim_broker` and `pub use` of
`SimRequest`/`SimRunner`/…). `sim_broker.rs:92-99`

```
pub struct XcrunSimRunner;
impl SimRunner for XcrunSimRunner {
    async fn run(&self, command: SimCommand) -> Result<SimCommandOutput, SimBrokerError> {
        let mut process = Command::new("/usr/bin/xcrun");
        process.env_clear().arg("simctl");
```

`actor.rs:192,217,270-273` always passes `Arc::new(XcrunSimRunner)` and always `SimBrokerHandle::start(...)` with no
`cfg`. Problem: this is not a test simulation. It is the production iOS Simulator broker. It must not be `cfg(test)`. It
also must not be linked or spawned on Linux: `/usr/bin/xcrun` does not exist, yet every `Gateway::start_host`/`start`
still starts the actor, and control `sim-configure`/`sim-boot`/`sim-list` stay live. 1244 lines plus a task in the Linux
production artifact. Fix: `cfg(target_os = "macos")` the module, the `lib.rs` re-exports, `XcrunSimRunner`, and the
actor start. Linux gets a stub `SimBrokerHandle` that returns `InstallDisabled`/`NotGranted` without spawning. Do not
feature-flag it on macOS — it is load-bearing there. Cost/Risk: actor/control/proxy (other slices) need the same `cfg`.
Linux control tests that exercise sim ops must expect the stub.

### F5 — MEDIUM — SSOT — `TOKEN_BYTES = 32` restated

Evidence: `packages/cowshed/crates/cowshed-gateway/src/config.rs:19` and
`packages/cowshed/crates/cowshed-core/src/workspace_credentials.rs:26` (`const TOKEN_BYTES: usize = 32`). Problem: mint
path (core) and parse path (gateway) each own the length. Drift is a silent `MalformedToken` on every session install.
Fix: use `cowshed_gateway::TOKEN_BYTES` from core (dependency already exists). Delete the private copy. Keep
`TOKEN_ENCODED_BYTES` as a derived check or compute from the constant. Cost/Risk: one-line core change.

### F6 — MEDIUM — DUPLICATION — percent-decode written twice in `policy.rs`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/policy.rs:512-558` (`normalize_mirror_suffix` npm arm) and
`561-593` (`normalize_path`). Both walk `%HH` with the same `hex` helper; they differ only in forbidden decoded bytes
(`/` rejected in `normalize_path`, allowed for npm `%2f` scopes) and in whether `?`/`\r`/`\n`/`//` are rejected.
Problem: two parsers for one URL-decode. A third copy lives in `mirror.rs:1082` (`fn hex`). A future path-safety fix
applied to one will miss the other. Fix: one `decode_percent(path, Forbid)` parameterized by the extra banned bytes.
`normalize_path` and the npm arm become callers. Delete `mirror.rs`'s `hex` in that slice. Cost/Risk: npm scoped-package
admission (`%2f`) must keep allowing decoded `/` in the admission path only. Table-drive that in the existing mirror
tests.

### F7 — MEDIUM — DEP-BLOAT — `rustls-pki-types` is unused

Evidence: `packages/cowshed/crates/cowshed-gateway/Cargo.toml:25` (`rustls-pki-types = "1"`). Slice uses
`rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName}` (`tls.rs:8-11`, `interfaces.rs:5`). Grep of the
crate finds no `rustls_pki_types` import. Problem: direct dep of a crate that rustls already re-exports. Extra lockfile
node, extra version to keep aligned. Fix: delete the dependency. Keep using `rustls::pki_types`. Cost/Risk: none if no
out-of-crate `extern crate` (none found).

### F8 — MEDIUM — STRUCTURE — `is_private` does not fold IPv4-mapped IPv6

Evidence: `packages/cowshed/crates/cowshed-gateway/src/interfaces.rs:166-182`

```
fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => { ip.is_private() || ip.is_loopback() || ip.is_link_local() || ... }
        IpAddr::V6(ip) => {
            ip.is_loopback() || ip.is_unspecified() || ip.is_unique_local() || ip.is_unicast_link_local()
        }
    }
}
```

Called at `interfaces.rs:115` before connect, the rebinding fence described at `67-68`. Problem: `::ffff:10.0.0.1` is
`IpAddr::V6` and fails every V6 predicate, so `private_network_authorized == false` still connects. Also missing
100.64.0.0/10. [INFERENCE] on whether `lookup_host` on Darwin/Linux actually returns mapped V6; the predicate is still
wrong. Fix: if `let Some(v4) = ipv6.to_ipv4_mapped() { return is_private(IpAddr::V4(v4)); }` then the V6 checks. Add
CGNAT if the fence is "not a public unicast". Unit-test mapped RFC1918, loopback-mapped, and a public mapped address.
Cost/Risk: tests in `tests/gateway/` that connect via V6-mapped fixtures. Actor/proxy consume `AuthorizedTarget`; no API
change.

### F9 — MEDIUM — STRUCTURE — `unsafe { libc::geteuid() }` with no SAFETY comment

Evidence: `config.rs:460`, `config.rs:584`, `platform.rs:150`, `sim_broker.rs:762` (and the linux test at
`platform.rs:321`). Problem: doctrine: `unsafe` without a stated invariant is a finding. `geteuid` is operationally
infallible but still an FFI call used as an authorization uid. A later edit that caches the value across `setuid` would
be silent. Fix: one `fn authorized_uid() -> u32` with a SAFETY comment (`geteuid` has no preconditions; returns the real
uid of this process) and call it from all four sites. Do not scatter `unsafe` in `Default::default`. Cost/Risk:
mechanical.

### F10 — MEDIUM — TESTS — `SimRequest` schema test cannot go red on the verb spelling

Evidence: `packages/cowshed/crates/cowshed-gateway/src/sim_broker.rs:1233-1242`

```
fn data_plane_schema_rejects_controller_and_unknown_verbs() {
    for request in [
        r#"{"verb":"list"}"#,
        r#"{"verb":"boot","device":"booted"}"#,
        r#"{"verb":"unknown"}"#,
        r#"{"verb":"open-url","device":"booted","url":"x://y","extra":true}"#,
    ] {
        assert!(serde_json::from_str::<SimRequest>(request).is_err());
    }
}
```

Problem: PERFORMANCE-HANDBOOK §7.10bb. The fourth payload is rejected because of `extra`, not because of the verb.
Changing kebab-case to `openurl` (or the reverse) leaves this test green. There is no positive parse of a valid body.
Fix: assert `OpenUrl` from `{"verb":"openurl","device":"booted","url":"cowshed-demo://x"}` (after F2) and `is_err` on
`open-url`. Keep the extra-field case separate. Cost/Risk: none.

### F11 — MEDIUM — COPIES — `CanonicalHost::as_str` allocates; credential prefixes re-decoded per lookup

Evidence: `packages/cowshed/crates/cowshed-gateway/src/policy.rs:77-81`

```
pub fn as_str(&self) -> String {
    match self {
        Self::Dns(name) => name.clone(),
        Self::Ip(ip) => ip.to_string(),
    }
}
```

Used for DNS at `interfaces.rs:109` (`lookup_host((target.host.as_str(), target.port))`). Regime: per-upstream-connect,
not a hot inner loop. `interfaces.rs:265-273`: `CredentialRecord::validate_for` calls `normalize_path` on the request
and on every stored prefix. `decode_record` (`platform.rs:52-75`) never certifies prefixes. Regime: per credentialed
request (L7 re-validation of immutable stored bytes). Problem: `as_str` is not a `str`. DNS pays a clone of an
already-canonical name. Credential lookup re-parses prefixes that should have been proven at decode. Fix:
`fn dns_name(&self) -> Option<&str>` plus `Display` (already implemented) for `lookup_host`. Normalize prefixes once in
`decode_record`; `validate_for` only `starts_with` on the certified strings. Cost/Risk: `lookup_host` currently takes
`(String, u16)` via the clone; switch to `(host.dns_name()?, port)` / `ip.to_string()` only on the IP arm.

### F12 — LOW — DUPLICATION — ALPN protocol lists copied

Evidence: `tls.rs:65` `config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];` and `interfaces.rs:89`
identical. Match arms at `interfaces.rs:153-157` restates the same two byte strings. Problem: leaf mint and upstream
client can drift (h3, order). Two `to_vec` allocations per `SystemConnector::new` / leaf mint. Regime: once per
connector / per new host — note, not a hot-loop finding except as SSOT. Fix: one `const ALPN: &[&[u8]]` in `tls.rs` (or
`interfaces.rs`), used to build both configs and to match negotiated protocol. Cost/Risk: proxy.rs has a third copy of
the match (other slice) that must move with it.

### F13 — LOW — STRUCTURE — `WorkspaceCa::new` is substring theatre, not a parse

Evidence: `packages/cowshed/crates/cowshed-gateway/src/config.rs:151-156`

```
if !certificate_pem.contains("BEGIN CERTIFICATE")
    || !private_key_pem.contains("BEGIN PRIVATE KEY")
{
    return Err(ConfigError::MalformedCa);
}
```

Real parse is `CaSigner::parse` (`tls.rs:25-35`) via rcgen + rustls-pemfile. Problem: `BEGIN CERTIFICATE` anywhere in
the string admits the session; PEM is parsed again later. Fake validation (cast-shaped). Core tests feed
`-----BEGIN CERTIFICATE-----\npublic\n` which passes this gate and would fail mint. Fix: `WorkspaceCa::new` should call
the same parse `CaSigner::parse` uses (or become `CaSigner::parse` and store the issuer). Delete the `contains` check.
Cost/Risk: session install fails earlier on junk PEM; good. Core fixture PEMs in `gateway_sessions.rs` tests must become
real rcgen material.

### F14 — LOW — COPIES — same CA PEM parsed twice at signer load

Evidence: `tls.rs:26-35`: `KeyPair::from_pem` + `Issuer::from_ca_cert_pem` then a second walk `rustls_pemfile::certs`
for the chain DER. `mint` then `certificate.der().to_vec()` and `certificate_der.clone()` (`tls.rs:56-58`). Problem:
evaporating work (Byproduct L0) on session install / first leaf. `rustls-pemfile` exists only for this second parse.
Regime: once per session CA load — note. Still a second parser in the TLS stack. Fix: take DER from the rcgen
certificate produced while building the issuer (or from `Issuer`) and drop `rustls-pemfile` if nothing else needs it.
[INFERENCE] that `Issuer` exposes the CA DER in rcgen 0.14 without re-parse — confirm against that API before deleting
the crate. Cost/Risk: if the issuer API does not expose DER, keep `rustls-pemfile` and this stays a non-finding.

## Cross-slice questions

- `cowshed-core/src/gateway_sessions.rs` `policy_from_grants` never copies `GrantSet.sim` into `SimProjectConfig`. Who
  is supposed to call `GatewayControlClient::configure_simulator`? I found no caller outside `control.rs`.
- `control.rs:66` re-parses `"127.0.0.1:7644"` while `config.rs:240,252` hardcodes `7_644`. Control slice should use
  `ControlTcpConfig`'s address as SSOT.
- `mirror.rs:1082` `fn hex` is a third percent-nibble decoder; belongs with F6.
- `cowshed-core/src/sandbox.rs:11-14` `EgressGrant { host: String, ports: Vec<u16> }` is a different type with the same
  name as `policy.rs:184`. Comment says Seatbelt does not enforce egress — confirm it stays a sandbox DTO and is not a
  second policy.
- `repo_mirror.rs:1304` is the only gateway copy that matches core's repo-id rule; after F1 it should call the shared
  function and die.

## Non-findings (checked, clean)

- **sim_broker is not `cfg(test)` material.** It is the production `simctl` broker (docs/ios.md, grant axis
  `openurl`/`install`). F4 is "gate to macOS", not "move to tests".
- **rcgen feature split is intentional.** Core (`Cargo.toml`: `crypto,pem,ring,zeroize`) only `self_signed`s a CA.
  Gateway adds `x509-parser` because `Issuer::from_ca_cert_pem` needs it. Core's separate
  `x509-parser = { version = "0.18", features = ["verify"] }` is for `workspace_credentials` validation, not rcgen. Do
  not "align" the feature sets.
- **TLS stack is load-bearing in-process.** `rustls` + `tokio-rustls` (leaf + upstream), `rustls-platform-verifier` (OS
  trust store; cannot shell `security`/`openssl`), `rcgen` (per-host leaf mint; needs typed errors, no shell),
  `security-framework` + `security-framework-sys` (Keychain generic-password; `security` CLI would put secrets in argv).
  `tls12` stays: package registries still negotiate 1.2. `idna`, `subtle`, `zeroize`, `sha2`, `time`, `base64` earn
  their weight. `libc` is `geteuid`/`O_NOFOLLOW`, not replaceable by a CLI.
- **EgressMode serde.** Gateway `rename_all = "kebab-case"` vs core `lowercase` produce the same
  `"intercept"`/`"opaque"` strings. Not a live split.
- **No god files in this slice.** Largest is `sim_broker.rs` at 1244 with a natural seam (handle / actor loop / fs
  verify / tests). Functions stay under ~100 lines aside from `execute_request` (~160) which is a verb match, not a
  blob.
- **Operational failures are `Result`.** `unreachable!` at `interfaces.rs:134` is the TLS arm already returned.
  `expect("1024 is non-zero")` / `expect("approval index exists")` are invariants. `XcrunSimRunner` maps runner IO to
  `RunnerFailed` (lossy but not a panic).
- **sim_broker tests** assert typed `SimBrokerError` variants and audit status enums. The one substitution-test miss is
  F10.
- **platform credential JSON** uses `deny_unknown_fields` + version gate. Debug redacts `header_value`. Linux path uses
  `O_NOFOLLOW` + dev/ino check. Keep; do not replace with `security`/`cat`.
- **Profile trap (§4.1):** no benches in this slice; no opt-z claims.
