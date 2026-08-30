# cowshed-gateway/proxy.rs

Scope: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs` (3311 lines). Rubric: `BYPRODUCT-ENGINEERING.md`,
`docs/handbook/04-mechanisms.md`, `docs/handbook/05-memory-toolkit.md`, `docs/handbook/02-measurement.md` §4.1.
Neighbour peeks (not audited): `mirror.rs` strip tables, `actor.rs` admit-cancel, `tls.rs` LeafCache, `policy.rs`
`CanonicalHost::as_str`/`authority`, `interfaces.rs` `CredentialQuery`, `Cargo.toml` deps used by this file.

## Summary

- HIGH SSOT: `strip_client_secrets` / `strip_hop_headers` / `strip_response_secrets` are restated in `mirror.rs` and the
  copies have already diverged (live bug).
- HIGH DUPLICATION: `prepare_upstream_request` and `ProxyMirrorUpstream::fetch` are two outbound HTTP builders (host,
  hop-strip, trace, credential inject, H1/H2 URI).
- MEDIUM COPIES: every request allocates a URI `String` solely to take `.len()`, then clones the path into
  `RequestIntent` and drops the original.
- MEDIUM COPIES: mirror hop clones the full `HeaderMap`; generic hop re-canonicalizes `Host` via `format!` + re-parse on
  every match.
- MEDIUM COPIES: generic hop double-boxes the request body (`Limited.boxed` then `TimedRequestBody.boxed`) and calls
  `authority()` twice for one H2 request.
- MEDIUM DUPLICATION: W3C `traceparent` grammar is copied in `parse_traceparent` and `extract_mirror_trace` with
  different length/version rules.
- MEDIUM STRUCTURE: `handle_request` is 348 lines of repeated `complete_now`+`problem` arms; intercept H1/H2 copies the
  same `service_fn`.
- MEDIUM SSOT: `/npm|/cargo|/go` prefixes are restated against `MirrorProtocol::local_prefix`.
- LOW TESTS: body-timeout tests assert `BoxError` Display strings; H2 validation tests assert only `is_err()`.
- No CRITICAL. Generic HTTP bodies stream (not buffered). Leaf certs are cached in `tls.rs`, not re-minted in this file.

## Findings

### F1 — HIGH — SSOT — Header-policy tables restated in mirror.rs and already disagree (live bug)

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:2444-2488`

```
fn strip_client_secrets(headers: &mut HeaderMap) {
    const SENSITIVE: &[&str] = &[
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "npm-auth-token",
        "x-npm-token",
        "npm-otp",
        "x-goog-api-key",
        "traceparent",
        "tracestate",
    ];
    ...
fn strip_response_secrets(headers: &mut HeaderMap) {
    headers.remove(header::SET_COOKIE);
    headers.remove(header::PROXY_AUTHENTICATE);
}
fn strip_hop_headers(headers: &mut HeaderMap) {
    let named_by_connection: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
```

Other copy (not this slice): `mirror.rs:1091-1124` `strip_request_secrets` / `strip_response_secrets` /
`strip_hop_headers`. Problem: One policy (which names never leave the gateway) exists as two tables. They have diverged:
proxy strips `npm-auth-token`, `x-npm-token`, `traceparent`, `tracestate` and walks `Connection:`; mirror strips
`npm-auth-type` and `WWW-Authenticate`, does not walk `Connection:`, and does not strip `npm-auth-token`/`x-npm-token`.
Generic HTTP uses only the proxy tables, so `npm-auth-type` is forwarded; mirror cache/redirect uses the mirror tables,
so a token that survived into `MirrorRequest` is not stripped there. Same name, different behavior. Fix: One
module-level table (names + role: secret / hop / response-secret) in this file, consumed by both proxy hops and
`mirror.rs`. Union the current lists, keep the `Connection:` walk, delete the mirror copies. Decision: proxy.rs is the
SSOT because it is the only hop that talks to arbitrary upstreams. Cost/Risk: `mirror.rs` cache keys and stored
responses change if newly-stripped names were part of a cache key. Audit one cache-key test after the union.

### F2 — HIGH — DUPLICATION — Outbound upstream request is built twice

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:1880-1956` and `1010-1139`

```
    strip_client_secrets(&mut parts.headers);
    strip_hop_headers(&mut parts.headers);
    parts.headers.insert(
        header::HOST,
        HeaderValue::from_str(&admission.target.authority())
            .map_err(|_| StatusCode::BAD_REQUEST)?,
    );
    if !admission.impersonate {
        ...
        let trace = serialize_traceparent(trace_id, upstream_span_id, admission.trace_flags);
        parts.headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_str(&trace).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        );
```

`ProxyMirrorUpstream::fetch` repeats: `TargetScheme`→`UpstreamPurpose`, `AuthorizedTarget`, connect, HOST insert,
traceparent/tracestate, `CredentialQuery` + `validate_for` + header inject, H1 path-only URI vs H2 scheme+authority URI,
`handshake_upstream`, strip response. Problem: Two implementations of one hop. F1 already shows this class of copy
drifts. Credential/trace/HOST bugs will be fixed on one path and missed on the other. Fix: One
`fn write_upstream_headers(...)` plus one `fn set_upstream_uri(transport, target, path)` used by both
`prepare_upstream_request` and `fetch`. Keep body wrapping (`Limited`/`TimedRequestBody`) only on the generic path.
Cost/Risk: Error types differ (`StatusCode` vs `CacheBodyError`). Map at the two call sites; do not unify those error
types.

### F3 — MEDIUM — COPIES — `Uri::to_string` allocated only to read its length

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:2255-2259`

```
    if request.uri().to_string().len() > MAX_TARGET {
        return Err(RequestError::new(
            StatusCode::URI_TOO_LONG,
            "request target exceeds 8 KiB",
        ));
    }
```

Problem: Regime: once per HTTP request on the gateway hot path (every CONNECT, absolute-form, intercept, and mirror
request). `to_string()` builds a new `String` of the request-target and immediately throws it away. Byproduct L0 /
handbook §7.7: the URI is already parsed; length is the sum of the components already in hand. Fix: Sum `scheme`,
`authority`, `path_and_query` byte lengths (plus the `://`/`?` separators the Display form includes) against
`MAX_TARGET`. Do not allocate. Cost/Risk: Must match Hyper's `Uri` Display exactly (IPv6 brackets, omitted default
ports) or the 8 KiB gate shifts. Pin with a table of URIs at the 8192 boundary.

### F4 — MEDIUM — COPIES — Path cloned into `RequestIntent` then dropped

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:213-220`

```
    let (path, trace_id) = extract_mirror_trace(&path, audit_kind).unwrap_or((path, None));
    let intent = RequestIntent {
        target,
        method: request.method().clone(),
        path: path.clone(),
        audit_kind,
        trace_id,
    };
```

Problem: Regime: once per HTTP request. `path` is not read after this struct literal. The clone is pure traffic. Fix:
Move `path` into `intent`. Same for `service_fn` in `spawn_connection` / `spawn_intercept`: take `&AcceptContext` (or
`Arc<AcceptContext>`) instead of cloning the struct by value per request (`AcceptContext` is `Clone` and includes a
`String` workspace_id; see `71-86`, `131-138`, `1755-1763`, `1780-1788`). Cost/Risk: `handle_request` currently takes
`AcceptContext` by value; change the signature and the three `service_fn` closures. `workspace_id` clones at
`Command::Admit` (`2549`) stay until `Command` borrows.

### F5 — MEDIUM — COPIES — Mirror hop clones the whole `HeaderMap`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:803-805`

```
    let mut headers = request.headers().clone();
    strip_client_secrets(&mut headers);
    strip_hop_headers(&mut headers);
```

Problem: Regime: once per mirror GET/HEAD. Generic hop uses `into_parts` (`1880`) and takes headers by move. Mirror
clones every header name/value, then drops `request` (body unused for GET/HEAD). `HeaderMap` clone is a real byte copy,
not a `Bytes` bump. Fix: `let (parts, _body) = request.into_parts();` and move `parts.headers`, matching
`prepare_upstream_request`. Cost/Risk: None if mirror remains GET/HEAD-only (`MirrorError::MethodNotAllowed`).

### F6 — MEDIUM — COPIES — Host matching allocates and re-parses a `CanonicalTarget` on every check

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:2407-2432`

```
fn authority_value_matches(value: &str, target: &CanonicalTarget) -> bool {
    let with_default_port = if value
        .parse::<http::uri::Authority>()
        .ok()
        .and_then(|authority| authority.port_u16())
        .is_none()
    {
        match &target.host {
            CanonicalHost::Ip(std::net::IpAddr::V6(ip)) => format!("[{ip}]:{}", target.port),
            _ => format!("{value}:{}", target.port),
        }
    } else {
        value.to_owned()
    };
    CanonicalTarget::from_authority(&with_default_port, target.scheme)
        .is_ok_and(|candidate| candidate == *target)
}
```

Problem: Regime: once per HTTP request that has a `Host` / `:authority` (all of them that pass `validate_request`).
Always allocates a `String`, then re-runs IDNA/host parse. `request_target` does it again at `2182-2188` via
`host.as_str()` (which itself returns a `String` — `policy.rs:77-81`) then `format!` then `from_authority`.
`prepare_upstream_request` calls `admission.target.authority()` for HOST (`1885`) and again for the H2 URI (`1946`);
`origin()` (`1917`) allocates a third related string. Validate-once (§7.7): the target was already canonical at admit
time. Fix: Compare host bytes + port integer against `target` without building a new `CanonicalTarget`. Parse `Host`
port with a scan, not `Authority::parse` + `format!`. Reuse one `authority` `HeaderValue` for HOST and H2 URI.
Cross-slice: `CanonicalHost::as_str` / `CanonicalTarget::authority` should return `&str` / write into a caller buffer
(`policy.rs`). Cost/Risk: IPv6 bracket and default-port cases must stay equivalent; the existing
`h2_authority_host_and_connect_target_must_be_equivalent` test is the oracle.

### F7 — MEDIUM — COPIES — Generic request body is boxed twice

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:1953-1956`

```
    let body = Limited::new(body, MAX_REQUEST_BODY).boxed();
    let (signal, request_timeout) = oneshot::channel();
    let body =
        TimedRequestBody::new(body, signal, context.timeouts.body_idle, total_deadline).boxed();
```

Problem: Regime: once per generic (non-mirror) HTTP request. `TimedRequestBody.inner` is already `BoxBody`
(`2708-2711`). `Limited` is boxed, then the wrapper is boxed again — two heap headers and two vtables before a single
upstream byte moves. The body itself is streamed (not a finding); the type-erasure is the waste. Fix: Make
`TimedRequestBody` generic over `B: Body` (or store `Limited<Incoming>` directly) and box once at the
`Request<BoxBody<...>>` boundary hyper requires. Cost/Risk: `UpstreamSender` is typed on `BoxBody<Bytes, BoxError>`
(`741-743`); one box remains. Monomorphize `TimedRequestBody` on `Limited<Incoming>` only.

### F8 — MEDIUM — DUPLICATION — `traceparent` grammar copied; the copies disagree

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:2005-2032` and `2215-2232`

```
fn parse_traceparent(value: &str) -> Option<(String, u64, u8)> {
    let bytes = value.as_bytes();
    if bytes.len() < 55
        || bytes[2] != b'-'
        || bytes[35] != b'-'
        || bytes[52] != b'-'
        || !bytes[..55]
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 2 | 35 | 52) || byte.is_ascii_hexdigit())
```

```
    if bytes.len() != 55
        || bytes[2] != b'-'
        || bytes[35] != b'-'
        || bytes[52] != b'-'
        ...
        || &traceparent[..2] == "ff"
        || traceparent[3..35].bytes().all(|byte| byte == b'0')
        || traceparent[36..52].bytes().all(|byte| byte == b'0')
```

Problem: Same W3C layout checked twice. `parse_traceparent` allows version ≠ 0 with trailing `-…`;
`extract_mirror_trace` requires exactly 55 bytes and does not parse flags/span. A version-1 parent that headers accept
is rejected in `/npm/t/` and `/go/t/` paths (`extract` returns `None`, prefix not stripped). Live divergence, path vs
header carrier. Fix: `extract_mirror_trace` calls `parse_traceparent(traceparent)` and keeps the path rewrite. One
grammar. Cost/Risk: URL paths that today fail-soft (no strip) would start adopting traces. That is the intended
bun-install attribution; update `npm_and_go_trace_paths_are_stripped_and_adopted` with a version-1 extra-field case.

### F9 — MEDIUM — SSOT — Local mirror prefixes restated against `MirrorProtocol`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:2236-2248`

```
fn local_request_kind(path: &str) -> Option<AuditKind> {
    if path == "/sim" {
        return Some(AuditKind::Sim);
    }
    if path == "/npm" || path.starts_with("/npm/") {
        Some(AuditKind::Npm)
    } else if path == "/cargo" || path.starts_with("/cargo/") {
        Some(AuditKind::Cargo)
    } else if path == "/go" || path.starts_with("/go/") {
        Some(AuditKind::Go)
```

Other copy: `policy.rs:273-279` `MirrorProtocol::local_prefix` → `"/npm/"`, `"/cargo/"`, `"/go/"`. Problem: Protocol
path names live in two places. Adding a fourth protocol requires both. Exact `/npm` (no trailing slash) exists only
here. Fix: Drive `local_request_kind` from `MirrorProtocol` (exact prefix-without-slash or `starts_with(local_prefix)`).
Keep `/sim` as the one non-protocol local route. Cost/Risk: `AuditKind` vs `MirrorProtocol` mapping must stay total; Sim
stays a separate arm.

### F10 — MEDIUM — STRUCTURE — `handle_request` is a 348-line error ladder; intercept H1/H2 is copy-paste

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:165-513` and `1753-1806`

```
                NegotiatedTransport::Http1 => {
                    let nested_context = context.clone();
                    let fixed_target = admission.target.clone();
                    let generation = admission.generation;
                    let service = service_fn(move |request| {
                        handle_request(
                            request,
                            nested_context.clone(),
                            Authentication::Generation(generation),
                            Some(fixed_target.clone()),
                        )
                    });
```

The Http2 arm (`1779-1806`) repeats that closure, then a builder. `handle_request` itself repeats `complete_now` +
`problem` ~10 times for connect/handshake/response-header failures (`299-496`). Problem: Functions over ~100 lines.
`spawn_opaque` is 236 lines (`1349-1585`), `spawn_intercept` 161 (`1661-1822`), `fetch` 130,
`handle_mirror_request` 145. The file is 3311 lines (under the 5k god-file bar) but the seams are already named by the
functions. Fix: Extract `fn nested_intercept_service(context, generation, target)`; extract
`fn fail(context, admission, status, classification, message)` for the timeout/error arms. Split the file along accept /
request / connect / trace / body when those extracts land — not before, or you get two copies of the header tables (see
F1). Cost/Risk: `complete_now` consumes `Admission`; the helper must take it by value. No behavior change.

### F11 — LOW — COPIES — `Completion::new` clones a `Sender` it never uses

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:503` and `2634-2653`

```
    let completion = Completion::new(context.commands.clone(), admission, status, None);
    ...
    fn new(
        _commands: mpsc::Sender<Command>,
        mut admission: Admission,
        status: StatusCode,
        mirror_cache_status: Option<MirrorCacheStatus>,
    ) -> Self {
```

Problem: Regime: once per successful proxied/mirrored response. `commands.clone()` is paid and discarded. Leftover from
when completion went over the command channel (`complete_now` also takes `_context` unused at `2580-2590`). Fix: Drop
the `_commands` argument. Same for `_context` on `complete_now`. Cost/Risk: None.

### F12 — LOW — COPIES — `proxy_token` and `parse_tracestate` allocate intermediates

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:2363-2378` and `2059-2065`

```
    let values: Vec<_> = headers
        .get_all(header::PROXY_AUTHORIZATION)
        .iter()
        .collect();
    ...
            let decoded = STANDARD.decode(credentials).ok()?;
            let decoded = String::from_utf8(decoded).ok()?;
            decoded.split_once(':')?.1.to_owned()
```

```
                || members
                    .iter()
                    .any(|existing: &String| existing.starts_with(&format!("{key}=")))
            {
                return Err(());
            }
            members.push(format!("{key}={value}"));
```

Problem: Regime: once per request that carries `Proxy-Authorization` (all authenticated proxy clients) / once per
request that carries `tracestate`. `Vec` collect of at most a few header values; Basic path is
`decode`→`String`→split→`to_owned` (three buffers for one token). Tracestate duplicate check allocates
`format!("{key}=")` per existing member (O(n²) tiny n≤32). `Authentication::Bearer(Option<String>)` forces an owned
token to cross `Command::Admit` — that ownership is load-bearing, the `Vec` and the inner `format!` are not. Fix:
Iterate `get_all` without collecting; for Basic, split the decoded bytes at `:` and `String::from_utf8` only the
password side. Duplicate-key check: `existing.split_once('=')` vs `key`. Leave the owned token. Cost/Risk: Actor still
needs `String` (`actor.rs:458,1154`). Cross-slice if `Authentication` grows a borrowed variant.

### F13 — LOW — TESTS — Timeouts and H2 validation assert rendered strings / `is_err()`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:3011-3017` and `3264-3288`

```
            assert!(validate_request(&request).is_err(), "{name} was accepted");
```

```
        assert_eq!(idle_task.await.expect("idle task"), "body idle timeout");
        ...
            "request total timeout"
```

Problem: H2 forbidden-header tests go green if `validate_request` returns _any_ error (substitution: a URI-too-long
regression still passes). `ProxyBody` idle/total tests pin Display text (`"body idle timeout"`) that is not
`RequestBodyTimeout::classification()` (`"request-body-idle-timeout"`) and not the audit classification
(`"body-idle-timeout"` at `2880`). Three spellings of one event. The `TimedRequestBody` test does assert the typed
oneshot (`3207-3208`) _and_ the Display string — the string half cannot catch a typed bug the oneshot missed. Fix:
Assert `RequestError.status` (or message) on H2 cases. Give `ProxyBody` the same `RequestBodyTimeout` enum (or a
response twin) and assert the enum; delete Display equality. Use the classification `&'static str` as the `BoxError`
payload so audit, client, and test share one token. Cost/Risk: Integration tests that scrape response bodies for
`"body idle timeout"` would need the same token. None in this file.

### F14 — LOW — STRUCTURE — `accept_loop` TCP/Unix arms are twins

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:95-118`

```
        BoundListener::Tcp(listener) => loop {
            tokio::select! {
                result = listener.accept() => {
                    let Ok((stream, _)) = result else { break };
                    let _ = stream.set_nodelay(true);
                    spawn_connection(stream, context.clone(), connection_stop.clone(), audit_stop.clone());
                }
```

Unix arm is the same minus `set_nodelay`. Problem: Two accept loops. A shutdown/select change must be made twice. Fix:
One generic accept loop; apply `set_nodelay` in a `BoundListener` helper or a `after_accept` callback on the TCP arm
only. Cost/Risk: None.

### F15 — LOW — DEP-BLOAT — `base64` in this file is load-bearing; do not shell out

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:15` and `2374-2377`

```
use base64::{Engine as _, engine::general_purpose::STANDARD};
            let decoded = STANDARD.decode(credentials).ok()?;
            let decoded = String::from_utf8(decoded).ok()?;
```

Problem: None as bloat. Precedent (`git2` → `git` on PATH) does **not** apply: this is per-request, in-process, on a
header the client already sent. A CLI is not present and not parseable here. `async_trait` (`1008`) exists because
`MirrorUpstream` is a foreign trait (`mirror.rs`); `pin_project_lite` is required for the two `Body` impls; `serde_json`
is required for `SimRequest` and acceptable for `problem` (error path). `uuid` is in the crate manifest and unused by
this file — not this slice's to delete. Fix: Keep `base64`. If the crate-level unused-by-proxy deps are trimmed, that is
a crate-slice job, not this file. Cost/Risk: Hand-rolled Base64 would be a second decoder next to `STANDARD` used by
tests (`tests/gateway.rs`) and `mirror.rs`.

## Cross-slice questions

- `mirror.rs:1091-1124` — F1 other copy. CsGwMirror owns internals; this slice wants one table, proxy as SSOT.
- `policy.rs:77-81,148-156` — `CanonicalHost::as_str` and `CanonicalTarget::authority`/`origin` return `String`. F6 fix
  is incomplete until those write `&str` or a caller buffer.
- `interfaces.rs:229-237` — `CredentialQuery` owns four `String`s; `prepare_upstream_request:1913-1919` and
  `fetch:1072-1078` clone into it every hop. Should the query borrow?
- `actor.rs:457-458,1248-1258,1344-1358` — `Authentication::Bearer(Option<String>)` forces F12's owned token.
  `AdmissionCancellation` Drop-on-success is **not** a cancel bug: queued watchers are `abort()`ed before
  `reply.send(Ok)`. Confirmed, not a finding.
- `tls.rs:99-116` — `LeafCache::get_or_mint` already hoists leaf `ServerConfig`. Per-CONNECT `Command::MintLeaf`
  (`1229-1234`) is an RPC + HashMap probe, not a re-sign. Not a §7.7 miss in this file.
- `actor.rs` / `interfaces.rs` `UpstreamConnector` — `handle_request` does `connect` + `handshake_upstream` + one
  `send_request` and drops the `UpstreamSender` (`292-434`). Is one HTTP handshake per permit intentional under
  `origin_active`, or is pooling expected in the connector? This file never reuses a sender.
- `cache.rs:28` — another `MAX_HEADER_BYTES` (disk metadata region `64KiB-4`). Name collision only; different constant.
  Do not unify.

## Non-findings (checked, clean)

- Generic HTTP request/response bodies stream via `Limited` + `TimedRequestBody` + `ProxyBody` (`1953-1956`, `504-511`,
  `2838-2921`). No `collect()` on that path. `Bytes` frames are refcounted; the 64 MiB cap is an admission gate, not a
  buffer.
- `handle_sim_request` does `body.collect()` (`548-549`) then `serde_json`; cap is `MAX_SIM_REQUEST` 64 KiB, sim path,
  not the proxy hop.
- `capture_client_hello` (`1608-1658`) buffers at most 64 KiB into a 4 KiB-capacity `Vec` in order to replay the
  handshake; that copy is the mechanism, not waste. Regime: once per opaque CONNECT.
- No `unsafe`. `expect`/`unreachable` sites are static responses (`679`, `2940`, `2971`) or the `protocol.is_some()`
  invariant (`801-802`).
- No `cfg(target_os)` in this file. `repo_mirror: #[allow(dead_code)]` (`81-84`) is an intentional structural fence, not
  dead code to delete.
- Leaf TLS: actor cache hit returns `Arc<ServerConfig>`; `TlsAcceptor::from(leaf)` per intercept CONNECT is an Arc wrap,
  not a re-validate of immutable cert bytes.
- `AdmissionCancellation` success-path `send(())` is a no-op after promote abort / fast-path drop of the receiver.
- W3C unit tests assert typed fields (`3091-3097`), not only strings. `PendingBody` (`3147-3158`) does not allocate a
  body fixture around the timer kernel.
- Hop-by-hop vs H2-forbidden lists (`2295-2300` vs `2478-2486`) are related but not the same set (`TE`/`TRAILER` vs
  request-forbidden); not a silent divergence.
- `MAX_HEADER_BYTES` 64 KiB here is the HTTP header budget, distinct from cache metadata (see cross-slice).
- Doctrine §4.1 profile trap: no benches in this file; nothing to mis-measure under opt-z.
