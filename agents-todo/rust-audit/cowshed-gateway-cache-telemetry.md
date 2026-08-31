# cowshed-gateway/cache+telemetry

Scope: `packages/cowshed/crates/cowshed-gateway/src/cache.rs` (1532 lines),
`packages/cowshed/crates/cowshed-gateway/src/telemetry.rs` (1114 lines). Doctrine: BYPRODUCT-ENGINEERING.md,
PERFORMANCE-HANDBOOK §4.1 / §7.7 / §7.11 / §7.12, handbook `04-mechanisms.md` + `05-memory-toolkit.md`. Neighbouring
reads only to resolve SSOT: `lmao-core/src/lib.rs` + `entry_type.rs`, `lmao-arrow/src/lib.rs` + `convert.rs:1-64`,
`cowshed-core/src/storage/audit.rs` (ArrowAuditSink surface), `cowshed-gateway/src/{interfaces,config,mirror,actor}.rs`
(targeted), `packages/cowshed/docs/telemetry.md`, `cowshed-gateway/Cargo.toml`.

## Summary

- HIGH SSOT: `telemetry.rs` hand-rolls Arrow IPC with a lmao-shaped but type-incompatible schema; docs claim one lmao
  substrate.
- HIGH COPIES: every cache hit SHA-256s the entire object body (`open_and_validate`) — Byproduct L7 / handbook §7.7.
- HIGH STRUCTURE: one `validate_event` failure stops the audit writer permanently; actor fail-closes the gateway on that
  error.
- HIGH STRUCTURE: `evict_if_needed` I/O errors are discarded after the map/accounting have already dropped the entry.
- MEDIUM SSOT: hex nibble + 32-byte hex decode restated in `mirror.rs`, and they already disagree on `A-F`.
- MEDIUM SSOT: sensitive-header denylist restated three ways (`cache.rs` / `mirror.rs` / `proxy.rs`) and already
  disagrees.
- MEDIUM COPIES: 64 KiB header hole per object; per-frame `Bytes::copy_from_slice` on the hit body path.
- MEDIUM TESTS: telemetry tests never read `timestamp`; fabricated 3 ns spans cannot go red. `cache.rs` has no unit
  tests.
- sha2 is load-bearing. arrow-* is load-bearing for IPC (the defect is SSOT, not “shell out”). uuid in `cache.rs` is
  only temp names.

## Findings

### F1 — HIGH — SSOT — Gateway audit IPC restates lmao schema and already diverges

Evidence: `packages/lmao/crates/lmao-arrow/src/convert.rs:21-63` vs
`packages/cowshed/crates/cowshed-gateway/src/telemetry.rs:700-728` vs `packages/cowshed/docs/telemetry.md:3-6`

```21:63:packages/lmao/crates/lmao-arrow/src/convert.rs
pub const ENTRY_TYPE_NAMES: [&str; 24] = [
    "span-start",
    "span-ok",
    "span-err",
    "span-exception",
    // ...
];
pub fn trace_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("trace_id", dict_type(DataType::UInt32), false),
        Field::new("thread_id", DataType::UInt64, false),
        Field::new("span_id", DataType::UInt32, false),
        // ...
        Field::new("entry_type", dict_type(DataType::UInt8), false),
        Field::new("message", dict_type(DataType::UInt32), true),
```

```700:728:packages/cowshed/crates/cowshed-gateway/src/telemetry.rs
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("thread_id", DataType::UInt64, false),
        Field::new("span_id", DataType::UInt64, false),
        // ...
        Field::new("entry_type", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
```

```3:6:packages/cowshed/docs/telemetry.md
cowshed's observability is **distributed tracing into Arrow columns**, not a pile of text logs. Every lifecycle
operation, every job, and every gateway request is a span; spans carry a W3C trace id across cowshed's boundaries; and
they flush as Arrow segments you query with `cowshed logs` / `cowshed audit` / `cowshed trace`. There is one storage
substrate ([lmao](https://github.com/smoothbricks)), no NDJSON files on disk, and no telemetry daemon.
```

Problem: This is not a second `lmao-core` tracer (`SpanBuffer` / `Clock` / `TraceId` / `EntryType`). It is a second
Arrow IPC writer that copies the identity-column _names_ and the `span-start`/`span-ok`/`span-err`/`span-exception`
vocabulary (`end_entry_type` at `telemetry.rs:767-774`, `ENTRY_TYPE_NAMES` / `lmao-core` `EntryType` discriminants
1..=4) and then disagrees on the types: `Timestamp(ns)` vs `Int64`, `Utf8` vs dictionary, `span_id: UInt64` vs `UInt32`,
`thread_id` set to `event.sequence` (`telemetry.rs:625`). `cowshed-gateway` does not depend on `lmao-core` or
`lmao-arrow`. Live divergence: a strict lmao reader cannot consume gateway segments.

Fix: `lmao-arrow::trace_schema` is the single source for the identity prefix. Gateway extra columns (`sequence`,
`workspace_id`, `decision`, …) are an extension table or a documented extra-field set on that schema, dictionary-encoded
through `lmao-arrow`, not a parallel `Schema::new`. Do not wrap HTTP in `lmao-core` `SpanBuffer` unless the in-process
span API is actually wanted; the SSOT that is violated today is the IPC schema, not the live tracer.

Cost/Risk: CLI `cowshed audit` / `lmao-inspect` (other slices) must move with the schema. Existing `gateway-*.arrow`
segments become unreadable; that is a greenfield cutover, not a compat shim.

### F2 — HIGH — COPIES — Every cache hit re-hashes the sealed object (L7)

Evidence: `cache.rs:253-254`, `cache.rs:1389-1432`

```253:262:packages/cowshed/crates/cowshed-gateway/src/cache.rs
    pub async fn open_candidate(&self, candidate: CacheCandidate) -> Result<CacheHit, CacheError> {
        match open_and_validate(&candidate.path, &candidate.response).await {
            Ok(file) => {
                let content_length = candidate.response.content_length;
                Ok(CacheHit {
                    response: candidate.response,
                    body: CacheReadBody {
                        file,
                        remaining: content_length,
                        buffer: vec![0; STREAM_CHUNK_BYTES],
```

```1398:1430:packages/cowshed/crates/cowshed-gateway/src/cache.rs
    file.seek(SeekFrom::Start(HEADER_REGION)).await?;
    let mut remaining = response.content_length;
    let mut digest = Sha256::new();
    let mut expected_sha512 = response
        .expected
        .and_then(|expected| matches!(expected.digest, ObjectDigest::Sha512(_)).then(Sha512::new));
    let mut buffer = vec![0; STREAM_CHUNK_BYTES];
    while remaining > 0 {
        // ...
        digest.update(&buffer[..read]);
        // ...
    }
    let actual: [u8; 32] = digest.finalize().into();
    // ...
    if actual != response.content_sha256 || !expected_matches {
        return Err(CacheError::DigestMismatch);
    }
```

Problem: Fill already computed `content_sha256` (`cache.rs:649`) and optionally SHA-512, then atomically renamed under
`O_NOFOLLOW`. Hit path re-streams and re-hashes the whole body, then seeks back to serve. Byproduct anti-pattern:
“Per-get verification of immutable bytes”. Regime: **hot, per cache hit, O(bytes)** — a 40 MiB crate tarball pays a full
SHA-256 (and maybe SHA-512) before the first body frame. `validate_previous` (`cache.rs:293`) does it again on
revalidation. Size-proportional hit cost is the §4.2 miss-storm signature.

Fix: Trust fill+rename+`O_NOFOLLOW`. `open_and_validate` should check
`metadata.len() == HEADER_REGION + content_length`, seek to `HEADER_REGION`, return the fd. Keep a corrupt path: if a
later read truncates (`cache.rs:510-512` already errors), then `Command::Corrupt`. Optional: a doctor/startup scan,
never the get path.

Cost/Risk: A bit-flip after seal would be served until a truncated read or an explicit scan. That is the L7 trade: the
cache _is_ the verified set. `sha2` stays for fill.

### F3 — HIGH — STRUCTURE — `validate_event` failure stops the writer

Evidence: `telemetry.rs:265-277`, `telemetry.rs:228-236`

```265:277:packages/cowshed/crates/cowshed-gateway/src/telemetry.rs
    async fn handle(&mut self, command: WriterCommand) -> bool {
        match command {
            WriterCommand::Record { event, reply } => {
                if let Err(error) = validate_event(&event, self.next_expected_sequence()) {
                    let _ = reply.send(Err(error));
                    return false;
                }
                let decision_boundary = is_decision_boundary(event.status);
                self.pending.push(PendingRecord { event, reply });
                // ...
                !(must_flush && self.flush_pending().await.is_err())
```

```228:236:packages/cowshed/crates/cowshed-gateway/src/telemetry.rs
    async fn run(mut self) {
        loop {
            if self.pending.is_empty() {
                let Some(command) = self.receiver.recv().await else {
                    break;
                };
                if !self.handle(command).await {
                    break;
                }
```

Problem: `validate_event` (`telemetry.rs:436-487`) rejects operational caller data (empty strings, tracestate with `\n`,
hex shape, `parent_span_id == Some(0)`). `handle` returns `false` for that the same way it does for a failed flush.
`run` then exits. Subsequent `record` calls get `writer_stopped()`. Actor (`actor.rs:1576-1579`) fail-closes the whole
gateway on that `Err`. A bad tracestate is not disk integrity; it takes down egress audit and, via the actor, the
gateway.

Fix: On `validate_event` error, `reply.send(Err(error))` and `return true`. Only `flush_pending` I/O failure should stop
the writer. Decision I'd take: reject the event, keep recording later denials.

Cost/Risk: Actor currently treats any `record` error as fatal (`fail_closed`). If this change lands, actor must
distinguish “bad event” from “sink dead” or it will still drain.

### F4 — HIGH — STRUCTURE — Eviction I/O errors are swallowed after accounting moves

Evidence: `cache.rs:860-878`, `cache.rs:1126-1134`

```860:878:packages/cowshed/crates/cowshed-gateway/src/cache.rs
    async fn run(mut self) {
        let _ = self.evict_if_needed().await;
        while let Some(command) = self.receiver.recv().await {
            match command {
                // ...
                Command::Release { digest, generation } => {
                    // ...
                    let _ = self.evict_if_needed().await;
                }
```

```1126:1134:packages/cowshed/crates/cowshed-gateway/src/cache.rs
        for (digest, _) in candidates {
            if self.total_bytes <= self.config.low_water_bytes {
                break;
            }
            if let Some(entry) = self.entries.remove(&digest) {
                self.total_bytes = self.total_bytes.saturating_sub(entry.stored_bytes);
                remove_generated_file(&entry.path).await?;
            }
        }
```

Problem: The entry is removed and `total_bytes` decremented _before_ `remove_generated_file`. On I/O error the `?`
returns, callers do `let _ =`, and the actor keeps running. Disk still holds the file; RAM thinks the bytes are free, so
high-water will not retrigger for that object. Next `load_entries` resurrects it. Silence is the failure. Regime:
eviction is rare (20 GiB high water) but the swallow is on every Release and on boot.

Fix: Do not decrement / remove from the map until `remove_generated_file` returns `Ok`. Surface the error on `Commit`
(already returned) and on `Release`/boot: log-and-retry or fail the actor. No `let _ =` on a mutating I/O.

Cost/Risk: Local to `CacheActor`. Commit path already returns the error; Release currently cannot.

### F5 — MEDIUM — SSOT — Hex decode/nibble copied, uppercase already diverged

Evidence: `cache.rs:1497-1524` vs `mirror.rs:931-1086`

```1519:1524:packages/cowshed/crates/cowshed-gateway/src/cache.rs
fn hex_nibble(value: u8) -> Result<u8, CacheError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(CacheError::InvalidMetadata),
    }
}
```

```1082:1086:packages/cowshed/crates/cowshed-gateway/src/mirror.rs
fn hex(value: u8) -> Result<u8, MirrorError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
```

`hex_decode_32` (`cache.rs:1497-1505`) is the same loop as `decode_hex_32` (`mirror.rs:931-939`). `hex_decode_64`
(`cache.rs:1508-1516`) is the 64-byte clone of `hex_decode_32`.

Problem: Two (three) codecs for one hex alphabet. Live divergence: cache rejects `A-F`, mirror accepts it. Cache disk
writes lowercase (`hex_encode` at `cache.rs:1487-1494`) so on-disk roundtrip is consistent; SRI/query hex that is
uppercase works in mirror and would fail if it ever hit `hex_decode_32`.

Fix: One `fn hex_nibble` / `fn hex_decode<const N: usize>` in this crate. Cache `hex_encode` is the writer SSOT. Mirror
deletes `decode_hex_32` + `hex`. Accept both cases at the parse boundary.

Cost/Risk: `mirror.rs` (CsGwMirror) must move. Error types differ (`CacheError` vs `MirrorError`) — keep a tiny mapper,
not a second decoder.

### F6 — MEDIUM — SSOT — Sensitive-header denylist restated and disagrees

Evidence: `cache.rs:1288-1294` vs `mirror.rs:1091-1108`

```1288:1294:packages/cowshed/crates/cowshed-gateway/src/cache.rs
fn is_sensitive_header(name: &HeaderName) -> bool {
    name == http::header::AUTHORIZATION
        || name == http::header::PROXY_AUTHORIZATION
        || name == http::header::COOKIE
        || name == http::header::SET_COOKIE
        || name.as_str().eq_ignore_ascii_case("npm-auth-type")
        || name.as_str().eq_ignore_ascii_case("npm-otp")
}
```

`mirror.rs:1091-1102` `strip_request_secrets` adds `x-goog-api-key`. `mirror.rs:1105-1108` `strip_response_secrets` is
SET_COOKIE / PROXY_AUTHENTICATE / WWW_AUTHENTICATE only. `proxy.rs` (grep) also names `x-npm-token`, `traceparent`,
`tracestate`.

Problem: Three denylists for one secret-header concept. Cache persist path will store any response header not in _its_
list, including `x-goog-api-key` if a fill ever sees it. [INFERENCE] whether the fill path always runs
`strip_response_secrets` first — that is mirror-owned.

Fix: One table of sensitive names. Cache `is_sensitive_header` is the persist SSOT (fail closed: unknown secret must not
hit disk). Mirror/proxy strip functions iterate that table.

Cost/Risk: `mirror.rs` / `proxy.rs` (other slices). Adding names is a behavior change for cached objects already on disk
(old objects may still contain the extra headers until eviction).

### F7 — MEDIUM — COPIES — Hit body copies every 64 KiB frame; 64 KiB header hole per object

Evidence: `cache.rs:27-29`, `cache.rs:319`, `cache.rs:515-518`

```27:29:packages/cowshed/crates/cowshed-gateway/src/cache.rs
const HEADER_REGION: u64 = 64 * 1024;
const MAX_HEADER_BYTES: usize = HEADER_REGION as usize - 4;
const STREAM_CHUNK_BYTES: usize = 64 * 1024;
```

```515:518:packages/cowshed/crates/cowshed-gateway/src/cache.rs
                let length = read_buf.filled().len();
                this.remaining -= length as u64;
                let bytes = Bytes::copy_from_slice(read_buf.filled());
                Poll::Ready(Some(Ok(Frame::data(bytes))))
```

Problem: (1) Hit path: regime **hot, per served byte**. `CacheReadBody` already owns a 64 KiB `Vec` (`cache.rs:262`);
each frame then heap-allocates a fresh `Bytes`. (2) Every object occupies ≥ 64 KiB on disk because the body starts at
`HEADER_REGION` (`cache.rs:319`) even when JSON metadata is hundreds of bytes. Closed-form waste: max object count under
20 GiB is `20GiB/64KiB ≈ 327k`, not “as many packuments as fit”. Fine for multi-meg crate tarballs; dominant for small
npm metadata.

Fix: Serve with `BytesMut` recycled into `Bytes` without a second copy, or `mmap` the body region. Shrink
`HEADER_REGION` to a tight max (4 KiB is enough for the JSON that `MAX_HEADER_BYTES` actually holds) or store metadata
as a sidecar so body files have no hole. Size is a formula; 64 KiB is not derived from `encoded.len()`.

Cost/Risk: On-disk layout is `CACHE_VERSION` (`cache.rs:26`). Changing the hole is a version bump and a wipe of existing
`obj-*` files (greenfield).

### F8 — MEDIUM — STRUCTURE — `event_batch` fabricates a 3 ns span tree from a single timestamp

Evidence: `telemetry.rs:618-697`

```618:697:packages/cowshed/crates/cowshed-gateway/src/telemetry.rs
        let completed_ns = event.timestamp_unix_ms.saturating_mul(1_000_000);
        let request_start_ns = completed_ns.saturating_sub(3);
        // ...
        push_row(request_start_ns, request_span, event.parent_span_id, "span-start", "gateway.request");
        if let Some(upstream_span) = upstream_span {
            push_row(request_start_ns.saturating_add(1), upstream_span, Some(request_span), "span-start", "gateway.upstream");
            push_row(request_start_ns.saturating_add(2), upstream_span, Some(request_span), request_end, "gateway.upstream");
        }
        push_row(request_start_ns.saturating_add(3), request_span, event.parent_span_id, request_end, "gateway.request");
```

Problem: `AuditEvent` (`interfaces.rs:302-324`) has one `timestamp_unix_ms` and no start time. The writer invents
`gateway.request` / `gateway.upstream` lifecycles with 1 ns steps so the IPC looks like lmao rows. `thread_id` is
`sequence.max(1)`. Waterfalls from these segments are fiction. `event_batch` is 167 lines of column-wise clone
(`telemetry.rs:643-665` clones every string field per synthetic row, 2 or 4 rows per event).

Fix: Either (a) emit one row per `AuditEvent` (the audit record _is_ the event; no fake spans), or (b) put real
start/end instants on `AuditEvent` at the actor and emit two rows with those times. I would take (a): audit is a
decision log, not a tracer. Delete `push_row`, `end_entry_type`, and the 4× capacity guess.

Cost/Risk: Tests (`telemetry.rs:908-911`) pin `num_rows() == 12` for 3 events. Control-plane consumers that assume
span-start/span-ok pairs must move. This is the same cutover as F1.

### F9 — MEDIUM — TESTS — Guards that cannot see the fake timestamps; `cache.rs` has no unit tests

Evidence: `telemetry.rs:908-953`, `telemetry.rs:1101-1104`

```908:953:packages/cowshed/crates/cowshed-gateway/src/telemetry.rs
        assert_eq!(
            batch.num_rows(),
            12,
            "request and upstream span lifecycle rows"
        );
        // ... asserts field names [0..8] as strings ...
        assert_eq!(entries.value(0), "span-start");
        assert_eq!(entries.value(1), "span-start");
        assert_eq!(entries.value(2), "span-ok");
        assert_eq!(entries.value(3), "span-ok");
```

```1101:1104:packages/cowshed/crates/cowshed-gateway/src/telemetry.rs
        assert!(
            error.0.contains("creating telemetry partition")
                || error.0.contains("syncing telemetry directory")
        );
```

Problem: Tests never read the `timestamp` column, so the 3 ns fabrication (F8) cannot go red (handbook §7.10bb). Schema
identity is asserted as rendered field-name strings, not against `lmao-arrow::trace_schema`. Storage failure is matched
on `AuditError` display text. `cache.rs` has no `#[cfg(test)]`; digest-on-hit, header denylist, uppercase hex, and
swallowed eviction have no unit oracle. LRU pin / crash-temp / symlink-root live in `tests/mirror_cache.rs` (mirror
slice) and do not pin F2/F4/F5/F6.

Fix: Assert `timestamp` values (or, with F8, row count == event count). Compare identity fields to `trace_schema()` if
F1 lands. Match `AuditError` by a typed variant, not `contains`. Add cache unit tests for: hit does not rehash (after
F2), `is_sensitive_header`, hex roundtrip including `A-F`, eviction error leaves `total_bytes` unchanged.

Cost/Risk: Test-only in this crate. Typed `AuditError` is a public-surface change (`interfaces.rs`).

### F10 — LOW — STRUCTURE — `unsafe { libc::geteuid() }` has no SAFETY comment

Evidence: `cache.rs:1297-1301`

```1297:1301:packages/cowshed/crates/cowshed-gateway/src/cache.rs
#[cfg(unix)]
fn root_is_owned_and_private(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    metadata.uid() == unsafe { libc::geteuid() } && metadata.permissions().mode() & 0o077 == 0
}
```

Problem: Repo rule: `unsafe` without a stated invariant comment. `geteuid` is a POSIX libc call with no memory unsafety;
the comment still has to say that.

Fix: One-line SAFETY: “`geteuid` is always defined on unix; return value is compared as `uid_t`.” Same bar as
`lmao-core` `EntryType::from_u8`.

Cost/Risk: Comment only.

### F11 — LOW — DEP-BLOAT — `uuid` in `cache.rs` is a temp-name RNG

Evidence: `cache.rs:315-316`

```315:316:packages/cowshed/crates/cowshed-gateway/src/cache.rs
        let temp_name = format!("{TEMP_PREFIX}{}", Uuid::new_v4().simple());
        let temp_path = self.config.root.join(temp_name);
```

Problem: Fill is exclusive per digest (`fills: HashMap<[u8; 32], FillStateEntry>`). Temp name can be
`{TEMP_PREFIX}{hex(digest)}` or digest+generation. `uuid` v4 is not load-bearing here. Do **not** drop the crate from
`Cargo.toml` in this slice: `telemetry.rs:137` uses it for `writer_id` in sealed names, and other gateway files use it
for temps. Precedent (`git2`) does not apply to `sha2` (in-process integrity, error typing, no `shasum` on the fill poll
path).

Fix: Name temps from `permit.digest` / `permit.generation`. Leave `uuid` for writer ids until core’s `fsio::temp_name`
is the crate SSOT (cross-slice).

Cost/Risk: Temp cleanup (`TEMP_PREFIX` scan at `cache.rs:1331`) still works.

### F12 — LOW — DUPLICATION — `CacheConfig::production` TTLs restated in `config.rs`

Evidence: `cache.rs:46-53` vs `config.rs:386-393`

```46:53:packages/cowshed/crates/cowshed-gateway/src/cache.rs
    pub fn production(root: PathBuf) -> Self {
        Self {
            root,
            high_water_bytes: DEFAULT_HIGH_WATER_BYTES,
            low_water_bytes: DEFAULT_LOW_WATER_BYTES,
            metadata_ttl: Duration::from_secs(5 * 60),
            fill_wait_timeout: Duration::from_secs(15 * 60),
        }
    }
```

`MirrorCacheConfig::new` copies the two `Duration::from_secs` literals instead of calling `CacheConfig::production`.
Water marks correctly import the `DEFAULT_*` constants.

Fix: `MirrorCacheConfig::new` builds via `CacheConfig::production(cache_root)` (or shared `DEFAULT_METADATA_TTL` /
`DEFAULT_FILL_WAIT` next to the byte constants). `production` is the SSOT.

Cost/Risk: `config.rs` (other gateway slice). Numbers currently agree; this is latent.

## Cross-slice questions

- `packages/cowshed/crates/cowshed-core/src/storage/audit.rs` also names `ArrowAuditSink` and hand-rolls the same
  durability protocol (date partition, mode 0600, `StreamWriter`, fsync, rename, writer `Uuid`). Gateway `write_segment`
  uses `fs::rename` after `exists()` (`telemetry.rs:557-582`); core uses `rename_noreplace`. Who owns the sealed-segment
  protocol? I did not audit core internals.
- Does `cowshed audit` / `lmao-inspect` actually read `…/telemetry/gateway/YYYY-MM-DD/gateway-*.arrow`? If yes, F1 is a
  live reader break (promote to CRITICAL). CLI slice owns that.
- `actor.rs:1570-1590` fail-closes the gateway on any `audit.record` error. F3’s fix is incomplete unless actor
  distinguishes bad event vs dead sink.
- `mirror.rs` owns `decode_hex_32` / `hex` / `strip_*_secrets` (F5, F6). `proxy.rs` has a third header list.
- `tls.rs:71` has a different `struct CacheKey` (workspace/host/fingerprint). Name collision only; not this cache.
- `lmao-arrow` / `lmao-core` (LmaoArrow / LmaoCore): F1 depends on `trace_schema` + `ENTRY_TYPE_NAMES` remaining the
  identity SSOT.

## Non-findings (checked, clean)

- **Not a lmao-core tracer.** `lmao-core` public surface is `SpanBuffer`, `Clock`, `TraceId`, `EntryType`,
  `CapacityRatchet`. `telemetry.rs` has none of that. The duplication is the IPC schema (F1), not a second in-process
  span runtime.
- **`sha2` is load-bearing.** Fill-path SHA-256/512 must run in-process on `poll_write` with typed mismatch errors.
  Shelling out to `shasum` is the wrong recommendation.
- **`arrow-array` / `arrow-ipc` / `arrow-schema` 56** are load-bearing for writing Arrow IPC. Do not replace with a
  hand-rolled IPC encoder in this slice; route through `lmao-arrow` (F1). Default features not disabled — not scored
  without a tree (cargo forbidden).
- **Cache map keys are `[u8; 32]`**, not `String`. `CacheKey::digest` length-prefixes components (`update_component`,
  `cache.rs:143-146`) so concatenations cannot collide. SipHash on an already-uniform SHA-256 is evaporating work at
  HTTP timescale — noted, not a finding.
- **LRU collect+sort** (`cache.rs:1117-1125`) runs only above 20 GiB high water. Two-limit (high/low bytes) is present.
  No entry-count cap; the 64 KiB hole (F7) is the de-facto cap. Sort-then-index on that path is once-per-eviction, not
  hot.
- **Operational `Result`.** I/O and parse paths return `CacheError` / `AuditError`. `expect` in these files is on
  state-machine invariants (non-empty batch, pending bytes, buffer-bounded read).
- **`cfg(unix)`** `O_NOFOLLOW` / mode 0600 have `not(unix)` stubs (`cache.rs:1447-1464`, `1304-1306`). Not a
  compile-break; they are weaker.
- **`uuid` crate** is used beyond this slice (writer id, other temps). F11 is cache.rs’s use only.
- **`flush_pending` clones** (`telemetry.rs:314-321`, `399`): once per ≤64-event flush, not the HTTP hit path.
  Clone-heavy, not a finding under §4.1 regime.
- **cache.rs tests:** no unit module (F9). Integration coverage for LRU pin, crash temps, symlink root exists in
  `tests/mirror_cache.rs` (not this slice).
- **No `cargo` / clippy / format / nx** was run.
