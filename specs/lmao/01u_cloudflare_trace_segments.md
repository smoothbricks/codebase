# Cloudflare Trace Segments and Log Sink <a id="smoo/lmao!n/cf-trace-segments"></a>

> Status: partially implemented. The LMAO sink-fit adapters ship as the `@smoothbricks/lmao/cloudflare` subpath
> (`packages/lmao/src/lib/cloudflare/`): the billing-grade `CollectorClient` interface + in-memory fake, the diagnostic
> `DiagnosticDrainTracer` with Pipelines-shaped and Queues-fallback transports, and the `ClassSplitTracer` routing rows
> per delivery class. The collector service, the 01s fetch wrapper, tail-worker crash net, and all platform wiring
> remain design. Investigated 2026-07-04 against current Cloudflare docs/pricing (cited inline; Pipelines and R2 Data
> Catalog are open beta — re-verify pricing before GA commitments).

## Governing Principle <a id="smoo/lmao!n/cf-trace-segments-principle"></a>

**Open formats at every boundary; the substrate is a pricing decision.** Trace data lives as Parquet segments under an
Iceberg catalog on an S3-compatible store. R2 today; any S3-compatible store or a dedicated ClickHouse box tomorrow —
same bytes, only the reader changes.

**Exit test (normative):** migrating the archive = sync the bucket + repoint the catalog. Any component that would break
this sentence is disqualified from the architecture.

## Delivery Classes <a id="smoo/lmao!n/cf-trace-segments-classes"></a>

Traces are **not uniformly loss-acceptable**: consuming systems may derive metered billing from LMAO traces — API
requests create trace rows with billing dimensions, billability derived from `span-ok`/`span-err` outcomes, and a
downstream metering contract consuming periodic aggregates of these traces. A lost billable span is lost revenue. Every
trace row therefore belongs to exactly one delivery class:

| Class             | Lane                                                                                                                                                     | Semantics                                                                                                                                                                                                    | Loss budget                    |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------ |
| **billing-grade** | worker POST → **collector service** (durable ack at the collector's transactional-storage commit — THE durability point) → reduce → staged segments → R2 | at-least-once, end to end. Duplication is safe when the downstream metering contract ingests chunks idempotently (duplicate chunks no-op; parameter-drift replays rejected). Rows carry mapped billing keys. | **zero** — loss = revenue loss |
| **diagnostic**    | **Pipelines** stream binding, direct                                                                                                                     | fire-and-forget. Lazy isolate buffering, best-effort accumulation, and everything in the cost analysis below apply to this class only.                                                                       | bounded, tolerated             |

Both lanes converge on the same Parquet segments under the same catalog — the split is transport and guarantees, never
format.

The sampling knob is **structurally inapplicable to billing-grade** — not "configured off" but absent from the class:
these rows are domain events (facts a downstream contract consumes), not derived plumbing, and domain events always
append. Any future sampling implementation operates on the diagnostic class only.

## Zero-Loss Sources <a id="smoo/lmao!n/cf-trace-segments-zero-loss"></a>

Zero-loss is achieved **by origin, not by transport** — each source type has its own guarantee:

**Durable-origin sources.** A host system that already records usage facts durably before any trace emission exists —
e.g. an event-sourced runtime that logs operation results as part of its own commit — may treat the trace lane as a
projection of records it already owns. Emission rides the host's post-commit outbox, so a crashed flush loses nothing:
the gap is healed by replay/backfill from the durable records, and the host's reconciliation closes the loop. The
collection path below needs no heroics for this class — it can lose chunks and still bill correctly.

**Plain-worker billable spans: send before the response completes.** There is no durable origin in a stateless isolate,
so the acknowledged handoff IS the durability point: the worker POSTs billing-grade rows to its collector shard and the
collector acks only after its **transactional-storage commit** — from that moment the rows are durable and replay-owned.
The send is awaited before/alongside response completion (per request or as a request-scoped micro-batch); the added
tail latency of one round-trip is the price of money. **No lazy cross-request isolate buffering for this class, ever**:
an isolate eviction between requests silently destroys revenue records.

**Crash path: Tail Workers as the safety net.** The tail handler's outcome enum documents that events are generated for
`exception`, `exceededCpu`, `exceededMemory`, and `canceled` producer outcomes — precisely the cases where an in-band
send never ran. A tail worker holding the same pipeline binding emits a minimal billing-grade row (request identity,
billing key, `span-err` outcome — failures are non-billable under an outcome-based billability contract, so the crash
row's job is completeness, not charging). Caveat, stated honestly: Cloudflare documents **no delivery guarantee** for
tail events (best-effort must be assumed), which is why tail is the safety net and never the artery.

**Independent loss detector.** Because no transport above is provably lossless, the leak alarm is external: a periodic
reconciliation job compares billed-span counts per (worker, billing key, window) in the segments against Cloudflare's
own request counts (GraphQL analytics API). Divergence beyond a stated bound (e.g. 0.1% per daily window) pages before
finance notices. This detector is what converts "we believe at-least-once" into a monitored invariant.

## Architecture <a id="smoo/lmao!n/cf-trace-segments-architecture"></a>

Two lanes, one segment tier. All analysis — inspector UI, runtime-metrics aggregates, ad-hoc queries — computes over the
segments: unsampled, schema-owned.

```
request/service code (LMAO tracer, in-band buffers)
  │
  ├─ billing-grade lane (awaited within the request / outbox-driven for durable-origin hosts)
  │    → worker POST → collector shard        (ack at transactional-storage commit = durability)
  │    → reduce (incremental aggregates; threshold alerting; hot window)
  │    → staged content-addressed segments    (bounded ring in collector storage)
  │    → R2 upload + catalog commit           (staging deleted after local durability ∧ upload)
  │
  └─ diagnostic lane (ctx.waitUntil lazy flush; 01s wrapper)
       → Pipelines stream binding: rows as JSON records
       → raw time-windowed segments           (Pipelines SQL transform → exactly-once Parquet to R2)

  → compaction / re-sort              (compactor over 01t primitives, both lanes)
  → optimized Parquet segments        (range-request-tuned layout, below)
  → Iceberg catalog                   (R2 Data Catalog; D1 manifest fallback)
  → readers: duckdb-wasm (browser UI) · chDB/ClickHouse (batch/scale) · downstream consumer jobs
```

The trace **schema definition is platform-neutral and lives with LMAO core**, not this sink: any host runtime, on any
platform, writes identical segment schemas to its S3-API store, so analysis is unified across where services ran.

## Collection Lanes and Fallbacks <a id="smoo/lmao!n/cf-trace-segments-hop"></a>

All paths deliver into the same segment tier, so moving between them changes wiring, not format. Isolate-buffer
semantics per class hold everywhere: billing-grade rows are sent within their request (or via a durable-origin outbox),
diagnostic rows may accumulate lazily in warm isolates as a best-effort optimization.

**Billing-grade primary: the collector service** (next section). Worker POSTs rows to its collector shard; the ack at
transactional-storage commit is the durability point; the collector reduces, stages content-addressed segments, and
uploads to R2. **Rows on the wire is normative for this lane**, not a convenience: the segment layout's sort and
row-group requirements mean the collector must encode at seal time — it alone sees the merged multi-client stream — so
Arrow-on-wire would be pure decode-then-re-encode overhead; rows also keep browser clients Arrow-free (crash-path
emitters must stay dumb). Arrow remains the storage format and the Queues-fallback envelope format. If row-parse CPU
ever becomes measurable, msgpack encoding of the same row shape is the mitigation — the architecture is unchanged by it.
The obvious caution — "a self-built collector lane is self-building Pipelines" — is answered, not ignored: the collector
is justified precisely because it is NOT transport. It does compute on the stream (incremental aggregate reduction,
threshold alerting, the hot-window freshness surface) that Pipelines cannot do; if it were only moving bytes, it would
still be disqualified.

**Diagnostic primary: Cloudflare Pipelines** (open beta; Workers Paid). Ingestion via Worker **stream binding**
(`env.TRACE_STREAM.send(rows)` — array of JSON-serializable records, promise resolves on confirmed ingest) or HTTP
endpoint; SQL transforms at ingest; **exactly-once delivery to R2**; Parquet/JSON/Iceberg output. Limits (beta): 5
MB/ingestion request, 5 MB/s per stream, 20 streams/pipelines/sinks per account. Currently unbilled beyond standard R2
storage/operations.

- Spans are sent as **rows**, not pre-built Arrow chunks: Pipelines owns batching and Parquet encoding for the raw tier.
  The 01s wrapper's `enqueue` seam stays; its payload becomes `rows` instead of an Arrow envelope. 01t's Arrow
  primitives remain the compactor's tools.
- Bindings are env-scoped, so Durable Object classes share their script's pipeline binding — diagnostics flow from DOs
  identically.
- 5 MB/s per stream is the scaling knob: shard streams per domain (or per worker script) before it binds.

**Considered and dropped — awaited Pipelines send as the billing-grade primary:** the `send()` confirmation is a real
durability point, but the lane does no compute on the stream (no reduction, no alerting), offers no hot-window
freshness, its ack semantics are opaque (beta), and post-beta pricing is unknown. It **remains the valid simpler
fallback** for billing-grade wherever the collector is not yet deployed — bootstrap environments bill correctly through
it, they just get no live aggregates.

**Fallback: Queues** if Pipelines' limits or post-beta pricing disqualify it — a drop-in behind the same sink seam.
Messages are 128 KB max and operations are billed per 64 KB (write+read+delete ≈ 3 ops), so the fallback MUST ship
batched chunk envelopes (01s/01t), never per-span messages: 100 M spans/month as ~250 k chunked messages ≈ 0.75 M ops ≈
within the 1 M included; per-span would be ~300 M ops ≈ $120/month. ($0.40/M ops after 1 M included.) At-least-once +
`chunk_id` dedup.

**Bootstrap only: direct R2 per request.** A worker PUTting its own rows/chunks straight to R2. Works on day one with
zero dependencies, but Class A ops are $4.50/M (per-request PUTs at 10 M requests/month ≈ $45 — dwarfing every other
line) and it manufactures a tiny-object compaction burden for the compactor. Acceptable for bootstrap/dev; leave before
volume.

## The Collector Service <a id="smoo/lmao!n/cf-trace-segments-collector"></a>

A stateful collector shard hosted by the consuming system — on Cloudflare, a Durable Object per shard:

- **Shape**: incoming trace batches are reduced incrementally — baseline aggregates (rates, error ratios,
  events-per-batch) accumulate as they arrive; threshold rules fire alert notifications on transitions. The **hot
  window** (last N minutes of rows, queryable from the shard) is the freshness surface — this is where the seconds-fresh
  gap closes, in the same component that owns durability.
- **Sharding**: collector shards are **load/time-oriented, never entity-oriented** — workers POST to their assigned
  lane; shard instances are temporal, matching the segment time-buckets, so shard lifecycle and segment boundaries
  coincide. Shard-local aggregates are rolled up by a downstream rollup step. Entity-keyed aggregation (per account, per
  site) happens behind the catalog boundary, never in the collector shard. The cost, stated honestly: co-located stream
  consumers see **per-shard** streams, so a global threshold is either shard-local or computed at the rollup — there is
  no free global view inside a lane.
- **Staged segments**: the collector's durable log records **references to content-addressed staged segments** — never
  span payloads. Segments are immutable once written and staged in the collector's transactional storage; the staging
  area is a **bounded ring buffer, not a second archive** — a staged segment is deletable once it is uploaded to R2 and
  the collector's own durable state covering it is committed. Tier moves (staging → R2 → cold) are catalog updates;
  content hashes never change.
- **Cost**: DO storage write units ≈ $0.25/GB. Staging 100 M spans/month (~25 GB flowing through) is single-digit
  dollars/month including write-read-upload-delete amplification — and it avoids the direct-R2 Class A storm by batching
  many requests' rows into each staged segment.
- **Hosting is an adapter, not the architecture**: the collector contract is platform-portable — a Durable Object today;
  the same service can run as a Lambda fed by direct HTTPS invoke from workers, or any host with transactional storage.
  If a shard cannot keep up, the options are re-shard or re-host — same service contract, same segments, same catalog,
  per the exit test.

Two normative cautions:

1. **Recursion cut-off**: the collector's own traces go to the **WAE watchdog channel** (see below) or a null sink —
   NEVER through itself. A collector that traces into itself amplifies its own load and deadlocks its backpressure
   story; the watchdog channel's independent failure domain is what makes its health observable while it is the thing
   failing.
2. **Backpressure ownership**: worker behavior when a collector shard is slow/unavailable is defined per class —
   **billable operations fail the request** (a customer op whose revenue record cannot be made durable does not execute;
   same principle as any payment-gateway outage), **diagnostics spill to Pipelines** (or drop, per budget). Never
   silently queue billing-grade rows in the isolate.

**Considered and dropped — Tail Workers as the span transport:** LMAO already holds _structured_ span buffers
in-request; round-tripping them through console-shaped tail items would discard structure to re-parse it, and tail
delivery carries no documented guarantee. Tail Workers instead serve two support roles: the **crash-path safety net for
billing-grade rows** (see Zero-Loss Sources) and complementary capture of _uninstrumented_ workers' console/exception
output (same pipeline binding, `source: 'tail'` column).

**Considered and dropped — Logpush:** account-level log-shaped export, no per-span schema control; fails the exit test's
"schema-owned" premise.

## Consumer Topology <a id="smoo/lmao!n/cf-trace-segments-consumers"></a>

The collector is not the only consumer of every trace row. Consumers split by **shape**, and the shape decides where
they live:

**Stream-shaped consumers are co-located with the collector.** Alerting, crash reporting with ticket-filing side
effects, and coarse global counters are keyed by the stream itself, not by any entity. They run co-located in the
collector shard: the ingest step stages each segment **once**, and co-located consumers read the same staged segment —
each consumer declaring the columns it needs, giving **column-pruned reads** of the shared Arrow batch: a consumer
touches only the columns its rules read. Crash-to-ticket files externally (e.g. Linear) keyed by the **crash-signature
digest** — one ticket per unique crash, idempotent, so redelivered or replayed segments never file duplicates.

**Live visitor view is a stream-shaped consumer — not a catalog consumer.** There is no separate presence beacon: LMAO
is JavaScript, so a page visit is already a `GET /…` trace, and SPAs emit their own LMAO traces for app analytics and
crash reporting anyway — the pageview trace **is** the visitor-active event. The live-visitor consumer maintains
minute-bucketed per-site visitor bitmap state from the same staged batches, reading only its columns (site attribution,
visitor id, `ts`); a visitor's repeat pageviews within a bucket change nothing and fan out to nothing. Cross-shard "who
is on site X now" is a roaring **OR** over shard-published per-site bitmaps; the dashboard subscribes to the rolled-up
view over SSE.

**Entity-shaped consumers never live in the collector shard.** Billable events per account, feature-flag usage per
account, web analytics per site (historical queries — the live view is the stream-shaped consumer above) — anything
keyed by an entity — consumes downstream from the catalog: per-consumer cursors over the immutable segments,
independently replayable.

Budgets and the recursion cut-off apply per co-located consumer: each prices its own canonical workload, and none of
them may trace into the collector.

## Browser Ingress and Column Provenance <a id="smoo/lmao!n/cf-trace-segments-provenance"></a>

Browsers are first-class trace emitters, and that makes ingress a trust boundary. Two rules govern it.

**Browser ingress.** The collector endpoint accepts browser-origin batches with: CORS restricted to registered site
origins, a **per-site ingest key** attributing every batch, **diagnostic class only** (browser rows are never
billing-grade), and per-key rate limits.

**Column provenance classes.** The trace schema (owned by LMAO core — the schema is platform-neutral, like the segment
format) marks every column as one of two classes:

- **Server-stamped** — user/org/site attribution, session, and **all billing dimensions**: written only by the first
  verified server-side hop, from JWT claims, the ingest key, or request metadata. Browser-origin batches containing
  server-stamped columns are stripped or rejected, never merged. The ingress check is structural, not evaluative:
  `browser batch ∩ server-stamped columns = ∅`, then stamp.
- **Client-supplied** — spans, timings, component names, client errors, breadcrumbs: accepted as self-report —
  schema-validated, size-bounded, and never billing- or authorization-relevant. A malicious client can lie, but only
  about itself.

Anonymous visitor ids (the live-view key) are client-generated but **ingest-key-scoped**: they can only attribute to the
site whose key sent them, so identity poisoning collapses to inflating your own site's numbers. The residual is
cardinality abuse — spraying fresh visitor ids to bloat per-site bitmaps — mitigated by the per-key rate limits plus a
bitmap-cardinality gauge on the watchdog channel.

The principle, the same invariant multi-tenant web systems converge on: **identity is a property of the verified
channel, never the payload.**

## Metering Boundary <a id="smoo/lmao!n/cf-trace-segments-billing-boundary"></a>

The collector produces **segments**; the archive processor's splitting stage derives the **chunks** a downstream
metering consumer ingests: idempotent chunk ingestion (duplicate no-ops, parameter-drift rejection), outcome-based
billability filtering, windowed usage aggregation.

**Two-layer identity.** A _segment_ is the sealed, content-addressed archive unit — its identity is its content hash,
which is what storage-level dedup keys on. A _chunk_ is the consumer-facing processing unit: a segment as-is, or a
partition-split of one where a mixed-group segment needs group-targeted routing. `chunk_id` ≡ the segment hash when
unsplit; when split, it derives deterministically from `(parent segment hash, partition key)`, preserving lineage.
Load/time-sharded collectors make segments inherently mixed-group, so a `chunk ≡ segment` identity would force
entity-sharded collectors — rejected; the split stage is where group routing belongs.

**Normative interface: the CATALOG is the boundary.** Metering watches/reads the segment catalog; the collector never
addresses billing systems directly. Considered and dropped — **direct collector→billing usage signals**: they would
duplicate the metering consumer's own aggregation stage, couple ops-infrastructure health to revenue processing (a
collector incident becomes a billing incident), and forfeit the chunk-idempotent replay safety that makes at-least-once
delivery sufficient.

## Segment Layout (load-bearing for range requests) <a id="smoo/lmao!n/cf-trace-segments-layout"></a>

Range-request pruning effectiveness is decided at write time; these are the compactor's output requirements:

- **Files**: time-windowed (e.g. 1 h raw → daily compacted), target 100 MB–1 GB compacted; duckdb-wasm's happy path is
  ~10 MB–2 GB per file.
- **Row groups**: 8–32 MB — small enough that a dashboard query pulls only matching groups over HTTP ranges, large
  enough to amortize footer/stat overhead.
- **Sort order within segment**: `(row scope/type column, ts)` — dashboard-shaped queries filter by source type + time
  window; zone maps on both columns then prune most groups. File-level partitioning stays time-major (Iceberg partition
  on window).
- **Column statistics/zone maps**: always written; they are the pruning input for both Parquet-footer readers and the
  catalog.
- Columns are the LMAO system columns (01f) plus host-system scope/metric columns stamped per the provenance rules.

## Catalog <a id="smoo/lmao!n/cf-trace-segments-catalog"></a>

**Primary: Apache Iceberg via R2 Data Catalog** (open beta): standard table format, schema evolution, snapshot/partition
metadata; Iceberg REST is readable by DuckDB/ClickHouse/Trino generally; unbilled during beta beyond R2 ops. A standard
catalog keeps the exit test intact: repointing = changing the catalog URI.

**Fallback: D1 manifest** if Data Catalog beta limits bite (unknown compaction/maintenance features are the main risk).
Manifest rows:
`(segment_id, window_start, window_end, row_types[], row_count, bytes, r2_key, schema_hash, bloom_trace_ids BLOB)`. D1
limits are comfortable for this role: 10 GB/db, unlimited rows, 2 MB/row (bounds the bloom blob), 30 s/query,
single-threaded — catalog queries are ms-scale point/range lookups. A bespoke catalog is strictly a fallback:
hand-rolled metadata over open files re-creates the lock-in the files avoid.

## Sampling Model <a id="smoo/lmao!n/cf-trace-segments-sampling"></a>

**Diagnostic class only, app-level or none — and the numbers say none.** Billing-grade rows are outside sampling's
domain by construction (see Delivery Classes — domain events always append). The storage path never samples for either
class (Pipelines is exactly-once; Queues fallback is at-least-once + `chunk_id` dedup). Keep-everything cost at current
pricing (2026-07, spans ≈ 200 B compressed columnar):

| Volume         | New data/mo | R2 storage (steady state ≈ 3 mo hot) | Hop cost                                              | Total order |
| -------------- | ----------- | ------------------------------------ | ----------------------------------------------------- | ----------- |
| 1 M spans/mo   | ~0.2 GB     | ~$0.01/mo                            | Pipelines: $0 (beta) · Queues fallback: $0 (included) | ~**$0**     |
| 10 M spans/mo  | ~2 GB       | ~$0.09/mo                            | $0 (beta) · fallback ~$0                              | **cents**   |
| 100 M spans/mo | ~20 GB      | ~$0.90/mo                            | $0 (beta) · fallback ~$0–0.4                          | **~$1/mo**  |

(R2: $0.015/GB-mo standard, Class A $4.50/M, Class B $0.36/M, zero egress; compaction reads/writes add Class A/B ops in
the thousands/month — noise. Post-beta Pipelines pricing is the open risk; the Queues fallback bounds it.)

At these numbers the sampling hook shrinks to a **future knob**: LMAO's tracer keeps a head-sampling extension point in
the API, unimplemented, and "always-keep errors/security" rules are moot while nothing is dropped. Retention/tiering
(Infrequent Access at $0.01/GB-mo, or delete-after-N-months as Iceberg snapshot expiry) is the cost lever, not sampling.

## WAE: Justify or Drop <a id="smoo/lmao!n/cf-trace-segments-wae"></a>

What Workers Analytics Engine would buy: `writeDataPoint` is fire-and-forget cheap (10 M points/mo included, +$0.25/M;
currently unbilled), and its SQL API gives **seconds-fresh** dashboards with zero pipeline work. What it costs: a second
store with a second schema (20 blobs/20 doubles/1 index of 96 B; 250 points/invocation), **externally-imposed sampling**
(write-time equitable per-index sampling + read-time ABR; per-row `_sample_interval` must be threaded through every
aggregate), fixed 3-month retention, and no export path.

Verdict: **drop as archive; retained as watchdog channel** (next section). As a system of record it fails the exit test
three ways — proprietary, sampled, unexportable — and that reasoning stands verbatim. The freshness gap it would have
covered is closed by the **collector's hot window** — the same component that owns billing-grade durability serves the
seconds-fresh live view, so no second analytics store is needed for the data itself. Equitable sampling is genuinely
clever (rare index values survive; only high-volume ones are sampled) — the right design for a constraint the archive
does not have, and exactly tolerable for the watchdog role below.

## WAE as Watchdog Channel <a id="smoo/lmao!n/cf-trace-segments-watchdog"></a>

The collector's **self-diagnostics** (the recursion cut-off's destination) and coarse global ops stats go to Workers
Analytics Engine via `writeDataPoint` from the collector shard. What disqualified WAE as the archive is irrelevant here
— watchdog data is disposable, so sampled/3-month/unexportable stop mattering — and one property becomes the point: WAE
is an **independent failure domain**. Fire-and-forget, no ack path, separate infrastructure — the collector's health
signals must not depend on the collector itself or on the R2/segments machinery it might currently be failing with.
Cross-shard global views (all-lanes ingest rate, staging-ring depth, upload lag) come from WAE's SQL API with seconds
freshness and no coordination between shards.

**Normative boundary:** WAE numbers are operational and approximate — never billing, never the metrics source of truth
(collector state + segments are). A dashboard may show both; an invoice or a baseline computation may only ever read
segments.

## Aggregates Consumers <a id="smoo/lmao!n/cf-trace-segments-aggregates"></a>

Two viable shapes over the same segments:

|           | Event-driven rollup in the host system                    | Scheduled OLAP job (chDB/DuckDB, container cron) |
| --------- | --------------------------------------------------------- | ------------------------------------------------ |
| Trigger   | archive-window change notification                        | cron                                             |
| Freshness | per closed window (minutes)                               | job cadence (hours)                              |
| Output    | aggregate facts → alert notifications; aggregate segments | aggregate segments only                          |
| Cost      | collector/host compute per window                         | container minutes per run                        |
| Notes     | alerting needs notifications anyway                       | trivially simple; heavy historical recomputes    |

Recommendation: the rollup role is **filled by the collector** (it already reduces every billing-grade row and holds the
hot window; alerting-grade aggregates fire from its threshold rules), with shard-local aggregates rolled up downstream.
The OLAP job owns heavy historical rollups. Both write aggregate segments back under the same catalog.

## Analysis Surfaces <a id="smoo/lmao!n/cf-trace-segments-analysis"></a>

- **Inspector/UI: duckdb-wasm in the browser** querying R2-hosted Parquet via httpfs HTTP range requests — full SQL over
  the archive with zero server compute. Requirements: bucket exposed on a custom domain with CORS allowing GET+HEAD from
  the UI origin (with correct CORS, httpfs pushes down filters and reads only needed row groups); avoid presigned URLs
  (documented duckdb-wasm HEAD-CORS bug); the layout section above is what makes these queries prune. Catalog access:
  Iceberg REST from the browser, or the D1-fallback manifest via a tiny API. The inspector's **segment source is
  pluggable**: the S3-API bucket in deployments, or **local files in dev** — the dev compactor writes segments to a
  local directory and the same inspector answers the same SQL against them, catalog-guided lazy loading either way, zero
  infrastructure.
- **Heavy/scheduled: chDB / clickhouse-local** over the same bucket via S3 API, in a Cloudflare Container or any
  on-demand runtime; plain DuckDB CLI locally for dev. (Terminology note: AWS S3 Select is deprecated — "S3-native
  querying" today means table formats + engines over range requests, or Athena on AWS.)
- **Worker-side duckdb-wasm: not assumed.** The WASM bundle is tens of MB against a 128 MB isolate and cold-start
  budget; treat as unproven, and unnecessary — browser + container cover the needs. Revisit only with a measured
  prototype.
- **Dedicated-machine exit**: at scale, ClickHouse (or chDB) on owned hardware reads the same bucket via S3 API — no
  format or pipeline change, per the exit test. That lane is a pricing decision, exercised when query volume outgrows
  on-demand runtimes.

## LMAO Sink Fit <a id="smoo/lmao!n/cf-trace-segments-sink-fit"></a>

No new abstraction: the base `Tracer` sink lifecycle (`onTraceEnd`/`flush`) already models this. The Cloudflare sink is
a `Tracer` subclass whose `flush()` drains completed buffers and sends **rows** through the pipeline binding; the 01s
wrapper schedules `flush()` via `ctx.waitUntil`. The Queues-fallback sink instead builds 01t chunk envelopes. Both are
constructor-injected transports — the tracer API is unchanged.

Implemented at `packages/lmao/src/lib/cloudflare/` (exported as `@smoothbricks/lmao/cloudflare`):
`DiagnosticDrainTracer` + `PipelinesStreamTransport`/`QueuesFallbackTransport` (this section),
`CollectorClient`/`FakeCollectorClient` (§Delivery Classes, §Zero-Loss Sources), and `ClassSplitTracer` with an injected
`DeliveryClassifier` — the spec fixes the classes, not the classification mechanism, so the row→class mapping stays a
constructor-injected seam.

## Open Questions <a id="smoo/lmao!n/cf-trace-segments-open"></a>

1. Post-beta Pipelines pricing (the cost table's main unknown; Queues fallback bounds the downside).
2. R2 Data Catalog maintenance/compaction maturity — whether Iceberg snapshot expiry + the compactor suffice, or the D1
   fallback is needed for v1.
3. Pipeline-binding availability inside DOs: mechanically expected (env-scoped bindings), verify at prototype.
4. Tail-event delivery behavior under load: no documented guarantee — measure empirically before relying on the crash
   net's coverage bound, and set the loss-detector threshold from that measurement.
5. WAE watchdog channel from DOs: verify `writeDataPoint` binding availability inside Durable Objects at implementation,
   and the per-invocation write ceiling in the DO context (docs state 250 data points per Worker invocation; confirm how
   that maps to DO alarm/fetch invocations and size collector self-diagnostics accordingly).

## Spec Cross-Impacts <a id="smoo/lmao!n/cf-trace-segments-cross"></a>

- The **billing-grade consumer contract lives with the consuming system**: idempotent chunk ingestion (duplicate no-ops,
  parameter-drift rejection) is what makes at-least-once sufficient, and an outcome-based billability contract is what
  the crash-net rows feed.
- [01s](./01s_cloudflare_fetch_trace_wrapper.md) carries the matching lane note (§Trace Chunk Envelope): the `enqueue`
  seam sends **rows** on the primary Pipelines rung; the Arrow chunk envelope remains the Queues-fallback and compactor
  format.

## Related <a id="smoo/lmao!n/cf-trace-segments-related"></a>

- [Cloudflare Fetch Trace Wrapper](./01s_cloudflare_fetch_trace_wrapper.md) — capture + `enqueue` seam
- [Trace Archive Primitives](./01t_trace_archive_pipeline.md) — chunk/compaction data-plane
- [Arrow Table Structure](./01f_arrow_table_structure.md)
