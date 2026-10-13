# lmao cloudflare, archive, query, inspector, and agent: gaps between the specs and the code

These specs under `specs/lmao/` state their design in present tense: the Cloudflare fetch wrapper (01s), the trace
archive pipeline (01t), the Cloudflare trace segments (01u), the query engine (02), the inspector (03), the inspect CLI
(04), and the AI agent integration (01d). The code does not yet do all of it. Items are independent unless an item names
a dependency. Work items that follow a decision name it by number. Every decision is listed under `## Decisions`, not
under the work items.

## 01s: withLmaoTracing wrapper API

- Spec: `specs/lmao/01s_cloudflare_fetch_trace_wrapper.md:37-50`: `TraceWrapperConfig` (37-43), `WrappedFetch` (45), and
  `withLmaoTracing` (47-50) wrap a Worker fetch handler. The composition note (29-30) names the tracer primitives the
  wrapper composes. Its `enqueue` seam takes rows (39), per decision 1.
- Code: `grep withLmaoTracing|TraceWrapperConfig|WrappedFetch packages/ → no hits`. Primitives it would compose:
  `packages/lmao/src/lib/tracers/ArrayQueueTracer.ts:72` (`drain()`), `packages/lmao/src/lib/cloudflare/traceRows.ts:39`
  (`spanBufferToTraceRows`), and `packages/lmao/src/lib/bufferStrategy.ts:114` (`releaseBuffer`).
  `DiagnosticDrainTracer.flush()` (`packages/lmao/src/lib/cloudflare/diagnosticDrain.ts:136-151`) already composes them.
- Work: add a `withLmaoTracing` module that exports the types at 37-50 and schedules the tracer's flush through
  `ctx.waitUntil`.
- Acceptance: with `maxRowsPerChunk: 1`, `withLmaoTracing(handler, config)(request, env, ctx)` calls an `enqueue` mock
  once per request, after the response returns, with rows that match the spans the handler wrote. With every trigger
  above one request's rows, a request makes no `enqueue` call, and its rows go out with the request that meets a trigger
  (decision 13).

## 01t: payload compaction output

- Spec: `specs/lmao/01t_trace_archive_pipeline.md:116-144`: `CompactionOutput` (116-131) carries `output_format`,
  `payload_bytes` (119), and `lineage` with a `CompactionWindow` (123). `compact` (133-139) takes `sources`, each an
  envelope paired with its Arrow IPC bytes (135, 142), and `targetFormat` (137). Compaction is append-only (144). The
  identity is `(window, source_set_hash, targetFormat)` (173).
- Code: `packages/lmao/src/lib/archive/chunkCompaction.ts:18` (`compactTraceChunks(chunks, window)`) returns metadata
  only. The result type is `CompactedChunkSet` (`:8-15`). Its `compaction_id` hashes `source_set_hash` and the window
  bounds (`:22`), with no format. `grep -E "payload_bytes|output_format|targetFormat" packages/lmao/src → no hits`.
- Work: implement a compactor that takes `sources`, writes payload bytes in the format named by `output_format`, and
  assigns a content-derived `output_chunk_id` over `(window, source_set_hash, targetFormat)`. The window is
  `CompactionWindow` (decision 5).
- Acceptance: compacting N chunks yields one output. Its `payload_bytes` decode to the union of the source rows. Its
  `output_chunk_id` is the same across reruns. The same sources compacted to `arrow` and to `parquet` get different
  `output_chunk_id`s.

## 01t: Rust archive primitives follow the 01t contract

- Spec: `specs/lmao/01t_trace_archive_pipeline.md:32-167` is one contract for both languages:
  `buildTraceChunkEnvelope(input)` takes `row_count`, `started_at_ms`, and `ended_at_ms` (33-41).
  `inspectPartitionCardinality(rows, partitionOf)` (69-72) yields `unknown` for no rows or an empty or missing partition
  value (79). `splitChunkByPartition(rows, partitionOf, partitionField?)` (91-95) returns `PartitionSplit`s with
  `partition`, `selector`, `row_indexes`, and `rows`, in sorted partition-value order, and puts rows without a partition
  in no split (100). `extractChunkStats(envelope, partitions?)` (158-161) returns `TraceChunkStats`.
- Code: the TypeScript primitives in `packages/lmao/src/lib/archive/` match the contract. The Rust twin in
  `packages/lmao/crates/lmao-arrow/src/archive.rs` does not:
  - `build_trace_chunk_envelope` (`:68`) derives `row_count` and the time bounds from a `RecordBatch` (`:69-71`) instead
    of taking them, so the same envelope fields cannot be passed to both languages.
  - `inspect_partition_cardinality(batch)` (`:159`) is hard-wired to the `trace_id` dictionary key (`:147-157`), has no
    partition function, and reads the key of a null `trace_id` row instead of yielding `Unknown`.
  - `split_chunk_by_partition(batch)` (`:173`) returns `Vec<(u32, Vec<usize>)>`: dictionary keys, not partition values,
    sorted by dictionary key (`:174`) rather than by partition value, with no `selector` and no rows.
  - `extract_chunk_stats(batch)` (`:47`) returns `(row_count, min_ns, max_ns)`, not `TraceChunkStats`.
  - Callers: `packages/lmao/crates/lmao-arrow/tests/convert.rs:104-126`,
    `packages/lmao/crates/lmao-arrow/tests/properties.rs:642`, and
    `packages/lmao/crates/lmao-query/tests/parity.rs:187`.
- Work: make the Rust primitives take and return the 01t shapes. `build_trace_chunk_envelope` takes `row_count`,
  `started_at_ms`, and `ended_at_ms`; the batch scan for them becomes a separate helper the caller runs first.
  `inspect_partition_cardinality` and `split_chunk_by_partition` take a partition function over row indexes, and the
  split returns a `PartitionSplit` with `partition: String`, `selector: String`, `row_indexes`, and `rows` as the
  `RecordBatch` of those rows, ordered by partition value. `extract_chunk_stats` takes the envelope and the splits and
  returns `TraceChunkStats`. Migrate the three callers.
- Acceptance: one fixture, given to TypeScript as row objects and to Rust as a `RecordBatch`, yields the same `chunk_id`
  string, the same cardinality, and the same split partitions, order, selectors, and `row_indexes` in both languages. A
  fixture with a null or empty partition value yields `unknown` in both.

## 01u: collector shard as a Durable Object

- Spec: `specs/lmao/01u_cloudflare_trace_segments.md:139-164`: the billing-grade lane ends in a collector Durable Object
  per shard (`Shape` at 143-146, sharding at 147-152). The shard acks at the storage commit (diagram at 71-75), reduces
  rows, and stages segments (153-157). The hosting adapter is at 161-164.
- Code: `packages/lmao/src/lib/cloudflare/collectorClient.ts:14-15` says "The collector DO itself is not deployed yet."
  `CollectorClient` is at `:41`, and `FakeCollectorClient` at `:59`.
  `grep DurableObject packages/lmao/src/lib/cloudflare → no class`.
- Work: implement the collector shard as a Durable Object. It acks only after the transactional-storage commit, reduces
  rows, stages content-addressed segments, and uploads them to R2 (see the platform wiring item). Wire `CollectorClient`
  (`collectorClient.ts:41`) to it.
- Acceptance: a POST returns only after the storage commit. Killing the shard after the ack loses no rows. Staged
  segments survive a restart.

## 01u: tail-worker crash net

- Spec: `specs/lmao/01u_cloudflare_trace_segments.md:51-56`: a tail worker that holds the pipeline binding emits a
  minimal billing-grade row for `exception`, `exceededCpu`, `exceededMemory`, and `canceled` outcomes. Tail events have
  no delivery guarantee (55-56).
- Code: `grep exceededCpu packages/lmao/src → no hits`. No tail handler exists under
  `packages/lmao/src/lib/cloudflare/`.
- Work: implement a tail handler that emits the `span-err` row through the pipeline binding.
- Acceptance: a forced CPU-exceeded invocation yields one tail row. The row carries the request identity and the billing
  key.

## 01u: platform wiring (R2 upload, Iceberg commit, D1 manifest)

- Spec: `specs/lmao/01u_cloudflare_trace_segments.md:74-75`: staged segments upload to R2 and commit to the catalog.
  Line 110 is the Pipelines stream send, `env.TRACE_STREAM.send(rows)`. Lines 277-286 name Iceberg through the R2 Data
  Catalog, with a D1 manifest fallback (281).
- Code: no R2, Iceberg, or D1 manifest code under `packages/lmao/src` or `packages/lmao-inspector/src`.
  `TRACE_STREAM.send` appears only in a comment at `packages/lmao/src/lib/cloudflare/diagnosticDrain.ts:10`. The real
  call is `this.transport.send(rows)` at `:146`.
- Work: add the R2 upload, the catalog commit, and the D1 manifest fallback. The Pipelines SQL transform lives in the
  consuming system.
- Acceptance: a diagnostic segment appears in R2 under the catalog after commit. Its staging copy is deleted only after
  the upload succeeds.

## 01u: diagnostic lane enqueue seam and waitUntil flush

- Spec: `specs/lmao/01u_cloudflare_trace_segments.md:77-79`: the diagnostic lane flushes lazily through `ctx.waitUntil`,
  via the 01s wrapper. Lines 115-117: the 01s wrapper's `enqueue` seam stays, and its payload becomes rows.
- Code: `packages/lmao/src/lib/cloudflare/diagnosticDrain.ts:126` (`DiagnosticDrainTracer`). `flush()` is at `:136-151`
  and calls `this.transport.send(rows)` at `:146`. No fetch wrapper exists yet (see the 01s wrapper item).
- Work: have the 01s fetch wrapper call `enqueue` on the tracer. It schedules `flush()` through `ctx.waitUntil`.
- Acceptance: one fetch through the wrapper that meets a flush trigger (decision 13) triggers one Pipelines `send` after
  the response ends.

## 01u: WAE watchdog channel

- Spec: `specs/lmao/01u_cloudflare_trace_segments.md:310` (`writeDataPoint`). Lines 326-329: WAE is an independent
  failure domain. Lines 333-335: the recursion cut-off, so collector traces never route through the watchdog channel
  itself. Open question Q5 (393-395) asks about the WAE binding inside a Durable Object.
- Code: `grep writeDataPoint packages/ → no hits`.
- Work: the collector shard writes its self-diagnostics through `writeDataPoint`. Verify the WAE binding inside a
  Durable Object first (Q5, 393-395).
- Acceptance: with R2 down, a backlog data point written by the collector is readable through the WAE SQL API.

## 01u: consumer topology

- Spec: `specs/lmao/01u_cloudflare_trace_segments.md:191-196`: stream-shaped consumers co-located in the collector shard
  declare their columns, which gives column-pruned reads of the shared Arrow batch. One consumer files one ticket per
  crash-signature digest. Lines 198-204: the live-visitor consumer keeps minute-bucketed per-site visitor bitmaps. Lines
  202-203: the cross-shard view is a roaring OR over shard-published bitmaps.
- Code: `grep -E "roaring|crashSignature" packages/lmao/src → no hits`. The `bitmap` matches in `packages/lmao/src` are
  Arrow null-bitmap code only.
- Work: implement the co-located consumers, the column-pruned read API, the crash-signature digest, and the visitor
  bitmap consumer.
- Acceptance: two consumers read the same staged segment, and each reads only its declared columns. Replaying a segment
  files no duplicate ticket.

## 01u: browser ingress and column provenance

- Spec: `specs/lmao/01u_cloudflare_trace_segments.md:218-220`: browser ingress uses CORS for registered site origins, a
  per-site ingest key, the diagnostic class only, and per-key rate limits. Lines 225-228: server-stamped columns come
  only from a verified server-side hop. A browser batch that contains them is stripped or rejected. The check is
  `browser batch ∩ server-stamped columns = ∅`.
- Code: `grep -E "serverStamped|ingestKey" packages/ → no hits`.
- Work: implement the browser ingress endpoint and the structural check `browser batch ∩ server-stamped columns = ∅`.
  Stamp server-side columns after the check passes.
- Acceptance: a browser batch that contains a server-stamped column is stripped or rejected. A browser row marked
  billing-grade is refused.

## 01u: segment layout

- Spec: `specs/lmao/01u_cloudflare_trace_segments.md:264-272`: files of 100 MB to 1 GB and row groups of 8 to 32 MB
  (264-267). Sort by the type column, then `ts` (268-269). Zone maps on both columns (271-272).
- Code: `grep -E "rowGroup|zone ?map|sortKey" packages/lmao/src → no hits`.
- Work: the compactor writes files with these sizes, row groups, and sort order. This item depends on the payload
  compaction item, and its windows are `CompactionWindow` bounds (decision 5).
- Acceptance: a compacted file's footer shows row groups of 8 to 32 MB, sorted by `(type, ts)`.

## 01u: head-sampling extension point

- Spec: `specs/lmao/01u_cloudflare_trace_segments.md:304-306`: the tracer API keeps a head-sampling extension point.
  Lines 29-32: sampling is structurally absent from billing-grade rows. Where applied, it operates on the diagnostic
  class only.
- Code: `grep -i sampl packages/lmao/src` hits are profiler or test comments only (for example
  `packages/lmao/src/lib/traceId.ts:63,116`). There is no sampler type or option.
- Work: add the head-sampling extension point to the tracer API. It applies to the diagnostic class only.
- Acceptance: a sampler that drops everything drops the diagnostic rows. Billing-grade rows are still written.

## 01u: analysis surfaces (segment source, httpfs, chDB)

- Spec: `specs/lmao/01u_cloudflare_trace_segments.md:355-358`: the browser path reads R2 Parquet through httpfs. Lines
  359-362: the segment source is pluggable, either an S3-API bucket or a local directory in development. Lines 363-365:
  a scheduled chDB job reads the same bucket. Lines 366-368: a Worker runs duckdb-wasm.
- Code: `grep -E "segmentSource|localDir" packages/ → no hits`.
  `grep -E "httpfs|chdb|clickhouse" packages/lmao/src packages/lmao-inspector/src → no hits`. The archive source is
  HTTP-only Arrow IPC (`packages/lmao-inspector/src/sources/archive-source.ts:16`).
- Work: add a pluggable segment source (S3-API or local directory), Parquet reads through httpfs in the browser path,
  and the scheduled chDB job. Each segment source is an `ArchiveSource` with `fetchRange(startTime, endTime)` (decisions
  7 and 10).
- Acceptance: the archive source reads the same batches from a local directory and over HTTP, for the same segment set.

## 02: server-side DuckDB reader on Lambda

- Spec: `specs/lmao/02_query_engine.md:5-9`: DuckDB runs on AWS Lambda. Lines 52-53: httpfs reads S3 natively with
  `read_ipc('s3://...')`. Lines 60-68: flushed Arrow IPC files are read directly from S3 (line 68). Lines 81-93: the S3
  reading section.
- Code: `grep -E "read_ipc|httpfs|duckdb" packages/` hits only `packages/lmao-inspector` (the DuckDB-WASM dependency,
  and `packages/lmao-inspector/src/engine/query-engine.ts:38-40`). There is no Lambda reader.
  `grep -E "s3://|S3" packages/lmao/src` hits test string literals only
  (`packages/lmao/src/lib/__tests__/trace-archive-primitives.test.ts:10,20`). There is no S3 flush writer.
- Work: build the DuckDB-on-Lambda reader for the flushed Arrow IPC files, plus the S3 flush path that writes them.
- Acceptance: `SELECT count(*) FROM read_ipc('s3://…/fixture.arrow')` on Lambda returns the fixture's row count.

## 02: msgpack_extract DuckDB extension

- Spec: `specs/lmao/02_query_engine.md:5-9` (overview: a custom `msgpack_extract()` DuckDB extension). Lines 113-141:
  the API. `msgpack_extract_string` and `msgpack_extract_int` are at 133-134.
- Code:
  `grep -E "msgpack_extract\(|msgpack_extract_string|msgpack_extract_int" packages/lmao packages/lmao-inspector → no hits`.
  `packages/columine/crates/columine-parsing/src/msgpack_extractor.rs:14` (`extract_msgpack_events`) is a columnar
  extractor, not a DuckDB scalar function.
- Work: build the DuckDB extension with the functions in the spec's API section (110-138).
- Acceptance: `SELECT msgpack_extract_string(custom_payload, '$.correlationId')` returns the value. A path that is
  absent returns NULL.

## 02: storage and runtime variants

- Spec: `specs/lmao/02_query_engine.md:213-221`: three variants are named: Parquet export, ClickHouse, and WASM DuckDB.
- Code: `parquet` appears only in comments (`packages/lmao/src/lib/cloudflare/diagnosticDrain.ts:11`, and one more
  comment in `traceRows.ts:5`). `grep -i clickhouse packages/ → no hits`.
- Work: none until the owner adopts a variant.
- Acceptance: none until a variant is adopted.

## 03: Tamagui components

- Spec: `specs/lmao/03_inspector.md:90-93`: the components are Tamagui-based: `TraceViewer`, `TraceTimeline`,
  `SpanDetail`, and `LogSearch`. The native build shows the list view without SQL.
- Code: `packages/lmao-inspector/package.json:29-30` lists `react-native` and `tamagui` as peer dependencies only.
  `grep -E "TraceViewer|TraceTimeline|SpanDetail|LogSearch|tamagui" packages/lmao-inspector/src → no hits`.
- Work: build the four Tamagui components. `TraceViewer` takes a `StreamSource` or an `ArchiveSource`
  (`specs/lmao/03_inspector.md:97-98`, decision 7).
- Acceptance: `TraceViewer` renders a fixed Arrow fixture on web. On native, the list view renders without SQL.

## 03: Parquet archive fetch

- Spec: `specs/lmao/03_inspector.md:51-58`: the archive source fetches historical Arrow IPC or Parquet files by time
  range (Parquet at line 58).
- Code: `packages/lmao-inspector/src/sources/archive-source.ts:32-83` decodes Arrow IPC only.
  `grep -i parquet packages/lmao-inspector/src → no hits`.
- Work: add Parquet decoding to the archive source.
- Acceptance: a Parquet object yields the same RecordBatches as the IPC encoding of the same rows.

## 03: msgpack_extract in the browser engine

- Spec: `specs/lmao/03_inspector.md:86-88`: `msgpack_extract()` runs in DuckDB-WASM, either as a WASM extension or
  through client-side msgpack deserialization.
- Code: `grep "msgpack_extract(" packages/lmao-inspector/src → no hits`. The engine
  (`packages/lmao-inspector/src/engine/query-engine.ts:38-40`) has no msgpack path.
- Work: ship the extension as WASM, or add client-side msgpack deserialization.
- Acceptance: a query over an `S.unknown()` column returns the extracted string in the browser.

## 04: lmao-inspect CLI

- Spec: `specs/lmao/04_inspect_cli.md:3-6`: a small, generic, local-first binary over `lmao-query`. Lines 16-24: the
  commands `scan`, `tail -f`, `query`, and `trace`, with `--json` and `--ndjson` output. Lines 35-36: byte-stable NDJSON
  over the golden fixture.
- Code: there is no `lmao-inspect` binary. `grep -E "\-\-ndjson" packages/ → no hits`. `packages/lmao/README.md:363`
  lists the `lmao-query` backends as remaining work. Lines 367-369 list `lmao-inspect` in the backlog. The `lmao-query`
  crate exists at `packages/lmao/crates/lmao-query`.
- Work: build the `lmao-inspect` binary. It has `scan`, `tail -f`, `query`, and `trace`, `--json` and `--ndjson` output,
  selector exit codes, and deterministic NDJSON.
- Acceptance: a selector that holds exits 0. A selector that fails exits 1. Two `--ndjson` runs over the golden fixture
  are byte-identical.

## 04: cowshed wrapper verbs

- Spec: `specs/lmao/04_inspect_cli.md:10-12`: consumers wrap the CLI in domain verbs: `cowshed logs`, `cowshed audit`,
  and `cowshed trace`.
- Code: `grep -E "cowshed logs|cowshed audit|cowshed trace" packages/ → no hits`.
- Work: add the three verbs as thin wrappers over `lmao-inspect`. This item depends on the CLI item.
- Acceptance: `cowshed logs` runs `lmao-inspect scan` and returns its output.

## 01d: MCP trace-analyzer server

- Spec: `specs/lmao/01d_ai_agent_integration.md:40` (`MCPServer`). Lines 47-146: `addTool` at 47, 63, 80, 102, 130,
  and 145. The tools are `get_traces_by_test_run` (48), `get_traces_by_span` (64), `get_span_by_identity` (81),
  `get_parent_span` (103), `get_performance_metrics` (131), and `analyze_feature_flag_usage` (146). The client configs
  at 489 and 515 name `dist/trace-mcp-server.js`.
- Code: `grep -E "MCPServer|addTool|get_traces_by_test_run|get_span_by_identity" packages/ → no hits`.
- Work: implement the MCP trace-analyzer server with the six tools. Build `dist/trace-mcp-server.js` so the client
  config paths at 489 and 515 resolve.
- Acceptance: an MCP client started from the 01d client config lists the six tools. Each tool returns the spans for a
  seeded `test_run_id`.

## 01d: Jest and Bun test-framework plugins

- Spec: `specs/lmao/01d_ai_agent_integration.md:184-254`. The Jest plugin calls `generateTestRunId()` (204), sets
  `globalThis.__TEST_RUN_ID__` (206), and calls `trace.setContext` (213-214). The Bun plugin does the same at 232 and
  239-240.
- Code: `grep -E "generateTestRunId|__TEST_RUN_ID__" packages/ → no hits`.
- Work: implement `generateTestRunId`, a Jest plugin that sets `globalThis.__TEST_RUN_ID__`, and a Bun plugin. Both wire
  `trace.setContext`.
- Acceptance: a Jest test and a Bun test each emit spans whose `test_run_id` equals the id in
  `globalThis.__TEST_RUN_ID__`.

## 01d: production trace server

- Spec: `specs/lmao/01d_ai_agent_integration.md:339` (`productionTraceServer`). Line 342: OAuth2 `authentication`. Lines
  349-363: `get_production_traces` (350) and `analyze_production_performance` (363). Line 382: `ProductionTraceRecord`.
  Lines 395-396: `get_production_trace_details`.
- Code:
  `grep -E "productionTraceServer|ProductionTraceRecord|get_production_traces|analyze_production_performance" packages/ → no hits`.
- Work: implement the production server with OAuth2 authentication. Add masking and encryption to
  `ProductionTraceRecord`. Implement the three production tools: `get_production_traces`,
  `analyze_production_performance`, and `get_production_trace_details`.
- Acceptance: a request without a valid OAuth2 token is rejected. A stored `ProductionTraceRecord` has no plaintext for
  the masked fields.

## Decisions

### 1. Producer envelope (01s)

- Spec says: the 01s lane note (`specs/lmao/01s_cloudflare_fetch_trace_wrapper.md:80-84`) says the `TraceChunkEnvelope`
  below is the payload format for the Queues fallback lane and for the compactor's archive primitives. The type at
  87-101 has `chunk_id`, `emitted_at`, `domain`, `runtime`, `trace_format`, `compression?`, `payload_bytes`, and
  `metadata`.
- Code does: `packages/lmao/src/lib/archive/chunkEnvelope.ts:12-21` defines `TraceChunkEnvelope` with `chunk_id`,
  `file_ref`, `chunk_ref`, `row_count`, `started_at_ms`, `ended_at_ms`, `partition_keys`, and `metadata?`. It has no
  payload bytes and no `emitted_at`. The builder is `buildTraceChunkEnvelope` (`:23`). The Queues fallback lane ships as
  `TraceChunkQueueMessage` (`packages/lmao/src/lib/cloudflare/diagnosticDrain.ts:46-49`): one envelope plus its rows.
  `QueuesFallbackTransport` (`:88-110`) sets `chunk_ref` to a content hash of the rows (`:102`).
- Options:
  - (a) The spec adopts the shipped by-reference envelope. The type at 87-101 is rewritten to match
    `chunkEnvelope.ts:12-21`.
  - (b) The code adds the spec fields and carries payload bytes inline.
  - (c) Two envelopes, one per lane: a payload envelope for the Queues lane and the by-reference envelope for the
    archive. Both are documented and provided.
  - Resolve together with decision 2.
- Applied: 01s now gives the shipped `TraceChunkEnvelope` and `TraceChunkQueueMessage`
  (`specs/lmao/01s_cloudflare_fetch_trace_wrapper.md:89-103`). The `enqueue` seam takes rows (39), the request flow
  flattens buffers with `spanBufferToTraceRows` (73-75), and the reference flush routine is the shipped
  `DiagnosticDrainTracer.flush()` (168-190). 01u's lane wording follows (101-103, 130-132, 377-378, 402-404). The flush
  example builder item is gone: the example no longer calls `encodeArrowTableAsChunkEnvelope`.
- Decision: adopt the shipped shape. The shipped Queues lane already pairs the by-reference envelope with its rows in
  one message and the primary lane sends rows, so one envelope type serves both lanes and the wrapper needs no Arrow
  encode step.

### 2. Archive envelope: inline bytes or by reference (01t)

- Spec says: `payload_bytes` is documented as "Arrow IPC bytes" (`specs/lmao/01t_trace_archive_pipeline.md:38`). The
  envelope type runs 33-46. The `chunk_id` sentence (58-59) says it is FNV-1a over a canonical body.
- Code does: the envelope is by reference (`packages/lmao/src/lib/archive/chunkEnvelope.ts:12-21`). It has no payload
  field. `grep payload_bytes packages/lmao/src → no hits`. The `chunk_id` construction matches the spec sentence:
  canonical body at `chunkEnvelope.ts:24-34`, and `fnv1a64` at `:69-80`. The Rust twin has the same shape
  (`packages/lmao/crates/lmao-arrow/src/archive.rs:27-36`).
- Options:
  - (a) Keep the by-reference envelope. Remove `payload_bytes` from the spec, so the bytes live in the object that
    `file_ref` names.
  - (b) Inline bytes. Add `payload_bytes` plus the `format`, `codec`, and `emitted_at` fields to the code.
  - The `chunk_id` sentence stays either way. Resolve together with decision 1.
- Applied: `specs/lmao/01t_trace_archive_pipeline.md:32-62` gives `TraceChunkEnvelopeInput`, `TraceChunkEnvelope`, and
  `buildTraceChunkEnvelope`, and says the bytes live in the object that `file_ref` names. Because the envelope carries
  no bytes, `compact` takes `sources` that pair each envelope with its Arrow IPC bytes (135, 142).
- Decision: adopt the shipped shape. The by-reference envelope ships in TypeScript and Rust, hashes a small descriptor
  instead of the payload, and leaves the bytes in the immutable object it names.

### 3. Partition cardinality shape (01t)

- Spec says: the spec defines `PartitionCardinality` as object kinds `single`, `mixed`, and `unknown`
  (`specs/lmao/01t_trace_archive_pipeline.md:64-67`). They are produced by
  `inspectPartitionCardinality({chunk, partitionColumns})` (`:70-73`).
- Code does: `packages/lmao/src/lib/archive/chunkRouting.ts:11` defines
  `inspectPartitionCardinality(rows, partitionOf)`. It returns a string tag, not an object. The type is at `:1`.
- Options:
  - (a) The spec adopts the string tag and the `(rows, partitionOf)` signature. `partition_count` and `reason` move to
    separate fields, or are dropped.
  - (b) The code returns the spec's object kinds with `partition`, `partition_count`, and `reason`, and takes the spec's
    `{chunk, partitionColumns}` input.
- Applied: `specs/lmao/01t_trace_archive_pipeline.md:66-79` gives the string tag and the `(rows, partitionOf)`
  signature. `partition_count` and `reason` are dropped.
- Decision: adopt the shipped shape. A by-reference envelope has no rows to inspect (decision 2), and the tag lets the
  scan stop at the second partition, where a `mixed` count would force the full scan that the split already does.

### 4. Split selector and chunk reference (01t)

- Spec says: a split chunk carries `row_count` (`specs/lmao/01t_trace_archive_pipeline.md:88`), an optional
  `row_bitmap?: Uint8Array; // optional compact row selector` (`:89`), and a `chunk_ref` with `chunk_id` (`:91-94`).
- Code does: `packages/lmao/src/lib/archive/chunkRouting.ts:36` (`splitChunkByPartition`) returns, per partition, a
  `partition`, a `selector`, `row_indexes`, and `rows`. The keys are sorted (`:60`). There is no `row_bitmap` and no
  `chunk_ref` in `packages/lmao/src`.
- Options:
  - (a) The spec adopts `row_indexes` and `rows`. The parent `chunk_id` is recorded in lineage instead of a `chunk_ref`.
  - (b) The code adds the `row_bitmap` selector and the `chunk_ref` to each split.
- Applied: `specs/lmao/01t_trace_archive_pipeline.md:83-104` gives `PartitionSplit<T>` and `splitChunkByPartition`. The
  caller records the parent `chunk_id` in each split chunk's lineage (102-104).
- Decision: adopt the shipped shape. `row_indexes` is always present, where the optional `row_bitmap` let a slice carry
  no row selection at all, and the caller already holds the parent `chunk_id`.

### 5. Compaction window and identity (01t)

- Spec says: `lineage.window` is a string tag (`specs/lmao/01t_trace_archive_pipeline.md:118`, and `compact` takes
  `window: string` at `:131`). The compaction identity is `(window, source_set_hash, targetFormat)` (`:162`).
- Code does: `packages/lmao/src/lib/archive/chunkCompaction.ts:3-6` defines `CompactionWindow` as
  `{started_at_ms, ended_at_ms}`. `CompactedChunkSet` (`:8-15`) has no `targetFormat`.
- Options:
  - (a) The spec adopts the window object `{started_at_ms, ended_at_ms}`. `targetFormat` is removed from the identity
    until a payload-producing compactor exists (work item 01t above).
  - (b) The code changes the window to a string tag and adds `targetFormat` to the identity. The `compaction_id`
    derivation changes with it.
- Applied: `specs/lmao/01t_trace_archive_pipeline.md:111-114` defines `CompactionWindow`, used by `lineage.window` (123)
  and `compact` (136). The identity at 173 keeps `targetFormat`: `CompactedChunkSet` has no format, so it needs none,
  but a `CompactionOutput` has `output_format`, and two outputs of the same sources in different formats must not share
  an id.
- Work: the payload compaction item derives `output_chunk_id` from `(window, source_set_hash, targetFormat)`.
- Acceptance: the same sources compacted to `arrow` and to `parquet` get different `output_chunk_id`s.
- Decision: adopt the shipped shape for the window, and keep `targetFormat` in the spec's identity. Window bounds say
  which interval a compaction covers, which a tag like `'1h'` does not, and only an output with a format needs the
  format in its id.

### 6. Per-column stats or chunk rollups (01t)

- Spec says: the spec gives two shapes. Per-column stats are `ChunkColumnStat` with `null_count`, and
  `TraceChunkStats.summarize({chunk, columns})` returns them (`specs/lmao/01t_trace_archive_pipeline.md:142-151`). Line
  154 calls them optional predicate pre-filtering primitives. Line 156 describes chunk-level rollups: "Chunk-level
  rollups carry `chunk_id`, `row_count`, `partition_count`, time bounds, and `duration_ms`."
- Code does: `packages/lmao/src/lib/archive/chunkStats.ts:4-11` defines `TraceChunkStats`. `extractChunkStats` (`:14`)
  produces chunk-level values only. There are no per-column stats.
- Options:
  - (a) The spec keeps only the chunk-level rollup at 156. The per-column `ChunkColumnStat` block (142-151) is removed.
  - (b) The code adds per-column `ChunkColumnStat` through `summarize`, and keeps the chunk-level rollup as well.
- Applied: `specs/lmao/01t_trace_archive_pipeline.md:148-167` gives `TraceChunkStats` and `extractChunkStats`, and
  points per-column statistics at the segments' column statistics and zone maps.
- Decision: adopt the shipped shape. Every compacted segment already writes per-column statistics and zone maps (01u
  §Segment Layout), and a by-reference envelope has no rows to summarize.

### 7. TraceSource type (03)

- Spec says: `TraceViewer` takes a `TraceSource` (`specs/lmao/03_inspector.md:94`). The spec's constructors return
  `TraceSource` (`:147-149`).
- Code does: `packages/lmao-inspector/src/index.ts:7-8` exports `createArchiveSource` and the `StreamSource` and
  `StreamSourceConfig` types. The source types are `StreamSource` (`stream-source.ts:12-19`) and `ArchiveSource`
  (`archive-source.ts:6-9`). There is no `TraceSource`.
- Options:
  - (a) Add one `TraceSource` interface that both sources implement. `TraceViewer` takes it.
  - (b) The spec drops `TraceSource`. `TraceViewer` takes `StreamSource | ArchiveSource`.
  - (c) `TraceSource` is the tagged union `StreamSource | ArchiveSource`, discriminated by a `kind` field.
- Applied: `specs/lmao/03_inspector.md:97-98` says `TraceViewer` accepts a `TraceSource` and switches on `kind`; the
  public API defines `StreamSource` (`kind: 'stream'`), `ArchiveSource` (`kind: 'archive'`) and
  `type TraceSource = StreamSource | ArchiveSource`.
- Work: add `readonly kind: 'stream'` to `StreamSource` (`stream-source.ts:12-19`) and the object `createStreamSource`
  returns, `readonly kind: 'archive'` to `ArchiveSource` (`archive-source.ts:6-9`) and `createArchiveSource`'s object,
  and export `type TraceSource` from `packages/lmao-inspector/src/index.ts`. `TraceViewer`, which the package does not
  have yet, takes a `TraceSource` and narrows on `source.kind` with an exhaustive switch.
- Acceptance: `package-exports.test.ts` sees `kind` on both created sources; a type test shows a `switch (source.kind)`
  over `TraceSource` is exhaustive with the two cases and that a source without `kind` is rejected.
- Decision: keep a `TraceSource` type, as the tagged union (c). The two sources differ in kind, a pushed `onBatch`
  stream against a pulled `fetchRange` over a time range, so one shared interface would hide the range that the archive
  needs; a `kind` discriminant lets `TraceViewer` narrow exactly instead of probing members structurally.

### 8. createStreamSource configuration (03)

- Spec says: the example at `specs/lmao/03_inspector.md:98` calls `createStreamSource({ wsUrl, groupId })`. The public
  API at `:147` returns `TraceSource`. The topic is `traces:{groupId}`.
- Code does: `packages/lmao-inspector/src/sources/stream-source.ts:3-10` defines `StreamSourceConfig` as
  `{url, transport: 'websocket' | 'sse', topic}`. `createStreamSource(config)` is at `:26`.
- Options:
  - (a) The spec adopts `{url, transport, topic}`, and the example at 98 changes to match.
  - (b) The code takes `{wsUrl, groupId}` and derives the topic `traces:{groupId}`. The `transport` option is removed.
    SSE then needs its own constructor, which is decision 9.
- Applied: `specs/lmao/03_inspector.md:34-36` and 48-49 describe the `transport` and `topic` options, the example at 102
  passes `traces:${groupId}` as the `topic`, and the public API gives `StreamSourceConfig` (151-155) and
  `createStreamSource(config: StreamSourceConfig): StreamSource` (164).
- Decision: adopt the shipped shape. One constructor covers both transports, and the relay's `traces:{groupId}` topic is
  one value of the caller's `topic`.

### 9. createSseSource constructor (03)

- Spec says: the public API lists a separate `createSseSource` constructor with `{url}`
  (`specs/lmao/03_inspector.md:148`).
- Code does: `grep createSseSource packages/ → no hits`. SSE is `transport: 'sse'` inside `createStreamSource`
  (`packages/lmao-inspector/src/sources/stream-source.ts:3-10`). SSE frames are base64 (`:89`).
- Options:
  - (a) Keep `createSseSource` as an exported wrapper over `createStreamSource({transport: 'sse'})`.
  - (b) Remove `createSseSource` from the spec. SSE is the `transport` option only.
- Applied: `createSseSource` is removed from `specs/lmao/03_inspector.md`. SSE is `transport: 'sse'` (48-49).
- Decision: adopt the shipped shape. It follows decision 8, and a second constructor would be a second name for
  `transport: 'sse'`.

### 10. createArchiveSource constructor (03)

- Spec says: the public API lists `createArchiveSource({fetchUrl}): TraceSource` (`specs/lmao/03_inspector.md:149`). It
  fetches Arrow IPC buffers over HTTP GET and yields them as an async iterable.
- Code does: `packages/lmao-inspector/src/sources/archive-source.ts:16` defines `createArchiveSource(baseUrl: string)`,
  which returns `ArchiveSource`. `ArchiveSource.fetchRange(startTime, endTime)` is at `:8`. The `?start=&end=` query is
  at `:19`.
- Options:
  - (a) The spec adopts `createArchiveSource(baseUrl: string): ArchiveSource` with `fetchRange`. The async iterable
    becomes the return value of `fetchRange`.
  - (b) The code takes `{fetchUrl}` and returns a `TraceSource`. `fetchRange` moves or is dropped.
  - The work items for the segment source and the archive source follow this choice.
- Applied: `specs/lmao/03_inspector.md:53-56` describes `createArchiveSource(baseUrl)` and `fetchRange`, and the public
  API gives `ArchiveSource` (161-163) and `createArchiveSource(baseUrl: string): ArchiveSource` (165).
- Decision: adopt the shipped shape. The spec fetches by time range but its `{fetchUrl}` constructor had no place for
  the range, which `fetchRange(startTime, endTime)` takes; it also follows decision 7.

### 11. _resetEngineForTesting export (03)

- Spec says: the public API lists `createQueryEngine(): Promise<ArrowQueryEngine>` (`specs/lmao/03_inspector.md:174`)
  and no test hook.
- Code does: `packages/lmao-inspector/src/index.ts:5` exports `_resetEngineForTesting`. It is defined at
  `packages/lmao-inspector/src/engine/query-engine.ts:97-99`. No test imports it:
  `grep _resetEngineForTesting packages/` hits only the export and the definition.
- Options:
  - (a) The spec documents `_resetEngineForTesting` as a test seam.
  - (b) Remove it from the public index. It stays as an internal module that tests import directly.
- Work: remove `_resetEngineForTesting` from `packages/lmao-inspector/src/index.ts:5`. Delete the function and its doc
  comment at `packages/lmao-inspector/src/engine/query-engine.ts:93-99`, since no test imports it.
- Acceptance: `packages/lmao-inspector/src/__tests__/package-exports.test.ts` asserts that the package index has no
  `_resetEngineForTesting` export, and `grep _resetEngineForTesting packages/` returns no hits.
- Decision: keep the spec. A test hook in the public index is API surface that no caller uses, and the engine already
  resets its singleton on an init failure and on `close()`.

### 12. Distributed-trace query engine (01d)

- Spec says: `specs/lmao/01d_ai_agent_integration.md:460` says "Use ClickHouse chDB or AWS Athena". The code block at
  465-468 deploys chDB on AWS Lambda. Line 474 says chDB on Lambda is the cost-effective option. The spec names
  `get_distributed_trace` at 449-450.
- Code does: `grep -E "queryDistributedTrace|get_distributed_trace" packages/ → no hits`. No chDB, Athena, or Lambda
  query code exists. The trace id the queries key on is the `TraceId` branded type at
  `packages/lmao/src/lib/traceId.ts:20`.
- Options:
  - (a) chDB on Lambda. Keep 465-468 and remove the Athena mention at 460.
  - (b) Athena. Rewrite 460-468 around Athena and drop chDB.
  - Either way, the acceptance is: `get_distributed_trace(traceId)` returns the rows for that id across all Parquet
    files that the spec's glob selects.
- Applied: the Athena mention at `specs/lmao/01d_ai_agent_integration.md:460` is removed. Decision 14 then names DuckDB
  as the engine for this on-demand query, so the block at 443-468 now reads the glob with `read_parquet`, binds
  `$trace_id`, and deploys DuckDB on Lambda, and line 474 names DuckDB.
- Work: implement `queryDistributedTrace` as a DuckDB query on AWS Lambda over the glob at 443, and register
  `get_distributed_trace` (449-463) on the MCP trace-analyzer server.
- Acceptance: `get_distributed_trace(traceId)` returns the rows for that id across all Parquet files that the glob at
  443 selects.
- Decision: keep the spec. The spec's single serverless Lambda query stays and Athena is dropped, because Athena would
  tie the query to one cloud's catalog; decision 14 sets its engine to DuckDB.

### 13. Wrapper flush triggers and the Queues message bound (01s)

- Spec says: `TraceWrapperConfig` has `maxRowsPerChunk`, `maxBytesPerChunk`, and `maxChunkAgeMs`
  (`specs/lmao/01s_cloudflare_fetch_trace_wrapper.md:40-42`). The wrapper flushes when `row_count >= maxRowsPerChunk`,
  `payload_size >= maxBytesPerChunk`, or `chunk_age_ms >= maxChunkAgeMs` (`:109-115`); otherwise it MAY keep the rows in
  a warm isolate (`:117`). 01u lets diagnostic rows accumulate lazily in warm isolates
  (`specs/lmao/01u_cloudflare_trace_segments.md:93-94`), and its Queues cost figure of ~250 k messages for 100 M spans
  (`:130-131`) assumes messages that batch many requests.
- Code does: there is no wrapper, so no trigger exists. `QueuesFallbackOptions.maxRowsPerMessage`
  (`packages/lmao/src/lib/cloudflare/diagnosticDrain.ts:55`) slices one flush into messages (`:97-98`).
  `DiagnosticDrainTracer.flush()` (`:136-151`) drains every queued buffer on each call, with no size or age gate, and
  `PipelinesStreamTransport` sends a flush whole (`:39-42`).
- Options:
  - (a) Adopt the shipped shape: drop the three triggers; the wrapper flushes on every request, and `maxRowsPerMessage`
    is the only bound.
  - (b) Keep the spec: the triggers decide when the wrapper calls `flush()`, and `maxRowsPerMessage` slices one flush
    into Queues messages. The two knobs answer different questions.
- Applied: 01s's request flow drains only when a trigger is met (71-72), the Queues paragraph says the caller sizes
  `maxRowsPerMessage` to stay under 128 KB (106-107), and §Flush Triggers says the triggers decide when to flush and do
  not size messages (119-120).
- Work: the 01s wrapper item checks the three triggers against the tracer's queued buffers after each request, and
  schedules `flush()` through `ctx.waitUntil` only when one is met.
- Acceptance: the wrapper item's acceptance, with `maxRowsPerChunk: 1` and with every trigger above one request's rows.
- Decision: keep the spec. The shipped code has no wrapper to compare against, and its only knob, `maxRowsPerMessage`,
  sizes messages rather than deciding when to flush; flushing on every request would turn 01u's batched Queues messages
  into at least one message per request.

### 14. One query engine per job (02, 01d)

- Spec says: `specs/lmao/02_query_engine.md:5` said "DuckDB is the query engine for lmao traces. It natively reads Arrow
  IPC, runs on AWS Lambda reading S3". `specs/lmao/01d_ai_agent_integration.md:465` said "Deploy chDB on AWS Lambda for
  serverless trace queries", over a ClickHouse-syntax `s3(...)` source (443). 01u places chDB or clickhouse-local on a
  different job: heavy, scheduled reads over the same bucket in a container
  (`specs/lmao/01u_cloudflare_trace_segments.md:363-365`).
- Code does: no Lambda, chDB, or server-side DuckDB code exists. `packages/lmao/crates/lmao-query` is an in-process scan
  over Arrow `RecordBatch`es with no SQL engine (`Cargo.toml:9-11` depends only on `arrow-array` and `arrow-schema`).
  The only shipped SQL engine is DuckDB-WASM in the browser
  (`packages/lmao-inspector/src/engine/query-engine.ts:26-91`).
- Options:
  - (a) DuckDB for every on-demand query (Lambda and browser), and chDB only for the scheduled heavy job. 01d's Lambda
    query moves to DuckDB.
  - (b) chDB for 01d's Lambda query, and DuckDB for 02's Lambda query: two engines for the same on-demand job.
- Applied: `specs/lmao/02_query_engine.md:5-9` scopes DuckDB to on-demand queries on Lambda and in the browser, and
  names the scheduled heavy job on chDB as a separate job. `specs/lmao/01d_ai_agent_integration.md:443-474` reads the
  glob with DuckDB's `read_parquet`, binds `$trace_id` instead of interpolating it, and deploys DuckDB on Lambda.
- Work: covered by the 02 Lambda reader item and decision 12's Work.
- Acceptance: decision 12's acceptance, run on the DuckDB Lambda reader.
- Decision: keep the spec. 01d's serverless lookup is the same on-demand job that 02 assigns to DuckDB, the only shipped
  SQL engine is DuckDB-WASM, and 02 specifies `msgpack_extract()` as a DuckDB extension. chDB keeps the scheduled job,
  which is a different job.
