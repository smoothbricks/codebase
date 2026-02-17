# Query Engine

## Overview

DuckDB is the most viable query engine for lmao traces. It natively reads Arrow IPC, runs on AWS Lambda reading S3, and
has a straightforward extension system that makes a custom `msgpack_extract()` function feasible.

This document covers:

1. **Why DuckDB** — comparison with alternatives
2. **Deployment model** — Lambda + S3 for serverless trace analysis
3. **Arrow IPC integration** — native zero-copy reads
4. **msgpack_extract() extension** — custom function for querying `S.unknown()` Binary columns
5. **Query patterns** — common trace analysis queries leveraging the extension

## Why DuckDB

### SQL Engine Comparison for Binary Column Queryability

lmao's `S.unknown()` columns store values as msgpack-encoded Arrow Binary columns. No major SQL engine has native
msgpack extraction:

| Engine     | `json_extract()` | `msgpack_extract()` | Arrow IPC Native | Extension System       |
| ---------- | ---------------- | ------------------- | ---------------- | ---------------------- |
| DuckDB     | Yes              | No (build it)       | Yes              | C API, ~17KB, easy     |
| ClickHouse | Yes              | No                  | Limited          | C++, harder to extend  |
| BigQuery   | Yes              | No                  | No               | No custom extensions   |
| Snowflake  | Yes              | No                  | No               | No custom extensions   |
| Databricks | Yes              | No                  | Via Spark        | JVM-based, heavyweight |

### Why NOT JSON for S.unknown()

Using JSON (Utf8 column) instead of msgpack (Binary column) was considered for universal `json_extract()` compatibility.
Rejected because:

- **Precision loss**: BigInt → number (loses precision above 2^53), Date → string (loses type), undefined vs null
  conflation, NaN/Infinity → null
- **Size**: JSON is ~2-4x larger than msgpack for typical payloads
- **Encoding cost**: JSON.stringify is slower than msgpack encoding on the Zig hot path
- **DuckDB extension is small**: The custom function is a bounded, well-scoped piece of work

### Why DuckDB Specifically

- **Arrow IPC native**: Reads Arrow IPC files directly with zero-copy, no format conversion needed — lmao already
  produces Arrow IPC
- **Embeddable**: Single binary, no server process, runs in-process or as Lambda function
- **Extension system**: C API with stable ABI, template repository for scaffolding, extensions compile to ~17KB
  binaries, version-stable across DuckDB releases
- **S3 native**: Built-in `httpfs` extension reads directly from S3 (`read_parquet('s3://bucket/path')`,
  `read_ipc('s3://...')`) — no intermediate download step
- **Lambda compatible**: Multiple proven deployment options (see below)

## Deployment: Lambda + S3

### Architecture

```
[lmao flush] → Arrow IPC → S3 bucket
                               ↓
                    [DuckDB on Lambda] ← query request
                               ↓
                         query results
```

Traces are flushed as Arrow IPC files to S3. DuckDB on Lambda reads them directly via `httpfs` for on-demand analysis.

### Proven Lambda Deployments

Multiple open-source projects demonstrate DuckDB running on AWS Lambda:

- **serverless-duckdb** — pre-built Lambda package with DuckDB, includes S3 integration
- **duck-query-lambda** — Lambda function specifically for running DuckDB queries against S3 data
- **quack-reduce** — MapReduce-style queries using DuckDB across Lambda invocations
- **Pre-built Lambda layers** — DuckDB compiled for Amazon Linux 2, ready to attach as Lambda layer

### S3 Reading

DuckDB's `httpfs` extension handles S3 reads natively:

```sql
-- Read Arrow IPC from S3
SELECT * FROM read_ipc('s3://my-traces/2026/02/15/*.arrow');

-- Glob patterns for time-range queries
SELECT * FROM read_ipc('s3://my-traces/2026/02/**/*.arrow')
WHERE timestamp > '2026-02-15T00:00:00'::timestamp;

-- Combine with Parquet (if traces are also exported as Parquet)
SELECT * FROM read_parquet('s3://my-traces/2026/02/15/*.parquet');
```

## Arrow IPC Integration

lmao already produces Arrow IPC via its flush path. DuckDB reads Arrow IPC natively — no conversion step needed.

```sql
-- Local file
SELECT * FROM read_ipc('/tmp/traces/batch-001.arrow');

-- S3
SELECT * FROM read_ipc('s3://traces/2026-02-15/batch-001.arrow');

-- Multiple files with glob
SELECT * FROM read_ipc('s3://traces/2026-02-15/*.arrow');
```

DuckDB's Arrow reader uses zero-copy for primitive columns (Float64, Int32, timestamps, etc.) and dictionary columns.
Binary columns (where msgpack data lives) are read as `BLOB` type.

## msgpack_extract() Extension

### Purpose

Query msgpack-encoded values inside Arrow Binary columns without deserializing the entire payload. This is what makes
`S.unknown()` queryable despite using msgpack instead of JSON.

### Target API

```sql
-- Extract a scalar value by path
SELECT msgpack_extract(payload, '$.userId') AS user_id
FROM traces
WHERE entry_type = 'info';

-- Extract nested values
SELECT msgpack_extract(payload, '$.response.status') AS status
FROM traces;

-- Type-specific extraction (like json_extract_string, json_extract_int, etc.)
SELECT msgpack_extract_string(payload, '$.name') AS name,
       msgpack_extract_int(payload, '$.count') AS count
FROM traces;

-- Filter on msgpack values
SELECT *
FROM traces
WHERE msgpack_extract_int(payload, '$.statusCode') >= 400;
```

### Implementation Approach

DuckDB extensions use the C API (`duckdb.h`):

- **Template**: DuckDB provides an [extension template repository](https://github.com/duckdb/extension-template) with
  build scaffolding, CI, and test infrastructure
- **Registration**: Extensions register scalar functions via `duckdb_create_scalar_function()`
- **Binary size**: Typical extensions compile to ~17KB
- **ABI stability**: The C API is version-stable — extensions don't break across DuckDB releases

The extension needs:

1. A msgpack decoder (C library, e.g. `msgpack-c` or minimal hand-rolled decoder for the subset of types lmao produces)
2. JSONPath-like path navigation over msgpack bytes
3. Type coercion functions (extract as VARCHAR, INTEGER, DOUBLE, BOOLEAN, BLOB)

### Scope

The extension only needs to handle the msgpack subset that lmao's Zig encoder produces:

- nil, boolean, integer (positive/negative fixint, uint8-64, int8-64), float32, float64
- string (fixstr, str8-32), binary (bin8-32)
- array (fixarray, array16-32), map (fixmap, map16-32)
- Extension type for "unserializable value" sentinel

This is a well-bounded subset — no need for a general-purpose msgpack library.

## Query Patterns

### Querying S.unknown() Columns

```sql
-- Traces with unknown payload columns
SELECT
  timestamp,
  message,
  msgpack_extract_string(request_body, '$.method') AS rpc_method,
  msgpack_extract(request_body, '$.params') AS params
FROM traces
WHERE entry_type = 'info'
  AND msgpack_extract_string(request_body, '$.method') = 'getUserProfile';

-- Aggregate over msgpack values
SELECT
  msgpack_extract_string(event_data, '$.eventType') AS event_type,
  count(*) AS occurrences,
  avg(msgpack_extract_int(event_data, '$.durationMs')) AS avg_duration
FROM traces
WHERE event_data IS NOT NULL
GROUP BY event_type
ORDER BY occurrences DESC;
```

### Combining with Standard Columns

The `S.unknown()` binary columns coexist with standard typed columns. Queries can mix both:

```sql
SELECT
  trace_id,
  message AS span_name,
  http_status,
  http_duration,
  msgpack_extract_string(custom_payload, '$.correlationId') AS correlation_id
FROM traces
WHERE package_name = '@mycompany/api-gateway'
  AND http_status >= 500
  AND msgpack_extract_string(custom_payload, '$.correlationId') IS NOT NULL;
```

## Future Considerations

- **Parquet export**: Arrow IPC files on S3 could be periodically compacted into Parquet for long-term storage with
  better compression. DuckDB reads both formats natively.
- **ClickHouse integration**: If ClickHouse is used for real-time dashboards, msgpack Binary columns can be loaded as
  opaque BLOBs. ClickHouse's MsgPack row format could potentially be leveraged for batch decoding, but column-level
  extraction would still need the DuckDB extension for ad-hoc queries.
- **WASM DuckDB**: DuckDB compiles to WASM — the msgpack_extract() extension could run in-browser for local trace
  analysis without a server.
