# Op and Buffer Metrics

## Overview

Op metrics track runtime performance of operations. Buffer metrics track memory efficiency of columnar buffers. Both are
stored as structured log entries in the same Arrow table as trace data.

**Key insight:** Metrics reuse span timestamps, achieving zero extra timing overhead in the hot path.

## Design Evolution: Why This Design

### Problem: No Runtime Visibility

Without metrics, users must query all trace data to calculate invocation counts, error rates, and durations. This is:

- **Expensive**: Scanning millions of trace rows for aggregates
- **Slow**: Real-time dashboards impossible without pre-aggregation
- **Wasteful**: Same calculations repeated on every query

### Key Design Decisions

#### 1. Metrics as Structured Logs (Not Separate Table)

**Decision:** Store metrics as rows in the same Arrow table as trace data.

**Why:**

- Same flush path, same Arrow table, same query tools
- Single schema to maintain
- Unified querying: filter by `entry_type LIKE 'op-%'`

**Alternative rejected:** Separate metrics table requires two flush paths, coordinating schemas, and teaching users two
query patterns.

#### 2. Reuse Span Timestamps for Duration Tracking

**Decision:** Compute duration from existing `span-start` and `span-ok/err` timestamps.

**Why:**

- Spans already capture `span-start` (row 0) and `span-ok/err` (row 1) timestamps
- Op computes `duration = endTs - startTs` from buffer's timestamp column
- Zero extra `performance.now()` or `hrtime.bigint()` calls in hot path

**The math:**

```
startTs = buffer.timestamp[0]  // Written by span-start
endTs = buffer.timestamp[1]    // Written by span-ok or span-err
durationNs = endTs - startTs    // Pure subtraction, no syscall
```

#### 3. Single `uint64_value` Lazy System Column

**Decision:** One lazy `BigUint64Array` column for all numeric values.

**Why:**

- Both metrics (counts, nanoseconds) and users need large integers
- Nanosecond timestamps can exceed `Number.MAX_SAFE_INTEGER` (~9 quadrillion)
- One lazy column serves metrics, user counts, byte sizes, record counts
- Lazy = only allocated when first written (sparse usage is free)

**Alternative rejected:** Multiple specific columns (`invocation_count`, `error_count`, `duration_ns`) wastes space and
limits user extensibility.

#### 4. Specific Entry Types (ENUMs)

**Decision:** Dedicated entry types like `op-invocations`, `op-errors`, `op-duration-total`.

**Why:**

- Self-documenting: `WHERE entry_type = 'op-errors'` is clear
- ENUM storage is 1 byte (Uint8Array with compile-time mapping)
- Fast filtering: integer comparison vs string parsing
- Type safety: invalid entry types caught at compile time

**Alternative rejected:** Generic `count` type + parsing `message` field is harder to query and slower to filter.

#### 5. Duration Breakdown by Outcome

**Decision:** Separate `op-duration-ok` and `op-duration-err` entry types.

**Why:**

- Reveals whether errors are fast-fails or slow timeouts
- Only 3 more entry types, a few more rows per flush
- Enables queries like "are my errors from validation (fast) or network timeouts (slow)?"

**Example insight:**

```
avg_ok_duration: 45ms    -- Normal requests complete quickly
avg_err_duration: 29800ms -- Errors are slow = timeout issue, not validation
```

#### 6. `period-start` as Entry Type (Not Separate Column)

**Decision:** Period start timestamp is stored as a `uint64_value` with entry type `period-start`.

**Why:**

- Period start is just another uint64 value
- Same pattern keeps schema simple (no new columns)
- Easy to query: `WHERE entry_type = 'period-start'`

**Alternative rejected:** Dedicated `period_start_ns` column adds complexity for one value per flush.

#### 7. Grouping by Existing Columns

**Decision:** Metrics inherit grouping from existing columns.

**Why:**

- `timestamp`: Exact nanosecond of flush (period end)
- `thread_id`: Process/worker identity
- `package_name` + `package_path`: Module identity
- No new grouping columns needed

Queries naturally aggregate:

```sql
GROUP BY timestamp, thread_id, package_name
```

## Entry Types for Metrics

**Total: 13 new entry types**

### Period Marker (1)

| Entry Type   | message | uint64_value                           |
| ------------ | ------- | -------------------------------------- |
| period-start | -       | Nanosecond timestamp when period began |

The period ends at `timestamp` (the flush time). Duration = `timestamp - uint64_value`.

### Op Metrics (8)

| Entry Type        | message | uint64_value                       |
| ----------------- | ------- | ---------------------------------- |
| op-invocations    | Op name | Total invocation count             |
| op-errors         | Op name | Count of `span-err` outcomes       |
| op-exceptions     | Op name | Count of `span-exception` outcomes |
| op-duration-total | Op name | Sum of all durations (nanoseconds) |
| op-duration-ok    | Op name | Sum of `span-ok` durations (ns)    |
| op-duration-err   | Op name | Sum of error durations (ns)        |
| op-duration-min   | Op name | Minimum duration (nanoseconds)     |
| op-duration-max   | Op name | Maximum duration (nanoseconds)     |

**Notes:**

- `op-duration-err` includes both `span-err` and `span-exception` outcomes
- `message` contains the Op name for grouping (e.g., "GET", "createUser", "processOrder")

### Buffer Metrics (4)

| Entry Type             | message | uint64_value                        |
| ---------------------- | ------- | ----------------------------------- |
| buffer-writes          | -       | Total entries written to buffers    |
| buffer-overflow-writes | -       | Entries written to overflow buffers |
| buffer-created         | -       | Number of SpanBuffers allocated     |
| buffer-overflows       | -       | Times a buffer overflowed           |

**Notes:**

- Buffer metrics are per-module (identified by `package_name`)
- High `buffer-overflows` suggests initial capacity tuning needed

## Op Class Metrics Tracking

The `Op` class maintains metrics counters that reset on each flush. Type parameters are ordered to match the function
signature `(ctx, ...args) => Promise<Result>`:

```typescript
class Op<Ctx, Args extends unknown[], Result> {
  readonly fn: (ctx: Ctx, ...args: Args) => Promise<Result>;
  readonly module: Module;
  readonly definitionLine?: number;

  // Metrics (reset on flush)
  private invocationCount: number = 0;
  private errorCount: number = 0; // span-err outcomes
  private exceptionCount: number = 0; // span-exception outcomes

  // Duration tracking (BigInt for nanosecond precision)
  private totalDurationNs: bigint = 0n;
  private okDurationNs: bigint = 0n;
  private errDurationNs: bigint = 0n; // span-err + span-exception
  private minDurationNs: bigint = BigInt(Number.MAX_SAFE_INTEGER);
  private maxDurationNs: bigint = 0n;

  // Period tracking
  private periodStartNs: bigint = hrtime.bigint();
}
```

**Why BigInt for durations:**

- Nanosecond precision over long periods can exceed `MAX_SAFE_INTEGER`
- 9,007,199,254,740,991 ns = ~104 days
- Summing many durations compounds quickly

## Collecting Metrics (Inside Op's Invoke)

```typescript
async invoke(parentCtx, spanName, line, ...args) {
  this.invocationCount++;

  // Buffer creation writes span-start timestamp to row 0
  const buffer = createSpanBuffer(...);
  const startTs = buffer.timestamp[0];  // Already captured!

  try {
    const result = await this.fn(ctx, ...args);

    // span-ok writes end timestamp to row 1
    const endTs = buffer.timestamp[1];  // Already captured!
    const durationNs = BigInt(endTs - startTs);

    this.totalDurationNs += durationNs;
    this.okDurationNs += durationNs;
    if (durationNs < this.minDurationNs) this.minDurationNs = durationNs;
    if (durationNs > this.maxDurationNs) this.maxDurationNs = durationNs;

    return result;
  } catch (e) {
    this.exceptionCount++;  // or errorCount for expected errors

    const endTs = buffer.timestamp[1];
    const durationNs = BigInt(endTs - startTs);

    this.totalDurationNs += durationNs;
    this.errDurationNs += durationNs;
    if (durationNs < this.minDurationNs) this.minDurationNs = durationNs;
    if (durationNs > this.maxDurationNs) this.maxDurationNs = durationNs;

    throw e;
  }
}
```

**Key insight:** No extra timing calls. We reuse `buffer.timestamp[0]` (written by `span-start`) and
`buffer.timestamp[1]` (written by `span-ok/err`).

## Metrics Flush

### When

Metrics flush alongside trace data using the same `FlushScheduler` triggers:

- Capacity threshold (80% full)
- Time threshold (10s max, 1s min intervals)
- Idle timeout (5s of inactivity)
- Manual `flush()` call

### Process

1. Capture current timestamp as `period_end` (this becomes `timestamp` for all metric rows)
2. For each module: a. Write `period-start` row with `periodStartNs` as `uint64_value` b. For each Op in module:
   - Write `op-invocations` row
   - Write `op-errors` row (if > 0)
   - Write `op-exceptions` row (if > 0)
   - Write `op-duration-total` row
   - Write `op-duration-ok` row (if > 0)
   - Write `op-duration-err` row (if > 0)
   - Write `op-duration-min` row
   - Write `op-duration-max` row c. Write `buffer-writes` row d. Write `buffer-overflow-writes` row (if > 0) e. Write
     `buffer-created` row f. Write `buffer-overflows` row (if > 0)
3. Reset all counters to zero
4. Update `periodStartNs` to current timestamp (start of next period)

### Example Flush Output

```
timestamp | thread_id | package_name | entry_type             | message | uint64_value
----------|-----------|--------------|------------------------|---------|---------------
1000      | 1         | @myco/http   | period-start           |         | 0
1000      | 1         | @myco/http   | op-invocations         | GET     | 1523
1000      | 1         | @myco/http   | op-errors              | GET     | 8
1000      | 1         | @myco/http   | op-exceptions          | GET     | 4
1000      | 1         | @myco/http   | op-duration-total      | GET     | 68535000000
1000      | 1         | @myco/http   | op-duration-ok         | GET     | 65000000000
1000      | 1         | @myco/http   | op-duration-err        | GET     | 3535000000
1000      | 1         | @myco/http   | op-duration-min        | GET     | 2100000
1000      | 1         | @myco/http   | op-duration-max        | GET     | 1203500000
1000      | 1         | @myco/http   | op-invocations         | POST    | 892
1000      | 1         | @myco/http   | op-errors              | POST    | 2
1000      | 1         | @myco/http   | op-duration-total      | POST    | 44600000000
1000      | 1         | @myco/http   | op-duration-ok         | POST    | 44100000000
1000      | 1         | @myco/http   | op-duration-err        | POST    | 500000000
1000      | 1         | @myco/http   | op-duration-min        | POST    | 8500000
1000      | 1         | @myco/http   | op-duration-max        | POST     | 892000000
1000      | 1         | @myco/http   | buffer-writes          |         | 50000
1000      | 1         | @myco/http   | buffer-overflow-writes |         | 1200
1000      | 1         | @myco/http   | buffer-created         |         | 47
1000      | 1         | @myco/http   | buffer-overflows       |         | 3
```

## User-Facing API

Users can write `uint64_value` for their own purposes using the `.uint64()` method:

```typescript
const processRecords = op(async ({ log, tag, ok }, records) => {
  // Tag the span with a large count
  tag.batchId(batchId).uint64(BigInt(recordCount));

  // Log with a large byte count
  log.info('Processing complete').uint64(BigInt(bytesProcessed));

  // Return with a large total
  return ok({ success: true }).uint64(BigInt(totalRecords));
});
```

**Why expose this:**

- Users have large numbers too (byte counts, record counts, IDs)
- Same column, same storage efficiency
- Consistent API: `.uint64(value)` works on `tag`, `log`, and result methods

## Query Examples (ClickHouse)

### Op Performance Summary

Get a complete picture of each operation's performance:

```sql
SELECT
  package_name,
  message as op_name,
  anyIf(uint64_value, entry_type = 'op-invocations') as invocations,
  anyIf(uint64_value, entry_type = 'op-errors') as errors,
  anyIf(uint64_value, entry_type = 'op-exceptions') as exceptions,
  -- Error rate (errors + exceptions / total)
  (anyIf(uint64_value, entry_type = 'op-errors') +
   anyIf(uint64_value, entry_type = 'op-exceptions')) /
    anyIf(uint64_value, entry_type = 'op-invocations') as error_rate,
  -- Average duration (total / invocations, converted to ms)
  anyIf(uint64_value, entry_type = 'op-duration-total') /
    anyIf(uint64_value, entry_type = 'op-invocations') / 1e6 as avg_duration_ms,
  anyIf(uint64_value, entry_type = 'op-duration-min') / 1e6 as min_duration_ms,
  anyIf(uint64_value, entry_type = 'op-duration-max') / 1e6 as max_duration_ms
FROM traces
WHERE entry_type LIKE 'op-%'
GROUP BY timestamp, package_name, message
ORDER BY invocations DESC
```

### Error Duration Analysis

Determine if errors are fast-fails (validation) or slow timeouts:

```sql
SELECT
  message as op_name,
  -- Average duration for successful calls
  anyIf(uint64_value, entry_type = 'op-duration-ok') /
    NULLIF(
      anyIf(uint64_value, entry_type = 'op-invocations') -
      anyIf(uint64_value, entry_type = 'op-errors') -
      anyIf(uint64_value, entry_type = 'op-exceptions'),
      0
    ) / 1e6 as avg_ok_duration_ms,
  -- Average duration for failed calls
  anyIf(uint64_value, entry_type = 'op-duration-err') /
    NULLIF(
      anyIf(uint64_value, entry_type = 'op-errors') +
      anyIf(uint64_value, entry_type = 'op-exceptions'),
      0
    ) / 1e6 as avg_err_duration_ms
FROM traces
WHERE entry_type IN (
  'op-duration-ok', 'op-duration-err',
  'op-invocations', 'op-errors', 'op-exceptions'
)
GROUP BY timestamp, message
-- If avg_err_duration >> avg_ok_duration: timeout issue
-- If avg_err_duration << avg_ok_duration: fast validation failures
```

### Buffer Health

Monitor buffer efficiency and detect capacity issues:

```sql
SELECT
  package_name,
  anyIf(uint64_value, entry_type = 'buffer-writes') as total_writes,
  anyIf(uint64_value, entry_type = 'buffer-overflow-writes') as overflow_writes,
  -- Overflow rate: high = needs larger initial capacity
  anyIf(uint64_value, entry_type = 'buffer-overflow-writes') /
    NULLIF(anyIf(uint64_value, entry_type = 'buffer-writes'), 0) as overflow_rate,
  anyIf(uint64_value, entry_type = 'buffer-created') as buffers_created,
  anyIf(uint64_value, entry_type = 'buffer-overflows') as overflow_count
FROM traces
WHERE entry_type LIKE 'buffer-%'
GROUP BY timestamp, package_name
ORDER BY overflow_rate DESC
```

### Invocation Rate (Throughput)

Calculate operations per second:

```sql
SELECT
  package_name,
  message as op_name,
  anyIf(uint64_value, entry_type = 'op-invocations') as invocations,
  -- Period duration in seconds
  (max(timestamp) - anyIf(uint64_value, entry_type = 'period-start')) / 1e9 as period_seconds,
  -- Throughput
  anyIf(uint64_value, entry_type = 'op-invocations') /
    NULLIF((max(timestamp) - anyIf(uint64_value, entry_type = 'period-start')) / 1e9, 0)
    as invocations_per_sec
FROM traces
WHERE entry_type IN ('op-invocations', 'period-start')
GROUP BY timestamp, package_name, message
ORDER BY invocations_per_sec DESC
```

### Latency Distribution Over Time

Track p50/p90/p99 approximations across periods:

```sql
SELECT
  toStartOfMinute(fromUnixTimestamp64Nano(timestamp)) as minute,
  package_name,
  message as op_name,
  avg(anyIf(uint64_value, entry_type = 'op-duration-total') /
      anyIf(uint64_value, entry_type = 'op-invocations')) / 1e6 as avg_duration_ms,
  avg(anyIf(uint64_value, entry_type = 'op-duration-min')) / 1e6 as min_duration_ms,
  avg(anyIf(uint64_value, entry_type = 'op-duration-max')) / 1e6 as max_duration_ms
FROM traces
WHERE entry_type LIKE 'op-duration-%' OR entry_type = 'op-invocations'
GROUP BY minute, package_name, message
ORDER BY minute, package_name, op_name
```

## Integration with Other Specs

| Spec                                                                                   | Integration                                        |
| -------------------------------------------------------------------------------------- | -------------------------------------------------- |
| [01h_entry_types_and_logging_primitives.md](01h_entry_types_and_logging_primitives.md) | Defines the 13 new entry types in the unified enum |
| [01f_arrow_table_structure.md](01f_arrow_table_structure.md)                           | Documents `uint64_value` as a lazy system column   |
| [01l_op_context_pattern.md](01l_op_context_pattern.md)                                 | Op class gains metrics properties and flush method |
| [01b2_buffer_self_tuning.md](01b2_buffer_self_tuning.md)                               | Buffer stats feed into `buffer-*` entry types      |
| [01g_trace_context_api_codegen.md](01g_trace_context_api_codegen.md)                   | Generates `.uint64()` method on SpanLogger         |
| [01a_trace_schema_system.md](01a_trace_schema_system.md)                               | Entry type enum extended with metric types         |

## Summary

Op and Buffer metrics provide runtime visibility with zero timing overhead by reusing span timestamps. The design stores
metrics as structured log entries (not a separate table), uses a single `uint64_value` column for all numeric needs, and
leverages specific entry types for efficient querying.

**Key benefits:**

- Zero extra timing syscalls (reuse span timestamps)
- Same flush path as trace data
- Rich insights (duration by outcome reveals timeout vs validation failures)
- User-extensible (`.uint64()` method for custom large numbers)
- Efficient storage (ENUM entry types, lazy uint64 column)
