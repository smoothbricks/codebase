# Arrow Table Structure

## Overview

The Arrow Table Structure defines the final queryable format produced by the trace logging system. It provides:

1. **Clean column schema** optimized for analytical queries
2. **Realistic trace data examples** showing spans, tags, and console.log compatibility
3. **ClickHouse query patterns** for common analytical use cases
4. **Performance characteristics** of the columnar format

## Design Philosophy

**Key Insight**: The Arrow table structure must balance query performance with data completeness. Every entry in the
system becomes a row in the final table, enabling rich analytical queries while maintaining efficient storage.

**Core Principles**:

- **Flat structure**: All data flattened to a single table for maximum query flexibility
- **Nullable columns**: Sparse data handled efficiently with null values
- **Dictionary encoding**: Repeated strings stored efficiently
- **Type optimization**: Appropriate data types for storage and performance

## Column Schema

### Core System Columns (Always Present)

| Column Name      | Type                 | Description                                                                  | Example Values                                                                                                                                |
| ---------------- | -------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `timestamp`      | `timestamp[μs]`      | When event occurred (microseconds since epoch, sub-ms precision from anchor) | `2024-01-01T10:00:00.000123Z`                                                                                                                 |
| `trace_id`       | `dictionary<string>` | Root trace identifier                                                        | `'req-abc123'`, `'X-Request-Id-456'`                                                                                                          |
| `span_id`        | `uint64`             | Span identifier                                                              | `1`, `2`, `3`                                                                                                                                 |
| `parent_span_id` | `uint64`             | Parent span (nullable)                                                       | `1` or `null`                                                                                                                                 |
| `entry_type`     | `dictionary<string>` | Log entry type                                                               | `'span-start'`, `'span-ok'`, `'span-err'`, `'span-exception'`, `'tag'`, `'info'`, `'debug'`, `'warn'`, `'error'`, `'ff-access'`, `'ff-usage'` |
| `module`         | `dictionary<string>` | Module name                                                                  | `'UserController'`, `'DatabaseService'`                                                                                                       |
| `label`          | `dictionary<string>` | Span name OR log message template (see Label Column section)                 | `'create-user'`, `'User ${userId} created'`, `'Processing ${count} items'`                                                                    |

### Library-Specific Attribute Columns (Sparse/Nullable)

| Column Name       | Type                 | Description                               | Example Values                                   |
| ----------------- | -------------------- | ----------------------------------------- | ------------------------------------------------ |
| `http_status`     | `uint16`             | HTTP status code (nullable)               | `200`, `404`, `500` or `null`                    |
| `http_method`     | `dictionary<string>` | HTTP method (nullable)                    | `'GET'`, `'POST'`, `'PUT'` or `null`             |
| `http_url`        | `string`             | Masked URL (nullable)                     | `'https://api.*****.com/users'` or `null`        |
| `http_duration`   | `float32`            | HTTP request duration ms (nullable)       | `125.5` or `null`                                |
| `db_query`        | `string`             | Masked SQL query (nullable)               | `'SELECT * FROM users WHERE id = ?'` or `null`   |
| `db_duration`     | `float32`            | Query duration ms (nullable)              | `12.3` or `null`                                 |
| `db_rows`         | `uint32`             | Rows affected/returned (nullable)         | `1`, `0`, `1000` or `null`                       |
| `db_table`        | `dictionary<string>` | Table name (nullable)                     | `'users'`, `'orders'` or `null`                  |
| `user_id`         | `binary[8]`          | Hashed user ID (nullable)                 | `0x8a7b6c5d...` or `null`                        |
| `business_metric` | `float64`            | Custom metric value (nullable)            | `42.7`, `1.0` or `null`                          |
| `ff_value`        | `dictionary<string>` | Feature flag value (nullable, S.category) | `'true'`, `'false'`, `'blue'`, `'100'` or `null` |

**Note**: Feature flag names are stored in the unified `label` column for `ff-access` and `ff-usage` entries. The
`ff_value` column uses `S.category()` (dictionary encoding) because flag values repeat frequently (e.g., `true`/`false`,
`'blue'`/`'green'`/`'red'`, etc.).

## Complete Trace Example: User Registration Flow

This example shows a complete user registration request with multiple spans, HTTP calls, database operations, and
console.log compatibility traces.

| trace_id     | span_id | parent_span_id | timestamp                  | entry_type   | module                | label                                           | http_status | http_method | http_url                               | http_duration | db_query                                                                | db_duration | db_rows | db_table | user_id         | business_metric | ff_value |
| ------------ | ------- | -------------- | -------------------------- | ------------ | --------------------- | ----------------------------------------------- | ----------- | ----------- | -------------------------------------- | ------------- | ----------------------------------------------------------------------- | ----------- | ------- | -------- | --------------- | --------------- | -------- |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.000Z` | `span-start` | `UserController`      | `register-user`                                 | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.002Z` | `ff-access`  | `UserController`      | `advancedValidation`                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | `true`   |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.005Z` | `info`       | `UserController`      | `Starting registration for ${userId}`           | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.010Z` | `span-start` | `ValidationService`   | `validate-email`                                | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.015Z` | `tag`        | `ValidationService`   | `validate-email`                                | 200         | `POST`      | `https://api.*****.com/validate-email` | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.045Z` | `tag`        | `ValidationService`   | `validate-email`                                | 200         | `POST`      | `https://api.*****.com/validate-email` | 30.2          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.046Z` | `span-ok`    | `ValidationService`   | `validate-email`                                | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.050Z` | `span-start` | `UserRepository`      | `check-user-exists`                             | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.052Z` | `tag`        | `UserRepository`      | `check-user-exists`                             | null        | null        | null                                   | null          | `SELECT id FROM users WHERE email = ?`                                  | null        | null    | `users`  | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.067Z` | `tag`        | `UserRepository`      | `check-user-exists`                             | null        | null        | null                                   | null          | `SELECT id FROM users WHERE email = ?`                                  | 15.3        | 0       | `users`  | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.068Z` | `debug`      | `UserRepository`      | `User does not exist, proceeding with creation` | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.069Z` | `span-ok`    | `UserRepository`      | `check-user-exists`                             | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.070Z` | `span-start` | `UserRepository`      | `create-user`                                   | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.072Z` | `tag`        | `UserRepository`      | `create-user`                                   | null        | null        | null                                   | null          | `INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)` | null        | null    | `users`  | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.095Z` | `tag`        | `UserRepository`      | `create-user`                                   | null        | null        | null                                   | null          | `INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)` | 23.1        | 1       | `users`  | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.096Z` | `span-ok`    | `UserRepository`      | `create-user`                                   | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.100Z` | `span-start` | `NotificationService` | `send-welcome-email`                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.105Z` | `tag`        | `NotificationService` | `send-welcome-email`                            | 202         | `POST`      | `https://email.*****.com/send`         | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.245Z` | `tag`        | `NotificationService` | `send-welcome-email`                            | 202         | `POST`      | `https://email.*****.com/send`         | 140.3         | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.246Z` | `span-ok`    | `NotificationService` | `send-welcome-email`                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.250Z` | `tag`        | `UserController`      | `register-user`                                 | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | 1.0             | null     |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.251Z` | `info`       | `UserController`      | `Registration completed for ${userId}`          | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.252Z` | `span-ok`    | `UserController`      | `register-user`                                 | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null     |

## The `label` System Column

### Unified Purpose

The `label` column serves different purposes based on entry type:

- **For span entries** (`span-start`, `span-ok`, `span-err`, `span-exception`, `tag`): Contains the **span name**
- **For log entries** (`info`, `debug`, `warn`, `error`): Contains the **message template**
- **For feature flag entries** (`ff-access`, `ff-usage`): Contains the **flag name**

| Entry Type                                                   | `label` Contains                                        |
| ------------------------------------------------------------ | ------------------------------------------------------- |
| `span-start`, `span-ok`, `span-err`, `span-exception`, `tag` | Span name (e.g., `'create-user'`)                       |
| `info`, `debug`, `warn`, `error`                             | Log message template (e.g., `'User ${userId} created'`) |
| `ff-access`, `ff-usage`                                      | Flag name (e.g., `'advancedValidation'`, `'darkMode'`)  |

### Format String Pattern (CRITICAL)

**Log messages use FORMAT STRINGS, not interpolated strings.**

When you write:

```typescript
ctx.log.info('User ${userId} created with ${itemCount} items').userId(123).itemCount(5);
```

The system stores:

| Column           | Value                                              |
| ---------------- | -------------------------------------------------- |
| `label`          | `'User ${userId} created with ${itemCount} items'` |
| `attr_userId`    | `123`                                              |
| `attr_itemCount` | `5`                                                |

**The message is NOT interpolated.** The template string `'User ${userId} created...'` is stored verbatim in the `label`
column, while the actual values (`123`, `5`) are stored in their respective typed attribute columns.

### Why This Design?

**1. Efficient Storage via String Interning (S.category)**

```typescript
// In systemSchema:
label: S.category(),  // Span name OR log message template
```

The `label` column uses `S.category()` type, which means:

- Templates are **string-interned** - each unique template stored once
- Repeated log messages (even with different values) share the same interned template
- Much more efficient than storing `"User 123 created"`, `"User 456 created"`, `"User 789 created"` as separate strings

**2. Better Analytics Through Template Grouping**

Because templates are stored separately from values, you can:

```sql
-- Find all occurrences of a specific log pattern
SELECT * FROM traces WHERE label = 'User ${userId} created with ${itemCount} items';

-- Group by log template to find most frequent messages
SELECT label, count(*) as occurrences
FROM traces
WHERE entry_type IN ('info', 'debug', 'warn', 'error')
GROUP BY label
ORDER BY occurrences DESC;

-- Analyze specific template with different values
SELECT attr_userId, attr_itemCount, timestamp
FROM traces
WHERE label = 'User ${userId} created with ${itemCount} items'
ORDER BY timestamp;
```

**3. Simpler Schema - One Column for Multiple Purposes**

Instead of separate `span_name`, `message`, and `ffName` columns (most always null), we have:

- Single `label` column that's always populated for relevant entry types
- Reduces schema complexity
- Better column utilization (less sparsity)
- Consistent pattern across all entry types

### Example Data

| entry_type   | label                                              | attr_userId | attr_itemCount |
| ------------ | -------------------------------------------------- | ----------- | -------------- |
| `span-start` | `'create-user'`                                    | `123`       | `null`         |
| `info`       | `'User ${userId} created with ${itemCount} items'` | `123`       | `5`            |
| `debug`      | `'Processing batch for ${userId}'`                 | `123`       | `null`         |
| `span-ok`    | `'create-user'`                                    | `123`       | `null`         |

### Contrast with Traditional Logging

**Traditional (interpolated strings):**

```typescript
console.log(`User ${userId} created with ${itemCount} items`);
// Stores: "User 123 created with 5 items" - unique string, no structure
```

**LMAO (format strings):**

```typescript
ctx.log.info('User ${userId} created with ${itemCount} items').userId(123).itemCount(5);
// Stores: template in label, values in typed columns - structured, queryable
```

## Key Patterns in the Data

### 1. Span Hierarchy & Lifecycle

- **Root span** (span_id=1): `register-user` with no parent
- **Child spans** (span_id=2,3,4,5): All have parent_span_id=1
- **Span lifecycle**: `span-start` → entries → `span-ok`/`span-err`/`span-exception`
- **Success/failure tracking**: `span-ok` vs `span-err` vs `span-exception` captures span outcome without extra columns

### 2. Structured Logging via Entry Type Enum

- **Log levels with structure**: `ctx.log.info('Template ${var}').var(value)` → `entry_type='info'` with typed
  attributes
- **Template storage**: Log message TEMPLATES stored in unified `label` column (NOT interpolated strings)
- **Values in attribute columns**: Actual values stored separately in `attr_*` columns for type safety and queryability
- **Optional attributes**: Structured data can accompany log messages
- **Gradual migration**: Familiar log levels but with structured data instead of string concatenation

### 3. Entry Type System

The `entry_type` column uses a dictionary-encoded enum that covers all possible trace events. For complete definitions
and low-level API details, see **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**.

**Entry Types**: `span-start`, `span-ok`, `span-err`, `span-exception`, `tag`, `info`, `debug`, `warn`, `error`,
`ff-access`, `ff-usage`

**Key Benefits**:

- **Dictionary encoding**: Entry type strings stored once, referenced by index
- **Minimal overhead**: Adding new entry types costs almost nothing in Arrow
- **Unified system**: All trace events use the same enum instead of separate columns

### 4. Library Integration

- **HTTP entries**: Multiple rows for single request (start tag, end tag with duration)
- **Database entries**: Query logged, then duration and row count added
- **Attribute isolation**: Each library's attributes are cleanly separated in dedicated columns

### 5. Feature Flag Integration via Entry Type Enum

- **Flag evaluation**: `ff-access` entry types capture when flags are checked
- **Usage tracking**: `ff-usage` entry types capture when flag-gated features are used
- **Context via attributes**: Flag evaluation context stored in regular attribute columns (user_id, user_plan, etc.)
- **Type safety**: Feature flag context uses same typed attribute system as other entry types
- **Query efficiency**: No JSON parsing needed - direct column access for flag context

### 6. Sparse Data Efficiency

- **Core columns always present**: 8 system columns in every row
- **Attribute columns sparse**: Library-specific columns mostly null
- **Efficient storage**: Arrow's null bitmap handles sparsity with minimal overhead
- **Targeted information**: Each row contains only relevant attributes

## ClickHouse Query Examples

### Request Performance Analysis

```sql
-- Average request duration by endpoint
SELECT
  trace_id,
  max(timestamp) - min(timestamp) as total_duration_ms,
  count(*) as total_entries,
  count(CASE WHEN entry_type = 'span-start' THEN 1 END) as span_count
FROM traces
WHERE module = 'UserController'
  AND span_name = 'register-user'
GROUP BY trace_id
ORDER BY total_duration_ms DESC
LIMIT 10;
```

### Database Performance Monitoring

```sql
-- Slow database queries
SELECT
  db_table,
  db_query,
  avg(db_duration) as avg_duration,
  max(db_duration) as max_duration,
  count(*) as query_count
FROM traces
WHERE db_duration IS NOT NULL
  AND db_duration > 100  -- Queries slower than 100ms
GROUP BY db_table, db_query
ORDER BY avg_duration DESC;
```

### HTTP Error Analysis

```sql
-- HTTP error rates by service
SELECT
  module,
  span_name,
  count(*) as total_requests,
  count(CASE WHEN http_status >= 400 THEN 1 END) as error_count,
  (error_count / total_requests) * 100 as error_rate_percent
FROM traces
WHERE http_status IS NOT NULL
GROUP BY module, span_name
HAVING total_requests > 100  -- Only services with significant traffic
ORDER BY error_rate_percent DESC;
```

### User Journey Analysis

```sql
-- Trace user journey through registration flow
SELECT
  span_id,
  parent_span_id,
  module,
  label,
  entry_type,
  timestamp,
  CASE
    WHEN entry_type IN ('info', 'debug', 'warn', 'error') THEN label  -- Log template
    WHEN http_url IS NOT NULL THEN concat('HTTP ', http_method, ' ', http_url)
    WHEN db_query IS NOT NULL THEN concat('DB: ', db_table)
    ELSE label  -- Span name
  END as description
FROM traces
WHERE trace_id = 'req-abc123'
ORDER BY timestamp;
```

### Structured Logging Analysis

```sql
-- Analyze structured logging usage patterns
-- Note: label contains MESSAGE TEMPLATES, not interpolated strings
SELECT
  module,
  entry_type as log_level,
  count(*) as message_count,
  count(DISTINCT trace_id) as trace_count,
  count(DISTINCT label) as unique_templates,  -- Templates, not interpolated messages!
  count(CASE WHEN http_status IS NOT NULL OR user_id IS NOT NULL THEN 1 END) as structured_entries
FROM traces
WHERE entry_type IN ('info', 'debug', 'warn', 'error')
GROUP BY module, entry_type
ORDER BY message_count DESC;

-- Find most common log message templates
SELECT
  label as template,
  count(*) as occurrences,
  count(DISTINCT trace_id) as unique_traces
FROM traces
WHERE entry_type IN ('info', 'debug', 'warn', 'error')
GROUP BY label
ORDER BY occurrences DESC
LIMIT 20;
```

### Feature Flag Analysis

```sql
-- Feature flag usage and performance impact
-- Note: flag name is in the unified `label` column, value in `ff_value`
SELECT
  label as flag_name,
  ff_value,
  count(*) as access_count,
  count(DISTINCT trace_id) as trace_count,
  avg(CASE WHEN ff_value = 'true' THEN 1.0 ELSE 0.0 END) as enabled_ratio
FROM traces
WHERE entry_type = 'ff-access'
GROUP BY label, ff_value
ORDER BY access_count DESC;

-- Correlation between feature flags and request performance by user plan
WITH flag_traces AS (
  SELECT DISTINCT trace_id, label as flag_name, ff_value, user_id
  FROM traces
  WHERE entry_type = 'ff-access' AND label = 'advancedValidation'
),
user_plans AS (
  SELECT DISTINCT trace_id, user_plan
  FROM traces
  WHERE user_plan IS NOT NULL
),
trace_performance AS (
  SELECT
    trace_id,
    max(timestamp) - min(timestamp) as duration_ms
  FROM traces
  GROUP BY trace_id
)
SELECT
  ft.ff_value,
  up.user_plan,
  avg(tp.duration_ms) as avg_duration,
  count(*) as request_count
FROM flag_traces ft
JOIN user_plans up ON ft.trace_id = up.trace_id
JOIN trace_performance tp ON ft.trace_id = tp.trace_id
GROUP BY ft.ff_value, up.user_plan;
```

## Performance Characteristics

### Timestamp Precision (High-Resolution Anchored Design)

The timestamp system uses a high-precision anchored design that captures a single time reference at trace root creation,
then uses high-resolution timers for all subsequent timestamps. See
[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md) for full implementation details.

**Core Design**:

- ONE `Date.now()` captured at trace root (RequestContext creation)
- ONE high-resolution timer captured at trace root (`performance.now()` or `process.hrtime.bigint()`)
- All subsequent timestamps: `anchorEpochMicros + (highResNow - anchorHighRes) * scale`

**Platform Implementations**:

- **Browser**: `performance.now()` deltas from anchor
  - ~5μs resolution for sub-millisecond precision
  - No `Date.now()` calls per span
  - Stored as `timestamp[μs]` (microsecond) in Arrow
- **Node.js**: `process.hrtime.bigint()` deltas from anchor
  - Nanosecond precision available
  - Stored as microseconds for consistency
  - Arrow type: `timestamp[μs]` (microsecond)
- **Fallback**: `Date.now()` (millisecond precision)
  - Used when high-resolution timing is unavailable
  - Arrow type: `timestamp[ms]` (millisecond)

**Hot Path Storage**: All timestamps stored as `Float64Array` (microseconds since epoch) during logging. No object
allocations, no `Date.now()` calls per entry.

**Cold Path Conversion**: Converted to Arrow `TimestampMicrosecond` type, compatible with ClickHouse `DateTime64(6)`.

**Benefits**:

- Zero allocations per timestamp
- Sub-millisecond precision enables detailed performance analysis
- All spans in trace share same anchor (comparable, consistent)
- DST/NTP safe - anchor per trace, traces are short-lived

### Storage Efficiency

- **Dictionary encoding**: Module names, span names, HTTP methods stored once
- **Null bitmap compression**: Sparse columns compressed efficiently
- **Type optimization**: Appropriate numeric types minimize storage
- **Parquet compression**: Additional compression when written to storage
- **Timestamp precision**: Platform-optimized precision minimizes storage while maximizing accuracy

### Query Performance

- **Columnar scanning**: Only relevant columns read for queries
- **Predicate pushdown**: Filters applied at storage level
- **Parallel processing**: ClickHouse can parallelize across columns
- **Index support**: Dictionary columns enable efficient filtering
- **Timestamp indexing**: Nanosecond/microsecond precision enables precise time-based queries

### Data Characteristics

- **High sparsity**: Most columns null for most rows (efficient with Arrow nulls)
- **Temporal ordering**: Timestamp allows efficient time-range queries
- **Hierarchical structure**: Span relationships enable trace reconstruction
- **Multi-dimensional**: Can slice by module, user, time, or entry type

## Integration Points

This Arrow table structure integrates with:

- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: Foundational entry type system
  and low-level logging API
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Direct conversion from SpanBuffer columns
  with `attr_` prefix stripping
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Prefixed columns from different libraries
  cleanly separated
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Runtime generation of APIs that populate these
  Arrow columns
- **Background Processing Pipeline** (future document): Batch conversion process from buffers to Arrow/Parquet

The flat table structure enables rich analytical queries while maintaining the performance benefits of columnar storage
and efficient null handling.
