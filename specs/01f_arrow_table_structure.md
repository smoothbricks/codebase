# Arrow Table Structure

## Overview

The Arrow Table Structure defines the final queryable format produced by the trace logging system. It provides:

1. **Clean column schema** optimized for analytical queries
2. **Realistic trace data examples** showing spans, tags, and console.log compatibility
3. **ClickHouse query patterns** for common analytical use cases
4. **Performance characteristics** of the columnar format

## Design Philosophy

**Key Insight**: The Arrow table structure must balance query performance with data completeness. Every entry in the system becomes a row in the final table, enabling rich analytical queries while maintaining efficient storage.

**Core Principles**:

- **Flat structure**: All data flattened to a single table for maximum query flexibility
- **Nullable columns**: Sparse data handled efficiently with null values
- **Dictionary encoding**: Repeated strings stored efficiently
- **Type optimization**: Appropriate data types for storage and performance

## Column Schema

### Core System Columns (Always Present)

| Column Name      | Type                 | Description            | Example Values                                                                                                                                |
| ---------------- | -------------------- | ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `timestamp`      | `timestamp[ns]`      | When event occurred    | `2024-01-01T10:00:00.000Z`                                                                                                                    |
| `trace_id`       | `dictionary<string>` | Root trace identifier  | `'req-abc123'`, `'X-Request-Id-456'`                                                                                                          |
| `span_id`        | `uint64`             | Span identifier        | `1`, `2`, `3`                                                                                                                                 |
| `parent_span_id` | `uint64`             | Parent span (nullable) | `1` or `null`                                                                                                                                 |
| `entry_type`     | `dictionary<string>` | Log entry type         | `'span-start'`, `'span-ok'`, `'span-err'`, `'span-exception'`, `'tag'`, `'info'`, `'debug'`, `'warn'`, `'error'`, `'ff-access'`, `'ff-usage'` |
| `module`         | `dictionary<string>` | Module name            | `'UserController'`, `'DatabaseService'`                                                                                                       |
| `span_name`      | `dictionary<string>` | Span/task name         | `'create-user'`, `'validate-email'`                                                                                                           |
| `message`        | `string`             | Log message (nullable) | `'Starting user registration'`, `'User created successfully'` or `null`                                                                       |

### Library-Specific Attribute Columns (Sparse/Nullable)

| Column Name       | Type                 | Description                         | Example Values                                 |
| ----------------- | -------------------- | ----------------------------------- | ---------------------------------------------- |
| `http_status`     | `uint16`             | HTTP status code (nullable)         | `200`, `404`, `500` or `null`                  |
| `http_method`     | `dictionary<string>` | HTTP method (nullable)              | `'GET'`, `'POST'`, `'PUT'` or `null`           |
| `http_url`        | `string`             | Masked URL (nullable)               | `'https://api.*****.com/users'` or `null`      |
| `http_duration`   | `float32`            | HTTP request duration ms (nullable) | `125.5` or `null`                              |
| `db_query`        | `string`             | Masked SQL query (nullable)         | `'SELECT * FROM users WHERE id = ?'` or `null` |
| `db_duration`     | `float32`            | Query duration ms (nullable)        | `12.3` or `null`                               |
| `db_rows`         | `uint32`             | Rows affected/returned (nullable)   | `1`, `0`, `1000` or `null`                     |
| `db_table`        | `dictionary<string>` | Table name (nullable)               | `'users'`, `'orders'` or `null`                |
| `user_id`         | `binary[8]`          | Hashed user ID (nullable)           | `0x8a7b6c5d...` or `null`                      |
| `business_metric` | `float64`            | Custom metric value (nullable)      | `42.7`, `1.0` or `null`                        |
| `ff_name`         | `dictionary<string>` | Feature flag name (nullable)        | `'advancedValidation'`, `'newUI'` or `null`    |
| `ff_value`        | `boolean`            | Feature flag value (nullable)       | `true`, `false` or `null`                      |

## Complete Trace Example: User Registration Flow

This example shows a complete user registration request with multiple spans, HTTP calls, database operations, and console.log compatibility traces.

| trace_id     | span_id | parent_span_id | timestamp                  | entry_type   | module                | span_name            | message                                         | http_status | http_method | http_url                               | http_duration | db_query                                                                | db_duration | db_rows | db_table | user_id         | business_metric | ff_name              | ff_value |
| ------------ | ------- | -------------- | -------------------------- | ------------ | --------------------- | -------------------- | ----------------------------------------------- | ----------- | ----------- | -------------------------------------- | ------------- | ----------------------------------------------------------------------- | ----------- | ------- | -------- | --------------- | --------------- | -------------------- | -------- |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.000Z` | `span-start` | `UserController`      | `register-user`      | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null                 | null     |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.002Z` | `ff-access`  | `UserController`      | `register-user`      | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | `advancedValidation` | `true`   |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.005Z` | `info`       | `UserController`      | `register-user`      | `Starting user registration`                    | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            | null                 | null     |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.010Z` | `span-start` | `ValidationService`   | `validate-email`     | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.015Z` | `tag`        | `ValidationService`   | `validate-email`     | null                                            | 200         | `POST`      | `https://api.*****.com/validate-email` | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.045Z` | `tag`        | `ValidationService`   | `validate-email`     | null                                            | 200         | `POST`      | `https://api.*****.com/validate-email` | 30.2          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 2       | 1              | `2024-01-01T10:00:00.046Z` | `span-ok`    | `ValidationService`   | `validate-email`     | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.050Z` | `span-start` | `UserRepository`      | `check-user-exists`  | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.052Z` | `tag`        | `UserRepository`      | `check-user-exists`  | null                                            | null        | null        | null                                   | null          | `SELECT id FROM users WHERE email = ?`                                  | null        | null    | `users`  | `0x8a7b6c5d...` | null            |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.067Z` | `tag`        | `UserRepository`      | `check-user-exists`  | null                                            | null        | null        | null                                   | null          | `SELECT id FROM users WHERE email = ?`                                  | 15.3        | 0       | `users`  | `0x8a7b6c5d...` | null            |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.068Z` | `debug`      | `UserRepository`      | `check-user-exists`  | `User does not exist, proceeding with creation` | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 3       | 1              | `2024-01-01T10:00:00.069Z` | `span-ok`    | `UserRepository`      | `check-user-exists`  | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.070Z` | `span-start` | `UserRepository`      | `create-user`        | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.072Z` | `tag`        | `UserRepository`      | `create-user`        | null                                            | null        | null        | null                                   | null          | `INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)` | null        | null    | `users`  | `0x8a7b6c5d...` | null            |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.095Z` | `tag`        | `UserRepository`      | `create-user`        | null                                            | null        | null        | null                                   | null          | `INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)` | 23.1        | 1       | `users`  | `0x8a7b6c5d...` | null            |
| `req-abc123` | 4       | 1              | `2024-01-01T10:00:00.096Z` | `span-ok`    | `UserRepository`      | `create-user`        | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.100Z` | `span-start` | `NotificationService` | `send-welcome-email` | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.105Z` | `tag`        | `NotificationService` | `send-welcome-email` | null                                            | 202         | `POST`      | `https://email.*****.com/send`         | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.245Z` | `tag`        | `NotificationService` | `send-welcome-email` | null                                            | 202         | `POST`      | `https://email.*****.com/send`         | 140.3         | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 5       | 1              | `2024-01-01T10:00:00.246Z` | `span-ok`    | `NotificationService` | `send-welcome-email` | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.250Z` | `tag`        | `UserController`      | `register-user`      | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | 1.0             |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.251Z` | `info`       | `UserController`      | `register-user`      | `User registration completed successfully`      | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |
| `req-abc123` | 1       | null           | `2024-01-01T10:00:00.252Z` | `span-ok`    | `UserController`      | `register-user`      | null                                            | null        | null        | null                                   | null          | null                                                                    | null        | null    | null     | `0x8a7b6c5d...` | null            |

## Key Patterns in the Data

### 1. Span Hierarchy & Lifecycle

- **Root span** (span_id=1): `register-user` with no parent
- **Child spans** (span_id=2,3,4,5): All have parent_span_id=1
- **Span lifecycle**: `span-start` → entries → `span-ok`/`span-err`/`span-exception`
- **Success/failure tracking**: `span-ok` vs `span-err` vs `span-exception` captures span outcome without extra columns

### 2. Structured Logging via Entry Type Enum

- **Log levels with structure**: `ctx.info(message, attributes)` → `entry_type='info'` with typed attributes
- **Message storage**: Log messages stored in unified `message` column
- **Optional attributes**: Structured data can accompany log messages
- **Gradual migration**: Familiar log levels but with structured data instead of string concatenation

### 3. Entry Type System

The `entry_type` column uses a dictionary-encoded enum that covers all possible trace events. For complete definitions and low-level API details, see **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**.

**Entry Types**: `span-start`, `span-ok`, `span-err`, `span-exception`, `tag`, `info`, `debug`, `warn`, `error`, `ff-access`, `ff-usage`

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
  span_name,
  entry_type,
  timestamp,
  CASE
    WHEN log_message IS NOT NULL THEN log_message
    WHEN http_url IS NOT NULL THEN concat('HTTP ', http_method, ' ', http_url)
    WHEN db_query IS NOT NULL THEN concat('DB: ', db_table)
    ELSE span_name
  END as description
FROM traces
WHERE trace_id = 'req-abc123'
ORDER BY timestamp;
```

### Structured Logging Analysis

```sql
-- Analyze structured logging usage patterns
SELECT
  module,
  entry_type as log_level,
  count(*) as message_count,
  count(DISTINCT trace_id) as trace_count,
  count(DISTINCT message) as unique_messages,
  count(CASE WHEN http_status IS NOT NULL OR user_id IS NOT NULL THEN 1 END) as structured_entries
FROM traces
WHERE entry_type IN ('info', 'debug', 'warn', 'error')
GROUP BY module, entry_type
ORDER BY message_count DESC;
```

### Feature Flag Analysis

```sql
-- Feature flag usage and performance impact
SELECT
  ff_name,
  ff_value,
  count(*) as access_count,
  count(DISTINCT trace_id) as trace_count,
  avg(CASE WHEN ff_value = true THEN 1.0 ELSE 0.0 END) as enabled_ratio
FROM traces
WHERE entry_type = 'ff-access'
GROUP BY ff_name, ff_value
ORDER BY access_count DESC;

-- Correlation between feature flags and request performance by user plan
WITH flag_traces AS (
  SELECT DISTINCT trace_id, ff_name, ff_value, user_id
  FROM traces
  WHERE entry_type = 'ff-access' AND ff_name = 'advancedValidation'
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

### Storage Efficiency

- **Dictionary encoding**: Module names, span names, HTTP methods stored once
- **Null bitmap compression**: Sparse columns compressed efficiently
- **Type optimization**: Appropriate numeric types minimize storage
- **Parquet compression**: Additional compression when written to storage

### Query Performance

- **Columnar scanning**: Only relevant columns read for queries
- **Predicate pushdown**: Filters applied at storage level
- **Parallel processing**: ClickHouse can parallelize across columns
- **Index support**: Dictionary columns enable efficient filtering

### Data Characteristics

- **High sparsity**: Most columns null for most rows (efficient with Arrow nulls)
- **Temporal ordering**: Timestamp allows efficient time-range queries
- **Hierarchical structure**: Span relationships enable trace reconstruction
- **Multi-dimensional**: Can slice by module, user, time, or entry type

## Integration Points

This Arrow table structure integrates with:

- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: Foundational entry type system and low-level logging API
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Direct conversion from SpanBuffer columns with `attr_` prefix stripping
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Prefixed columns from different libraries cleanly separated
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Runtime generation of APIs that populate these Arrow columns
- **Background Processing Pipeline** (future document): Batch conversion process from buffers to Arrow/Parquet

The flat table structure enables rich analytical queries while maintaining the performance benefits of columnar storage and efficient null handling.
