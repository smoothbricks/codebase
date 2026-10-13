# Entry Types and Logging Primitives <a id="smoo/lmao!n/lmao-entry"></a>

## Design Philosophy <a id="smoo/lmao!n/lmao-entry-design-philosophy"></a>

**Key Insight**: All trace events in the system are unified under a single entry type enum. Whether it's a span
lifecycle event, console.log call, structured data tag, or feature flag evaluation - everything becomes a row in the
trace with a specific entry type.

**Core Principles**:

- **Unified event model**: One enum covers all possible trace events
- **Zero ambiguity**: Each entry type has a precise, well-defined meaning
- **Performance first**: Entry types are enum-encoded for minimal overhead
- **Extensible**: New entry types can be added without breaking existing code
- **Destructured context**: Context properties available via destructuring in op signatures

## Entry Type Definitions <a id="smoo/lmao!n/lmao-entry-entry-type-definitions"></a>

The entry type system defines exactly what each row in a trace represents:

### Span Lifecycle Entry Types <a id="smoo/lmao!n/lmao-entry-span-lifecycle-entry-types"></a>

Spans represent units of work with a clear beginning and end. These entry types use **fixed row positions** in the
SpanBuffer (see [Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md) for details):

```
SpanBuffer row layout:
┌──────────────────────────────────────────────────────────────────────────┐
│ Row 0: span-start     │ tag writes HERE (overwrites)                     │
├──────────────────────────────────────────────────────────────────────────┤
│ Row 1: span-exception │ Pre-initialized at span creation                 │
│        (pre-init)     │ Overwritten by ok() → span-ok                    │
│                       │            or err() → span-err                   │
├──────────────────────────────────────────────────────────────────────────┤
│ Row 2+: events        │ log.info/debug/warn/error appends here           │
│                       │ span-retry also appends here (transient failures)│
└──────────────────────────────────────────────────────────────────────────┘

Span completion entry types (all written to row 1):
  - span-ok (2):        Success, written by ok()
  - span-err (3):       Expected business error, written by err()
  - span-exception (4): Uncaught exception, pre-initialized at row 1; records the thrown value
```

- **`span-start`** - Beginning of a work unit (span) - **Always row 0**
  - Created when entering a traced function or operation
  - Always paired with exactly one completion entry type
  - Contains span metadata (module, span_name, parent relationships)
  - `tag.*` writes span attributes to this row (overwrites, not appends)
  - Can optionally include structured attributes and message

- **`span-ok`** - Span completed with `return ok()` - **Always row 1**
  - Normal successful completion path
  - Overwrites the pre-initialized span-exception entry
  - May contain result data in attribute columns
  - Indicates the span achieved its intended outcome

- **`span-err`** - Span completed with `return err()` - **Always row 1**
  - Expected error/failure completion path
  - Overwrites the pre-initialized span-exception entry
  - May contain error details in attribute columns
  - Still considered "handled" - not an exception

- **`span-exception`** - Span threw unexpected exception - **Always row 1**
  - Pre-initialized at span creation (row 1 defaults to this)
  - Remains if exception bypassed normal `ok()`/`err()` flow
  - Records the thrown value, because a thrown value is arbitrary: an `Error` whose outer message is empty and whose
    meaning sits in its `cause`, an `AggregateError` whose meaning sits in its `errors`, an `Error` from another realm,
    or no `Error` at all. A value is an `Error` when `Error.isError` says so (its `[[ErrorData]]` brand, which holds
    across realms and which neither a `Symbol.toStringTag` nor a proxy can claim) or when it carries the host's
    `DOMException` brand (the receiver check of `DOMException.prototype`'s `code` getter, which neither an object made
    from that prototype nor a proxy can pass): some hosts (bun) answer `Error.isError` false for a `DOMException`, and
    an `AbortError` or `TimeoutError` must keep its name, message, code and stack. Recording requires a host with
    `Error.isError`. A value is an `AggregateError` when it is an instance of this realm's `AggregateError`, so another
    realm's `AggregateError` is recorded as an `Error` without its `errors`. `message` holds the outer `Error` message,
    or a non-`Error` value as `<typeof>: <String(value)>`, each bounded to 4096 characters with the cut counted; in
    `exception_stack` each link's `name` and `message` carry the same bound, so a long message leaves room for its
    stack. The `exception_stack` column holds, for each `Error` link, `name: message`, then `[code]: <code>` when the
    link has a string or number `code`, then `[stack]: <stack>`; for an `AggregateError`, each of its `errors` as a
    chain of its own after `\n[errors <i>]: `, when the list's `length` is a non-negative safe integer (a proxy's
    `length` may answer anything; any other answer is written as `[unreadable errors: …]` in its place); then every
    `cause` link after `\n[cause]: `. An object recorded earlier in the same value is written as `[already recorded]`
    and ends its chain; primitives are recorded each time. A non-`Error` link is recorded like a non-`Error` value and
    ends its chain.
  - The whole evidence has one budget: at most 64 links (the thrown value, every cause and every aggregated error) and
    16384 characters. Every recorded string, marker included, is cut to what the budget has left; what it cannot hold is
    never walked but marked in place (`[<part> not recorded]` for a link, `[<n> more errors not recorded]` for an
    `AggregateError`'s remaining errors). Two closing lines count what was left out:
    `[evidence budget spent: <n> links not walked]` (a stopped cause chain and each unwalked `errors` entry count as
    one, whatever lies beneath them; an `errors` entry that cannot be read still spends its link) and
    `[<n> more characters not recorded]`. A thrown value can be any size, and a getter or proxy can mint a new link on
    every read, so only the budget bounds the work.
  - No other own field of the thrown value is read (an `AggregateError`'s `errors` aside), and no non-`Error` value is
    serialized beyond its own bounded `String()`: an error's fields can carry request configuration or credentials, and
    values reach traces only when explicitly logged ([Security by Default](./01a_trace_schema_system.md)). Log what a
    failure needs explicitly.
  - Recording never throws, so it cannot replace the exception it records. A part that cannot be read (a throwing
    getter, a revoked proxy as the value, a cause, an `errors` entry or the `errors` list) is written as
    `[unreadable <part>: <reason>]` in its place, and the rest of the value is still recorded; the original value is
    rethrown unchanged.
  - Indicates truly exceptional circumstances
  - Duration still valid: `timestamps[1] - timestamps[0]`

- **`span-retry`** - Transient failure triggering retry - **Row 2+ (appended)**
  - Written by Op class when result is TransientError and retry will occur
  - Contains timing info for the failed attempt
  - Parent span has full context tags (op name, key, etc.)
  - Minimal child span with retry-specific data:
    - `retry_attempt`: Attempt number (1, 2, 3...)
    - `retry_error`: Error message that triggered retry
    - `retry_delay_ms`: Delay before next attempt
  - Duration measures the failed attempt (not including retry delay)

- **`span-detach`** - Detached-Op dispatch complete, business result pending - **Row 2+ (appended)**
  - Written when an Op handler returns the detached-pending sentinel (the streaming-operations contract): the dispatch
    succeeded, the business result arrives through a later execution
  - The span still closes normally at row 1 (`span-ok` = the dispatch itself succeeded)
  - Continuation executions (pumps, re-attach, post-restart delivery) are separate spans linked by identity — see
    "Detach Entry Type" below

#### Buffer Initialization Code

The fixed row layout is established at span creation and updated on completion:

```typescript
function initializeSpanBuffer(buffer: SpanBuffer, spanName: string): void {
  const now = getTimestamp();

  // Row 0: span-start (always present)
  buffer.timestamp[0] = now;
  buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;

  // Row 1: pre-initialized as span-exception (exception safety)
  buffer.timestamp[1] = now; // Updated by ok()/err()
  buffer.entry_type[1] = ENTRY_TYPE_SPAN_EXCEPTION; // Overwritten on completion

  // Ready for events at row 2
  buffer._writeIndex = 2;
}

function completeSpanOk(buffer: SpanBuffer, result?: any): void {
  buffer.timestamp[1] = getTimestamp();
  buffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;
  // Result data written to row 1's attribute columns
}

function completeSpanErr(buffer: SpanBuffer, error: string): void {
  buffer.timestamp[1] = getTimestamp();
  buffer.entry_type[1] = ENTRY_TYPE_SPAN_ERR;
  // Error details written to row 1's attribute columns
}
```

This design ensures every span has valid duration data, even when exceptions bypass normal completion.

### Fluent Result Integration <a id="smoo/lmao!n/lmao-entry-fluent-result-integration"></a>

`ctx.ok()` and `ctx.err()` return fluent result objects bound to their creating span context. The context already owns
the immutable span-root buffer and the mutable append buffer; the result captures that context reference once.

```typescript
return ctx.ok(value).with({ cached: true }).message('Success');
return ctx.err(NOT_FOUND({ id })).with({ searched: true }).message('Not found');
```

#### Fixed-Row Attribute Writes

The span-start operation reserves row 1 for completion. `.with()`, `.message()`, and `.line()` write directly to that
row through the lazily materialized result writer. They do not allocate deferred closure chains or retarget attributes
to whichever span later receives the result. Overflow advances the context's append buffer, not the span-root buffer
used by fixed-row writers.

#### Span-End Write Sequence

The execution envelope first checks that the result's captured writer state is the exact executing context. Only then
does `writeSpanEnd` write `span-ok` or `span-err`, its timestamp, and any structured error code. A mismatch throws a
programmer `TypeError` and the envelope records `span-exception` with the failure details. Retry dispatch performs the
same check before applying a transient error's policy, so a foreign result never retries the receiving span.

Completed child results remain inspectable. To propagate their outcome, return the receiving context's own
`ctx.ok(child.value)` or `ctx.err(child.error)`. TypeScript checks result payload/schema types, not the identity of
individual callback invocations. See [Result Ownership](./01c_context_flow_and_op_wrappers.md#result-ownership) for the
runtime guarantee and its lifetime limits.

Attribute writes happen before completion checking; the check does not undo prior writes, revoke captured contexts, or
make result payloads immutable. Low-level result constructors can take an explicit writer state; unbound results cannot
complete a traced callback.

#### Message Column Semantics

The `message` column has different semantics for row 0 vs row 1:

| Row | Default Value | Can Be Overwritten? | How to Overwrite |
| --- | ------------- | ------------------- | ---------------- |
| 0   | Span name     | No (immutable)      | N/A              |
| 1   | Span name     | Yes                 | `.message()`     |

```typescript
// Row 1 message = "create-user" (span name, default)
return ctx.ok(user);

// Row 1 message = "User created successfully" (custom)
return ctx.ok(user).message('User created successfully');
```

#### Row 1 Entry Types

| Return Type                                 | Entry Type Written   | Written By     |
| ------------------------------------------- | -------------------- | -------------- |
| `Ok` (from the span's own `ctx.ok()`)       | `span-ok` (2)        | span()/trace() |
| `Err` (from the span's own `ctx.err()`)     | `span-err` (3)       | span()/trace() |
| Any other value, including a foreign result | `span-exception` (4) | span()/trace() |
| Exception thrown                            | `span-exception` (4) | span()/trace() |

**Note**: Entry types are always written by `span()`/`trace()` at span-end, never by the result constructor.

#### When to Return vs Throw

- **Known operational failures MUST return `Err`** (including `ctx.err(...)`), which produces `span-err`.
- **Unexpected invariant/programmer failures MAY throw**, which produces `span-exception`.
- Operational retry signaling must stay in `Err` values; do not rely on thrown exceptions for known retry paths.
- A thrown exception represents a bug or violated invariant, not normal control flow.

### Retry Entry Type <a id="smoo/lmao!n/lmao-entry-retry-entry-type"></a>

The `span-retry` entry type provides observability for transient failure handling in Op execution:

**When written:**

- Op returns TransientError (via `ctx.err(TRANSIENT_CODE({ data }))`)
- Op has retry policy and attempt limit not reached
- Logged BEFORE the delay wait, captures the failed attempt

**What it contains:**

- Timestamp of the failure
- Entry type: span-retry (5)
- Message: retry:op:{opName} (for prefix-based querying)
- retry_attempt: Which attempt failed (1-indexed)
- retry_error: Error message or code
- retry_delay_ms: How long until next attempt

**Key design decisions:**

- **Appended to parent span buffer**: Not a separate span, just a log entry
- **Prefix naming**: `retry:op:{name}` enables `retry:*` queries
- **Minimal data**: Parent span has full context (op_name, op_key, args)

**Counting retries:**

Count `span-retry` entries for a span to know how many retries occurred. If the final result is `span-ok`, all retries
succeeded eventually. If the final result is `span-err` with code RETRIES_EXHAUSTED, retries failed.

**Example trace:**

```
span-start: op:fetchPayment
span-retry: retry:op:fetchPayment (attempt=1, error="503 Service Unavailable", delay=100ms)
span-retry: retry:op:fetchPayment (attempt=2, error="503 Service Unavailable", delay=200ms)
span-ok: op:fetchPayment (success on attempt 3)
```

### Detach Entry Type <a id="smoo/lmao!n/lmao-entry-detach-entry-type"></a>

The `span-detach` entry type provides inspector legibility for **detached Ops** — Ops whose execution outlives their
dispatch (the streaming-operations contract: the handler returns the pending sentinel; the business result arrives later
via a poller, webhook, or journal attach). It adds **no execution semantics**: retry, result delivery, and suspension
are unchanged. It exists so one logical Op renders as one story across detach, re-attach, and process restart instead of
as orphan spans.

**When written:**

- The Op handler returns the detached-pending sentinel — dispatch (submit, enqueue, journal registration) succeeded
- Logged before the span closes; the span then completes row 1 with `span-ok` (the dispatch itself succeeded)

**What it contains:**

- Message: `detach:op:{opName}` (prefix-based querying, mirroring `retry:op:{name}`)
- Attributes: the Op idempotency key and executor kind (batch poll / remote worker / journal attach) — enough for a
  continuation span to be joined back without new columns

**Continuations are segment-linked, not re-parented:**

Later executions of the same logical Op (result-delivery pumps, journal re-attach, post-restart completion) run in fresh
buffers, possibly in a different process. They are **separate spans** carrying the same `trace_id` and referencing the
originating span through the existing packed span identity ([Span Identity](./01b4_span_identity.md)) — the same
convergence discipline the Cloudflare segment tier already uses for split-brain traces
([Cloudflare Trace Segments](./01u_cloudflare_trace_segments.md)). No new linking machinery: the inspector groups by
`(trace_id, originating span_id)` and renders detach → pump(s) → completion as one logical Op.

**Numeric assignment:**

`span-detach` takes the **next free ordinal at implementation time** (the tail of the entry-type registry in
`schema/systemSchema.ts`). It is never inserted into the existing 1–5 span-lifecycle band: consumers range-check
contiguous bands (e.g. the log-level classification in `convertToArrow.ts`), so bands are append-only.

**Example trace:**

```
span-start:  op:transcodeVideo
span-detach: detach:op:transcodeVideo (key=job-42, executor=batch-poll)
span-ok:     op:transcodeVideo (dispatch complete)
--- later, possibly another process ---
span-start:  op:transcodeVideo/attach (same trace_id, links job-42's originating span)
span-ok:     op:transcodeVideo/attach (result delivered)
```

### Log Level Entry Types <a id="smoo/lmao!n/lmao-entry-log-level-entry-types"></a>

Structured logging appends rows starting at row 2. The entry-type enum includes all five levels:

- **`trace`** - Most verbose diagnostic events
- **`debug`** - Diagnostic events
- **`info`** - Structured operational information
- **`warn`** - Structured operational warning
- **`error`** - Structured operational failure

#### Static operational templates and diagnostic raw messages

Logging has two deliberate policies; they MUST NOT be blended:

1. **Operational `info`/`warn`/`error`** require a compiler-visible literal template. Runtime values belong in an
   object-literal field set whose keys are schema-known. Template interpolation, string concatenation, a dynamic message
   expression, and a dynamically shaped attributes object are rejected. Placeholders use `{fieldName}`; `{{` and `}}`
   encode literal braces. `{{fieldName}}` is not accepted as an alias. The stored template keeps its escapes exactly as
   written; `{{` and `}}` are decoded only when a reader renders the message, never on the entry path.
2. **Diagnostic `debug`/`trace`** may retain the existing raw dynamic string-reference form when the whole argument is
   an existing string value. Avoidable interpolation and concatenation are rejected: use a literal template plus
   object-literal fields when structure is available. A raw diagnostic string uses the dynamic message lane; it does not
   become operational vocabulary.

```typescript
log.info('User {userId} processed {count} items', { userId, count }); // valid structured operational form
log.warn('Queue depth {depth} exceeded {limit}', { depth, limit }); // valid structured operational form
log.info('Parser emitted {{token}}', {}); // valid: escaped braces, no field; renders as "Parser emitted {token}"
log.error(`Request ${requestId} failed`); // invalid: interpolation
log.info(messageFromConfig); // invalid: dynamic operational message

log.debug(debugText); // valid dynamic diagnostic reference
log.trace('packet {packetId}', { packetId }); // valid structured diagnostic template
log.debug(`packet ${packetId}`); // invalid: avoidable interpolation
```

The callsite classes are **static** (literal with no fields), **structured** (literal plus object-literal fields), and
**dynamic** (an allowed existing diagnostic string reference). `mixed` is not a callsite class; it names only a physical
buffer/layout that contains both a static-index lane and a dynamic-reference lane.

#### Message physical representation

Every compiler-known log template or literal span name has a fragment-local ordinal. `kindTag = 1` means `LOG_TEMPLATE`;
`kindTag = 2` means `SPAN_NAME`. Checker-proven literal span names use the static lane, while non-literal span names
remain dynamic. The LMAO runtime installer MUST be imported and evaluated before any module invokes the Symbol-keyed
registration callback. Direct `binding[ordinal]` indexing yields the process-dense `Uint32` message index; there is no
warmed method dispatch or `messageIndex()` lookup.

The registry validates the typed fragment and copies/merges it into a runtime-owned immutable dictionary generation.
Module arrays are inputs, not borrowed mutable Arrow storage. Dense indices are process-local and append-only in
fragment registration order: each new generation preserves the previous generation as an unchanged prefix and appends
only unseen decoded values, so prior bindings remain prefix-valid. Ids and decoded values are deterministic; dense
indices and dictionary ordering are not deterministic across processes. Dense index `0` is valid and null is represented
only by the Arrow validity bitmap.

The warmed path performs no hash lookup, interning, string rendering, object enumeration, backend dispatch, or
allocation. Dynamic references are resolved during overflow/flush processing. Each flush reuses the active vocabulary
dictionary and `PhysicalLayoutPlan`; an `ArrowLease` pins the exact runtime-owned storage and immutable dictionary
generation until export finishes, never the module's fragment arrays.

Precompiled libraries register and own their vocabulary fragment inputs when evaluated. The application neither
recompiles nor assumes ownership of library templates. Registration IDs are stable content-derived identifiers, while
dense message indices are process-local hot/Arrow indices; they are not interchangeable.

#### The `message` Column: Operational Templates and Diagnostic Raw References

Operational messages use format templates stored logically in the `message` column; the column stores their dense
vocabulary indices. Diagnostic `debug`/`trace` may instead use the raw dynamic-reference policy above. Placeholders use
`{fieldName}`, with `{{` and `}}` for literal braces. `{{fieldName}}` is not accepted as an alias. The stored template
keeps the escapes as written, so in stored text a single-brace `{name}` is always a placeholder and a literal brace is
always doubled.

```typescript
const processUser = defineOp('processUser', async ({ log, ok }) => {
  log.info('User {userId} processed {count} items', { userId: 'user-123', count: 42 });
  return ok(null);
});
```

The logical row contains the template identity plus typed `userId` and `count` fields. It never contains a rendered
string. This preserves queryable template identity, typed aggregation, and one stable vocabulary entry without
allocating an interpolated message per call.

```sql
SELECT userId, count(*), avg(count)
FROM traces
WHERE message = 'User {userId} processed {count} items'
GROUP BY userId;
```

Traditional interpolation remains invalid because it destroys template identity:

```typescript
console.log(`User ${userId} processed ${count} items`); // unstructured, allocates a rendered string
log.info('User {userId} processed {count} items', { userId, count }); // structured form
```

**Row Behavior**: Unlike `tag.*` which overwrites row 0, `log.*` methods APPEND new rows:

```typescript
const processUser = defineOp('processUser', async ({ log, ok }) => {
  // writeIndex starts at 2 after span initialization
  log.info('Starting process'); // Writes to row 2, writeIndex → 3
  log.debug('Details...'); // Writes to row 3, writeIndex → 4
  log.warn('Slow operation'); // Writes to row 4, writeIndex → 5
  return ok(null);
});
```

### Span Attributes (tag) <a id="smoo/lmao!n/lmao-entry-span-attributes-tag"></a>

**Note**: The `tag` API does NOT create a separate entry type. It updates attributes on the span-start row (row 0).

- `tag.attribute()` calls update **row 0** (span-start row) directly
- **OVERWRITES** row 0 attribute columns, does NOT append new rows
- Multiple `tag` calls update the same row - last write wins
- Follows Datadog's `span.set_tag()` / OpenTelemetry's `Span.setAttribute()` pattern
- Contrast with `log.*` which APPENDS new rows (events)

```typescript
const processUser = defineOp('processUser', async ({ tag, log, ok }) => {
  // tag writes to row 0 (span-start) - OVERWRITES, not appends
  tag.userId('user-123'); // Writes to userId_values[0]
  tag.requestId('req-456'); // Writes to requestId_values[0]
  tag.userId('user-999'); // Overwrites userId_values[0]

  // log creates new rows - APPENDS
  log.info('Processing...'); // Appends at row 2
  log.debug('Details...'); // Appends at row 3
  return ok(null);
});
```

### Feature Flag Entry Types <a id="smoo/lmao!n/lmao-entry-feature-flag-entry-types"></a>

- **`ff-access`** - When feature flags are evaluated
  - Logged automatically by `FeatureFlagEvaluator`
  - Flag name stored in unified `message` column (consistent with span names and log templates)
  - Flag value stored in `ff_value` column (S.category for efficient storage of repeated values)
  - Tracks when decisions are made based on flags

- **`ff-usage`** - When flag-gated features are actually used
  - Logged when flag-controlled code paths execute
  - Flag name stored in unified `message` column
  - Contains usage context and outcome data (user-defined attribute columns)
  - Tracks actual feature utilization vs just evaluation

See **[Feature Flags](./01p_feature_flags.md)** for details on the evaluator implementation and tracking API.

### Metrics Entry Types <a id="smoo/lmao!n/lmao-entry-metrics-entry-types"></a>

Metrics are structured logs that capture operational statistics during flush cycles. All metrics rows from the same
flush share identical `timestamp` (the period end time).

**Period Marker**:

- **`period-start`** - Marks the beginning of a metrics period
  - `uint64_value`: Nanosecond timestamp when the period began
  - `message`: unused (null)
  - Enables calculating period duration: `row.timestamp - row.uint64_value`

**Op Metrics** (8 entry types for operation statistics):

- **`op-invocations`** - Total invocation count for an operation
  - `message`: Op name (e.g., `'GET'`, `'createUser'`)
  - `uint64_value`: Total count of invocations during the period

- **`op-errors`** - Count of operations that returned `err()` (span-err)
  - `message`: Op name
  - `uint64_value`: Error count

- **`op-exceptions`** - Count of operations that threw exceptions (span-exception)
  - `message`: Op name
  - `uint64_value`: Exception count

- **`op-duration-total`** - Sum of all operation durations
  - `message`: Op name
  - `uint64_value`: Total nanoseconds spent in this operation

- **`op-duration-ok`** - Sum of durations for successful operations (span-ok)
  - `message`: Op name
  - `uint64_value`: Nanoseconds spent in successful invocations

- **`op-duration-err`** - Sum of durations for error operations (span-err)
  - `message`: Op name
  - `uint64_value`: Nanoseconds spent in error invocations
  - **Design insight**: Comparing `op-duration-err` to `op-duration-ok` reveals if errors are fast-fails or slow
    timeouts

- **`op-duration-min`** - Minimum operation duration observed
  - `message`: Op name
  - `uint64_value`: Shortest duration in nanoseconds

- **`op-duration-max`** - Maximum operation duration observed
  - `message`: Op name
  - `uint64_value`: Longest duration in nanoseconds

**Buffer Metrics** (3 entry types for buffer statistics, read from the SpanBuffer class's `stats`):

- **`buffer-writes`** - Total entries written to this module's buffers
  - `uint64_value`: Count of completed entries written during the period (`stats.totalWrites`)
  - `message`: unused (null)

- **`buffer-spans`** - Spans created
  - `uint64_value`: Number of non-overflow SpanBuffers created during the period (`stats.spansCreated`)
  - `message`: unused (null)

- **`buffer-capacity`** - Current buffer capacity
  - `uint64_value`: Rows per new SpanBuffer for this module (`stats.capacity`)
  - `message`: unused (null)

## Fluent API Design <a id="smoo/lmao!n/lmao-entry-fluent-api-design"></a>

With unified tag attributes across all entry types, we use a fluent/chainable API pattern that can be mixed into
different operations:

```typescript
const processOrder = defineOp('processOrder', async ({ ok, err, span, log }, order) => {
  // Fluent span completion API
  return ok(result).with({ userId: user.id, operation: 'CREATE' }).message('User created successfully');

  return err(VALIDATION_FAILED({ field: 'email' }))
    .with({
      field: 'email',
      attemptedValue: userData.email,
      validationRule: 'unique_constraint',
    })
    .message('Email validation failed');

  // Fluent logging API
  log.info('Processing user data').with({ userId: user.id, operation: 'PROCESS' });

  // Simple usage when no additional context needed
  return ok(result);
  return err(VALIDATION_FAILED({ field: 'email' }));
  log.info('Simple message');
});
```

This fluent pattern provides a consistent, composable API across all trace operations.

### The `.uint64_value()` Fluent Method <a id="smoo/lmao!n/lmao-entry-the-uint64-fluent-method"></a>

The `.uint64_value()` method attaches a large integer value to any entry type. It writes to the `uint64_value` lazy
system column (see [Arrow Table Structure](./01f_arrow_table_structure.md)). The method name matches the column; there
is no `.uint64()` alias ([Op and Buffer Metrics](./01n_op_and_buffer_metrics.md#smoo/lmao!n/op-metrics-uint64-api)).

**Use Cases**:

- Large record counts that exceed safe JavaScript integer range
- Byte counts for data processing
- External IDs (Snowflake IDs, Discord IDs, etc.)
- Any uint64 value that needs to be queryable

**API Examples**:

```typescript
const processRecords = defineOp('processRecords', async ({ log, tag, ok }, records) => {
  // Attach to tag entry (row 0)
  tag.batchId(batchId).uint64_value(recordCount);

  // Attach to log entry (appended row)
  log.info('Processing complete').uint64_value(bytesProcessed);

  // Attach to result (row 1)
  return ok(result).uint64_value(totalRecords);
});
```

**Why a Dedicated Column**:

1. **JavaScript's number limitation**: `Number.MAX_SAFE_INTEGER` is 2^53-1 (~9 quadrillion), but many systems use full
   uint64 values
2. **Shared with metrics**: Both metrics (counts, nanoseconds) and users need large integers - one lazy column serves
   all
3. **Type safety**: Stored as `BigUint64Array`, converted to Arrow `uint64`
4. **Query efficiency**: Direct column access without JSON parsing

### SpanLogger Allocation Design <a id="smoo/lmao!n/lmao-entry-spanlogger-zero-allocation-design"></a>

The design avoids a per-call builder allocation by reusing each span's `SpanLogger`; span setup still creates the
instance. A warmed entry write allocates nothing while capacity remains:

#### Design Evolution and Trade-offs

**❌ Approach 1: Separate Builder Objects**

```typescript
// Creates new objects on every call - expensive!
info(message): FluentLogBuilder { return new FluentLogBuilder(...); }
tag: TagProxy { return createTagProxy(...); }
```

_Problem_: Object allocation on every logging call defeats performance goals.

**❌ Approach 2: Shared Objects with Buffer Swapping**

```typescript
// Single shared tag object, swap buffer per span
sharedTag.buffer = currentBuffer;
```

_Problem_: Async/promises would cause buffer conflicts between concurrent spans.

**❌ Approach 3: Runtime Proxy**

```typescript
tag = new Proxy(target, handler); // Dynamic property access
```

_Problem_: Proxy overhead on every property access contradicts zero-overhead goals.

**❌ Approach 4: Object Creation in Constructor**

```typescript
constructor(buffer) {
  this.tag = new CompiledTagOps(buffer); // One allocation per SpanLogger
}
```

_Problem_: Still allocating objects, even if only once per span.

**✅ Final Approach: Separate tag and log APIs via Destructuring**

The API is simplified by destructuring `tag` and `log` directly from the context:

```typescript
// Op context provides separate API surfaces via destructuring
const processUser = defineOp('processUser', async ({ tag, log, setScope, ok, err }, userData) => {
  // tag: For span attributes (chainable setters)
  // log: For log messages (info/debug/warn/error)
  // setScope: For scoped attributes
  // ok/err: For span completion; every op returns one of them
  return ok(userData);
});

// TagWriter handles span attributes (chainable); its only instance field is the context's writer state
class TagWriter {
  constructor(state) {
    this._state = state;
    // Runtime class generation adds attribute methods to prototype
  }

  // One-column illustration of tag.with({ userId: "123" }); the full setter writes every supplied attribute.
  with(attributes) {
    if (attributes.userId !== undefined && attributes.userId !== null) {
      this._state._spanBuffer.userId(0, attributes.userId);
    }
    return this;
  }

  // Attribute methods compiled onto prototype at module context creation time
  // Each writes one column of row 0 through the buffer's column setter
  userId(value) {
    this._state._spanBuffer.userId(0, value); // Always row 0
    return this; // Chainable!
  }

  httpStatus(value) {
    this._state._spanBuffer.httpStatus(0, value);
    return this;
  }
}

// BaseSpanLogger handles log messages
class BaseSpanLogger {
  constructor(buffer) {
    this.buffer = buffer;
  }

  // Log methods return fluent interface for .with() chaining
  info(message) {
    this._writeLogEntry('info', message);
    return this;
  }

  with(attributes) {
    this._writeAttributes(attributes);
    return this;
  }
}
```

#### Key Insights

1. **Destructured APIs**: `tag` and `log` destructured directly from context - cleaner than nested access
2. **No per-call chaining allocation**: All fluent methods return the same instance for continued chaining
3. **Runtime class generation**: TagWriter and BaseSpanLogger classes built at runtime with `new Function` for typed
   methods
4. **Per-span instances**: Each span creates new tag/log instances with direct buffer references
5. **Span-local grouping**: Each span's entries stay together in final Arrow output; this does not order dictionaries
6. **Prototype compilation**: Attribute methods compiled onto prototypes at module context creation time
7. **Module boundary safety**: Unknown columns written as null when called from deeper contexts
8. **Reserved method names**: `with` is reserved and cannot be used as attribute column names
9. **OpenTelemetry alignment**: `tag` mirrors Span.setAttribute(), `ok()/err()` mirrors Span.setStatus()

#### Usage Examples

```typescript
const processUser = defineOp('processUser', async ({ tag, log, setScope, span, ok, err }, userData) => {
  // ===== SPAN ATTRIBUTES (tag) =====
  // Set span attributes - chainable methods
  tag.userId('user-123').requestId('req-456').operation('CREATE');

  // Object-based API for multiple attributes at once
  tag.with({ userId: 'user-123', requestId: 'req-456', operation: 'CREATE' });

  // ===== LOG MESSAGES (log) =====
  // Format: {fieldName} placeholder; {{ and }} encode literal braces.

  log.info('Processing user {userId}', { userId: 123 });
  // Logical message='Processing user {userId}', userId=123

  log.debug('Query on {table} took {duration}ms', { table: 'users', duration: 12.5 });
  // Logical message='Query on {table} took {duration}ms', table='users', duration=12.5

  log.warn('Rate limit {current}/{max}', { current: 95, max: 100 });
  // Logical message='Rate limit {current}/{max}', current=95, max=100

  log.error('Connection to {host} failed', { host: 'db.example.com' });
  // Logical message='Connection to {host} failed', host='db.example.com'

  // ===== SCOPED ATTRIBUTES (setScope) =====
  // Set once, propagates to all entries and child spans
  setScope({ requestId: 'req-456', userId: 'user-123' });
  // All subsequent entries in this span include these attributes

  // ===== CHILD SPANS (span) =====
  const payment = await span('process-payment', processPaymentOp, order);
  if (payment.isErr()) return err(payment.error); // Rebuild a child's outcome on this context

  // ===== SPAN COMPLETION (ok/err) =====
  // Success with result data
  return ok(result).with({ rowsAffected: 5, duration: 12.5 });

  // Error with context (VALIDATION_FAILED and NOT_FOUND come from defineCodeError)
  return err(VALIDATION_FAILED({ field: 'email' })).with({ rule: 'unique' });

  // Simple usage when no additional context needed
  return ok(result);
  return err(NOT_FOUND({ userId: 'user-123' }));
});
```

#### Performance Characteristics

- **Allocation profile**: Span setup allocates the SpanLogger; individual fluent calls allocate no builder
- **Prototype methods**: All tag operations pre-compiled at module context creation time
- **V8 optimization**: Single object with stable hidden class for optimal JIT compilation
- **Memory efficiency**: No intermediate objects or proxies

**Transformer Optimization for `with()` Bulk Setter:**

The `with()` bulk setter is syntactic sugar that the transformer can unroll into individual setter calls:

```typescript
// What you write:
tag.with({ userId: 'user-123', requestId: 'req-456' });

// What the transformer produces (unrolled for V8 inline caching):
tag.userId('user-123').requestId('req-456');
```

This unrolling eliminates the object allocation and `Object.keys()` iteration at runtime, making `with()` zero-cost when
the transformer is enabled. Without the transformer, `with()` still works but has minor overhead from iterating the
object keys.

This design preserves fluent ergonomics while each warmed, in-capacity entry write stays allocation-free.

### Type Safety with Generics <a id="smoo/lmao!n/lmao-entry-type-safety-with-generics"></a>

The API uses TypeScript generics to provide full type safety for attribute operations:

```typescript
// Reserved method names that cannot be used as attributes
type ReservedTagNames = 'with'; // Fewer reserved names since tag is separate
type ReservedLogNames = 'with' | 'info' | 'debug' | 'warn' | 'error';

// TagWriter - for span attributes (tag)
type TagWriter<TAttributes extends ValidAttributes<TAttributes>> = {
  // Dynamically generated attribute methods
  [K in keyof TAttributes]: (value: TAttributes[K]) => TagWriter<TAttributes>;
} & {
  // Object-based API
  with(attributes: Partial<TAttributes>): TagWriter<TAttributes>;
};

// BaseSpanLogger - for log messages (log)
interface BaseSpanLogger<TAttributes extends ValidAttributes<TAttributes>> {
  info(message: string): FluentLogEntry<TAttributes>;
  debug(message: string): FluentLogEntry<TAttributes>;
  warn(message: string): FluentLogEntry<TAttributes>;
  error(message: string): FluentLogEntry<TAttributes>;
}

interface FluentLogEntry<TAttributes> {
  with(attributes: Partial<TAttributes>): FluentLogEntry<TAttributes>;
}
```

**Usage with Full Type Safety**:

```typescript
// Define attribute schema for module
interface UserServiceAttributes {
  user_id: string;
  http_status: number;
  operation: 'CREATE' | 'UPDATE' | 'DELETE';
  email?: string;
  // with: string;  // ❌ Compile error: reserved name
}

// The op context is typed by the defineOpContext() that created defineOp: SpanContext<Ctx>
const createUser = defineOp(
  'createUser',
  async ({ tag, log, setScope, ok }: SpanContext<OpContextOf<typeof userContext>>, userData: UserData) => {
    // ===== tag - Span Attributes =====
    // Chainable attribute setters (destructured from context)
    tag
      .user_id(userData.id) // ✅ Generated: (value: string) => this
      .http_status(201) // ✅ Generated: (value: number) => this
      .operation('CREATE'); // ✅ Generated: (value: 'CREATE' | 'UPDATE' | 'DELETE') => this

    // Object-based API
    tag.with({
      user_id: userData.id,
      operation: 'CREATE',
      email: userData.email, // ✅ optional property
    });

    // ===== log - Log Messages =====
    log.info('Creating user').with({
      user_id: userData.id, // ✅ string
      operation: 'CREATE', // ✅ literal type
    });

    // Scoped attributes for the span
    setScope({ user_id: userData.id });

    // TypeScript knows exactly what's available:
    // tag.user_id        ✅ Available
    // tag.http_status    ✅ Available
    // tag.invalidProp    ❌ TypeScript error: doesn't exist
    return ok(userData);
  }
);
```

This provides complete compile-time safety while maintaining zero runtime overhead through prototype compilation.

### Reserved Method Names <a id="smoo/lmao!n/lmao-entry-reserved-method-names"></a>

The APIs reserve certain method names that cannot be used as attribute column names:

**On `tag` (TagWriter)**:

- **`with`**: Reserved on `tag` for bulk attribute setting: `tag.with({ ... })` is the bulk setter.

**On `log` (BaseSpanLogger)**:

- **`with`**: Used for fluent attribute chaining
- **`info`, `debug`, `warn`, `error`**: Used for log level methods

When defining attribute schemas, these names must be avoided to prevent conflicts with the API methods.

### Public vs Low-Level API Separation <a id="smoo/lmao!n/lmao-entry-public-vs-low-level-api-separation"></a>

**Span Attributes** (`tag`):

- `tag.attribute(value)` - chainable attribute setters
- `tag.with({ ... })` - object-based attribute setting
- Follows OpenTelemetry's `Span.setAttribute()` pattern

**Log Messages** (`log`):

- `info()`, `debug()`, `warn()`, `error()` - structured logging
- `with({ ... })` - fluent attribute chaining
- Every span creates a new `log` instance with reference to its buffer

**Scoped Attributes** (`setScope`, `scope`):

- `setScope({ ... })` - scoped attributes that propagate to all entries; `scope` reads the current values

**Span Completion** (`ok`, `err`):

- `ok(result)` - success with optional result data (creates `span-ok` entry)
- `err(error)` - error with optional context (creates `span-err` entry; a code error also fills `error_code`)
- Follows OpenTelemetry's `Span.setStatus()` pattern

**Child Spans** (`span`):

- `span(name, op, ...args)` - invoke another op as child span

**Low-Level Operations** (handled by context system):

- `span-start` entries created automatically when spans begin
- `span-exception` entries created when exceptions bypass normal completion
- Direct TypedArray writes for minimal overhead

Each span's context (`tag` + `log`) references its own buffer, avoiding trace-id/span-id appends and preserving
span-local row grouping in Arrow output. This is not a guarantee about vocabulary dictionary ordering.

The `log` API is for explicit logging during execution, while `ok()`/`err()` are for span completion. Both use the same
underlying entry type system and the span's own buffer.

## Fluent Result API <a id="smoo/lmao!n/lmao-entry-fluent-result-api"></a>

The `ok()` and `err()` functions return fluent result objects bound to the span context that created them. Chained
methods write to that span's reserved row 1 when they are called, and the objects keep ordinary TypeScript type
narrowing for Result pattern consumption.

### Row-1 Result Writes <a id="smoo/lmao!n/lmao-entry-row1-result-writes"></a>

A result captures its creating context's writer state (`_state`) once, when `ctx.ok()` or `ctx.err()` runs. That state
is both the write target and the completion owner. This ensures:

1. **Correct buffer targeting**: `.with()`, `.message()`, and `.line()` write to the creating span's row 1, never to a
   span that later receives the result
2. **No deferred work**: each chained call writes at once through the lazily materialized result writer; nothing is
   queued for span end
3. **Ownership**: the execution envelope completes a span only with a result whose `_state` is the executing context
   (see Span-End Write Sequence above)

```typescript
// What happens when you write:
return ctx.ok(user).with({ cached: true }).message('Created');

// 1. ctx.ok(user) creates an Ok holding the value and the context's writer state
// 2. .with({ cached: true }) writes the cached column of row 1
// 3. .message('Created') writes the message column of row 1
// 4. span() checks that the result belongs to the executing context, then calls writeSpanEnd(buffer, result)
// 5. writeSpanEnd writes entry_type and timestamp, plus error_code for an Err
```

### Ok <a id="smoo/lmao!n/lmao-entry-fluentok"></a>

Returned by `ctx.ok(value)`. It is the `Ok<V>` class with row-1 chaining methods:

```typescript
class Ok<V, T extends LogSchema = LogSchema> {
  readonly value: V;
  protected readonly _state: WriterState | undefined; // Creating context: write target and completion owner

  constructor(value: V, state?: WriterState) {
    this.value = value;
    this._state = state;
  }

  get success(): true {
    return true;
  } // Derived from class identity

  // Write attributes to row 1
  with(attributes: Partial<InferSchema<T>>): this;

  // Write the row 1 message (overwrites the span name in row 1)
  message(text: string): this;

  // Write the row 1 line number
  line(lineNumber: number): this;
}
```

### Err <a id="smoo/lmao!n/lmao-entry-fluenterr"></a>

Returned by `ctx.err(error)`. It is the `Err<E>` class with the same row-1 chaining methods. A code error built with
`defineCodeError` (for example `NOT_FOUND({ userId })`) carries the `code` that becomes `error_code`:

```typescript
class Err<E, T extends LogSchema = LogSchema> {
  readonly error: E;
  protected readonly _state: WriterState | undefined; // Creating context: write target and completion owner

  constructor(error: E, state?: WriterState) {
    this.error = error;
    this._state = state;
  }

  get success(): false {
    return false;
  } // Derived from class identity

  // Same chaining methods as Ok
  with(attributes: Partial<InferSchema<T>>): this;
  message(text: string): this;
  line(lineNumber: number): this;
}
```

### Buffer Write Timing <a id="smoo/lmao!n/lmao-entry-buffer-write-timing"></a>

Row 1 is written in two phases:

1. **Attributes, message, line**: written by the chained methods while the op body runs, before the result is returned
2. **Entry type**: written by `writeSpanEnd` at span completion, from `instanceof Ok` vs `Err`
3. **Timestamp**: written by `writeSpanEnd` at span-end time
4. **Error code**: written by `writeSpanEnd` from `result.error.code` when the error carries a string `code`

Row 1 reads as `span-exception` until `writeSpanEnd` runs, so a span whose body throws after a chained write still
records its exception outcome.

### Usage Examples <a id="smoo/lmao!n/lmao-entry-usage-examples"></a>

```typescript
const createUser = defineOp('createUser', async ({ ok, err }, userData) => {
  // Simple success - just the value
  return ok(user);

  // Success with attributes
  return ok(user).with({ rowsAffected: 1, cacheHit: false });

  // Success with message
  return ok(user).message('User created successfully');

  // Success with both attributes and message
  return ok(user).with({ rowsAffected: 1, duration: 42.5 }).message('User created successfully');

  // Simple error - a code error (NOT_FOUND and VALIDATION_FAILED come from defineCodeError)
  return err(NOT_FOUND({ userId: 'user-123' }));

  // Error with additional attributes
  return err(VALIDATION_FAILED({ field: 'email' })).with({
    attemptedValue: userData.email,
    validationRule: 'unique_constraint',
  });

  // Error with message
  return err(NOT_FOUND({ userId })).message('User not found in database');

  // Error with both attributes and message
  return err(VALIDATION_FAILED({ field: 'email' }))
    .with({ validationRule: 'format' })
    .message('Invalid email format provided');
});
```

### Result Pattern Compatibility <a id="smoo/lmao!n/lmao-entry-result-pattern-compatibility"></a>

The fluent result objects are fully compatible with TypeScript's discriminated union narrowing:

```typescript
const result = await userService.createUser(userData);

if (result.success) {
  // TypeScript knows: result is FluentSuccessResult
  // result.value is available
  console.log('Created user:', result.value.id);
} else {
  // TypeScript knows: result is Err
  // result.error.code and result.error.details are available
  console.log('Failed:', result.error.code, result.error.details);
}
```

### Error Code Storage <a id="smoo/lmao!n/lmao-entry-error-code-storage"></a>

When `err(code, details)` is called, the error code is automatically written to the `errorCode` column. This enables
efficient querying of errors by code:

```sql
SELECT * FROM traces
WHERE entry_type = 'span-err'
  AND errorCode = 'VALIDATION_FAILED';
```

### Message Storage <a id="smoo/lmao!n/lmao-entry-message-storage"></a>

Both `.message()` methods write to the unified `message` column, enabling message-based queries:

```sql
SELECT span_name, message, errorCode
FROM traces
WHERE entry_type IN ('span-ok', 'span-err')
  AND message IS NOT NULL;
```

## Low-Level Logging API <a id="smoo/lmao!n/lmao-entry-low-level-logging-api"></a>

The entry type system is implemented through low-level column writers that directly populate trace buffers:

### Column Writer Interface <a id="smoo/lmao!n/lmao-entry-column-writer-interface"></a>

The concrete column-writing API is the **schema-driven, codegen'd** writer set defined in
[Trace Context API Codegen](./01g_trace_context_api_codegen.md#1-schema-driven-column-writers); that spec is the source
of truth for the generated writers. The `writeColumnName()` interface below is an **example** — it names the columns
each entry type populates, not the generated method surface:

```typescript
// Example shape — the generated writers are specified in 01g §Schema-Driven Column Writers
interface ColumnWriters {
  // Core system columns
  writeTimestamp(value: bigint): void;
  writeTraceId(value: string): void;
  writeSpanId(value: bigint): void;
  writeParentSpanId(value: bigint | null): void;
  writeEntryType(value: EntryType): void;
  writeModule(value: string): void;
  writeMessage(value: string): void; // Span name, log message template, OR flag name (S.category)

  // Generated attribute columns (per-schema; see 01g §Schema-Driven Column Writers)
  writeHttpStatus(value: number): void;
  writeHttpMethod(value: string): void;
  writeUserId(value: string): void;
  // ... more generated based on schema
}
```

### Entry Type Creation Patterns <a id="smoo/lmao!n/lmao-entry-entry-type-creation-patterns"></a>

Each entry type populates a fixed set of columns; the per-type column contract below is normative (it matches
[Entry Type Validation](#entry-type-validation)). The `writeColumnName()` calls stand in for the codegen'd writers
([01g §Schema-Driven Column Writers](./01g_trace_context_api_codegen.md#1-schema-driven-column-writers)):

#### Span Start Pattern

```typescript
function createSpanStart(module: string, spanName: string, parentSpanId?: bigint, attributes?: Record<string, any>) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(generateSpanId());
  writers.writeParentSpanId(parentSpanId ?? null);
  writers.writeEntryType('span-start');
  writers.writeModule(module);
  writers.writeMessage(spanName); // message = span name for span entries

  // Optional structured attributes for span start
  // (attribute columns are written by the codegen'd writers — 01g §Schema-Driven Column Writers)
  if (attributes) {
    for (const [key, value] of Object.entries(attributes)) {
      const writerMethod = `write${capitalize(key)}`;
      if (writers[writerMethod]) {
        writers[writerMethod](value);
      }
    }
  }
}
```

#### Span Completion Pattern

```typescript
function createSpanCompletion(entryType: 'span-ok' | 'span-err', result?: any, attributes?: Record<string, any>) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(getCurrentSpanId());
  writers.writeParentSpanId(getCurrentParentSpanId());
  writers.writeEntryType(entryType);
  writers.writeModule(getCurrentModule());
  writers.writeMessage(getCurrentSpanName()); // message = span name for span entries

  // Structured attributes from completion
  // (attribute columns are written by the codegen'd writers — 01g §Schema-Driven Column Writers)
  if (attributes) {
    for (const [key, value] of Object.entries(attributes)) {
      const writerMethod = `write${capitalize(key)}`;
      if (writers[writerMethod]) {
        writers[writerMethod](value);
      }
    }
  }

  // Result data may also populate attribute columns
  // (implementation depends on how result data is structured)
}
```

#### Log Entry Pattern

```typescript
// Grammar: {fieldName} is a placeholder; {{ and }} encode literal braces.
// Example: 'User {userId} created' is stored as template identity, never rendered on the entry path.
// Values such as userId are written to their own typed columns.
function createLogEntry(
  level: 'info' | 'debug' | 'warn' | 'error',
  messageTemplate: string, // FORMAT STRING - stored as-is, NOT interpolated
  attributes?: Record<string, any>
) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(getCurrentSpanId());
  writers.writeParentSpanId(getCurrentParentSpanId());
  writers.writeEntryType(level);
  writers.writeModule(getCurrentModule());
  writers.writeMessage(messageTemplate); // message = message TEMPLATE (not interpolated!)

  // Attribute VALUES go in their own columns.
  // Template references such as {userId} are stored verbatim, not replaced.
  // (attribute columns are written by the codegen'd writers — 01g §Schema-Driven Column Writers)
  if (attributes) {
    for (const [key, value] of Object.entries(attributes)) {
      const writerMethod = `write${capitalize(key)}`;
      if (writers[writerMethod]) {
        writers[writerMethod](value);
      }
    }
  }
}
```

#### Span Attribute Pattern (tag)

**Note**: `tag` does NOT create a new entry. It updates row 0 (span-start row) directly.

```typescript
// tag writes directly to row 0's attribute columns
// No new entry type - just updating the span-start row
function writeTagAttribute(buffer: SpanBuffer, columnName: string, value: any) {
  // ALWAYS write to row 0 (span-start row) - overwrite semantics
  buffer[`${columnName}_values`][0] = value;
  buffer[`${columnName}_nulls`][0] |= 1; // Mark as valid
}

// Usage: tag.userId('123') → writeTagAttribute(buffer, 'userId', '123')
```

#### Feature Flag Pattern

```typescript
function createFeatureFlagEntry(
  entryType: 'ff-access' | 'ff-usage',
  flagName: string,
  flagValue: boolean | number | string,
  context: Record<string, any>
) {
  writers.writeTimestamp(BigInt(Date.now()));
  writers.writeTraceId(getCurrentTraceId());
  writers.writeSpanId(getCurrentSpanId());
  writers.writeParentSpanId(getCurrentParentSpanId());
  writers.writeEntryType(entryType);
  writers.writeModule(getCurrentModule());
  writers.writeMessage(flagName); // message = flag name for ff entries (unified column)

  // Feature flag value column (S.category for efficient storage of repeated values)
  writers.writeFfValue(String(flagValue)); // Serialized to string, category-interned

  if (entryType === 'ff-usage' && context) {
    // track(context) is applied as track().with(context)
    // Unknown keys are ignored by with(); known schema fields are written to this row.
    writers.with(context);
  }
}
```

## Span Duration Calculation <a id="smoo/lmao!n/lmao-entry-span-duration-calculation"></a>

The fixed row layout guarantees that duration is always computable:

```typescript
// Duration = timestamps[1] - timestamps[0]
// Works for ALL completion types: span-ok, span-err, AND span-exception
function getSpanDuration(buffer: SpanBuffer): number {
  return buffer.timestamp[1] - buffer.timestamp[0];
}
```

**Why This Works**:

- Row 0 (span-start) timestamp set at span creation
- Row 1 timestamp set at completion (ok/err) OR remains at creation time (exception)
- Even uncaught exceptions have valid duration (time from span-start to when exception was thrown)

**Query Pattern** (ClickHouse):

```sql
-- Duration from fixed row positions
SELECT
  thread_id,
  span_id,
  span_name,
  entry_type,
  -- Rows 0 and 1 are guaranteed to exist for every span
  max(timestamp) - min(timestamp) as duration_ms
FROM traces
WHERE entry_type IN ('span-start', 'span-ok', 'span-err', 'span-exception')
GROUP BY thread_id, span_id, span_name, entry_type;
```

## Entry Type Validation <a id="smoo/lmao!n/lmao-entry-entry-type-validation"></a>

The system enforces entry type constraints at the API level:

### Required Columns by Entry Type <a id="smoo/lmao!n/lmao-entry-required-columns-by-entry-type"></a>

- **All entry types**: `timestamp`, `trace_id`, `thread_id`, `span_id`, `entry_type`, `module`, `message`
- **Span lifecycle** (`span-start`, `span-ok`, `span-err`): `message` contains span name; `span-exception` row 1 holds
  the thrown value's message (see `span-exception` above)
- **Operational log types** (`info`, `warn`, `error`): `message` identifies a literal template (never interpolated)
- **Diagnostic log types** (`debug`, `trace`): `message` identifies a literal template or an allowed raw dynamic string
- **Feature flag types** (`ff-access`, `ff-usage`): `message` contains flag name, `ff_value` contains the evaluated
  value
- **Op metrics** (`op-invocations`, `op-errors`, `op-exceptions`, `op-duration-*`): `message` contains op name,
  `uint64_value` contains the metric value
- **Buffer metrics** (`buffer-writes`, `buffer-spans`, `buffer-capacity`): `uint64_value` contains the metric value
- **Period marker** (`period-start`): `uint64_value` contains the period start timestamp (nanoseconds)

### The `message` Column by Entry Type <a id="smoo/lmao!n/lmao-entry-the-message-column-by-entry-type"></a>

| Entry Type                           | `message` Contains                                             |
| ------------------------------------ | -------------------------------------------------------------- |
| `span-start`, `span-ok`, `span-err`  | Span name (e.g., `'create-user'`)                              |
| `span-exception`                     | Outer `Error` message, or `<typeof>: <String(value)>`, bounded |
| `info`, `warn`, `error`              | Literal operational template (e.g., `'User {userId} created'`) |
| `debug`, `trace`                     | Literal template or allowed raw dynamic diagnostic string      |
| `ff-access`, `ff-usage`              | Flag name (e.g., `'advancedValidation'`, `'darkMode'`)         |
| `op-*` (all 8 op metric types)       | Op name (e.g., `'GET'`, `'createUser'`)                        |
| `period-start`, `buffer-*` (4 types) | unused (null)                                                  |

### Fixed Row Constraints <a id="smoo/lmao!n/lmao-entry-fixed-row-constraints"></a>

- **Row 0**: Always `span-start` - created at span initialization
- **Row 1**: Always completion type (`span-ok`, `span-err`, or `span-exception`) - pre-initialized as `span-exception`
- **Row 2+**: Event entries (`trace`, `debug`, `info`, `warn`, `error`, `ff-access`, `ff-usage`)

**Metrics rows** (`period-start`, `op-*`, `buffer-*`) are written during flush cycles, not during span execution. They
are appended to the global trace buffer, not to individual SpanBuffers.

### Forbidden Combinations <a id="smoo/lmao!n/lmao-entry-forbidden-combinations"></a>

- **Span completion without start**: `span-ok`/`span-err`/`span-exception` must have matching `span-start`
- **Orphaned spans**: All spans except root must have valid `parent_thread_id` and `parent_span_id`
- **Mixed concerns**: Feature flag columns only valid with `ff-access`/`ff-usage` entry types

## Performance Characteristics <a id="smoo/lmao!n/lmao-entry-performance-characteristics"></a>

### Dictionary Encoding Benefits <a id="smoo/lmao!n/lmao-entry-dictionary-encoding-benefits"></a>

Entry types use Arrow's dictionary encoding:

- **Storage efficiency**: Each unique entry type string stored once
- **Query performance**: Numeric comparisons instead of string matching
- **Memory efficiency**: References are small integers

### Hot Path Optimization <a id="smoo/lmao!n/lmao-entry-hot-path-optimization"></a>

The entry type system is designed for minimal hot path overhead:

- **Pre-generated writers**: Column writers are generated and bound during startup
- **Specialized callsites**: Static, structured, and dynamic callsite classes each have a fixed path
- **Physical mixed layout**: Only a buffer with both static-index and dynamic-reference lanes is called mixed
- **Direct memory writes**: Warmed in-capacity calls perform fixed stores without intermediate objects

## Integration Points <a id="smoo/lmao!n/lmao-entry-integration-points"></a>

This entry type system integrates with:

- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Entry types become the `entry_type` column
- **[Trace Context API Codegen](./01g_trace_context_api_codegen.md)**: Generated APIs use these primitives
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Column writers populate buffer arrays
- **[Context Flow and Op Wrappers](./01c_context_flow_and_op_wrappers.md)**: Op lifecycle creates span entry types

The entry type system provides the foundational vocabulary for all trace events, ensuring consistency and performance
across the entire logging system.
