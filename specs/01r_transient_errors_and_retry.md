# Transient Errors and Retry System

## Overview

The transient error and retry system provides automatic retry logic for operations that may temporarily fail due to
external dependencies. This pattern is common in distributed systems where network issues, rate limiting, or service
unavailability require graceful retry behavior.

Key components:

1. **TransientError class**: Error type that signals a retry-eligible failure
2. **RetryPolicy**: Configurable backoff strategies (exponential, linear, fixed)
3. **Blocked class**: Error type for non-retry scenarios (dependency unavailable)
4. **executeWithRetry**: Retry loop in span execution
5. **span-retry entries**: Observability entries for retry tracking

## Error Type Hierarchy

```
Error (JavaScript built-in)
├── CodeError (base class for typed errors)
│   ├── code: string
│   └── data: T
│
├── TransientError extends Error
│   ├── code: string
│   ├── data: T
│   └── policy: RetryPolicy
│
└── Blocked extends Error
    ├── reason: BlockedReason
    └── resource: string
```

**Key Distinction**: TransientError triggers retry, Blocked and CodeError do not.

## TransientError Class

TransientError represents a temporary failure that should be retried according to a configurable policy.

### Factory Pattern

```typescript
// Define a transient error code with default policy
const SERVICE_UNAVAILABLE = Transient<{ status?: number }>('SERVICE_UNAVAILABLE', exponentialBackoff(3));

const RATE_LIMITED = Transient<{ retryAfter?: number }>(
  'RATE_LIMITED',
  fixedDelay(3, 5000) // 3 attempts, 5 second delay
);

// Use in an op
const fetchData = defineOp('fetchData', async (ctx, url: string) => {
  try {
    const res = await fetch(url);
    if (res.status === 503) {
      return ctx.err(SERVICE_UNAVAILABLE({ status: res.status }));
    }
    if (res.status === 429) {
      // Override default policy with rate limit header
      const retryAfter = parseInt(res.headers.get('Retry-After') || '5000');
      return ctx.err(RATE_LIMITED({ retryAfter }, { baseDelayMs: retryAfter * 1000 }));
    }
    return ctx.ok(await res.json());
  } catch (e) {
    return ctx.err(SERVICE_UNAVAILABLE({ status: 0 }));
  }
});
```

### Transient Factory Signature

```typescript
function Transient<T>(code: string, defaultPolicy: RetryPolicy): TransientErrorFactory<T>;

// TransientErrorFactory allows data + optional policy override
type TransientErrorFactory<T> = {
  // With data only (uses default policy)
  (data: T): TransientError<T>;

  // With data and policy override (merges with default)
  (data: T, policyOverride: Partial<RetryPolicy>): TransientError<T>;

  // With full policy replacement (for void data)
  (policy: RetryPolicy): TransientError<void>;
};
```

### TransientError Instance

```typescript
class TransientError<C extends string = string, T = unknown> extends Error {
  readonly code: C;
  readonly data: T;
  readonly policy: RetryPolicy;

  // Checked by retry loop via instanceof
  static readonly isTransient = true;
}
```

## RetryPolicy

RetryPolicy configures how retries are performed:

```typescript
interface RetryPolicy {
  /** Backoff strategy: exponential, linear, or fixed */
  backoff: 'exponential' | 'linear' | 'fixed';

  /** Maximum number of attempts (including initial) */
  maxAttempts: number;

  /** Base delay in milliseconds */
  baseDelayMs: number;

  /** Maximum delay in milliseconds (caps exponential growth) */
  maxDelayMs?: number;

  /** Add random jitter to delays (default: true for exponential/linear) */
  jitter?: boolean;
}
```

### Backoff Strategy Helpers

```typescript
// Exponential backoff: delay = baseDelayMs * 2^(attempt-1)
// Example: 100ms, 200ms, 400ms, 800ms...
function exponentialBackoff(maxAttempts: number, baseDelayMs = 100): RetryPolicy {
  return {
    backoff: 'exponential',
    maxAttempts,
    baseDelayMs,
    jitter: true, // Default jitter for exponential
  };
}

// Linear backoff: delay = baseDelayMs * attempt
// Example: 100ms, 200ms, 300ms, 400ms...
function linearBackoff(maxAttempts: number, baseDelayMs = 100): RetryPolicy {
  return {
    backoff: 'linear',
    maxAttempts,
    baseDelayMs,
    jitter: true,
  };
}

// Fixed delay: delay = baseDelayMs (constant)
// Example: 5000ms, 5000ms, 5000ms...
function fixedDelay(maxAttempts: number, baseDelayMs: number): RetryPolicy {
  return {
    backoff: 'fixed',
    maxAttempts,
    baseDelayMs,
    jitter: false, // No jitter for fixed delay
  };
}
```

### Delay Calculation

```typescript
function calculateDelay(policy: RetryPolicy, attempt: number): number {
  let delay: number;

  switch (policy.backoff) {
    case 'exponential':
      // 2^(attempt-1) * baseDelayMs
      delay = policy.baseDelayMs * Math.pow(2, attempt - 1);
      break;
    case 'linear':
      // attempt * baseDelayMs
      delay = policy.baseDelayMs * attempt;
      break;
    case 'fixed':
      delay = policy.baseDelayMs;
      break;
  }

  // Apply maxDelayMs cap
  if (policy.maxDelayMs !== undefined) {
    delay = Math.min(delay, policy.maxDelayMs);
  }

  // Apply jitter (0-50% additional delay)
  if (policy.jitter) {
    delay = delay + Math.random() * delay * 0.5;
  }

  return Math.floor(delay);
}
```

## Blocked Class

Blocked represents a non-transient blocking condition - the operation cannot proceed and should NOT be retried. This is
distinct from TransientError which indicates temporary failure.

### Factory Methods

```typescript
class Blocked extends Error {
  readonly reason: BlockedReason;
  readonly resource: string;

  // Factory methods for common blocking scenarios
  static service(name: string): Blocked;
  static index(name: string): Blocked;
  static ended(executionId: string): Blocked;
  static resource(type: string, id: string): Blocked;
}

// BlockedReason types
type BlockedReason =
  | { type: 'service'; name: string }
  | { type: 'index'; name: string }
  | { type: 'ended'; executionId: string }
  | { type: 'resource'; resourceType: string; resourceId: string };
```

### Usage

```typescript
const processOrder = defineOp('processOrder', async (ctx, orderId: string) => {
  // Check if payment service is available
  if (!(await isServiceHealthy('payment-api'))) {
    // Do NOT retry - service is down, wait for recovery
    return ctx.err(Blocked.service('payment-api'));
  }

  // Check if order execution already completed
  const execution = await getExecution(orderId);
  if (execution.ended) {
    // Do NOT retry - execution already finished
    return ctx.err(Blocked.ended(execution.id));
  }

  // This CAN be retried if it fails transiently
  return ctx.span('charge-payment', chargePayment, orderId);
});
```

## Retry Loop Implementation

The retry loop is implemented in span execution (`spanContext.ts`), not at the tracer level. This means:

- Root traces via `tracer.trace()` do NOT have retry
- Child spans via `ctx.span()` DO have retry
- The span buffer captures all retry attempts via `span-retry` entries

### executeWithRetry Flow

```typescript
async function executeWithRetry<T, E>(buffer: SpanBuffer, fn: () => Promise<Result<T, E>>): Promise<Result<T, E>> {
  let attempt = 0;
  let lastError: TransientError | null = null;

  while (true) {
    attempt++;
    const result = await fn();

    // Success - return immediately
    if (result.success) {
      return result;
    }

    // Check error type
    const error = result.error;

    // Blocked - return immediately, no retry
    if (error instanceof Blocked) {
      return result;
    }

    // TransientError - check if we should retry
    if (error instanceof TransientError) {
      const policy = error.policy;

      // Exhausted attempts - return final error
      if (attempt >= policy.maxAttempts) {
        return result;
      }

      // Write span-retry entry for observability
      writeRetryEntry(buffer, attempt, error, calculateDelay(policy, attempt));

      // Wait before retry
      await sleep(calculateDelay(policy, attempt));

      // Continue loop for next attempt
      lastError = error;
      continue;
    }

    // Non-transient error (CodeError or other) - return immediately
    return result;
  }
}
```

### writeRetryEntry

Each retry attempt writes a `span-retry` entry to the buffer for observability:

```typescript
function writeRetryEntry(buffer: SpanBuffer, attempt: number, error: TransientError, delayMs: number): void {
  // Get current write index
  const index = buffer._writeIndex;

  // Write entry via TraceRoot for timestamp handling
  buffer._traceRoot.writeLogEntry(buffer, ENTRY_TYPE_SPAN_RETRY);

  // Write retry message: retry:op:{opName}
  const opName = buffer._opMetadata?.name ?? 'unknown';
  buffer.message(index, `retry:op:${opName}`);

  // Write error code
  if (error.code) {
    buffer.error_code(index, error.code);
  }
}
```

### Entry Type

```typescript
// In systemSchema.ts
export const ENTRY_TYPE_SPAN_RETRY = 5;

// Entry types summary:
// 1 = span-start
// 2 = span-ok
// 3 = span-err
// 4 = span-exception
// 5 = span-retry (new)
// 6+ = log levels (info, debug, warn, error)
```

## Op Class Integration

The Op class wraps span execution with retry logic. The flow is:

1. Parent calls `ctx.span('name', op, ...args)`
2. SpanContext creates child buffer
3. `executeWithRetry` wraps the op invocation
4. On TransientError: write span-retry entry, wait, retry
5. On Blocked/CodeError: return immediately
6. On success or exhausted retries: return result

```
ctx.span('fetch-data', fetchDataOp, url)
    │
    ▼
SpanContext.span0-span8()
    │
    ├─► Create child SpanBuffer
    │
    ├─► writeSpanStart(buffer, 'fetch-data')
    │
    ├─► executeWithRetry(buffer, async () => {
    │       return op.fn(childCtx, ...args)
    │   })
    │   │
    │   ├─► Attempt 1: SERVICE_UNAVAILABLE → write span-retry, delay
    │   │
    │   ├─► Attempt 2: SERVICE_UNAVAILABLE → write span-retry, delay
    │   │
    │   └─► Attempt 3: success → return Ok
    │
    └─► writeSpanEnd(buffer, result)
```

## Observability

### span-retry Entry Format

Each retry attempt produces a `span-retry` entry in the span buffer:

| Column     | Value                       | Description              |
| ---------- | --------------------------- | ------------------------ |
| entry_type | 5 (ENTRY_TYPE_SPAN_RETRY)   | Identifies retry entry   |
| timestamp  | nanoseconds since epoch     | When retry was triggered |
| message    | `retry:op:{opName}`         | Queryable pattern        |
| error_code | e.g., `SERVICE_UNAVAILABLE` | Error that caused retry  |

### Querying Retries

```sql
-- Find all retries
SELECT * FROM traces WHERE message LIKE 'retry:op:%'

-- Count retries by error code
SELECT error_code, COUNT(*) as retry_count
FROM traces
WHERE entry_type = 5
GROUP BY error_code

-- Find traces with excessive retries
SELECT trace_id, COUNT(*) as retry_count
FROM traces
WHERE entry_type = 5
GROUP BY trace_id
HAVING COUNT(*) > 5
```

### Metrics Derivation

From span-retry entries, you can derive:

- **Retry rate**: `COUNT(span-retry) / COUNT(span-start)`
- **Retry distribution**: Histogram of retries per span
- **Error breakdown**: Which error codes cause most retries
- **Retry success rate**: Spans that succeeded after retry vs exhausted retries

## Best Practices

### When to Use TransientError

Use TransientError for:

- Network timeouts
- HTTP 5xx responses
- Connection refused/reset
- Rate limiting (429)
- Temporary resource unavailability
- Optimistic locking failures

### When to Use Blocked

Use Blocked for:

- Service explicitly unavailable (circuit breaker open)
- Required dependency offline
- Execution already completed
- Resource permanently gone (404)

### When to Use CodeError

Use CodeError for:

- Validation failures
- Business rule violations
- Authentication/authorization errors
- Client errors (4xx except 429)

### Policy Tuning

```typescript
// API calls - exponential backoff with cap
const API_UNAVAILABLE = Transient('API_UNAVAILABLE', {
  backoff: 'exponential',
  maxAttempts: 5,
  baseDelayMs: 100,
  maxDelayMs: 10000, // Cap at 10s
  jitter: true,
});

// Rate limiting - fixed delay from server
const RATE_LIMITED = Transient('RATE_LIMITED', fixedDelay(3, 5000));

// Database deadlock - linear backoff
const DEADLOCK = Transient('DEADLOCK', linearBackoff(3, 50));

// Quick retry for transient failures
const NETWORK_BLIP = Transient('NETWORK_BLIP', {
  backoff: 'fixed',
  maxAttempts: 2,
  baseDelayMs: 10,
  jitter: false,
});
```

## Integration Points

This system integrates with:

- **[Context Flow and Op Wrappers](./01c_context_flow_and_op_wrappers.md)**: span() invokes executeWithRetry
- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: ENTRY_TYPE_SPAN_RETRY constant
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: span-retry entries written to buffer
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: span-retry entries included in Arrow output

## Files

| File                                           | Purpose                           |
| ---------------------------------------------- | --------------------------------- |
| `packages/lmao/src/lib/errors/Transient.ts`    | TransientError class and factory  |
| `packages/lmao/src/lib/errors/Blocked.ts`      | Blocked class                     |
| `packages/lmao/src/lib/spanContext.ts`         | executeWithRetry, writeRetryEntry |
| `packages/lmao/src/lib/schema/systemSchema.ts` | ENTRY_TYPE_SPAN_RETRY constant    |
