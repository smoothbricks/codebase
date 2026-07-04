# Transient Errors and Retry System <a id="smoo/lmao!n/op-retry.transient-errors-and-retry-system"></a>

## Overview <a id="smoo/lmao!n/op-retry.overview"></a>

The transient error and retry system provides automatic retry logic for operations that may temporarily fail due to
external dependencies. This pattern is common in distributed systems where network issues, rate limiting, or service
unavailability require graceful retry behavior.

Key components:

1. **TransientError class**: Error type that signals a retry-eligible failure
2. **RetryPolicy**: Configurable backoff strategies (exponential, linear, fixed)
3. **Blocked class**: Error type for non-retry scenarios (dependency unavailable)
4. **executeWithRetry**: Retry loop in span execution
5. **span-retry entries**: Observability entries for retry tracking

## Error Type Hierarchy <a id="smoo/lmao!n/op-context-tagged-errors.error-type-hierarchy"></a>

```
Error (JavaScript built-in)
├── CodeError<C, D> (base class for typed errors)        errors/CodeError.ts
│   ├── code: C
│   └── data: D
│
├── TransientError<C, D> extends CodeError<C, D>          errors/Transient.ts
│   └── policy: RetryPolicy
│
├── Blocked extends Error (TaggedError<'Blocked'>)        errors/Blocked.ts
│   ├── reason: BlockedReason
│   └── blockedConfig?: BlockedConfig   (closure-based nextRetry)
│
└── RetriesExhausted extends Error                        errors/RetriesExhausted.ts
```

**Key Distinction**: `TransientError` triggers retry; `Blocked`, `CodeError`, and `RetriesExhausted` do not.

**Implementation status:** `TransientError` extends `CodeError<C, D>` (not bare `Error`), inheriting the
`code`/`data`/`_tag` pattern and adding the embedded `policy`. `Blocked` is a `TaggedError<'Blocked'>` carrying a
`reason` union (`service` / `ended` / `index`) and an optional closure-based `BlockedConfig` (`maxAttempts` +
`nextRetry(attempt)`), not a flat `resource` string. `RetriesExhausted` (`errors/RetriesExhausted.ts`) is the terminal
error when a `Blocked` retry budget is consumed; both `Blocked` and `RetriesExhausted` are tree-shakable built-ins for
workflow-engine integration. Realized in `errors/` (`n/op-context-tagged-errors` — `Blocked.ts` fenced `.blocked`).

## TransientError Class <a id="smoo/lmao!n/op-retry.transienterror-class"></a>

TransientError represents a temporary failure that should be retried according to a configurable policy.

### Factory Pattern <a id="smoo/lmao!n/op-retry.factory-pattern"></a>

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

### Transient Factory Signature <a id="smoo/lmao!n/op-retry.transient-factory-signature"></a>

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

### TransientError Instance <a id="smoo/lmao!n/op-retry.transienterror-instance"></a>

```typescript
class TransientError<C extends string = string, T = unknown> extends Error {
  readonly code: C;
  readonly data: T;
  readonly policy: RetryPolicy;

  // Checked by retry loop via instanceof
  static readonly isTransient = true;
}
```

## RetryPolicy <a id="smoo/lmao!n/op-retry.retrypolicy"></a>

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

### Backoff Strategy Helpers <a id="smoo/lmao!n/op-retry.backoff-strategy-helpers"></a>

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

### Delay Calculation <a id="smoo/lmao!n/op-retry.delay-calculation"></a>

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

## Blocked Class <a id="smoo/lmao!n/op-context-tagged-errors.blocked-class"></a>

Blocked represents a non-transient blocking condition - the operation cannot proceed and should NOT be retried by the
LMAO span retry loop (it signals structural unavailability that needs coordination at the engine level). This is
distinct from TransientError which indicates temporary failure. Realized in `errors/Blocked.ts` (fenced
`n/op-context-tagged-errors.blocked`).

### Factory Methods <a id="smoo/lmao!n/op-context-tagged-errors.factory-methods"></a>

```typescript
class Blocked extends Error implements TaggedError<'Blocked'> {
  static readonly _tag = 'Blocked' as const;
  readonly reason: BlockedReason;
  readonly blockedConfig: BlockedConfig | undefined;

  // Factory methods for common blocking scenarios
  static service(name: string, config?: BlockedConfig): Blocked;
  static index(indexName: string, config?: BlockedConfig): Blocked;
  static ended(target: string, config?: BlockedConfig): Blocked;
}

// BlockedReason union (no `resource` variant — service/ended/index only)
type BlockedReason =
  | { readonly type: 'service'; readonly name: string }
  | { readonly type: 'ended'; readonly target: string }
  | { readonly type: 'index'; readonly indexName: string };

// Closure-based retry config — the closure captures Op execution context, so each
// re-execution can produce a fresh delay (e.g. updated Retry-After headers).
interface BlockedConfig {
  readonly maxAttempts?: number; // default 5, then RetriesExhausted
  readonly nextRetry?: (attempt: number) => number; // delay ms before next retry
}
```

### Usage <a id="smoo/lmao!n/op-context-tagged-errors.usage"></a>

```typescript
import { Blocked } from '@smoothbricks/lmao/errors/Blocked';

const processOrder = defineOp('processOrder', async (ctx, orderId: string) => {
  // Check if payment service is available
  if (!(await isServiceHealthy('payment-api'))) {
    // Do NOT retry in the span loop - service is down, wait for recovery
    return new Err(Blocked.service('payment-api'));
  }

  // Check if order execution already completed
  const execution = await getExecution(orderId);
  if (execution.ended) {
    // Do NOT retry - execution already finished
    return new Err(Blocked.ended(execution.id));
  }

  // This CAN be retried if it fails transiently
  return ctx.span('charge-payment', chargePayment, orderId);
});

// Discriminate blocked from other errors at the call site
const result = await trace('order', processOrder, orderId);
if (result.isErr(Blocked)) {
  // Handle temporary unavailability - retry, queue, etc.
}
```

## Retry Loop Implementation <a id="smoo/lmao!n/op-retry.retry-loop-implementation"></a>

The retry loop is implemented in span execution (`spanContext.ts`), not at the tracer level. This means:

- Root traces via `tracer.trace()` do NOT have retry
- Child spans via `ctx.span()` DO have retry
- The span buffer captures all retry attempts via `span-retry` entries

### executeWithRetry Flow <a id="smoo/lmao!n/op-retry.executewithretry-flow"></a>

The loop, `calculateDelay`, and `sleep` live in `spanContext.ts`, fenced `n/op-retry.loop` and `n/op-retry.delay`;
success/error is discriminated by `instanceof Ok` / `Err` (not a `.success` field). One delay is computed and reused for
both the `span-retry` entry and the `sleep`.

```typescript
async function executeWithRetry<T extends LogSchema, S, E>(
  buffer: SpanBuffer<T>,
  fn: () => Result<S, E> | Promise<Result<S, E>>
): Promise<Result<S, E>> {
  let attempt = 0;

  while (true) {
    attempt++;
    const result = await fn();

    // Success - return immediately
    if (result instanceof Ok) {
      return result;
    }

    const error = (result as Err<E>).error;

    // Blocked - return immediately, no retry (structural unavailability)
    if (error instanceof Blocked) {
      return result;
    }

    // TransientError - check if we should retry
    if (error instanceof TransientError) {
      const { policy } = error;

      // Exhausted attempts (maxAttempts includes the initial) - return final error
      if (attempt >= policy.maxAttempts) {
        return result;
      }

      // Compute the delay once, reuse for the entry and the wait
      const delay = calculateDelay(policy, attempt);
      writeRetryEntry(buffer, attempt, error, delay);
      await sleep(delay);
      continue;
    }

    // Non-transient error (CodeError or other) - return immediately
    return result;
  }
}
```

### writeRetryEntry <a id="smoo/lmao!n/op-retry.writeretryentry"></a>

Each retry attempt writes a `span-retry` entry to the buffer for observability. Realized in `spanContext.ts`, fenced
`n/lmao-entry-span-lifecycle-entry-types.retry` (the entry-type's spec side is `n/lmao-entry-retry-entry-type`). Beyond
the message + error code, it writes `retry_attempt` and `retry_delay_ms` so attempt/delay are directly queryable columns
in the Arrow output:

```typescript
function writeRetryEntry<T extends LogSchema>(
  buffer: SpanBuffer<T>,
  attempt: number,
  error: TransientError<string, unknown>,
  delayMs: number
): void {
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

  // Retry metadata for direct queryability
  buffer.retry_attempt(index, attempt);
  buffer.retry_delay_ms(index, delayMs);
}
```

### Entry Type <a id="smoo/lmao!n/op-retry.entry-type"></a>

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

## Op Class Integration <a id="smoo/lmao!n/op-retry.op-class-integration"></a>

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

## Observability <a id="smoo/lmao!n/lmao-entry-retry-entry-type.observability"></a>

### span-retry Entry Format <a id="smoo/lmao!n/lmao-entry-retry-entry-type.span-retry-entry-format"></a>

Each retry attempt produces a `span-retry` entry in the span buffer:

| Column         | Value                       | Description                  |
| -------------- | --------------------------- | ---------------------------- |
| entry_type     | 5 (ENTRY_TYPE_SPAN_RETRY)   | Identifies retry entry       |
| timestamp      | nanoseconds since epoch     | When retry was triggered     |
| message        | `retry:op:{opName}`         | Queryable pattern            |
| error_code     | e.g., `SERVICE_UNAVAILABLE` | Error that caused retry      |
| retry_attempt  | attempt number (1-indexed)  | Which attempt triggered this |
| retry_delay_ms | delay before next attempt   | Backoff delay applied (ms)   |

### Querying Retries <a id="smoo/lmao!n/lmao-entry-retry-entry-type.querying-retries"></a>

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

### Metrics Derivation <a id="smoo/lmao!n/lmao-entry-retry-entry-type.metrics-derivation"></a>

From span-retry entries, you can derive:

- **Retry rate**: `COUNT(span-retry) / COUNT(span-start)`
- **Retry distribution**: Histogram of retries per span
- **Error breakdown**: Which error codes cause most retries
- **Retry success rate**: Spans that succeeded after retry vs exhausted retries

## Best Practices <a id="smoo/lmao!n/op-retry.best-practices"></a>

### When to Use TransientError <a id="smoo/lmao!n/op-retry.when-to-use-transienterror"></a>

Use TransientError for:

- Network timeouts
- HTTP 5xx responses
- Connection refused/reset
- Rate limiting (429)
- Temporary resource unavailability
- Optimistic locking failures

### When to Return vs Throw <a id="smoo/lmao!n/op-retry.when-to-return-vs-throw"></a>

- Retry-eligible failures must be returned as `ctx.err(Transient(...))`, not thrown.
- `ctx.err(...)`/`Err` covers all known operational outcomes (transient, blocked, code/business errors).
- Throw only for unexpected invariant failures (bugs, impossible states, broken runtime contracts).
- `span-retry` and `span-err` entries are driven by returned errors; `span-exception` indicates unexpected throw paths.

### When to Use Blocked <a id="smoo/lmao!n/op-retry.when-to-use-blocked"></a>

Use Blocked for:

- Service explicitly unavailable (circuit breaker open)
- Required dependency offline
- Execution already completed
- Resource permanently gone (404)

### When to Use CodeError <a id="smoo/lmao!n/op-retry.when-to-use-codeerror"></a>

Use CodeError for:

- Validation failures
- Business rule violations
- Authentication/authorization errors
- Client errors (4xx except 429)

### Policy Tuning <a id="smoo/lmao!n/op-retry.policy-tuning"></a>

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

## Integration Points <a id="smoo/lmao!n/op-retry.integration-points"></a>

This system integrates with:

- **[Context Flow and Op Wrappers](./01c_context_flow_and_op_wrappers.md)**: span() invokes executeWithRetry
- **[Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md)**: ENTRY_TYPE_SPAN_RETRY constant
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: span-retry entries written to buffer
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: span-retry entries included in Arrow output

## Files <a id="smoo/lmao!n/op-retry.files"></a>

| File                                               | Purpose                                                             |
| -------------------------------------------------- | ------------------------------------------------------------------- |
| `packages/lmao/src/lib/errors/Transient.ts`        | `TransientError` class + `Transient()` factory (`.transient` fence) |
| `packages/lmao/src/lib/errors/retry-policy.ts`     | `RetryPolicy` + backoff helpers + `mergePolicy` (`.policy` fence)   |
| `packages/lmao/src/lib/errors/Blocked.ts`          | `Blocked` class (`.blocked` fence)                                  |
| `packages/lmao/src/lib/errors/RetriesExhausted.ts` | Terminal error when a `Blocked` retry budget is exhausted           |
| `packages/lmao/src/lib/spanContext.ts`             | `executeWithRetry` / `calculateDelay` / `sleep` / `writeRetryEntry` |
| `packages/lmao/src/lib/schema/systemSchema.ts`     | `ENTRY_TYPE_SPAN_RETRY` constant                                    |
