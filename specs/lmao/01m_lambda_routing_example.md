# Lambda Routing Example: Library Composition with `defineOp()` + `span()` <a id="smoo/lmao!n/lambda-routing-example"></a>

> **Example.** This worked example shows op-wrapped operations, `span(name, op, …args)` invocation, `tag`/`log`,
> dependency injection via `deps`, prefix-namespaced columns, shared dependency instances, and the resulting prefixed
> Arrow table. The authored surface is specified in [`01l_op_context_pattern.md`](./01l_op_context_pattern.md).

## Overview <a id="smoo/lmao!n/lambda-routing-example-overview"></a>

This spec demonstrates how the LMAO op-context API enables elegant library composition through a practical Lambda
routing example. It showcases:

1. **Clean library authoring** with unprefixed schemas
2. **Dependency injection** via `deps` declarations
3. **Shared dependency instances** across multiple consumers
4. **The `span()` pattern** as the universal invocation mechanism
5. **Op-based routing** for Lambda handlers

## The Pattern: `defineOp()` + `span()` <a id="smoo/lmao!n/lambda-routing-example-pattern"></a>

The key insight is that **every cross-cutting operation should be wrapped in a span**. This gives you:

- Automatic timing and error tracking
- Proper parent-child trace relationships
- Attribute isolation per operation
- Clean composition via dependency injection

```typescript
// The universal pattern:
const result = await span('operation-name', someOp, ...args);
```

## Complete Example: Lambda Op Router <a id="smoo/lmao!n/lambda-routing-example-router"></a>

### Auth Library (`@mycompany/auth-tracing`) <a id="smoo/lmao!n/lambda-routing-example-router.auth-library"></a>

A simple authentication library that validates tokens and extracts user information.

```typescript
// @mycompany/auth-tracing/src/index.ts
import { defineOpContext, S } from '@smoothbricks/lmao';

// Define the op context with an unprefixed log schema
const { defineOp, defineOps } = defineOpContext({
  logSchema: {
    tokenType: S.enum(['bearer', 'api-key', 'session']),
    userId: S.category(),
    valid: S.boolean(),
  },
});

// Token decoding helper (implementation details)
function decodeToken(token: string): DecodedToken {
  // JWT decode, API key lookup, session validation, etc.
  return { type: 'bearer', userId: 'user-123', permissions: ['read', 'write'] };
}

// Define operation - receives { tag, log, span, deps, ok, err } context
export const validateToken = defineOp('validateToken', async ({ tag, log, ok }, token: string) => {
  log.info('Validating token');

  const decoded = decodeToken(token);

  // Tag attributes using unprefixed names
  // Final column names depend on prefix applied at composition time
  tag.tokenType(decoded.type);
  tag.userId(decoded.userId);
  tag.valid(true);

  log.info('Token validated successfully');

  return ok({ userId: decoded.userId, permissions: decoded.permissions });
});

// Another auth operation
export const refreshToken = defineOp('refreshToken', async ({ tag, log, ok }, refreshToken: string) => {
  log.info('Refreshing token');
  tag.tokenType('bearer');

  // ... refresh logic ...

  tag.valid(true);
  return ok({ accessToken: 'new-token', expiresIn: 3600 });
});

// Batch the ops into the exported OpGroup
export const authModule = defineOps({ validateToken, refreshToken });
```

**Key Points**:

- Schema uses unprefixed names (`userId`, not `auth_userId`)
- `defineOp()` creates traced operations with automatic span management
- Context provides `{ tag, log, span, deps, ok, err }` - destructure what you need; every op returns `ok()` or `err()`
- No prefix concerns in library code - clean, domain-focused

### HTTP Library (`@mycompany/http-tracing`) <a id="smoo/lmao!n/lambda-routing-example-router.http-library"></a>

An HTTP client library that can optionally authenticate requests using the auth library.

```typescript
// @mycompany/http-tracing/src/index.ts
import { defineOpContext, S } from '@smoothbricks/lmao';
import { authModule } from '@mycompany/auth-tracing';

// HTTP module declares auth as a dependency
const { defineOp, defineOps } = defineOpContext({
  logSchema: {
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
    url: S.text().masked('url'), // URL masking for PII
    status: S.number(),
    duration: S.number(),
  },
  deps: {
    auth: authModule, // Declare dependency - will be injected at composition time
  },
});

interface RequestOpts {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  body?: unknown;
  token?: string;
  headers?: Record<string, string>;
}

// Core request operation - uses auth dependency via span()
export const request = defineOp(
  'request',
  async ({ span, tag, log, deps, ok, err }, url: string, opts: RequestOpts) => {
    const startTime = performance.now();

    // Destructure deps for ergonomic access
    const { auth } = deps;

    // Tag request attributes
    tag.method(opts.method);
    tag.url(url);

    log.info('Starting HTTP request');

    // If token provided, authenticate via auth dependency
    // span() creates a child span for the auth operation
    if (opts.token) {
      const authResult = await span('auth', auth.validateToken, opts.token);
      if (authResult.isErr()) return err(authResult.error); // Rebuild the child's outcome on this context
      // authResult.value contains { userId, permissions }
      log.info('Request authenticated');
    }

    // Perform the actual HTTP request
    const response = await fetch(url, {
      method: opts.method,
      body: opts.body ? JSON.stringify(opts.body) : undefined,
      headers: {
        'Content-Type': 'application/json',
        ...opts.headers,
      },
    });

    // Tag response attributes
    tag.status(response.status);
    tag.duration(performance.now() - startTime);

    log.info('HTTP request completed');

    return ok(response);
  }
);

// Convenience methods that delegate to request via span(); each rebuilds the child's outcome on its own context
export const GET = defineOp('GET', async ({ span, ok, err }, url: string, opts?: { token?: string }) => {
  const res = await span('request', request, url, { method: 'GET' as const, ...opts });
  return res.isOk() ? ok(res.value) : err(res.error);
});

export const POST = defineOp(
  'POST',
  async ({ span, ok, err }, url: string, body: unknown, opts?: { token?: string }) => {
    const res = await span('request', request, url, { method: 'POST' as const, body, ...opts });
    return res.isOk() ? ok(res.value) : err(res.error);
  }
);

export const PUT = defineOp('PUT', async ({ span, ok, err }, url: string, body: unknown, opts?: { token?: string }) => {
  const res = await span('request', request, url, { method: 'PUT' as const, body, ...opts });
  return res.isOk() ? ok(res.value) : err(res.error);
});

export const DELETE = defineOp('DELETE', async ({ span, ok, err }, url: string, opts?: { token?: string }) => {
  const res = await span('request', request, url, { method: 'DELETE' as const, ...opts });
  return res.isOk() ? ok(res.value) : err(res.error);
});

// Batch the ops into the exported OpGroup
export const httpModule = defineOps({ request, GET, POST, PUT, DELETE });
```

**Key Points**:

- `deps: { auth: authModule }` declares the dependency
- `const { auth } = deps` destructures for ergonomic access
- `span('auth', auth.validateToken, opts.token)` invokes auth op as a child span
- Convenience methods (`GET`, `POST`, etc.) use `span('request', request, ...)` for composition
- All attributes unprefixed - prefix applied at composition time

### Lambda Router Library (`@mycompany/lambda-router`) <a id="smoo/lmao!n/lambda-routing-example-router.lambda-library"></a>

A Lambda handler that routes operations by name, composing HTTP and auth libraries.

```typescript
// @mycompany/lambda-router/src/index.ts
import { defineCodeError, defineOpContext, S, type Result } from '@smoothbricks/lmao';
import { httpModule } from '@mycompany/http-tracing';
import { authModule } from '@mycompany/auth-tracing';

// Lambda module composes both HTTP and auth
const { defineOp, defineOps } = defineOpContext({
  logSchema: {
    op: S.category(), // Operation name from request (repeated values → category)
    duration: S.number(), // Total operation duration
    success: S.boolean(), // Operation success/failure
    errorCode: S.category(), // Error code if failed
  },
  deps: {
    http: httpModule, // HTTP operations
    auth: authModule, // Direct auth operations (e.g., token refresh)
  },
});

// Lambda event structure
interface LambdaEvent {
  op: string; // Operation to invoke
  url?: string; // URL for HTTP operations
  body?: unknown; // Request body for POST/PUT
  token?: string; // Auth token
  requestId?: string; // Numeric request id, tagged as uint64_value
}

// Failure of a routed operation, as a code error (its code fills error_code on row 1)
const ROUTE_FAILED = defineCodeError('ROUTE_FAILED')<{ message: string; errorCode: string }>();

// The main router operation
export const routeOp = defineOp('routeOp', async ({ span, tag, log, deps, ok, err }, event: LambdaEvent) => {
  const startTime = performance.now();

  // Destructure deps for ergonomic access
  const { http, auth } = deps;

  // Tag the operation name immediately
  tag.op(event.op);

  // Use .uint64_value() for large integers (e.g., request IDs, byte counts)
  if (event.requestId) {
    tag.uint64_value(BigInt(event.requestId));
  }

  log.info('Routing operation');

  try {
    let result: Result<unknown, unknown>;

    // Route based on operation name
    // Each route uses span() to invoke the appropriate op
    switch (event.op) {
      // HTTP operations - delegate to http module
      case 'GET':
        if (!event.url) throw new Error('URL required for GET');
        result = await span('GET', http.GET, event.url, { token: event.token });
        break;

      case 'POST':
        if (!event.url) throw new Error('URL required for POST');
        result = await span('POST', http.POST, event.url, event.body, { token: event.token });
        break;

      case 'PUT':
        if (!event.url) throw new Error('URL required for PUT');
        result = await span('PUT', http.PUT, event.url, event.body, { token: event.token });
        break;

      case 'DELETE':
        if (!event.url) throw new Error('URL required for DELETE');
        result = await span('DELETE', http.DELETE, event.url, { token: event.token });
        break;

      // Auth operations - delegate to auth module directly
      case 'validateToken':
        if (!event.token) throw new Error('Token required for validateToken');
        result = await span('validateToken', auth.validateToken, event.token);
        break;

      case 'refreshToken':
        if (!event.token) throw new Error('Refresh token required');
        result = await span('refreshToken', auth.refreshToken, event.token);
        break;

      default:
        throw new Error(`Unknown operation: ${event.op}`);
    }

    if (result.isErr()) {
      // A routed op failed: rebuild its outcome on this context
      tag.success(false);
      tag.duration(performance.now() - startTime);
      log.error('Operation failed');
      return err(result.error);
    }

    // Tag success
    tag.success(true);
    tag.duration(performance.now() - startTime);

    log.info('Operation completed successfully');

    return ok(result.value);
  } catch (error) {
    // Tag failure
    tag.success(false);
    tag.duration(performance.now() - startTime);

    const errorCode = error instanceof Error ? error.name : 'UNKNOWN_ERROR';
    tag.errorCode(errorCode);

    log.error('Operation failed');

    return err(ROUTE_FAILED({ message: error instanceof Error ? error.message : String(error), errorCode }));
  }
});

// Batch the ops into the exported OpGroup
export const lambdaModule = defineOps({ routeOp });
```

### Direct Invoke Argument Shape <a id="smoo/lmao!n/lambda-routing-example-invoke-shape"></a>

For JSON invoke routers (`{ op: 'name', ...fields }`), use Ops with one of these signatures:

- `(ctx) => Result`
- `(ctx, argObject) => Result`

Avoid multi-positional Op args for direct JSON invoke paths. Transport adapters have no positional argument ordering, so
remaining payload fields should map to a single object arg.

**Key Points**:

- `deps: { http: httpModule, auth: authModule }` declares both dependencies
- `const { http, auth } = deps` destructures for clean access
- Switch routes by operation name, each branch uses `span()` to invoke
- `span('GET', http.GET, ...)` - operation name + op + args
- Error handling with proper attribute tagging
- Single entry point (`routeOp`) handles all operations

### Application Entry Point <a id="smoo/lmao!n/lambda-routing-example-router.entry-point"></a>

Wire everything together with prefixes in the application's own op context, then trace the Lambda handler.

```typescript
// src/lambda.ts
import { defineOpContext, defineLogSchema, ArrayQueueTracer } from '@smoothbricks/lmao';
import { lambdaModule } from '@mycompany/lambda-router';
import { httpModule } from '@mycompany/http-tracing';
import { authModule } from '@mycompany/auth-tracing';

// ============================================
// Wire dependencies with prefixes
// ============================================

// Each dep carries its own prefix:
// auth columns: tokenType → auth_tokenType
// HTTP columns: method → http_method, url → http_url, etc.
// Lambda columns: op → lambda_op, duration → lambda_duration, etc.
const appContext = defineOpContext({
  logSchema: defineLogSchema({}),
  deps: {
    lambda: lambdaModule.prefix('lambda'),
    http: httpModule.prefix('http'),
    auth: authModule.prefix('auth'),
  },
  ctx: {
    requestId: undefined as string | undefined, // OPTIONAL - set per invocation
  },
});

// Root op: routes the event through the prefixed Lambda router
const handle = appContext.defineOp('handle', async (ctx, event: LambdaEvent) => {
  const routed = await ctx.span('route', ctx.deps.lambda.routeOp, event);
  return routed.isOk() ? ctx.ok(routed.value) : ctx.err(routed.error);
});

const { trace } = new ArrayQueueTracer(appContext);

// ============================================
// Lambda Handler
// ============================================

export const handler = async (event: LambdaEvent) => {
  // trace() creates the root span for this Lambda invocation
  return trace('lambda-invoke', handle, event);
};

// ============================================
// Alternative: Request-scoped context
// ============================================

// trace() overrides set user context properties for this invocation
export const handlerWithContext = async (event: LambdaEvent, context: LambdaContext) => {
  return trace('lambda-invoke', { requestId: context.awsRequestId }, handle, event);
};
```

**Key Points**:

- `deps: { lambda: lambdaModule.prefix('lambda'), http: ..., auth: ... }` wires every dependency with its prefix
- **Shared deps**: child spans receive the same `deps` references as their parent
  ([Context Flow](./01c_context_flow_and_op_wrappers.md)), so `routeOp`'s `http` and `auth`, and the HTTP library's
  `auth`, all resolve to the application's prefixed groups
- `trace('lambda-invoke', handle, event)` creates the root span and invokes the router through `span()`
- `trace(name, overrides, op, ...args)` sets request-scoped context properties (requestId, etc.); use `setScope()` for
  column values that every row should carry

## Why This Pattern is Elegant <a id="smoo/lmao!n/lambda-routing-example-why"></a>

### 1. Deps Can Be Destructured <a id="smoo/lmao!n/lambda-routing-example-why.deps-destructure"></a>

```typescript
// Clean, JavaScript-native pattern
const { http, auth } = deps;

// No verbose ctx.deps.http.GET or deps['http'].GET
await span('GET', http.GET, url);
```

Destructuring makes library code readable and IDE-friendly with full autocomplete.

### 2. Shared Dependency Instances <a id="smoo/lmao!n/lambda-routing-example-why.shared-instances"></a>

```typescript
// One prefixed auth group, shared by the whole trace through deps
const appContext = defineOpContext({
  logSchema: defineLogSchema({}),
  deps: {
    lambda: lambdaModule.prefix('lambda'),
    http: httpModule.prefix('http'),
    auth: authModule.prefix('auth'), // Reached by HTTP and Lambda alike
  },
});
```

Both HTTP and Lambda write to the SAME `auth_*` columns. No duplicate `http_auth_tokenType` or `lambda_auth_tokenType`.

### 3. `span()` is the Universal Invocation <a id="smoo/lmao!n/lambda-routing-example-why.universal-span"></a>

```typescript
// Every cross-cutting call uses span()
await span('auth', auth.validateToken, token);
await span('GET', http.GET, url);
await span('route', routeOp, event);

// Pattern: span(name, op, ...args)
// - Creates child span with given name
// - Invokes op with proper context
// - Returns op's result
// - Automatic timing, error tracking, parent-child linking
```

No need for different invocation patterns - `span()` handles everything.

### 4. Clean Library Code <a id="smoo/lmao!n/lambda-routing-example-why.clean-library"></a>

Libraries use unprefixed schemas:

```typescript
// In auth library - no prefix concerns
tag.tokenType('bearer');
tag.userId('user-123');

// In HTTP library - no prefix concerns
tag.method('GET');
tag.status(200);
```

Prefix is applied at composition time, not authoring time.

### 5. Op Routing Pattern <a id="smoo/lmao!n/lambda-routing-example-why.op-routing"></a>

The switch-based routing is explicit and traceable:

```typescript
switch (event.op) {
  case 'GET':
    result = await span('GET', http.GET, event.url, opts);
    break;
  case 'POST':
    result = await span('POST', http.POST, event.url, body, opts);
    break;
  // ...
}
```

Each route creates a properly named span with full trace context.

## Resulting Trace Structure <a id="smoo/lmao!n/lambda-routing-example-trace-structure"></a>

For a `POST` request with authentication:

```
lambda/route (routeOp)
├── lambda_op = "POST"
├── lambda_duration = 150.5
├── lambda_success = true
│
└── http/POST (POST op)
    ├── http_method = "POST"
    ├── http_url = "https://api.example.com/users"
    ├── http_status = 201
    ├── http_duration = 145.2
    │
    └── http/request (request op)
        │
        └── http/auth (validateToken op)
            ├── auth_tokenType = "bearer"
            ├── auth_userId = "user-123"
            └── auth_valid = true
```

## Arrow Table Output <a id="smoo/lmao!n/lambda-routing-example-arrow-output"></a>

The final Arrow table has clean, prefixed columns from all libraries:

| Column             | Type                 | Source        | Example Value                   |
| ------------------ | -------------------- | ------------- | ------------------------------- |
| `timestamp`        | `timestamp[ns]`      | System        | 2024-01-15T10:30:00.123456789Z  |
| `trace_id`         | `dictionary<string>` | System        | "abc-123-def-456"               |
| `span_id`          | `uint32`             | System        | 1                               |
| `parent_span_id`   | `uint32`             | System        | 0 (root) or parent's span_id    |
| `entry_type`       | `dictionary<string>` | System        | "span-start", "info", "span-ok" |
| `package_name`     | `dictionary<string>` | System        | "@mycompany/lambda-router"      |
| `message`          | `string`             | System        | "Routing operation"             |
| `lambda_op`        | `dictionary<string>` | Lambda module | "POST"                          |
| `lambda_duration`  | `float64`            | Lambda module | 150.5                           |
| `lambda_success`   | `uint8` (boolean)    | Lambda module | 1                               |
| `lambda_errorCode` | `dictionary<string>` | Lambda module | null                            |
| `http_method`      | `dictionary<string>` | HTTP module   | "POST"                          |
| `http_url`         | `string`             | HTTP module   | "https://api.example.com/users" |
| `http_status`      | `uint16`             | HTTP module   | 201                             |
| `http_duration`    | `float64`            | HTTP module   | 145.2                           |
| `auth_tokenType`   | `dictionary<string>` | Auth module   | "bearer"                        |
| `auth_userId`      | `dictionary<string>` | Auth module   | "user-123"                      |
| `auth_valid`       | `uint8` (boolean)    | Auth module   | 1                               |
| `uint64_value`     | `uint64`             | System (lazy) | null (or user-provided BigInt)  |

**Storage Optimizations**:

- `S.enum()` columns use dictionary encoding with compile-time values
- `S.category()` columns use dictionary encoding with runtime string interning
- `S.text()` columns are plain strings (no dictionary overhead for unique values)
- Boolean columns use `uint8` (0 or 1)
- `uint64_value` is a lazy system column (only allocated when used)

## ClickHouse Query Examples <a id="smoo/lmao!n/lambda-routing-example-clickhouse"></a>

```sql
-- Analyze Lambda operations by type
SELECT
  lambda_op,
  count(*) as invocations,
  avg(lambda_duration) as avg_duration_ms,
  sum(if(lambda_success = 1, 1, 0)) / count(*) * 100 as success_rate
FROM traces
WHERE entry_type = 'span-ok' OR entry_type = 'span-err'
  AND package_name = '@mycompany/lambda-router'
GROUP BY lambda_op
ORDER BY invocations DESC;

-- Find slow HTTP requests with auth
SELECT
  trace_id,
  http_url,
  http_status,
  http_duration,
  auth_userId
FROM traces
WHERE http_duration > 1000
  AND auth_userId IS NOT NULL
ORDER BY http_duration DESC
LIMIT 100;

-- Auth failure analysis
SELECT
  auth_tokenType,
  count(*) as attempts,
  sum(if(auth_valid = 0, 1, 0)) as failures
FROM traces
WHERE auth_tokenType IS NOT NULL
GROUP BY auth_tokenType;

-- Cross-library latency breakdown
SELECT
  trace_id,
  max(if(package_name = '@mycompany/lambda-router', lambda_duration, 0)) as total_ms,
  max(if(package_name = '@mycompany/http-tracing', http_duration, 0)) as http_ms,
  max(if(package_name = '@mycompany/auth-tracing' AND auth_valid IS NOT NULL,
         -- Auth duration would need to be tracked separately
         0, 0)) as auth_overhead_ms
FROM traces
GROUP BY trace_id
HAVING total_ms > 100;

-- Op metrics: Performance summary per operation
-- (Metrics are flushed periodically alongside traces)
SELECT
  package_name,
  message as op_name,
  anyIf(uint64_value, entry_type = 'op-invocations') as invocations,
  anyIf(uint64_value, entry_type = 'op-errors') +
    anyIf(uint64_value, entry_type = 'op-exceptions') as failures,
  anyIf(uint64_value, entry_type = 'op-duration-total') /
    anyIf(uint64_value, entry_type = 'op-invocations') / 1e6 as avg_duration_ms,
  anyIf(uint64_value, entry_type = 'op-duration-min') / 1e6 as min_duration_ms,
  anyIf(uint64_value, entry_type = 'op-duration-max') / 1e6 as max_duration_ms
FROM traces
WHERE entry_type LIKE 'op-%'
GROUP BY timestamp, package_name, message
ORDER BY invocations DESC;
```

## Client Invocation Examples <a id="smoo/lmao!n/lambda-routing-example-client-invocation"></a>

```typescript
// Simple GET
await invoke({ op: 'GET', url: 'https://api.example.com/users' });

// Authenticated POST
await invoke({
  op: 'POST',
  url: 'https://api.example.com/users',
  body: { name: 'John', email: 'john@example.com' },
  token: 'eyJhbGciOiJIUzI1NiIs...',
});

// Direct token validation
await invoke({ op: 'validateToken', token: 'eyJhbGciOiJIUzI1NiIs...' });

// Token refresh
await invoke({ op: 'refreshToken', token: 'refresh-token-xxx' });
```

## Summary <a id="smoo/lmao!n/lambda-routing-example-summary"></a>

The `defineOp()` + `span()` pattern enables:

1. **Clean library authoring** - Focus on domain logic, not tracing plumbing
2. **Type-safe composition** - Dependencies declared and injected with full TypeScript support
3. **Shared instances** - No column duplication across consumers
4. **Universal invocation** - `span(name, op, ...args)` for everything
5. **Op-based routing** - Clean switch statements with proper span creation
6. **Queryable traces** - Prefixed columns in Arrow tables for analytics

This pattern scales from simple single-library usage to complex multi-library compositions like Lambda routers, GraphQL
resolvers, or microservice gateways.

## Integration with Other Specs <a id="smoo/lmao!n/lambda-routing-example-integration"></a>

- **[Op Context Pattern](./01l_op_context_pattern.md)**: Defines `defineOpContext()`, `defineOp()`, and Tracer API
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: RemapDescriptor for Arrow conversion
- **[Context Flow](./01c_context_flow_and_op_wrappers.md)**: How context propagates through span() calls
- **[Trace Schema System](./01a_trace_schema_system.md)**: `S.enum()`, `S.category()`, `S.text()` definitions
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Final queryable format
