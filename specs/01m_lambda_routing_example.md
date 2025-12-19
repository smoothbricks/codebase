# Lambda Routing Example: Library Composition with `op()` + `span()`

## Overview

This spec demonstrates how the `defineModule()` API enables elegant library composition through a practical Lambda
routing example. It showcases:

1. **Clean library authoring** with unprefixed schemas
2. **Dependency injection** via `deps` declarations
3. **Shared dependency instances** across multiple consumers
4. **The `span()` pattern** as the universal invocation mechanism
5. **Op-based routing** for Lambda handlers

## The Pattern: `op()` + `span()`

The key insight is that **every cross-cutting operation should be wrapped in a span**. This gives you:

- Automatic timing and error tracking
- Proper parent-child trace relationships
- Attribute isolation per operation
- Clean composition via dependency injection

```typescript
// The universal pattern:
const result = await span('operation-name', someOp, ...args);
```

## Complete Example: Lambda Op Router

### Auth Library (`@mycompany/auth-tracing`)

A simple authentication library that validates tokens and extracts user information.

```typescript
// @mycompany/auth-tracing/src/index.ts
import { defineModule, S } from '@smoothbricks/lmao';

// Define module with unprefixed schema
export const authModule = defineModule({
  metadata: {
    packageName: '@mycompany/auth-tracing',
    packagePath: 'src/index.ts',
  },
  schema: {
    tokenType: S.enum(['bearer', 'api-key', 'session']),
    userId: S.category(),
    valid: S.boolean(),
  },
});

// Destructure op factory from module
const { op } = authModule;

// Token decoding helper (implementation details)
function decodeToken(token: string): DecodedToken {
  // JWT decode, API key lookup, session validation, etc.
  return { type: 'bearer', userId: 'user-123', permissions: ['read', 'write'] };
}

// Define operation - receives { tag, log, span, deps } context
export const validateToken = op(async ({ tag, log }, token: string) => {
  log.info('Validating token');

  const decoded = decodeToken(token);

  // Tag attributes using unprefixed names
  // Final column names depend on prefix applied at composition time
  tag.tokenType(decoded.type);
  tag.userId(decoded.userId);
  tag.valid(true);

  log.info('Token validated successfully');

  return { userId: decoded.userId, permissions: decoded.permissions };
});

// Another auth operation
export const refreshToken = op(async ({ tag, log }, refreshToken: string) => {
  log.info('Refreshing token');
  tag.tokenType('bearer');

  // ... refresh logic ...

  tag.valid(true);
  return { accessToken: 'new-token', expiresIn: 3600 };
});
```

**Key Points**:

- Schema uses unprefixed names (`userId`, not `auth_userId`)
- `op()` creates traced operations with automatic span management
- Context provides `{ tag, log, span, deps }` - destructure what you need
- No prefix concerns in library code - clean, domain-focused

### HTTP Library (`@mycompany/http-tracing`)

An HTTP client library that can optionally authenticate requests using the auth library.

```typescript
// @mycompany/http-tracing/src/index.ts
import { defineModule, S } from '@smoothbricks/lmao';
import { authModule } from '@mycompany/auth-tracing';

// HTTP module declares auth as a dependency
export const httpModule = defineModule({
  metadata: {
    packageName: '@mycompany/http-tracing',
    packagePath: 'src/index.ts',
  },
  schema: {
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
    url: S.text().masked('url'), // URL masking for PII
    status: S.number(),
    duration: S.number(),
  },
  deps: {
    auth: authModule, // Declare dependency - will be injected at composition time
  },
});

const { op } = httpModule;

interface RequestOpts {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  body?: unknown;
  token?: string;
  headers?: Record<string, string>;
}

// Core request operation - uses auth dependency via span()
export const request = op(async ({ span, tag, log, deps }, url: string, opts: RequestOpts) => {
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
    // authResult contains { userId, permissions }
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

  return response;
});

// Convenience methods that delegate to request via span()
export const GET = op(async ({ span }, url: string, opts?: { token?: string }) => {
  return span('request', request, url, { method: 'GET' as const, ...opts });
});

export const POST = op(async ({ span }, url: string, body: unknown, opts?: { token?: string }) => {
  return span('request', request, url, { method: 'POST' as const, body, ...opts });
});

export const PUT = op(async ({ span }, url: string, body: unknown, opts?: { token?: string }) => {
  return span('request', request, url, { method: 'PUT' as const, body, ...opts });
});

export const DELETE = op(async ({ span }, url: string, opts?: { token?: string }) => {
  return span('request', request, url, { method: 'DELETE' as const, ...opts });
});
```

**Key Points**:

- `deps: { auth: authModule }` declares the dependency
- `const { auth } = deps` destructures for ergonomic access
- `span('auth', auth.validateToken, opts.token)` invokes auth op as a child span
- Convenience methods (`GET`, `POST`, etc.) use `span('request', request, ...)` for composition
- All attributes unprefixed - prefix applied at composition time

### Lambda Router Library (`@mycompany/lambda-router`)

A Lambda handler that routes operations by name, composing HTTP and auth libraries.

```typescript
// @mycompany/lambda-router/src/index.ts
import { defineModule, S } from '@smoothbricks/lmao';
import { httpModule, GET, POST, PUT, DELETE } from '@mycompany/http-tracing';
import { authModule, validateToken, refreshToken } from '@mycompany/auth-tracing';

// Lambda module composes both HTTP and auth
export const lambdaModule = defineModule({
  metadata: {
    packageName: '@mycompany/lambda-router',
    packagePath: 'src/index.ts',
  },
  schema: {
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

const { op } = lambdaModule;

// Lambda event structure
interface LambdaEvent {
  op: string; // Operation to invoke
  url?: string; // URL for HTTP operations
  body?: unknown; // Request body for POST/PUT
  token?: string; // Auth token
}

interface LambdaResult {
  success: boolean;
  data?: unknown;
  error?: string;
  errorCode?: string;
}

// The main router operation
export const routeOp = op(async ({ span, tag, log, deps }, event: LambdaEvent): Promise<LambdaResult> => {
  const startTime = performance.now();

  // Destructure deps for ergonomic access
  const { http, auth } = deps;

  // Tag the operation name immediately
  tag.op(event.op);

  // Use .uint64() for large integers (e.g., request IDs, byte counts)
  if (event.requestId) {
    tag.uint64(BigInt(event.requestId));
  }

  log.info('Routing operation');

  try {
    let result: unknown;

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

    // Tag success
    tag.success(true);
    tag.duration(performance.now() - startTime);

    log.info('Operation completed successfully');

    return { success: true, data: result };
  } catch (error) {
    // Tag failure
    tag.success(false);
    tag.duration(performance.now() - startTime);

    const errorCode = error instanceof Error ? error.name : 'UNKNOWN_ERROR';
    tag.errorCode(errorCode);

    log.error('Operation failed');

    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
      errorCode,
    };
  }
});
```

**Key Points**:

- `deps: { http: httpModule, auth: authModule }` declares both dependencies
- `const { http, auth } = deps` destructures for clean access
- Switch routes by operation name, each branch uses `span()` to invoke
- `span('GET', http.GET, ...)` - operation name + op + args
- Error handling with proper attribute tagging
- Single entry point (`routeOp`) handles all operations

### Application Entry Point

Wire everything together with prefixes and create the Lambda handler.

```typescript
// src/lambda.ts
import { lambdaModule, routeOp } from '@mycompany/lambda-router';
import { httpModule } from '@mycompany/http-tracing';
import { authModule } from '@mycompany/auth-tracing';

// ============================================
// Wire dependencies with prefixes
// ============================================

// Create auth instance with prefix
// All auth columns will be prefixed: tokenType → auth_tokenType
const authRoot = authModule.prefix('auth').use();

// Create HTTP instance with prefix and inject auth dependency
// HTTP columns: method → http_method, url → http_url, etc.
// HTTP's auth dependency uses the SAME auth instance
const httpRoot = httpModule.prefix('http').use({
  auth: authRoot, // HTTP uses our auth instance
});

// Create Lambda router instance with prefix
// Lambda columns: op → lambda_op, duration → lambda_duration, etc.
// Lambda has access to both HTTP and auth
const lambdaRoot = lambdaModule.prefix('lambda').use({
  http: httpRoot, // Lambda uses our HTTP instance
  auth: authRoot, // Lambda also uses SAME auth instance for direct auth ops
});

// ============================================
// Lambda Handler
// ============================================

export const handler = async (event: LambdaEvent) => {
  // Single entry point - routeOp handles all operations
  // span() creates the root span for this Lambda invocation
  return lambdaRoot.span('route', routeOp, event);
};

// ============================================
// Alternative: Request-scoped context
// ============================================

// For request-scoped attributes (requestId, userId from JWT, etc.)
export const handlerWithContext = async (event: LambdaEvent, context: LambdaContext) => {
  // Create request context with scope attributes
  const requestCtx = lambdaRoot.forContext({
    requestId: context.awsRequestId,
    // userId could come from JWT in event.token
  });

  return requestCtx.span('route', routeOp, event);
};
```

**Key Points**:

- `authModule.prefix('auth').use()` creates instance with `auth_` prefix
- `httpModule.prefix('http').use({ auth: authRoot })` injects auth dependency
- **Shared instance**: `authRoot` is used by BOTH `httpRoot` and `lambdaRoot`
- `lambdaRoot.span('route', routeOp, event)` creates root span and invokes
- `forContext()` adds request-scoped attributes (requestId, etc.)

## Why This Pattern is Elegant

### 1. Deps Can Be Destructured

```typescript
// Clean, JavaScript-native pattern
const { http, auth } = deps;

// No verbose ctx.deps.http.GET or deps['http'].GET
await span('GET', http.GET, url);
```

Destructuring makes library code readable and IDE-friendly with full autocomplete.

### 2. Shared Dependency Instances

```typescript
// Single auth instance shared across the entire trace
const authRoot = authModule.prefix('auth').use();

const httpRoot = httpModule.prefix('http').use({
  auth: authRoot, // HTTP uses auth
});

const lambdaRoot = lambdaModule.prefix('lambda').use({
  http: httpRoot,
  auth: authRoot, // Lambda also uses SAME auth
});
```

Both HTTP and Lambda write to the SAME `auth_*` columns. No duplicate `http_auth_tokenType` or `lambda_auth_tokenType`.

### 3. `span()` is the Universal Invocation

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

### 4. Clean Library Code

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

### 5. Op Routing Pattern

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

## Resulting Trace Structure

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

## Arrow Table Output

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

## ClickHouse Query Examples

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

## Client Invocation Examples

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

## Summary

The `op()` + `span()` pattern enables:

1. **Clean library authoring** - Focus on domain logic, not tracing plumbing
2. **Type-safe composition** - Dependencies declared and injected with full TypeScript support
3. **Shared instances** - No column duplication across consumers
4. **Universal invocation** - `span(name, op, ...args)` for everything
5. **Op-based routing** - Clean switch statements with proper span creation
6. **Queryable traces** - Prefixed columns in Arrow tables for analytics

This pattern scales from simple single-library usage to complex multi-library compositions like Lambda routers, GraphQL
resolvers, or microservice gateways.

## Integration with Other Specs

- **[Module Builder Pattern](./01l_module_builder_pattern.md)**: Defines `defineModule()`, `.prefix()`, `.use()` API
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: RemappedBufferView for Arrow conversion
- **[Context Flow](./01c_context_flow_and_op_wrappers.md)**: How context propagates through span() calls
- **[Trace Schema System](./01a_trace_schema_system.md)**: `S.enum()`, `S.category()`, `S.text()` definitions
- **[Arrow Table Structure](./01f_arrow_table_structure.md)**: Final queryable format
