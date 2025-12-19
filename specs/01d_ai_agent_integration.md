# AI Agent Integration

## Overview

The AI agent integration provides structured access to trace data for automated analysis and debugging. It includes:

1. **Model Context Protocol (MCP) server** for standardized trace querying
2. **Test framework integration** with automatic test run correlation
3. **AI agent workflow** for trace-driven development and debugging
4. **Production deployment** with authentication and privacy controls

## Design Philosophy

**Key Insight**: AI agents need structured access to trace data without consuming limited context windows. The MCP
protocol provides tool-based access where detailed trace data is only loaded when specifically requested.

**Benefits**:

- **Context efficiency**: Detailed trace data doesn't consume AI context until requested
- **Standardized protocol**: Works with Claude Desktop, Cursor, VS Code Copilot, and other MCP clients
- **Background processing**: Server watches trace files, AI queries on-demand
- **Tool-based interface**: AI agents can call trace query functions as tools

## MCP Server Architecture

**Purpose**: Provide AI agents with structured access to trace data through standardized tools.

```typescript
// MCP server exposes trace querying tools
const traceServer = new MCPServer({
  name: 'trace-analyzer',
  version: '1.0.0',
});

// Tool: Query traces by test run ID
// Note: testRunId IS the trace_id for test runs - it's a custom trace_id value used for test correlation
traceServer.addTool({
  name: 'get_traces_by_test_run',
  description: 'Get all traces for a specific test run',
  inputSchema: {
    type: 'object',
    properties: {
      testRunId: { type: 'string', description: 'The trace_id used for this test run (testRunId === trace_id)' },
    },
  },
  handler: async ({ testRunId }) => {
    // testRunId is used as the trace_id value for all spans in this test run
    return await queryTraceDatabase({ traceId: testRunId });
  },
});

// Tool: Query traces by span type
traceServer.addTool({
  name: 'get_traces_by_span',
  description: 'Get traces filtered by span name/type',
  inputSchema: {
    type: 'object',
    properties: {
      spanName: { type: 'string' },
      testRunId: { type: 'string', optional: true },
      timeRange: { type: 'object', optional: true },
    },
  },
  handler: async ({ spanName, testRunId, timeRange }) => {
    return await queryTraceDatabase({ spanName, testRunId, timeRange });
  },
});

// Tool: Query by span identity (thread_id + span_id composite)
traceServer.addTool({
  name: 'get_span_by_identity',
  description: 'Get a specific span by its identity (thread_id + span_id)',
  inputSchema: {
    type: 'object',
    properties: {
      threadId: { type: 'string', description: 'Thread ID (uint64, hex format)' },
      spanId: { type: 'number', description: 'Span ID (uint32)' },
      traceId: { type: 'string', optional: true, description: 'Optional: filter by trace_id for validation' },
    },
  },
  handler: async ({ threadId, spanId, traceId }) => {
    // Query by composite identity
    return await queryTraceDatabase({
      threadId,
      spanId,
      traceId, // Optional validation
    });
  },
});

// Tool: Get parent span
traceServer.addTool({
  name: 'get_parent_span',
  description: 'Get the parent span of a given span',
  inputSchema: {
    type: 'object',
    properties: {
      threadId: { type: 'string', description: 'Thread ID of the child span' },
      spanId: { type: 'number', description: 'Span ID of the child span' },
      traceId: { type: 'string', description: 'Trace ID (for validation)' },
    },
  },
  handler: async ({ threadId, spanId, traceId }) => {
    // First get the child span to find parent references
    const childSpan = await queryTraceDatabase({ threadId, spanId, traceId });
    if (!childSpan || !childSpan.parent_thread_id || !childSpan.parent_span_id) {
      return null; // Root span has no parent
    }

    // Query parent using parent_thread_id + parent_span_id
    return await queryTraceDatabase({
      threadId: childSpan.parent_thread_id,
      spanId: childSpan.parent_span_id,
      traceId, // Same trace
    });
  },
});

// Tool: Get performance metrics
traceServer.addTool({
  name: 'get_performance_metrics',
  description: 'Get performance summary for a test run',
  inputSchema: {
    type: 'object',
    properties: {
      testRunId: { type: 'string' },
    },
  },
  handler: async ({ testRunId }) => {
    return await generatePerformanceReport(testRunId);
  },
});

// Tool: Analyze feature flag usage
traceServer.addTool({
  name: 'analyze_feature_flag_usage',
  description: 'Analyze feature flag access patterns in traces',
  inputSchema: {
    type: 'object',
    properties: {
      testRunId: { type: 'string' },
      flagName: { type: 'string', optional: true },
    },
  },
  handler: async ({ testRunId, flagName }) => {
    return await analyzeFeatureFlagUsage({ testRunId, flagName });
  },
});
```

**Why This Design**:

- **Tool-based access**: AI agents call specific functions rather than loading all data
- **Flexible querying**: Multiple query patterns for different analysis needs
- **Performance focus**: Dedicated tools for performance analysis
- **Feature flag analytics**: Specialized tools for A/B testing insights

## Test Framework Integration

**Purpose**: Automatically correlate traces with test runs for AI-driven analysis.

**Why Jest/Bun Examples**:

Jest and Bun are shown as **examples** of how test framework integration works. The design is **framework-agnostic** -
any test framework can integrate similarly by:

1. Generating a single `testRunId` for the entire test suite
2. Setting that `testRunId` as the `trace_id` for all spans during the test run
3. Optionally tagging individual test cases with metadata

The integration pattern works with Vitest, Mocha, Jest, Bun, and any other test framework that supports plugins or
hooks.

### Jest Plugin

```typescript
// jest-trace-plugin
export default {
  setupFilesAfterEnv: ['<rootDir>/jest-trace-setup.js'],
  reporters: [
    'default',
    [
      'jest-trace-reporter',
      {
        outputDir: './traces',
        includeTestMetadata: true,
      },
    ],
  ],
};

// Single test run ID for entire suite
beforeAll(() => {
  const testRunId = generateTestRunId();
  console.log(`🔍 Test Run ID: ${testRunId}`); // AI agent can see this
  globalThis.__TEST_RUN_ID__ = testRunId;
});

// Tag individual test cases within the run
beforeEach(() => {
  const testCase = expect.getState().currentTestName;
  const testFile = expect.getState().testPath;
  trace.setContext({
    testRunId: globalThis.__TEST_RUN_ID__,
    testCase,
    testFile,
  });
});
```

### Bun Test Plugin

```typescript
// bun-trace-plugin
import { plugin } from 'bun';

plugin({
  name: 'trace-logger',
  setup(build) {
    // Single test run ID for entire suite
    build.onStart(() => {
      const testRunId = generateTestRunId();
      console.log(`🔍 Test Run ID: ${testRunId}`);
      globalThis.__TEST_RUN_ID__ = testRunId;
    });

    // Tag individual test cases
    build.onTestStart((testName, filePath) => {
      trace.setContext({
        testRunId: globalThis.__TEST_RUN_ID__,
        testCase: testName,
        testFile: filePath,
      });
    });
  },
});
```

**Why This Integration**:

- **Single test run ID**: All tests in a suite share the same correlation ID
- **Console visibility**: AI agents can see test run ID in console output
- **Automatic tagging**: Test metadata automatically added to traces
- **Framework agnostic**: Works with Jest, Bun, Vitest, etc.

## AI Agent Workflow

**Purpose**: Enable AI agents to analyze actual execution behavior rather than just static code.

### Example AI Agent Interaction

```
AI: Running tests...
Console: 🔍 Test Run ID: test-run-abc123
Console: ✅ 1000 tests passed (15 failed)

AI: Let me check the trace data for that run...
MCP Tool Call: get_traces_by_test_run({ testRunId: "test-run-abc123" })
Result: [
  {
    testCase: 'user login flow',
    message: 'user-validation',
    entry_type: 'span-ok',
    thread_id: '0x1a2b3c4d5e6f7890',
    span_id: 1,
    parent_thread_id: null,
    parent_span_id: null,
    timestamp: '2024-01-01T10:00:00.002300Z',
    // Duration calculated from timestamps (not a column)
  },
  {
    testCase: 'user login flow',
    message: 'database-query',
    entry_type: 'span-ok',
    thread_id: '0x1a2b3c4d5e6f7890',
    span_id: 2,
    parent_thread_id: '0x1a2b3c4d5e6f7890',
    parent_span_id: 1,
    timestamp: '2024-01-01T10:00:00.045000Z',
  },
  {
    testCase: 'email notification',
    message: 'email-send',
    entry_type: 'span-err',
    thread_id: '0x1a2b3c4d5e6f7890',
    span_id: 3,
    parent_thread_id: '0x1a2b3c4d5e6f7890',
    parent_span_id: 1,
    timestamp: '2024-01-01T10:00:00.120000Z',
    // Error details in attribute columns
  },
  // ... more traces
]

Note: The actual Arrow table uses separate columns: `message` (span name), `entry_type`, `thread_id`, `span_id`,
`parent_thread_id`, `parent_span_id`. Duration is calculated from `timestamp` differences, not stored as a column.
Span identification uses the composite `(thread_id, span_id)` - see [Arrow Table Structure](./01f_arrow_table_structure.md).

AI: I see multiple email-send spans failing with SMTP timeouts across different test cases.
    Let me check the pattern...
MCP Tool Call: get_traces_by_span({ spanName: "email-send", testRunId: "test-run-abc123" })
AI: All 15 failures are SMTP timeouts. Let me fix the retry logic and timeout configuration...
```

### Workflow Steps

1. **Test Execution**: AI runs tests, sees test run ID in console output
2. **Trace Collection**: Test framework plugin writes traces with test metadata
3. **MCP Query**: AI queries specific test run traces via MCP server
4. **Analysis**: AI analyzes actual execution patterns, performance, errors
5. **Code Improvement**: AI makes informed changes based on trace data

## Production Deployment

**Purpose**: Deploy authenticated MCP server for production trace access with privacy controls.

### Production MCP Server

**Why OAuth2**:

- **Standard protocol**: Industry-standard API authentication protocol with broad tooling support
- **Scope-based access control**: Fine-grained permissions via scopes (`trace:read`, `trace:decrypt`)
- **Audit trail**: OAuth2 tokens provide built-in audit logging for security compliance
- **Token management**: Standard token refresh and revocation mechanisms
- **Integration**: Works seamlessly with existing identity providers (Okta, Auth0, etc.)

```typescript
// Production MCP server with OAuth
const productionTraceServer = new MCPServer({
  name: 'production-trace-analyzer',
  version: '1.0.0',
  authentication: {
    provider: 'oauth2',
    scopes: ['trace:read', 'trace:decrypt'],
  },
});

// Tool requires authentication
traceServer.addTool({
  name: 'get_production_traces',
  description: 'Query production traces (requires authentication)',
  requiresAuth: true,
  requiredScopes: ['trace:read'],
  handler: async ({ testRunId }, { user, scopes }) => {
    // Only return decrypted data if user has decrypt scope
    const includeDecrypted = scopes.includes('trace:decrypt');
    return await queryProductionTraces({ testRunId, includeDecrypted });
  },
});

// Tool for performance analysis
traceServer.addTool({
  name: 'analyze_production_performance',
  description: 'Analyze production performance patterns',
  requiresAuth: true,
  requiredScopes: ['trace:read'],
  handler: async ({ timeRange, service }, { user }) => {
    return await analyzeProductionPerformance({
      timeRange,
      service,
      userId: user.id, // For audit logging
    });
  },
});
```

### Dual Storage Architecture

**Purpose**: Store both masked data (for analytics) and encrypted unmasked data (for debugging).

```typescript
interface ProductionTraceRecord {
  // Masked data - queryable for analytics
  maskedData: TraceRecord;

  // Encrypted unmasked data - for authorized debugging
  encryptedData: ArrayBuffer; // Encrypted serialized blob

  // Metadata for decryption access control
  encryptionKeyId: string;
  accessLevel: 'public' | 'internal' | 'sensitive';
}

// MCP tool with conditional decryption
traceServer.addTool({
  name: 'get_production_trace_details',
  description: 'Get detailed production trace with optional decryption',
  requiresAuth: true,
  handler: async ({ traceId, includeDecrypted }, { user, scopes }) => {
    const trace = await getProductionTrace(traceId);

    if (includeDecrypted && scopes.includes('trace:decrypt')) {
      // Decrypt sensitive data for authorized users
      const decryptedData = await decryptTraceData(trace.encryptedData, trace.encryptionKeyId, user.id);

      return {
        ...trace.maskedData,
        decryptedData,
        accessLevel: trace.accessLevel,
      };
    }

    // Return only masked data
    return trace.maskedData;
  },
});
```

**Why This Architecture**:

- **Analytics-ready**: Masked data can be queried without privacy concerns
- **Debug capability**: Authorized users can decrypt full trace data when needed
- **Compliance**: Meets privacy requirements while maintaining debugging capability
- **Audit trail**: All decryption access logged for security

## Distributed Tracing Integration

**Purpose**: Query traces across multiple services and deployments.

### Cross-Service Query Architecture

```typescript
// Query distributed traces across S3 files
const distributedQuery = `
  SELECT 
    traceId,
    service,
    span,
    duration,
    timestamp
  FROM s3('s3://traces/*/trace-*.parquet')
  WHERE traceId = '${traceId}'
  ORDER BY timestamp
`;

// MCP tool for distributed trace analysis
traceServer.addTool({
  name: 'get_distributed_trace',
  description: 'Get complete trace across all services',
  inputSchema: {
    type: 'object',
    properties: {
      traceId: { type: 'string' },
      services: { type: 'array', items: { type: 'string' }, optional: true },
    },
  },
  handler: async ({ traceId, services }) => {
    // Use ClickHouse chDB or AWS Athena for cross-service queries
    return await queryDistributedTrace({ traceId, services });
  },
});

// Deploy chDB on AWS Lambda for serverless trace queries
const lambdaTraceQuery = async (traceId: string) => {
  return await chdb.query(distributedQuery, { traceId });
};
```

**Benefits**:

- **Complete trace visibility**: See operations across all services
- **Serverless querying**: chDB on Lambda provides cost-effective trace analysis
- **Standard tooling**: Leverage existing analytics infrastructure

## MCP Configuration

**Purpose**: Configure AI agents to use the trace analysis MCP server.

### Claude Desktop Configuration

```json
// claude_desktop_config.json
{
  "mcpServers": {
    "trace-analyzer": {
      "command": "node",
      "args": ["./dist/trace-mcp-server.js"],
      "env": {
        "TRACE_DATA_PATH": "/path/to/trace/files",
        "DATABASE_URL": "postgres://localhost/traces"
      }
    },
    "production-trace-analyzer": {
      "command": "node",
      "args": ["./dist/production-trace-mcp-server.js"],
      "env": {
        "TRACE_DATA_PATH": "/path/to/production/traces",
        "AUTH_PROVIDER": "oauth2",
        "ENCRYPTION_KEY_SERVICE": "aws-kms"
      }
    }
  }
}
```

### Cursor/VS Code Configuration

```json
// .vscode/settings.json
{
  "mcp.servers": {
    "trace-analyzer": {
      "command": ["node", "./dist/trace-mcp-server.js"],
      "env": {
        "TRACE_DATA_PATH": "./traces"
      }
    }
  }
}
```

## AI Agent Documentation Requirements

**Purpose**: Provide AI agents with concise prompts that enforce trace logging patterns.

### Agent Prompt Structure

```typescript
// AI agent-specific prompts (< 500 tokens each)
const AI_AGENT_PROMPTS = {
  traceLogging: `
    CRITICAL: Always use trace logging instead of console.log.
    
    ❌ NEVER generate: console.log('Processing user request');
    ✅ ALWAYS generate: await span('process-user-request', processRequestOp, ...args);
    
    Required patterns:
    - Use op() wrapper to define operations
    - Use span('name', op, ...args) to execute with tracing
    - Use tag.* for structured data
    - Use ok() and err() for results
    - Use setScope() for attributes that propagate to all rows and child spans
    
    Operation definition pattern:
    const { op } = myModule;
    const processUser = op(async ({ span, log, tag, setScope }, userId: string) => {
      // Scope: Set once, appears on all rows and child spans
      setScope({ requestId: 'req-123', userId });
      
      // Tag: Span attributes (row 0 only)
      tag.userId(userId);           // Automatically hashed
      tag.operation('SELECT');      // Enum validation
      
      // Nested operation - name provided at call site
      await span('fetch-data', fetchOp, userId);
      
      // Log: Creates new rows (appends)
      log.info('Processing complete').with({ itemCount: 42 });
      
      return ok({ processed: true });
    });
    
    Note: The TypeScript transformer injects line numbers as the first argument to span():
    - User writes: await span('fetch-data', fetchOp, userId);
    - Transformer outputs: await span(42, 'fetch-data', fetchOp, userId);
    This enables source code linking without runtime stack trace parsing.
    
    Scope attributes (setScope):
    - Set once at span level, propagates to ALL rows and child spans
    - Use for request context (requestId, userId, tenantId) that applies to entire trace
    - Child spans inherit parent scope by reference (zero-cost, immutable)
    - Direct writes (tag, ok/err, log fluent) override scope for specific rows
    - Example: setScope({ requestId: 'req-123', userId: 'user-456' })
    See [Span Scope Attributes](./01i_span_scope_attributes.md) for details.
    
    Schema examples:
    tag.userId(user.id);            // Automatically hashed
    tag.duration(45);               // Performance tracking
    tag.operation('SELECT');        // Enum validation
  `,

  featureFlags: `
    Feature flag access patterns:
    
    const myOp = op(async ({ ff, tag }) => {
      // Sync flags (direct property access):
      if (ff.advancedValidation) { ... }
      
      // Async flags (method call):
      const limit = await ff.get('userSpecificLimit');
      
      // Usage tracking (for A/B testing):
      ff.track('experimentalFeature');
    });
  `,

  environmentVariables: `
    Environment variable access (zero overhead):
    
    const myOp = op(async ({ env, tag }) => {
      const region = env.awsRegion;      // Just property access
      const dbUrl = env.databaseUrl;     // Real value for app use
      
      // Security: Environment values only appear in traces if explicitly logged:
      tag.region(region);                // Safe to log
      // tag.databaseUrl(dbUrl);         // Would be masked
    });
  `,

  metrics: `
    Metrics are structured logs with specific entry types:
    
    - Op metrics: op-invocations, op-errors, op-exceptions, op-duration-total, 
      op-duration-ok, op-duration-err, op-duration-min, op-duration-max
    - Buffer metrics: buffer-writes, buffer-overflow-writes, buffer-created, buffer-overflows
    - Period marker: period-start (marks metrics period boundaries)
    
    Metrics are automatically emitted during flush cycles. The entry_type column identifies
    the metric type, and uint64_value contains the metric value (count or nanoseconds).
    
    Example query patterns:
    - Find slow operations: WHERE entry_type = 'op-duration-max' AND uint64_value > 1000000000
    - Buffer health: WHERE entry_type LIKE 'buffer-%'
    - Error rates: Compare op-invocations vs op-errors
    
    See [Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md) for complete list.
  `,
};
```

## Benefits

1. **Actual Behavior Analysis**: AI sees real execution traces, not just static code
2. **Performance Insights**: AI can identify slow operations and memory issues
3. **Error Analysis**: AI can trace error propagation through spans
4. **Test Validation**: AI can verify that traces match expected patterns
5. **Feature Flag Analytics**: AI can analyze A/B testing effectiveness
6. **Production Debugging**: Authorized access to production trace data
7. **Context Efficiency**: Detailed data only loaded when specifically requested

This integration enables AI agents to make informed decisions based on actual application behavior rather than just code
analysis.

## Arrow Table Conversion

**Purpose**: MCP tools convert SpanBuffer data to Arrow tables for efficient querying and analysis.

### Zero-Copy Conversion

MCP tools use **zero-copy Arrow conversion** to transform SpanBuffer data into Arrow tables. This process:

- **Uses direct TypedArray references**: No copying of data during conversion (see
  [Arrow Table Structure](./01f_arrow_table_structure.md#zero-copy-mandate))
- **Builds dictionaries in cold path**: String interning happens during conversion, not at write time
- **Two-pass tree conversion**: First pass builds dictionaries, second pass creates RecordBatches (see
  [Tree Walker and Arrow Conversion](./01k_tree_walker_and_arrow_conversion.md))
- **Shared dictionaries**: All RecordBatches reference the same dictionary vectors for efficient storage

The conversion happens in the cold path (when MCP tools are called), ensuring zero overhead on the hot path (runtime
logging).

## Arrow Table Column Reference

When querying trace data via MCP tools, the following columns are available:

| Column             | Description                                                                                                                                                    |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `message`          | Span name, log message template, exception message, OR flag name (see [Message Column](#message-column))                                                       |
| `package_name`     | npm package name of the module the op is bound to (see [Module Identification](#module-identification))                                                        |
| `package_path`     | Path within package where module was defined (see [Module Identification](#module-identification))                                                             |
| `entry_type`       | `span-start`, `span-ok`, `span-err`, `info`, `debug`, `op-invocations`, `buffer-writes`, etc. (see [Entry Types](./01h_entry_types_and_logging_primitives.md)) |
| `trace_id`         | Request correlation ID (W3C format: 32 lowercase hex characters). For test runs, `testRunId` IS the `trace_id`                                                 |
| `thread_id`        | Thread/worker identifier (uint64, crypto-secure random)                                                                                                        |
| `span_id`          | Unit of work within thread (uint32, incrementing counter)                                                                                                      |
| `parent_thread_id` | Parent span's thread (uint64, nullable - null for root spans)                                                                                                  |
| `parent_span_id`   | Parent span's ID (uint32, nullable - null for root spans)                                                                                                      |
| `ff_value`         | Feature flag value (dictionary-encoded string, S.category). Flag name is in `message` column for `ff-access`/`ff-usage` entries                                |

### Message Column

The `message` column serves different purposes based on entry type:

- **Span entries** (`span-start`, `span-ok`, `span-err`, `span-exception`): Contains the span name
- **Log entries** (`info`, `debug`, `warn`, `error`): Contains the message template (format string, NOT interpolated)
- **Feature flag entries** (`ff-access`, `ff-usage`): Contains the flag name
- **Op metrics** (`op-invocations`, `op-errors`, etc.): Contains the op name
- **Exception entries** (`span-exception`): Contains the exception message

See [Arrow Table Structure](./01f_arrow_table_structure.md#the-message-system-column) for complete details.

### Module Identification

The `package_name` and `package_path` columns work together to identify the source location:

- **`package_name`**: npm package name (e.g., `'@smoothbricks/lmao'`) - globally unique namespace
- **`package_path`**: Path within package relative to package.json (e.g., `'src/services/user.ts'`)
- **Dual module attribution**:
  - **Row 0 (span-start)**: Uses `callsiteModule` - identifies where the span was invoked (call site)
  - **Rows 1+ (span-end, log entries)**: Uses `module` - identifies where the code executes (op definition)
  - This enables distinguishing "who called this span" vs "where is the code that runs"

Example: If `httpModule.GET` is called from `userService.createUser`, row 0 shows `userService` (caller) and rows 1+
show `httpModule` (execution).

See [Arrow Table Structure](./01f_arrow_table_structure.md#module-identification) for complete details.

### Span Identification

Spans are uniquely identified by the composite `(thread_id, span_id)`:

- **`thread_id`**: Crypto-secure 64-bit random, generated once per thread/worker
- **`span_id`**: Thread-local incrementing counter (32-bit)
- **Parent relationship**: Use `parent_thread_id` + `parent_span_id` to find parent spans
- **Global uniqueness**: `(trace_id, thread_id, span_id)` is globally unique

To query a specific span:

```sql
SELECT * FROM traces
WHERE thread_id = @thread_id AND span_id = @span_id;
```

To find parent spans:

```sql
SELECT * FROM traces
WHERE thread_id = @parent_thread_id AND span_id = @parent_span_id;
```

**Example MCP Query**:

```typescript
// Get a specific span by identity
const span = await mcp.callTool('get_span_by_identity', {
  threadId: '0x1a2b3c4d5e6f7890',
  spanId: 42,
  traceId: 'test-run-abc123', // Optional validation
});

// Get parent span
const parentSpan = await mcp.callTool('get_parent_span', {
  threadId: '0x1a2b3c4d5e6f7890',
  spanId: 42,
  traceId: 'test-run-abc123',
});
```

See [Arrow Table Structure](./01f_arrow_table_structure.md#span-identification) for complete details.

See [Arrow Table Structure](./01f_arrow_table_structure.md) for complete schema.
