# AI Agent Integration

## Overview

The AI agent integration provides structured access to trace data for automated analysis and debugging. It includes:

1. **Model Context Protocol (MCP) server** for standardized trace querying
2. **Test framework integration** with automatic test run correlation
3. **AI agent workflow** for trace-driven development and debugging
4. **Production deployment** with authentication and privacy controls

## Design Philosophy

**Key Insight**: AI agents need structured access to trace data without consuming limited context windows. The MCP protocol provides tool-based access where detailed trace data is only loaded when specifically requested.

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
  name: "trace-analyzer",
  version: "1.0.0"
});

// Tool: Query traces by test run ID
traceServer.addTool({
  name: "get_traces_by_test_run",
  description: "Get all traces for a specific test run",
  inputSchema: {
    type: "object",
    properties: {
      testRunId: { type: "string" }
    }
  },
  handler: async ({ testRunId }) => {
    return await queryTraceDatabase({ testRunId });
  }
});

// Tool: Query traces by span type
traceServer.addTool({
  name: "get_traces_by_span",
  description: "Get traces filtered by span name/type",
  inputSchema: {
    type: "object", 
    properties: {
      spanName: { type: "string" },
      testRunId: { type: "string", optional: true },
      timeRange: { type: "object", optional: true }
    }
  },
  handler: async ({ spanName, testRunId, timeRange }) => {
    return await queryTraceDatabase({ spanName, testRunId, timeRange });
  }
});

// Tool: Get performance metrics
traceServer.addTool({
  name: "get_performance_metrics", 
  description: "Get performance summary for a test run",
  inputSchema: {
    type: "object",
    properties: {
      testRunId: { type: "string" }
    }
  },
  handler: async ({ testRunId }) => {
    return await generatePerformanceReport(testRunId);
  }
});

// Tool: Analyze feature flag usage
traceServer.addTool({
  name: "analyze_feature_flag_usage",
  description: "Analyze feature flag access patterns in traces",
  inputSchema: {
    type: "object",
    properties: {
      testRunId: { type: "string" },
      flagName: { type: "string", optional: true }
    }
  },
  handler: async ({ testRunId, flagName }) => {
    return await analyzeFeatureFlagUsage({ testRunId, flagName });
  }
});
```

**Why This Design**:
- **Tool-based access**: AI agents call specific functions rather than loading all data
- **Flexible querying**: Multiple query patterns for different analysis needs
- **Performance focus**: Dedicated tools for performance analysis
- **Feature flag analytics**: Specialized tools for A/B testing insights

## Test Framework Integration

**Purpose**: Automatically correlate traces with test runs for AI-driven analysis.

### Jest Plugin

```typescript
// jest-trace-plugin
export default {
  setupFilesAfterEnv: ['<rootDir>/jest-trace-setup.js'],
  reporters: [
    'default',
    ['jest-trace-reporter', { 
      outputDir: './traces',
      includeTestMetadata: true 
    }]
  ]
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
    testFile 
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
        testFile: filePath
      });
    });
  }
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
  { testCase: 'user login flow', span: 'user-validation', duration: 2.3ms, success: true },
  { testCase: 'user login flow', span: 'database-query', duration: 45ms, success: true },
  { testCase: 'email notification', span: 'email-send', duration: 120ms, error: 'SMTP timeout' },
  { testCase: 'password reset', span: 'email-send', duration: 125ms, error: 'SMTP timeout' },
  // ... more traces
]

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

```typescript
// Production MCP server with OAuth
const productionTraceServer = new MCPServer({
  name: "production-trace-analyzer",
  version: "1.0.0",
  authentication: {
    provider: "oauth2",
    scopes: ["trace:read", "trace:decrypt"]
  }
});

// Tool requires authentication
traceServer.addTool({
  name: "get_production_traces",
  description: "Query production traces (requires authentication)",
  requiresAuth: true,
  requiredScopes: ["trace:read"],
  handler: async ({ testRunId }, { user, scopes }) => {
    // Only return decrypted data if user has decrypt scope
    const includeDecrypted = scopes.includes("trace:decrypt");
    return await queryProductionTraces({ testRunId, includeDecrypted });
  }
});

// Tool for performance analysis
traceServer.addTool({
  name: "analyze_production_performance",
  description: "Analyze production performance patterns",
  requiresAuth: true,
  requiredScopes: ["trace:read"],
  handler: async ({ timeRange, service }, { user }) => {
    return await analyzeProductionPerformance({ 
      timeRange, 
      service,
      userId: user.id // For audit logging
    });
  }
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
  name: "get_production_trace_details",
  description: "Get detailed production trace with optional decryption",
  requiresAuth: true,
  handler: async ({ traceId, includeDecrypted }, { user, scopes }) => {
    const trace = await getProductionTrace(traceId);
    
    if (includeDecrypted && scopes.includes("trace:decrypt")) {
      // Decrypt sensitive data for authorized users
      const decryptedData = await decryptTraceData(
        trace.encryptedData, 
        trace.encryptionKeyId,
        user.id
      );
      
      return {
        ...trace.maskedData,
        decryptedData,
        accessLevel: trace.accessLevel
      };
    }
    
    // Return only masked data
    return trace.maskedData;
  }
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
  name: "get_distributed_trace",
  description: "Get complete trace across all services",
  inputSchema: {
    type: "object",
    properties: {
      traceId: { type: "string" },
      services: { type: "array", items: { type: "string" }, optional: true }
    }
  },
  handler: async ({ traceId, services }) => {
    // Use ClickHouse chDB or AWS Athena for cross-service queries
    return await queryDistributedTrace({ traceId, services });
  }
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
    ✅ ALWAYS generate: ctx.withSpan('process-user-request', async () => { ... });
    
    Required patterns:
    - Use task() wrapper for all operations
    - Use ctx.tag.* for structured data
    - Use ctx.ok() and ctx.err() for results
    - Use ctx.span() for nested operations
    
    Schema examples:
    ctx.tag.userId(user.id);        // Automatically hashed
    ctx.tag.duration(45);           // Performance tracking
    ctx.tag.operation('SELECT');    // Enum validation
  `,
  
  featureFlags: `
    Feature flag access patterns:
    
    Sync flags (direct property access):
    if (ctx.ff.advancedValidation) { ... }
    
    Async flags (method call):
    const limit = await ctx.ff.get('userSpecificLimit');
    
    Usage tracking (for A/B testing):
    ctx.ff.trackUsage('experimentalFeature', { 
      action: 'used', 
      outcome: 'success' 
    });
  `,
  
  environmentVariables: `
    Environment variable access (zero overhead):
    
    const region = ctx.env.awsRegion;      // Just property access
    const dbUrl = ctx.env.databaseUrl;     // Real value for app use
    
    Security: Environment values only appear in traces if explicitly logged:
    ctx.tag.region(region);                // Safe to log
    // ctx.tag.databaseUrl(dbUrl);         // Would be masked
  `
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

This integration enables AI agents to make informed decisions based on actual application behavior rather than just code analysis. 
