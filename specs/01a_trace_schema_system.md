# Trace Schema System

## Overview

The trace schema system provides type-safe, high-performance configuration and attribute management for the trace logging system. It handles three types of data:

1. **Tag Attributes** - Structured data logged to spans
2. **Feature Flags** - Dynamic behavior configuration with analytics
3. **Environment Variables** - Static deployment configuration

## Design Philosophy

**Key Insight**: Different types of configuration data have different access patterns, tracking needs, and performance requirements. The system optimizes each type appropriately rather than forcing them into a single pattern.

| Type | Complexity | Tracking | Performance | Use Case |
|------|------------|----------|-------------|----------|
| **Tag Attributes** | Schema + validation | Always | Columnar writes | Structured span data |
| **Feature Flags** | Schema + evaluator | Always | Cached + analytics | A/B testing, rollouts |
| **Environment Variables** | Plain object | Never | Property access | Infrastructure config |

## Tag Attribute Schema Definition

**Purpose**: Define structured data that can be logged to spans with type safety and automatic masking.

```typescript
// Base attributes available everywhere
const baseAttributes = defineTagAttributes({
  requestId: S.string,
  userId: S.optional(S.string.with(S.hash)),
  timestamp: S.number,
  
  // Feature flag operations now use ff-access and ff-usage operations
  // Context stored in regular attribute columns (user_id, user_plan, etc.)
  // No separate featureFlagAccess/featureFlagUsage attributes needed
});

// Database-specific attributes
const dbAttributes = baseAttributes.extend({
  query: S.string.with(S.mask, 'sql'),
  duration: S.number,
  rowCount: S.optional(S.number),
  connectionId: S.string,
  operation: S.union(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
});

// Payment-specific attributes  
const paymentAttributes = baseAttributes.extend({
  amount: S.number,
  currency: S.string,
  paymentMethodId: S.string.with(S.mask, 'hash'),
  merchantId: S.string,
});
```

**Why This Design**:
- **Composable**: Base attributes can be extended for specific domains
- **Type-safe**: Full TypeScript inference for tag operations
- **Masking rules**: Sensitive data automatically masked during serialization
- **Columnar storage**: Schema drives efficient TypedArray column generation

## Feature Flag Schema Definition

**Purpose**: Define feature flags with type-safe access and automatic analytics tracking.

```typescript
const featureFlags = defineFeatureFlags({
  // Sync flags - cached/static values, direct property access
  advancedValidation: S.boolean.default(false).sync(),
  debugMode: S.boolean.default(false).sync(),
  maxRetries: S.number.default(3).sync(),
  
  // Async flags - require runtime evaluation
  userSpecificLimit: S.number.default(100).async(),
  dynamicPaymentProvider: S.union(['stripe', 'paypal']).default('stripe').async(),
});

interface FeatureFlagEvaluator<T> {
  // Sync flags become direct properties
  [K in SyncFlagKeys<T>]: T[K];
  
  // Async flags use methods
  get<K extends AsyncFlagKeys<T>>(flag: K): Promise<T[K]>;
  
  // Usage tracking
  trackUsage<K extends keyof T>(flag: K, context?: UsageContext): void;
}
```

**Why This Design**:
- **Performance**: Sync flags are direct properties (no method calls)
- **Analytics**: All flag access automatically logged for A/B testing
- **Type safety**: Compile-time enforcement of sync vs async access patterns
- **Flexibility**: Supports both cached and dynamic evaluation

## Environment Variable Configuration

**Purpose**: Provide simple, fast access to deployment configuration without overhead.

```typescript
// Simple configuration object loaded at startup
const environmentConfig = {
  // Static values from process.env or config service
  awsRegion: process.env.AWS_REGION || 'us-east-1',
  nodeEnv: process.env.NODE_ENV || 'development',
  logLevel: process.env.LOG_LEVEL || 'info',
  
  // Sensitive values - real values for app use, masked only if logged
  databaseUrl: process.env.DATABASE_URL,
  apiKey: process.env.API_KEY,
  
  // Numeric values
  maxConnections: parseInt(process.env.MAX_CONNECTIONS) || 100,
  rateLimitPerMinute: parseInt(process.env.RATE_LIMIT) || 1000,
};

// No interface needed - just a plain object
type EnvironmentConfig = typeof environmentConfig;
```

**Why This Design**:
- **Zero overhead**: Just property access, no evaluator or tracking
- **Security**: Values only appear in traces if explicitly logged
- **Simplicity**: No schema validation needed for deployment config
- **Performance**: Fastest possible access pattern

## Feature Flag Evaluator Implementation

**Purpose**: Handle feature flag evaluation with span-aware logging for analytics.

```typescript
// Feature flag evaluator (only flags need complex evaluation)
class FeatureFlagEvaluator<T> {
  protected schema: T;
  protected context: EvaluationContext;
  protected evaluator: FlagEvaluator;
  
  constructor(schema: T, context: EvaluationContext, evaluator: FlagEvaluator) {
    this.schema = schema;
    this.context = context;
    this.evaluator = evaluator;
    
    // Initialize sync flags as direct properties
    this.initializeSyncFlags();
  }
  
  protected initializeSyncFlags(): void {
    // Create getters for each sync flag that also log access
    for (const [key, config] of Object.entries(this.schema)) {
      if (config.type === 'sync') {
        Object.defineProperty(this, key, {
          get: () => {
            const value = this.evaluator[key];
            
            // TODO: Use low-level column writers (design TBD)
            // this.columnWriters.writeEntryType('ff-access');
            // this.columnWriters.writeFfName(key);
            // this.columnWriters.writeFfValue(value);
            // + context attributes from evaluation context
            
            return value;
          },
          enumerable: true,
          configurable: true
        });
      }
    }
  }
  
  async get<K extends AsyncFlagKeys<T>>(flag: K): Promise<T[K]> {
    const value = await this.evaluator.getAsync(flag, this.context);
    
    // TODO: Use low-level column writers (design TBD)
    // this.columnWriters.writeEntryType('ff-access');
    // this.columnWriters.writeFfName(flag);
    // this.columnWriters.writeFfValue(value);
    // + context attributes from evaluation context
    
    return value;
  }
  
  trackUsage<K extends keyof T>(flag: K, context?: UsageContext): void {
    // TODO: Use low-level column writers (design TBD)
    // this.columnWriters.writeEntryType('ff-usage');
    // this.columnWriters.writeFfName(flag);
    // this.columnWriters.writeAction(context?.action);
    // this.columnWriters.writeOutcome(context?.outcome);
    // + evaluation context attributes
  }
}
```

**Why This Implementation**:
- **Automatic logging**: All flag access logged to current span for analytics
- **Performance optimization**: Sync flags cached as properties
- **Span correlation**: Flag usage tied to specific operations
- **A/B testing support**: Explicit usage tracking for product analytics

## Schema Integration Patterns

### Context Creation

```typescript
// Create context at request boundary
function createRequestContext(params: { 
  requestId: string, 
  userId?: string 
}): Context {
  return {
    ...params,
    traceId: generateTraceId(),
    
    // Feature flag evaluator with context
    ff: new FeatureFlagEvaluator(
      featureFlags,
      { userId: params.userId, requestId: params.requestId },
      new DatabaseFlagEvaluator()
    ),
    
    // Environment config (just a plain object)
    env: environmentConfig
  };
}
```

### Task Integration

```typescript
// Module context with tag attributes
const { task } = createModuleContext({
  moduleMetadata: {
    gitSha: 'abc123...',
    filePath: 'src/services/user.ts',
    moduleName: 'UserService'
  },
  tagAttributes: dbAttributes  // Use DB-specific attributes
});

export const createUser = task('create-user', async (ctx, userData: UserData) => {
  // ctx has: tag, ok, err, span, ff (feature flags), env (environment)
  
  // Feature flag access (with automatic analytics)
  if (ctx.ff.advancedValidation) {
    const result = await performAdvancedValidation(userData);
    ctx.ff.trackUsage('advancedValidation', {
      action: 'validation_performed',
      outcome: result.success ? 'success' : 'failure'
    });
  }
  
  // Environment access (just plain property access, no tracking)
  const region = ctx.env.awsRegion;        // 'us-east-1'
  const maxConnections = ctx.env.maxConnections; // 100
  const dbUrl = ctx.env.databaseUrl;       // Real postgres URL
  
  // Tag operations (typed based on dbAttributes)
  ctx.tag.requestId(ctx.requestId);  // Sets bit 0, writes to attr_requestId column
  ctx.tag.userId(userData.id);       // Sets bit 1, writes to attr_userId column  
  ctx.tag.operation('INSERT');       // Sets bit 4, writes to attr_operation column

  // Or, object-based API for multiple attributes, appending to multiple columns in the same row
  ctx.tag({ 
    requestId: ctx.requestId,
    userId: userData.id,
    operation: 'INSERT'
  });
  
  // Masking only happens if you explicitly log environment values
  ctx.tag.region(region);                  // Safe to log
  // ctx.tag.databaseUrl(dbUrl);           // Would be masked by tag schema
  
  // Child spans create child SpanBuffers in tree structure
  const validation = await ctx.span('validate-user', async (ctx) => {
    ctx.tag.query('SELECT COUNT(*) FROM users WHERE email = ?');  // Sets bit 5
    ctx.tag.duration(12.5);                                       // Sets bit 2
    
    if (existingUser) {
      return ctx.err('USER_EXISTS', { email: userData.email });
    }
    return ctx.ok({ valid: true });
  });
  
  if (!validation.success) {
    return ctx.err('VALIDATION_FAILED', validation.error);
  }
  
  const user = await db.createUser(userData);
  return ctx.ok(user);
});
```

## Performance Characteristics

### Tag Attributes
- **Runtime**: <0.1ms per tag operation (TypedArray writes + bitmap)
- **Memory**: Columnar storage with null bitmaps
- **Type safety**: Zero runtime overhead (compile-time only)

### Feature Flags
- **Sync flags**: Direct property access (fastest)
- **Async flags**: Cached evaluation with analytics logging
- **Analytics**: Automatic span correlation for A/B testing

### Environment Variables
- **Access**: Plain property lookup (zero overhead)
- **Security**: Values only in traces if explicitly logged
- **Masking**: Applied during background processing if logged

## Benefits

1. **Type Safety**: Full TypeScript inference across all three systems
2. **Performance Optimization**: Each system optimized for its access patterns
3. **Security by Default**: Sensitive data only appears in traces when explicitly logged
4. **Analytics Integration**: Feature flags automatically tracked for product decisions
5. **Composable Schemas**: Tag attributes can be extended and reused across modules
6. **Zero Configuration Overhead**: Environment variables are just plain objects

This design provides the right tool for each job - sophisticated analytics for feature flags, structured logging for tag attributes, and zero-overhead access for environment configuration. 
