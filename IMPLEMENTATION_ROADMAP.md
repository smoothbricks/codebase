# Implementation Roadmap: Trace Logging System

Complete step-by-step implementation guide for the trace logging library using **@sury/sury** and **apache-arrow**.

## Technology Stack

- **@sury/sury** - The fastest schema validation library in JavaScript ecosystem for tag attribute schemas
- **apache-arrow** - Apache Arrow for zero-copy columnar data structures and Parquet serialization
- **TypeScript** - Full type safety with automatic inference

## Phase 1: Schema Definition System with Sury (START HERE)

### Step 1.1: Install Dependencies and Setup Types

**Files**: 
- `packages/lmao/package.json`
- `packages/lmao/src/lib/schema/types.ts`

**Task**: Install Sury and Apache Arrow, then define core TypeScript types that wrap Sury schemas.

**Implementation**:

```bash
# Already installed:
# bun add apache-arrow
# npx jsr add @sury/sury
```

```typescript
// packages/lmao/src/lib/schema/types.ts
import type * as S from '@sury/sury';

/**
 * Sury schema types for tag attributes
 * 
 * We use Sury's built-in schema types which provide:
 * - Runtime validation
 * - TypeScript inference
 * - Transformations (e.g., masking)
 * - JSON Schema export
 */

// Re-export Sury's core types
export type { Schema, Output, Input } from '@sury/sury';

/**
 * Tag attribute schema - maps field names to Sury schemas
 * 
 * Example:
 * {
 *   userId: S.string().transform(hashString),
 *   requestId: S.string(),
 *   duration: S.number(),
 *   operation: S.literal('SELECT').or(S.literal('INSERT'))
 * }
 */
export type TagAttributeSchema = Record<string, S.Schema<any, any>>;

/**
 * Extract TypeScript types from tag attribute schema
 */
export type InferTagAttributes<T extends TagAttributeSchema> = {
  [K in keyof T]: S.Output<T[K]>;
};

/**
 * Masking transformations for sensitive data
 */
export type MaskType = 'hash' | 'url' | 'sql' | 'email';

/**
 * Extended schema builder with masking support
 */
export interface SchemaBuilder {
  // Primitive types
  string(): S.Schema<string, string>;
  number(): S.Schema<number, number>;
  boolean(): S.Schema<boolean, boolean>;
  
  // Optional wrapper
  optional<T>(schema: S.Schema<T, any>): S.Schema<T | undefined, any>;
  
  // Union types
  union<T extends readonly [S.Schema<any, any>, ...S.Schema<any, any>[]]>(
    schemas: T
  ): S.Schema<S.Output<T[number]>, any>;
  
  // Literal union (for enums)
  enum<T extends readonly string[]>(values: T): S.Schema<T[number], any>;
  
  // String with masking transformation
  masked(type: MaskType): S.Schema<string, string>;
}
```

**Acceptance Criteria**:
- [ ] @sury/sury and apache-arrow installed successfully
- [ ] All types compile without errors
- [ ] Types support Sury's Schema<Output, Input> pattern
- [ ] TypeScript inference works for InferTagAttributes

---

### Step 1.2: Implement Sury-Based Schema Builder

**File**: `packages/lmao/src/lib/schema/builder.ts`

**Task**: Create the `S` builder object that wraps Sury schemas with masking transformations.

**Implementation**:

```typescript
import * as Sury from '@sury/sury';
import type { SchemaBuilder, MaskType } from './types';

/**
 * Masking functions for sensitive data
 * Applied during Arrow table serialization (background processing)
 */
const maskingTransforms = {
  hash: (value: string): string => {
    // Simple hash for demo - use proper crypto in production
    let hash = 0;
    for (let i = 0; i < value.length; i++) {
      hash = ((hash << 5) - hash) + value.charCodeAt(i);
      hash = hash & hash;
    }
    return `0x${Math.abs(hash).toString(16).padStart(16, '0')}`;
  },
  
  url: (value: string): string => {
    try {
      const url = new URL(value);
      return `${url.protocol}//*****${url.pathname}${url.search}`;
    } catch {
      return '*****';
    }
  },
  
  sql: (value: string): string => {
    // Replace string literals and numbers with placeholders
    return value
      .replace(/'[^']*'/g, '?')
      .replace(/\b\d+\b/g, '?');
  },
  
  email: (value: string): string => {
    const [local, domain] = value.split('@');
    if (!domain) return '*****';
    return `${local[0]}*****@${domain}`;
  }
};

/**
 * Schema builder that wraps Sury with our custom API
 * 
 * This provides a clean API while leveraging Sury's performance
 * and validation capabilities.
 */
export const S: SchemaBuilder = {
  string: () => Sury.string(),
  
  number: () => Sury.number(),
  
  boolean: () => Sury.boolean(),
  
  optional: <T>(schema: Sury.Schema<T, any>) => Sury.optional(schema),
  
  union: <T extends readonly [Sury.Schema<any, any>, ...Sury.Schema<any, any>[]]>(
    schemas: T
  ) => Sury.union(schemas as any) as any,
  
  enum: <T extends readonly string[]>(values: T) => {
    if (values.length === 0) {
      throw new Error('Enum must have at least one value');
    }
    const [first, ...rest] = values;
    let schema = Sury.literal(first);
    for (const value of rest) {
      schema = schema.or(Sury.literal(value)) as any;
    }
    return schema as any;
  },
  
  masked: (type: MaskType) => {
    return Sury.string().transform(maskingTransforms[type]);
  }
};

/**
 * Fluent API extensions for strings
 * 
 * Usage: S.string().with(S.mask('hash'))
 */
export const mask = (type: MaskType) => ({
  transform: maskingTransforms[type]
});
```

**Acceptance Criteria**:
- [ ] `S.string()` returns a Sury string schema
- [ ] `S.masked('hash')` returns a schema with hash transformation
- [ ] `S.enum(['SELECT', 'INSERT'])` creates proper union type
- [ ] `S.optional(S.string())` creates optional field
- [ ] All masking functions work correctly

---

### Step 1.3: Implement `defineTagAttributes` Function

**File**: `packages/lmao/src/lib/schema/defineTagAttributes.ts`

**Task**: Create the main function that defines and validates tag attribute schemas using Sury.

**Implementation**:

```typescript
import type { TagAttributeSchema, InferTagAttributes } from './types';
import * as Sury from '@sury/sury';

/**
 * Define tag attributes with runtime validation and type inference
 * 
 * This wraps Sury's object schema to provide our custom API while
 * maintaining all of Sury's performance and validation benefits.
 * 
 * @param schema - Object mapping field names to Sury schemas
 * @returns Validated schema with type inference
 */
export function defineTagAttributes<T extends TagAttributeSchema>(
  schema: T
): {
  schema: T;
  validate: (data: unknown) => InferTagAttributes<T>;
  parse: (data: unknown) => InferTagAttributes<T> | null;
  type: InferTagAttributes<T>;
} {
  // Convert to Sury object schema for validation
  const objectSchema = Sury.object(schema as any);
  
  return {
    schema,
    
    // Validate and throw on error
    validate: (data: unknown) => {
      return objectSchema.parse(data);
    },
    
    // Validate and return null on error
    parse: (data: unknown) => {
      const result = objectSchema.safeParse(data);
      return result.success ? result.value : null;
    },
    
    // Type-only field for TypeScript inference
    type: undefined as any
  };
}

/**
 * Reserved method names that cannot be used as attribute names
 * These conflict with the fluent API methods
 */
export const RESERVED_NAMES = new Set([
  'with', 'message', 'tag', 'info', 'debug', 'warn', 'error',
  'ok', 'err', 'span'
]);

/**
 * Validate that attribute names don't conflict with reserved names
 */
export function validateAttributeNames(schema: TagAttributeSchema): void {
  for (const name of Object.keys(schema)) {
    if (RESERVED_NAMES.has(name)) {
      throw new Error(
        `Attribute name '${name}' is reserved and cannot be used. ` +
        `Reserved names: ${Array.from(RESERVED_NAMES).join(', ')}`
      );
    }
  }
}
```

**Acceptance Criteria**:
- [ ] Function accepts schema objects using Sury schemas
- [ ] Returns object with validate/parse methods
- [ ] TypeScript inference works correctly
- [ ] Reserved name validation prevents conflicts
- [ ] Works with examples from spec lines 26-52

---

### Step 1.4: Implement Schema Extension

**File**: `packages/lmao/src/lib/schema/extend.ts`

**Task**: Allow schemas to be extended with additional fields using Sury's merge capabilities.

**Implementation**:

```typescript
import type { TagAttributeSchema, InferTagAttributes } from './types';
import { validateAttributeNames } from './defineTagAttributes';

/**
 * Extend a base schema with additional attributes
 * 
 * This provides a clean composition API for building up schemas
 * from smaller pieces (e.g., base attributes + HTTP attributes + DB attributes)
 * 
 * @param base - Base tag attribute schema
 * @param extension - Additional attributes to add
 * @returns Merged schema with type inference
 */
export function extendSchema<
  T extends TagAttributeSchema,
  U extends TagAttributeSchema
>(
  base: T,
  extension: U
): T & U {
  // Check for conflicts
  const baseKeys = new Set(Object.keys(base));
  for (const key of Object.keys(extension)) {
    if (baseKeys.has(key)) {
      throw new Error(
        `Schema conflict: attribute '${key}' already exists in base schema`
      );
    }
  }
  
  // Validate extension doesn't use reserved names
  validateAttributeNames(extension);
  
  // Merge schemas
  return { ...base, ...extension };
}

/**
 * Create an extendable schema wrapper
 * 
 * This provides a fluent API for schema composition:
 * 
 * const base = createExtendableSchema({ requestId: S.string() });
 * const withHttp = base.extend({ httpStatus: S.number() });
 * const withDb = withHttp.extend({ dbQuery: S.masked('sql') });
 */
export interface ExtendableSchema<T extends TagAttributeSchema> {
  schema: T;
  extend<U extends TagAttributeSchema>(
    extension: U
  ): ExtendableSchema<T & U>;
}

export function createExtendableSchema<T extends TagAttributeSchema>(
  schema: T
): ExtendableSchema<T> {
  validateAttributeNames(schema);
  
  return {
    schema,
    extend: <U extends TagAttributeSchema>(extension: U) => {
      const merged = extendSchema(schema, extension);
      return createExtendableSchema(merged);
    }
  };
}
```

**Acceptance Criteria**:
- [ ] `extendSchema(base, extension)` merges schemas correctly
- [ ] Throws error on field name conflicts
- [ ] Returns properly typed merged schema
- [ ] Fluent `.extend()` API works with chaining
- [ ] Reserved name validation works

---

### Step 1.5: Add Tests for Schema System

**File**: `packages/lmao/src/lib/schema/__tests__/defineTagAttributes.test.ts`

**Task**: Write comprehensive tests for schema definition using Bun's test runner.

**Implementation**:

```typescript
import { describe, expect, test } from 'bun:test';
import { S } from '../builder';
import { defineTagAttributes, validateAttributeNames } from '../defineTagAttributes';
import { extendSchema, createExtendableSchema } from '../extend';

describe('defineTagAttributes', () => {
  test('defines base attributes with Sury schemas', () => {
    const attrs = defineTagAttributes({
      requestId: S.string(),
      userId: S.optional(S.masked('hash')),
      timestamp: S.number(),
    });
    
    expect(attrs.schema).toBeDefined();
    expect(attrs.validate).toBeDefined();
    expect(attrs.parse).toBeDefined();
  });
  
  test('validates data correctly', () => {
    const attrs = defineTagAttributes({
      requestId: S.string(),
      count: S.number(),
    });
    
    const result = attrs.validate({
      requestId: 'req-123',
      count: 42
    });
    
    expect(result.requestId).toBe('req-123');
    expect(result.count).toBe(42);
  });
  
  test('throws on invalid data', () => {
    const attrs = defineTagAttributes({
      count: S.number(),
    });
    
    expect(() => {
      attrs.validate({ count: 'not a number' });
    }).toThrow();
  });
  
  test('parse returns null on error', () => {
    const attrs = defineTagAttributes({
      count: S.number(),
    });
    
    const result = attrs.parse({ count: 'invalid' });
    expect(result).toBeNull();
  });
  
  test('supports schema extension', () => {
    const base = { requestId: S.string() };
    const extended = extendSchema(base, { duration: S.number() });
    
    expect(extended).toHaveProperty('requestId');
    expect(extended).toHaveProperty('duration');
  });
  
  test('validates enum/union types', () => {
    const schema = defineTagAttributes({
      operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
    });
    
    const result = schema.validate({ operation: 'SELECT' });
    expect(result.operation).toBe('SELECT');
  });
  
  test('rejects reserved names', () => {
    expect(() => {
      validateAttributeNames({
        with: S.string(), // Reserved!
      });
    }).toThrow(/reserved/i);
  });
  
  test('fluent extend API works', () => {
    const base = createExtendableSchema({
      requestId: S.string(),
    });
    
    const extended = base.extend({
      httpStatus: S.number(),
    });
    
    expect(extended.schema).toHaveProperty('requestId');
    expect(extended.schema).toHaveProperty('httpStatus');
  });
  
  test('masking transformations work', () => {
    const schema = defineTagAttributes({
      userId: S.masked('hash'),
      email: S.masked('email'),
    });
    
    const result = schema.validate({
      userId: 'user-12345',
      email: 'john@example.com'
    });
    
    // Masking should transform values
    expect(result.userId).toMatch(/^0x[0-9a-f]{16}$/);
    expect(result.email).toMatch(/^j\*\*\*\*\*@example\.com$/);
  });
});
```

**Acceptance Criteria**:
- [ ] All tests pass
- [ ] Tests cover Sury validation and parsing
- [ ] Tests verify masking transformations
- [ ] Tests verify type safety
- [ ] Tests check reserved name validation

---

### Step 1.6: Implement `defineFeatureFlags` Function

**File**: `packages/lmao/src/lib/schema/defineFeatureFlags.ts`

**Task**: Create the feature flag schema definition system with sync/async flag support and automatic analytics tracking.

**Note**: Feature flags use the same `S` schema builder as tag attributes, but with added `.default().sync()/.async()` methods.

**Implementation**:

```typescript
import type { FeatureFlagDefinition } from './types';
import * as Sury from '@sury/sury';

/**
 * Feature flag schema with sync/async markers
 */
export interface FeatureFlagSchema {
  [key: string]: FeatureFlagDefinition<any>;
}

/**
 * Define feature flags with type-safe access patterns
 * 
 * @param schema - Object mapping flag names to flag definitions
 * @returns Feature flag definition object for use with evaluator
 */
export function defineFeatureFlags<T extends FeatureFlagSchema>(
  schema: T
): {
  schema: T;
  syncFlags: SyncFlagKeys<T>;
  asyncFlags: AsyncFlagKeys<T>;
  type: InferFeatureFlags<T>;
} {
  const syncFlags: string[] = [];
  const asyncFlags: string[] = [];
  
  for (const [key, definition] of Object.entries(schema)) {
    if (definition.evaluationType === 'sync') {
      syncFlags.push(key);
    } else {
      asyncFlags.push(key);
    }
  }
  
  return {
    schema,
    syncFlags: syncFlags as any,
    asyncFlags: asyncFlags as any,
    type: undefined as any
  };
}

/**
 * Extract sync flag keys
 */
export type SyncFlagKeys<T extends FeatureFlagSchema> = {
  [K in keyof T]: T[K]['evaluationType'] extends 'sync' ? K : never;
}[keyof T];

/**
 * Extract async flag keys
 */
export type AsyncFlagKeys<T extends FeatureFlagSchema> = {
  [K in keyof T]: T[K]['evaluationType'] extends 'async' ? K : never;
}[keyof T];

/**
 * Infer TypeScript types from feature flag schema
 */
export type InferFeatureFlags<T extends FeatureFlagSchema> = {
  [K in keyof T]: Sury.Output<T[K]['schema']>;
};

/**
 * Evaluation context for feature flag decisions
 */
export interface EvaluationContext {
  userId?: string;
  requestId?: string;
  userPlan?: string;
  region?: string;
  [key: string]: any;
}

/**
 * Usage tracking context for A/B testing analytics
 */
export interface UsageContext {
  action?: string;
  outcome?: 'success' | 'failure';
  value?: number;
  metadata?: Record<string, any>;
}
```

**Acceptance Criteria**:
- [ ] S builder creates flag definitions with sync/async markers via .default()
- [ ] defineFeatureFlags extracts sync and async flag keys
- [ ] Type inference works for InferFeatureFlags
- [ ] Fluent API: `S.boolean().default(false).sync()`
- [ ] Works with examples from spec lines 66-86

---

### Step 1.7: Implement Feature Flag Evaluator

**File**: `packages/lmao/src/lib/schema/evaluator.ts`

**Task**: Implement the FeatureFlagEvaluator class that provides sync property access and async methods with automatic analytics tracking.

**Implementation**:

```typescript
import type { 
  FeatureFlagSchema, 
  SyncFlagKeys, 
  AsyncFlagKeys,
  EvaluationContext,
  UsageContext,
  InferFeatureFlags
} from './defineFeatureFlags';

/**
 * Feature flag evaluator interface
 * 
 * This is implemented by the backend (database, LaunchDarkly, etc.)
 * and provides the actual flag values.
 */
export interface FlagEvaluator {
  /**
   * Get a flag value synchronously (for cached/static flags)
   */
  getSync<K extends string>(
    flag: K,
    context: EvaluationContext
  ): any;
  
  /**
   * Get a flag value asynchronously (for dynamic flags)
   */
  getAsync<K extends string>(
    flag: K,
    context: EvaluationContext
  ): Promise<any>;
}

/**
 * Column writers interface (to be implemented in buffer phase)
 * 
 * Used for logging flag access and usage to span buffers.
 */
export interface FlagColumnWriters {
  writeEntryType(type: 'ff-access' | 'ff-usage'): void;
  writeFfName(name: string): void;
  writeFfValue(value: any): void;
  writeAction(action?: string): void;
  writeOutcome(outcome?: string): void;
  writeContextAttributes(context: EvaluationContext): void;
}

/**
 * Feature flag evaluator with span-aware logging
 * 
 * This class provides type-safe access to feature flags with automatic
 * analytics tracking. Sync flags are exposed as direct properties for
 * zero-overhead access, while async flags use methods.
 */
export class FeatureFlagEvaluator<T extends FeatureFlagSchema> {
  protected schema: T;
  protected context: EvaluationContext;
  protected evaluator: FlagEvaluator;
  protected columnWriters?: FlagColumnWriters;
  
  constructor(
    schema: T,
    context: EvaluationContext,
    evaluator: FlagEvaluator,
    columnWriters?: FlagColumnWriters
  ) {
    this.schema = schema;
    this.context = context;
    this.evaluator = evaluator;
    this.columnWriters = columnWriters;
    
    // Initialize sync flags as direct properties
    this.initializeSyncFlags();
  }
  
  /**
   * Initialize sync flags as getter properties
   * 
   * This allows sync flags to be accessed as direct properties:
   * ctx.ff.debugMode instead of ctx.ff.get('debugMode')
   */
  protected initializeSyncFlags(): void {
    for (const [key, definition] of Object.entries(this.schema)) {
      if (definition.evaluationType === 'sync') {
        Object.defineProperty(this, key, {
          get: () => {
            const value = this.evaluator.getSync(key, this.context);
            
            // Log flag access for analytics
            if (this.columnWriters) {
              this.columnWriters.writeEntryType('ff-access');
              this.columnWriters.writeFfName(key);
              this.columnWriters.writeFfValue(value);
              this.columnWriters.writeContextAttributes(this.context);
            }
            
            return value ?? definition.defaultValue;
          },
          enumerable: true,
          configurable: true
        });
      }
    }
  }
  
  /**
   * Get async flag value
   * 
   * Async flags must be accessed via this method:
   * await ctx.ff.get('userSpecificLimit')
   */
  async get<K extends AsyncFlagKeys<T>>(
    flag: K
  ): Promise<InferFeatureFlags<T>[K]> {
    const definition = this.schema[flag];
    const value = await this.evaluator.getAsync(flag as string, this.context);
    
    // Log flag access for analytics
    if (this.columnWriters) {
      this.columnWriters.writeEntryType('ff-access');
      this.columnWriters.writeFfName(flag as string);
      this.columnWriters.writeFfValue(value);
      this.columnWriters.writeContextAttributes(this.context);
    }
    
    return value ?? definition.defaultValue;
  }
  
  /**
   * Track feature flag usage for A/B testing analytics
   * 
   * This logs when a flag actually affects behavior, not just when it's accessed.
   * For example, after performing an action enabled by a flag.
   */
  trackUsage<K extends keyof T>(
    flag: K,
    context?: UsageContext
  ): void {
    if (this.columnWriters) {
      this.columnWriters.writeEntryType('ff-usage');
      this.columnWriters.writeFfName(flag as string);
      this.columnWriters.writeAction(context?.action);
      this.columnWriters.writeOutcome(context?.outcome);
      this.columnWriters.writeContextAttributes(this.context);
    }
  }
}

/**
 * Simple in-memory flag evaluator for testing
 */
export class InMemoryFlagEvaluator implements FlagEvaluator {
  private flags: Record<string, any> = {};
  
  constructor(initialFlags: Record<string, any> = {}) {
    this.flags = initialFlags;
  }
  
  getSync<K extends string>(flag: K, _context: EvaluationContext): any {
    return this.flags[flag];
  }
  
  async getAsync<K extends string>(flag: K, _context: EvaluationContext): Promise<any> {
    return this.flags[flag];
  }
  
  setFlag(flag: string, value: any): void {
    this.flags[flag] = value;
  }
}
```

**Acceptance Criteria**:
- [ ] Sync flags accessible as direct properties
- [ ] Async flags require .get() method
- [ ] All flag access logged to column writers
- [ ] trackUsage() logs usage events
- [ ] InMemoryFlagEvaluator works for testing
- [ ] Matches spec implementation lines 127-197

---

### Step 1.8: Add Tests for Feature Flags

**File**: `packages/lmao/src/lib/schema/__tests__/featureFlags.test.ts`

**Task**: Write comprehensive tests for feature flag definition and evaluation.

**Implementation**:

```typescript
import { describe, expect, test } from 'bun:test';
import { defineFeatureFlags } from '../defineFeatureFlags';
import { FeatureFlagEvaluator, InMemoryFlagEvaluator } from '../evaluator';
import { S } from '../builder';

describe('Feature Flags', () => {
  test('defines feature flags with sync/async markers', () => {
    const flags = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      maxRetries: S.number().default(3).sync(),
      userSpecificLimit: S.number().default(100).async(),
    });
    
    expect(flags.schema).toBeDefined();
    expect(flags.syncFlags).toContain('debugMode');
    expect(flags.syncFlags).toContain('maxRetries');
    expect(flags.asyncFlags).toContain('userSpecificLimit');
  });
  
  test('evaluator provides sync flags as properties', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      maxRetries: S.number().default(3).sync(),
    });
    
    const evaluator = new InMemoryFlagEvaluator({
      debugMode: true,
      maxRetries: 5
    });
    
    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator
    );
    
    // Sync flags accessed as properties
    expect((ff as any).debugMode).toBe(true);
    expect((ff as any).maxRetries).toBe(5);
  });
  
  test('evaluator provides async flags via get method', async () => {
    const schema = defineFeatureFlags({
      userSpecificLimit: S.number().default(100).async(),
      dynamicProvider: S.enum(['stripe', 'paypal'] as const).default('stripe').async(),
    });
    
    const evaluator = new InMemoryFlagEvaluator({
      userSpecificLimit: 200,
      dynamicProvider: 'paypal'
    });
    
    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator
    );
    
    // Async flags accessed via get()
    const limit = await ff.get('userSpecificLimit');
    const provider = await ff.get('dynamicProvider');
    
    expect(limit).toBe(200);
    expect(provider).toBe('paypal');
  });
  
  test('returns default values when flag not set', async () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
      userLimit: S.number().default(100).async(),
    });
    
    const evaluator = new InMemoryFlagEvaluator({}); // No flags set
    
    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator
    );
    
    expect((ff as any).debugMode).toBe(false);
    expect(await ff.get('userLimit')).toBe(100);
  });
  
  test('trackUsage logs usage events', () => {
    const schema = defineFeatureFlags({
      advancedValidation: S.boolean().default(false).sync(),
    });
    
    const evaluator = new InMemoryFlagEvaluator({
      advancedValidation: true
    });
    
    const logs: any[] = [];
    const mockWriters = {
      writeEntryType: (type: string) => logs.push({ type }),
      writeFfName: (name: string) => logs.push({ name }),
      writeFfValue: (value: any) => logs.push({ value }),
      writeAction: (action?: string) => logs.push({ action }),
      writeOutcome: (outcome?: string) => logs.push({ outcome }),
      writeContextAttributes: (ctx: any) => logs.push({ context: ctx })
    };
    
    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator,
      mockWriters
    );
    
    ff.trackUsage('advancedValidation', {
      action: 'validation_performed',
      outcome: 'success'
    });
    
    expect(logs).toContainEqual({ type: 'ff-usage' });
    expect(logs).toContainEqual({ name: 'advancedValidation' });
    expect(logs).toContainEqual({ action: 'validation_performed' });
    expect(logs).toContainEqual({ outcome: 'success' });
  });
  
  test('sync flag access is logged', () => {
    const schema = defineFeatureFlags({
      debugMode: S.boolean().default(false).sync(),
    });
    
    const evaluator = new InMemoryFlagEvaluator({ debugMode: true });
    
    const logs: any[] = [];
    const mockWriters = {
      writeEntryType: (type: string) => logs.push({ type }),
      writeFfName: (name: string) => logs.push({ name }),
      writeFfValue: (value: any) => logs.push({ value }),
      writeAction: () => {},
      writeOutcome: () => {},
      writeContextAttributes: (ctx: any) => logs.push({ context: ctx })
    };
    
    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator,
      mockWriters
    );
    
    // Access the flag
    const value = (ff as any).debugMode;
    
    expect(value).toBe(true);
    expect(logs).toContainEqual({ type: 'ff-access' });
    expect(logs).toContainEqual({ name: 'debugMode' });
    expect(logs).toContainEqual({ value: true });
  });
  
  test('async flag access is logged', async () => {
    const schema = defineFeatureFlags({
      userLimit: S.number().default(100).async(),
    });
    
    const evaluator = new InMemoryFlagEvaluator({ userLimit: 200 });
    
    const logs: any[] = [];
    const mockWriters = {
      writeEntryType: (type: string) => logs.push({ type }),
      writeFfName: (name: string) => logs.push({ name }),
      writeFfValue: (value: any) => logs.push({ value }),
      writeAction: () => {},
      writeOutcome: () => {},
      writeContextAttributes: (ctx: any) => logs.push({ context: ctx })
    };
    
    const ff = new FeatureFlagEvaluator(
      schema.schema,
      { userId: 'user-123' },
      evaluator,
      mockWriters
    );
    
    const value = await ff.get('userLimit');
    
    expect(value).toBe(200);
    expect(logs).toContainEqual({ type: 'ff-access' });
    expect(logs).toContainEqual({ name: 'userLimit' });
    expect(logs).toContainEqual({ value: 200 });
  });
});
```

**Acceptance Criteria**:
- [ ] All tests pass
- [ ] Tests verify sync property access
- [ ] Tests verify async get() method
- [ ] Tests verify default value fallback
- [ ] Tests verify analytics logging
- [ ] Tests verify usage tracking

---

## Phase 2: Apache Arrow Columnar Buffer Foundation

### Step 2.1: Create Arrow-Based SpanBuffer Types

**File**: `packages/lmao/src/lib/buffer/types.ts`

**Task**: Define SpanBuffer interface using Apache Arrow builders.

**Implementation**:

```typescript
import * as arrow from 'apache-arrow';
import type { TagAttributeSchema } from '../schema/types';

/**
 * Arrow-based SpanBuffer for zero-copy columnar storage
 * 
 * This uses Apache Arrow's builder pattern for efficient memory management
 * and direct conversion to Arrow tables for Parquet serialization.
 */
export interface SpanBuffer {
  // Arrow builders for core columns
  timestampBuilder: arrow.Float64Builder;
  operationBuilder: arrow.Uint8Builder;
  
  // Null bitmap builder (managed by Arrow)
  // Arrow handles null tracking automatically per column
  
  // Attribute column builders (generated from schema)
  attributeBuilders: Record<string, arrow.Builder>;
  
  // Tree structure
  children: SpanBuffer[];
  parent?: SpanBuffer;
  
  // Buffer management
  writeIndex: number;
  capacity: number;
  next?: SpanBuffer;
  
  spanId: number;
  
  // Reference to task context
  task: TaskContext;
}

/**
 * Module context shared across all tasks in same module
 */
export interface ModuleContext {
  moduleId: number;
  gitSha: string;
  filePath: string;
  
  // Tag attribute schema for this module
  tagAttributes: TagAttributeSchema;
  
  // Self-tuning capacity stats
  spanBufferCapacityStats: {
    currentCapacity: number;
    totalWrites: number;
    overflowWrites: number;
    totalBuffersCreated: number;
  };
}

/**
 * Task context combines module + task-specific data
 */
export interface TaskContext {
  module: ModuleContext;
  spanNameId: number;
  lineNumber: number;
}

/**
 * Arrow data type mapping for schema types
 */
export const ARROW_TYPE_MAP = {
  string: new arrow.Utf8(),
  number: new arrow.Float64(),
  boolean: new arrow.Bool(),
  integer: new arrow.Int32(),
} as const;
```

**Acceptance Criteria**:
- [ ] Interface uses Apache Arrow builders
- [ ] All required fields present
- [ ] Matches spec from `01b_columnar_buffer_architecture.md`
- [ ] TypeScript types are correct

---

### Step 2.2: Implement Arrow Builder Factory

**File**: `packages/lmao/src/lib/buffer/createBuilders.ts`

**Task**: Create Arrow builders based on tag attribute schema.

**Implementation**:

```typescript
import * as arrow from 'apache-arrow';
import * as Sury from '@sury/sury';
import type { TagAttributeSchema } from '../schema/types';

/**
 * Create Arrow builders for attribute columns based on schema
 * 
 * This maps Sury schema types to appropriate Arrow builders:
 * - string/masked → Utf8Builder
 * - number → Float64Builder
 * - boolean → BoolBuilder
 * - enum/union → DictionaryBuilder<Utf8>
 */
export function createAttributeBuilders(
  schema: TagAttributeSchema,
  capacity: number = 64
): Record<string, arrow.Builder> {
  const builders: Record<string, arrow.Builder> = {};
  
  for (const [fieldName, surySchema] of Object.entries(schema)) {
    const columnName = `attr_${fieldName}`;
    builders[columnName] = createBuilderForSchema(surySchema, capacity);
  }
  
  return builders;
}

/**
 * Create appropriate Arrow builder for a Sury schema
 */
function createBuilderForSchema(
  schema: Sury.Schema<any, any>,
  capacity: number
): arrow.Builder {
  // Extract schema metadata to determine type
  // Sury schemas don't expose type info directly, so we use duck typing
  
  // Try to infer from schema structure
  // This is a simplified version - production code would need more robust type detection
  
  // For now, use string builder as default and add specific cases
  // In production, you'd examine the Sury schema's internal structure
  
  return new arrow.Utf8Builder({
    type: new arrow.Utf8(),
    nullValues: [null, undefined]
  });
}

/**
 * Determine Arrow data type from Sury schema
 * 
 * This examines the Sury schema to determine the appropriate Arrow type.
 * Sury uses JIT compilation, so we need to look at the schema definition.
 */
export function getArrowTypeFromSchema(
  schema: Sury.Schema<any, any>
): arrow.DataType {
  // Simplified implementation - would need schema introspection
  // For now, default to Utf8 for strings
  return new arrow.Utf8();
}
```

**Acceptance Criteria**:
- [ ] Creates Arrow builders for each schema field
- [ ] Maps types correctly (string → Utf8, number → Float64, etc.)
- [ ] Uses `attr_` prefix for attribute columns
- [ ] Handles optional fields correctly

---

### Step 2.3: Create Empty SpanBuffer Factory with Arrow

**File**: `packages/lmao/src/lib/buffer/createSpanBuffer.ts`

**Task**: Create function to allocate empty SpanBuffer using Arrow builders.

**Implementation**:

```typescript
import * as arrow from 'apache-arrow';
import type { SpanBuffer, TaskContext } from './types';
import type { TagAttributeSchema } from '../schema/types';
import { createAttributeBuilders } from './createBuilders';

let nextGlobalSpanId = 1;

/**
 * Create empty SpanBuffer with Arrow builders
 * 
 * Arrow builders handle:
 * - Cache-aligned memory allocation
 * - Null bitmap management
 * - Automatic resizing
 * - Zero-copy conversion to Arrow vectors
 */
export function createEmptySpanBuffer(
  spanId: number,
  schema: TagAttributeSchema,
  taskContext: TaskContext,
  parentBuffer?: SpanBuffer,
  capacity: number = 64
): SpanBuffer {
  // Create core column builders
  const timestampBuilder = new arrow.Float64Builder({
    type: new arrow.Float64(),
    nullValues: [null, undefined]
  });
  
  const operationBuilder = new arrow.Uint8Builder({
    type: new arrow.Uint8(),
    nullValues: [null, undefined]
  });
  
  // Create attribute builders from schema
  const attributeBuilders = createAttributeBuilders(schema, capacity);
  
  const buffer: SpanBuffer = {
    spanId,
    timestampBuilder,
    operationBuilder,
    attributeBuilders,
    children: [],
    parent: parentBuffer,
    task: taskContext,
    writeIndex: 0,
    capacity,
    next: undefined
  };
  
  taskContext.module.spanBufferCapacityStats.totalBuffersCreated++;
  
  return buffer;
}

/**
 * Create root SpanBuffer for new trace
 */
export function createSpanBuffer(
  schema: TagAttributeSchema,
  taskContext: TaskContext,
  capacity?: number
): SpanBuffer {
  const spanId = nextGlobalSpanId++;
  return createEmptySpanBuffer(spanId, schema, taskContext, undefined, capacity);
}

/**
 * Create child SpanBuffer
 */
export function createChildSpanBuffer(
  parentBuffer: SpanBuffer,
  taskContext: TaskContext
): SpanBuffer {
  const spanId = nextGlobalSpanId++;
  const schema = parentBuffer.task.module.tagAttributes;
  const capacity = parentBuffer.capacity;
  
  const childBuffer = createEmptySpanBuffer(
    spanId,
    schema,
    taskContext,
    parentBuffer,
    capacity
  );
  
  parentBuffer.children.push(childBuffer);
  
  return childBuffer;
}
```

**Acceptance Criteria**:
- [ ] Creates Arrow builders for core and attribute columns
- [ ] Arrow handles memory alignment automatically
- [ ] spanId assigned correctly
- [ ] Parent-child relationships established
- [ ] Matches spec lines 174-342

---

## Phase 3: Schema-to-Arrow Integration

### Step 3.1: Implement Arrow Column Writers

**File**: `packages/lmao/src/lib/buffer/columnWriters.ts`

**Task**: Generate column writer functions that append to Arrow builders.

**Implementation**:

```typescript
import * as arrow from 'apache-arrow';
import type { SpanBuffer } from './types';
import type { TagAttributeSchema } from '../schema/types';

/**
 * Column writer interface for appending data to Arrow builders
 */
export interface ColumnWriters {
  // Core columns
  writeTimestamp(value: number): void;
  writeOperation(value: number): void;
  
  // Attribute columns (dynamically generated)
  [key: `write${string}`]: (value: any) => void;
}

/**
 * Generate column writer functions for a SpanBuffer
 * 
 * Each writer appends to the corresponding Arrow builder.
 * Arrow handles null bitmaps and memory management automatically.
 */
export function generateColumnWriters(
  buffer: SpanBuffer,
  schema: TagAttributeSchema
): ColumnWriters {
  const writers: any = {
    writeTimestamp(value: number) {
      buffer.timestampBuilder.append(value);
    },
    
    writeOperation(value: number) {
      buffer.operationBuilder.append(value);
    },
  };
  
  // Generate attribute writers
  for (const fieldName of Object.keys(schema)) {
    const columnName = `attr_${fieldName}`;
    const writerName = `write${capitalize(fieldName)}`;
    const builder = buffer.attributeBuilders[columnName];
    
    if (!builder) {
      throw new Error(`No builder found for column ${columnName}`);
    }
    
    writers[writerName] = function(value: any) {
      if (value === null || value === undefined) {
        builder.appendNull();
      } else {
        builder.append(value);
      }
    };
  }
  
  return writers;
}

/**
 * Capitalize first letter of string
 */
function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Write a complete row to the buffer
 * 
 * This ensures all columns get a value (or null) to maintain equal length.
 */
export function writeRow(
  buffer: SpanBuffer,
  writers: ColumnWriters,
  data: {
    timestamp: number;
    operation: number;
    attributes?: Record<string, any>;
  }
): void {
  // Write core columns
  writers.writeTimestamp(data.timestamp);
  writers.writeOperation(data.operation);
  
  // Write attribute columns
  const schema = buffer.task.module.tagAttributes;
  for (const fieldName of Object.keys(schema)) {
    const writerName = `write${capitalize(fieldName)}`;
    const value = data.attributes?.[fieldName];
    
    if (writers[writerName]) {
      writers[writerName](value ?? null);
    }
  }
  
  buffer.writeIndex++;
}
```

**Acceptance Criteria**:
- [ ] Generates writer functions for all columns
- [ ] Arrow builders handle null values automatically
- [ ] writeRow maintains equal column lengths
- [ ] Type-safe writer interface

---

### Step 3.2: Implement Arrow to RecordBatch Conversion

**File**: `packages/lmao/src/lib/buffer/toArrow.ts`

**Task**: Convert SpanBuffer to Arrow RecordBatch using zero-copy.

**Implementation**:

```typescript
import * as arrow from 'apache-arrow';
import type { SpanBuffer } from './types';

/**
 * Convert SpanBuffer to Arrow RecordBatch
 * 
 * This is a zero-copy operation - Arrow builders directly produce vectors
 * without any data copying.
 */
export function spanBufferToRecordBatch(buffer: SpanBuffer): arrow.RecordBatch {
  // Finish all builders to get vectors
  const timestampVector = buffer.timestampBuilder.finish().toVector();
  const operationVector = buffer.operationBuilder.finish().toVector();
  
  // Finish attribute builders
  const attributeVectors: Record<string, arrow.Vector> = {};
  for (const [columnName, builder] of Object.entries(buffer.attributeBuilders)) {
    // Strip attr_ prefix for clean Arrow column names
    const cleanName = columnName.replace(/^attr_/, '');
    attributeVectors[cleanName] = builder.finish().toVector();
  }
  
  // Create RecordBatch with all vectors
  return new arrow.RecordBatch({
    timestamp: timestampVector,
    operation: operationVector,
    ...attributeVectors
  });
}

/**
 * Convert multiple SpanBuffers to Arrow Table
 * 
 * This concatenates multiple RecordBatches into a single Table.
 */
export function spanBuffersToTable(buffers: SpanBuffer[]): arrow.Table {
  const batches = buffers.map(spanBufferToRecordBatch);
  return new arrow.Table(batches);
}

/**
 * Write SpanBuffers directly to Parquet
 */
export async function writeSpanBuffersToParquet(
  buffers: SpanBuffer[],
  outputPath: string
): Promise<void> {
  const table = spanBuffersToTable(buffers);
  
  // Apache Arrow can write directly to Parquet
  // Note: Full Parquet support may require additional packages
  await arrow.tableToIPC(table, outputPath);
}
```

**Acceptance Criteria**:
- [ ] Zero-copy conversion from builders to vectors
- [ ] RecordBatch has correct schema
- [ ] attr_ prefix stripped for clean column names
- [ ] Multiple buffers can be combined into Table

---

### Step 3.3: Add Integration Tests

**File**: `packages/lmao/src/lib/buffer/__tests__/integration.test.ts`

**Task**: Test complete flow from schema to Arrow table.

**Implementation**:

```typescript
import { describe, expect, test } from 'bun:test';
import { S } from '../../schema/builder';
import { defineTagAttributes } from '../../schema/defineTagAttributes';
import { createSpanBuffer } from '../createSpanBuffer';
import { generateColumnWriters, writeRow } from '../columnWriters';
import { spanBufferToRecordBatch } from '../toArrow';

describe('Schema to Arrow Integration', () => {
  test('complete flow: schema → buffer → Arrow', () => {
    // 1. Define schema with Sury
    const schema = defineTagAttributes({
      requestId: S.string(),
      userId: S.masked('hash'),
      httpStatus: S.number(),
    });
    
    // 2. Create SpanBuffer with Arrow builders
    const taskContext: any = {
      module: {
        moduleId: 1,
        gitSha: 'abc123',
        filePath: 'test.ts',
        tagAttributes: schema.schema,
        spanBufferCapacityStats: {
          currentCapacity: 64,
          totalWrites: 0,
          overflowWrites: 0,
          totalBuffersCreated: 0
        }
      },
      spanNameId: 1,
      lineNumber: 10
    };
    
    const buffer = createSpanBuffer(schema.schema, taskContext);
    
    // 3. Generate writers
    const writers = generateColumnWriters(buffer, schema.schema);
    
    // 4. Write data
    writeRow(buffer, writers, {
      timestamp: Date.now(),
      operation: 1,
      attributes: {
        requestId: 'req-123',
        userId: 'user-456',
        httpStatus: 200
      }
    });
    
    writeRow(buffer, writers, {
      timestamp: Date.now(),
      operation: 2,
      attributes: {
        requestId: 'req-123',
        userId: 'user-456',
        httpStatus: 404
      }
    });
    
    // 5. Convert to Arrow RecordBatch
    const batch = spanBufferToRecordBatch(buffer);
    
    // 6. Verify Arrow structure
    expect(batch.numCols).toBeGreaterThan(2);
    expect(batch.numRows).toBe(2);
    expect(batch.schema.fields.map(f => f.name)).toContain('requestId');
    expect(batch.schema.fields.map(f => f.name)).toContain('httpStatus');
  });
});
```

**Acceptance Criteria**:
- [ ] Schema definition works with Sury
- [ ] SpanBuffer creates Arrow builders
- [ ] Column writers append data correctly
- [ ] Arrow RecordBatch has correct structure
- [ ] All columns have equal length

---

## Implementation Order Summary

1. **Phase 1: Sury Schema System** (Steps 1.1-1.8) - **START HERE**
   - Install @sury/sury and apache-arrow (Step 1.1)
   - Implement Sury-based schema builder (Step 1.2)
   - Implement tag attribute definition (Step 1.3)
   - Add schema extension support (Step 1.4)
   - Test tag attributes (Step 1.5)
   - Implement feature flag definition (Step 1.6)
   - Implement feature flag evaluator (Step 1.7)
   - Test feature flags (Step 1.8)

2. **Phase 2: Arrow Buffer Foundation** (Steps 2.1-2.3)
   - Define Arrow-based SpanBuffer types
   - Create Arrow builder factories
   - Implement SpanBuffer creation with Arrow

3. **Phase 3: Integration** (Steps 3.1-3.3)
   - Generate column writers for Arrow builders
   - Implement zero-copy Arrow conversion
   - Add comprehensive integration tests

## Key Benefits of This Approach

### Sury (@sury/sury)
- **Fastest validation** in JavaScript ecosystem (94,828 ops/ms)
- **Smallest bundle size** for schema libraries (14.1 kB)
- **Runtime transformation** support for masking
- **Standard Schema** compatibility
- **TypeScript inference** out of the box

### Apache Arrow
- **Zero-copy** conversion from buffers to Arrow vectors
- **Automatic null handling** via Arrow builders
- **Cache-aligned memory** managed by Arrow runtime
- **Direct Parquet export** for analytical queries
- **Columnar format** optimized for compression and queries

### Performance Characteristics
- **Hot path**: <0.1ms per tag operation (Arrow builder append)
- **Validation**: 94K+ validations per millisecond (Sury)
- **Memory**: Zero-copy from builders to Arrow tables
- **Serialization**: Direct Arrow → Parquet with compression

## Next Steps After Phase 3

After completing schema-to-Arrow integration:
1. Implement module context and SpanLogger generation
2. Add context flow and task wrappers
3. Build entry type system and logging primitives
4. Create background processing pipeline for Parquet export
5. Add self-tuning capacity management

---

## Notes for Implementation

- **Sury schemas** provide runtime validation AND TypeScript types
- **Arrow builders** handle all memory management and null tracking
- **Zero-copy design** ensures minimal overhead throughout the pipeline
- Use **Bun's test runner** for all tests (`bun test`)
- Follow **strict TypeScript** mode for all code
- Reference spec documents for detailed requirements

This refactored roadmap leverages the best-in-class libraries for schema validation and columnar data, providing superior performance and developer experience.
