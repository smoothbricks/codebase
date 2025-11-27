/**
 * Main LMAO integration - Context creation and task wrapper system
 * 
 * This module ties together:
 * - Feature flags with automatic analytics
 * - Environment configuration 
 * - Tag attributes with columnar storage
 * - Task wrappers with span buffers
 */

import type { TagAttributeSchema, InferTagAttributes } from './schema/types.js';
import type { FeatureFlagSchema, InferFeatureFlags, EvaluationContext } from './schema/defineFeatureFlags.js';
import { FeatureFlagEvaluator, type FlagEvaluator, type FlagColumnWriters } from './schema/evaluator.js';
import type { SpanBuffer, ModuleContext, TaskContext } from '@smoothbricks/arrow-builder';
import { createSpanBuffer, createChildSpanBuffer } from '@smoothbricks/arrow-builder';

/**
 * Result types for ok/err pattern
 */
export type SuccessResult<V> = { success: true; value: V };
export type ErrorResult<E> = { success: false; error: { code: string; details: E } };
export type Result<V, E = unknown> = SuccessResult<V> | ErrorResult<E>;

/**
 * Generate unique trace ID
 */
function generateTraceId(): string {
  // Simple implementation - can be replaced with more sophisticated ID generation
  return `trace-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * String interning for category columns
 * 
 * Per specs/01b1_buffer_performance_optimizations.md:
 * - Store strings once, reference by index
 * - Fast integer comparison vs string comparison
 * - Direct Arrow dictionary creation
 * - Cache-friendly integer storage
 */
class StringInterner {
  private strings: string[] = [];
  private indices = new Map<string, number>();
  
  /**
   * Intern a string and return its index
   * O(1) lookup via Map, O(1) insertion
   */
  intern(str: string): number {
    let idx = this.indices.get(str);
    
    if (idx === undefined) {
      idx = this.strings.length;
      this.strings.push(str);
      this.indices.set(str, idx);
    }
    
    return idx;
  }
  
  /**
   * Get string by index
   * Used during Arrow conversion
   */
  getString(idx: number): string | undefined {
    return this.strings[idx];
  }
  
  /**
   * Get all strings for Arrow dictionary
   */
  getStrings(): readonly string[] {
    return this.strings;
  }
  
  /**
   * Get count of unique strings
   */
  size(): number {
    return this.strings.length;
  }
}

/**
 * Global string interners
 * One per string type to keep dictionaries separate
 */
const categoryInterner = new StringInterner();
const moduleIdInterner = new StringInterner();
const spanNameInterner = new StringInterner();

/**
 * Request context created at request boundary
 * Contains trace ID, feature flags, and environment config
 */
export interface RequestContext<
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = Record<string, unknown>
> {
  requestId: string;
  userId?: string;
  traceId: string;
  
  // Feature flag evaluator (buffer reference set later by task wrapper)
  ff: FeatureFlagEvaluator<FF> & InferFeatureFlags<FF>;
  
  // Environment config (just plain object, no tracking)
  env: Env;
  
  // Additional context fields
  [key: string]: unknown;
}

/**
 * Create request context with feature flags and environment
 * 
 * @param params - Request parameters (requestId, userId, etc.)
 * @param featureFlagSchema - Feature flag schema object
 * @param evaluator - Feature flag evaluator backend
 * @param environmentConfig - Environment configuration object
 * @returns Request context with ff and env
 */
export function createRequestContext<
  FF extends FeatureFlagSchema,
  Env extends Record<string, unknown>
>(
  params: {
    requestId: string;
    userId?: string;
    [key: string]: unknown;
  },
  featureFlagSchema: { schema: FF },
  evaluator: FlagEvaluator,
  environmentConfig: Env
): RequestContext<FF, Env> {
  const evaluationContext: EvaluationContext = {
    userId: params.userId,
    requestId: params.requestId,
  };
  
  // Create feature flag evaluator (buffer will be set by task wrapper)
  const ffEvaluator = new FeatureFlagEvaluator(
    featureFlagSchema.schema,
    evaluationContext,
    evaluator,
    undefined // Column writers set later when span context is created
  ) as FeatureFlagEvaluator<FF> & InferFeatureFlags<FF>;
  
  return {
    ...params,
    traceId: generateTraceId(),
    ff: ffEvaluator,
    env: environmentConfig,
  };
}

/**
 * Chainable tag API type
 * Each method returns the tag object for chaining
 */
export type ChainableTagAPI<T extends TagAttributeSchema> = {
  /**
   * Set multiple attributes at once (chainable)
   * Example: ctx.log.tag.with({ userId: 'u1', requestId: 'r1' }).operation('INSERT')
   */
  with(attributes: Partial<InferTagAttributes<T>>): ChainableTagAPI<T>;
} & {
  /**
   * Set individual attributes (chainable)
   * Example: ctx.log.tag.userId('u1').requestId('r1').operation('INSERT')
   */
  [K in keyof InferTagAttributes<T>]: (value: InferTagAttributes<T>[K]) => ChainableTagAPI<T>;
};

/**
 * Span logger context - provides logging API for spans
 * This is what's available as ctx.log in task wrappers
 */
export interface SpanLogger<T extends TagAttributeSchema> {
  /**
   * Tag attribute API with method chaining support
   * Each method returns the tag object for chaining:
   * - ctx.log.tag.userId(value) - returns tag for chaining
   * - ctx.log.tag.userId(value).requestId(value2) - chain multiple calls
   * - ctx.log.tag.with({ userId, requestId }) - bulk set (chainable)
   */
  tag: ChainableTagAPI<T>;
  
  /**
   * Log a message entry
   */
  message(level: 'info' | 'debug' | 'warn' | 'error', message: string): void;
  
  /**
   * Convenience methods for message logging
   */
  info(message: string): void;
  debug(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

/**
 * Span context provided to task functions
 * Contains logging API, feature flags, environment, and span operations
 */
export interface SpanContext<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>
> {
  // Logging API
  log: SpanLogger<T>;
  
  // Feature flags (with buffer reference set)
  ff: FeatureFlagEvaluator<FF> & InferFeatureFlags<FF>;
  
  // Environment config
  env: Env;
  
  // Result helpers
  ok<V>(value: V): SuccessResult<V>;
  err<E>(code: string, error: E): ErrorResult<E>;
  
  // Child span creation
  // The span can return any type R, and TypeScript will infer it from the child function
  span<R>(name: string, fn: (ctx: SpanContext<T, FF, Env>) => Promise<R>): Promise<R>;
  
  // Additional request context fields
  [key: string]: unknown;
}

/**
 * Task function signature
 */
export type TaskFunction<
  Args extends unknown[],
  Result,
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>
> = (ctx: SpanContext<T, FF, Env>, ...args: Args) => Promise<Result>;

/**
 * Module context builder result
 * Provides task wrapper function
 */
export interface ModuleContextBuilder<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>
> {
  /**
   * Create a task wrapper with span tracking
   */
  task<Args extends unknown[], Result>(
    name: string,
    fn: TaskFunction<Args, Result, T, FF, Env>
  ): (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result>;
}

/**
 * Entry type codes for operation tracking
 */
const ENTRY_TYPE_FF_ACCESS = 1;
const ENTRY_TYPE_FF_USAGE = 2;
const ENTRY_TYPE_TAG = 3;
const ENTRY_TYPE_MESSAGE = 4;

/**
 * Create column writers for feature flag analytics
 * Writes to TypedArray columnar buffers in memory (hot path)
 * 
 * Per specs/01b1_buffer_performance_optimizations.md:
 * - String interning for category columns
 * - Direct TypedArray writes (no allocations)
 */
function createFlagColumnWriters(buffer: SpanBuffer): FlagColumnWriters {
  return {
    writeEntryType(type: 'ff-access' | 'ff-usage'): void {
      // Write entry type code to operation column
      const typeCode = type === 'ff-access' ? ENTRY_TYPE_FF_ACCESS : ENTRY_TYPE_FF_USAGE;
      buffer.operations[buffer.writeIndex] = typeCode;
      
      // Write timestamp
      buffer.timestamps[buffer.writeIndex] = Date.now();
    },
    
    writeFfName(name: string): void {
      // Write to attr_ffName column with string interning
      const column = buffer['attr_ffName' as keyof SpanBuffer];
      if (column && column instanceof Uint32Array) {
        column[buffer.writeIndex] = categoryInterner.intern(name);
      }
    },
    
    writeFfValue(value: string | number | boolean | null): void {
      // Write to attr_ffValue column
      // For mixed types, serialize to string and intern
      const column = buffer['attr_ffValue' as keyof SpanBuffer];
      if (column && column instanceof Uint32Array) {
        const strValue = value === null ? 'null' : String(value);
        column[buffer.writeIndex] = categoryInterner.intern(strValue);
      }
    },
    
    writeAction(action?: string): void {
      // Write to attr_action column
      const column = buffer['attr_action' as keyof SpanBuffer];
      if (column && column instanceof Uint32Array) {
        const idx = buffer.writeIndex;
        if (action) {
          column[idx] = categoryInterner.intern(action);
          // Mark as non-null in bitmap
          const nullBitmap = buffer.nullBitmaps['attr_action'];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= (1 << bitOffset);
          }
        }
      }
    },
    
    writeOutcome(outcome?: string): void {
      // Write to attr_outcome column
      const column = buffer['attr_outcome' as keyof SpanBuffer];
      if (column && column instanceof Uint32Array) {
        const idx = buffer.writeIndex;
        if (outcome) {
          column[idx] = categoryInterner.intern(outcome);
          // Mark as non-null in bitmap
          const nullBitmap = buffer.nullBitmaps['attr_outcome'];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= (1 << bitOffset);
          }
        }
      }
    },
    
    writeContextAttributes(context: EvaluationContext): void {
      const idx = buffer.writeIndex;
      
      // Write context attributes to their respective columns with string interning
      if (context.userId) {
        const column = buffer['attr_contextUserId' as keyof SpanBuffer];
        if (column && column instanceof Uint32Array) {
          column[idx] = categoryInterner.intern(context.userId);
          // Mark as non-null in bitmap
          const nullBitmap = buffer.nullBitmaps['attr_contextUserId'];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= (1 << bitOffset);
          }
        }
      }
      
      if (context.requestId) {
        const column = buffer['attr_contextRequestId' as keyof SpanBuffer];
        if (column && column instanceof Uint32Array) {
          column[idx] = categoryInterner.intern(context.requestId);
          // Mark as non-null in bitmap
          const nullBitmap = buffer.nullBitmaps['attr_contextRequestId'];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= (1 << bitOffset);
          }
        }
      }
      
      // Increment write index after all writes
      buffer.writeIndex++;
    },
  };
}

/**
 * Write a value to a TypedArray column
 * Handles type conversion for different column types
 * 
 * Per specs/01b1_buffer_performance_optimizations.md:
 * - String interning for category types
 * - Null bitmap management per Arrow spec
 * - Direct TypedArray writes (hot path)
 */
function writeToColumn(buffer: SpanBuffer, columnName: string, value: unknown, index: number): void {
  const column = buffer[columnName as keyof SpanBuffer];
  
  if (!column || !ArrayBuffer.isView(column)) return;
  
  // Get null bitmap for this column (Arrow format: 1 Uint8Array per column)
  const nullBitmap = buffer.nullBitmaps[columnName as `attr_${string}`];
  
  // Handle null/undefined - store 0 and set null bitmap
  if (value === null || value === undefined) {
    if (column instanceof Uint8Array) {
      column[index] = 0;
    } else if (column instanceof Uint16Array) {
      column[index] = 0;
    } else if (column instanceof Uint32Array) {
      column[index] = 0;
    } else if (column instanceof Float64Array) {
      column[index] = 0;
    }
    
    // Set null bit in bitmap (Arrow format: 1 bit per row, 0 = null, 1 = valid)
    // We store 0 for null, so no need to set the bit (defaults to 0)
    if (nullBitmap) {
      const byteIndex = Math.floor(index / 8);
      const bitOffset = index % 8;
      // Clear the bit (0 = null in Arrow format)
      nullBitmap[byteIndex] &= ~(1 << bitOffset);
    }
    return;
  }
  
  // Mark as non-null in bitmap (1 = valid in Arrow format)
  if (nullBitmap) {
    const byteIndex = Math.floor(index / 8);
    const bitOffset = index % 8;
    nullBitmap[byteIndex] |= (1 << bitOffset);
  }
  
  // Write based on column type
  if (column instanceof Uint8Array) {
    // For boolean or small enum types
    if (typeof value === 'boolean') {
      column[index] = value ? 1 : 0;
    } else if (typeof value === 'number') {
      column[index] = value;
    } else if (typeof value === 'string') {
      // For string enums, try to find enum value index
      // This requires schema metadata to be available
      // For now, store 0 (will be improved with compile-time mapping)
      column[index] = 0;
    }
  } else if (column instanceof Uint16Array) {
    // For medium enum types (256-65535 values)
    if (typeof value === 'number') {
      column[index] = value;
    }
  } else if (column instanceof Uint32Array) {
    // For category/text types - store string index via interning
    if (typeof value === 'string') {
      column[index] = categoryInterner.intern(value);
    } else if (typeof value === 'number') {
      column[index] = value;
    }
  } else if (column instanceof Float64Array) {
    // For number types
    column[index] = typeof value === 'number' ? value : 0;
  }
}

/**
 * Create span logger with typed tag methods and method chaining
 * Writes to TypedArray columnar buffers in memory (hot path)
 */
function createSpanLogger<T extends TagAttributeSchema>(
  schema: T,
  buffer: SpanBuffer
): SpanLogger<T> {
  // Create the chainable tag API
  // Each method writes to the appropriate TypedArray column and returns itself for chaining
  const createChainableTag = (): ChainableTagAPI<T> => {
    // Create a record to hold the methods, starting with the 'with' method
    type TagMethod = (value: InferTagAttributes<T>[keyof T]) => ChainableTagAPI<T>;
    type TagAPIRecord = Record<string, TagMethod | ((attributes: Partial<InferTagAttributes<T>>) => ChainableTagAPI<T>)>;
    
    const tagAPI = {} as TagAPIRecord;
    
    // Add the 'with' method for bulk setting
    tagAPI.with = function(attributes: Partial<InferTagAttributes<T>>): ChainableTagAPI<T> {
      const idx = buffer.writeIndex;
      
      // Write entry type for tag entry
      buffer.operations[idx] = ENTRY_TYPE_TAG;
      
      // Write timestamp
      buffer.timestamps[idx] = Date.now();
      
      // Write each attribute to its column
      for (const [key, value] of Object.entries(attributes)) {
        const columnName = `attr_${key}`;
        writeToColumn(buffer, columnName, value, idx);
      }
      
      // Increment write index
      buffer.writeIndex++;
      
      return tagAPI as ChainableTagAPI<T>;
    };
    
    // Add individual attribute methods dynamically
    for (const key of Object.keys(schema)) {
      tagAPI[key] = function(value: unknown): ChainableTagAPI<T> {
        const idx = buffer.writeIndex;
        
        // Write entry type for tag entry
        buffer.operations[idx] = ENTRY_TYPE_TAG;
        
        // Write timestamp
        buffer.timestamps[idx] = Date.now();
        
        // Write the attribute value to its column
        const columnName = `attr_${key}`;
        writeToColumn(buffer, columnName, value, idx);
        
        // Increment write index
        buffer.writeIndex++;
        
        return tagAPI as ChainableTagAPI<T>;
      };
    }
    
    return tagAPI as ChainableTagAPI<T>;
  };
  
  const tag = createChainableTag();
  
  return {
    tag,
    message(level: 'info' | 'debug' | 'warn' | 'error', message: string): void {
      const idx = buffer.writeIndex;
      
      // Write entry type for message
      buffer.operations[idx] = ENTRY_TYPE_MESSAGE;
      
      // Write timestamp
      buffer.timestamps[idx] = Date.now();
      
      // Write message level and content to attribute columns
      writeToColumn(buffer, 'attr_logLevel', level, idx);
      writeToColumn(buffer, 'attr_logMessage', message, idx);
      
      // Increment write index
      buffer.writeIndex++;
    },
    info(message: string): void {
      this.message('info', message);
    },
    debug(message: string): void {
      this.message('debug', message);
    },
    warn(message: string): void {
      this.message('warn', message);
    },
    error(message: string): void {
      this.message('error', message);
    },
  };
}

/**
 * Extract just the schema fields from an object, removing methods
 * This allows us to accept objects with additional methods like validate, extend, etc.
 * 
 * This type recursively picks all properties that are not functions from intersections
 */
type ExtractSchemaFields<T> = T extends infer U
  ? {
      [K in keyof U as U[K] extends Function ? never : K]: U[K];
    }
  : never;

/**
 * Type predicate to check if extracted fields match TagAttributeSchema
 */
type IsValidTagSchema<T> = ExtractSchemaFields<T> extends TagAttributeSchema
  ? ExtractSchemaFields<T>
  : TagAttributeSchema;

/**
 * Create module context with tag attributes
 * 
 * This creates a task wrapper that:
 * - Creates span buffers for each task execution
 * - Connects feature flag evaluator to buffer for analytics
 * - Provides typed logging API based on tag attributes
 * 
 * @param options - Module metadata and tag attributes
 * @returns Module context builder with task wrapper
 */
export function createModuleContext<
  TInput,
  T extends TagAttributeSchema = IsValidTagSchema<TInput>,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = Record<string, unknown>
>(options: {
  moduleMetadata: {
    gitSha: string;
    filePath: string;
    moduleName: string;
  };
  tagAttributes: TInput;
}): ModuleContextBuilder<T, FF, Env> {
  const { moduleMetadata, tagAttributes } = options;
  
  // Extract only the schema fields, removing methods like validate, extend, etc.
  const schemaOnly = Object.keys(tagAttributes as Record<string, unknown>).reduce((acc, key) => {
    const value = (tagAttributes as Record<string, unknown>)[key];
    // Only include non-function properties
    if (typeof value !== 'function') {
      acc[key] = value;
    }
    return acc;
  }, {} as Record<string, unknown>) as T;
  
  // Create module context with string-interned module ID
  // Module ID is the file path, interned for efficient storage
  const moduleContext: ModuleContext = {
    moduleId: moduleIdInterner.intern(moduleMetadata.filePath),
    gitSha: moduleMetadata.gitSha,
    filePath: moduleMetadata.filePath,
    tagAttributes: schemaOnly,
    spanBufferCapacityStats: {
      currentCapacity: 64, // Start with cache-friendly size (see specs/01b_columnar_buffer_architecture.md)
      totalWrites: 0,
      overflowWrites: 0,
      totalBuffersCreated: 0,
    },
  };
  
  return {
    task<Args extends unknown[], Result>(
      name: string,
      fn: TaskFunction<Args, Result, T, FF, Env>
    ): (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result> {
      return async (requestCtx: RequestContext<FF, Env>, ...args: Args): Promise<Result> => {
        // Create task context with string-interned span name
        const taskContext: TaskContext = {
          module: moduleContext,
          spanNameId: spanNameInterner.intern(name),
          lineNumber: 0, // Would be set by code generation
        };
        
        // Create span buffer with Arrow builders
        const spanBuffer = createSpanBuffer(schemaOnly, taskContext);
        
        // Connect feature flag evaluator to buffer for analytics
        // The evaluator is a FeatureFlagEvaluator instance, we need to set columnWriters
        if (requestCtx.ff instanceof FeatureFlagEvaluator) {
          (requestCtx.ff as FeatureFlagEvaluator<FF>)['columnWriters'] = createFlagColumnWriters(spanBuffer);
        }
        
        // Create span logger with typed tag methods
        const spanLogger = createSpanLogger(schemaOnly, spanBuffer);
        
        // Create span context
        const spanContext: SpanContext<T, FF, Env> = {
          ...requestCtx,
          log: spanLogger,
          
          ok<V>(value: V): SuccessResult<V> {
            return { success: true, value };
          },
          
          err<E>(code: string, error: E): ErrorResult<E> {
            return { success: false, error: { code, details: error } };
          },
          
          async span<R>(
            childName: string,
            childFn: (ctx: SpanContext<T, FF, Env>) => Promise<R>
          ): Promise<R> {
            // Create child span buffer with Arrow builders
            const childBuffer = createChildSpanBuffer(spanBuffer, taskContext);
            
            // Create child context with its own logger
            const childLogger = createSpanLogger(schemaOnly, childBuffer);
            const childContext: SpanContext<T, FF, Env> = {
              ...spanContext,
              log: childLogger,
            };
            
            return childFn(childContext);
          },
        };
        
        // Execute task function
        return fn(spanContext, ...args);
      };
    },
  };
}
