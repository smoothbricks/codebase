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
import type { SpanBuffer, ModuleContext, TaskContext } from './buffer/types.js';
import { createSpanBuffer, createChildSpanBuffer } from './buffer/createSpanBuffer.js';

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
      // Write to attr_ffName column if it exists
      // For string types (category/text), we store as string in TypedArray (will be interned later)
      const column = buffer['attr_ffName' as keyof SpanBuffer];
      if (column && ArrayBuffer.isView(column)) {
        // For now, store string index (TODO: implement string interning)
        // Placeholder: store 0 for now, will implement string table later
        (column as Uint32Array)[buffer.writeIndex] = 0;
      }
    },
    
    writeFfValue(value: string | number | boolean | null): void {
      // Write to attr_ffValue column
      // For mixed types, serialize to string and store index
      const column = buffer['attr_ffValue' as keyof SpanBuffer];
      if (column && ArrayBuffer.isView(column)) {
        // Placeholder: store 0 for now, will implement string table later
        (column as Uint32Array)[buffer.writeIndex] = 0;
      }
    },
    
    writeAction(action?: string): void {
      // Write to attr_action column
      const column = buffer['attr_action' as keyof SpanBuffer];
      if (column && ArrayBuffer.isView(column)) {
        // Placeholder: store 0 for now, will implement string table later
        (column as Uint32Array)[buffer.writeIndex] = 0;
      }
    },
    
    writeOutcome(outcome?: string): void {
      // Write to attr_outcome column
      const column = buffer['attr_outcome' as keyof SpanBuffer];
      if (column && ArrayBuffer.isView(column)) {
        // Placeholder: store 0 for now, will implement string table later
        (column as Uint32Array)[buffer.writeIndex] = 0;
      }
    },
    
    writeContextAttributes(context: EvaluationContext): void {
      // Write context attributes to their respective columns
      if (context.userId) {
        const column = buffer['attr_contextUserId' as keyof SpanBuffer];
        if (column && ArrayBuffer.isView(column)) {
          (column as Uint32Array)[buffer.writeIndex] = 0; // Placeholder
        }
      }
      if (context.requestId) {
        const column = buffer['attr_contextRequestId' as keyof SpanBuffer];
        if (column && ArrayBuffer.isView(column)) {
          (column as Uint32Array)[buffer.writeIndex] = 0; // Placeholder
        }
      }
      // Additional context fields can be written similarly
      
      // Increment write index after all writes
      buffer.writeIndex++;
    },
  };
}

/**
 * Write a value to a TypedArray column
 * Handles type conversion for different column types
 */
function writeToColumn(buffer: SpanBuffer, columnName: string, value: unknown, index: number): void {
  const column = buffer[columnName as keyof SpanBuffer];
  
  if (!column || !ArrayBuffer.isView(column)) return;
  
  // Handle null/undefined - store 0 and set null bitmap
  if (value === null || value === undefined) {
    if (column instanceof Uint8Array) {
      column[index] = 0;
    } else if (column instanceof Uint32Array) {
      column[index] = 0;
    } else if (column instanceof Float64Array) {
      column[index] = 0;
    }
    // Set null bit in bitmap (TODO: implement null bitmap)
    return;
  }
  
  // Write based on column type
  if (column instanceof Uint8Array) {
    // For boolean or enum types
    if (typeof value === 'boolean') {
      column[index] = value ? 1 : 0;
    } else if (typeof value === 'number') {
      column[index] = value;
    } else {
      // For string enums, need compile-time mapping (TODO)
      column[index] = 0;
    }
  } else if (column instanceof Uint32Array) {
    // For category/text types - store string index
    // TODO: implement string interning for categories
    column[index] = 0; // Placeholder
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
  
  // Create module context (stub for now - will integrate with buffer system)
  const moduleContext: ModuleContext = {
    moduleId: Math.floor(Math.random() * 1000000), // Stub
    gitSha: moduleMetadata.gitSha,
    filePath: moduleMetadata.filePath,
    tagAttributes: schemaOnly,
    spanBufferCapacityStats: {
      currentCapacity: 1024,
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
        // Create task context
        const taskContext: TaskContext = {
          module: moduleContext,
          spanNameId: Math.floor(Math.random() * 1000000), // TODO: Use string table for span names
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
