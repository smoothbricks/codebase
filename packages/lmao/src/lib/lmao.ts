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
import type { SpanBuffer, ModuleContext, TaskContext, BufferCapacityStats } from '@smoothbricks/arrow-builder';
import { createSpanBuffer, createChildSpanBuffer, createNextBuffer } from '@smoothbricks/arrow-builder';
import { createSpanLoggerClass, type BaseSpanLogger } from './codegen/spanLoggerGenerator.js';

/**
 * Result types for ok/err pattern
 */
export type SuccessResult<V> = { success: true; value: V };
export type ErrorResult<E> = { success: false; error: { code: string; details: E } };
export type Result<V, E = unknown> = SuccessResult<V> | ErrorResult<E>;

/**
 * Fluent result builder for ctx.ok()/ctx.err()
 * Allows chaining attributes and message before returning result
 * 
 * Per specs/01h_entry_types_and_logging_primitives.md:
 * - Writes span-ok or span-err entry to buffer
 * - Supports .with() for attributes and .message() for text
 * - Returns final result after writing to buffer
 * 
 * This class extends the base Result type to support method chaining
 * while maintaining proper TypeScript type narrowing.
 */
class FluentSuccessResult<V, T extends TagAttributeSchema> implements SuccessResult<V> {
  readonly success = true as const;
  readonly value: V;
  private buffer: SpanBuffer;
  private entryIndex: number;
  
  constructor(
    buffer: SpanBuffer,
    value: V,
    _schema: T // Needed for generic type inference
  ) {
    this.value = value;
    
    // Find buffer with space and create entry
    const { buffer: bufferWithSpace } = getBufferWithSpace(buffer);
    this.buffer = bufferWithSpace;
    
    this.entryIndex = this.buffer.writeIndex;
    
    // Write entry type (span-ok)
    this.buffer.operations[this.entryIndex] = ENTRY_TYPE_SPAN_OK;
    
    // Write timestamp
    this.buffer.timestamps[this.entryIndex] = Date.now();
    
    // Increment write index
    this.buffer.writeIndex++;
  }
  
  /**
   * Set multiple attributes on the result entry
   * Example: ctx.ok(result).with({ userId: 'u1', operation: 'CREATE' })
   */
  with(attributes: Partial<InferTagAttributes<T>>): this {
    // Write each attribute to its column
    for (const [key, value] of Object.entries(attributes)) {
      const columnName = `attr_${key}`;
      writeToColumn(this.buffer, columnName, value, this.entryIndex);
    }
    return this;
  }
  
  /**
   * Set a message on the result entry
   * Example: ctx.ok(result).message('User created successfully')
   */
  message(text: string): this {
    writeToColumn(this.buffer, 'attr_resultMessage', text, this.entryIndex);
    return this;
  }
}

/**
 * Fluent error result with chaining support
 */
class FluentErrorResult<E, T extends TagAttributeSchema> implements ErrorResult<E> {
  readonly success = false as const;
  readonly error: { code: string; details: E };
  private buffer: SpanBuffer;
  private entryIndex: number;
  
  constructor(
    buffer: SpanBuffer,
    code: string,
    details: E,
    _schema: T // Needed for generic type inference
  ) {
    this.error = { code, details };
    
    // Find buffer with space and create entry
    const { buffer: bufferWithSpace } = getBufferWithSpace(buffer);
    this.buffer = bufferWithSpace;
    
    this.entryIndex = this.buffer.writeIndex;
    
    // Write entry type (span-err)
    this.buffer.operations[this.entryIndex] = ENTRY_TYPE_SPAN_ERR;
    
    // Write timestamp
    this.buffer.timestamps[this.entryIndex] = Date.now();
    
    // Write error code
    writeToColumn(this.buffer, 'attr_errorCode', code, this.entryIndex);
    
    // Increment write index
    this.buffer.writeIndex++;
  }
  
  /**
   * Set multiple attributes on the result entry
   * Example: ctx.err('ERROR', details).with({ userId: 'u1' })
   */
  with(attributes: Partial<InferTagAttributes<T>>): this {
    // Write each attribute to its column
    for (const [key, value] of Object.entries(attributes)) {
      const columnName = `attr_${key}`;
      writeToColumn(this.buffer, columnName, value, this.entryIndex);
    }
    return this;
  }
  
  /**
   * Set a message on the result entry
   * Example: ctx.err('ERROR', details).message('Operation failed')
   */
  message(text: string): this {
    writeToColumn(this.buffer, 'attr_resultMessage', text, this.entryIndex);
    return this;
  }
}

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
 * 
 * Exported for Arrow table conversion
 */
export const categoryInterner = new StringInterner();
export const moduleIdInterner = new StringInterner();
export const spanNameInterner = new StringInterner();

/**
 * Check if capacity should be tuned based on usage patterns
 * 
 * Per specs/01b_columnar_buffer_architecture.md:
 * - Increase if >15% writes overflow
 * - Decrease if <5% writes overflow with many buffers
 * - Bounded growth: 8-1024 entries
 */
function shouldTuneCapacity(stats: BufferCapacityStats): boolean {
  const minSamples = 100; // Need enough data
  if (stats.totalWrites < minSamples) return false;
  
  const overflowRatio = stats.overflowWrites / stats.totalWrites;
  
  // Increase if >15% writes overflow
  if (overflowRatio > 0.15 && stats.currentCapacity < 1024) {
    const newCapacity = Math.min(stats.currentCapacity * 2, 1024);
    
    // TODO: Use system tracer for self-tracing capacity tuning events
    // For now, removed console.log to avoid hot path overhead
    
    stats.currentCapacity = newCapacity;
    resetStats(stats);
    return true;
  }
  
  // Decrease if <5% writes overflow and we have many buffers
  if (overflowRatio < 0.05 && stats.totalBuffersCreated >= 10 && stats.currentCapacity > 8) {
    const newCapacity = Math.max(8, stats.currentCapacity / 2);
    
    // TODO: Use system tracer for self-tracing capacity tuning events
    // For now, removed console.log to avoid hot path overhead
    
    stats.currentCapacity = newCapacity;
    resetStats(stats);
    return true;
  }
  
  return false;
}

/**
 * Reset stats after capacity tuning
 */
function resetStats(stats: BufferCapacityStats): void {
  stats.totalWrites = 0;
  stats.overflowWrites = 0;
  stats.totalBuffersCreated = 0;
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
 * 
 * Per specs/01h_entry_types_and_logging_primitives.md:
 * - Tag getter creates a new entry in the buffer
 * - Subsequent method calls write to the SAME row
 * - All methods return this for zero-allocation chaining
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
 * 
 * Per specs/01h_entry_types_and_logging_primitives.md:
 * - tag is a GETTER that creates a new entry and returns chainable API
 * - All methods write to the SAME row until the next tag access
 * 
 * Per specs/01i_span_scope_attributes.md:
 * - scope() sets attributes that auto-propagate to all subsequent entries
 */
export interface SpanLogger<T extends TagAttributeSchema> {
  /**
   * Tag attribute API with method chaining support
   * This is a GETTER that creates a new tag entry in the buffer
   * 
   * Usage:
   * - ctx.log.tag.userId(value) - creates entry, sets userId, returns this
   * - ctx.log.tag.userId(value).requestId(value2) - continues on SAME entry
   * - ctx.log.tag.with({ userId, requestId }) - bulk set on SAME entry
   */
  readonly tag: ChainableTagAPI<T>;
  
  /**
   * Set scoped attributes that auto-propagate to all subsequent entries
   * 
   * Usage:
   * - ctx.log.scope({ requestId: req.id, userId: req.user?.id })
   * - All subsequent log.tag, log.info, etc. will include these attributes
   */
  scope(attributes: Partial<InferTagAttributes<T>>): void;
  
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
  
  // Result helpers with fluent API
  ok<V>(value: V): FluentSuccessResult<V, T>;
  err<E>(code: string, error: E): FluentErrorResult<E, T>;
  
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
 * Per specs/01h_entry_types_and_logging_primitives.md
 * 
 * Exported for use in codegen and Arrow conversion
 */
export const ENTRY_TYPE_FF_ACCESS = 1;
export const ENTRY_TYPE_FF_USAGE = 2;
export const ENTRY_TYPE_TAG = 3;
export const ENTRY_TYPE_MESSAGE = 4; // Generic message (deprecated, use specific levels)
export const ENTRY_TYPE_SPAN_START = 5;
export const ENTRY_TYPE_SPAN_OK = 6;
export const ENTRY_TYPE_SPAN_ERR = 7;
export const ENTRY_TYPE_SPAN_EXCEPTION = 8;
// Distinct entry types for log levels (specs/01h)
export const ENTRY_TYPE_INFO = 9;
export const ENTRY_TYPE_DEBUG = 10;
export const ENTRY_TYPE_WARN = 11;
export const ENTRY_TYPE_ERROR = 12;

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
 * Find buffer with space, creating chained buffer if needed
 * 
 * Per specs/01b_columnar_buffer_architecture.md:
 * - Buffer chaining handles overflow gracefully
 * - Tracks overflow stats for self-tuning
 * - CPU branch predictor friendly
 */
function getBufferWithSpace(buffer: SpanBuffer): { buffer: SpanBuffer; didOverflow: boolean } {
  const originalBuffer = buffer;
  let didOverflow = false;
  
  // Find buffer with space (CPU branch predictor friendly)
  while (buffer.writeIndex >= buffer.capacity) {
    if (!buffer.next) {
      buffer.next = createNextBuffer(buffer);
    }
    // Type assertion: createNextBuffer always returns SpanBuffer
    buffer = buffer.next as SpanBuffer;
    didOverflow = true;
  }
  
  // Track stats for self-tuning
  const stats = originalBuffer.task.module.spanBufferCapacityStats;
  stats.totalWrites++;
  if (didOverflow) {
    stats.overflowWrites++;
  }
  
  // Check if capacity should be tuned
  shouldTuneCapacity(stats);
  
  return { buffer, didOverflow };
}

/**
 * Write span-start entry to buffer
 * Per specs/01h_entry_types_and_logging_primitives.md
 */
function writeSpanStart(buffer: SpanBuffer, spanName: string): void {
  // Find buffer with space
  const { buffer: bufferWithSpace } = getBufferWithSpace(buffer);
  const idx = bufferWithSpace.writeIndex;
  
  // Write entry type
  bufferWithSpace.operations[idx] = ENTRY_TYPE_SPAN_START;
  
  // Write timestamp
  bufferWithSpace.timestamps[idx] = Date.now();
  
  // Write span name
  writeToColumn(bufferWithSpace, 'attr_spanName', spanName, idx);
  
  // Increment write index
  bufferWithSpace.writeIndex++;
}

/**
 * Text string storage - raw strings without interning
 * Separate from category interning to avoid dictionary overhead for unique strings
 */
class TextStringStorage {
  private strings: string[] = [];
  
  /**
   * Store a text string and return its index
   * No deduplication - every string gets a new index
   */
  store(str: string): number {
    const idx = this.strings.length;
    this.strings.push(str);
    return idx;
  }
  
  /**
   * Get string by index
   */
  getString(idx: number): string | undefined {
    return this.strings[idx];
  }
  
  /**
   * Get all strings for Arrow column
   */
  getStrings(): readonly string[] {
    return this.strings;
  }
}

/**
 * Global text string storage
 * One instance for all text columns
 * 
 * Exported for Arrow table conversion
 */
export const textStringStorage = new TextStringStorage();

/**
 * Write a value to a TypedArray column
 * Handles type conversion for different column types
 * 
 * Per specs/01b1_buffer_performance_optimizations.md:
 * - String interning for category types
 * - Raw storage for text types
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
  
  // Get schema metadata to determine string type (category vs text)
  const fieldName = columnName.replace('attr_', '');
  const schema = buffer.task.module.tagAttributes;
  const fieldSchema = schema[fieldName];
  
  // Write based on column type
  if (column instanceof Uint8Array) {
    // For boolean or small enum types
    if (typeof value === 'boolean') {
      column[index] = value ? 1 : 0;
    } else if (typeof value === 'number') {
      column[index] = value;
    } else if (typeof value === 'string') {
      // For string enums, map to index using enum values from schema metadata
      const schemaWithMetadata = fieldSchema as import('./schema/types.js').EnumSchemaWithMetadata;
      const enumValues = schemaWithMetadata?.__lmao_enum_values;
      
      if (enumValues) {
        // Find index of value in enum values array
        const enumIndex = enumValues.indexOf(value);
        column[index] = enumIndex >= 0 ? enumIndex : 0;
      } else {
        column[index] = 0;
      }
    }
  } else if (column instanceof Uint16Array) {
    // For medium enum types (256-65535 values)
    if (typeof value === 'number') {
      column[index] = value;
    } else if (typeof value === 'string') {
      // Map enum string to index
      const schemaWithMetadata = fieldSchema as import('./schema/types.js').EnumSchemaWithMetadata;
      const enumValues = schemaWithMetadata?.__lmao_enum_values;
      
      if (enumValues) {
        const enumIndex = enumValues.indexOf(value);
        column[index] = enumIndex >= 0 ? enumIndex : 0;
      } else {
        column[index] = 0;
      }
    }
  } else if (column instanceof Uint32Array) {
    // For category/text types - check schema metadata to decide
    if (typeof value === 'string') {
      // Check if this is a category or text type via schema metadata
      const schemaWithMetadata = fieldSchema as import('./schema/types.js').SchemaWithMetadata;
      const lmaoType = schemaWithMetadata?.__lmao_type;
      
      if (lmaoType === 'text') {
        // Text: raw storage without interning
        column[index] = textStringStorage.store(value);
      } else {
        // Category (or unknown): use string interning
        column[index] = categoryInterner.intern(value);
      }
    } else if (typeof value === 'number') {
      column[index] = value;
    }
  } else if (column instanceof Float64Array) {
    // For number types
    column[index] = typeof value === 'number' ? value : 0;
  }
}

/**
 * Get buffer with space function type
 */
type GetBufferWithSpaceFn = (buffer: SpanBuffer) => { buffer: SpanBuffer; didOverflow: boolean };

/**
 * Cache for generated SpanLogger classes
 * Per-schema cache to avoid regenerating the same class
 */
const spanLoggerClassCache = new WeakMap<TagAttributeSchema, new (
  buffer: SpanBuffer,
  categoryInterner: StringInterner,
  textStorage: TextStringStorage,
  getBufferWithSpace: GetBufferWithSpaceFn
) => BaseSpanLogger<TagAttributeSchema>>();

/**
 * Create span logger with typed tag methods and method chaining
 * Writes to TypedArray columnar buffers in memory (hot path)
 * 
 * Per specs/01g_trace_context_api_codegen.md and 01j_module_context_and_spanlogger_generation.md:
 * - Uses runtime class generation with new Function() for zero-overhead prototype methods
 * - Tag getter creates a new entry and returns a chainable API
 * - All chained methods write to the SAME row
 * - Zero allocations: returns same object instance
 * 
 * @param schema - Tag attribute schema with field definitions
 * @param buffer - SpanBuffer to write entries to (per-span instance)
 * @returns SpanLogger with typed methods matching schema
 */
function createSpanLogger<T extends TagAttributeSchema>(
  schema: T,
  buffer: SpanBuffer
): SpanLogger<T> {
  // Get or create the generated SpanLogger class (cold path - happens once per schema)
  let SpanLoggerClass = spanLoggerClassCache.get(schema);
  
  if (!SpanLoggerClass) {
    SpanLoggerClass = createSpanLoggerClass(schema);
    spanLoggerClassCache.set(schema, SpanLoggerClass);
  }
  
  // TypeScript doesn't know the WeakMap guarantees non-null after set
  // So we add an assertion here
  if (!SpanLoggerClass) {
    throw new Error('Failed to create SpanLogger class');
  }
  
  // Create instance (hot path - happens once per span)
  const logger = new SpanLoggerClass(
    buffer,
    categoryInterner,
    textStringStorage,
    getBufferWithSpace
  );
  
  return logger as SpanLogger<T>;
}

/**
 * Extract just the schema fields from an object, removing methods
 * This allows us to accept objects with additional methods like validate, extend, etc.
 * 
 * This type recursively picks all properties that are not functions from intersections
 * 
 * IMPORTANT: This must properly filter out methods added by defineTagAttributes like:
 * - validate
 * - parse
 * - safeParse
 * - extend
 */
type ExtractSchemaFields<T> = {
  [K in keyof T as T[K] extends Function ? never : K]: T[K];
};

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
        
        // Write span-start entry
        writeSpanStart(spanBuffer, name);
        
        // Create span logger with typed tag methods
        const spanLogger = createSpanLogger(schemaOnly, spanBuffer);
        
        // Create span context
        const spanContext: SpanContext<T, FF, Env> = {
          ...requestCtx,
          log: spanLogger,
          
          ok<V>(value: V): FluentSuccessResult<V, T> {
            return new FluentSuccessResult<V, T>(spanBuffer, value, schemaOnly);
          },
          
          err<E>(code: string, error: E): FluentErrorResult<E, T> {
            return new FluentErrorResult<E, T>(spanBuffer, code, error, schemaOnly);
          },
          
          async span<R>(
            childName: string,
            childFn: (ctx: SpanContext<T, FF, Env>) => Promise<R>
          ): Promise<R> {
            // Create child span buffer with Arrow builders
            const childBuffer = createChildSpanBuffer(spanBuffer, taskContext);
            
            // Write span-start for child span
            writeSpanStart(childBuffer, childName);
            
            // Create child context with its own logger
            const childLogger = createSpanLogger(schemaOnly, childBuffer);
            const childContext: SpanContext<T, FF, Env> = {
              ...spanContext,
              log: childLogger,
            };
            
            // Execute child span with exception handling
            try {
              return await childFn(childContext);
            } catch (error) {
              // Write span-exception entry
              const { buffer: bufferWithSpace } = getBufferWithSpace(childBuffer);
              const idx = bufferWithSpace.writeIndex;
              
              bufferWithSpace.operations[idx] = ENTRY_TYPE_SPAN_EXCEPTION;
              bufferWithSpace.timestamps[idx] = Date.now();
              
              // Write exception details
              const errorMessage = error instanceof Error ? error.message : String(error);
              const errorStack = error instanceof Error ? error.stack : undefined;
              
              writeToColumn(bufferWithSpace, 'attr_exceptionMessage', errorMessage, idx);
              if (errorStack) {
                writeToColumn(bufferWithSpace, 'attr_exceptionStack', errorStack, idx);
              }
              
              bufferWithSpace.writeIndex++;
              
              // Re-throw to propagate
              throw error;
            }
          },
        };
        
        // Execute task function with exception handling
        try {
          return await fn(spanContext, ...args);
        } catch (error) {
          // Write span-exception entry
          const { buffer: bufferWithSpace } = getBufferWithSpace(spanBuffer);
          const idx = bufferWithSpace.writeIndex;
          
          bufferWithSpace.operations[idx] = ENTRY_TYPE_SPAN_EXCEPTION;
          bufferWithSpace.timestamps[idx] = Date.now();
          
          // Write exception details
          const errorMessage = error instanceof Error ? error.message : String(error);
          const errorStack = error instanceof Error ? error.stack : undefined;
          
          writeToColumn(bufferWithSpace, 'attr_exceptionMessage', errorMessage, idx);
          if (errorStack) {
            writeToColumn(bufferWithSpace, 'attr_exceptionStack', errorStack, idx);
          }
          
          bufferWithSpace.writeIndex++;
          
          // Re-throw to propagate
          throw error;
        }
      };
    },
  };
}
