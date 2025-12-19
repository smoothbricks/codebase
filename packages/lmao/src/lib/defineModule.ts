/**
 * Module Builder Pattern - Fluent API for defining traced modules
 *
 * Per specs/01l_module_builder_pattern.md:
 * - defineModule(config) returns a builder with .ctx<Extra>(defaults)
 * - .ctx<Extra>(defaults) captures Extra type, returns builder with .make()
 * - .make(opts?) finalizes module with optional ffEvaluator
 * - module.op(name, fn) creates Op instances
 * - module.traceContext(opts) creates TraceContext with auto-generated system props
 *
 * This is the primary entry point for instrumenting application modules.
 */

import { Nanoseconds } from '@smoothbricks/arrow-builder';
import { ModuleContext } from './moduleContext.js';
import { Op } from './op.js';
import type { EvaluationContext, FeatureFlagSchema } from './schema/defineFeatureFlags.js';
import { FeatureFlagEvaluator, type FlagEvaluator, type FlagValue } from './schema/evaluator.js';
import { LogSchema } from './schema/LogSchema.js';
import { mergeWithSystemSchema } from './schema/systemSchema.js';
import type { Schema, SchemaFields } from './schema/types.js';
import type { SpanContext } from './spanContext.js';
import { getThreadId } from './threadId.js';
import {
  type ReservedTraceContextKeys,
  type RootSpanFn,
  type TraceContext,
  type TraceContextBase,
  TraceContextProto,
} from './traceContext.js';
import { generateTraceId, type TraceId } from './traceId.js';

// =============================================================================
// Module-Level Constants
// =============================================================================

// RESERVED_CONTEXT_KEYS moved to op.ts where it's used

// =============================================================================
// Default Value Flag Evaluator
// =============================================================================

/**
 * DefaultValueFlagEvaluator - Returns schema defaults when no evaluator provided
 *
 * Per spec 01l lines 244-248:
 * - Used when .make() is called without ffEvaluator
 * - Simply returns the default values from the ff schema
 */
export class DefaultValueFlagEvaluator implements FlagEvaluator {
  private defaults: Record<string, FlagValue>;

  constructor(ffSchema: FeatureFlagSchema | undefined) {
    this.defaults = {};
    if (ffSchema) {
      for (const [key, def] of Object.entries(ffSchema)) {
        // Access the default value from the schema definition
        const schemaWithDefault = def as { default?: FlagValue };
        this.defaults[key] = schemaWithDefault.default ?? null;
      }
    }
  }

  getSync<K extends string>(flag: K, _context: EvaluationContext): FlagValue {
    return this.defaults[flag] ?? null;
  }

  async getAsync<K extends string>(flag: K, _context: EvaluationContext): Promise<FlagValue> {
    return this.defaults[flag] ?? null;
  }
}

// =============================================================================
// Op Metrics Tracking Helpers
// =============================================================================

/**
 * Track an overflow write and check if capacity should be tuned.
 *
 * Per specs/01b2_buffer_self_tuning.md:
 * - Increase if >15% writes overflow
 * - Decrease if <5% writes overflow with many buffers
 * - Bounded growth: 8-1024 entries
 *
 * @param module - ModuleContext with sb_* capacity statistics
 */
export function trackOverflowAndTune(module: ModuleContext): void {
  module.sb_overflowWrites++;
  shouldTuneCapacity(module);
}

/**
 * Check if capacity should be tuned based on usage patterns and update stats if needed.
 *
 * @param module - ModuleContext with sb_* capacity statistics
 * @internal Exported for testing
 */
export function shouldTuneCapacity(module: ModuleContext): void {
  const minSamples = 100;
  if (module.sb_totalWrites < minSamples) return;

  const overflowRatio = module.sb_overflowWrites / module.sb_totalWrites;

  // Increase if >15% writes overflow
  if (overflowRatio > 0.15 && module.sb_capacity < 1024) {
    module.sb_capacity = Math.min(module.sb_capacity * 2, 1024);
    module.sb_totalWrites = 0;
    module.sb_overflowWrites = 0;
    module.sb_totalCreated = 0;
    return;
  }

  // Decrease if <5% writes overflow and we have many buffers
  if (overflowRatio < 0.05 && module.sb_totalCreated >= 10 && module.sb_capacity > 8) {
    module.sb_capacity = Math.max(8, module.sb_capacity / 2);
    module.sb_totalWrites = 0;
    module.sb_overflowWrites = 0;
    module.sb_totalCreated = 0;
  }
}

// =============================================================================
// Type Helpers
// =============================================================================

/**
 * Type signature for op functions wrapped by module contexts.
 */
export type OpFunction<
  Args extends unknown[],
  Result,
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>,
  Extra extends Record<string, unknown> = Record<string, never>,
> = (ctx: SpanContext<T, FF, Env> & Extra, ...args: Args) => Promise<Result>;

/**
 * Module metadata injected by TypeScript transformer
 */
export interface ModuleMetadata {
  packageName: string;
  packagePath: string;
  gitSha?: string;
}

/**
 * Type to validate Extra doesn't contain reserved keys
 */
export type ValidateExtra<Extra extends Record<string, unknown>> = {
  [K in keyof Extra]: K extends ReservedTraceContextKeys ? never : Extra[K];
};

/**
 * Type helpers for batch op definitions
 *
 * Per spec 01l lines 313-342 and plan lines 102-108
 */
export type ExtractArgs<Ctx, Fn> = Fn extends (ctx: Ctx, ...args: infer Args) => Promise<infer _R> ? Args : never;
export type ExtractResult<Ctx, Fn> = Fn extends (ctx: Ctx, ...args: infer _Args) => Promise<infer R> ? R : never;
export type BatchResult<Ctx, T extends Record<string, (ctx: Ctx, ...args: any[]) => Promise<any>>> = {
  [K in keyof T]: Op<Ctx, ExtractArgs<Ctx, T[K]>, ExtractResult<Ctx, T[K]>>;
};

/**
 * OpRecord - Record of op functions for batch definition
 */
export type OpRecord<Ctx> = Record<string, (ctx: Ctx, ...args: any[]) => Promise<any>>;

// =============================================================================
// Module Builder Types
// =============================================================================

/**
 * Options for traceContext creation
 */
export type TraceContextParams<FF extends FeatureFlagSchema, Extra extends Record<string, unknown>> = {
  /** Optional ff evaluator override */
  ff?: FeatureFlagEvaluator<FF>;
  /** Optional trace ID (auto-generated if not provided) */
  traceId?: TraceId;
} & Extra;

/**
 * RootSpanFn - Function type for ctx.span() at the root level
 *
 * The first argument (lineNumber) is injected by the transformer.
 */
// RootSpanFn is imported from traceContext.ts - no need to redefine here

/**
 * PrefixedModule - Module with prefix applied for library composition
 *
 * Per spec 01l lines 415-416: prefix() returns PrefixedModule
 * Note: PrefixedModule does NOT extend Module to avoid method/property name conflict
 */
export interface PrefixedModule<
  T extends LogSchema | SchemaFields,
  FF extends FeatureFlagSchema,
  Extra extends Record<string, unknown>,
  P extends string,
> {
  readonly metadata: ModuleMetadata;
  readonly module: ModuleContext;
  readonly _prefix: P; // Use _prefix to avoid conflict with prefix() method
  // Buffer metrics (flattened names)
  readonly sb_capacity: number;
  readonly sb_totalWrites: number;
  readonly sb_overflows: number;
  readonly sb_overflowWrites: number;
  readonly sb_totalCreated: number;
  /**
   * Apply another prefix (for chaining)
   */
  prefix<P2 extends string>(prefix: P2): PrefixedModule<T, FF, Extra, `${P}_${P2}`>;
  /**
   * Wire dependencies
   */
  use(wiredDeps: Record<string, unknown>): BoundModule<T, FF, Extra>;
}

/**
 * BoundModule - Module with dependencies wired
 *
 * Per spec 01l lines 418-419: use() returns BoundModule with span() method
 */
export interface BoundModule<
  T extends SchemaFields,
  FF extends FeatureFlagSchema,
  Extra extends Record<string, unknown>,
> {
  /**
   * Create a root span directly (without traceContext)
   *
   * Per spec 01l lines 666-674: BoundModule has span() method
   * Supports both with and without line number (matching RootSpanFn pattern)
   */
  // Overload 1: With line number (transformer output)
  span<R, Args extends unknown[]>(
    lineNumber: number,
    name: string,
    op: Op<SpanContext<LogSchema<T>, FF, Record<string, unknown>> & Extra, Args, R>,
    ...args: Args
  ): Promise<R>;

  // Overload 2: Without line number (user writes)
  span<R, Args extends unknown[]>(
    name: string,
    op: Op<SpanContext<LogSchema<T>, FF, Record<string, unknown>> & Extra, Args, R>,
    ...args: Args
  ): Promise<R>;
}

/**
 * Module interface returned by defineModule().ctx().make()
 */
export interface Module<T extends SchemaFields, FF extends FeatureFlagSchema, Extra extends Record<string, unknown>> {
  readonly metadata: ModuleMetadata;
  readonly module: ModuleContext;

  // Buffer metrics (flattened names)
  readonly sb_capacity: number;
  readonly sb_totalWrites: number;
  readonly sb_overflows: number;
  readonly sb_overflowWrites: number;
  readonly sb_totalCreated: number;

  /**
   * Create root TraceContext
   */
  traceContext(params: TraceContextParams<FF, Extra>): TraceContext<FF, Extra>;

  /**
   * Create an Op wrapper for a function
   *
   * Per spec 01l lines 407-410: Returns Op instance directly
   */
  op<Args extends unknown[], R>(
    name: string,
    fn: OpFunction<Args, R, LogSchema<T>, FF, Record<string, unknown>, Extra>,
    line?: number,
  ): Op<SpanContext<LogSchema<T>, FF, Record<string, unknown>> & Extra, Args, R>;

  /**
   * Batch op definition - define multiple ops at once
   *
   * Per spec 01l lines 313-342:
   * - Property key becomes the Op name (for metrics)
   * - ThisType enables `this.otherOp` binding within batch op definitions
   *
   * @example
   * const ops = op({
   *   fetchUser: async ({ tag }, userId: string) => { ... },
   *   updateUser: async ({ tag, span }, userId: string, data: UserData) => {
   *     await span('validate', this.fetchUser, userId); // this binding works!
   *     ...
   *   },
   * });
   */
  op<BatchDefs extends OpRecord<SpanContext<LogSchema<T>, FF, Record<string, unknown>> & Extra>>(
    definitions: BatchDefs &
      ThisType<BatchResult<SpanContext<LogSchema<T>, FF, Record<string, unknown>> & Extra, BatchDefs>>,
  ): BatchResult<SpanContext<LogSchema<T>, FF, Record<string, unknown>> & Extra, BatchDefs>;

  /**
   * Apply prefix for library composition
   *
   * Per spec 01l lines 415-416
   */
  prefix<P extends string>(prefix: P): PrefixedModule<T, FF, Extra, P>;

  /**
   * Wire dependencies
   *
   * Per spec 01l lines 418-419
   */
  use(wiredDeps: Record<string, unknown>): BoundModule<T, FF, Extra>;
}

// =============================================================================
// Builder Implementation
// =============================================================================

/**
 * Internal builder state
 */
interface BuilderState<T extends SchemaFields, FF extends FeatureFlagSchema> {
  metadata: ModuleMetadata;
  logSchema: LogSchema<T>;
  deps?: Record<string, unknown>;
  ff?: FF;
  moduleContext: ModuleContext;
  schemaOnly: SchemaFields;
}

/**
 * Builder after .ctx<Extra>(defaults) is called
 */
interface CtxBuilder<T extends SchemaFields, FF extends FeatureFlagSchema, Extra extends Record<string, unknown>> {
  /**
   * Finalize the module with optional ff evaluator
   */
  make(opts?: { ffEvaluator?: FlagEvaluator }): Module<T, FF, Extra>;
}

/**
 * Initial builder returned by defineModule()
 */
interface DefineModuleBuilder<T extends SchemaFields, FF extends FeatureFlagSchema> {
  /**
   * Define Extra context properties with defaults
   *
   * @param defaults - Default values for Extra props. Use null! for required, undefined for optional.
   *
   * **Type Enforcement**: TypeScript enforces required vs optional based on the Extra type itself:
   * - Properties without `?` in Extra type are REQUIRED in traceContext()
   * - Properties with `?` in Extra type are OPTIONAL in traceContext()
   *
   * **Runtime Convention**: The `null!` vs `undefined` values in defaults are for:
   * - V8 hidden class optimization (Object.keys() provides fixed property list for codegen)
   * - Documentation clarity (makes required vs optional explicit at call site)
   *
   * The type system enforces required/optional based on Extra type's `?` markers, not the defaults values.
   */
  ctx<Extra extends Record<string, unknown>>(
    defaults: ValidateExtra<Extra>,
  ): CtxBuilder<T, FF, Extra> & Module<T, FF, Extra>;

  // Also expose Module interface directly (when .ctx() is not called)
  readonly metadata: ModuleMetadata;
  readonly module: ModuleContext;
  readonly sb_capacity: number;
  readonly sb_totalWrites: number;
  readonly sb_overflows: number;
  readonly sb_overflowWrites: number;
  readonly sb_totalCreated: number;
  traceContext(params: TraceContextParams<FF, Record<string, unknown>>): TraceContext<FF, Record<string, unknown>>;
  op<Args extends unknown[], R>(
    name: string,
    fn: OpFunction<Args, R, LogSchema<T>, FF, Record<string, unknown>, Record<string, unknown>>,
    line?: number,
  ): Op<SpanContext<LogSchema<T>, FF, Record<string, unknown>> & Record<string, unknown>, Args, R>;
}

// =============================================================================
// defineModule Implementation
// =============================================================================

/**
 * Defines a module with typed log schema and op wrappers.
 *
 * This is the primary entry point for instrumenting application modules.
 * Uses the fluent builder pattern:
 *
 * ```typescript
 * const myModule = defineModule({
 *   metadata: { packageName: '@myorg/users', packagePath: 'src/users.ts' },
 *   logSchema: { userId: S.category(), operation: S.enum(['CREATE', 'READ']) },
 *   deps: { http: httpModule },
 *   ff: { premiumApi: ff.boolean() },
 * })
 *   .ctx<{
 *     env: { apiTimeout: number };  // Required (no ?)
 *     requestId: string;            // Required (no ?)
 *     userId?: string;              // Optional (has ?)
 *   }>({
 *     env: null!,        // null! = runtime convention for required
 *     requestId: null!, // null! = runtime convention for required
 *     userId: undefined, // undefined = runtime convention for optional
 *   })
 *   .make({ ffEvaluator: ldEvaluator });
 *
 * // TypeScript enforces required/optional based on Extra type's ? markers:
 * const ctx = myModule.traceContext({
 *   env: { apiTimeout: 5000 },  // Required - TypeScript error if missing
 *   requestId: 'req-123',       // Required - TypeScript error if missing
 *   // userId can be omitted - it's optional
 * });
 * ```
 *
 * **Type Enforcement**: The type system enforces required vs optional based on whether properties
 * have `?` in the Extra type definition. The `null!` vs `undefined` values in defaults are
 * runtime conventions for V8 optimization and documentation, not type-level enforcement.
 */
export function defineModule<T extends SchemaFields, FF extends FeatureFlagSchema = FeatureFlagSchema>(options: {
  metadata?: ModuleMetadata;
  moduleMetadata?: ModuleMetadata;
  logSchema: T;
  deps?: Record<string, unknown>;
  ff?: FF;
}): DefineModuleBuilder<T, FF> {
  // Support both 'metadata' and 'moduleMetadata' for compatibility
  const metadata = options.metadata ??
    options.moduleMetadata ?? {
      packageName: 'unknown',
      packagePath: 'unknown',
      gitSha: undefined,
    };

  // Wrap user input in LogSchema if needed (handles both LogSchema instances and plain objects)
  const logSchema = options.logSchema instanceof LogSchema ? options.logSchema : new LogSchema(options.logSchema);

  // Extract schema fields from LogSchema instance
  const userSchemaOnly = logSchema.fields;

  // Merge with system schema
  const schemaOnly = mergeWithSystemSchema(userSchemaOnly);

  // Create ModuleContext
  const moduleContext = new ModuleContext(
    metadata.gitSha ?? 'unknown',
    metadata.packageName,
    metadata.packagePath,
    schemaOnly,
  );
  // Store deps in moduleContext for access in Op._invoke()
  (moduleContext as unknown as { deps?: Record<string, unknown> }).deps = options.deps;

  const state: BuilderState<T, FF> = {
    metadata,
    logSchema,
    deps: options.deps,
    ff: options.ff,
    moduleContext,
    schemaOnly,
  };

  /**
   * Create the final module implementation
   */
  function createModule<Extra extends Record<string, unknown>>(
    extraDefaults: Extra | undefined,
    ffEvaluator?: FlagEvaluator,
  ): Module<T, FF, Extra> {
    // Use provided evaluator or create default
    const evaluator = ffEvaluator ?? new DefaultValueFlagEvaluator(state.ff);

    return {
      metadata: state.metadata,
      module: state.moduleContext,

      // Buffer metrics passthrough
      get sb_capacity() {
        return state.moduleContext.sb_capacity;
      },
      get sb_totalWrites() {
        return state.moduleContext.sb_totalWrites;
      },
      get sb_overflows() {
        return state.moduleContext.sb_overflows;
      },
      get sb_overflowWrites() {
        return state.moduleContext.sb_overflowWrites;
      },
      get sb_totalCreated() {
        return state.moduleContext.sb_totalCreated;
      },

      /**
       * Create TraceContext for starting a new trace
       */
      traceContext(params: TraceContextParams<FF, Extra>): TraceContext<FF, Extra> {
        const { ff: ffOverride, traceId: providedTraceId, ...userProps } = params;

        // Create evaluation context from user props
        const evaluationContext: EvaluationContext = {
          userId: (userProps as Record<string, unknown>).userId as string | undefined,
          requestId: (userProps as Record<string, unknown>).requestId as string | undefined,
        };

        // Create or use provided ff evaluator
        const ffEval =
          ffOverride ?? new FeatureFlagEvaluator(state.ff ?? ({} as FF), evaluationContext, evaluator, undefined);

        // Generate system properties
        const traceId = providedTraceId ?? generateTraceId();
        // Use Nanoseconds.now() for better precision, convert to microseconds
        const anchorEpochMicros = Number(Nanoseconds.now() / 1000n);
        const anchorPerfNow = performance.now();
        const threadId = getThreadId();

        // Create context with prototype inheritance
        const ctx = Object.create(TraceContextProto) as TraceContextBase & {
          span: RootSpanFn;
        } & Extra;

        // Assign system properties
        ctx.traceId = traceId;
        ctx.anchorEpochMicros = anchorEpochMicros;
        ctx.anchorPerfNow = anchorPerfNow;
        ctx.threadId = threadId;
        ctx.ff = ffEval;

        // Create root span function supporting both overloads
        const spanImpl = <Ctx, R, Args extends unknown[]>(
          lineNumberOrName: number | string,
          nameOrOp: string | Op<Ctx, Args, R>,
          ...rest: unknown[]
        ): Promise<R> => {
          const hasLine = typeof lineNumberOrName === 'number';
          const lineNumber = hasLine ? (lineNumberOrName as number) : 0;
          const name = hasLine ? (nameOrOp as string) : (lineNumberOrName as string);
          const op = hasLine ? (rest[0] as Op<Ctx, Args, R>) : (nameOrOp as Op<Ctx, Args, R>);
          const args = (hasLine ? rest.slice(1) : rest) as Args;

          return op._invoke(
            ctx as unknown as TraceContext<FeatureFlagSchema, Record<string, unknown>>,
            null,
            state.moduleContext,
            name,
            lineNumber,
            args,
          );
        };

        // RootSpanFn is a function type with overloads, so we cast the implementation
        ctx.span = spanImpl as unknown as RootSpanFn;

        // Assign user properties
        if (extraDefaults) {
          for (const key of Object.keys(extraDefaults)) {
            if (key in userProps) {
              (ctx as Record<string, unknown>)[key] = (userProps as Record<string, unknown>)[key];
            } else {
              (ctx as Record<string, unknown>)[key] = (extraDefaults as Record<string, unknown>)[key];
            }
          }
        }
        // Also copy any extra user props not in defaults
        for (const key of Object.keys(userProps)) {
          if (!(key in ctx)) {
            (ctx as Record<string, unknown>)[key] = (userProps as Record<string, unknown>)[key];
          }
        }

        return ctx as unknown as TraceContext<FF, Extra>;
      },

      /**
       * Create an Op wrapper for a function (single or batch)
       *
       * Per spec 01l lines 407-410: Returns Op instance directly, not a function wrapper
       */
      op(nameOrDefs: any, fn?: any, line?: number): any {
        // Check if first arg is string (single) or object (batch)
        if (typeof nameOrDefs === 'string') {
          // Single op definition
          if (!fn) {
            throw new Error('op() requires function when name is provided');
          }
          // Return Op instance directly (per spec and experiment)
          return new Op<SpanContext<T, FF, Record<string, unknown>> & Extra, any[], any>(
            nameOrDefs, // name is FIRST parameter
            state.moduleContext,
            fn as (ctx: SpanContext<T, FF, Record<string, unknown>> & Extra, ...args: any[]) => Promise<any>,
            line,
          );
        }
        // Batch op definition
        const result: Record<string, Op<any, any[], any>> = {};

        // Iterate entries, create Op for each function
        for (const [opName, opFn] of Object.entries(nameOrDefs)) {
          const op = new Op<any, any[], any>(
            opName, // name is FIRST parameter
            state.moduleContext,
            opFn as (ctx: any, ...args: any[]) => Promise<any>,
            undefined, // No line number for batch ops (could be enhanced later)
          );

          // Bind `this` to result object for `this.otherOp` access
          result[opName] = op;
        }

        // Bind `this` to result object for all functions
        // This enables `this.otherOp` within batch op definitions
        for (const [opName, opFn] of Object.entries(nameOrDefs)) {
          const boundFn = (opFn as CallableFunction).bind(result);
          const op = result[opName];
          // Replace the function in Op with bound version
          (op as unknown as { fn: unknown }).fn = boundFn;
        }

        return result;
      },

      /**
       * Apply prefix for library composition
       *
       * Per spec 01l lines 415-416: prefix() returns PrefixedModule
       * Uses prefixSchema to rename schema fields with prefix
       */
      prefix<P extends string>(prefix: P): PrefixedModule<T, FF, Extra, P> {
        // Import prefixSchema dynamically to avoid circular dependency
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        const { prefixSchema } = require('./library.js') as {
          prefixSchema: (schema: LogSchema, prefix: string) => LogSchema;
        };

        // Apply prefix to schema
        const prefixedSchema = prefixSchema(state.schemaOnly, prefix) as T;

        // Create new module context with prefixed schema
        const prefixedModuleContext = new ModuleContext(
          state.metadata.gitSha ?? 'unknown',
          state.metadata.packageName,
          state.metadata.packagePath,
          prefixedSchema,
        );
        // Copy deps
        (prefixedModuleContext as unknown as { deps?: Record<string, unknown> }).deps = state.deps;

        // Create prefixed module by calling defineModule again with prefixed schema
        // This creates a new module with the prefixed schema
        const prefixedSchemaFields = prefixedSchema.fields;
        const prefixedModuleDef = defineModule<typeof prefixedSchemaFields, FF>({
          metadata: state.metadata,
          logSchema: prefixedSchemaFields,
          deps: state.deps,
          ff: state.ff,
        });

        // Get the module instance (without .ctx() call, uses default Extra = Record<string, unknown>)
        const prefixedModule = prefixedModuleDef as unknown as Module<T, FF, Extra>;

        return {
          ...prefixedModule,
          _prefix: prefix,
        } as PrefixedModule<T, FF, Extra, P>;
      },

      /**
       * Wire dependencies
       *
       * Per spec 01l lines 418-419
       * Returns BoundModule with span() method
       */
      use(wiredDeps: Record<string, unknown>): BoundModule<T, FF, Extra> {
        // Store wired deps in state for use in span context
        const boundState = { ...state, deps: wiredDeps };

        // Implementation that handles both overloads
        const spanImpl = <R, Args extends unknown[]>(
          lineNumberOrName: number | string,
          nameOrOp: string | Op<SpanContext<LogSchema | SchemaFields, FF, Record<string, unknown>> & Extra, Args, R>,
          ...rest: unknown[]
        ): Promise<R> => {
          // Determine if line number is provided (first arg is number)
          const hasLine = typeof lineNumberOrName === 'number';
          const lineNumber = hasLine ? (lineNumberOrName as number) : 0;
          const name = hasLine ? (nameOrOp as string) : (lineNumberOrName as string);
          const op = hasLine
            ? (rest[0] as Op<SpanContext<LogSchema | SchemaFields, FF, Record<string, unknown>> & Extra, Args, R>)
            : (nameOrOp as Op<SpanContext<LogSchema | SchemaFields, FF, Record<string, unknown>> & Extra, Args, R>);
          const args = (hasLine ? rest.slice(1) : rest) as Args;

          // Create a minimal trace context for root span
          const traceCtx = this.traceContext({} as TraceContextParams<FF, Extra>);
          // Override deps with wired deps
          (traceCtx as unknown as { deps: Record<string, unknown> }).deps = wiredDeps;
          // Call op._invoke directly
          return op._invoke(
            traceCtx as unknown as TraceContext<FeatureFlagSchema, Record<string, unknown>>,
            null,
            boundState.moduleContext,
            name,
            lineNumber,
            args,
          );
        };

        // Create bound module with span method supporting both overloads
        const boundModule: BoundModule<T, FF, Extra> = {
          // TypeScript doesn't support overloads in object literals, so we use a function
          // that handles both cases internally
          span: spanImpl as BoundModule<T, FF, Extra>['span'],
        };

        return boundModule;
      },
    };
  }

  // Return the builder
  const builder: DefineModuleBuilder<T, FF> = {
    ctx<Extra extends Record<string, unknown>>(
      defaults: ValidateExtra<Extra>,
    ): CtxBuilder<T, FF, Extra> & Module<T, FF, Extra> {
      // The returned object has both .make() and all Module methods
      const module = createModule<Extra>(defaults as Extra, undefined);

      return {
        ...module,
        make(opts?: { ffEvaluator?: FlagEvaluator }) {
          return createModule<Extra>(defaults as Extra, opts?.ffEvaluator);
        },
      };
    },

    // Expose Module interface directly for when .ctx() is not called
    get metadata() {
      return state.metadata;
    },
    get module() {
      return state.moduleContext;
    },
    get sb_capacity() {
      return state.moduleContext.sb_capacity;
    },
    get sb_totalWrites() {
      return state.moduleContext.sb_totalWrites;
    },
    get sb_overflows() {
      return state.moduleContext.sb_overflows;
    },
    get sb_overflowWrites() {
      return state.moduleContext.sb_overflowWrites;
    },
    get sb_totalCreated() {
      return state.moduleContext.sb_totalCreated;
    },

    traceContext(params: TraceContextParams<FF, Record<string, unknown>>): TraceContext<FF, Record<string, unknown>> {
      return createModule<Record<string, unknown>>(undefined, undefined).traceContext(params);
    },

    op<Args extends unknown[], R>(
      name: string,
      fn: OpFunction<Args, R, T, FF, Record<string, unknown>, Record<string, unknown>>,
      line?: number,
    ): Op<SpanContext<T, FF, Record<string, unknown>> & Record<string, unknown>, Args, R> {
      return createModule<Record<string, unknown>>(undefined, undefined).op(name, fn, line);
    },
  };

  return builder;
}

// =============================================================================
// Re-exports for Convenience
// =============================================================================

// Re-export from result.ts
export type { ErrorResult, Result, SuccessResult } from './result.js';
export { FluentErrorResult, FluentSuccessResult } from './result.js';

// Re-export from spanContext.ts
export type { FluentLogEntry, SpanContext, SpanFn, SpanLogger } from './spanContext.js';
export {
  createSpanContextProto,
  createSpanLoggerWithScope,
  isSpanContext,
  SPAN_CONTEXT_MARKER,
  writeSpanStart,
} from './spanContext.js';
// Re-export from traceContext.ts
export type { Op, RootSpanFn as TraceContextRootSpanFn, TraceContext, TraceContextBase } from './traceContext.js';
export { isTraceContext, TRACE_CONTEXT_MARKER, TraceContextProto } from './traceContext.js';
