/**
 * OpGroup Creation
 *
 * Creates OpGroups from a schema, flags, and ops. Provides prefix() and mapColumns()
 * methods for column mapping when wiring as dependencies.
 *
 * @module opContext/createOpGroup
 */

import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import { generateRemappedBufferViewClass } from '../library.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
import type { LogBinding } from '../types.js';
import type { FeatureFlagSchema } from './featureFlagTypes.js';
import type {
  ColumnMapping,
  EmptyDeps,
  MappedOpGroup,
  MappedSchema,
  OpGroup,
  PrefixedSchema,
  SchemaFieldsOf,
} from './opGroupTypes.js';
import type { Op } from './opTypes.js';

// =============================================================================
// INTERNAL HELPERS
// =============================================================================

/**
 * Create reverse mapping for RemappedBufferView.
 * Input: clean -> prefixed mapping (e.g., { status: 'http_status' })
 * Output: prefixed -> clean mapping (e.g., { http_status: 'status' })
 *
 * RemappedBufferView needs the reverse direction: when Arrow conversion
 * asks for 'http_status', we need to find 'status' in the library's buffer.
 */
function createReverseMapping(mapping: Record<string, string | null>): Record<string, string> {
  const reverse: Record<string, string> = {};
  for (const [cleanName, prefixedName] of Object.entries(mapping)) {
    if (prefixedName !== null) {
      reverse[prefixedName] = cleanName;
    }
  }
  return reverse;
}

/**
 * Build a prefix mapping for all schema fields
 *
 * @param fieldNames - The field names from the schema
 * @param prefix - The prefix to apply
 * @returns Column mapping where each field becomes `${prefix}_${field}`
 */
function buildPrefixMapping<T extends SchemaFields, P extends string>(
  fieldNames: readonly string[],
  prefix: P,
): ColumnMapping<T> {
  const mapping: Record<string, string> = {};
  for (const name of fieldNames) {
    mapping[name] = `${prefix}_${name}`;
  }
  return mapping as ColumnMapping<T>;
}

/**
 * Apply a prefix to an existing mapping
 *
 * For each entry in the mapping:
 * - If mapped to a string, prefix that string: 'status' -> 'http_status'
 * - If mapped to null (dropped), keep as null
 *
 * @param mapping - Existing column mapping
 * @param prefix - Prefix to apply
 * @returns New mapping with prefixed target names
 */
function prefixMapping<T extends SchemaFields, P extends string>(
  mapping: ColumnMapping<T>,
  prefix: P,
): ColumnMapping<T> {
  const result: Record<string, string | null> = {};
  for (const [key, value] of Object.entries(mapping)) {
    if (value === null) {
      result[key] = null;
    } else if (typeof value === 'string') {
      result[key] = `${prefix}_${value}`;
    }
  }
  return result as ColumnMapping<T>;
}

/**
 * Build the contributed schema object from the original schema and mapping
 *
 * The contributed schema reflects what columns this group adds to the app's schema
 * after mapping is applied.
 *
 * @param fields - Original schema fields
 * @param mapping - Column mapping
 * @returns The contributed schema with renamed keys
 */
function buildContributedSchema<T extends SchemaFields, M extends ColumnMapping<T>>(
  fields: T,
  mapping: M,
): MappedSchema<T, M> {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(fields)) {
    const mappedName = mapping[key as keyof M];
    if (mappedName === null) {
      // Dropped column - not included in contributed schema
    } else if (typeof mappedName === 'string') {
      // Explicitly mapped - use the mapped name
      result[mappedName] = value;
    } else {
      // Not in mapping - keep original name (only applies when mapColumns is called
      // with a partial mapping, but our types ensure full coverage via prefix)
      result[key] = value;
    }
  }
  return result as MappedSchema<T, M>;
}

// =============================================================================
// MAPPED OP GROUP IMPLEMENTATION
// =============================================================================

/**
 * Create a MappedOpGroup from an existing group and mapping
 */
function createMappedOpGroup<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  UserCtx extends Record<string, unknown>,
  ContributedSchema extends SchemaFields,
>(
  logSchema: T,
  flags: FF,
  ops: Record<string, Op<T, FF, EmptyDeps, UserCtx, unknown[], unknown>>,
  columnMapping: ColumnMapping<SchemaFieldsOf<T>>,
  contributedSchema: ContributedSchema,
  parentLogBinding: LogBinding,
): MappedOpGroup<T, FF, UserCtx, ContributedSchema> {
  // Generate RemappedBufferView class for this mapping.
  // The mapping is clean→prefixed (e.g., { status: 'http_status' })
  // RemappedBufferView needs the REVERSE: prefixed→clean (e.g., { http_status: 'status' })
  // so that Arrow conversion can ask for 'http_status' and find 'status' in the buffer.
  const reverseMapping = createReverseMapping(columnMapping as Record<string, string | null>);
  const remappedViewClass = generateRemappedBufferViewClass(reverseMapping);

  // Create new LogBinding with the remappedViewClass set.
  // Share stats with parent (object spread creates shared reference for primitives,
  // but stats are updated in-place on the binding object, so they stay synchronized)
  const logBinding: LogBinding = {
    logSchema: parentLogBinding.logSchema,
    remappedViewClass,
    sb_capacity: parentLogBinding.sb_capacity,
    sb_totalWrites: parentLogBinding.sb_totalWrites,
    sb_overflowWrites: parentLogBinding.sb_overflowWrites,
    sb_totalCreated: parentLogBinding.sb_totalCreated,
    sb_overflows: parentLogBinding.sb_overflows,
  };

  return {
    logSchema,
    flags,
    ops,
    _columnMapping: columnMapping,
    _contributedSchema: contributedSchema,

    prefix<P extends string>(p: P): MappedOpGroup<T, FF, UserCtx, PrefixedSchema<ContributedSchema, P>> {
      // Apply prefix to the current mapping's target names
      const newMapping = prefixMapping(columnMapping, p);
      // Build new contributed schema by prefixing the current contributed schema
      const newContributed = buildContributedSchema(
        contributedSchema as SchemaFields,
        buildPrefixMapping(Object.keys(contributedSchema), p),
      );
      return createMappedOpGroup(
        logSchema,
        flags,
        ops,
        newMapping,
        newContributed as PrefixedSchema<ContributedSchema, P>,
        logBinding, // Pass our logBinding (which has remappedViewClass)
      );
    },

    mapColumns<M extends ColumnMapping<SchemaFieldsOf<T>>>(
      mapping: M,
    ): MappedOpGroup<T, FF, UserCtx, MappedSchema<SchemaFieldsOf<T>, M>> {
      // Override/extend the existing mapping with new mapping
      const newMapping = { ...columnMapping, ...mapping };
      // Build contributed schema from original fields with new mapping
      const newContributed = buildContributedSchema(logSchema.fields as SchemaFieldsOf<T>, newMapping as M);
      return createMappedOpGroup(logSchema, flags, ops, newMapping, newContributed, logBinding);
    },
  };
}

// =============================================================================
// OP GROUP CREATION
// =============================================================================

/**
 * Create an OpGroup from a schema, flags, and ops
 *
 * The ops passed to this function already have a LogBinding attached from the factory.
 * All ops in the same context share the SAME LogBinding instance, which enables self-tuning:
 * - Stats (sb_capacity, sb_totalWrites, sb_overflows) aggregate across the group
 * - This allows buffer capacity to be tuned based on the entire group's workload
 *
 * The returned OpGroup can be:
 * - Used directly as a dependency (ops accessible via ctx.deps.name)
 * - Prefixed with `.prefix('http')` to namespace all columns
 * - Mapped with `.mapColumns({ query: 'query', _internal: null })` for fine control
 *
 * @param logSchema - The LogSchema defining columns for this group's ops
 * @param flags - Feature flag schema
 * @param ops - Record of Op instances belonging to this group (all share same LogBinding)
 * @returns OpGroup with prefix() and mapColumns() methods
 *
 * @example
 * ```typescript
 * const httpOps = createOpGroup(httpLogSchema, httpFlags, {
 *   fetch: fetchOp,
 *   post: postOp,
 * });
 *
 * // Later, wire with prefix:
 * deps: { http: httpOps.prefix('http') }
 * // Columns become: http_status, http_url, http_duration, etc.
 *
 * // Or with explicit mapping:
 * deps: { http: httpOps.mapColumns({ status: 'http_status', _debug: null }) }
 * ```
 */
export function createOpGroup<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  UserCtx extends Record<string, unknown>,
>(
  logSchema: T,
  flags: FF,
  ops: Record<string, Op<T, FF, EmptyDeps, UserCtx, unknown[], unknown>>,
): OpGroup<T, FF, UserCtx> {
  // Extract the logBinding from the first op (all ops in the group share the same binding)
  const firstOp = Object.values(ops)[0];
  const logBinding: LogBinding = firstOp?.logBinding ?? {
    logSchema,
    sb_capacity: DEFAULT_BUFFER_CAPACITY, // Start small, self-tuning grows as needed
    sb_totalWrites: 0,
    sb_overflowWrites: 0,
    sb_totalCreated: 0,
    sb_overflows: 0,
    remappedViewClass: undefined,
  };

  return {
    logSchema,
    flags,
    ops,

    prefix<P extends string>(p: P): MappedOpGroup<T, FF, UserCtx, PrefixedSchema<SchemaFieldsOf<T>, P>> {
      const fieldNames = logSchema.fieldNames;
      const mapping = buildPrefixMapping<SchemaFieldsOf<T>, P>(fieldNames, p);
      const contributedSchema = buildContributedSchema(logSchema.fields as SchemaFieldsOf<T>, mapping);
      return createMappedOpGroup(
        logSchema,
        flags,
        ops,
        mapping,
        contributedSchema as unknown as PrefixedSchema<SchemaFieldsOf<T>, P>,
        logBinding,
      );
    },

    mapColumns<M extends ColumnMapping<SchemaFieldsOf<T>>>(
      mapping: M,
    ): MappedOpGroup<T, FF, UserCtx, MappedSchema<SchemaFieldsOf<T>, M>> {
      const contributedSchema = buildContributedSchema(logSchema.fields as SchemaFieldsOf<T>, mapping);
      return createMappedOpGroup(logSchema, flags, ops, mapping, contributedSchema, logBinding);
    },
  };
}
