/**
 * Test helpers for creating ModuleContext and TaskContext instances.
 */

import { ModuleContext } from '../moduleContext.js';
import { LogSchema } from '../schema/LogSchema.js';
import { mergeWithSystemSchema } from '../schema/systemSchema.js';
import type { SchemaFields } from '../schema/types.js';
import { TaskContext } from '../taskContext.js';

/**
 * Create a LogSchema with system fields merged for testing.
 * Use this when calling createSpanBuffer directly in tests.
 */
export function createTestSchema<T extends SchemaFields>(fields: T): LogSchema<T> {
  const merged = mergeWithSystemSchema(fields);
  return new LogSchema(merged as T);
}

/**
 * Create a ModuleContext for testing.
 * Accepts either LogSchema or plain SchemaFields.
 */
export function createTestModuleContext(
  schema: SchemaFields | LogSchema,
  options: {
    gitSha?: string;
    packageName?: string;
    packagePath?: string;
  } = {},
): ModuleContext {
  return new ModuleContext(
    options.gitSha ?? 'test-sha',
    options.packageName ?? '@test/package',
    options.packagePath ?? 'src/test.ts',
    schema,
  );
}

/**
 * Create a TaskContext for testing.
 * Accepts either LogSchema or plain SchemaFields.
 */
export function createTestTaskContext(
  schema: SchemaFields | LogSchema,
  options: {
    gitSha?: string;
    packageName?: string;
    packagePath?: string;
    spanName?: string;
    lineNumber?: number;
  } = {},
): TaskContext {
  const moduleContext = createTestModuleContext(schema, options);
  return new TaskContext(moduleContext, options.spanName ?? 'test-span', options.lineNumber ?? 0);
}
