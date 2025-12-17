/**
 * Test helpers for creating ModuleContext and TaskContext instances.
 * Mirrors packages/lmao/src/lib/__tests__/test-helpers.ts
 */

import { ModuleContext, type TagAttributeSchema, TaskContext } from '@smoothbricks/lmao';

/**
 * Create a ModuleContext for testing.
 */
export function createTestModuleContext(
  tagAttributes: TagAttributeSchema,
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
    tagAttributes,
  );
}

/**
 * Create a TaskContext for testing.
 */
export function createTestTaskContext(
  tagAttributes: TagAttributeSchema,
  options: {
    gitSha?: string;
    packageName?: string;
    packagePath?: string;
    spanName?: string;
    lineNumber?: number;
  } = {},
): TaskContext {
  const moduleContext = createTestModuleContext(tagAttributes, options);
  return new TaskContext(moduleContext, options.spanName ?? 'test-span', options.lineNumber ?? 0);
}
