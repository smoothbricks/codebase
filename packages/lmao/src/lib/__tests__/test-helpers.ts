/**
 * Test helpers for creating ModuleContext and TaskContext instances.
 */

import { ModuleContext } from '../moduleContext.js';
import type { TagAttributeSchema } from '../schema/types.js';
import { TaskContext } from '../taskContext.js';

/**
 * Create a ModuleContext for testing.
 */
export function createTestModuleContext(
  tagAttributes: TagAttributeSchema,
  options: {
    moduleId?: number;
    gitSha?: string;
    packageName?: string;
    packagePath?: string;
  } = {},
): ModuleContext {
  return new ModuleContext(
    options.moduleId ?? 1,
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
    moduleId?: number;
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
