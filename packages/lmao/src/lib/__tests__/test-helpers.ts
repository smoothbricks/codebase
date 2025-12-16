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
    filePath?: string;
  } = {},
): ModuleContext {
  return new ModuleContext(
    options.moduleId ?? 1,
    options.gitSha ?? 'test-sha',
    options.filePath ?? 'test.ts',
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
    filePath?: string;
    spanNameId?: number;
    lineNumber?: number;
  } = {},
): TaskContext {
  const moduleContext = createTestModuleContext(tagAttributes, options);
  return new TaskContext(moduleContext, options.spanNameId ?? 1, options.lineNumber ?? 0);
}
