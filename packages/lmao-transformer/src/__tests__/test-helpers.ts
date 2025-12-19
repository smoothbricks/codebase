/**
 * Test helpers for creating ModuleContext and SpanBuffer instances.
 * Mirrors packages/lmao/src/lib/__tests__/test-helpers.ts
 */

import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import { createSpanBuffer, generateTraceId, type LogSchema, ModuleContext } from '@smoothbricks/lmao';

/**
 * Create a ModuleContext for testing.
 */
export function createTestModuleContext(
  tagAttributes: LogSchema,
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
 * Create a test module context for testing.
 */
export function createTestTaskContext(
  tagAttributes: LogSchema,
  options: {
    gitSha?: string;
    packageName?: string;
    packagePath?: string;
    spanName?: string;
    lineNumber?: number;
  } = {},
): ModuleContext {
  return createTestModuleContext(tagAttributes, options);
}
