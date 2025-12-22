/**
 * Test helpers for creating ModuleContext and SpanBuffer instances.
 * Mirrors packages/lmao/src/lib/__tests__/test-helpers.ts
 */

import { type LogSchema, ModuleContext } from '@smoothbricks/lmao';

/**
 * Create a ModuleContext for testing.
 */
export function createTestModuleContext(
  tagAttributes: LogSchema,
  options: {
    git_sha?: string;
    package_name?: string;
    package_file?: string;
  } = {},
): ModuleContext {
  return new ModuleContext(
    options.git_sha ?? 'test-sha',
    options.package_name ?? '@test/package',
    options.package_file ?? 'src/test.ts',
    tagAttributes,
  );
}

/**
 * Create a test module context for testing.
 */
export function createTestTaskContext(
  tagAttributes: LogSchema,
  options: {
    git_sha?: string;
    package_name?: string;
    package_file?: string;
    spanName?: string;
    line?: number;
  } = {},
): ModuleContext {
  return createTestModuleContext(tagAttributes, options);
}
