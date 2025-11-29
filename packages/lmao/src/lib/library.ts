/**
 * Library integration pattern with prefix support
 * 
 * Per specs/01e_library_integration_pattern.md:
 * - Libraries define clean schemas without prefixes
 * - Prefixing happens at composition time
 * - Avoids naming conflicts across libraries
 */

import type { TagAttributeSchema } from './schema/types.js';
import { getSchemaFields } from './schema/types.js';
import type { ModuleContextBuilder, TaskFunction, RequestContext } from './lmao.js';
import { createModuleContext } from './lmao.js';
import type { FeatureFlagSchema } from './schema/defineFeatureFlags.js';
import { S } from './schema/builder.js';

/**
 * Library operation definition
 * Maps operation name to implementation function and span name
 */
export interface LibraryOperation<
  Args extends unknown[],
  Result,
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>
> {
  fn: TaskFunction<Args, Result, T, FF, Env>;
  spanName: string;
}

/**
 * Library module with operations
 * Created by library authors, consumed by applications
 */
export interface LibraryModule<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>,
  Ops extends Record<string, LibraryOperation<any[], any, T, FF, Env>> = Record<string, LibraryOperation<any[], any, T, FF, Env>>
> {
  schema: T;
  operations: Ops;
  task<Args extends unknown[], Result>(
    name: string,
    fn: TaskFunction<Args, Result, T, FF, Env>
  ): (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result>;
}

/**
 * Prefix a tag attribute schema
 * Renames all fields with a prefix to avoid conflicts
 * 
 * Example:
 * - Input: { status: S.number(), method: S.enum(['GET', 'POST']) }
 * - Prefix: 'http'
 * - Output: { http_status: S.number(), http_method: S.enum(['GET', 'POST']) }
 */
export function prefixSchema<T extends TagAttributeSchema>(
  schema: T,
  prefix: string
): TagAttributeSchema {
  const prefixedSchema: TagAttributeSchema = {};
  
  // Get schema fields, excluding methods added by defineTagAttributes
  for (const [fieldName, fieldSchema] of getSchemaFields(schema)) {
    const prefixedName = `${prefix}_${fieldName}`;
    prefixedSchema[prefixedName] = fieldSchema;
  }
  
  return prefixedSchema;
}

/**
 * Create a library module with clean schema
 * Library authors use this to define their module
 * 
 * @param options - Library metadata, schema, and operations
 * @returns Library module with operations
 */
export function createLibraryModule<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = Record<string, unknown>,
  Ops extends Record<string, LibraryOperation<any[], any, T, FF, Env>> = Record<string, LibraryOperation<any[], any, T, FF, Env>>
>(options: {
  gitSha: string;
  filePath: string;
  moduleName?: string;
  schema: T;
  operations?: Ops;
}): LibraryModule<T, FF, Env, Ops> {
  // Create module context with clean schema (no prefix yet)
  // Note: createModuleContext defaults to FeatureFlagSchema and Record<string, unknown>
  // but the actual FF and Env types will be provided by the application at runtime
  const moduleContext = createModuleContext<typeof options.schema, T, FF, Env>({
    moduleMetadata: {
      gitSha: options.gitSha,
      filePath: options.filePath,
      moduleName: options.moduleName || options.filePath,
    },
    tagAttributes: options.schema,
  });
  
  return {
    schema: options.schema,
    operations: (options.operations || {}) as Ops,
    task: moduleContext.task,
  };
}

/**
 * Module context factory for library composition
 * Applications use this to compose libraries with prefixes
 * 
 * Per specs/01e:
 * - Library writes: ctx.tag.status(200)
 * - Final column: http_status (with prefix)
 * - All mapping happens at task creation time (cold path)
 * 
 * @param prefix - Prefix to apply to all schema fields
 * @param moduleMetadata - Library metadata
 * @param schema - Clean library schema
 * @param operations - Library operations
 * @returns Module context builder with prefixed schema
 */
export function moduleContextFactory<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = Record<string, unknown>
>(
  prefix: string,
  moduleMetadata: {
    gitSha: string;
    filePath: string;
    moduleName: string;
  },
  schema: T,
  operations?: Record<string, LibraryOperation<any[], any, T, FF, Env>>
): ModuleContextBuilder<TagAttributeSchema, FF, Env> & {
  operations: Record<string, (...args: any[]) => any>;
} {
  // Apply prefix to schema
  const prefixedSchema = prefixSchema(schema, prefix);
  
  // Create module context with prefixed schema
  const moduleContext = createModuleContext({
    moduleMetadata,
    tagAttributes: prefixedSchema,
  }) as ModuleContextBuilder<TagAttributeSchema, FF, Env>;
  
  // Wrap operations with task wrappers
  const wrappedOperations: Record<string, (...args: any[]) => any> = {};
  
  if (operations) {
    for (const [opName, opDef] of Object.entries(operations)) {
      // Create task wrapper for operation
      // Note: opDef.fn uses unprefixed schema T, but moduleContext uses prefixed schema.
      // This is safe because the code generator handles the transformation at runtime.
      // The prefixing only affects column names, not the TypeScript types at call sites.
      const taskWrapper = moduleContext.task(
        opDef.spanName, 
        opDef.fn as TaskFunction<any[], any, TagAttributeSchema, FF, Env>
      );
      wrappedOperations[opName] = taskWrapper;
    }
  }
  
  return {
    ...moduleContext,
    operations: wrappedOperations,
  };
}

/**
 * Library factory result type
 */
export interface LibraryFactory<T extends TagAttributeSchema, FF extends FeatureFlagSchema = FeatureFlagSchema, Env = Record<string, unknown>> {
  task: <Args extends unknown[], Result>(
    name: string,
    fn: TaskFunction<Args, Result, T, FF, Env>
  ) => (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result>;
  operations: Record<string, (...args: unknown[]) => Promise<unknown>>;
}

/**
 * Example: HTTP library factory
 * Shows how a library would be structured
 */
export function createHttpLibrary(prefix = 'http'): LibraryFactory<TagAttributeSchema> {
  // Example HTTP schema (library defines clean names using S builder)
  // Note: In real usage, you would call defineTagAttributes() for validation
  const httpSchemaDefinition = {
    status: S.number(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
    url: S.text(),
    duration: S.number(),
  };
  
  const moduleMetadata = {
    gitSha: 'dev',
    filePath: 'http-library',
    moduleName: 'http',
  };
  
  // Operations would be defined here
  const operations = {};
  
  // Pass the raw schema object (without methods)
  return moduleContextFactory(prefix, moduleMetadata, httpSchemaDefinition, operations);
}

/**
 * Example: Database library factory
 */
export function createDatabaseLibrary(prefix = 'db'): LibraryFactory<TagAttributeSchema> {
  // Example DB schema (library defines clean names using S builder)
  // Note: In real usage, you would call defineTagAttributes() for validation
  const dbSchemaDefinition = {
    query: S.text(),
    duration: S.number(),
    table: S.category(),
    operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
  };
  
  const moduleMetadata = {
    gitSha: 'dev',
    filePath: 'db-library',
    moduleName: 'database',
  };
  
  const operations = {};
  
  // Pass the raw schema object (without methods)
  return moduleContextFactory(prefix, moduleMetadata, dbSchemaDefinition, operations);
}
