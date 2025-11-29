/**
 * Library Integration Pattern Example
 * 
 * Demonstrates how to create libraries with LMAO using clean schemas.
 * 
 * Key concepts:
 * - Libraries define clean schemas without prefixes
 * - Each library uses createLibraryModule with flat API
 * - Libraries can define their own operations
 * - The new ergonomic API makes library creation simple
 * 
 * Note: This is a simplified example showing library definition.
 * For full composition patterns with multiple libraries, see the test files.
 */

import { S, defineTagAttributes, createLibraryModule } from '../src/index.js';
import { getSchemaFields } from '../src/lib/schema/types.js';

// ============================================================================
// LIBRARY DEFINITIONS (Third-Party Code)
// ============================================================================

/**
 * HTTP Client Library
 * Demonstrates creating a library with the new ergonomic API
 */
const httpLibrary = createLibraryModule({
  gitSha: 'http-lib-v1.2.3',
  filePath: '@acme/http-client/index.ts',
  schema: defineTagAttributes({
    // Clean field names without prefixes
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
    url: S.text(),
    status: S.number(),
    duration: S.number(),
  }),
  // Note: Operations would be added here in a real library
  // For this example, we're just showing library creation
});

/**
 * Database Library
 * Notice: Both HTTP and DB have 'duration' field - no conflicts when prefixed
 */
const dbLibrary = createLibraryModule({
  gitSha: 'db-lib-v2.0.1',
  filePath: '@acme/database/client.ts',
  schema: defineTagAttributes({
    // Clean field names without prefixes
    operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
    table: S.category(),
    duration: S.number(), // Same name as HTTP library - will be prefixed
    rowsAffected: S.number(),
  }),
});

/**
 * Cache Library  
 * Demonstrates boolean fields and string categories
 */
const cacheLibrary = createLibraryModule({
  gitSha: 'cache-lib-v1.0.0',
  filePath: '@acme/cache/redis.ts',
  schema: defineTagAttributes({
    operation: S.enum(['GET', 'SET', 'DELETE', 'EXISTS']),
    key: S.category(), // Category: keys often repeat
    hit: S.boolean(),
    ttl: S.number(),
  }),
});

// ============================================================================
// DEMONSTRATION
// ============================================================================

console.log('\n📚 Library Integration Pattern Example\n');
console.log('✅ HTTP Library created with schema:');
console.log('   ', getSchemaFields(httpLibrary.schema).map(([name]) => name));

console.log('\n✅ Database Library created with schema:');
console.log('   ', getSchemaFields(dbLibrary.schema).map(([name]) => name));

console.log('\n✅ Cache Library created with schema:');
console.log('   ', getSchemaFields(cacheLibrary.schema).map(([name]) => name));

console.log('\n💡 Key Points:');
console.log('   1. Each library has a clean schema without prefixes');
console.log('   2. Both HTTP and DB have "duration" field - no conflict');
console.log('   3. Libraries use proper string types (enum/category/text)');
console.log('   4. New flat API makes library creation ergonomic');
console.log('   5. Libraries include task method for creating operations\n');

/**
 * For full composition and usage examples, see:
 * - packages/lmao/src/lib/__tests__/library.test.ts
 * - packages/lmao/examples/basic-usage.ts
 * - packages/lmao/examples/chaining-showcase.ts
 */
