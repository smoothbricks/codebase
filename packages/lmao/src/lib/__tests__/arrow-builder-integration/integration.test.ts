import { describe, expect, it } from 'bun:test';
import { createAttributeColumns, createColumnWriter, maskingTransforms } from '@smoothbricks/arrow-builder';
import type { TagAttributeSchema } from '@smoothbricks/lmao';
import {
  convertToArrowTable,
  createSpanBuffer,
  defineTagAttributes,
  ENTRY_TYPE_SPAN_START,
  S,
} from '@smoothbricks/lmao';
import { createTestTaskContext } from '../test-helpers.js';

// TestStringInterner no longer needed - convertToArrowTable now uses direct string access
// via buf.task.module.packageName, buf.task.module.packagePath, and buf.task.spanName

/**
 * Type helper to extract schema fields from ExtendedSchema
 * Removes the validation and extension methods, leaving only Sury schemas
 */
type ExtractSchemaFields<T> = Omit<T, 'validate' | 'parse' | 'safeParse' | 'extend'>;

describe('Buffer Integration', () => {
  it('generates TypedArray columns with proper names for defined schema', () => {
    const schema = defineTagAttributes({
      userId: S.category(), // Category: user IDs repeat
      isActive: S.boolean(),
      score: S.number(),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const capacity = 64;
    const columns = createAttributeColumns(tagAttributes, capacity);

    // Keys - should have  prefix
    expect(columns).toHaveProperty('userId');
    expect(columns).toHaveProperty('isActive');
    expect(columns).toHaveProperty('score');

    // All columns should be TypedArrays
    expect(columns.userId).toBeInstanceOf(Uint32Array); // category
    expect(columns.isActive).toBeInstanceOf(Uint8Array); // boolean
    expect(columns.score).toBeInstanceOf(Float64Array); // number
  });

  it('creates a SpanBuffer with core and attribute TypedArray columns', () => {
    const schema = defineTagAttributes({
      userId: S.category(), // Category: user IDs repeat
      score: S.number(),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const taskContext = createTestTaskContext(tagAttributes);
    const capacity = 64;
    const buf = createSpanBuffer(tagAttributes, taskContext, undefined, capacity);

    // Core TypedArrays exist
    expect(buf.timestamps).toBeInstanceOf(BigInt64Array);
    expect(buf.operations).toBeInstanceOf(Uint8Array);

    // Attribute columns exist with correct types (use _values suffix)
    expect(Array.isArray(buf['userId_values'])).toBe(true); // category (raw strings)
    expect(buf['score_values']).toBeInstanceOf(Float64Array); // number

    // Metadata
    expect(buf.capacity).toBe(capacity);
    expect(buf.writeIndex).toBe(0);
    expect(buf.children).toHaveLength(0);
  });

  it('integrates schema definition with buffer creation', () => {
    // Define schema with defineTagAttributes
    const schema = defineTagAttributes({
      requestId: S.category(), // Category: request IDs repeat
      httpStatus: S.number(),
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']), // Enum: known HTTP methods
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    // Create buffer with schema
    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // Verify all attribute columns created as TypedArrays with correct types (use _values suffix)
    expect(Array.isArray(buffer['requestId_values'])).toBe(true); // category (raw strings)
    expect(buffer['httpStatus_values']).toBeInstanceOf(Float64Array); // number
    expect(buffer['operation_values']).toBeInstanceOf(Uint8Array); // enum

    // Verify task context is set
    expect(buffer.task).toBe(taskContext);
    expect(buffer.task.module.tagAttributes).toBe(tagAttributes);
  });

  it('handles optional fields in schema', () => {
    const schema = defineTagAttributes({
      required: S.category(), // Category string
      optional: S.optional(S.number()),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // Both should have TypedArray columns (use _values suffix)
    expect(Array.isArray(buffer['required_values'])).toBe(true); // category (raw strings)
    // Note: S.optional() wraps the inner schema, losing __schema_type metadata
    // This falls back to Uint32Array (default)
    expect(buffer['optional_values']).toBeInstanceOf(Uint32Array);
  });

  it('handles masked fields in schema', () => {
    const schema = defineTagAttributes({
      userId: S.category().mask('hash'),
      email: S.text().mask('email'),
      plainText: S.text(), // Text: unmasked plain text
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // All should have TypedArray columns (use _values suffix)
    // Masking is applied during Arrow conversion (cold path), not buffer creation
    // With chainable .mask(), the __schema_type metadata is preserved
    expect(Array.isArray(buffer['userId_values'])).toBe(true); // category (raw strings) - masked during Arrow conversion
    expect(Array.isArray(buffer['email_values'])).toBe(true); // text (raw strings) - masked during Arrow conversion
    expect(Array.isArray(buffer['plainText_values'])).toBe(true); // text (raw strings)
  });

  it('selects correct enum TypedArray size based on value count', () => {
    // Small enum (<256 values) should use Uint8Array
    const smallEnumSchema = defineTagAttributes({
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']), // 4 values → Uint8Array
    });

    const { validate: v1, parse: p1, safeParse: s1, extend: e1, ...smallFields } = smallEnumSchema;
    const smallAttrs = smallFields as ExtractSchemaFields<typeof smallEnumSchema> & TagAttributeSchema;

    const smallContext = createTestTaskContext(smallAttrs);
    const smallBuffer = createSpanBuffer(smallAttrs, smallContext);

    // Should use Uint8Array for enums with ≤255 values (use _values suffix)
    expect(smallBuffer['operation_values']).toBeInstanceOf(Uint8Array);
  });

  it('applies masking during Arrow conversion for category fields', () => {
    const schema = defineTagAttributes({
      userId: S.category().mask('hash'),
      plainUserId: S.category(), // No masking
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    // Create interners
    // const moduleIdInterner = new TestStringInterner();
    // const spanNameInterner = new TestStringInterner();
    // moduleIdInterner.intern('test-module');
    // spanNameInterner.intern('test-span');

    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // Use ColumnWriter fluent API to write values
    const writer = createColumnWriter(tagAttributes, buffer);
    // biome-ignore lint/suspicious/noExplicitAny: testing dynamic methods
    (writer as any).nextRow().userId('user-12345').plainUserId('user-12345');

    // Set required system columns
    buffer.timestamps[0] = 1000n;
    buffer.operations[0] = ENTRY_TYPE_SPAN_START;
    buffer.writeIndex = 1;

    // Convert to Arrow table
    const table = convertToArrowTable(buffer);

    // Get the userId column (should be masked)
    const userIdCol = table.getChild('userId');
    expect(userIdCol).toBeDefined();

    // Get the plainUserId column (should NOT be masked)
    const plainUserIdCol = table.getChild('plainUserId');
    expect(plainUserIdCol).toBeDefined();

    // Verify the masked value matches the expected hash
    const maskedValue = userIdCol?.get(0);
    const expectedHash = maskingTransforms.hash('user-12345');
    expect(maskedValue).toBe(expectedHash);

    // Verify the unmasked value is unchanged
    const unmaskedValue = plainUserIdCol?.get(0);
    expect(unmaskedValue).toBe('user-12345');
  });

  it('applies masking during Arrow conversion for text fields', () => {
    const schema = defineTagAttributes({
      email: S.text().mask('email'),
      sqlQuery: S.text().mask('sql'),
      plainText: S.text(), // No masking
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    // Create interners
    // const moduleIdInterner = new TestStringInterner();
    // const spanNameInterner = new TestStringInterner();
    // moduleIdInterner.intern('test-module');
    // spanNameInterner.intern('test-span');

    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // Use ColumnWriter fluent API to write values
    const writer = createColumnWriter(tagAttributes, buffer);
    // biome-ignore lint/suspicious/noExplicitAny: testing dynamic methods
    (writer as any)
      .nextRow()
      .email('john@example.com')
      .sqlQuery("SELECT * FROM users WHERE id = 123 AND name = 'test'")
      .plainText('Plain unmasked text');

    // Set required system columns
    buffer.timestamps[0] = 1000n;
    buffer.operations[0] = ENTRY_TYPE_SPAN_START;
    buffer.writeIndex = 1;

    // Convert to Arrow table
    const table = convertToArrowTable(buffer);

    // Get columns
    const emailCol = table.getChild('email');
    const sqlQueryCol = table.getChild('sqlQuery');
    const plainTextCol = table.getChild('plainText');

    // Verify email masking
    const maskedEmail = emailCol?.get(0);
    expect(maskedEmail).toBe(maskingTransforms.email('john@example.com'));
    expect(maskedEmail).toMatch(/^j\*\*\*\*\*@example\.com$/);

    // Verify SQL masking
    const maskedSql = sqlQueryCol?.get(0);
    expect(maskedSql).toBe(maskingTransforms.sql("SELECT * FROM users WHERE id = 123 AND name = 'test'"));
    expect(maskedSql).toBe('SELECT * FROM users WHERE id = ? AND name = ?');

    // Verify plain text is unchanged
    const plainValue = plainTextCol?.get(0);
    expect(plainValue).toBe('Plain unmasked text');
  });

  it('applies custom mask function during Arrow conversion', () => {
    // Custom mask function that keeps first 4 chars and replaces rest with *
    const customMask = (value: string) => value.slice(0, 4) + '*'.repeat(Math.max(0, value.length - 4));

    const schema = defineTagAttributes({
      secretKey: S.text().mask(customMask),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    // Create interners
    // const moduleIdInterner = new TestStringInterner();
    // const spanNameInterner = new TestStringInterner();
    // moduleIdInterner.intern('test-module');
    // spanNameInterner.intern('test-span');

    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // Use ColumnWriter fluent API to write value
    const writer = createColumnWriter(tagAttributes, buffer);
    // biome-ignore lint/suspicious/noExplicitAny: testing dynamic methods
    (writer as any).nextRow().secretKey('sk_live_abcd1234efgh5678');

    // Set required system columns
    buffer.timestamps[0] = 1000n;
    buffer.operations[0] = ENTRY_TYPE_SPAN_START;
    buffer.writeIndex = 1;

    // Convert to Arrow table
    const table = convertToArrowTable(buffer);

    // Get the column
    const secretKeyCol = table.getChild('secretKey');

    // Verify custom masking was applied
    // Input is 24 chars, first 4 are kept, remaining 20 are replaced with *
    const maskedValue = secretKeyCol?.get(0);
    expect(maskedValue).toBe('sk_l********************');
  });
});
