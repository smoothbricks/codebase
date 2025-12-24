import { describe, expect, it } from 'bun:test';
import {
  createAttributeColumns,
  createColumnWriter,
  DEFAULT_BUFFER_CAPACITY,
  maskingTransforms,
} from '@smoothbricks/arrow-builder';
import { convertToArrowTable, createSpanBuffer, ENTRY_TYPE_SPAN_START, S } from '@smoothbricks/lmao';
import { ENTRY_TYPE_INFO } from '../../schema/systemSchema.js';
import { createTestLogBinding, createTestSchema } from '../test-helpers.js';

describe('Buffer Integration', () => {
  it('generates TypedArray columns with proper names for defined schema', () => {
    const schema = createTestSchema({
      userId: S.category(), // Category: user IDs repeat
      isActive: S.boolean(),
      score: S.number(),
    });

    const capacity = 64;
    // createAttributeColumns expects ColumnSchema instance (schema extends LogSchema extends ColumnSchema)
    const columns = createAttributeColumns(schema, capacity);

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
    const schema = createTestSchema({
      userId: S.category(), // Category: user IDs repeat
      score: S.number(),
    });

    const module = createTestLogBinding(schema);
    const capacity = 64;
    const buf = createSpanBuffer(schema, module, 'test-span', undefined, capacity);

    // Core TypedArrays exist
    expect(buf.timestamp).toBeInstanceOf(BigInt64Array);
    expect(buf.entry_type).toBeInstanceOf(Uint8Array);

    // Attribute columns exist with correct types (use _values suffix)
    expect(Array.isArray(buf.userId_values)).toBe(true); // category (raw strings)
    expect(buf.score_values).toBeInstanceOf(Float64Array); // number

    // Metadata
    expect(buf._capacity).toBe(capacity);
    expect(buf._writeIndex).toBe(0);
    expect(buf._children).toHaveLength(0);
  });

  it('integrates schema definition with buffer creation', () => {
    // Define schema with defineLogSchema
    const schema = createTestSchema({
      requestId: S.category(), // Category: request IDs repeat
      httpStatus: S.number(),
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']), // Enum: known HTTP methods
    });

    // Create buffer with schema
    const module = createTestLogBinding(schema);
    const buffer = createSpanBuffer(schema, module, 'test-span');

    // Verify all attribute columns created as TypedArrays with correct types (use _values suffix)
    expect(Array.isArray(buffer.requestId_values)).toBe(true); // category (raw strings)
    expect(buffer.httpStatus_values).toBeInstanceOf(Float64Array); // number
    expect(buffer.operation_values).toBeInstanceOf(Uint8Array); // enum

    // Verify module is set
    expect(buffer._logBinding).toBe(module);
    expect(buffer._logBinding.logSchema.fields).toBe(schema.fields);
  });

  it('handles optional fields in schema', () => {
    const schema = createTestSchema({
      required: S.category(), // Category string
      optional: S.optional(S.number()),
    });

    const module = createTestLogBinding(schema);
    const buffer = createSpanBuffer(schema, module, 'test-span');

    // Both should have TypedArray columns (use _values suffix)
    expect(Array.isArray(buffer.required_values)).toBe(true); // category (raw strings)
    // Note: S.optional() wraps the inner schema, losing __schema_type metadata
    // This falls back to Uint32Array (default)
    expect(buffer.optional_values).toBeInstanceOf(Uint32Array);
  });

  it('handles masked fields in schema', () => {
    const schema = createTestSchema({
      userId: S.category().mask('hash'),
      email: S.text().mask('email'),
      plainText: S.text(), // Text: unmasked plain text
    });

    const module = createTestLogBinding(schema);
    const buffer = createSpanBuffer(schema, module, 'test-span');

    // All should have TypedArray columns (use _values suffix)
    // Masking is applied during Arrow conversion (cold path), not buffer creation
    // With chainable .mask(), the __schema_type metadata is preserved
    expect(Array.isArray(buffer.userId_values)).toBe(true); // category (raw strings) - masked during Arrow conversion
    expect(Array.isArray(buffer.email_values)).toBe(true); // text (raw strings) - masked during Arrow conversion
    expect(Array.isArray(buffer.plainText_values)).toBe(true); // text (raw strings)
  });

  it('selects correct enum TypedArray size based on value count', () => {
    // Small enum (<256 values) should use Uint8Array
    const smallEnumSchema = createTestSchema({
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']), // 4 values → Uint8Array
    });

    const smallModule = createTestLogBinding(smallEnumSchema);
    const smallBuffer = createSpanBuffer(smallEnumSchema, smallModule, 'test-span');

    // Should use Uint8Array for enums with ≤255 values (use _values suffix)
    expect(smallBuffer.operation_values).toBeInstanceOf(Uint8Array);
  });

  it('applies masking during Arrow conversion for category fields', () => {
    const schema = createTestSchema({
      userId: S.category().mask('hash'),
      plainUserId: S.category(), // No masking
    });

    const module = createTestLogBinding(schema);
    const buffer = createSpanBuffer(schema, module, 'test-span');

    // Use ColumnWriter fluent API to write values (createColumnWriter expects ColumnSchema instance)
    const writer = createColumnWriter(schema, buffer);
    // Dynamic fluent API - schema methods are generated at runtime and can't be typed statically
    (writer._nextRow() as any).userId('user-12345').plainUserId('user-12345');

    // Set required system columns
    buffer.timestamp[0] = 1000n;
    buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
    buffer._writeIndex = 1;

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
    const schema = createTestSchema({
      email: S.text().mask('email'),
      sqlQuery: S.text().mask('sql'),
      plainText: S.text(), // No masking
    });

    const module = createTestLogBinding(schema);
    const buffer = createSpanBuffer(schema, module, 'test-span');

    // Use ColumnWriter fluent API to write values (createColumnWriter expects ColumnSchema instance)
    const writer = createColumnWriter(schema, buffer);
    (writer._nextRow() as any)
      .email('john@example.com')
      .sqlQuery("SELECT * FROM users WHERE id = 123 AND name = 'test'")
      .plainText('Plain unmasked text');

    // Set required system columns
    buffer.timestamp[0] = 1000n;
    buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
    buffer._writeIndex = 1;

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

    const schema = createTestSchema({
      secretKey: S.text().mask(customMask),
    });

    const module = createTestLogBinding(schema);
    const buffer = createSpanBuffer(schema, module, 'test-span');

    // Use ColumnWriter fluent API to write value (createColumnWriter expects ColumnSchema instance)
    const writer = createColumnWriter(schema, buffer);
    (writer._nextRow() as any).secretKey('sk_live_abcd1234efgh5678');

    // Set required system columns
    buffer.timestamp[0] = 1000n;
    buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
    buffer._writeIndex = 1;

    // Convert to Arrow table
    const table = convertToArrowTable(buffer);

    // Get the column
    const secretKeyCol = table.getChild('secretKey');

    // Verify custom masking was applied
    // Input is 24 chars, first 4 are kept, remaining 20 are replaced with *
    const maskedValue = secretKeyCol?.get(0);
    expect(maskedValue).toBe('sk_l********************');
  });

  describe('Cross-Package Integration', () => {
    it('validates lmao schema creates correct arrow-builder TypedArray types', () => {
      const schema = createTestSchema({
        enumField: S.enum(['A', 'B', 'C']), // Maps to Uint8Array
        categoryField: S.category(), // Maps to string[]
        textField: S.text(), // Maps to string[]
        numberField: S.number(), // Maps to Float64Array
        booleanField: S.boolean(), // Maps to Uint8Array
        optionalField: S.optional(S.number()), // Maps to Uint32Array
      });

      const module = createTestLogBinding(schema);
      const buffer = createSpanBuffer(schema, module, 'integration-test');

      // Verify arrow-builder created correct TypedArray types for each schema type
      expect(buffer.enumField_values).toBeInstanceOf(Uint8Array);
      expect(Array.isArray(buffer.categoryField_values)).toBe(true);
      expect(Array.isArray(buffer.textField_values)).toBe(true);
      expect(buffer.numberField_values).toBeInstanceOf(Float64Array);
      expect(buffer.booleanField_values).toBeInstanceOf(Uint8Array);
      expect(buffer.optionalField_values).toBeInstanceOf(Uint32Array);

      // Verify Arrow null bitmaps exist for all columns
      expect(buffer.enumField_nulls).toBeInstanceOf(Uint8Array);
      expect(buffer.categoryField_nulls).toBeInstanceOf(Uint8Array);
      expect(buffer.textField_nulls).toBeInstanceOf(Uint8Array);
      expect(buffer.numberField_nulls).toBeInstanceOf(Uint8Array);
      expect(buffer.booleanField_nulls).toBeInstanceOf(Uint8Array);
      expect(buffer.optionalField_nulls).toBeInstanceOf(Uint8Array);
    });

    it('ensures data flows correctly between lmao APIs and arrow-builder buffers', () => {
      const schema = createTestSchema({
        userId: S.category(),
        operation: S.enum(['GET', 'POST']),
        httpStatus: S.number(),
        error: S.text().mask('email'),
      });

      const module = createTestLogBinding(schema);
      const buffer = createSpanBuffer(schema, module, 'data-flow-test');

      // Use ColumnWriter API to write data (more robust than direct array access)
      const writer = createColumnWriter(schema, buffer);
      (writer._nextRow() as any)
        .userId('user-123')
        .operation(1) // Use numeric value for POST (index 1 in ['GET', 'POST'])
        .httpStatus(200)
        .error('john@example.com');

      // Set system columns
      buffer.timestamp[0] = 1000n;
      buffer.entry_type[0] = ENTRY_TYPE_INFO;
      buffer._writeIndex = 1;

      // Convert to Arrow using arrow-builder
      const table = convertToArrowTable(buffer as any);

      // Verify Arrow conversion preserves data and applies masking
      expect(table.numRows).toBe(1);
      expect(table.getChild('userId')?.get(0)).toBe('user-123');
      expect(table.getChild('operation')?.get(0)).toBe('POST');
      expect(table.getChild('httpStatus')?.get(0)).toBe(200);
      expect(table.getChild('error')?.get(0)).toBe('j*****@example.com'); // Email masked
    });

    it('validates module context integration with buffer metadata', () => {
      const schema = createTestSchema({
        requestId: S.category(),
      });

      const module = createTestLogBinding(schema, {
        git_sha: 'test-sha-123',
        package_name: '@test/package',
        package_file: 'src/integration.test.ts',
      });

      const buffer = createSpanBuffer(schema, module, 'context-integration');

      // Verify buffer properly references module context
      expect(buffer._logBinding).toBe(module);
      expect(buffer._logBinding.logSchema).toBe(module.logSchema);
      expect(buffer._spanName).toBe('context-integration');

      // Verify system metadata columns are accessible
      expect(buffer.span_id).toBeGreaterThan(0);
      expect(typeof buffer._hasParent).toBe('boolean');
      expect(buffer._children).toBeInstanceOf(Array);
      expect(buffer._writeIndex).toBe(0);
      expect(buffer._capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });
  });
});
