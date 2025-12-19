/**
 * Tests for Sury-based schema definition system
 *
 * Validates:
 * - Schema creation with Sury
 * - Runtime validation
 * - Masking transformations
 * - Schema extension
 * - Reserved name validation
 */

import { describe, expect, test } from 'bun:test';
import { S } from '../builder.js';
import { defineLogSchema } from '../defineLogSchema.js';
import { extendSchema } from '../extend.js';
import { LogSchema } from '../LogSchema.js';

describe('defineLogSchema with Sury', () => {
  test('defines base attributes with Sury schemas', () => {
    const attrs = defineLogSchema({
      requestId: S.category(),
      userId: S.category().mask('hash'), // Masking applied during Arrow conversion, not validation
      timestamp: S.number(),
    });

    expect(attrs).toBeDefined();
    expect(attrs.validate).toBeDefined();
    expect(attrs.parse).toBeDefined();
    expect(attrs.safeParse).toBeDefined();
  });

  test('validates data correctly with Sury', () => {
    const attrs = defineLogSchema({
      requestId: S.category(),
      count: S.number(),
    });

    const result = attrs.validate({
      requestId: 'req-123',
      count: 42,
    });

    expect(result.requestId).toBe('req-123');
    expect(result.count).toBe(42);
  });

  test('throws on invalid data', () => {
    const attrs = defineLogSchema({
      count: S.number(),
    });

    expect(() => {
      attrs.validate({ count: 'not a number' });
    }).toThrow();
  });

  test('parse returns null on error', () => {
    const attrs = defineLogSchema({
      count: S.number(),
    });

    const result = attrs.parse({ count: 'invalid' });
    expect(result).toBeNull();
  });

  test('safeParse returns detailed result', () => {
    const attrs = defineLogSchema({
      count: S.number(),
    });

    // Valid data
    const validResult = attrs.safeParse({ count: 42 });
    expect(validResult.success).toBe(true);
    if (validResult.success) {
      expect(validResult.value.count).toBe(42);
    }

    // Invalid data
    const invalidResult = attrs.safeParse({ count: 'invalid' });
    expect(invalidResult.success).toBe(false);
    if (!invalidResult.success) {
      expect(invalidResult.error).toBeDefined();
    }
  });

  test('supports schema extension', () => {
    const base = defineLogSchema({
      requestId: S.category(),
    });

    const extended = base.extend({ duration: S.number() });
    // extend() returns a new LogSchema instance, fields are in .fields
    expect(extended.fields).toHaveProperty('requestId');
    expect(extended.fields).toHaveProperty('duration');
  });

  test('validates enum types with Sury', () => {
    const schema = defineLogSchema({
      operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
    });

    const result = schema.validate({ operation: 'SELECT' });
    expect(result.operation).toBe('SELECT');
  });

  test('rejects invalid enum values', () => {
    const schema = defineLogSchema({
      operation: S.enum(['SELECT', 'INSERT']),
    });

    expect(() => {
      schema.validate({ operation: 'INVALID' });
    }).toThrow();
  });

  test('handles optional fields correctly', () => {
    const schema = defineLogSchema({
      required: S.category(),
      optional: S.optional(S.number()),
    });

    // With optional field
    const result1 = schema.validate({
      required: 'test',
      optional: 42,
    });
    expect(result1.required).toBe('test');
    expect(result1.optional).toBe(42);

    // Without optional field
    const result2 = schema.validate({
      required: 'test',
    });
    expect(result2.required).toBe('test');
    expect(result2.optional).toBeUndefined();
  });

  test('masking metadata is set on schema', () => {
    // Masking is now applied during Arrow conversion, not validation
    // This test verifies the mask metadata is correctly attached to schemas
    const schema = defineLogSchema({
      userId: S.category().mask('hash'),
      email: S.text().mask('email'),
      apiUrl: S.text().mask('url'),
      query: S.text().mask('sql'),
    });

    // Validation should pass through values unchanged (masking happens at Arrow conversion)
    const result = schema.validate({
      userId: 'user-12345',
      email: 'john@example.com',
      apiUrl: 'https://api.example.com/users',
      query: "SELECT * FROM users WHERE id = 123 AND name = 'test'",
    });

    // Values should be unchanged at validation time
    expect(result.userId).toBe('user-12345');
    expect(result.email).toBe('john@example.com');
    expect(result.apiUrl).toBe('https://api.example.com/users');
    expect(result.query).toBe("SELECT * FROM users WHERE id = 123 AND name = 'test'");

    // Verify mask metadata exists on schemas (will be used during Arrow conversion)
    // Fields are accessed via schema.fields
    expect((schema.fields.userId as { __mask_transform?: unknown }).__mask_transform).toBeDefined();
    expect((schema.fields.email as { __mask_transform?: unknown }).__mask_transform).toBeDefined();
    expect((schema.fields.apiUrl as { __mask_transform?: unknown }).__mask_transform).toBeDefined();
    expect((schema.fields.query as { __mask_transform?: unknown }).__mask_transform).toBeDefined();
  });

  test('should reject field names starting with _', () => {
    expect(() =>
      defineLogSchema({
        _reserved: S.text(),
      }),
    ).toThrow("Field name '_reserved' cannot start with '_'");
  });

  test('rejects reserved attribute names', () => {
    expect(() => {
      LogSchema.assertUserFieldNames(['with']); // Reserved!
    }).toThrow(/reserved/i);

    expect(() => {
      LogSchema.assertUserFieldNames(['message']); // Reserved!
    }).toThrow(/reserved/i);

    expect(() => {
      LogSchema.assertUserFieldNames(['tag']); // Reserved!
    }).toThrow(/reserved/i);
  });

  test('allows non-reserved names', () => {
    expect(() => {
      LogSchema.assertUserFieldNames(['requestId', 'userId']);
    }).not.toThrow();
  });

  test('fluent extend API works', () => {
    const base = new LogSchema({
      requestId: S.category(),
    });

    const extended = base.extend({
      httpStatus: S.number(),
    });

    expect(extended.fields).toHaveProperty('requestId');
    expect(extended.fields).toHaveProperty('httpStatus');
  });

  test('chained extensions work', () => {
    const base = new LogSchema({
      requestId: S.category(),
    });

    const withHttp = base.extend({
      httpStatus: S.number(),
    });

    const withDb = withHttp.extend({
      dbQuery: S.text().mask('sql'),
    });

    expect(withDb.fields).toHaveProperty('requestId');
    expect(withDb.fields).toHaveProperty('httpStatus');
    expect(withDb.fields).toHaveProperty('dbQuery');
  });

  test('extendSchema detects conflicts', () => {
    const base = { requestId: S.category() };
    const extension = { requestId: S.number() }; // Conflict!

    expect(() => {
      extendSchema(base, extension);
    }).toThrow(/conflict/i);
  });

  test('union schemas work', () => {
    const schema = defineLogSchema({
      value: S.union([S.category(), S.number()]),
    });

    const result1 = schema.validate({ value: 'test' });
    expect(result1.value).toBe('test');

    const result2 = schema.validate({ value: 42 });
    expect(result2.value).toBe(42);
  });

  test('complex nested schema validation', () => {
    const schema = defineLogSchema({
      requestId: S.category(),
      userId: S.optional(S.category()), // Masking would be: S.category().mask('hash')
      httpStatus: S.number(),
      operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
      metadata: S.optional(S.category()),
    });

    const result = schema.validate({
      requestId: 'req-abc-123',
      userId: 'user-456',
      httpStatus: 201,
      operation: 'CREATE',
    });

    expect(result.requestId).toBe('req-abc-123');
    expect(result.userId).toBe('user-456'); // No masking at validation time
    expect(result.httpStatus).toBe(201);
    expect(result.operation).toBe('CREATE');
    expect(result.metadata).toBeUndefined();
  });
});
