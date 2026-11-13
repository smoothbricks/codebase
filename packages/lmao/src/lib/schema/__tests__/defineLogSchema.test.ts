/**
 * Tests for Sury-based schema definition system
 *
 * Validates:
 * - Schema creation with Sury
 * - Runtime validation
 * - Masking transformations
 * - Schema extension
 * - Reserved names: refused where a tag context is built (defineOpContext), accepted by the schema itself
 */

import { describe, expect, test } from 'bun:test';
import { getMaskTransform } from '@smoothbricks/arrow-builder';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../builder.js';
import { defineLogSchema } from '../defineLogSchema.js';
import { extendSchema } from '../extend.js';
import { LogSchema } from '../LogSchema.js';

//#region smoo/lmao!n/schema-log-schema
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
    }).toThrow('Field "count": expected number, got "not a number"');
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
    }).toThrow('Field "operation": expected one of [SELECT, INSERT], got "INVALID"');
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

    // The mask is a field hint, no part of the type: it is read where the column is written out (Arrow conversion).
    for (const field of [schema.fields.userId, schema.fields.email, schema.fields.apiUrl, schema.fields.query]) {
      expect(typeof getMaskTransform(field)).toBe('function');
    }
    expect(getMaskTransform(schema.fields.email)?.('john@example.com')).toBe('j*****@example.com');
    expect(getMaskTransform(S.category())).toBeUndefined();
  });

  //#region smoo/lmao!n/schema-fields
  test('a field name starting with _ is a legal schema field and is refused by defineOpContext', () => {
    const logSchema = defineLogSchema({ _reserved: S.text() });

    expect(logSchema._columnNames).toContain('_reserved');
    expect(() => defineOpContext({ logSchema })).toThrow("Field name '_reserved' cannot start with '_'");
  });

  test.each([
    'with',
    'message',
    'tag',
    'success',
    'value',
    'error',
    'map',
    'mapErr',
    'flatMap',
    'match',
    'isOk',
    'isErr',
    'maybeValue',
    'maybeError',
    'unwrapOr',
    'unwrapOrElse',
    'toJSON',
    'line',
    'then',
    'constructor',
    'toString',
    'toLocaleString',
    'valueOf',
    'hasOwnProperty',
    'isPrototypeOf',
    'propertyIsEnumerable',
  ])('the tag-member name %s is a legal schema field and is refused by defineOpContext', (name) => {
    const fields = { [name]: S.text() };
    const logSchema = defineLogSchema(fields);

    // A schema nothing tags through may use the name: defining and extending it never refuses.
    expect(logSchema._columnNames).toContain(name);
    expect(new LogSchema({ requestId: S.category() }).extend(fields)._columnNames).toContain(name);
    expect(Object.keys(extendSchema({ requestId: S.category() }, fields))).toContain(name);

    // The op context builds the setters that the name would shadow, so it refuses with the reserved-name message.
    expect(() => defineOpContext({ logSchema })).toThrow(`Attribute name '${name}' is reserved and cannot be used.`);
  });

  test('defineOpContext accepts field names that no tag-context member shadows', () => {
    const logSchema = defineLogSchema({ requestId: S.category(), userId: S.category() });

    expect(() => defineOpContext({ logSchema })).not.toThrow();
  });
  //#endregion smoo/lmao!n/schema-fields

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

  // =========================================================================
  // Regression tests for Sury removal (eb6fbce2)
  // Validates that schema validation semantics were not weakened
  // =========================================================================

  test('a missing or null required value is refused naming the field and its type', () => {
    const schema = defineLogSchema({ count: S.number() });

    expect(() => schema.validate({})).toThrow('Field "count": expected f64(), got undefined');
    expect(() => schema.validate({ count: null })).toThrow('Field "count": expected f64(), got null');
  });
  test('optional(enum(...)) rejects invalid inner values', () => {
    const schema = defineLogSchema({
      status: S.optional(S.enum(['active', 'inactive'])),
    });

    // Valid: undefined
    expect(schema.validate({}).status).toBeUndefined();

    // Valid: correct enum value
    expect(schema.validate({ status: 'active' }).status).toBe('active');

    // Invalid: wrong enum value — must throw, not silently accept
    expect(() => {
      schema.validate({ status: 'bogus' });
    }).toThrow('Field "status": expected one of [active, inactive], got "bogus"');
  });

  test('optional(number()) rejects wrong type when present', () => {
    const schema = defineLogSchema({
      count: S.optional(S.number()),
    });

    // Valid: undefined
    expect(schema.validate({}).count).toBeUndefined();

    // Valid: number
    expect(schema.validate({ count: 5 }).count).toBe(5);

    // Invalid: string when number expected
    expect(() => {
      schema.validate({ count: 'five' });
    }).toThrow('Field "count": expected number, got "five"');
  });
});
//#endregion smoo/lmao!n/schema-log-schema
