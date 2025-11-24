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

import { describe, test, expect } from 'bun:test';
import { defineTagAttributes, validateAttributeNames } from '../defineTagAttributes.js';
import { extendSchema, createExtendedSchema } from '../extend.js';
import { S } from '../builder.js';

describe('defineTagAttributes with Sury', () => {
  test('defines base attributes with Sury schemas', () => {
    const attrs = defineTagAttributes({
      requestId: S.category(),
      userId: S.optional(S.masked('hash')),
      timestamp: S.number(),
    });
    
    expect(attrs).toBeDefined();
    expect(attrs.validate).toBeDefined();
    expect(attrs.parse).toBeDefined();
    expect(attrs.safeParse).toBeDefined();
  });
  
  test('validates data correctly with Sury', () => {
    const attrs = defineTagAttributes({
      requestId: S.category(),
      count: S.number(),
    });
    
    const result = attrs.validate({
      requestId: 'req-123',
      count: 42
    });
    
    expect(result.requestId).toBe('req-123');
    expect(result.count).toBe(42);
  });
  
  test('throws on invalid data', () => {
    const attrs = defineTagAttributes({
      count: S.number(),
    });
    
    expect(() => {
      attrs.validate({ count: 'not a number' });
    }).toThrow();
  });
  
  test('parse returns null on error', () => {
    const attrs = defineTagAttributes({
      count: S.number(),
    });
    
    const result = attrs.parse({ count: 'invalid' });
    expect(result).toBeNull();
  });
  
  test('safeParse returns detailed result', () => {
    const attrs = defineTagAttributes({
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
    const base = defineTagAttributes({
      requestId: S.category()
    });

    const extended = base.extend({ duration: S.number() });
    expect(extended).toHaveProperty('requestId');
    expect(extended).toHaveProperty('duration');
  });
  
  test('validates enum types with Sury', () => {
    const schema = defineTagAttributes({
      operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
    });
    
    const result = schema.validate({ operation: 'SELECT' });
    expect(result.operation).toBe('SELECT');
  });
  
  test('rejects invalid enum values', () => {
    const schema = defineTagAttributes({
      operation: S.enum(['SELECT', 'INSERT']),
    });
    
    expect(() => {
      schema.validate({ operation: 'INVALID' });
    }).toThrow();
  });
  
  test('handles optional fields correctly', () => {
    const schema = defineTagAttributes({
      required: S.category(),
      optional: S.optional(S.number()),
    });
    
    // With optional field
    const result1 = schema.validate({
      required: 'test',
      optional: 42
    });
    expect(result1.required).toBe('test');
    expect(result1.optional).toBe(42);
    
    // Without optional field
    const result2 = schema.validate({
      required: 'test'
    });
    expect(result2.required).toBe('test');
    expect(result2.optional).toBeUndefined();
  });
  
  test('masking transformations work', () => {
    const schema = defineTagAttributes({
      userId: S.masked('hash'),
      email: S.masked('email'),
      apiUrl: S.masked('url'),
      query: S.masked('sql'),
    });
    
    const result = schema.validate({
      userId: 'user-12345',
      email: 'john@example.com',
      apiUrl: 'https://api.example.com/users',
      query: "SELECT * FROM users WHERE id = 123 AND name = 'test'"
    });
    
    // Hash masking creates hex hash
    expect(result.userId).toMatch(/^0x[0-9a-f]{16}$/);
    
    // Email masking shows first char and domain
    expect(result.email).toMatch(/^j\*\*\*\*\*@example\.com$/);
    
    // URL masking hides domain
    expect(result.apiUrl).toBe('https://*****/users');
    
    // SQL masking replaces literals with ?
    expect(result.query).toBe('SELECT * FROM users WHERE id = ? AND name = ?');
  });
  
  test('rejects reserved attribute names', () => {
    expect(() => {
      validateAttributeNames({
        with: S.category(), // Reserved!
      });
    }).toThrow(/reserved/i);
    
    expect(() => {
      validateAttributeNames({
        message: S.category(), // Reserved!
      });
    }).toThrow(/reserved/i);
    
    expect(() => {
      validateAttributeNames({
        tag: S.category(), // Reserved!
      });
    }).toThrow(/reserved/i);
  });
  
  test('allows non-reserved names', () => {
    expect(() => {
      validateAttributeNames({
        requestId: S.category(),
        userId: S.category(),
        customField: S.number(),
      });
    }).not.toThrow();
  });
  
  test('fluent extend API works', () => {
    const base = createExtendedSchema({
      requestId: S.category(),
    });
    
    const extended = base.extend({
      httpStatus: S.number(),
    });
    
    expect(extended).toHaveProperty('requestId');
    expect(extended).toHaveProperty('httpStatus');
  });
  
  test('chained extensions work', () => {
    const base = createExtendedSchema({
      requestId: S.category(),
    });
    
    const withHttp = base.extend({
      httpStatus: S.number(),
    });
    
    const withDb = withHttp.extend({
      dbQuery: S.masked('sql'),
    });
    
    expect(withDb).toHaveProperty('requestId');
    expect(withDb).toHaveProperty('httpStatus');
    expect(withDb).toHaveProperty('dbQuery');
  });
  
  test('extendSchema detects conflicts', () => {
    const base = { requestId: S.category() };
    const extension = { requestId: S.number() }; // Conflict!
    
    expect(() => {
      extendSchema(base, extension);
    }).toThrow(/conflict/i);
  });
  
  test('union schemas work', () => {
    const schema = defineTagAttributes({
      value: S.union([S.category(), S.number()]),
    });
    
    const result1 = schema.validate({ value: 'test' });
    expect(result1.value).toBe('test');
    
    const result2 = schema.validate({ value: 42 });
    expect(result2.value).toBe(42);
  });
  
  test('complex nested schema validation', () => {
    const schema = defineTagAttributes({
      requestId: S.category(),
      userId: S.optional(S.masked('hash')),
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
    expect(result.userId).toMatch(/^0x[0-9a-f]{16}$/);
    expect(result.httpStatus).toBe(201);
    expect(result.operation).toBe('CREATE');
    expect(result.metadata).toBeUndefined();
  });
});

