/**
 * Tests for Scope class generation
 *
 * Per user requirements:
 * - Scope is a SEPARATE generated class from column storage
 * - Only contains schema attributes (NOT system columns)
 * - Initialized with undefined for all values
 * - Has _getScopeValues() method for pre-filling child spans
 */

import { describe, expect, test } from 'bun:test';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import type { SchemaFields } from '../../schema/types.js';
import {
  createScope,
  createScopeWithInheritance,
  generateScopeClass,
  generateScopeClassCode,
} from '../scopeGenerator.js';

describe('Scope Class Generation', () => {
  describe('Basic Scope Creation', () => {
    test('should generate a Scope class from schema', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        count: S.number(),
      });

      const ScopeClass = generateScopeClass(schema);
      expect(ScopeClass).toBeDefined();
      expect(typeof ScopeClass).toBe('function');
    });

    test('should create Scope instance with all fields initialized to undefined', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        count: S.number(),
        isValid: S.boolean(),
      });

      const scope = createScope(schema);

      // All fields should be undefined initially
      expect(scope.userId).toBeUndefined();
      expect(scope.requestId).toBeUndefined();
      expect(scope.count).toBeUndefined();
      expect(scope.isValid).toBeUndefined();
    });

    test('should allow setting and getting values', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        count: S.number(),
      });

      const scope = createScope(schema);

      // Set values
      scope.userId = 'user123';
      scope.requestId = 'req456';
      scope.count = 42;

      // Get values
      expect(scope.userId).toBe('user123');
      expect(scope.requestId).toBe('req456');
      expect(scope.count).toBe(42);
    });

    test('should have _getScopeValues() method', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        count: S.number(),
      });

      const scope = createScope(schema);
      scope.userId = 'user123';
      scope.count = 42;

      const values = scope._getScopeValues();

      expect(values).toEqual({
        userId: 'user123',
        requestId: undefined,
        count: 42,
      });
    });
  });

  describe('Scope Inheritance', () => {
    test('should create Scope with inherited values', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        operation: S.category(),
      });

      const parentScope = createScope(schema);
      parentScope.userId = 'user123';
      parentScope.requestId = 'req456';

      // Create child scope with inherited values
      const childScope = createScopeWithInheritance(schema, parentScope._getScopeValues());

      expect(childScope.userId).toBe('user123');
      expect(childScope.requestId).toBe('req456');
      expect(childScope.operation).toBeUndefined();
    });

    test('should allow child to override inherited values', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        level: S.number(),
      });

      const parentScope = createScope(schema);
      parentScope.userId = 'user123';
      parentScope.level = 1;

      const childScope = createScopeWithInheritance(schema, parentScope._getScopeValues());

      // Child can override parent values
      childScope.level = 2;

      expect(parentScope.level).toBe(1);
      expect(childScope.level).toBe(2);
    });

    test('should not copy undefined values during inheritance', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        count: S.number(),
      });

      const parentScope = createScope(schema);
      parentScope.userId = 'user123';
      // requestId and count are undefined

      const childScope = createScopeWithInheritance(schema, parentScope._getScopeValues());

      expect(childScope.userId).toBe('user123');
      expect(childScope.requestId).toBeUndefined();
      expect(childScope.count).toBeUndefined();
    });
  });

  describe('Different Data Types', () => {
    test('should support enum types', () => {
      const schema = defineLogSchema({
        status: S.enum(['pending', 'active', 'completed']),
        operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
      });

      const scope = createScope(schema);
      scope.status = 'active';
      scope.operation = 'UPDATE';

      const values = scope._getScopeValues();
      expect(values.status).toBe('active');
      expect(values.operation).toBe('UPDATE');
    });

    test('should support category types', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        sessionId: S.category(),
      });

      const scope = createScope(schema);
      scope.userId = 'user-abc-123';
      scope.sessionId = 'session-xyz-789';

      const values = scope._getScopeValues();
      expect(values.userId).toBe('user-abc-123');
      expect(values.sessionId).toBe('session-xyz-789');
    });

    test('should support text types', () => {
      const schema = defineLogSchema({
        errorMessage: S.text(),
        requestBody: S.text(),
      });

      const scope = createScope(schema);
      scope.errorMessage = 'Something went wrong';
      scope.requestBody = '{"key": "value"}';

      const values = scope._getScopeValues();
      expect(values.errorMessage).toBe('Something went wrong');
      expect(values.requestBody).toBe('{"key": "value"}');
    });

    test('should support number types', () => {
      const schema = defineLogSchema({
        count: S.number(),
        duration: S.number(),
      });

      const scope = createScope(schema);
      scope.count = 42;
      scope.duration = 123.456;

      const values = scope._getScopeValues();
      expect(values.count).toBe(42);
      expect(values.duration).toBe(123.456);
    });

    test('should support boolean types', () => {
      const schema = defineLogSchema({
        isValid: S.boolean(),
        isEnabled: S.boolean(),
      });

      const scope = createScope(schema);
      scope.isValid = true;
      scope.isEnabled = false;

      const values = scope._getScopeValues();
      expect(values.isValid).toBe(true);
      expect(values.isEnabled).toBe(false);
    });
  });

  describe('Scope Class Caching', () => {
    test('should reuse cached Scope class for same schema', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        count: S.number(),
      });

      const ScopeClass1 = generateScopeClass(schema);
      const ScopeClass2 = generateScopeClass(schema);

      // Should return the same class (cached)
      expect(ScopeClass1).toBe(ScopeClass2);
    });

    test('should create different Scope classes for different schemas', () => {
      const schema1 = defineLogSchema({
        userId: S.category(),
      });

      const schema2 = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
      });

      const ScopeClass1 = generateScopeClass(schema1);
      const ScopeClass2 = generateScopeClass(schema2);

      // Should be different classes
      expect(ScopeClass1).not.toBe(ScopeClass2);
    });
  });

  describe('Separation from Column Storage', () => {
    test('should NOT contain system columns', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        count: S.number(),
      });

      const scope = createScope(schema);

      // System columns should NOT exist
      expect(scope.timestamps).toBeUndefined();
      expect(scope.operations).toBeUndefined();
      expect(scope.spanId).toBeUndefined();
      expect(scope.traceId).toBeUndefined();
    });

    test('should NOT allocate TypedArrays', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        count: S.number(),
      });

      const scope = createScope(schema);

      // Values should be plain JavaScript values, not TypedArrays
      scope.count = 42;
      expect(typeof scope.count).toBe('number');
      expect(scope.count).toBe(42);

      scope.userId = 'user123';
      expect(typeof scope.userId).toBe('string');
      expect(scope.userId).toBe('user123');
    });

    test('should store raw values (not interned)', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        description: S.text(),
      });

      const scope = createScope(schema);

      // Scope stores raw strings, not interned indices
      scope.userId = 'user123';
      scope.description = 'Hello World';

      expect(scope.userId).toBe('user123');
      expect(scope.description).toBe('Hello World');

      // _getScopeValues should return the raw values
      const values = scope._getScopeValues();
      expect(values.userId).toBe('user123');
      expect(values.description).toBe('Hello World');
    });
  });

  describe('Multiple Instances', () => {
    test('should create independent Scope instances', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        count: S.number(),
      });

      const scope1 = createScope(schema);
      const scope2 = createScope(schema);

      scope1.userId = 'user1';
      scope1.count = 10;

      scope2.userId = 'user2';
      scope2.count = 20;

      // Instances should be independent
      expect(scope1.userId).toBe('user1');
      expect(scope1.count).toBe(10);
      expect(scope2.userId).toBe('user2');
      expect(scope2.count).toBe(20);
    });
  });
});

describe('generateScopeClassCode snapshots', () => {
  test('snapshot: empty schema', () => {
    const schema = defineLogSchema({});
    const code = generateScopeClassCode(schema as SchemaFields);
    expect(code).toMatchSnapshot();
  });

  test('snapshot: all field types', () => {
    const schema = defineLogSchema({
      userId: S.category(),
      operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
      errorMsg: S.text(),
      count: S.number(),
      enabled: S.boolean(),
    });
    const code = generateScopeClassCode(schema as SchemaFields);
    expect(code).toMatchSnapshot();
  });

  test('snapshot: category fields only', () => {
    const schema = defineLogSchema({
      userId: S.category(),
      sessionId: S.category(),
      requestId: S.category(),
    });
    const code = generateScopeClassCode(schema as SchemaFields);
    expect(code).toMatchSnapshot();
  });

  test('snapshot: mixed types', () => {
    const schema = defineLogSchema({
      status: S.enum(['pending', 'active', 'completed']),
      duration: S.number(),
      isValid: S.boolean(),
      errorMessage: S.text(),
    });
    const code = generateScopeClassCode(schema as SchemaFields);
    expect(code).toMatchSnapshot();
  });
});
