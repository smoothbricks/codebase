/**
 * Debug test to understand Sury schema structure
 */
import { describe, expect, it } from 'bun:test';
import * as Sury from '@sury/sury';

describe('Sury schema structure', () => {
  it('should show Sury.string properties', () => {
    console.log('Sury.string properties:', Object.keys(Sury.string));
    console.log('Sury.string prototype:', Object.getPrototypeOf(Sury.string));
    console.log('Sury.string type:', typeof Sury.string);

    // Show what spreading loses
    const copy = { ...Sury.string };
    console.log('Spread copy properties:', Object.keys(copy));

    // Sury uses S.parseOrThrow(value, schema), not Sury.parse(schema, value)
    console.log('Validation with original:', Sury.parseOrThrow('test', Sury.string));

    // Try with copy
    try {
      console.log('Validation with copy:', Sury.parseOrThrow('test', copy as typeof Sury.string));
    } catch (e) {
      console.log('ERROR with copy:', (e as Error).message);
    }

    expect(true).toBe(true);
  });

  it('should validate union with original schemas', () => {
    const union = Sury.union([Sury.string, Sury.number]);
    console.log('Union validates string:', Sury.parseOrThrow('test', union));
    console.log('Union validates number:', Sury.parseOrThrow(42, union));
    expect(true).toBe(true);
  });

  it('should validate union with spread schemas', () => {
    const stringCopy = { ...Sury.string };
    const numberCopy = { ...Sury.number };

    console.log('String copy type field:', (stringCopy as any).type);
    console.log('Number copy type field:', (numberCopy as any).type);

    try {
      const union = Sury.union([stringCopy as any, numberCopy as any]);
      console.log('Union created from copies');
      console.log('Union validates string:', Sury.parseOrThrow('test', union));
    } catch (e) {
      console.log('ERROR creating/using union:', (e as Error).message);
    }

    expect(true).toBe(true);
  });

  it('should validate union with Object.create schemas', () => {
    // Use Object.create to preserve prototype chain while allowing own properties
    const stringCopy = Object.create(Object.getPrototypeOf(Sury.string), Object.getOwnPropertyDescriptors(Sury.string));
    const numberCopy = Object.create(Object.getPrototypeOf(Sury.number), Object.getOwnPropertyDescriptors(Sury.number));

    // Add custom metadata to the copies
    stringCopy.__schema_type = 'category';
    numberCopy.__schema_type = 'number';

    console.log('String copy with Object.create:');
    console.log('  prototype:', Object.getPrototypeOf(stringCopy));
    console.log('  own props:', Object.keys(stringCopy));
    console.log('  type:', stringCopy.type);
    console.log('  __schema_type:', stringCopy.__schema_type);

    try {
      // Test parsing with copied schema
      console.log('Validation with Object.create copy:', Sury.parseOrThrow('test', stringCopy));

      // Test union with copied schemas
      const union = Sury.union([stringCopy, numberCopy]);
      console.log('Union created from Object.create copies');
      console.log('Union validates string:', Sury.parseOrThrow('test', union));
      console.log('Union validates number:', Sury.parseOrThrow(42, union));
    } catch (e) {
      console.log('ERROR:', (e as Error).message);
    }

    expect(true).toBe(true);
  });

  it('should NOT share metadata between Object.create copies', () => {
    // Create two independent copies
    const copy1 = Object.create(Object.getPrototypeOf(Sury.string), Object.getOwnPropertyDescriptors(Sury.string));
    const copy2 = Object.create(Object.getPrototypeOf(Sury.string), Object.getOwnPropertyDescriptors(Sury.string));

    // Set different metadata
    copy1.__schema_type = 'category';
    copy2.__schema_type = 'text';

    console.log('copy1.__schema_type:', copy1.__schema_type);
    console.log('copy2.__schema_type:', copy2.__schema_type);
    console.log('Sury.string.__schema_type:', (Sury.string as any).__schema_type);

    // Verify independence
    expect(copy1.__schema_type).toBe('category');
    expect(copy2.__schema_type).toBe('text');
    // Original should NOT have __schema_type
    expect((Sury.string as any).__schema_type).toBeUndefined();
  });
});
