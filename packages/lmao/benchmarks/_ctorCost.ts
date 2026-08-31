/**
 * Scratch instrument: prices each piece of the ThreadSpanView constructor.
 *
 * createSpanBuffer is 71% of an empty span's 5.9 us, and it makes zero ABI
 * calls, so the floor is JS-side setup. This says which line to delete.
 */

import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { getEnumValues, getSchemaType } from '../src/lib/schema/typeGuards.js';
import {
  attributeKindForSchemaType,
  isThreadSystemColumn,
  schemaAttributeOrdinals,
} from '../src/lib/wasm/schemaBlob.js';

const schema = defineLogSchema({ n: S.number() });

function time(label: string, iterations: number, fn: () => void): void {
  for (let i = 0; i < 2000; i++) fn();
  const start = Bun.nanoseconds();
  for (let i = 0; i < iterations; i++) fn();
  console.log(`${label.padEnd(46)} ${((Bun.nanoseconds() - start) / iterations).toFixed(1)} ns`);
}

console.log(`schema columns: ${schema._columnNames.length}`);
console.log(`attribute ordinals: ${schemaAttributeOrdinals(schema).size}`);

time('schemaAttributeOrdinals(schema)', 50_000, () => {
  schemaAttributeOrdinals(schema);
});

time('kinds + enums maps from _columnNames', 50_000, () => {
  const kinds = new Map<string, number>();
  const enums = new Map<string, readonly string[]>();
  for (const name of schema._columnNames) {
    if (isThreadSystemColumn(name)) continue;
    const type = getSchemaType(schema.fields[name]);
    if (type === undefined) continue;
    const kind = attributeKindForSchemaType(type);
    if (kind === undefined) continue;
    kinds.set(name, kind);
    if (type === 'enum') {
      const variants = getEnumValues(schema.fields[name]);
      if (variants) enums.set(name, variants);
    }
  }
});

const laneProxy = (write: (index: number, value: unknown) => void): unknown[] => {
  const target: unknown[] = [];
  return new Proxy(target, {
    set(obj, prop, value) {
      if (prop === 'length') {
        obj.length = Number(value);
        return true;
      }
      const index = typeof prop === 'string' ? Number(prop) : Number.NaN;
      if (Number.isInteger(index) && index >= 0) {
        write(index, value);
        obj[index] = value;
        return true;
      }
      Reflect.set(obj, prop, value);
      return true;
    },
  });
};

time('4 base laneProxy allocations', 50_000, () => {
  laneProxy(() => {});
  laneProxy(() => {});
  laneProxy(() => {});
  laneProxy(() => {});
});

const ordinalNames = [...schemaAttributeOrdinals(schema).keys()];
time(`per-ordinal defineProperty x3 (${ordinalNames.length} ordinals)`, 50_000, () => {
  const host: Record<string, unknown> = {};
  for (const name of ordinalNames) {
    Object.defineProperty(host, `${name}_values`, {
      value: laneProxy(() => {}),
      writable: true,
      configurable: true,
      enumerable: false,
    });
    Object.defineProperty(host, `${name}_nulls`, {
      value: new Uint8Array(8192),
      writable: true,
      configurable: true,
      enumerable: false,
    });
    Object.defineProperty(host, name, {
      value: () => {},
      writable: true,
      configurable: true,
      enumerable: false,
    });
  }
});

time(`new Uint8Array(8192) x${ordinalNames.length}`, 50_000, () => {
  for (const _ of ordinalNames) new Uint8Array(8192);
});

time('fixed typed-array fields of the view', 50_000, () => {
  const _a = new ArrayBuffer(8);
  const _b = new Uint8Array(12);
  const _c = new BigInt64Array(2);
  const _d = new Uint8Array(2);
  const _e = new Float64Array(1);
  const _f = new Uint8Array(1);
  const _g = new Float64Array(1);
  const _h = new Uint8Array(1);
  const _i = new Float64Array(1);
  const _j = new Uint8Array(1);
  const _k = new Uint8Array(1);
  const _l = new Uint8Array(1);
  const _m = new BigUint64Array(1);
  const _n = new Uint8Array(1);
});

time('new Map() (fakeToReal)', 50_000, () => {
  const _m = new Map<number, number>();
});
