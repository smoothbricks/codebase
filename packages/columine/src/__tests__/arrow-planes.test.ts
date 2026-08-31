import { describe, expect, it } from 'bun:test';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { ARROW_PLANES } from '../arrow-planes.js';
import { renderCompactColumnModule, renderRustPlaneDeclarations } from '../arrow-planes-codegen.js';

const SRC = join(import.meta.dir, '..');
const GENERATED_TS = join(SRC, 'compact-column.generated.ts');
const SCHEMA_RS = join(SRC, '..', 'crates', 'columine-arrow', 'src', 'schema.rs');
const WRITE = process.env.UPDATE_GENERATED === '1';

/** Committed generated output, refreshed in place when explicitly requested. */
function expectGenerated(path: string, rendered: string): void {
  if (WRITE) {
    if (!existsSync(path) || readFileSync(path, 'utf8') !== rendered) {
      writeFileSync(path, rendered);
    }
    return;
  }
  expect(existsSync(path)).toBe(true);
  expect(readFileSync(path, 'utf8')).toBe(rendered);
}

describe('arrow plane table', () => {
  it('generates the committed TypeScript surface', () => {
    expectGenerated(GENERATED_TS, renderCompactColumnModule());
  });

  it('generates the committed Rust plane declarations', () => {
    const source = readFileSync(SCHEMA_RS, 'utf8');
    expectGenerated(SCHEMA_RS, renderRustPlaneDeclarations(source));
  });

  // Tags are baked into the shipped wasm and every persisted fixture, so a gap
  // means a plane was deleted rather than appended, and a duplicate means two
  // planes decode as one.
  it('keeps tags a gapless block from zero', () => {
    const tags: number[] = ARROW_PLANES.map((plane) => plane.tag);
    expect([...tags].sort((a, b) => a - b)).toEqual(tags.map((_, index) => index));
  });

  it('gives every plane a distinct name on both sides', () => {
    expect(new Set(ARROW_PLANES.map((plane) => plane.kind)).size).toBe(ARROW_PLANES.length);
    expect(new Set(ARROW_PLANES.map((plane) => plane.variant)).size).toBe(ARROW_PLANES.length);
  });
});

/**
 * The carrier a width-and-signedness name REQUIRES, or `null` when the name
 * does not encode one (`binary`, `bool`, `decimal128`, `intervalDayTime`, …).
 *
 * This is the defect the table exists to prevent: `'u32'` carried by
 * `Int32Array` compiles and reads back sign-flipped, which is exactly why it
 * survived review and shipped.
 */
function requiredCarrier(kind: string): string | null {
  const numeric = /^(?<sign>[iuf])(?<width>8|16|32|64)$/.exec(kind);
  const sign = numeric?.groups?.sign;
  const width = numeric?.groups?.width;
  if (!sign || !width) {
    return null;
  }
  if (width === '64') {
    // No Int64Array exists; the eight-byte integer planes ride the BigInt arrays.
    return sign === 'f' ? 'Float64Array' : sign === 'i' ? 'BigInt64Array' : 'BigUint64Array';
  }
  if (sign === 'f') {
    // Float16Array is not in the language, so the half-width float plane is
    // carried as raw 16-bit words and converted by the reader.
    return width === '16' ? 'Uint16Array' : `Float${width}Array`;
  }
  return sign === 'i' ? `Int${width}Array` : `Uint${width}Array`;
}

describe('plane names agree with their carriers', () => {
  for (const plane of ARROW_PLANES) {
    const required = requiredCarrier(plane.kind);
    if (!required) {
      continue;
    }
    it(`carries '${plane.kind}' as ${required}`, () => {
      const carrier: string | null = plane.data;
      expect(carrier).toBe(required);
    });
  }
});

/**
 * The TypeScript name a Rust variant REQUIRES, or `null` when the variant name
 * carries no width and signedness (`Binary`, `Bool`, `Decimal128`, …).
 */
function requiredKind(variant: string): string | null {
  const parsed = /^(?<sign>U?Int|Float)(?<width>8|16|32|64)$/.exec(variant);
  const sign = parsed?.groups?.sign;
  const width = parsed?.groups?.width;
  if (!sign || !width) {
    return null;
  }
  return `${sign === 'UInt' ? 'u' : sign === 'Int' ? 'i' : 'f'}${width}`;
}

/**
 * THE defect, stated as an invariant: tag 1 is `ArrowType::Int32`, a signed
 * plane, and it was named `'u32'` on the TypeScript side. Nothing compared the
 * two names, because each side was internally consistent — `'u32'` did carry a
 * `Uint32Array`. Binding the variant, the tag, the name and the carrier in one
 * row is only half the fix; this is the half that rejects the combination.
 */
describe('plane names agree across the ABI', () => {
  for (const plane of ARROW_PLANES) {
    const required = requiredKind(plane.variant);
    if (!required) {
      continue;
    }
    it(`names ${plane.variant} '${required}'`, () => {
      const kind: string = plane.kind;
      expect(kind).toBe(required);
    });
  }
});

describe('arrow plane codegen', () => {
  it('rejects a plane the Rust table does not declare', () => {
    const source = readFileSync(SCHEMA_RS, 'utf8');
    expect(() =>
      renderRustPlaneDeclarations(source, [
        ...ARROW_PLANES,
        { kind: 'i128', variant: 'Int128', tag: 23, data: 'BigInt64Array', offsets: null, validity: 'Uint8Array' },
      ]),
    ).toThrow(/absent from schema\.rs/);
  });
});
