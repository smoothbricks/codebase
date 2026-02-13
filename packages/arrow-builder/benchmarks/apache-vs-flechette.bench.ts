import { existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { bench, boxplot, group, run, summary } from 'mitata';
import {
  createBoolData as createBoolDataFlechette,
  createDictionary8Data as createDictionary8DataFlechette,
  createDictionary16Data as createDictionary16DataFlechette,
  createDictionary32Data as createDictionary32DataFlechette,
  createFloat64Data as createFloat64DataFlechette,
  createInt32Data as createInt32DataFlechette,
  createUint8Data as createUint8DataFlechette,
  createUint16Data as createUint16DataFlechette,
  createUint32Data as createUint32DataFlechette,
  createUtf8Data as createUtf8DataFlechette,
} from '../src/lib/arrow/data.ts';

type DataFactories = {
  createBoolData: typeof createBoolDataFlechette;
  createDictionary8Data: typeof createDictionary8DataFlechette;
  createDictionary16Data: typeof createDictionary16DataFlechette;
  createDictionary32Data: typeof createDictionary32DataFlechette;
  createFloat64Data: typeof createFloat64DataFlechette;
  createInt32Data: typeof createInt32DataFlechette;
  createUint8Data: typeof createUint8DataFlechette;
  createUint16Data: typeof createUint16DataFlechette;
  createUint32Data: typeof createUint32DataFlechette;
  createUtf8Data: typeof createUtf8DataFlechette;
};

const ROWS = 65_536;

const ROOT_DIR = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const APACHE_DATA_MODULE = resolve(
  ROOT_DIR,
  'extern/arrow-builder-pre-flechette/packages/arrow-builder/src/lib/arrow/data.ts',
);

function makeNullBitmap(length: number): Uint8Array {
  const bytes = Math.ceil(length / 8);
  const bitmap = new Uint8Array(bytes);
  for (let i = 0; i < bytes; i++) {
    bitmap[i] = i % 2 === 0 ? 0b1111_1111 : 0b1111_0000;
  }
  return bitmap;
}

function makeUtf8Fixture(length: number): { data: Uint8Array; offsets: Int32Array } {
  const encoder = new TextEncoder();
  const chunks = new Array<Uint8Array>(length);
  const offsets = new Int32Array(length + 1);

  let total = 0;
  for (let i = 0; i < length; i++) {
    const chunk = encoder.encode(`value-${i % 1000}`);
    chunks[i] = chunk;
    offsets[i] = total;
    total += chunk.length;
  }
  offsets[length] = total;

  const data = new Uint8Array(total);
  let pos = 0;
  for (const chunk of chunks) {
    data.set(chunk, pos);
    pos += chunk.length;
  }

  return { data, offsets };
}

function touch(value: unknown): number {
  if (value && typeof value === 'object' && 'length' in value && typeof value.length === 'number') {
    return value.length;
  }
  return 0;
}

function addCompareGroup(name: string, flechetteFn: () => unknown, apacheFn?: () => unknown): void {
  group(name, () => {
    summary(() => {
      if (apacheFn) {
        bench('apache', () => {
          touch(apacheFn());
        });
      }
      bench('flechette', () => {
        touch(flechetteFn());
      });
    });
  });
}

async function loadApacheFactories(): Promise<DataFactories | null> {
  if (!existsSync(APACHE_DATA_MODULE)) {
    return null;
  }

  const mod = (await import(APACHE_DATA_MODULE)) as DataFactories;
  return mod;
}

const apache = await loadApacheFactories();

const nullBitmap = makeNullBitmap(ROWS);
const uint8Values = Uint8Array.from({ length: ROWS }, (_, i) => i & 0xff);
const uint16Values = Uint16Array.from({ length: ROWS }, (_, i) => i & 0xffff);
const uint32Values = Uint32Array.from({ length: ROWS }, (_, i) => i >>> 0);
const int32Values = Int32Array.from({ length: ROWS }, (_, i) => (i % 2 === 0 ? i : -i));
const float64Values = Float64Array.from({ length: ROWS }, (_, i) => i * 0.5 + 3.14);
const boolBits = new Uint8Array(Math.ceil(ROWS / 8));
for (let i = 0; i < boolBits.length; i++) {
  boolBits[i] = i % 2 === 0 ? 0b1010_1010 : 0b0101_0101;
}

const utf8Fixture = makeUtf8Fixture(ROWS);
const dictCardinality = 2048;
const dictEncoded = makeUtf8Fixture(dictCardinality);
const dict8Indices = Uint8Array.from({ length: ROWS }, (_, i) => i % Math.min(256, dictCardinality));
const dict16Indices = Uint16Array.from({ length: ROWS }, (_, i) => i % dictCardinality);
const dict32Indices = Uint32Array.from({ length: ROWS }, (_, i) => i % dictCardinality);

if (touch(createUint8DataFlechette(uint8Values, ROWS)) !== ROWS) {
  throw new Error('sanity check failed for flechette');
}

if (apache && touch(apache.createUint8Data(uint8Values, ROWS)) !== ROWS) {
  throw new Error('sanity check failed for apache');
}

console.log('\narrow-builder benchmark: apache-arrow (pre-migration) vs flechette (current)');
console.log(`rows=${ROWS.toLocaleString()} dictCardinality=${dictCardinality.toLocaleString()}`);
console.log(`apache baseline: ${apache ? 'enabled' : `disabled (missing ${APACHE_DATA_MODULE})`}`);

boxplot(() => {
  addCompareGroup(
    'uint8/no-nulls',
    () => createUint8DataFlechette(uint8Values, ROWS),
    apache ? () => apache.createUint8Data(uint8Values, ROWS) : undefined,
  );
  addCompareGroup(
    'uint8/with-nulls',
    () => createUint8DataFlechette(uint8Values, ROWS, nullBitmap),
    apache ? () => apache.createUint8Data(uint8Values, ROWS, nullBitmap) : undefined,
  );

  addCompareGroup(
    'uint16/with-nulls',
    () => createUint16DataFlechette(uint16Values, ROWS, nullBitmap),
    apache ? () => apache.createUint16Data(uint16Values, ROWS, nullBitmap) : undefined,
  );
  addCompareGroup(
    'uint32/with-nulls',
    () => createUint32DataFlechette(uint32Values, ROWS, nullBitmap),
    apache ? () => apache.createUint32Data(uint32Values, ROWS, nullBitmap) : undefined,
  );
  addCompareGroup(
    'int32/with-nulls',
    () => createInt32DataFlechette(int32Values, ROWS, nullBitmap),
    apache ? () => apache.createInt32Data(int32Values, ROWS, nullBitmap) : undefined,
  );
  addCompareGroup(
    'float64/with-nulls',
    () => createFloat64DataFlechette(float64Values, ROWS, nullBitmap),
    apache ? () => apache.createFloat64Data(float64Values, ROWS, nullBitmap) : undefined,
  );

  addCompareGroup(
    'bool/no-nulls',
    () => createBoolDataFlechette(boolBits, ROWS),
    apache ? () => apache.createBoolData(boolBits, ROWS) : undefined,
  );
  addCompareGroup(
    'bool/with-nulls',
    () => createBoolDataFlechette(boolBits, ROWS, nullBitmap),
    apache ? () => apache.createBoolData(boolBits, ROWS, nullBitmap) : undefined,
  );

  addCompareGroup(
    'utf8/with-nulls',
    () => createUtf8DataFlechette(utf8Fixture.data, utf8Fixture.offsets, ROWS, nullBitmap),
    apache ? () => apache.createUtf8Data(utf8Fixture.data, utf8Fixture.offsets, ROWS, nullBitmap) : undefined,
  );

  addCompareGroup(
    'dict8/with-nulls',
    () => createDictionary8DataFlechette(dict8Indices, dictEncoded.data, dictEncoded.offsets, ROWS, nullBitmap),
    apache
      ? () => apache.createDictionary8Data(dict8Indices, dictEncoded.data, dictEncoded.offsets, ROWS, nullBitmap)
      : undefined,
  );
  addCompareGroup(
    'dict16/with-nulls',
    () => createDictionary16DataFlechette(dict16Indices, dictEncoded.data, dictEncoded.offsets, ROWS, nullBitmap),
    apache
      ? () => apache.createDictionary16Data(dict16Indices, dictEncoded.data, dictEncoded.offsets, ROWS, nullBitmap)
      : undefined,
  );
  addCompareGroup(
    'dict32/with-nulls',
    () => createDictionary32DataFlechette(dict32Indices, dictEncoded.data, dictEncoded.offsets, ROWS, nullBitmap),
    apache
      ? () => apache.createDictionary32Data(dict32Indices, dictEncoded.data, dictEncoded.offsets, ROWS, nullBitmap)
      : undefined,
  );
});

await run();
