import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import fc from 'fast-check';
import { defineOpContext } from '../defineOpContext.js';
import { resolveMessage } from '../resolveMessage.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY,
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY,
  RUNTIME_HINT_RESULT,
  runtimeHintMessageLayoutFamily,
  type MessageLayoutFamily,
} from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_SPAN_EXCEPTION,
  ENTRY_TYPE_SPAN_OK,
  ENTRY_TYPE_SPAN_START,
} from '../schema/systemSchema.js';
import {
  createChildSpanBuffer,
  createOverflowBuffer,
  createSpanBuffer,
  getSpanBufferClass,
  type SpanBufferConstructor,
} from '../spanBuffer.js';
import { extractFacts } from '../testing/extractFacts.js';
import type { AnySpanBuffer, SpanBuffer } from '../types.js';
import {
  getVocabularyGeneration,
  registerVocabularyFragment,
  type VocabularyFragment,
} from '../vocabularyRegistry.js';
import { createTestOpMetadata, createTestTraceRoot } from './test-helpers.js';

const CAPACITY = 8;
const encoder = new TextEncoder();
const schema = defineLogSchema({ marker: S.category() });
const context = defineOpContext({ logSchema: schema });
const runtimeSchema = context.logBinding.logSchema;

function encodeRecord(text: string): Uint8Array {
  const textBytes = encoder.encode(text);
  const record = new Uint8Array(4 + textBytes.length + 2);
  new DataView(record.buffer).setUint32(0, textBytes.length, true);
  record.set(textBytes, 4);
  return record;
}

function fragmentHash(fragment: Omit<VocabularyFragment, 'contentHash'>): string {
  const algorithm = encoder.encode(fragment.idAlgorithm);
  const byteLength =
    1 +
    2 +
    algorithm.length +
    4 +
    fragment.ids.length * 4 +
    4 +
    fragment.kindTags.length +
    4 +
    fragment.utf8.length +
    4 +
    fragment.offsets.length * 4;
  const bytes = new Uint8Array(byteLength);
  const view = new DataView(bytes.buffer);
  let offset = 0;
  bytes[offset++] = fragment.schemaVersion;
  view.setUint16(offset, algorithm.length, true);
  offset += 2;
  bytes.set(algorithm, offset);
  offset += algorithm.length;
  view.setUint32(offset, fragment.ids.length, true);
  offset += 4;
  for (const id of fragment.ids) {
    view.setUint32(offset, id, true);
    offset += 4;
  }
  view.setUint32(offset, fragment.kindTags.length, true);
  offset += 4;
  bytes.set(fragment.kindTags, offset);
  offset += fragment.kindTags.length;
  view.setUint32(offset, fragment.utf8.length, true);
  offset += 4;
  bytes.set(fragment.utf8, offset);
  offset += fragment.utf8.length;
  view.setUint32(offset, fragment.offsets.length, true);
  offset += 4;
  for (const boundary of fragment.offsets) {
    view.setInt32(offset, boundary, true);
    offset += 4;
  }
  return createHash('sha256').update(bytes).digest('hex');
}

function makeFragment(text: string): VocabularyFragment {
  const record = encodeRecord(text);
  const digest = createHash('sha256').update(Uint8Array.of(1)).update(record).digest();
  const fragment: Omit<VocabularyFragment, 'contentHash'> = {
    schemaVersion: 1,
    idAlgorithm: 'sha256-24-v1',
    ids: new Uint32Array([(digest[0] << 16) | (digest[1] << 8) | digest[2]]),
    kindTags: new Uint8Array([1]),
    utf8: record,
    offsets: new Int32Array([0, record.length]),
  };
  return { ...fragment, contentHash: fragmentHash(fragment) };
}

function textAtDenseZero(): string {
  let generation = getVocabularyGeneration();
  if (generation.ids.length === 0) {
    const binding = registerVocabularyFragment(makeFragment('family dense zero'));
    expect(binding[0]).toBe(0);
    generation = getVocabularyGeneration();
  }
  const start = generation.offsets[0];
  const end = generation.offsets[1];
  const record = generation.records.subarray(start, end);
  const textLength = new DataView(record.buffer, record.byteOffset, record.byteLength).getUint32(0, true);
  return new TextDecoder().decode(record.subarray(4, 4 + textLength));
}

function hint(familyBits: number): number {
  return RUNTIME_HINT_ANALYZED_VALID | familyBits | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | CAPACITY;
}

const staticOp = context.defineOp('static-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY),
});
const dynamicOp = context.defineOp('dynamic-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY),
});
const mixedOp = context.defineOp('mixed-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_MIXED),
});

function createPlannedBuffer(
  family: MessageLayoutFamily,
  SpanBufferClass: SpanBufferConstructor<typeof runtimeSchema>,
): SpanBuffer<typeof runtimeSchema> {
  SpanBufferClass.stats.capacity = CAPACITY;
  return createSpanBuffer(
    runtimeSchema,
    createTestTraceRoot('family-layout'),
    createTestOpMetadata({ name: family }),
    CAPACITY,
    SpanBufferClass,
  );
}

function expectFamilyShape(buffer: AnySpanBuffer, family: MessageLayoutFamily): void {
  if (buffer._messageLayoutFamily !== family) {
    throw new Error(`Expected ${family} buffer, received ${buffer._messageLayoutFamily}`);
  }
  if (family === 'static-only') {
    expect(buffer._logHeaders).toBeInstanceOf(Uint32Array);
    expect('message_values' in buffer).toBe(false);
    expect('message_nulls' in buffer).toBe(false);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(true);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(true);
    expect(Array.isArray(buffer._spanName)).toBe(false);
    expect(ArrayBuffer.isView(buffer._spanName)).toBe(false);
  } else if (family === 'dynamic-only') {
    expect('_logHeaders' in buffer).toBe(false);
    expect(buffer.message_values).toBeInstanceOf(Array);
    expect('message_nulls' in buffer).toBe(false);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(true);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(false);
  } else {
    expect(buffer._logHeaders).toBeInstanceOf(Uint32Array);
    expect(buffer.message_values).toBeInstanceOf(Array);
    expect('message_nulls' in buffer).toBe(false);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(false);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(false);
  }
}

function collectLogRows(root: AnySpanBuffer): Array<{ entryType: number; message: string | undefined }> {
  const rows: Array<{ entryType: number; message: string | undefined }> = [];
  let segment: AnySpanBuffer | undefined = root;
  let first = true;
  while (segment) {
    for (let row = first ? 2 : 0; row < segment._writeIndex; row++) {
      rows.push({ entryType: segment.entry_type[row], message: resolveMessage(segment, row) });
    }
    first = false;
    segment = segment._overflow;
  }
  return rows;
}

describe('specialized message buffer families', () => {
  it('selects explicit families in immutable callsite plans and keeps missing metadata conservative', () => {
    expect(runtimeHintMessageLayoutFamily(hint(RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY))).toBe('static-only');
    expect(runtimeHintMessageLayoutFamily(hint(RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY))).toBe('dynamic-only');
    expect(runtimeHintMessageLayoutFamily(hint(RUNTIME_HINT_MESSAGE_LAYOUT_MIXED))).toBe('mixed');
    expect(runtimeHintMessageLayoutFamily(0)).toBe('mixed');
    expect(runtimeHintMessageLayoutFamily(RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT)).toBe('mixed');

    expect(staticOp.callsitePlan.messageLayoutFamily).toBe('static-only');
    expect(dynamicOp.callsitePlan.messageLayoutFamily).toBe('dynamic-only');
    expect(mixedOp.callsitePlan.messageLayoutFamily).toBe('mixed');
    expect(staticOp.callsitePlan.SpanBufferClass).toBe(getSpanBufferClass(runtimeSchema, 'static-only'));
    expect(dynamicOp.callsitePlan.SpanBufferClass).toBe(getSpanBufferClass(runtimeSchema, 'dynamic-only'));
    expect(mixedOp.callsitePlan.SpanBufferClass).toBe(getSpanBufferClass(runtimeSchema, 'mixed'));
    expect(new Set([staticOp.callsitePlan.SpanBufferClass, dynamicOp.callsitePlan.SpanBufferClass, mixedOp.callsitePlan.SpanBufferClass]).size).toBe(3);
  });

  it('omits every unused capacity lane and exposes exact JS storage bytes', () => {
    const staticBuffer = createPlannedBuffer('static-only', staticOp.callsitePlan.SpanBufferClass);
    const dynamicBuffer = createPlannedBuffer('dynamic-only', dynamicOp.callsitePlan.SpanBufferClass);
    const mixedBuffer = createPlannedBuffer('mixed', mixedOp.callsitePlan.SpanBufferClass);

    expectFamilyShape(staticBuffer, 'static-only');
    expectFamilyShape(dynamicBuffer, 'dynamic-only');
    expectFamilyShape(mixedBuffer, 'mixed');
    if (staticBuffer._messageLayoutFamily !== 'static-only' || staticBuffer._logHeaders === undefined) {
      throw new Error('Expected static-only packed header lane');
    }
    if (dynamicBuffer._messageLayoutFamily !== 'dynamic-only' || dynamicBuffer.message_values === undefined) {
      throw new Error('Expected dynamic-only raw message lane');
    }
    if (mixedBuffer._messageLayoutFamily !== 'mixed' || mixedBuffer._logHeaders === undefined) {
      throw new Error('Expected mixed packed header lane');
    }
    if (mixedBuffer.message_values === undefined) throw new Error('Expected mixed raw message lane');
    expect(staticBuffer._logHeaders.byteLength).toBe(CAPACITY * Uint32Array.BYTES_PER_ELEMENT);
    expect(mixedBuffer._logHeaders.byteLength).toBe(CAPACITY * Uint32Array.BYTES_PER_ELEMENT);
    expect(staticBuffer._system.byteLength).toBe(mixedBuffer._system.byteLength);
    expect(staticBuffer._system.byteLength - dynamicBuffer._system.byteLength).toBe(staticBuffer._logHeaders.byteLength);
    expect(dynamicBuffer.message_values.length).toBe(CAPACITY);
    expect(mixedBuffer.message_values.length).toBe(CAPACITY);
  });

  it('represents dense index zero, dynamic/static span names, null/raw rows, and terminal scalars', () => {
    const denseZeroText = textAtDenseZero();
    const cases = [
      { family: 'static-only' as const, op: staticOp },
      { family: 'dynamic-only' as const, op: dynamicOp },
      { family: 'mixed' as const, op: mixedOp },
    ];

    for (const { family, op } of cases) {
      const denseName = createPlannedBuffer(family, op.callsitePlan.SpanBufferClass);
      op.callsitePlan.appenders.writeSpanStart(denseName, 0);
      expect(denseName.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
      expect(resolveMessage(denseName, 0)).toBe(denseZeroText);
      if (family !== 'dynamic-only') {
        if (denseName._messageLayoutFamily === 'dynamic-only' || denseName._logHeaders === undefined) {
          throw new Error(`Expected ${family} packed header lane`);
        }
        expect(denseName._logHeaders[0]).toBe(ENTRY_TYPE_SPAN_START);
      }

      const dynamicName = createPlannedBuffer(family, op.callsitePlan.SpanBufferClass);
      op.callsitePlan.appenders.writeSpanStart(dynamicName, `raw ${family} span`);
      expect(resolveMessage(dynamicName, 0)).toBe(`raw ${family} span`);
      expect(resolveMessage(dynamicName, 1)).toBeUndefined();
      dynamicName.message(1, `terminal ${family}`);
      expect(resolveMessage(dynamicName, 1)).toBe(`terminal ${family}`);

      if (family === 'static-only') {
        expect(() => dynamicName.message(2, 'forbidden raw log')).toThrow('rows 0 and 1');
      } else {
        const nullRow = op.callsitePlan.appenders.writeLogEntry(dynamicName, ENTRY_TYPE_DEBUG);
        expect(resolveMessage(dynamicName, nullRow)).toBeUndefined();
        const rawRow = op.callsitePlan.appenders.writeLogEntry(dynamicName, ENTRY_TYPE_DEBUG);
        dynamicName.message(rawRow, `raw ${family} log`);
        expect(resolveMessage(dynamicName, rawRow)).toBe(`raw ${family} log`);
      }
    }
  });

  it('propagates the exact family constructor and omissions through child and overflow buffers', () => {
    for (const { family, op } of [
      { family: 'static-only' as const, op: staticOp },
      { family: 'dynamic-only' as const, op: dynamicOp },
      { family: 'mixed' as const, op: mixedOp },
    ]) {
      const root = createPlannedBuffer(family, op.callsitePlan.SpanBufferClass);
      const metadata = createTestOpMetadata({ name: `${family}-child` });
      const child = createChildSpanBuffer(root, op.callsitePlan.SpanBufferClass, metadata, metadata, CAPACITY);
      const overflow = createOverflowBuffer(child);
      expect(child.constructor).toBe(op.callsitePlan.SpanBufferClass);
      expect(overflow.constructor).toBe(op.callsitePlan.SpanBufferClass);
      expect(child._parent).toBe(root);
      expect(overflow._parent).toBe(root);
      expect(overflow._identity).toBe(child._identity);
      expectFamilyShape(child, family);
      expectFamilyShape(overflow, family);
    }
  });


  it('preserves decoded lifecycle and log facts across mixed-family parent/child rows', () => {
    const denseZeroText = textAtDenseZero();
    const root = createPlannedBuffer('mixed', mixedOp.callsitePlan.SpanBufferClass);
    const rootContext = new mixedOp.callsitePlan.SpanContextClass(root, runtimeSchema, mixedOp.callsitePlan);
    mixedOp.callsitePlan.appenders.writeSpanStart(root, 'mixed root');
    const rootLogger = rootContext._spanLogger;
    rootLogger._infoTemplate(0);
    rootLogger.debug('root raw');
    mixedOp.callsitePlan.appenders.writeSpanEnd(root, ENTRY_TYPE_SPAN_OK);

    const child = createChildSpanBuffer(
      root,
      staticOp.callsitePlan.SpanBufferClass,
      createTestOpMetadata({ name: 'static child callsite' }),
      createTestOpMetadata({ name: 'static child op' }),
      CAPACITY,
    );
    const childContext = new staticOp.callsitePlan.SpanContextClass(child, runtimeSchema, staticOp.callsitePlan);
    staticOp.callsitePlan.appenders.writeSpanStart(child, 0);
    const childLogger = childContext._spanLogger;
    childLogger._infoTemplate(0);
    child.message(1, 'child boom');
    staticOp.callsitePlan.appenders.writeSpanEnd(child, ENTRY_TYPE_SPAN_EXCEPTION);

    expect(Array.from(extractFacts(root, { includeMetrics: false }))).toEqual([
      'span:mixed root: started',
      `log:info: ${denseZeroText}`,
      'log:debug: root raw',
      `span:${denseZeroText}: started`,
      `log:info: ${denseZeroText}`,
      `span:${denseZeroText}: exception(child boom)`,
      'span:mixed root: ok',
    ]);
  });

  it('matches a semantic model for random static/dynamic mixes across overflow segments', () => {
    const denseZeroText = textAtDenseZero();
    fc.assert(
      fc.property(
        fc.array(
          fc.oneof(
            fc.constant({ kind: 'static' as const, text: denseZeroText }),
            fc.record({ kind: fc.constant('dynamic' as const), text: fc.string({ minLength: 1, maxLength: 32 }) }),
          ),
          { minLength: CAPACITY, maxLength: 80 },
        ),
        (operations) => {
          const root = createPlannedBuffer('mixed', mixedOp.callsitePlan.SpanBufferClass);
          const rootContext = new mixedOp.callsitePlan.SpanContextClass(root, runtimeSchema, mixedOp.callsitePlan);
          mixedOp.callsitePlan.appenders.writeSpanStart(root, 'property root');
          const logger = rootContext._spanLogger;
          for (const operation of operations) {
            if (operation.kind === 'static') logger._infoTemplate(0);
            else logger.debug(operation.text);
          }
          const actual = collectLogRows(root);
          expect(actual.map((row) => row.message)).toEqual(operations.map((operation) => operation.text));
          expect(actual.map((row) => row.entryType)).toEqual(
            operations.map((operation) => (operation.kind === 'static' ? ENTRY_TYPE_INFO : ENTRY_TYPE_DEBUG)),
          );
          expect(root._overflow).toBeDefined();
          for (let segment: AnySpanBuffer | undefined = root; segment; segment = segment._overflow) {
            expectFamilyShape(segment, 'mixed');
          }
        },
      ),
      { numRuns: 100 },
    );
  });
});
