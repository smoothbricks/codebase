import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import fc from 'fast-check';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { MAX_PACKED_MESSAGE_DENSE_INDEX, resolveEntryType, resolveMessage } from '../resolveMessage.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY,
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY,
  RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED,
  RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED,
  RUNTIME_HINT_RESULT,
  runtimeHintMessageLayoutFamily,
  runtimeHintMessagePhysicalLayout,
  type MessageLayoutFamily,
  type MessagePhysicalLayout,
} from '../runtimeHint.js';
import type { OpMetadata } from '../opContext/opTypes.js';
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
import { createTestTraceRoot } from './test-helpers.js';

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

function hint(familyBits: number, physicalBits = 0, capacity = CAPACITY): number {
  return RUNTIME_HINT_ANALYZED_VALID | familyBits | physicalBits | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | capacity;
}

const staticOp = context.defineOp('static-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY),
  localMessageDictionary: [0],
});
const dynamicOp = context.defineOp('dynamic-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY),
});
const mixedOp = context.defineOp('mixed-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_MIXED),
  localMessageDictionary: [0],
});
const fallbackCurrentOp = context.defineOp('fallback-current-family', (ctx) => ctx.ok(null));
const specializedStaticOp = context.defineOp('specialized-static-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY, RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED),
});
const specializedDynamicOp = context.defineOp('specialized-dynamic-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY, RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED),
});
const specializedMixedOp = context.defineOp('specialized-mixed-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_MIXED, RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED),
});
const packedStaticOp = context.defineOp('packed-static-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY, RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED),
});
const packedDynamicOp = context.defineOp('packed-dynamic-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY, RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED),
});
const packedMixedOp = context.defineOp('packed-mixed-family', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: hint(RUNTIME_HINT_MESSAGE_LAYOUT_MIXED, RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED),
});

function createPlannedBuffer(
  family: MessageLayoutFamily,
  SpanBufferClass: SpanBufferConstructor<typeof runtimeSchema>,
  metadata: OpMetadata,
): SpanBuffer<typeof runtimeSchema> {
  SpanBufferClass.stats.capacity = CAPACITY;
  return createSpanBuffer(
    runtimeSchema,
    createTestTraceRoot('family-layout'),
    metadata,
    CAPACITY,
    SpanBufferClass,
  );
}

function expectFamilyShape(buffer: AnySpanBuffer, family: MessageLayoutFamily): void {
  if (buffer._messageLayoutFamily !== family) {
    throw new Error(`Expected ${family} buffer, received ${buffer._messageLayoutFamily}`);
  }
  if (family === 'static-only') {
    expect('message_values' in buffer).toBe(false);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(true);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(true);
    expect(Array.isArray(buffer._spanName)).toBe(false);
    expect(ArrayBuffer.isView(buffer._spanName)).toBe(false);
  } else if (family === 'dynamic-only') {
    expect(buffer.message_values).toBeInstanceOf(Array);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(true);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(false);
  } else {
    expect(buffer.message_values).toBeInstanceOf(Array);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(false);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(false);
  }
}

function expectPhysicalShape(buffer: AnySpanBuffer, layout: MessagePhysicalLayout): void {
  if (buffer._messagePhysicalLayout !== layout) {
    throw new Error(`Expected ${layout} buffer, received ${buffer._messagePhysicalLayout}`);
  }
  const hasStaticMessages = buffer._messageLayoutFamily !== 'dynamic-only';
  if (layout === 'packed') {
    expect(buffer._rowHeaders).toBeInstanceOf(Uint32Array);
    expect('entry_type' in buffer).toBe(false);
    expect('_messageIds' in buffer).toBe(false);
    expect('_logHeaders' in buffer).toBe(false);
    expect('message_nulls' in buffer).toBe(false);
  } else {
    expect(buffer.entry_type).toBeInstanceOf(Uint8Array);
    expect('_rowHeaders' in buffer).toBe(false);
    if (layout === 'current' && hasStaticMessages) {
      expect(buffer._messageIds).toBeInstanceOf(Uint16Array);
      expect('_logHeaders' in buffer).toBe(false);
    } else if (layout === 'specialized' && hasStaticMessages) {
      expect('_messageIds' in buffer).toBe(false);
      expect(buffer._logHeaders).toBeInstanceOf(Uint32Array);
    } else {
      expect('_messageIds' in buffer).toBe(false);
      expect('_logHeaders' in buffer).toBe(false);
    }
    expect('message_nulls' in buffer).toBe(false);
  }
}

function collectLogRows(root: AnySpanBuffer): Array<{ entryType: number; message: string | undefined }> {
  const rows: Array<{ entryType: number; message: string | undefined }> = [];
  let segment: AnySpanBuffer | undefined = root;
  let first = true;
  while (segment) {
    for (let row = first ? 2 : 0; row < segment._writeIndex; row++) {
      rows.push({ entryType: resolveEntryType(segment, row), message: resolveMessage(segment, row) });
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

  it('decodes physical bits conservatively and caches every family/physical constructor dimension', () => {
    const packedHint = hint(RUNTIME_HINT_MESSAGE_LAYOUT_MIXED, RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED, 64);
    const specializedHint = hint(
      RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
      RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED,
      64,
    );
    expect(runtimeHintMessagePhysicalLayout(packedHint)).toBe('packed');
    expect(runtimeHintMessagePhysicalLayout(specializedHint)).toBe('specialized');
    expect(runtimeHintMessagePhysicalLayout(hint(RUNTIME_HINT_MESSAGE_LAYOUT_MIXED, 0, 64))).toBe('current');
    expect(runtimeHintMessagePhysicalLayout(0)).toBe('current');
    expect(runtimeHintMessagePhysicalLayout(packedHint | RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED)).toBe('current');

    const classes = [
      getSpanBufferClass(runtimeSchema, 'static-only', 'current'),
      getSpanBufferClass(runtimeSchema, 'static-only', 'specialized'),
      getSpanBufferClass(runtimeSchema, 'static-only', 'packed'),
      getSpanBufferClass(runtimeSchema, 'mixed', 'current'),
      getSpanBufferClass(runtimeSchema, 'mixed', 'specialized'),
      getSpanBufferClass(runtimeSchema, 'mixed', 'packed'),
      getSpanBufferClass(runtimeSchema, 'dynamic-only', 'current'),
      getSpanBufferClass(runtimeSchema, 'dynamic-only', 'specialized'),
      getSpanBufferClass(runtimeSchema, 'dynamic-only', 'packed'),
    ];
    expect(new Set(classes).size).toBe(9);
    expect(getSpanBufferClass(runtimeSchema, 'mixed', 'specialized')).toBe(classes[4]);
    expect(getSpanBufferClass(runtimeSchema, 'mixed', 'packed')).toBe(classes[5]);
    expect(specializedStaticOp.callsitePlan.messagePhysicalLayout).toBe('specialized');
    expect(specializedDynamicOp.callsitePlan.messagePhysicalLayout).toBe('specialized');
    expect(specializedMixedOp.callsitePlan.messagePhysicalLayout).toBe('specialized');
    expect(packedStaticOp.callsitePlan.messagePhysicalLayout).toBe('packed');
    expect(packedDynamicOp.callsitePlan.messagePhysicalLayout).toBe('packed');
    expect(packedMixedOp.callsitePlan.messagePhysicalLayout).toBe('packed');
    expect(specializedMixedOp.callsitePlan.arrowExposure.entryTypeStorage).toBe('borrowed-u8');
    expect(packedMixedOp.callsitePlan.arrowExposure.entryTypeStorage).toBe('derived-row-headers');
  });

  it('packs low-8 entry types and dense-plus-one high-24 messages with exact lane omissions', () => {
    const denseZeroText = textAtDenseZero();
    const cases = [
      { family: 'static-only' as const, op: packedStaticOp },
      { family: 'dynamic-only' as const, op: packedDynamicOp },
      { family: 'mixed' as const, op: packedMixedOp },
    ];

    for (const { family, op } of cases) {
      const buffer = createPlannedBuffer(family, op.callsitePlan.SpanBufferClass, op.metadata);
      expectFamilyShape(buffer, family);
      expectPhysicalShape(buffer, 'packed');
      if (buffer._rowHeaders === undefined) throw new Error('Expected packed row headers');
      if (family === 'static-only') expect('message_values' in buffer).toBe(false);
      else expect(buffer.message_values).toBeInstanceOf(Array);

      op.callsitePlan.appenders.writeSpanStart(buffer, 0);
      expect(buffer._rowHeaders[0] & 0xff).toBe(ENTRY_TYPE_SPAN_START);
      expect(buffer._rowHeaders[0] >>> 8).toBe(1);
      expect(buffer._rowHeaders[1] & 0xff).toBe(ENTRY_TYPE_SPAN_EXCEPTION);
      expect(resolveEntryType(buffer, 0)).toBe(ENTRY_TYPE_SPAN_START);
      expect(resolveMessage(buffer, 0)).toBe(denseZeroText);

      if (family !== 'static-only') {
        const nullRow = op.callsitePlan.appenders.writeLogEntry(buffer, ENTRY_TYPE_DEBUG);
        expect(buffer._rowHeaders[nullRow]).toBe(ENTRY_TYPE_DEBUG);
        expect(resolveMessage(buffer, nullRow)).toBeUndefined();
        buffer.message(nullRow, `raw-${family}`);
        expect(buffer._rowHeaders[nullRow] >>> 8).toBe(0);
        expect(resolveMessage(buffer, nullRow)).toBe(`raw-${family}`);
      }
    }
  });

  it('accepts the maximum packed dense index, rejects the next index, and preserves topology', () => {
    const root = createPlannedBuffer('mixed', packedMixedOp.callsitePlan.SpanBufferClass, packedMixedOp.metadata);
    if (root._rowHeaders === undefined) throw new Error('Expected root packed row headers');
    packedMixedOp.callsitePlan.appenders.writeSpanStart(root, MAX_PACKED_MESSAGE_DENSE_INDEX);
    expect(root._rowHeaders[0] >>> 8).toBe(0x00ffffff);
    expect(() => packedMixedOp.callsitePlan.appenders.writeSpanStart(root, MAX_PACKED_MESSAGE_DENSE_INDEX + 1)).toThrow(
      '0xFFFFFE',
    );

    const child = createChildSpanBuffer(
      root,
      packedMixedOp.callsitePlan.SpanBufferClass,
      packedMixedOp.metadata,
      packedMixedOp.metadata,
      CAPACITY,
    );
    const overflow = createOverflowBuffer(child);
    for (const buffer of [root, child, overflow]) {
      expect(buffer.constructor).toBe(packedMixedOp.callsitePlan.SpanBufferClass);
      expectPhysicalShape(buffer, 'packed');
      expectFamilyShape(buffer, 'mixed');
    }
    expect(child._parent).toBe(root);
    expect(overflow._parent).toBe(root);
    expect(overflow._identity).toBe(child._identity);
  });

  it('keeps packed lifecycle, facts, and semantic message resolution identical', () => {
    const denseZeroText = textAtDenseZero();
    const root = createPlannedBuffer('mixed', packedMixedOp.callsitePlan.SpanBufferClass, packedMixedOp.metadata);
    const rootContext = new packedMixedOp.callsitePlan.SpanContextClass(root, runtimeSchema, packedMixedOp.callsitePlan);
    packedMixedOp.callsitePlan.appenders.writeSpanStart(root, 'packed root');
    rootContext._spanLogger._infoTemplate(0);
    rootContext._spanLogger.debug('packed raw');
    packedMixedOp.callsitePlan.appenders.writeSpanEnd(root, ENTRY_TYPE_SPAN_OK);

    expect(Array.from({ length: root._writeIndex }, (_, row) => resolveEntryType(root, row))).toEqual([
      ENTRY_TYPE_SPAN_START,
      ENTRY_TYPE_SPAN_OK,
      ENTRY_TYPE_INFO,
      ENTRY_TYPE_DEBUG,
    ]);
    expect(Array.from({ length: root._writeIndex }, (_, row) => resolveMessage(root, row))).toEqual([
      'packed root',
      undefined,
      denseZeroText,
      'packed raw',
    ]);
    expect(Array.from(extractFacts(root, { includeMetrics: false }))).toEqual([
      'span:packed root: started',
      `log:info: ${denseZeroText}`,
      'log:debug: packed raw',
      'span:packed root: ok',
    ]);
  });

  it('omits every unused capacity lane and exposes exact JS storage bytes for all family/mode pairs', () => {
    const cases = [
      { family: 'static-only' as const, mode: 'current' as const, op: staticOp },
      { family: 'dynamic-only' as const, mode: 'current' as const, op: dynamicOp },
      { family: 'mixed' as const, mode: 'current' as const, op: mixedOp },
      { family: 'static-only' as const, mode: 'specialized' as const, op: specializedStaticOp },
      { family: 'dynamic-only' as const, mode: 'specialized' as const, op: specializedDynamicOp },
      { family: 'mixed' as const, mode: 'specialized' as const, op: specializedMixedOp },
      { family: 'static-only' as const, mode: 'packed' as const, op: packedStaticOp },
      { family: 'dynamic-only' as const, mode: 'packed' as const, op: packedDynamicOp },
      { family: 'mixed' as const, mode: 'packed' as const, op: packedMixedOp },
    ];
    for (const { family, mode, op } of cases) {
      const buffer = createPlannedBuffer(family, op.callsitePlan.SpanBufferClass, op.metadata);
      expectFamilyShape(buffer, family);
      expectPhysicalShape(buffer, mode);
      const timestampAndEntryBytes = CAPACITY * 9;
      let expectedSystemBytes: number;
      if (mode === 'packed') {
        expectedSystemBytes = (CAPACITY * 12 + 7) & ~7;
      } else if (family === 'dynamic-only') {
        expectedSystemBytes = (timestampAndEntryBytes + 7) & ~7;
      } else if (mode === 'current') {
        const messageIdOffset = (timestampAndEntryBytes + 1) & ~1;
        expectedSystemBytes = (messageIdOffset + CAPACITY * Uint16Array.BYTES_PER_ELEMENT + 7) & ~7;
        expect(buffer._messageIds?.byteLength).toBe(CAPACITY * Uint16Array.BYTES_PER_ELEMENT);
      } else {
        const denseOffset = (timestampAndEntryBytes + 3) & ~3;
        expectedSystemBytes = (denseOffset + CAPACITY * Uint32Array.BYTES_PER_ELEMENT + 7) & ~7;
        expect(buffer._logHeaders?.byteLength).toBe(CAPACITY * Uint32Array.BYTES_PER_ELEMENT);
      }
      expect(buffer._system.byteLength - buffer._identity.byteLength).toBe(expectedSystemBytes);
      if (family !== 'static-only') expect(buffer.message_values?.length).toBe(CAPACITY);
    }
  });

  it('keeps empty-dictionary current plans on ID zero with raw fallback and no validity sidecar', () => {
    const root = createPlannedBuffer(
      'mixed',
      fallbackCurrentOp.callsitePlan.SpanBufferClass,
      fallbackCurrentOp.metadata,
    );
    expect(fallbackCurrentOp.callsitePlan.messagePhysicalLayout).toBe('current');
    expect(fallbackCurrentOp.callsitePlan.localMessageDictionary).toEqual([]);
    const rootContext = new fallbackCurrentOp.callsitePlan.SpanContextClass(
      root,
      runtimeSchema,
      fallbackCurrentOp.callsitePlan,
    );
    fallbackCurrentOp.callsitePlan.appenders.writeSpanStart(root, 'fallback root');
    expect(() => rootContext._spanLogger._infoTemplate(0)).not.toThrow();
    const row = root._writeIndex - 1;
    if (root._messageIds === undefined || root.message_values === undefined) {
      throw new Error('Expected fallback current ID and raw lanes');
    }
    expect('message_nulls' in root).toBe(false);
    expect(root._messageIds[row]).toBe(0);
    expect(root.message_values[row]).toBe(textAtDenseZero());
    expect(resolveMessage(root, row)).toBe(textAtDenseZero());
  });

  it('shares one frozen current-mode local dictionary across root, child, and overflow buffers', () => {
    const root = createPlannedBuffer('mixed', mixedOp.callsitePlan.SpanBufferClass, mixedOp.metadata);
    const child = createChildSpanBuffer(
      root,
      mixedOp.callsitePlan.SpanBufferClass,
      mixedOp.metadata,
      mixedOp.metadata,
      CAPACITY,
    );
    const overflow = createOverflowBuffer(child);
    const dictionary = mixedOp.callsitePlan.localMessageDictionary;
    expect(dictionary).toEqual([0]);
    expect(Object.isFrozen(dictionary)).toBe(true);
    for (const buffer of [root, child, overflow]) {
      const plan = buffer._opMetadata._physicalLayoutPlan;
      if (!plan) throw new Error('Expected buffer physical layout plan');
      expect(plan.localMessageDictionary).toBe(dictionary);
      expect(Object.hasOwn(buffer, '_messageDictionary')).toBe(false);
      expect('_messageDictionary' in buffer).toBe(false);
      if (buffer.entry_type === undefined || buffer._messageIds === undefined) {
        throw new Error('Expected current local-ID and entry-type lanes');
      }
      expect('message_nulls' in buffer).toBe(false);
      buffer.entry_type[0] = ENTRY_TYPE_INFO;
      buffer._messageIds[0] = 1;
      buffer._writeIndex = 1;
      expect(resolveMessage(buffer, 0)).toBe(textAtDenseZero());
    }
  });

  it('uses zero as the specialized raw/null sentinel and dense-plus-one for every static identity', () => {
    const buffer = createPlannedBuffer(
      'mixed',
      specializedMixedOp.callsitePlan.SpanBufferClass,
      specializedMixedOp.metadata,
    );
    expectPhysicalShape(buffer, 'specialized');
    if (buffer.entry_type === undefined || buffer._logHeaders === undefined || buffer.message_values === undefined) {
      throw new Error('Expected specialized entry, dense, and raw lanes');
    }
    expect('message_nulls' in buffer).toBe(false);
    buffer.entry_type[0] = ENTRY_TYPE_INFO;
    buffer._logHeaders[0] = 1;
    buffer.entry_type[1] = ENTRY_TYPE_DEBUG;
    buffer.message(1, '');
    buffer.entry_type[2] = ENTRY_TYPE_DEBUG;
    buffer.message(2, 'specialized raw');
    buffer.entry_type[3] = ENTRY_TYPE_DEBUG;
    buffer._logHeaders[4] = MAX_PACKED_MESSAGE_DENSE_INDEX + 1;
    buffer._writeIndex = 5;

    expect(resolveMessage(buffer, 0)).toBe(textAtDenseZero());
    expect(resolveMessage(buffer, 1)).toBe('');
    expect(resolveMessage(buffer, 2)).toBe('specialized raw');
    expect(resolveMessage(buffer, 3)).toBeUndefined();
    expect(buffer._logHeaders[4]).toBe(0x00ffffff);
  });

  it('keeps same-plan JS buffers isolated across stale raw to static, null, and fresh raw transitions', () => {
    for (const { mode, op } of [
      { mode: 'current' as const, op: mixedOp },
      { mode: 'specialized' as const, op: specializedMixedOp },
    ]) {
      const staleRaw = createPlannedBuffer('mixed', op.callsitePlan.SpanBufferClass, op.metadata);
      if (staleRaw.entry_type === undefined) throw new Error(`Expected ${mode} entry-type lane`);
      staleRaw.entry_type[0] = ENTRY_TYPE_DEBUG;
      staleRaw.message(0, 'stale raw');
      expect(resolveMessage(staleRaw, 0)).toBe('stale raw');

      const staticRow = createPlannedBuffer('mixed', op.callsitePlan.SpanBufferClass, op.metadata);
      expect(staticRow.constructor).toBe(staleRaw.constructor);
      if (staticRow.entry_type === undefined) throw new Error(`Expected ${mode} entry-type lane`);
      staticRow.entry_type[0] = ENTRY_TYPE_INFO;
      expect(staticRow.message_values?.[0]).toBeUndefined();
      if (mode === 'current') {
        if (staticRow._messageIds === undefined) throw new Error('Expected current local-ID lane');
        staticRow._messageIds[0] = 1;
        expect(staticRow._messageIds[0]).toBe(1);
      } else {
        if (staticRow._logHeaders === undefined) throw new Error('Expected specialized dense lane');
        staticRow._logHeaders[0] = 1;
        expect(staticRow._logHeaders[0]).toBe(1);
      }
      expect(resolveMessage(staticRow, 0)).toBe(textAtDenseZero());

      const nullRow = createPlannedBuffer('mixed', op.callsitePlan.SpanBufferClass, op.metadata);
      if (nullRow.entry_type === undefined) throw new Error(`Expected ${mode} entry-type lane`);
      nullRow.entry_type[0] = ENTRY_TYPE_DEBUG;
      expect(nullRow.message_values?.[0]).toBeUndefined();
      expect(resolveMessage(nullRow, 0)).toBeUndefined();
      if (mode === 'current') expect(nullRow._messageIds?.[0]).toBe(0);
      else expect(nullRow._logHeaders?.[0]).toBe(0);

      const freshRaw = createPlannedBuffer('mixed', op.callsitePlan.SpanBufferClass, op.metadata);
      if (freshRaw.entry_type === undefined) throw new Error(`Expected ${mode} entry-type lane`);
      freshRaw.entry_type[0] = ENTRY_TYPE_DEBUG;
      freshRaw.message(0, '');
      expect(resolveMessage(freshRaw, 0)).toBe('');
      if (mode === 'current') expect(freshRaw._messageIds?.[0]).toBe(0);
      else expect(freshRaw._logHeaders?.[0]).toBe(0);
      expect('message_nulls' in freshRaw).toBe(false);
    }
  });

  it('starts every new JS root, child, and overflow identity lane at the zero null/raw sentinel', () => {
    for (const { mode, op } of [
      { mode: 'current' as const, op: mixedOp },
      { mode: 'specialized' as const, op: specializedMixedOp },
    ]) {
      const root = createPlannedBuffer('mixed', op.callsitePlan.SpanBufferClass, op.metadata);
      const child = createChildSpanBuffer(
        root,
        op.callsitePlan.SpanBufferClass,
        op.metadata,
        op.metadata,
        CAPACITY,
      );
      const overflow = createOverflowBuffer(child);

      for (const [kind, buffer] of [
        ['root', root],
        ['child', child],
        ['overflow', overflow],
      ] as const) {
        if (buffer.entry_type === undefined || buffer.message_values === undefined) {
          throw new Error(`Expected ${mode} ${kind} entry-type and raw message lanes`);
        }
        const identityLane = mode === 'current' ? buffer._messageIds : buffer._logHeaders;
        if (identityLane === undefined) throw new Error(`Expected ${mode} ${kind} identity lane`);

        expect(identityLane.every((identity) => identity === 0)).toBe(true);
        expect(buffer.message_values[0]).toBeUndefined();
        buffer.entry_type[0] = ENTRY_TYPE_DEBUG;
        expect(resolveMessage(buffer, 0)).toBeUndefined();

        buffer.entry_type[1] = ENTRY_TYPE_DEBUG;
        buffer.message(1, '');
        expect(identityLane[1]).toBe(0);
        expect(resolveMessage(buffer, 1)).toBe('');
      }
    }
  });

  it('zero-initializes every fresh physical lane across exact capacities, layouts, and allocation kinds', () => {
    for (const capacity of [3, 8]) {
      for (const { mode, op } of [
        { mode: 'current' as const, op: mixedOp },
        { mode: 'specialized' as const, op: specializedMixedOp },
        { mode: 'packed' as const, op: packedMixedOp },
      ]) {
        const SpanBufferClass = op.callsitePlan.SpanBufferClass;
        const root = createSpanBuffer(
          runtimeSchema,
          createTestTraceRoot(`zero-${mode}-${capacity}`),
          op.metadata,
          capacity,
          SpanBufferClass,
        );
        const child = createChildSpanBuffer(root, SpanBufferClass, op.metadata, op.metadata, capacity);
        const overflow = new SpanBufferClass(
          capacity,
          SpanBufferClass.stats,
          child,
          true,
          child._callsiteMetadata,
          child._opMetadata,
          child._traceRoot,
          child._vocabularyGeneration,
        );

        expect(child._identity).not.toBe(root._identity);
        expect(overflow._identity).toBe(child._identity);
        for (const [kind, buffer] of [
          ['root', root],
          ['child', child],
          ['overflow', overflow],
        ] as const) {
          expect(buffer._capacity).toBe(capacity);
          expect(buffer._writeIndex).toBe(0);
          expect(buffer.constructor).toBe(SpanBufferClass);
          expect(buffer._opMetadata).toBe(op.metadata);
          expect(buffer.timestamp.every((value) => value === 0n)).toBe(true);

          if (mode === 'packed') {
            const rowHeaders = buffer._rowHeaders;
            if (rowHeaders === undefined) throw new Error(`Expected ${kind} packed row headers`);
            expect(rowHeaders.every((value) => value === 0)).toBe(true);
            expect('entry_type' in buffer).toBe(false);
            expect('_messageIds' in buffer).toBe(false);
            expect('_logHeaders' in buffer).toBe(false);
          } else {
            const entryTypes = buffer.entry_type;
            if (entryTypes === undefined) throw new Error(`Expected ${kind} ${mode} entry types`);
            expect(entryTypes.every((value) => value === 0)).toBe(true);
            expect('_rowHeaders' in buffer).toBe(false);
            if (mode === 'current') {
              const messageIds = buffer._messageIds;
              if (messageIds === undefined) throw new Error(`Expected ${kind} current message IDs`);
              expect(messageIds.every((value) => value === 0)).toBe(true);
              expect('_logHeaders' in buffer).toBe(false);
            } else {
              const logHeaders = buffer._logHeaders;
              if (logHeaders === undefined) throw new Error(`Expected ${kind} specialized log headers`);
              expect(logHeaders.every((value) => value === 0)).toBe(true);
              expect('_messageIds' in buffer).toBe(false);
            }
          }
        }
      }
    }
  });

  it('derives Arrow null, raw, and static messages across current and specialized root, child, and overflow', () => {
    const denseZeroText = textAtDenseZero();
    for (const { mode, op } of [
      { mode: 'current' as const, op: mixedOp },
      { mode: 'specialized' as const, op: specializedMixedOp },
    ]) {
      const root = createPlannedBuffer('mixed', op.callsitePlan.SpanBufferClass, op.metadata);
      const rootContext = new op.callsitePlan.SpanContextClass(root, runtimeSchema, op.callsitePlan);
      op.callsitePlan.appenders.writeSpanStart(root, `${mode} root`);
      rootContext._spanLogger._infoTemplate(0);
      rootContext._spanLogger.debug(`${mode} root raw`);
      op.callsitePlan.appenders.writeLogEntry(root, ENTRY_TYPE_DEBUG);
      op.callsitePlan.appenders.writeSpanEnd(root, ENTRY_TYPE_SPAN_OK);

      const child = createChildSpanBuffer(
        root,
        op.callsitePlan.SpanBufferClass,
        op.metadata,
        op.metadata,
        CAPACITY,
      );
      const childContext = new op.callsitePlan.SpanContextClass(child, runtimeSchema, op.callsitePlan);
      op.callsitePlan.appenders.writeSpanStart(child, `${mode} child`);
      childContext._spanLogger._infoTemplate(0);
      childContext._spanLogger.debug(`${mode} child raw`);
      op.callsitePlan.appenders.writeLogEntry(child, ENTRY_TYPE_DEBUG);
      op.callsitePlan.appenders.writeSpanEnd(child, ENTRY_TYPE_SPAN_OK);

      const overflow = createOverflowBuffer(child);
      const overflowContext = new op.callsitePlan.SpanContextClass(overflow, runtimeSchema, op.callsitePlan);
      overflowContext._spanLogger._infoTemplate(0);
      overflowContext._spanLogger.debug(`${mode} overflow raw`);
      op.callsitePlan.appenders.writeLogEntry(overflow, ENTRY_TYPE_DEBUG);

      const table = convertSpanTreeToArrowTable(root);
      const messages = table.getChild('message');
      if (!messages) throw new Error('Expected Arrow message column');
      expect(messages.nullCount).toBe(5);
      expect(Array.from({ length: table.numRows }, (_, row) => messages.get(row))).toEqual([
        `${mode} root`,
        null,
        denseZeroText,
        `${mode} root raw`,
        null,
        `${mode} child`,
        null,
        denseZeroText,
        `${mode} child raw`,
        null,
        denseZeroText,
        `${mode} overflow raw`,
        null,
      ]);
      expect('message_nulls' in root).toBe(false);
      expect('message_nulls' in child).toBe(false);
      expect('message_nulls' in overflow).toBe(false);
    }
  });

  it('represents dense index zero, dynamic/static span names, null/raw rows, and terminal scalars', () => {
    const denseZeroText = textAtDenseZero();
    const cases = [
      { family: 'static-only' as const, op: staticOp },
      { family: 'dynamic-only' as const, op: dynamicOp },
      { family: 'mixed' as const, op: mixedOp },
    ];

    for (const { family, op } of cases) {
      const denseName = createPlannedBuffer(family, op.callsitePlan.SpanBufferClass, op.metadata);
      op.callsitePlan.appenders.writeSpanStart(denseName, 0);
      expect(resolveEntryType(denseName, 0)).toBe(ENTRY_TYPE_SPAN_START);
      expect(resolveMessage(denseName, 0)).toBe(denseZeroText);
      expectPhysicalShape(denseName, 'current');
      expect(Object.hasOwn(denseName, '_messageDictionary')).toBe(false);

      const dynamicName = createPlannedBuffer(family, op.callsitePlan.SpanBufferClass, op.metadata);
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
      const root = createPlannedBuffer(family, op.callsitePlan.SpanBufferClass, op.metadata);
      const child = createChildSpanBuffer(root, op.callsitePlan.SpanBufferClass, op.metadata, op.metadata, CAPACITY);
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
    const root = createPlannedBuffer('mixed', mixedOp.callsitePlan.SpanBufferClass, mixedOp.metadata);
    const rootContext = new mixedOp.callsitePlan.SpanContextClass(root, runtimeSchema, mixedOp.callsitePlan);
    mixedOp.callsitePlan.appenders.writeSpanStart(root, 'mixed root');
    const rootLogger = rootContext._spanLogger;
    rootLogger._infoTemplate(0);
    rootLogger.debug('root raw');
    mixedOp.callsitePlan.appenders.writeSpanEnd(root, ENTRY_TYPE_SPAN_OK);

    const child = createChildSpanBuffer(
      root,
      staticOp.callsitePlan.SpanBufferClass,
      staticOp.metadata,
      staticOp.metadata,
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

  it('preserves overflow order, reserved-row formula, identities, and stats across plugin and physical modes', () => {
    const denseZeroText = textAtDenseZero();
    const cases: Array<{
      name: string;
      op: typeof mixedOp;
      physicalLayout: MessagePhysicalLayout;
      compiledTemplates: boolean;
    }> = [
      { name: 'plugin-off-current', op: fallbackCurrentOp, physicalLayout: 'current', compiledTemplates: false },
      { name: 'plugin-on-current', op: mixedOp, physicalLayout: 'current', compiledTemplates: true },
      {
        name: 'plugin-on-specialized',
        op: specializedMixedOp,
        physicalLayout: 'specialized',
        compiledTemplates: true,
      },
      { name: 'plugin-on-packed', op: packedMixedOp, physicalLayout: 'packed', compiledTemplates: true },
    ];

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
          for (const testCase of cases) {
            const stats = testCase.op.callsitePlan.SpanBufferClass.stats;
            stats.capacity = CAPACITY;
            stats.totalWrites = 0;
            stats.spansCreated = 0;
            const root = createPlannedBuffer('mixed', testCase.op.callsitePlan.SpanBufferClass, testCase.op.metadata);
            const rootContext = new testCase.op.callsitePlan.SpanContextClass(
              root,
              runtimeSchema,
              testCase.op.callsitePlan,
            );
            testCase.op.callsitePlan.appenders.writeSpanStart(root, `${testCase.name} root`);
            const logger = rootContext._spanLogger;
            for (const operation of operations) {
              if (operation.kind === 'static') {
                if (testCase.compiledTemplates) logger._infoTemplate(0);
                else logger.info(operation.text);
              } else {
                logger.debug(operation.text);
              }
            }
            testCase.op.callsitePlan.appenders.writeSpanEnd(root, ENTRY_TYPE_SPAN_OK);

            const actual = collectLogRows(root);
            expect(actual.map((row) => row.message)).toEqual(operations.map((operation) => operation.text));
            expect(actual.map((row) => row.entryType)).toEqual(
              operations.map((operation) => (operation.kind === 'static' ? ENTRY_TYPE_INFO : ENTRY_TYPE_DEBUG)),
            );
            expect(resolveEntryType(root, 1)).toBe(ENTRY_TYPE_SPAN_OK);

            const segments: AnySpanBuffer[] = [];
            for (let segment: AnySpanBuffer | undefined = root; segment; segment = segment._overflow) {
              segments.push(segment);
              expectFamilyShape(segment, 'mixed');
              expectPhysicalShape(segment, testCase.physicalLayout);
              expect(segment.span_id).toBe(root.span_id);
              expect(segment.trace_id).toBe(root.trace_id);
              expect(segment.parent_span_id).toBe(0);
            }
            expect(segments).toHaveLength(1 + Math.ceil((operations.length - (CAPACITY - 2)) / CAPACITY));
            expect(segments[0]._writeIndex).toBe(CAPACITY);
            let remaining = operations.length - (CAPACITY - 2);
            for (let index = 1; index < segments.length; index++) {
              expect(segments[index]._writeIndex).toBe(Math.min(remaining, CAPACITY));
              remaining -= CAPACITY;
            }
            expect(stats.totalWrites).toBe(operations.length);
            expect(stats.spansCreated).toBe(1);
          }
        },
      ),
      { numRuns: 30 },
    );
  });
});
