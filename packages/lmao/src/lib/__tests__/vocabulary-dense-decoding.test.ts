import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { Column, type Table } from '@uwdata/flechette';
import { getVocabularyDictionaryPrefix } from '../arrow/vocabularyDictionary.js';
import {
  convertSpanTreeToLeasedArrowTable,
  convertToArrowTable,
  convertToLeasedArrowTable,
} from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { resolveMessage } from '../resolveMessage.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED,
  RUNTIME_HINT_RESULT,
} from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { SpanBuffer } from '../types.js';
import {
  getVocabularyGeneration,
  registerVocabularyFragment,
  type VocabularyFragment,
} from '../vocabularyRegistry.js';
import { createTestTracerOptions } from './test-helpers.js';

const encoder = new TextEncoder();

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

function makeFragment(texts: readonly string[]): VocabularyFragment {
  const records = texts.map(encodeRecord);
  const offsets = new Int32Array(records.length + 1);
  const ids = new Uint32Array(records.length);
  let byteLength = 0;
  for (let ordinal = 0; ordinal < records.length; ordinal++) {
    const record = records[ordinal];
    byteLength += record.length;
    offsets[ordinal + 1] = byteLength;
    const digest = createHash('sha256').update(Uint8Array.of(1)).update(record).digest();
    ids[ordinal] = (digest[0] << 16) | (digest[1] << 8) | digest[2];
  }
  const utf8 = new Uint8Array(byteLength);
  let offset = 0;
  for (const record of records) {
    utf8.set(record, offset);
    offset += record.length;
  }
  const fragment: Omit<VocabularyFragment, 'contentHash'> = {
    schemaVersion: 1,
    idAlgorithm: 'sha256-24-v1',
    ids,
    kindTags: new Uint8Array(texts.length).fill(1),
    utf8,
    offsets,
  };
  return { ...fragment, contentHash: fragmentHash(fragment) };
}

function requireMessageDictionary(table: Table): {
  readonly message: Column<unknown>;
  readonly dictionary: Column<unknown>;
  readonly keys: Uint8Array | Uint16Array | Uint32Array;
} {
  const message = table.getChild('message');
  if (!message) throw new Error('Arrow table did not contain message column');
  const batch = message.data[0];
  if (!batch || !('dictionary' in batch) || !(batch.dictionary instanceof Column)) {
    throw new Error('Arrow message column was not dictionary encoded');
  }
  if (!(batch.values instanceof Uint8Array || batch.values instanceof Uint16Array || batch.values instanceof Uint32Array)) {
    throw new Error('Arrow message dictionary did not use unsigned integer keys');
  }
  return { dictionary: batch.dictionary, keys: batch.values, message };
}

const schema = defineLogSchema({ marker: S.category() });
const opContext = defineOpContext({ logSchema: schema });

interface DenseTemplateLogger {
  _infoTemplate(vocabularyIndex: number): unknown;
  _debugTemplate(vocabularyIndex: number): unknown;
  _warnTemplate(vocabularyIndex: number): unknown;
  _errorTemplate(vocabularyIndex: number): unknown;
  _traceTemplate(vocabularyIndex: number): unknown;
}

function isDenseTemplateLogger(value: unknown): value is DenseTemplateLogger {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof Reflect.get(value, '_infoTemplate') === 'function' &&
    typeof Reflect.get(value, '_debugTemplate') === 'function' &&
    typeof Reflect.get(value, '_warnTemplate') === 'function' &&
    typeof Reflect.get(value, '_errorTemplate') === 'function' &&
    typeof Reflect.get(value, '_traceTemplate') === 'function'
  );
}

function requireDenseTemplateLogger(context: object): DenseTemplateLogger {
  const logger = Reflect.get(context, '_spanLogger');
  if (!isDenseTemplateLogger(logger)) throw new Error('Expected SpanContext to own a dense-template logger');
  return logger;
}


describe('global vocabulary dense decoding', () => {
  it('keeps the canonical static prefix and uses direct dense zero before the first-seen dynamic suffix', async () => {
    const op = opContext.defineOp('dense-zero-static-prefix', (ctx) => {
      const logger = requireDenseTemplateLogger(ctx);
      logger._infoTemplate(0);
      logger._infoTemplate(0);
      ctx.log.info('dynamic-b');
      ctx.log.info('dynamic-a');
      ctx.log.info('dynamic-b');
      return ctx.ok(null);
    });
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    await tracer.trace('dense-zero-static-prefix', op);

    const buffer = tracer.rootBuffers[0];
    const lease = convertSpanTreeToLeasedArrowTable(buffer);
    const table = lease.table;
    const { dictionary, keys, message } = requireMessageDictionary(table);
    const prefix = getVocabularyDictionaryPrefix(buffer._vocabularyGeneration);
    const prefixValues = Array.from(prefix.column);
    expect(Array.from(dictionary)).toEqual([
      ...prefixValues,
      'dense-zero-static-prefix',
      'dynamic-b',
      'dynamic-a',
    ]);
    const suffixOffset = prefix.length;
    expect(Array.from(keys)).toEqual([suffixOffset, 0, 0, 0, suffixOffset + 1, suffixOffset + 2, suffixOffset + 1]);
    expect(Array.from({ length: message.length }, (_, row) => message.get(row))).toEqual([
      'dense-zero-static-prefix',
      null,
      prefixValues[0],
      prefixValues[0],
      'dynamic-b',
      'dynamic-a',
      'dynamic-b',
    ]);
    expect(dictionary.data).toHaveLength(2);
    expect(dictionary.data[0]).toBe(getVocabularyDictionaryPrefix(buffer._vocabularyGeneration).column.data[0]);
    tracer.bufferStrategy.releaseBuffer(buffer);
    expect(message.get(4)).toBe('dynamic-b');
    lease.release();
    expect(lease.released).toBe(true);
  });

  it('reuses the pinned cached dictionary object when overflow rows need no dynamic suffix', async () => {
    const op = opContext.defineOp('static-only-overflow', (ctx) => {
      const logger = requireDenseTemplateLogger(ctx);
      for (let index = 0; index < 40; index++) logger._infoTemplate(0);
      return ctx.ok(null);
    });
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    await tracer.trace('static-only-overflow', op);
    const overflow = tracer.rootBuffers[0]._overflow;
    if (!overflow) throw new Error('Expected static log rows to create an overflow segment');

    const lease = convertToLeasedArrowTable(overflow);
    const table = lease.table;
    const { dictionary, keys, message } = requireMessageDictionary(table);
    const prefix = getVocabularyDictionaryPrefix(overflow._vocabularyGeneration);
    expect(dictionary).toBe(prefix.column);
    expect(Array.from(keys)).toEqual(Array.from({ length: message.length }, () => 0));
    expect(Array.from({ length: message.length }, (_, row) => message.get(row))).toEqual(
      Array.from({ length: message.length }, () => prefix.column.get(0)),
    );
    tracer.bufferStrategy.releaseBuffer(overflow);
    expect(dictionary.get(0)).toBe(prefix.column.get(0));
    lease.release();
    expect(lease.released).toBe(true);
  });

  it('keeps registered dense bindings semantic through the default current raw lane', async () => {
    const messages = [
      'dense info literal',
      'dense debug literal',
      'dense warn literal',
      'dense error literal',
      'dense trace literal',
    ];
    const binding = registerVocabularyFragment(makeFragment(messages));
    const op = opContext.defineOp('dense-levels', (ctx) => {
      const logger = requireDenseTemplateLogger(ctx);
      logger._infoTemplate(binding[0]);
      logger._debugTemplate(binding[1]);
      logger._warnTemplate(binding[2]);
      logger._errorTemplate(binding[3]);
      logger._traceTemplate(binding[4]);
      ctx.log.info('dynamic message');
      return ctx.ok(null);
    });
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    await tracer.trace('dense-levels', op);
    const buffer = tracer.rootBuffers[0];
    const localIds = buffer._messageIds;
    const rawMessages = buffer.message_values;
    if (!localIds || !rawMessages) throw new Error('Expected current local-ID and raw lanes');
    expect(buffer._messagePhysicalLayout).toBe('current');
    expect(buffer._opMetadata._physicalLayoutPlan?.localMessageDictionary).toEqual([]);
    expect('_logHeaders' in buffer).toBe(false);
    expect('message_nulls' in buffer).toBe(false);
    expect(Array.from(localIds.subarray(2, 8))).toEqual([0, 0, 0, 0, 0, 0]);
    expect(rawMessages.slice(2, 8)).toEqual([...messages, 'dynamic message']);
    expect(Array.from({ length: 6 }, (_, index) => resolveMessage(buffer, index + 2))).toEqual([
      ...messages,
      'dynamic message',
    ]);

    const table = convertToArrowTable(buffer);
    const messageColumn = table.getChild('message');
    if (!messageColumn) throw new Error('Arrow table did not contain message column');
    expect(Array.from({ length: 6 }, (_, index) => messageColumn.get(index + 2))).toEqual([
      ...messages,
      'dynamic message',
    ]);
  });

  it('keeps overflow rows pinned to their original vocabulary generation after later registration', async () => {
    const firstBinding = registerVocabularyFragment(makeFragment(['pinned generation literal']));
    const op = opContext.defineOp('dense-overflow-generation', (ctx) => {
      const logger = requireDenseTemplateLogger(ctx);
      for (let index = 0; index < 20; index++) logger._infoTemplate(firstBinding[0]);
      return ctx.ok(null);
    });
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    await tracer.trace('dense-overflow-generation', op);
    const buffer = tracer.rootBuffers[0];

    registerVocabularyFragment(makeFragment(['registered after buffer creation']));

    const lease = convertSpanTreeToLeasedArrowTable(buffer);
    const table = lease.table;
    const messageColumn = table.getChild('message');
    if (!messageColumn) throw new Error('Arrow table did not contain message column');
    expect(Array.from({ length: 20 }, (_, index) => messageColumn.get(index + 2))).toEqual(
      Array.from({ length: 20 }, () => 'pinned generation literal'),
    );
    const { dictionary } = requireMessageDictionary(table);
    expect(dictionary.data[0]).toBe(getVocabularyDictionaryPrefix(buffer._vocabularyGeneration).column.data[0]);
    expect(Array.from(dictionary)).toContain('pinned generation literal');
    expect(Array.from(dictionary)).not.toContain('registered after buffer creation');
    expect(lease.released).toBe(false);
    tracer.bufferStrategy.releaseBuffer(buffer);
    expect(() => buffer._traceRoot._topology.assertLive(buffer)).toThrow('stale');
    expect(messageColumn.get(2)).toBe('pinned generation literal');
    lease[Symbol.dispose]();
    expect(lease.released).toBe(true);
    expect(() => buffer._traceRoot._topology.assertLive(buffer)).toThrow('stale');
  });

  it('rejects a packed dense index outside the buffer generation', async () => {
    let invalidDenseIndex = 0;
    const op = opContext.defineOp('invalid-dense-index', (ctx) => {
      const logger = requireDenseTemplateLogger(ctx);
      invalidDenseIndex = ctx.buffer._vocabularyGeneration.ids.length;
      logger._infoTemplate(invalidDenseIndex);
      return ctx.ok(null);
    }, undefined, {
      runtimeHint:
        RUNTIME_HINT_ANALYZED_VALID |
        RUNTIME_HINT_LOG |
        RUNTIME_HINT_RESULT |
        RUNTIME_HINT_MESSAGE_LAYOUT_MIXED |
        RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED |
        3,
    });
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    await tracer.trace('invalid-dense-index', op);
    const buffer = tracer.rootBuffers[0];

    expect(() => resolveMessage(buffer, 2)).toThrow(
      `Invalid vocabulary dense index ${invalidDenseIndex} for generation ${buffer._vocabularyGeneration.generation}`,
    );
  });

  it('deduplicates repeated registration without a new observable generation', () => {
    const fragment = makeFragment(['single registration owner']);
    const firstBinding = registerVocabularyFragment(fragment);
    const generation = getVocabularyGeneration();
    const secondBinding = registerVocabularyFragment(fragment);

    expect(secondBinding).toBe(firstBinding);
    expect(getVocabularyGeneration()).toBe(generation);
  });

  it('leaves child span lifecycle hooks and Promise scheduling unchanged', async () => {
    const lifecycleContext = defineOpContext({ logSchema: schema });
    type LifecycleSchema = (typeof lifecycleContext)['logBinding']['logSchema'];
    class LifecycleTracer extends TestTracer<typeof lifecycleContext> {
      starts = 0;
      ends = 0;

      override onSpanStart(_buffer: SpanBuffer<LifecycleSchema>): void {
        this.starts++;
      }

      override onSpanEnd(_buffer: SpanBuffer<LifecycleSchema>): void {
        this.ends++;
      }
    }

    const child = lifecycleContext.defineOp('child', (ctx) => ctx.ok('child'), undefined, { runtimeHint: 0 });
    const parent = lifecycleContext.defineOp('parent', async (ctx) => {
      const pending = ctx.span('child-call', child);
      expect(pending).toBeInstanceOf(Promise);
      await pending;
      return ctx.ok('parent');
    });
    const tracer = new LifecycleTracer(lifecycleContext, createTestTracerOptions());

    await tracer.trace('root', parent);
    expect(tracer.starts).toBe(1);
    expect(tracer.ends).toBe(1);
    expect(tracer.rootBuffers).toHaveLength(1);
  });
});
