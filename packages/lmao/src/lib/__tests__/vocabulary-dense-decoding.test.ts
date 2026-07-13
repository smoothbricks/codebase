import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { convertToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { resolveMessage } from '../resolveMessage.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_TRACE,
  ENTRY_TYPE_WARN,
} from '../schema/systemSchema.js';
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

const schema = defineLogSchema({ marker: S.category() });
const opContext = defineOpContext({ logSchema: schema });


describe('global vocabulary dense decoding', () => {
  it('packs registered dense bindings for every level while dynamic rows stay in the raw message lane', async () => {
    const messages = [
      'dense info literal',
      'dense debug literal',
      'dense warn literal',
      'dense error literal',
      'dense trace literal',
    ];
    const binding = registerVocabularyFragment(makeFragment(messages));
    const op = opContext.defineOp('dense-levels', (ctx) => {
      ctx.log._infoTemplate(binding[0]);
      ctx.log._debugTemplate(binding[1]);
      ctx.log._warnTemplate(binding[2]);
      ctx.log._errorTemplate(binding[3]);
      ctx.log._traceTemplate(binding[4]);
      ctx.log.info('dynamic message');
      return ctx.ok(null);
    });
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    await tracer.trace('dense-levels', op);
    const buffer = tracer.rootBuffers[0];

    const entryTypes = [ENTRY_TYPE_INFO, ENTRY_TYPE_DEBUG, ENTRY_TYPE_WARN, ENTRY_TYPE_ERROR, ENTRY_TYPE_TRACE];
    expect(Array.from(buffer._logHeaders.subarray(2, 7))).toEqual(
      entryTypes.map((entryType, ordinal) => (binding[ordinal] << 8) | entryType),
    );
    expect(buffer._logHeaders[7]).toBe(0);
    expect(buffer.message_values.slice(2, 7)).toEqual([undefined, undefined, undefined, undefined, undefined]);
    expect(buffer.message_values[7]).toBe('dynamic message');
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
      for (let index = 0; index < 20; index++) ctx.log._infoTemplate(firstBinding[0]);
      return ctx.ok(null);
    });
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    await tracer.trace('dense-overflow-generation', op);
    const buffer = tracer.rootBuffers[0];

    registerVocabularyFragment(makeFragment(['registered after buffer creation']));

    const table = convertToArrowTable(buffer);
    const messageColumn = table.getChild('message');
    if (!messageColumn) throw new Error('Arrow table did not contain message column');
    expect(Array.from({ length: 20 }, (_, index) => messageColumn.get(index + 2))).toEqual(
      Array.from({ length: 20 }, () => 'pinned generation literal'),
    );
  });

  it('rejects a packed dense index outside the buffer generation', async () => {
    let invalidDenseIndex = 0;
    const op = opContext.defineOp('invalid-dense-index', (ctx) => {
      invalidDenseIndex = ctx.buffer._vocabularyGeneration.ids.length;
      ctx.log._infoTemplate(invalidDenseIndex);
      return ctx.ok(null);
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
