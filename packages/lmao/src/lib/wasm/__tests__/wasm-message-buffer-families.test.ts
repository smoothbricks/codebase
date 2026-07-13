import { beforeEach, describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { createTestOpMetadata, createTestTraceRoot, createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { resolveMessage } from '../../resolveMessage.js';
import type { MessageLayoutFamily } from '../../runtimeHint.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_SPAN_OK,
  ENTRY_TYPE_SPAN_START,
} from '../../schema/systemSchema.js';
import { createOverflowBuffer, createSpanBuffer, getSpanBufferClass } from '../../spanBuffer.js';
import type { AnySpanBuffer, SpanBuffer } from '../../types.js';
import { getVocabularyGeneration } from '../../vocabularyRegistry.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { createWasmAllocator, type WasmAllocator } from '../wasmAllocator.js';
import { createWasmLayoutTemplate, getWasmPhysicalLayout } from '../wasmPhysicalLayout.js';
import {
  createWasmChildSpanBuffer,
  createWasmOverflowBuffer,
  createWasmSpanBuffer,
  getWasmSpanBufferClass,
  type WasmSpanBufferInstance,
} from '../wasmSpanBuffer.js';
import { WasmTraceRoot } from '../wasmTraceRoot.js';
import { EMPTY_SCOPE } from '../../spanBuffer.js';
import { createTraceId } from '../../traceId.js';

const CAPACITY = 8;
const context = defineOpContext({ logSchema: defineLogSchema({ marker: S.category(), count: S.number() }) });
const schema = context.logBinding.logSchema;
const lifecycleTracer = new TestTracer(context, createTestTracerOptions<typeof schema>());

function align(value: number, alignment: number): number {
  return (value + alignment - 1) & ~(alignment - 1);
}

function expectWasmFamilyShape(
  buffer: WasmSpanBufferInstance<typeof schema>,
  family: MessageLayoutFamily,
): void {
  if (buffer._messageLayoutFamily !== family) {
    throw new Error(`Expected ${family} WASM buffer, received ${buffer._messageLayoutFamily}`);
  }
  expect(buffer._layout.messageLayoutFamily).toBe(family);
  expect(buffer._descriptor.layout).toBe(buffer._layout);
  if (family === 'static-only') {
    expect(buffer._logHeaders).toBeInstanceOf(Uint32Array);
    expect('message_values' in buffer).toBe(false);
    expect('message_nulls' in buffer).toBe(false);
    expect(Object.hasOwn(buffer, '_message')).toBe(false);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(true);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(true);
  } else if (family === 'dynamic-only') {
    expect('_logHeaders' in buffer).toBe(false);
    expect(buffer.message_values).toBeInstanceOf(Array);
    expect('message_nulls' in buffer).toBe(false);
    expect(Object.hasOwn(buffer, '_message')).toBe(true);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(true);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(false);
  } else {
    expect(buffer._logHeaders).toBeInstanceOf(Uint32Array);
    expect(buffer.message_values).toBeInstanceOf(Array);
    expect('message_nulls' in buffer).toBe(false);
    expect(Object.hasOwn(buffer, '_message')).toBe(true);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(false);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(false);
  }
}

function collectMessages(root: AnySpanBuffer): Array<string | undefined> {
  const messages: Array<string | undefined> = [];
  let segment: AnySpanBuffer | undefined = root;
  let first = true;
  while (segment) {
    for (let row = first ? 2 : 0; row < segment._writeIndex; row++) messages.push(resolveMessage(segment, row));
    first = false;
    segment = segment._overflow;
  }
  return messages;
}

describe('WASM specialized message buffer families', () => {
  let allocator: WasmAllocator;
  let traceRoot: WasmTraceRoot<typeof schema>;
  const metadata = createTestOpMetadata({ name: 'wasm-message-family' });

  beforeEach(async () => {
    allocator = await createWasmAllocator({ capacity: 64 });
    allocator.reset();
    allocator.setThreadId(0, 42);
    traceRoot = new WasmTraceRoot<typeof schema>(allocator, createTraceId('wasm-message-family'), lifecycleTracer);
  });

  function createBuffer(family: MessageLayoutFamily): WasmSpanBufferInstance<typeof schema> {
    return createWasmSpanBuffer(
      schema,
      {
        allocator,
        capacity: CAPACITY,
        messageLayoutFamily: family,
        trace_id: createTraceId(`wasm-${family}`),
        thread_id: 42n,
        span_id: 1,
      },
      traceRoot,
      EMPTY_SCOPE,
      metadata,
      metadata,
    );
  }

  it('caches distinct family classes and exact byte descriptors for arbitrary capacities', () => {
    expect(getWasmSpanBufferClass(schema, 'static-only')).toBe(getWasmSpanBufferClass(schema, 'static-only'));
    expect(new Set([
      getWasmSpanBufferClass(schema, 'static-only'),
      getWasmSpanBufferClass(schema, 'dynamic-only'),
      getWasmSpanBufferClass(schema, 'mixed'),
    ]).size).toBe(3);
    const staticTemplate = createWasmLayoutTemplate(schema, 'static-only');
    const dynamicTemplate = createWasmLayoutTemplate(schema, 'dynamic-only');
    const mixedTemplate = createWasmLayoutTemplate(schema, 'mixed');
    expect(staticTemplate).toBe(createWasmLayoutTemplate(schema, 'static-only'));
    expect(new Set([staticTemplate, dynamicTemplate, mixedTemplate]).size).toBe(3);

    fc.assert(
      fc.property(fc.integer({ min: 1, max: 512 }), (capacity) => {
        const staticLayout = getWasmPhysicalLayout(schema, capacity, 'static-only');
        const dynamicLayout = getWasmPhysicalLayout(schema, capacity, 'dynamic-only');
        const mixedLayout = getWasmPhysicalLayout(schema, capacity, 'mixed');
        const headerOffset = align(capacity * 9, Uint32Array.BYTES_PER_ELEMENT);

        expect(staticLayout).toBe(staticTemplate.forCapacity(capacity));
        expect(dynamicLayout).toBe(dynamicTemplate.forCapacity(capacity));
        expect(mixedLayout).toBe(mixedTemplate.forCapacity(capacity));
        expect(staticLayout.messageLayoutFamily).toBe('static-only');
        expect(dynamicLayout.messageLayoutFamily).toBe('dynamic-only');
        expect(mixedLayout.messageLayoutFamily).toBe('mixed');
        expect(staticLayout.system.logHeaderOffset).toBe(headerOffset);
        expect(mixedLayout.system.logHeaderOffset).toBe(headerOffset);
        expect(dynamicLayout.system.logHeaderOffset).toBeUndefined();
        expect(dynamicLayout.system.byteLength).toBe(capacity * 9);
        expect(staticLayout.system.byteLength).toBe(headerOffset + capacity * Uint32Array.BYTES_PER_ELEMENT);
        expect(mixedLayout.system.byteLength).toBe(staticLayout.system.byteLength);
        expect(staticLayout.columns).toEqual(dynamicLayout.columns);
        expect(dynamicLayout.columns).toEqual(mixedLayout.columns);
        expect(staticLayout.slabs).toEqual(dynamicLayout.slabs);
        expect(dynamicLayout.slabs).toEqual(mixedLayout.slabs);
      }),
      { numRuns: 100 },
    );
  });

  it('allocates only the selected lanes and preserves dense-zero/null/raw semantics', () => {
    if (getVocabularyGeneration().ids.length === 0) throw new Error('Expected the test vocabulary to contain dense zero');
    for (const family of ['static-only', 'dynamic-only', 'mixed'] as const) {
      const buffer = createBuffer(family);
      expectWasmFamilyShape(buffer, family);
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer._writeIndex = 2;
      if (family === 'dynamic-only') buffer._spanName = 0;
      else {
        if (!buffer._logHeaders) throw new Error('Expected WASM header lane');
        buffer._logHeaders[0] = ENTRY_TYPE_SPAN_START;
      }
      expect(resolveMessage(buffer, 0)).toBeDefined();
      expect(resolveMessage(buffer, 1)).toBeUndefined();

      if (family === 'static-only') {
        expect(() => buffer.message(2, 'raw')).toThrow('rows 0 and 1');
      } else {
        buffer.entry_type[2] = ENTRY_TYPE_DEBUG;
        buffer._writeIndex = 3;
        expect(resolveMessage(buffer, 2)).toBeUndefined();
        buffer.message(2, `raw ${family}`);
        expect(resolveMessage(buffer, 2)).toBe(`raw ${family}`);
      }
    }
  });

  it('propagates family, pointers, and omissions through child and overflow descriptors', () => {
    for (const family of ['static-only', 'dynamic-only', 'mixed'] as const) {
      const root = createBuffer(family);
      const child = createWasmChildSpanBuffer(
        root,
        { allocator, capacity: CAPACITY, messageLayoutFamily: family, thread_id: 42n, span_id: 2 },
        traceRoot,
        EMPTY_SCOPE,
        metadata,
        metadata,
      );
      const overflow = createWasmOverflowBuffer(child, traceRoot, EMPTY_SCOPE, metadata, metadata);
      expect(child.constructor).toBe(getWasmSpanBufferClass(schema, family));
      expect(overflow.constructor).toBe(child.constructor);
      expect(child._descriptor.parent).toBe(root._descriptor);
      expect(overflow._descriptor.parent).toBe(root._descriptor);
      expect(overflow._identityPtr).toBe(child._identityPtr);
      expect(overflow._layout).toBe(child._layout);
      expectWasmFamilyShape(child, family);
      expectWasmFamilyShape(overflow, family);
    }
  });

  it('keeps JS and WASM decoded rows identical across random mixed overflow streams', () => {
    if (getVocabularyGeneration().ids.length === 0) throw new Error('Expected the test vocabulary to contain dense zero');
    fc.assert(
      fc.property(
        fc.array(
          fc.oneof(
            fc.constant({ kind: 'static' as const, text: undefined }),
            fc.record({ kind: fc.constant('dynamic' as const), text: fc.string({ minLength: 1, maxLength: 24 }) }),
            fc.constant({ kind: 'null' as const, text: undefined }),
          ),
          { minLength: CAPACITY, maxLength: 64 },
        ),
        (operations) => {
          const JsClass = getSpanBufferClass(schema, 'mixed');
          JsClass.stats.capacity = CAPACITY;
          const jsRoot = createSpanBuffer(schema, createTestTraceRoot('js-wasm-parity'), metadata, CAPACITY, JsClass);
          const wasmRoot = createBuffer('mixed');
          jsRoot.entry_type[0] = ENTRY_TYPE_SPAN_START;
          jsRoot.entry_type[1] = ENTRY_TYPE_SPAN_OK;
          jsRoot.message(0, 'parity root');
          jsRoot._writeIndex = 2;
          wasmRoot.entry_type[0] = ENTRY_TYPE_SPAN_START;
          wasmRoot.entry_type[1] = ENTRY_TYPE_SPAN_OK;
          wasmRoot.message(0, 'parity root');
          wasmRoot._writeIndex = 2;

          let jsSegment: SpanBuffer<typeof schema> = jsRoot;
          let wasmSegment: WasmSpanBufferInstance<typeof schema> = wasmRoot;
          let jsRow = 2;
          let wasmRow = 2;
          const expected: Array<string | undefined> = [];
          for (const operation of operations) {
            if (jsRow === CAPACITY) {
              jsSegment = createOverflowBuffer(jsSegment);
              jsRow = 0;
            }
            if (wasmRow === CAPACITY) {
              wasmSegment = createWasmOverflowBuffer(wasmSegment, traceRoot, EMPTY_SCOPE, metadata, metadata);
              wasmRow = 0;
            }
            const entryType = operation.kind === 'dynamic' ? ENTRY_TYPE_DEBUG : ENTRY_TYPE_INFO;
            jsSegment.entry_type[jsRow] = entryType;
            wasmSegment.entry_type[wasmRow] = entryType;
            if (operation.kind === 'static') {
              if (
                jsSegment._messageLayoutFamily !== 'mixed' ||
                wasmSegment._messageLayoutFamily !== 'mixed' ||
                jsSegment._logHeaders === undefined ||
                wasmSegment._logHeaders === undefined
              ) {
                throw new Error('Expected mixed header lanes');
              }
              jsSegment._logHeaders[jsRow] = entryType;
              wasmSegment._logHeaders[wasmRow] = entryType;
            } else if (operation.kind === 'dynamic') {
              jsSegment.message(jsRow, operation.text);
              wasmSegment.message(wasmRow, operation.text);
            }
            expected.push(resolveMessage(jsSegment, jsRow));
            jsSegment._writeIndex = ++jsRow;
            wasmSegment._writeIndex = ++wasmRow;
          }

          const jsMessages = collectMessages(jsRoot);
          const wasmMessages = collectMessages(wasmRoot);
          expect(wasmMessages).toEqual(jsMessages);
          expect(jsMessages).toEqual(expected);
          for (
            let segment: WasmSpanBufferInstance<typeof schema> | undefined = wasmRoot;
            segment;
            segment = segment._overflow
          ) {
            expectWasmFamilyShape(segment, 'mixed');
          }
        },
      ),
      { numRuns: 75 },
    );
  });
});
