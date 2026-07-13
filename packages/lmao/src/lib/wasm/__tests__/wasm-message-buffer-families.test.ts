import { beforeEach, describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { createTestOpMetadata, createTestTraceRoot, createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { MAX_PACKED_MESSAGE_DENSE_INDEX, resolveEntryType, resolveMessage } from '../../resolveMessage.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED,
  RUNTIME_HINT_RESULT,
  type MessageLayoutFamily,
  type MessagePhysicalLayout,
} from '../../runtimeHint.js';
import type { OpMetadata } from '../../opContext/opTypes.js';
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
const currentMixedOp = context.defineOp('wasm-current-mixed', (ctx) => ctx.ok(null), undefined, {
  runtimeHint:
    RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | RUNTIME_HINT_MESSAGE_LAYOUT_MIXED | CAPACITY,
  localMessageDictionary: [0],
});
const specializedMixedOp = context.defineOp('wasm-specialized-mixed', (ctx) => ctx.ok(null), undefined, {
  runtimeHint:
    RUNTIME_HINT_ANALYZED_VALID |
    RUNTIME_HINT_LOG |
    RUNTIME_HINT_RESULT |
    RUNTIME_HINT_MESSAGE_LAYOUT_MIXED |
    RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED |
    CAPACITY,
});

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
    expect('message_values' in buffer).toBe(false);
    expect(Object.hasOwn(buffer, '_message')).toBe(false);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(true);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(true);
  } else if (family === 'dynamic-only') {
    expect(buffer.message_values).toBeInstanceOf(Array);
    expect(Object.hasOwn(buffer, '_message')).toBe(true);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(true);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(false);
  } else {
    expect(buffer.message_values).toBeInstanceOf(Array);
    expect(Object.hasOwn(buffer, '_message')).toBe(true);
    expect(Object.hasOwn(buffer, '_spanName')).toBe(false);
    expect(Object.hasOwn(buffer, '_terminalMessage')).toBe(false);
  }
}

function expectWasmPhysicalShape(
  buffer: WasmSpanBufferInstance<typeof schema>,
  physicalLayout: MessagePhysicalLayout,
): void {
  if (buffer._messagePhysicalLayout !== physicalLayout) {
    throw new Error(`Expected ${physicalLayout} WASM buffer, received ${buffer._messagePhysicalLayout}`);
  }
  expect(buffer._layout.messagePhysicalLayout).toBe(physicalLayout);
  const hasStaticMessages = buffer._messageLayoutFamily !== 'dynamic-only';
  if (physicalLayout === 'packed') {
    expect(buffer._rowHeaders).toBeInstanceOf(Uint32Array);
    expect('entry_type' in buffer).toBe(false);
    expect('_messageIds' in buffer).toBe(false);
    expect('_logHeaders' in buffer).toBe(false);
    expect('message_nulls' in buffer).toBe(false);
  } else {
    expect(buffer.entry_type).toBeInstanceOf(Uint8Array);
    expect('_rowHeaders' in buffer).toBe(false);
    if (physicalLayout === 'current' && hasStaticMessages) {
      expect(buffer._messageIds).toBeInstanceOf(Uint16Array);
      expect('_logHeaders' in buffer).toBe(false);
    } else if (physicalLayout === 'specialized' && hasStaticMessages) {
      expect('_messageIds' in buffer).toBe(false);
      expect(buffer._logHeaders).toBeInstanceOf(Uint32Array);
    } else {
      expect('_messageIds' in buffer).toBe(false);
      expect('_logHeaders' in buffer).toBe(false);
    }
    expect('message_nulls' in buffer).toBe(false);
    }
  }

function writeEntryType(buffer: AnySpanBuffer, row: number, entryType: number): void {
  if (buffer._rowHeaders !== undefined) {
    buffer._rowHeaders[row] = ((buffer._rowHeaders[row] & 0xffffff00) | entryType) >>> 0;
    return;
  }
  if (buffer.entry_type === undefined) throw new Error('Expected split entry-type lane');
  {
    const entryTypes = buffer.entry_type;
    if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
    entryTypes[row] = entryType;
  };
}

function writeStaticDense(buffer: AnySpanBuffer, row: number, entryType: number, denseIndex: number): void {
  if (buffer._rowHeaders !== undefined) {
    buffer._rowHeaders[row] = (((denseIndex + 1) << 8) | entryType) >>> 0;
    return;
  }
  if (buffer.entry_type === undefined) throw new Error('Expected nonpacked entry-type lane');
  buffer.entry_type[row] = entryType;
  if (buffer._messagePhysicalLayout === 'current') {
    if (buffer._messageIds === undefined) throw new Error('Expected current local-ID lane');
    buffer._messageIds[row] = denseIndex + 1;
  } else {
    if (buffer._logHeaders === undefined) throw new Error('Expected specialized global dense lane');
    buffer._logHeaders[row] = denseIndex + 1;
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

  function createBuffer(
    family: MessageLayoutFamily,
    physicalLayout: MessagePhysicalLayout = 'current',
    opMetadata: OpMetadata = metadata,
  ): WasmSpanBufferInstance<typeof schema> {
    return createWasmSpanBuffer(
      schema,
      {
        allocator,
        capacity: CAPACITY,
        messageLayoutFamily: family,
        messagePhysicalLayout: physicalLayout,
      },
      traceRoot,
      EMPTY_SCOPE,
      opMetadata,
      opMetadata,
    );
  }

  it('caches all family/mode classes and exposes exact lane offsets and bytes for arbitrary capacities', () => {
    const families = ['static-only', 'dynamic-only', 'mixed'] as const;
    const modes = ['current', 'specialized', 'packed'] as const;
    const classes = families.flatMap((family) => modes.map((mode) => getWasmSpanBufferClass(schema, family, mode)));
    const templates = families.flatMap((family) => modes.map((mode) => createWasmLayoutTemplate(schema, family, mode)));
    expect(new Set(classes).size).toBe(9);
    expect(new Set(templates).size).toBe(9);
    for (const family of families) {
      for (const mode of modes) {
        expect(getWasmSpanBufferClass(schema, family, mode)).toBe(getWasmSpanBufferClass(schema, family, mode));
        expect(createWasmLayoutTemplate(schema, family, mode)).toBe(createWasmLayoutTemplate(schema, family, mode));
      }
    }

    fc.assert(
      fc.property(fc.integer({ min: 1, max: 512 }), (capacity) => {
        const timestampBytes = capacity * BigUint64Array.BYTES_PER_ELEMENT;
        const entryTypeBytes = capacity * Uint8Array.BYTES_PER_ELEMENT;
        for (const family of families) {
          for (const mode of modes) {
            const template = createWasmLayoutTemplate(schema, family, mode);
            const layout = getWasmPhysicalLayout(schema, capacity, family, mode);
            const hasStaticMessages = family !== 'dynamic-only';
            expect(layout).toBe(template.forCapacity(capacity));
            expect(layout.messageLayoutFamily).toBe(family);
            expect(layout.messagePhysicalLayout).toBe(mode);
            expect(layout.system.timestampOffset).toBe(0);
            expect(layout.system.messageValueOffset).toBe(family === 'static-only' ? null : 0);
            expect('messageIdValidityOffset' in layout.system).toBe(false);
            expect('messageValidityOffset' in layout.system).toBe(false);

            if (mode === 'packed') {
              const rowHeaderOffset = align(timestampBytes, Uint32Array.BYTES_PER_ELEMENT);
              expect(layout.system.entryTypeOffset).toBeNull();
              expect(layout.system.messageIdOffset).toBeNull();
              expect(layout.system.messageDenseIndexOffset).toBeNull();
              expect(layout.system.rowHeaderOffset).toBe(rowHeaderOffset);
              expect(layout.system.byteLength).toBe(
                align(rowHeaderOffset + capacity * Uint32Array.BYTES_PER_ELEMENT, BigUint64Array.BYTES_PER_ELEMENT),
              );
            } else {
              const entryTypeOffset = timestampBytes;
              expect(layout.system.entryTypeOffset).toBe(entryTypeOffset);
              expect(layout.system.rowHeaderOffset).toBeNull();
              if (!hasStaticMessages) {
                expect(layout.system.messageIdOffset).toBeNull();
                expect(layout.system.messageDenseIndexOffset).toBeNull();
                expect(layout.system.byteLength).toBe(
                  align(entryTypeOffset + entryTypeBytes, BigUint64Array.BYTES_PER_ELEMENT),
                );
              } else if (mode === 'current') {
                const messageIdOffset = align(entryTypeOffset + entryTypeBytes, Uint16Array.BYTES_PER_ELEMENT);
                expect(layout.system.messageIdOffset).toBe(messageIdOffset);
                expect(layout.system.messageDenseIndexOffset).toBeNull();
                expect(layout.system.byteLength).toBe(
                  align(messageIdOffset + capacity * Uint16Array.BYTES_PER_ELEMENT, BigUint64Array.BYTES_PER_ELEMENT),
                );
              } else {
                const messageDenseIndexOffset = align(
                  entryTypeOffset + entryTypeBytes,
                  Uint32Array.BYTES_PER_ELEMENT,
                );
                expect(layout.system.messageIdOffset).toBeNull();
                expect(layout.system.messageDenseIndexOffset).toBe(messageDenseIndexOffset);
                expect(layout.system.byteLength).toBe(
                  align(messageDenseIndexOffset + capacity * Uint32Array.BYTES_PER_ELEMENT, BigUint64Array.BYTES_PER_ELEMENT),
                );
              }
            }
          }
        }
      }),
      { numRuns: 100 },
    );
  });

  it('allocates current and specialized lanes and distinguishes dense-zero, null, and raw rows', () => {
    if (getVocabularyGeneration().ids.length === 0) throw new Error('Expected the test vocabulary to contain dense zero');
    for (const { mode, op } of [
      { mode: 'current' as const, op: currentMixedOp },
      { mode: 'specialized' as const, op: specializedMixedOp },
    ]) {
      const buffer = createBuffer('mixed', mode, op.metadata);
      expectWasmFamilyShape(buffer, 'mixed');
      expectWasmPhysicalShape(buffer, mode);
      writeStaticDense(buffer, 0, ENTRY_TYPE_INFO, 0);
      buffer._writeIndex = 3;
      expect(resolveEntryType(buffer, 0)).toBe(ENTRY_TYPE_INFO);
      expect(resolveMessage(buffer, 0)).toBeDefined();
      if (mode === 'current') expect(buffer._messageIds?.[0]).toBe(1);
      else expect(buffer._logHeaders?.[0]).toBe(1);
      expect('message_nulls' in buffer).toBe(false);

      writeEntryType(buffer, 1, ENTRY_TYPE_DEBUG);
      expect(resolveMessage(buffer, 1)).toBeUndefined();
      buffer.message(1, '');
      expect(resolveMessage(buffer, 1)).toBe('');
      writeEntryType(buffer, 2, ENTRY_TYPE_DEBUG);
      expect(resolveMessage(buffer, 2)).toBeUndefined();
      if (mode === 'specialized') {
        if (buffer._logHeaders === undefined) throw new Error('Expected specialized dense lane');
        buffer._logHeaders[2] = MAX_PACKED_MESSAGE_DENSE_INDEX + 1;
        expect(buffer._logHeaders[2]).toBe(0x00ffffff);
      }
    }
  });

  it('clears stale raw rows across allocator reuse before current and specialized static/null/raw transitions', () => {
    for (const { mode, metadata } of [
      { mode: 'current' as const, metadata: currentMixedOp.metadata },
      { mode: 'specialized' as const, metadata: specializedMixedOp.metadata },
    ]) {
      allocator.reset();
      allocator.setThreadId(0, 42);
      traceRoot = new WasmTraceRoot<typeof schema>(allocator, createTraceId(`${mode}-raw-cycle`), lifecycleTracer);
      const staleRaw = createBuffer('mixed', mode, metadata);
      writeEntryType(staleRaw, 0, ENTRY_TYPE_DEBUG);
      staleRaw.message(0, 'stale raw');
      expect(resolveMessage(staleRaw, 0)).toBe('stale raw');
      const reusedSystemPtr = staleRaw._descriptor.systemPtr;
      const reducedSystemBytes = staleRaw._layout.system.byteLength;

      allocator.reset();
      allocator.setThreadId(0, 42);
      traceRoot = new WasmTraceRoot<typeof schema>(allocator, createTraceId(`${mode}-static-cycle`), lifecycleTracer);
      const staticRow = createBuffer('mixed', mode, metadata);
      expect(staticRow._descriptor.systemPtr).toBe(reusedSystemPtr);
      expect(staticRow._layout.system.byteLength).toBe(reducedSystemBytes);
      expect(staticRow.message_values?.[0]).toBeUndefined();
      expect('message_nulls' in staticRow).toBe(false);
      writeStaticDense(staticRow, 0, ENTRY_TYPE_INFO, 0);
      expect(resolveMessage(staticRow, 0)).toBeDefined();
      if (mode === 'current') expect(staticRow._messageIds?.[0]).toBe(1);
      else expect(staticRow._logHeaders?.[0]).toBe(1);

      allocator.reset();
      allocator.setThreadId(0, 42);
      traceRoot = new WasmTraceRoot<typeof schema>(allocator, createTraceId(`${mode}-null-cycle`), lifecycleTracer);
      const nullRow = createBuffer('mixed', mode, metadata);
      expect(nullRow._descriptor.systemPtr).toBe(reusedSystemPtr);
      writeEntryType(nullRow, 0, ENTRY_TYPE_DEBUG);
      expect(nullRow.message_values?.[0]).toBeUndefined();
      expect(resolveMessage(nullRow, 0)).toBeUndefined();
      if (mode === 'current') expect(nullRow._messageIds?.[0]).toBe(0);
      else expect(nullRow._logHeaders?.[0]).toBe(0);

      allocator.reset();
      allocator.setThreadId(0, 42);
      traceRoot = new WasmTraceRoot<typeof schema>(allocator, createTraceId(`${mode}-fresh-raw-cycle`), lifecycleTracer);
      const freshRaw = createBuffer('mixed', mode, metadata);
      expect(freshRaw._descriptor.systemPtr).toBe(reusedSystemPtr);
      writeEntryType(freshRaw, 0, ENTRY_TYPE_DEBUG);
      freshRaw.message(0, '');
      expect(resolveMessage(freshRaw, 0)).toBe('');
      if (mode === 'current') expect(freshRaw._messageIds?.[0]).toBe(0);
      else expect(freshRaw._logHeaders?.[0]).toBe(0);
    }
  });

  it('allocates packed lanes for all families and preserves dense-zero/max/null/raw semantics', () => {
    if (getVocabularyGeneration().ids.length === 0) throw new Error('Expected the test vocabulary to contain dense zero');
    for (const family of ['static-only', 'dynamic-only', 'mixed'] as const) {
      const buffer = createBuffer(family, 'packed');
      expectWasmFamilyShape(buffer, family);
      expectWasmPhysicalShape(buffer, 'packed');
      if (buffer._rowHeaders === undefined) throw new Error('Expected packed WASM headers');
      writeStaticDense(buffer, 0, ENTRY_TYPE_SPAN_START, 0);
      buffer._writeIndex = 2;
      expect(buffer._rowHeaders[0] >>> 8).toBe(1);
      expect(resolveEntryType(buffer, 0)).toBe(ENTRY_TYPE_SPAN_START);
      expect(resolveMessage(buffer, 0)).toBeDefined();

      buffer._rowHeaders[1] = (((MAX_PACKED_MESSAGE_DENSE_INDEX + 1) << 8) | ENTRY_TYPE_SPAN_OK) >>> 0;
      expect(buffer._rowHeaders[1] >>> 8).toBe(0x00ffffff);
      if (family !== 'static-only') {
        writeEntryType(buffer, 2, ENTRY_TYPE_DEBUG);
        buffer._writeIndex = 3;
        expect(resolveMessage(buffer, 2)).toBeUndefined();
        buffer.message(2, `raw-packed-${family}`);
        expect(buffer._rowHeaders[2] >>> 8).toBe(0);
        expect(resolveMessage(buffer, 2)).toBe(`raw-packed-${family}`);
      }
    }
  });

  it('propagates family, physical layout, pointers, and omissions through child and overflow descriptors', () => {
    for (const family of ['static-only', 'dynamic-only', 'mixed'] as const) {
      for (const physicalLayout of ['current', 'specialized', 'packed'] as const) {
        const modeMetadata =
          physicalLayout === 'current'
            ? currentMixedOp.metadata
            : physicalLayout === 'specialized'
              ? specializedMixedOp.metadata
              : metadata;
        const root = createBuffer(family, physicalLayout, modeMetadata);
        const child = createWasmChildSpanBuffer(
          root,
          { allocator, capacity: CAPACITY, messageLayoutFamily: family },
          traceRoot,
          EMPTY_SCOPE,
          modeMetadata,
          modeMetadata,
        );
        const overflow = createWasmOverflowBuffer(child, traceRoot, EMPTY_SCOPE, modeMetadata, modeMetadata);
        if (physicalLayout === 'current') {
          const dictionary = currentMixedOp.callsitePlan.localMessageDictionary;
          expect(root._opMetadata._physicalLayoutPlan?.localMessageDictionary).toBe(dictionary);
          expect(child._opMetadata._physicalLayoutPlan?.localMessageDictionary).toBe(dictionary);
          expect(overflow._opMetadata._physicalLayoutPlan?.localMessageDictionary).toBe(dictionary);
          for (const buffer of [root, child, overflow]) {
            expect(Object.hasOwn(buffer, '_messageDictionary')).toBe(false);
            expect('_messageDictionary' in buffer).toBe(false);
          }
        }
        expect(child.constructor).toBe(getWasmSpanBufferClass(schema, family, physicalLayout));
        expect(overflow.constructor).toBe(child.constructor);
        expect(child._descriptor.parent).toBe(root._descriptor);
        expect(overflow._descriptor.parent).toBe(root._descriptor);
        expect(overflow._identityPtr).toBe(child._identityPtr);
        expect(overflow._layout).toBe(child._layout);
        expectWasmFamilyShape(child, family);
        expectWasmFamilyShape(overflow, family);
        expectWasmPhysicalShape(child, physicalLayout);
        expectWasmPhysicalShape(overflow, physicalLayout);
      }
    }
  });

  it('keeps JS and WASM decoded rows identical across random mixed overflow streams', () => {
    if (getVocabularyGeneration().ids.length === 0) throw new Error('Expected the test vocabulary to contain dense zero');
    fc.assert(
      fc.property(
        fc.constantFrom<MessagePhysicalLayout>('current', 'specialized', 'packed'),
        fc.array(
          fc.oneof(
            fc.constant({ kind: 'static' as const, text: undefined }),
            fc.record({ kind: fc.constant('dynamic' as const), text: fc.string({ minLength: 1, maxLength: 24 }) }),
            fc.constant({ kind: 'null' as const, text: undefined }),
          ),
          { minLength: CAPACITY, maxLength: 64 },
        ),
        (physicalLayout, operations) => {
          const modeMetadata =
            physicalLayout === 'current'
              ? currentMixedOp.metadata
              : physicalLayout === 'specialized'
                ? specializedMixedOp.metadata
                : metadata;
          const JsClass = getSpanBufferClass(schema, 'mixed', physicalLayout);
          JsClass.stats.capacity = CAPACITY;
          const jsRoot = createSpanBuffer(schema, createTestTraceRoot('js-wasm-parity'), modeMetadata, CAPACITY, JsClass);
          const wasmRoot = createBuffer('mixed', physicalLayout, modeMetadata);
          writeEntryType(jsRoot, 0, ENTRY_TYPE_SPAN_START);
          writeEntryType(jsRoot, 1, ENTRY_TYPE_SPAN_OK);
          jsRoot.message(0, 'parity root');
          jsRoot._writeIndex = 2;
          writeEntryType(wasmRoot, 0, ENTRY_TYPE_SPAN_START);
          writeEntryType(wasmRoot, 1, ENTRY_TYPE_SPAN_OK);
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
              wasmSegment = createWasmOverflowBuffer(
                wasmSegment,
                traceRoot,
                EMPTY_SCOPE,
                modeMetadata,
                modeMetadata,
              );
              wasmRow = 0;
            }
            const entryType = operation.kind === 'dynamic' ? ENTRY_TYPE_DEBUG : ENTRY_TYPE_INFO;
            writeEntryType(jsSegment, jsRow, entryType);
            writeEntryType(wasmSegment, wasmRow, entryType);
            if (operation.kind === 'static') {
              writeStaticDense(jsSegment, jsRow, entryType, 0);
              writeStaticDense(wasmSegment, wasmRow, entryType, 0);
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
            expectWasmPhysicalShape(segment, physicalLayout);
          }
        },
      ),
      { numRuns: 75 },
    );
  });
});
