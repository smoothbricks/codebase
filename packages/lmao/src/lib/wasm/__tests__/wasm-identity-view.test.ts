import { beforeAll, describe, expect, it, spyOn } from 'bun:test';
import fc from 'fast-check';
import { createTestOpMetadata, createTestSchema, createTestTraceRoot } from '../../__tests__/test-helpers.js';
import { JsBufferStrategy } from '../../JsBufferStrategy.js';
import { createTraceId } from '../../traceId.js';
import type { TracerLifecycleHooks } from '../../traceRoot.js';
import type { AnySpanBuffer } from '../../types.js';
import { WasmBufferStrategy } from '../WasmBufferStrategy.js';
import { isWasmSpanBufferInstance, type WasmSpanBufferInstance } from '../wasmSpanBuffer.js';
import { createWasmTraceRoot } from '../wasmTraceRoot.js';

const CAPACITY = 8;
const schema = createTestSchema({});
const metadata = createTestOpMetadata({ name: 'wasm-identity-view', line: 14 });
const encoder = new TextEncoder();

function requireWasmBuffer(buffer: AnySpanBuffer): WasmSpanBufferInstance<typeof schema> {
  if (!isWasmSpanBufferInstance<typeof schema>(buffer)) throw new Error('Expected a WASM span buffer');
  return buffer;
}

function exactBytes(bytes: Uint8Array): number[] {
  return Array.from(bytes);
}

function identityData(buffer: WasmSpanBufferInstance<typeof schema>): DataView {
  const value = Reflect.get(buffer, '_identityData');
  if (!(value instanceof DataView)) throw new Error('Expected a canonical identity DataView');
  return value;
}

function expectWasmIdentity(
  buffer: WasmSpanBufferInstance<typeof schema>,
  parent: WasmSpanBufferInstance<typeof schema> | undefined,
  traceBytes: Uint8Array,
): void {
  const identity = buffer._identity;
  const view = identityData(buffer);

  expect(identity.byteLength).toBe(128);
  expect(view.byteOffset).toBe(identity.byteOffset);
  expect(view.byteLength).toBe(identity.byteLength);
  expect(view.getUint32(4, true)).toBe(buffer.span_id);
  if (parent === undefined) {
    expect(identity[8]).toBe(traceBytes.byteLength);
    expect(exactBytes(identity.subarray(9, 9 + traceBytes.byteLength))).toEqual(exactBytes(traceBytes));
    expect(buffer.parent_thread_id).toBe(0n);
    expect(buffer.parent_span_id).toBe(0);
    expect(buffer._hasParent).toBe(false);
  } else {
    expect(identity[8]).toBe(0);
    expect(buffer.parent_thread_id).toBe(parent.thread_id);
    expect(buffer.parent_span_id).toBe(parent.span_id);
    expect(buffer._hasParent).toBe(true);
  }
}

function printableTraceIdArbitrary() {
  return fc
    .array(fc.integer({ min: 0x20, max: 0x7e }), { minLength: 1, maxLength: 128 })
    .map((codes) => String.fromCharCode(...codes));
}

describe('WASM span identity views', () => {
  let strategy: WasmBufferStrategy<typeof schema>;
  let lifecycle: TracerLifecycleHooks<typeof schema>;

  beforeAll(async () => {
    strategy = await WasmBufferStrategy.create<typeof schema>({
      capacity: CAPACITY,
      initialPages: 16,
      maxPages: 128,
    });
    lifecycle = {
      onTraceStart: () => {},
      onTraceEnd: () => {},
      onSpanStart: () => {},
      onSpanEnd: () => {},
      onStatsWillResetFor: () => {},
      getFlagEvaluatorForContext: () => undefined,
      bufferStrategy: strategy,
    };
  });

  it('shares overflow identity views, refreshes them together once per memory epoch, and rejects released generations', () => {
    strategy.reset();
    strategy.allocator.setThreadId(0x12345678, 0x9abcdef0);
    const incoming = createTraceId('0af7651916cd43dd8448eb211c80319c');
    const traceRoot = createWasmTraceRoot(strategy.allocator, incoming, lifecycle);
    const root = requireWasmBuffer(strategy.createSpanBuffer(schema, traceRoot, metadata, CAPACITY));
    const child = requireWasmBuffer(strategy.createChildSpanBuffer(root, metadata, metadata, CAPACITY));
    const overflow = requireWasmBuffer(strategy.createOverflowBuffer(child));
    const traceBytes = traceRoot._traceIdBytes;

    expect(traceRoot.trace_id).toBe(incoming);
    expect(traceRoot._traceIdBytes).toBe(traceBytes);
    expect(exactBytes(traceBytes)).toEqual(exactBytes(encoder.encode(incoming)));
    expect(root._traceRoot._traceIdBytes).toBe(traceBytes);
    expect(child._traceRoot._traceIdBytes).toBe(traceBytes);
    expect(overflow._traceRoot._traceIdBytes).toBe(traceBytes);
    expect(overflow._identityPtr).toBe(child._identityPtr);
    expect(overflow._identity).toBe(child._identity);
    expect(identityData(overflow)).toBe(identityData(child));

    const childIdentityBefore = child._identity;
    const childDataBefore = identityData(child);
    const versionBefore = child._descriptor.memoryVersion;
    const growthAllocation = strategy.allocator.allocExact(strategy.allocator.memory.buffer.byteLength, 8);
    expect(growthAllocation).toBeGreaterThan(0);

    const childIdentityAfter = child._identity;
    const overflowIdentityAfter = overflow._identity;
    const childDataAfter = identityData(child);
    const overflowDataAfter = identityData(overflow);
    expect(child._descriptor.memoryVersion).toBeGreaterThan(versionBefore);
    expect(childIdentityAfter).not.toBe(childIdentityBefore);
    expect(childDataAfter).not.toBe(childDataBefore);
    expect(overflowIdentityAfter).toBe(childIdentityAfter);
    expect(overflowDataAfter).toBe(childDataAfter);
    expect(child._identity).toBe(childIdentityAfter);
    expect(overflow._identity).toBe(childIdentityAfter);
    expect(identityData(child)).toBe(childDataAfter);
    expect(identityData(overflow)).toBe(childDataAfter);
    expect(traceRoot._traceIdBytes).toBe(traceBytes);

    const identityPointers = [root._identityPtr, child._identityPtr];
    const releasedDescriptorGeneration = child._descriptor.generation;
    const topologyGeneration = traceRoot._topology.generation;
    strategy.releaseBuffer(root);

    expect(traceRoot._topology.generation).toBe(topologyGeneration + 1);
    expect(() => root._identity).toThrow(/generation .* released/);
    expect(() => child._identity).toThrow(/generation .* released/);
    expect(() => overflow._identity).toThrow(/generation .* released/);

    const nextIncoming = createTraceId('11111111111111112222222222222222');
    const nextTraceRoot = createWasmTraceRoot(strategy.allocator, nextIncoming, lifecycle);
    const nextRoot = requireWasmBuffer(strategy.createSpanBuffer(schema, nextTraceRoot, metadata, CAPACITY));
    expect(identityPointers).toContain(nextRoot._identityPtr);
    expect(nextRoot._descriptor.generation).toBeGreaterThan(releasedDescriptorGeneration);
    expect(nextRoot.trace_id).toBe(nextIncoming);
    expect(nextRoot._identity).toBe(nextRoot._identity);
    expect(identityData(nextRoot)).toBe(identityData(nextRoot));
    expectWasmIdentity(nextRoot, undefined, nextTraceRoot._traceIdBytes);
    strategy.releaseBuffer(nextRoot);
  });

  it('performs no DataView allocation or UTF-8 decode during repeated identity access within one memory epoch', () => {
    strategy.reset();
    const incoming = createTraceId('aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb');
    const traceRoot = createWasmTraceRoot(strategy.allocator, incoming, lifecycle);
    const root = requireWasmBuffer(strategy.createSpanBuffer(schema, traceRoot, metadata, CAPACITY));
    const child = requireWasmBuffer(strategy.createChildSpanBuffer(root, metadata, metadata, CAPACITY));
    const overflow = requireWasmBuffer(strategy.createOverflowBuffer(child));
    void root._identity;
    void child._identity;
    void overflow._identity;

    const originalDataView = globalThis.DataView;
    const dataViewDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'DataView');
    if (dataViewDescriptor === undefined) throw new Error('global DataView descriptor is unavailable');
    let dataViewAllocations = 0;
    const CountingDataView = new Proxy(originalDataView, {
      construct(target, argumentsList, newTarget) {
        dataViewAllocations++;
        return Reflect.construct(target, argumentsList, newTarget);
      },
    });
    const decode = spyOn(TextDecoder.prototype, 'decode');
    decode.mockClear();
    Object.defineProperty(globalThis, 'DataView', { ...dataViewDescriptor, value: CountingDataView });

    try {
      for (let iteration = 0; iteration < 20; iteration++) {
        void root._identity;
        void root.span_id;
        void root.thread_id;
        void root.trace_id;
        void child._identity;
        void child.span_id;
        void child.thread_id;
        void child.trace_id;
        void child.parent_span_id;
        void child.parent_thread_id;
        void overflow._identity;
        void overflow.span_id;
        void overflow.thread_id;
        void overflow.trace_id;
        void overflow.parent_span_id;
        void overflow.parent_thread_id;
      }

      expect(dataViewAllocations).toBe(0);
      expect(decode).toHaveBeenCalledTimes(0);
    } finally {
      Object.defineProperty(globalThis, 'DataView', dataViewDescriptor);
      decode.mockRestore();
      strategy.releaseBuffer(root);
    }
  });

  it('keeps exact JS and WASM identity ABIs in semantic parity for randomized span trees and overflows', () => {
    fc.assert(
      fc.property(
        printableTraceIdArbitrary(),
        fc.integer({ min: 0, max: 3 }),
        fc.array(
          fc.record({
            parentSelector: fc.nat(),
            overflowCount: fc.integer({ min: 0, max: 3 }),
          }),
          { minLength: 1, maxLength: 28 },
        ),
        (traceIdValue, rootOverflowCount, nodes) => {
          strategy.reset();
          const incoming = createTraceId(traceIdValue);
          const jsStrategy = new JsBufferStrategy<typeof schema>();
          const jsTraceRoot = createTestTraceRoot(incoming);
          const jsRoot = jsStrategy.createSpanBuffer(schema, jsTraceRoot, metadata, CAPACITY);
          const threadId = jsRoot.thread_id;
          strategy.allocator.setThreadId(Number((threadId >> 32n) & 0xffffffffn), Number(threadId & 0xffffffffn));
          const wasmTraceRoot = createWasmTraceRoot(strategy.allocator, incoming, lifecycle);
          const wasmRoot = requireWasmBuffer(strategy.createSpanBuffer(schema, wasmTraceRoot, metadata, CAPACITY));
          const jsLogicalSpans = [jsRoot];
          const wasmLogicalSpans = [wasmRoot];
          const traceBytes = encoder.encode(incoming);

          expect(jsTraceRoot.trace_id).toBe(wasmTraceRoot.trace_id);
          expect(exactBytes(jsTraceRoot._traceIdBytes)).toEqual(exactBytes(traceBytes));
          expect(exactBytes(wasmTraceRoot._traceIdBytes)).toEqual(exactBytes(traceBytes));
          expect(jsRoot.thread_id).toBe(wasmRoot.thread_id);
          expectJsAndWasmRoots(jsRoot, wasmRoot, traceBytes);

          let jsRootOverflowOwner = jsRoot;
          let wasmRootOverflowOwner = wasmRoot;
          for (let index = 0; index < rootOverflowCount; index++) {
            const jsOverflow = jsStrategy.createOverflowBuffer(jsRootOverflowOwner);
            const wasmOverflow = requireWasmBuffer(strategy.createOverflowBuffer(wasmRootOverflowOwner));
            expect(jsOverflow._identity).toBe(jsRoot._identity);
            expect(wasmOverflow._identity).toBe(wasmRoot._identity);
            expect(identityData(wasmOverflow)).toBe(identityData(wasmRoot));
            expect(jsOverflow.trace_id).toBe(wasmOverflow.trace_id);
            expect(jsOverflow.thread_id).toBe(wasmOverflow.thread_id);
            jsRootOverflowOwner = jsOverflow;
            wasmRootOverflowOwner = wasmOverflow;
          }

          for (const node of nodes) {
            const parentIndex = node.parentSelector % jsLogicalSpans.length;
            const jsParent = jsLogicalSpans[parentIndex];
            const wasmParent = wasmLogicalSpans[parentIndex];
            if (jsParent === undefined || wasmParent === undefined)
              throw new Error('randomized parent selection failed');
            const jsChild = jsStrategy.createChildSpanBuffer(jsParent, metadata, metadata, CAPACITY);
            const wasmChild = requireWasmBuffer(
              strategy.createChildSpanBuffer(wasmParent, metadata, metadata, CAPACITY),
            );
            jsLogicalSpans.push(jsChild);
            wasmLogicalSpans.push(wasmChild);

            expect(jsChild.trace_id).toBe(wasmChild.trace_id);
            expect(jsChild.thread_id).toBe(wasmChild.thread_id);
            expect(jsChild.parent_thread_id).toBe(wasmChild.parent_thread_id);
            expect(jsChild.parent_span_id).toBe(jsParent.span_id);
            expect(wasmChild.parent_span_id).toBe(wasmParent.span_id);
            expectWasmIdentity(wasmChild, wasmParent, traceBytes);

            let jsOverflowOwner = jsChild;
            let wasmOverflowOwner = wasmChild;
            for (let index = 0; index < node.overflowCount; index++) {
              const jsOverflow = jsStrategy.createOverflowBuffer(jsOverflowOwner);
              const wasmOverflow = requireWasmBuffer(strategy.createOverflowBuffer(wasmOverflowOwner));
              expect(jsOverflow._identity).toBe(jsChild._identity);
              expect(wasmOverflow._identity).toBe(wasmChild._identity);
              expect(identityData(wasmOverflow)).toBe(identityData(wasmChild));
              expect(jsOverflow.trace_id).toBe(wasmOverflow.trace_id);
              expect(jsOverflow.thread_id).toBe(wasmOverflow.thread_id);
              expect(jsOverflow.parent_span_id).toBe(jsParent.span_id);
              expect(wasmOverflow.parent_span_id).toBe(wasmParent.span_id);
              jsOverflowOwner = jsOverflow;
              wasmOverflowOwner = wasmOverflow;
            }
          }

          jsStrategy.releaseBuffer(jsRoot);
          strategy.releaseBuffer(wasmRoot);
        },
      ),
      { numRuns: 40 },
    );
  });
});

function expectJsAndWasmRoots(
  jsRoot: AnySpanBuffer,
  wasmRoot: WasmSpanBufferInstance<typeof schema>,
  traceBytes: Uint8Array,
): void {
  const jsIdentity = jsRoot._identity;
  const jsView = new DataView(jsIdentity.buffer, jsIdentity.byteOffset, jsIdentity.byteLength);
  expect(jsIdentity.byteLength).toBe(13 + traceBytes.byteLength);
  expect(jsView.getBigUint64(0, true)).toBe(jsRoot.thread_id);
  expect(jsView.getUint32(8, true)).toBe(jsRoot.span_id);
  expect(jsIdentity[12]).toBe(traceBytes.byteLength);
  expect(exactBytes(jsIdentity.subarray(13))).toEqual(exactBytes(traceBytes));

  expectWasmIdentity(wasmRoot, undefined, traceBytes);
  expect(exactBytes(wasmRoot._identity.subarray(9, 9 + traceBytes.byteLength))).toEqual(
    exactBytes(jsIdentity.subarray(13)),
  );
}
