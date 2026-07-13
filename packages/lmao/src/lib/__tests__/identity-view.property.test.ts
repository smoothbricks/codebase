import { describe, expect, it, spyOn } from 'bun:test';
import fc from 'fast-check';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import { createTraceId } from '../traceId.js';
import type { AnySpanBuffer } from '../types.js';
import { createTestOpMetadata, createTestSchema, createTestTraceRoot } from './test-helpers.js';

const schema = createTestSchema({});
const metadata = createTestOpMetadata({ name: 'identity-view', line: 14 });
const encoder = new TextEncoder();

function exactBytes(bytes: Uint8Array): number[] {
  return Array.from(bytes);
}

function expectJsIdentity(buffer: AnySpanBuffer, parent: AnySpanBuffer | undefined, traceBytes: Uint8Array): void {
  const identity = buffer._identity;
  const view = new DataView(identity.buffer, identity.byteOffset, identity.byteLength);

  expect(view.getBigUint64(0, true)).toBe(buffer.thread_id);
  expect(view.getUint32(8, true)).toBe(buffer.span_id);
  if (parent === undefined) {
    expect(identity.byteLength).toBe(13 + traceBytes.byteLength);
    expect(identity[12]).toBe(traceBytes.byteLength);
    expect(exactBytes(identity.subarray(13))).toEqual(exactBytes(traceBytes));
    expect(buffer.parent_thread_id).toBe(0n);
    expect(buffer.parent_span_id).toBe(0);
    expect(buffer._hasParent).toBe(false);
  } else {
    expect(identity.byteLength).toBe(12);
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

describe('JavaScript span identity views', () => {
  it('uses stable trace bytes and performs no DataView allocation or UTF-8 decode during identity access', () => {
    const strategy = new JsBufferStrategy<typeof schema>();
    const incoming = createTraceId('0af7651916cd43dd8448eb211c80319c');
    const traceRoot = createTestTraceRoot(incoming);
    const root = strategy.createSpanBuffer(schema, traceRoot, metadata, 8);
    const child = strategy.createChildSpanBuffer(root, metadata, metadata, 8);
    const overflow = strategy.createOverflowBuffer(child);
    const traceBytes = traceRoot._traceIdBytes;

    expect(traceRoot.trace_id).toBe(incoming);
    expect(traceRoot._traceIdBytes).toBe(traceBytes);
    expect(exactBytes(traceBytes)).toEqual(exactBytes(encoder.encode(incoming)));
    expect(root._identity).not.toBe(child._identity);
    expect(overflow._identity).toBe(child._identity);
    expect(root._traceRoot._traceIdBytes).toBe(traceBytes);
    expect(child._traceRoot._traceIdBytes).toBe(traceBytes);
    expect(overflow._traceRoot._traceIdBytes).toBe(traceBytes);

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
    const copiedThreadIds = new Uint8Array(32);

    try {
      for (let iteration = 0; iteration < 20; iteration++) {
        void root.span_id;
        void root.thread_id;
        void root.trace_id;
        void child.span_id;
        void child.thread_id;
        void child.trace_id;
        void child.parent_span_id;
        void child.parent_thread_id;
        void overflow.span_id;
        void overflow.thread_id;
        void overflow.trace_id;
        void overflow.parent_span_id;
        void overflow.parent_thread_id;
      }
      root.copyThreadIdTo(copiedThreadIds, 3);
      child.copyParentThreadIdTo(copiedThreadIds, 19);

      expect(dataViewAllocations).toBe(0);
      expect(decode).toHaveBeenCalledTimes(0);
    } finally {
      Object.defineProperty(globalThis, 'DataView', dataViewDescriptor);
      decode.mockRestore();
    }

    const copied = new DataView(copiedThreadIds.buffer);
    expect(copied.getBigUint64(3, true)).toBe(root.thread_id);
    expect(copied.getBigUint64(19, true)).toBe(root.thread_id);
  });

  it('preserves exact root, child, and overflow identity bytes for randomized span trees and release generations', () => {
    fc.assert(
      fc.property(
        printableTraceIdArbitrary(),
        fc.integer({ min: 0, max: 3 }),
        fc.array(
          fc.record({
            parentSelector: fc.nat(),
            overflowCount: fc.integer({ min: 0, max: 3 }),
          }),
          { minLength: 1, maxLength: 36 },
        ),
        (traceIdValue, rootOverflowCount, nodes) => {
          const strategy = new JsBufferStrategy<typeof schema>();
          const incoming = createTraceId(traceIdValue);
          const traceRoot = createTestTraceRoot(incoming);
          const root = strategy.createSpanBuffer(schema, traceRoot, metadata, 8);
          const logicalSpans = [root];
          const traceBytes = traceRoot._traceIdBytes;
          const encodedTraceId = encoder.encode(incoming);

          expect(traceRoot.trace_id).toBe(incoming);
          expect(traceRoot._traceIdBytes).toBe(traceBytes);
          expect(exactBytes(traceBytes)).toEqual(exactBytes(encodedTraceId));
          expectJsIdentity(root, undefined, encodedTraceId);

          let rootOverflowOwner = root;
          for (let index = 0; index < rootOverflowCount; index++) {
            const overflow = strategy.createOverflowBuffer(rootOverflowOwner);
            expect(overflow._identity).toBe(root._identity);
            expect(overflow._traceRoot._traceIdBytes).toBe(traceBytes);
            expect(overflow.trace_id).toBe(incoming);
            expect(overflow.span_id).toBe(root.span_id);
            rootOverflowOwner = overflow;
          }

          for (const node of nodes) {
            const parent = logicalSpans[node.parentSelector % logicalSpans.length];
            if (parent === undefined) throw new Error('randomized parent selection failed');
            const child = strategy.createChildSpanBuffer(parent, metadata, metadata, 8);
            logicalSpans.push(child);

            expect(child._identity).not.toBe(parent._identity);
            expect(child._traceRoot._traceIdBytes).toBe(traceBytes);
            expect(child.trace_id).toBe(incoming);
            expectJsIdentity(child, parent, encodedTraceId);

            let overflowOwner = child;
            for (let index = 0; index < node.overflowCount; index++) {
              const overflow = strategy.createOverflowBuffer(overflowOwner);
              expect(overflow._identity).toBe(child._identity);
              expect(overflow._traceRoot._traceIdBytes).toBe(traceBytes);
              expect(overflow.trace_id).toBe(incoming);
              expect(overflow.span_id).toBe(child.span_id);
              expect(overflow.parent_span_id).toBe(parent.span_id);
              expect(overflow.parent_thread_id).toBe(parent.thread_id);
              overflowOwner = overflow;
            }
          }

          const topology = traceRoot._topology;
          const generation = topology.generation;
          strategy.releaseBuffer(root);
          expect(topology.generation).toBe(generation + 1);
          expect(traceRoot._traceIdBytes).toBe(traceBytes);
          expect(exactBytes(traceBytes)).toEqual(exactBytes(encodedTraceId));
        },
      ),
      { numRuns: 60 },
    );
  });
});
