import { describe, expect, it } from 'bun:test';

import { createParseCompactWasmBackend, type EventProcessorWasmExports } from '../parse-backend.js';

const OUTPUT_HEADER_BYTES = 16;

interface ParseInvocation {
  inputPtr: number;
  inputLen: number;
  outputPtr: number;
  outputLen: number;
}

function createMockEventProcessor() {
  const memory = new WebAssembly.Memory({ initial: 2 });
  const parseCalls: ParseInvocation[] = [];

  const exports: EventProcessorWasmExports = {
    memory,
    ep_version: () => 1,
    ep_create_with_schema: () => 1,
    ep_create_with_schema_and_names: () => 1,
    ep_destroy: () => {},
    ep_create_log_entry: (_handle, inputPtr, inputLen, _format, outputPtr, outputLen) => {
      parseCalls.push({ inputPtr, inputLen, outputPtr, outputLen });

      const payload = new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]);
      const view = new DataView(memory.buffer);
      const mem = new Uint8Array(memory.buffer);

      view.setUint32(outputPtr, 0, true);
      view.setUint32(outputPtr + 4, OUTPUT_HEADER_BYTES, true);
      view.setUint32(outputPtr + 8, payload.length, true);
      view.setUint32(outputPtr + 12, Math.max(1, Math.floor(inputLen / 10)), true);
      mem.set(payload, outputPtr + OUTPUT_HEADER_BYTES);

      return 0;
    },
  };

  return { exports, parseCalls };
}

describe('parse backend dynamic memory layout', () => {
  it('starts small and grows memory on demand', () => {
    const mock = createMockEventProcessor();
    const backend = createParseCompactWasmBackend(mock.exports);

    const beforeBytes = mock.exports.memory.buffer.byteLength;
    const input = new Uint8Array(3 * 1024 * 1024);

    const result = backend.parse(input, {
      schemaBytes: new Uint8Array([1, 2, 3]),
      fieldMetadata: new Uint8Array([1, 0, 0, 0]),
    });

    const afterBytes = mock.exports.memory.buffer.byteLength;

    expect(afterBytes).toBeGreaterThan(beforeBytes);
    expect(result.arrowIpc).toEqual(new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]));
    expect(result.eventCount).toBeGreaterThan(0);
  });

  it('keeps deterministic region layout for identical parse calls', () => {
    const mock = createMockEventProcessor();
    const backend = createParseCompactWasmBackend(mock.exports);

    const input = new Uint8Array(1024 * 1024);
    const config = {
      schemaBytes: new Uint8Array([1, 2, 3, 4]),
      fieldMetadata: new Uint8Array([1, 0, 0, 0, 2, 0, 0, 0]),
      fieldNames: ['id', 'name'],
    };

    backend.parse(input, config);
    backend.parse(input, config);

    expect(mock.parseCalls).toHaveLength(2);
    expect(mock.parseCalls[1]).toEqual(mock.parseCalls[0]);
  });

  it('rejects parse requirements larger than 64MB with explicit error', () => {
    const mock = createMockEventProcessor();
    const backend = createParseCompactWasmBackend(mock.exports);

    const veryLargeInput = new Uint8Array(22 * 1024 * 1024);

    expect(() => {
      backend.parse(veryLargeInput, {
        schemaBytes: new Uint8Array([1]),
        fieldMetadata: new Uint8Array([1, 0, 0, 0]),
      });
    }).toThrow(/exceeds 64MB limit/i);
  });
});
