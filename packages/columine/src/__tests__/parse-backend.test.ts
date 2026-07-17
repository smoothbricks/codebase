import { describe, expect, it } from 'bun:test';

import { createParseCompactWasmBackend, type EventProcessorWasmExports } from '../parse-backend.js';

const OUTPUT_HEADER_BYTES = 16;

interface ParseInvocation {
  handle: number;
  inputPtr: number;
  inputLen: number;
  outputPtr: number;
  outputLen: number;
}

interface MockEventProcessorOptions {
  readonly parseResults?: number[];
  readonly headerCodes?: number[];
  readonly throwOnParseCall?: number;
}

function createMockEventProcessor(options: MockEventProcessorOptions = {}) {
  const memory = new WebAssembly.Memory({ initial: 2 });
  const createCalls: number[] = [];
  const destroyCalls: number[] = [];
  const parseCalls: ParseInvocation[] = [];
  let nextHandle = 1;

  const exports: EventProcessorWasmExports = {
    memory,
    ep_version: () => 1,
    ep_create_with_schema: () => {
      const handle = nextHandle++;
      createCalls.push(handle);
      return handle;
    },
    ep_create_with_schema_and_names: () => {
      const handle = nextHandle++;
      createCalls.push(handle);
      return handle;
    },
    ep_destroy: (handle) => {
      destroyCalls.push(handle);
    },
    ep_create_log_entry: (handle, inputPtr, inputLen, _format, outputPtr, outputLen) => {
      parseCalls.push({ handle, inputPtr, inputLen, outputPtr, outputLen });
      if (options.throwOnParseCall === parseCalls.length) {
        throw new Error('mock EventProcessor trap');
      }

      const result = options.parseResults?.shift() ?? 0;
      if (result !== 0) {
        return result;
      }

      const payload = new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]);
      const view = new DataView(memory.buffer);
      const mem = new Uint8Array(memory.buffer);

      view.setUint32(outputPtr, options.headerCodes?.shift() ?? 0, true);
      view.setUint32(outputPtr + 4, OUTPUT_HEADER_BYTES, true);
      view.setUint32(outputPtr + 8, payload.length, true);
      view.setUint32(outputPtr + 12, Math.max(1, Math.floor(inputLen / 10)), true);
      mem.set(payload, outputPtr + OUTPUT_HEADER_BYTES);

      return 0;
    },
  };

  return { exports, createCalls, destroyCalls, parseCalls };
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

  it('rejects parse requirements larger than configured cap with explicit error', () => {
    const mock = createMockEventProcessor();
    const backend = createParseCompactWasmBackend(mock.exports);

    const veryLargeInput = new Uint8Array(96 * 1024 * 1024);

    expect(() => {
      backend.parse(veryLargeInput, {
        schemaBytes: new Uint8Array([1]),
        fieldMetadata: new Uint8Array([1, 0, 0, 0]),
      });
    }).toThrow(/configured cap|exceeds max batch input/i);
  });
});

describe('parse backend EventProcessor lifecycle', () => {
  it('uses exact schema, metadata, and field-name bytes for cached config identity', () => {
    const mock = createMockEventProcessor();
    const backend = createParseCompactWasmBackend(mock.exports);
    const input = new Uint8Array([1]);

    const schemaA = new Uint8Array([1, 2, 3, 4]);
    const schemaB = new Uint8Array([1, 9, 3, 4]);
    const metadataA = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
    const metadataB = new Uint8Array([1, 9, 3, 4, 5, 6, 7, 8]);

    backend.parse(input, {
      schemaBytes: schemaA,
      fieldMetadata: metadataA,
      fieldNames: ['a,b', 'c'],
    });
    backend.parse(input, {
      schemaBytes: schemaA,
      fieldMetadata: metadataA,
      fieldNames: ['a,b', 'c'],
    });
    backend.parse(input, {
      schemaBytes: schemaB,
      fieldMetadata: metadataA,
      fieldNames: ['a,b', 'c'],
    });
    backend.parse(input, {
      schemaBytes: schemaB,
      fieldMetadata: metadataB,
      fieldNames: ['a,b', 'c'],
    });
    backend.parse(input, {
      schemaBytes: schemaB,
      fieldMetadata: metadataB,
      fieldNames: ['a', 'b,c'],
    });

    expect(mock.createCalls).toEqual([1, 2, 3, 4]);
    expect(mock.destroyCalls).toEqual([1, 2, 3]);

    backend.dispose();
    expect(mock.destroyCalls).toEqual([1, 2, 3, 4]);
  });

  it('destroys and clears the cached handle when processing returns an error', () => {
    const mock = createMockEventProcessor({ parseResults: [7, 0] });
    const backend = createParseCompactWasmBackend(mock.exports);
    const config = {
      schemaBytes: new Uint8Array([1, 2, 3]),
      fieldMetadata: new Uint8Array([1, 0, 0, 0]),
    };

    expect(() => backend.parse(new Uint8Array([1]), config)).toThrow('ep_create_log_entry failed with code 7');
    expect(mock.destroyCalls).toEqual([1]);

    backend.parse(new Uint8Array([1]), config);
    expect(mock.createCalls).toEqual([1, 2]);
    expect(mock.parseCalls.map((call) => call.handle)).toEqual([1, 2]);

    backend.dispose();
    expect(mock.destroyCalls).toEqual([1, 2]);
  });

  it('destroys the cached handle for reported header errors and thrown calls', () => {
    const config = {
      schemaBytes: new Uint8Array([1, 2, 3]),
      fieldMetadata: new Uint8Array([1, 0, 0, 0]),
    };
    const headerFailure = createMockEventProcessor({ headerCodes: [9] });
    const headerBackend = createParseCompactWasmBackend(headerFailure.exports);

    expect(() => headerBackend.parse(new Uint8Array([1]), config)).toThrow('ep_create_log_entry returned error code 9');
    expect(headerFailure.destroyCalls).toEqual([1]);

    const trappedCall = createMockEventProcessor({ throwOnParseCall: 1 });
    const trappedBackend = createParseCompactWasmBackend(trappedCall.exports);
    expect(() => trappedBackend.parse(new Uint8Array([1]), config)).toThrow('mock EventProcessor trap');
    expect(trappedCall.destroyCalls).toEqual([1]);
  });

  it('disposes deterministically exactly once and refuses later use', () => {
    const mock = createMockEventProcessor();
    const backend = createParseCompactWasmBackend(mock.exports);
    const config = {
      schemaBytes: new Uint8Array([1, 2, 3]),
      fieldMetadata: new Uint8Array([1, 0, 0, 0]),
    };

    backend.parse(new Uint8Array([1]), config);
    backend.dispose();
    backend.dispose();

    expect(mock.destroyCalls).toEqual([1]);
    expect(() => backend.parse(new Uint8Array([1]), config)).toThrow('Parse backend has been disposed');
    expect(mock.createCalls).toEqual([1]);
  });
});
