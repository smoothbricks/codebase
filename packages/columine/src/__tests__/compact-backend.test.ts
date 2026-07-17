import { describe, expect, it } from 'bun:test';

import {
  CompactEncodingError,
  createParseCompactWasmBackend,
  type EventProcessorWasmExports,
} from '../parse-backend.js';
import type { CompactBatch, CompactColumn, EncodedArrowSchema } from '../pipeline.js';

const OUTPUT_HEADER_BYTES = 32;

interface CompactInvocation {
  readonly handle: number;
  readonly batchPtr: number;
  readonly batchLen: number;
  readonly outputPtr: number;
  readonly outputLen: number;
  readonly request: Uint8Array;
}

interface CompactMockOptions {
  readonly payload?: Uint8Array;
  readonly capacityPayload?: Uint8Array;
  readonly resultStatus?: number;
  readonly headerStatus?: number;
  readonly diagnostic?: Partial<{
    version: number;
    stage: number;
    detail: number;
    expectedType: number;
    actualType: number;
    fieldIndex: number;
    rowIndex: number;
  }>;
  readonly throwOnCompact?: boolean;
}

function createCompactMock(options: CompactMockOptions = {}) {
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 4096 });
  const createCalls: Array<{ schema: Uint8Array; metadata: Uint8Array; fieldCount: number }> = [];
  const destroyCalls: number[] = [];
  const compactCalls: CompactInvocation[] = [];
  let nextHandle = 1;

  function createHandle(
    _capacity: number,
    schemaPtr: number,
    schemaLen: number,
    metadataPtr: number,
    fieldCount: number,
  ): number {
    createCalls.push({
      schema: new Uint8Array(memory.buffer, schemaPtr, schemaLen).slice(),
      metadata: new Uint8Array(memory.buffer, metadataPtr, fieldCount * 4).slice(),
      fieldCount,
    });
    return nextHandle++;
  }

  const exports: EventProcessorWasmExports = {
    memory,
    ep_version: () => 1,
    ep_create_with_schema: createHandle,
    ep_create_with_schema_and_names: createHandle,
    ep_destroy: (handle) => destroyCalls.push(handle),
    ep_create_log_entry: (_handle, _inputPtr, _inputLen, _format, outputPtr) => {
      const view = new DataView(memory.buffer);
      view.setUint32(outputPtr, 0, true);
      view.setUint32(outputPtr + 4, OUTPUT_HEADER_BYTES, true);
      view.setUint32(outputPtr + 8, 0, true);
      return 0;
    },
    ep_compact: (handle, batchPtr, batchLen, outputPtr, outputLen) => {
      compactCalls.push({
        handle,
        batchPtr,
        batchLen,
        outputPtr,
        outputLen,
        request: new Uint8Array(memory.buffer, batchPtr, batchLen).slice(),
      });
      if (options.throwOnCompact) {
        throw new Error('mock Compact trap');
      }

      const capacityPayload = options.capacityPayload;
      if (
        capacityPayload !== undefined &&
        outputLen < OUTPUT_HEADER_BYTES + Math.max(4096, capacityPayload.byteLength)
      ) {
        const view = new DataView(memory.buffer);
        view.setUint32(outputPtr, 3, true);
        view.setUint32(outputPtr + 4, OUTPUT_HEADER_BYTES, true);
        view.setUint32(outputPtr + 8, capacityPayload.byteLength, true);
        view.setUint32(outputPtr + 12, 0, true);
        view.setUint32(outputPtr + 16, 0, true);
        view.setUint8(outputPtr + 20, 1);
        view.setUint8(outputPtr + 21, 5);
        return 3;
      }

      const resultStatus = options.resultStatus ?? 0;
      const headerStatus = options.headerStatus ?? resultStatus;
      const payload = options.capacityPayload ?? options.payload ?? new Uint8Array([0xa1, 0xb2, 0xc3]);
      const diagnostic = options.diagnostic ?? {};
      const view = new DataView(memory.buffer);
      view.setUint32(outputPtr, headerStatus, true);
      view.setUint32(outputPtr + 4, headerStatus === 0 ? OUTPUT_HEADER_BYTES : 0, true);
      view.setUint32(outputPtr + 8, headerStatus === 0 ? payload.length : 0, true);
      view.setUint32(outputPtr + 12, headerStatus === 0 ? view.getUint32(batchPtr + 8, true) : 0, true);
      view.setUint32(outputPtr + 16, 0, true);
      view.setUint8(outputPtr + 20, diagnostic.version ?? 1);
      view.setUint8(outputPtr + 21, diagnostic.stage ?? 4);
      view.setUint8(outputPtr + 22, diagnostic.detail ?? 0);
      view.setUint8(outputPtr + 23, diagnostic.expectedType ?? 0);
      view.setUint8(outputPtr + 24, diagnostic.actualType ?? 0);
      view.setUint8(outputPtr + 25, 0);
      view.setUint16(outputPtr + 26, diagnostic.fieldIndex ?? 0xffff, true);
      view.setUint32(outputPtr + 28, diagnostic.rowIndex ?? 0xffff_ffff, true);
      if (headerStatus === 0) {
        new Uint8Array(memory.buffer).set(payload, outputPtr + OUTPUT_HEADER_BYTES);
      }
      return resultStatus;
    },
  };

  return { exports, createCalls, destroyCalls, compactCalls };
}

function requiredAt<T>(values: readonly T[], index: number, label: string): T {
  const value = values[index];
  if (value === undefined) {
    throw new Error(`Missing ${label} at index ${index}`);
  }
  return value;
}

function schema(...fields: ReadonlyArray<readonly [tag: number, nullable: 0 | 1]>): EncodedArrowSchema {
  const metadata = new Uint8Array(fields.length * 4);
  for (let index = 0; index < fields.length; index += 1) {
    const field = requiredAt(fields, index, 'schema field');
    metadata[index * 4] = field[0];
    metadata[index * 4 + 1] = field[1];
  }
  return { schemaBytes: new Uint8Array([0xff, 0xff, 0xff, 0xff, fields.length]), fieldMetadata: metadata };
}

function batch(rowCount: number, encodedSchema: EncodedArrowSchema, columns: readonly CompactColumn[]): CompactBatch {
  return { rowCount, schema: encodedSchema, columns };
}

function validBits(rowCount: number): Uint8Array {
  const bytes = new Uint8Array(Math.ceil(rowCount / 8));
  bytes.fill(0xff);
  if (rowCount % 8 !== 0) {
    bytes[bytes.length - 1] = (1 << (rowCount % 8)) - 1;
  }
  return bytes;
}

describe('Compact CPB1 packing', () => {
  it('writes exact header and descriptor bytes and honors typed-array subviews', () => {
    const mock = createCompactMock();
    const backend = createParseCompactWasmBackend(mock.exports);
    const validityBacking = new Uint8Array([0x44, 0xff, 0x01, 0x55]);
    const valueBacking = new Uint32Array([0xdead_beef, 1, 2, 3, 4, 5, 6, 7, 8, 0xffff_ffff, 0xcafe_babe]);

    backend.encode(
      batch(9, schema([1, 1]), [
        {
          kind: 'u32',
          validity: validityBacking.subarray(1, 3),
          data: valueBacking.subarray(1, 10),
        },
      ]),
    );

    expect(mock.compactCalls).toHaveLength(1);
    const call = requiredAt(mock.compactCalls, 0, 'Compact invocation');
    const expected = new Uint8Array(48);
    const expectedView = new DataView(expected.buffer);
    expectedView.setUint32(0, 0x3142_5043, true);
    expectedView.setUint16(4, 1, true);
    expectedView.setUint16(6, 32, true);
    expectedView.setUint32(8, 9, true);
    expectedView.setUint32(12, 1, true);
    expectedView.setUint8(16, 1);
    expectedView.setUint8(17, 1);
    expectedView.setUint32(20, 48, true);
    expectedView.setUint32(24, 2, true);
    expectedView.setUint32(36, 56, true);
    expectedView.setUint32(40, 36, true);
    expect(call.request.subarray(0, 48)).toEqual(expected);
    expect(call.request.subarray(48, 50)).toEqual(new Uint8Array([0xff, 0x01]));

    const packedValues = new DataView(call.request.buffer, call.request.byteOffset + 56, 36);
    expect(Array.from({ length: 9 }, (_, index) => packedValues.getUint32(index * 4, true))).toEqual([
      1, 2, 3, 4, 5, 6, 7, 8, 0xffff_ffff,
    ]);
  });

  it('accepts and packs the 0/7/8/9 bitmap boundaries', () => {
    for (const rowCount of [0, 7, 8, 9]) {
      const mock = createCompactMock();
      const backend = createParseCompactWasmBackend(mock.exports);
      const bits = validBits(rowCount);
      backend.encode(batch(rowCount, schema([5, 1]), [{ kind: 'bool', data: bits.slice(), validity: bits.slice() }]));
      const request = requiredAt(mock.compactCalls, 0, 'Compact invocation').request;
      const descriptor = new DataView(request.buffer, request.byteOffset + 16, 32);
      expect(descriptor.getUint8(1)).toBe(1);
      expect(descriptor.getUint32(8, true)).toBe(Math.ceil(rowCount / 8));
      expect(descriptor.getUint32(24, true)).toBe(Math.ceil(rowCount / 8));
    }
  });

  it('grows memory before creating views and returns an exact independent copy', () => {
    const payload = new Uint8Array([9, 8, 7, 6]);
    const mock = createCompactMock({ payload });
    const backend = createParseCompactWasmBackend(mock.exports);
    const initialBuffer = mock.exports.memory.buffer;

    const result = backend.encode(batch(1, schema([1, 0]), [{ kind: 'u32', data: new Uint32Array([42]) }]));
    expect(mock.exports.memory.buffer).not.toBe(initialBuffer);
    expect(result).toEqual(payload);
    expect(result.buffer).not.toBe(mock.exports.memory.buffer);

    const call = requiredAt(mock.compactCalls, 0, 'Compact invocation');
    new Uint8Array(mock.exports.memory.buffer, call.outputPtr + OUTPUT_HEADER_BYTES, payload.length).fill(0);
    expect(result).toEqual(payload);
  });

  it('uses little-endian offsets and copies Binary and Utf8 subviews without surrounding bytes', () => {
    const mock = createCompactMock();
    const backend = createParseCompactWasmBackend(mock.exports);
    const offsetsBacking = new Uint32Array([99, 0, 1, 3, 77]);
    const dataBacking = new Uint8Array([55, 0x61, 0xc3, 0xa9, 66]);

    backend.encode(
      batch(2, schema([4, 0]), [
        { kind: 'utf8', offsets: offsetsBacking.subarray(1, 4), data: dataBacking.subarray(1, 4) },
      ]),
    );

    const request = requiredAt(mock.compactCalls, 0, 'Compact invocation').request;
    const descriptor = new DataView(request.buffer, request.byteOffset + 16, 32);
    const offsetsOffset = descriptor.getUint32(12, true);
    const dataOffset = descriptor.getUint32(20, true);
    const offsets = new DataView(request.buffer, request.byteOffset + offsetsOffset, 12);
    expect([offsets.getUint32(0, true), offsets.getUint32(4, true), offsets.getUint32(8, true)]).toEqual([0, 1, 3]);
    expect(request.subarray(dataOffset, dataOffset + 3)).toEqual(new Uint8Array([0x61, 0xc3, 0xa9]));
  });

  it('preserves fixed-width scalar bit patterns', () => {
    const mock = createCompactMock();
    const backend = createParseCompactWasmBackend(mock.exports);
    const f64 = new Float64Array([-0, Number.NaN]);
    const i64 = new BigInt64Array([-(1n << 63n), -1n]);
    const u32 = new Uint32Array([0, 0xffff_ffff]);

    backend.encode(
      batch(2, schema([2, 0], [6, 0], [1, 0]), [
        { kind: 'f64', data: f64 },
        { kind: 'i64', data: i64 },
        { kind: 'u32', data: u32 },
      ]),
    );

    const call = mock.compactCalls.at(0);
    if (call === undefined) {
      throw new Error('Expected one Compact invocation');
    }
    const sources = [new Uint8Array(f64.buffer), new Uint8Array(i64.buffer), new Uint8Array(u32.buffer)];
    for (let index = 0; index < sources.length; index += 1) {
      const descriptor = new DataView(call.request.buffer, call.request.byteOffset + 16 + index * 32, 32);
      const dataOffset = descriptor.getUint32(20, true);
      const source = sources[index];
      if (source === undefined) {
        throw new Error(`Missing scalar source ${index}`);
      }
      expect(call.request.subarray(dataOffset, dataOffset + source.byteLength)).toEqual(source);
    }
  });
});

describe('Compact validation', () => {
  it('rejects batch, schema, metadata, and column-shape violations before any WASM call', () => {
    const invalidBatches: Array<[string, CompactBatch]> = [
      ['rowCount negative', batch(-1, schema(), [])],
      ['rowCount fractional', batch(1.5, schema(), [])],
      ['rowCount too large', batch(65_537, schema(), [])],
      [
        'metadata not four-byte records',
        batch(0, { schemaBytes: new Uint8Array(), fieldMetadata: new Uint8Array([1]) }, []),
      ],
      ['field count mismatch', batch(0, schema([1, 0]), [])],
      [
        'unknown physical type',
        batch(0, { schemaBytes: new Uint8Array(), fieldMetadata: new Uint8Array([7, 0, 0, 0]) }, [{ kind: 'null' }]),
      ],
      [
        'invalid nullable byte',
        batch(0, { schemaBytes: new Uint8Array(), fieldMetadata: new Uint8Array([0, 2, 0, 0]) }, [{ kind: 'null' }]),
      ],
      [
        'nonzero metadata padding',
        batch(0, { schemaBytes: new Uint8Array(), fieldMetadata: new Uint8Array([0, 1, 1, 0]) }, [{ kind: 'null' }]),
      ],
      ['kind mismatch', batch(0, schema([2, 0]), [{ kind: 'u32', data: new Uint32Array() }])],
      ['non-nullable Null', batch(0, schema([0, 0]), [{ kind: 'null' }])],
    ];

    for (const [label, invalidBatch] of invalidBatches) {
      const mock = createCompactMock();
      const backend = createParseCompactWasmBackend(mock.exports);
      expect(() => backend.encode(invalidBatch), label).toThrow();
      expect(mock.createCalls, label).toHaveLength(0);
      expect(mock.compactCalls, label).toHaveLength(0);
    }
  });

  it('validates fixed-width arrays and nullable validity exactly', () => {
    const cases: CompactBatch[] = [
      batch(2, schema([1, 0]), [{ kind: 'u32', data: new Uint32Array([1]) }]),
      batch(1, schema([2, 0]), [{ kind: 'f64', data: new Float64Array(2) }]),
      batch(1, schema([6, 0]), [{ kind: 'i64', data: new BigInt64Array(0) }]),
      batch(1, schema([1, 0]), [{ kind: 'u32', data: new Uint32Array([1]), validity: new Uint8Array([1]) }]),
      batch(9, schema([1, 1]), [{ kind: 'u32', data: new Uint32Array(9), validity: new Uint8Array([0xff]) }]),
      batch(9, schema([1, 1]), [{ kind: 'u32', data: new Uint32Array(9), validity: new Uint8Array([0xff, 0x81]) }]),
    ];

    for (const invalidBatch of cases) {
      const mock = createCompactMock();
      expect(() => createParseCompactWasmBackend(mock.exports).encode(invalidBatch)).toThrow();
      expect(mock.compactCalls).toHaveLength(0);
    }
  });

  it('validates Bool lengths and unused high bits', () => {
    for (const data of [new Uint8Array(), new Uint8Array([0x80])]) {
      const mock = createCompactMock();
      const backend = createParseCompactWasmBackend(mock.exports);
      expect(() => backend.encode(batch(1, schema([5, 0]), [{ kind: 'bool', data }]))).toThrow();
      expect(mock.compactCalls).toHaveLength(0);
    }
  });

  it('validates variable offsets, null intervals, final boundaries, and per-value UTF-8', () => {
    const invalidColumns: CompactColumn[] = [
      { kind: 'binary', offsets: new Uint32Array([0, 1]), data: new Uint8Array([1]) },
      { kind: 'binary', offsets: new Uint32Array([1, 1, 1]), data: new Uint8Array([1]) },
      { kind: 'binary', offsets: new Uint32Array([0, 2, 1]), data: new Uint8Array([1, 2]) },
      { kind: 'binary', offsets: new Uint32Array([0, 1, 3]), data: new Uint8Array([1, 2]) },
      { kind: 'binary', offsets: new Uint32Array([0, 1, 1]), data: new Uint8Array([1, 2]) },
      {
        kind: 'binary',
        offsets: new Uint32Array([0, 1, 1]),
        data: new Uint8Array([1]),
        validity: new Uint8Array([0b10]),
      },
      { kind: 'utf8', offsets: new Uint32Array([0, 1, 2]), data: new Uint8Array([0xc3, 0xa9]) },
    ];

    for (const column of invalidColumns) {
      const mock = createCompactMock();
      const tag = column.kind === 'utf8' ? 4 : 3;
      const nullable = 'validity' in column && column.validity !== undefined ? 1 : 0;
      expect(() =>
        createParseCompactWasmBackend(mock.exports).encode(batch(2, schema([tag, nullable]), [column])),
      ).toThrow();
      expect(mock.compactCalls).toHaveLength(0);
    }
  });

  it('enforces explicit zero-row shapes', () => {
    const mock = createCompactMock();
    const backend = createParseCompactWasmBackend(mock.exports);
    const result = backend.encode(
      batch(0, schema([0, 1], [1, 1], [5, 1], [3, 1]), [
        { kind: 'null' },
        { kind: 'u32', data: new Uint32Array(), validity: new Uint8Array() },
        { kind: 'bool', data: new Uint8Array(), validity: new Uint8Array() },
        { kind: 'binary', offsets: new Uint32Array([0]), data: new Uint8Array(), validity: new Uint8Array() },
      ]),
    );
    expect(result).toEqual(new Uint8Array([0xa1, 0xb2, 0xc3]));

    const invalid = batch(0, schema([3, 0]), [{ kind: 'binary', offsets: new Uint32Array(), data: new Uint8Array() }]);
    expect(() => backend.encode(invalid)).toThrow(/rowCount \+ 1/);
    expect(mock.compactCalls).toHaveLength(1);
  });

  it('rejects field and packed-batch bounds before calling native code', () => {
    const tooManyFields = new Uint8Array(257 * 4);
    for (let index = 0; index < 257; index += 1) {
      tooManyFields[index * 4 + 1] = 1;
    }
    const tooManyColumns = Array.from({ length: 257 }, (): CompactColumn => ({ kind: 'null' }));
    const fieldMock = createCompactMock();
    expect(() =>
      createParseCompactWasmBackend(fieldMock.exports).encode(
        batch(0, { schemaBytes: new Uint8Array(), fieldMetadata: tooManyFields }, tooManyColumns),
      ),
    ).toThrow(/more than 256 fields/);
    expect(fieldMock.compactCalls).toHaveLength(0);

    const data = new Uint8Array(16 * 1024 * 1024);
    const offsets = new Uint32Array([0, data.byteLength]);
    const oversizedColumns = Array.from({ length: 5 }, (): CompactColumn => ({ kind: 'binary', offsets, data }));
    const oversizedMetadata = new Uint8Array(oversizedColumns.length * 4);
    for (let index = 0; index < oversizedColumns.length; index += 1) {
      oversizedMetadata[index * 4] = 3;
    }
    const sizeMock = createCompactMock();
    expect(() =>
      createParseCompactWasmBackend(sizeMock.exports).encode(
        batch(1, { schemaBytes: new Uint8Array(), fieldMetadata: oversizedMetadata }, oversizedColumns),
      ),
    ).toThrow(/packed batch/);
    expect(sizeMock.compactCalls).toHaveLength(0);
  });
});

describe('Compact native results and lifecycle', () => {
  it('maps native status and the complete diagnostic to a typed error without discarding the handle', () => {
    const mock = createCompactMock({
      resultStatus: 6,
      diagnostic: { detail: 7, expectedType: 4, actualType: 3, fieldIndex: 2, rowIndex: 9 },
    });
    const backend = createParseCompactWasmBackend(mock.exports);
    const input = batch(0, schema(), []);

    let thrown: unknown;
    try {
      backend.encode(input);
    } catch (error) {
      thrown = error;
    }
    if (!(thrown instanceof CompactEncodingError)) {
      throw new Error('Expected CompactEncodingError');
    }
    const compactError = thrown;
    expect(compactError.code).toBe('INVALID_INPUT');
    expect(compactError.status).toBe(6);
    expect(compactError.diagnostic).toEqual({
      version: 1,
      stage: 4,
      detail: 7,
      expectedType: 4,
      actualType: 3,
      fieldIndex: 2,
      rowIndex: 9,
    });

    expect(() => backend.encode(input)).toThrow(CompactEncodingError);
    expect(mock.createCalls).toHaveLength(1);
    expect(mock.destroyCalls).toHaveLength(0);
  });

  it('retries once with the exact native-reported Arrow capacity', () => {
    const payload = new Uint8Array(5000);
    payload.fill(0x5a);
    const mock = createCompactMock({ capacityPayload: payload });
    const backend = createParseCompactWasmBackend(mock.exports);

    const result = backend.encode(batch(0, schema(), []));

    expect(result).toEqual(payload);
    expect(mock.compactCalls.map((call) => call.outputLen)).toEqual([
      OUTPUT_HEADER_BYTES + 4096,
      OUTPUT_HEADER_BYTES + payload.byteLength,
    ]);
    expect(mock.destroyCalls).toHaveLength(0);
  });

  it('destroys the handle when the return and result-header statuses disagree', () => {
    const mock = createCompactMock({ resultStatus: 6, headerStatus: 7 });
    const backend = createParseCompactWasmBackend(mock.exports);

    expect(() => backend.encode(batch(0, schema(), []))).toThrow('returned status 6 but wrote result-header status 7');
    expect(mock.destroyCalls).toEqual([1]);
  });

  it('reuses byte-identical schema and metadata and destroys the handle on a schema change', () => {
    const mock = createCompactMock();
    const backend = createParseCompactWasmBackend(mock.exports);
    const first = batch(1, schema([1, 0]), [{ kind: 'u32', data: new Uint32Array([1]) }]);
    const identical = batch(
      1,
      {
        schemaBytes: first.schema.schemaBytes.slice(),
        fieldMetadata: first.schema.fieldMetadata.slice(),
      },
      [{ kind: 'u32', data: new Uint32Array([2]) }],
    );
    const changed = batch(1, { schemaBytes: new Uint8Array([9]), fieldMetadata: new Uint8Array([1, 0, 0, 0]) }, [
      { kind: 'u32', data: new Uint32Array([3]) },
    ]);

    backend.encode(first);
    backend.encode(identical);
    backend.encode(changed);
    expect(mock.createCalls).toHaveLength(2);
    expect(mock.destroyCalls).toEqual([1]);
    expect(mock.compactCalls.map((call) => call.handle)).toEqual([1, 1, 2]);
  });

  it('destroys a trapped handle, disposes once, and rejects encode after disposal', () => {
    const trapped = createCompactMock({ throwOnCompact: true });
    const trappedBackend = createParseCompactWasmBackend(trapped.exports);
    expect(() => trappedBackend.encode(batch(0, schema(), []))).toThrow('mock Compact trap');
    expect(trapped.destroyCalls).toEqual([1]);

    const mock = createCompactMock();
    const backend = createParseCompactWasmBackend(mock.exports);
    const input = batch(0, schema(), []);
    backend.encode(input);
    backend.dispose();
    backend.dispose();
    expect(mock.destroyCalls).toEqual([1]);
    expect(() => backend.encode(input)).toThrow('Parse/Compact backend has been disposed');
    expect(mock.compactCalls).toHaveLength(1);
  });
});
