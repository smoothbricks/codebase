/**
 * Integration tests for @smoothbricks/columine standalone usage.
 *
 * Verifies all core pipeline success criteria:
 *   SC1: Reduce stage for aggregation (HashMap + Aggregate slots)
 *   SC2: Undo stage for event cancellation (checkpoint/rollback/commit)
 *   SC3: Pipeline composition with explicit backend ownership
 *   SC4: Streaming processing (multiple batches accumulate state)
 *
 * SC3 is a TypeScript-only test (no WASM needed).
 * SC1, SC2, SC4 require the columine WASM binary and skip gracefully if unavailable.
 */

import { beforeAll, describe, expect, it } from 'bun:test';
import { existsSync } from 'node:fs';

import {
  AggType,
  type ColumineBackend,
  type ColumnInput,
  createPipeline,
  ErrorCode,
  HEADER_SIZE,
  MAGIC,
  Opcode,
  type ParseCompactBackend,
  PROGRAM_HASH_PREFIX,
  type ReducerProgram,
  SlotType,
  SlotTypeFlag,
  ValueType,
} from '../index.js';
import { loadColumineWasm } from '../wasm-backend.js';

// =============================================================================
// WASM Binary Detection
// =============================================================================

const WASM_PATH = new URL('../../dist/columine.wasm', import.meta.url);
const WASM_EXISTS = existsSync(WASM_PATH.pathname);
const TEST_PARSE_BACKEND: ParseCompactBackend = {
  backend: 'test-parse',
  parse: () => ({ arrowIpc: new Uint8Array(0), eventCount: 0 }),
  encode: () => new Uint8Array(0),
  dispose: () => {},
};

function createMockColumineBackend(overrides: Partial<ColumineBackend> = {}): ColumineBackend {
  return {
    backend: 'mock',
    loadProgram: async () => ({ bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] }),
    createState: () => ({ _brand: 'ColumineStateHandle' }),
    resetState: () => {},
    executeBatch: () => ErrorCode.OK,
    executeBatchDelta: () => ErrorCode.OK,
    getMapSize: () => 0,
    getSetSize: () => 0,
    getAggregateValue: () => 0,
    getScalarValue: () => ({ kind: 'empty' }),
    mapGet: () => undefined,
    setContains: () => false,
    structMap2GetRow: () => undefined,
    structMap2Entries: () => [],
    evictExpired: () => ({ ok: true, count: 0, rows: [] }),
    serialize: () => new Uint8Array(0),
    deserialize: () => ({ _brand: 'ColumineStateHandle' }),
    undoEnable: () => {},
    undoCheckpoint: () => 0,
    undoRollback: () => {},
    undoCommit: () => {},
    undoHasOverflow: () => false,
    deltaExportSegment: () => ({
      undoBytes: new Uint8Array(0),
      redoBytes: new Uint8Array(0),
      entrySize: 24,
      overflow: false,
    }),
    deltaApplyRollbackSegment: () => {},
    deltaApplyRollforwardSegment: () => {},
    ...overrides,
  };
}

// =============================================================================
// BytecodeBuilder (minimal, local to avoid circular dep on axe-runtime)
// =============================================================================

/**
 * Minimal bytecode builder for test programs.
 * We avoid importing from axe-runtime to prove columine works standalone.
 */
function buildProgram(opts: {
  slots: Array<
    | {
        type: 'hashmap';
        capacity: number;
        ttl?: { seconds: number; trigger: boolean; timestampField: number };
      }
    | { type: 'hashset'; capacity: number }
    | { type: 'aggregate'; aggType: AggType }
    | { type: 'scalar'; aggType: AggType }
    | { type: 'condition-tree' }
  >;
  numInputs: number;
  reduceOps: number[];
}): Uint8Array {
  const initCode: number[] = [];
  for (let i = 0; i < opts.slots.length; i++) {
    const slot = opts.slots[i];
    switch (slot.type) {
      case 'hashmap': {
        const flags =
          SlotType.HASHMAP |
          (slot.ttl ? SlotTypeFlag.HAS_TTL : 0) |
          (slot.ttl?.trigger ? SlotTypeFlag.HAS_EVICT_TRIGGER : 0);
        initCode.push(Opcode.SLOT_DEF, i, flags, slot.capacity & 0xff, (slot.capacity >> 8) & 0xff);
        if (slot.ttl) {
          const ttlBytes = new ArrayBuffer(8);
          const ttlView = new DataView(ttlBytes);
          ttlView.setFloat32(0, slot.ttl.seconds, true);
          ttlView.setFloat32(4, 0, true);
          initCode.push(...new Uint8Array(ttlBytes), slot.ttl.timestampField, 0);
        }
        break;
      }
      case 'hashset':
        initCode.push(Opcode.SLOT_DEF, i, SlotType.HASHSET, slot.capacity & 0xff, (slot.capacity >> 8) & 0xff);
        break;
      case 'aggregate':
        // SLOT_DEF: slot, type_flags(AGGREGATE), aggType in cap_lo, 0
        initCode.push(Opcode.SLOT_DEF, i, SlotType.AGGREGATE, slot.aggType, 0);
        break;
      case 'scalar':
        initCode.push(Opcode.SLOT_DEF, i, SlotType.SCALAR, slot.aggType, 0);
        break;
      case 'condition-tree':
        initCode.push(Opcode.SLOT_DEF, i, SlotType.CONDITION_TREE, 0, 0);
        break;
    }
  }
  initCode.push(Opcode.HALT);

  const reduceCode = [...opts.reduceOps, Opcode.HALT];
  const contentLen = HEADER_SIZE + initCode.length + reduceCode.length;
  const program = new Uint8Array(PROGRAM_HASH_PREFIX + contentLen);
  // [0..31] hash prefix (zeros - filled by fillProgramHash at async use)
  // [32..] content header + init + reduce

  const base = PROGRAM_HASH_PREFIX;
  // Magic "AXE1" (little-endian)
  program[base + 0] = MAGIC & 0xff;
  program[base + 1] = (MAGIC >> 8) & 0xff;
  program[base + 2] = (MAGIC >> 16) & 0xff;
  program[base + 3] = (MAGIC >> 24) & 0xff;

  // Version 1.0
  program[base + 4] = 1;
  program[base + 5] = 0;

  // num_slots, num_cols
  program[base + 6] = opts.slots.length;
  program[base + 7] = opts.numInputs;

  // Reserved
  program[base + 8] = 0;
  program[base + 9] = 0;

  // init_len (u16 LE)
  program[base + 10] = initCode.length & 0xff;
  program[base + 11] = (initCode.length >> 8) & 0xff;

  // reduce_len (u16 LE)
  program[base + 12] = reduceCode.length & 0xff;
  program[base + 13] = (reduceCode.length >> 8) & 0xff;

  // Init section
  program.set(initCode, base + HEADER_SIZE);

  // Reduce section
  program.set(reduceCode, base + HEADER_SIZE + initCode.length);

  return program;
}

// =============================================================================
// SC1: Reduce stage for aggregation
// =============================================================================

describe('SC1: Reduce stage for aggregation', () => {
  let backend: ColumineBackend;

  beforeAll(async () => {
    if (!WASM_EXISTS) return;
    backend = await loadColumineWasm(WASM_PATH);
  });

  it.skipIf(!WASM_EXISTS)('executes HashMap upsert-last program', async () => {
    // Build: 1 HashMap (capacity 64), 2 inputs (key, value)
    // Op: BATCH_MAP_UPSERT_LAST slot=0, keyCol=0, valCol=1
    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Keys: [1, 2, 3, 1] -> last wins for key 1
    // Values: [10, 20, 30, 40]
    const keys = new Uint32Array([1, 2, 3, 1]);
    const values = new Uint32Array([10, 20, 30, 40]);

    const columns: ColumnInput[] = [
      { data: keys, type: ValueType.UINT32 },
      { data: values, type: ValueType.UINT32 },
    ];

    const result = backend.executeBatch(state, program, columns, 4);
    expect(result).toBe(ErrorCode.OK);

    // Verify map contents
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 1)).toBe(40); // last write wins
    expect(backend.mapGet(state, program, 0, 2)).toBe(20);
    expect(backend.mapGet(state, program, 0, 3)).toBe(30);
  });

  it.skipIf(!WASM_EXISTS)('executes Aggregate SUM program', async () => {
    // Build: 1 Aggregate SUM, 1 input (values)
    // Op: BATCH_AGG_SUM slot=0, valCol=0
    const bytecode = buildProgram({
      slots: [{ type: 'aggregate', aggType: AggType.SUM }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_AGG_SUM, 0, 0],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // SUM reads Float64 values from the column
    const values = new Float64Array([10.0, 20.0, 30.0, 40.0]);
    const columns: ColumnInput[] = [{ data: values, type: ValueType.FLOAT64 }];

    const result = backend.executeBatch(state, program, columns, 4);
    expect(result).toBe(ErrorCode.OK);

    expect(backend.getAggregateValue(state, program, 0)).toBe(100);
  });

  it.skipIf(!WASM_EXISTS)('executes Aggregate COUNT program', async () => {
    // Build: 1 Aggregate COUNT, 1 input (dummy, COUNT ignores column values)
    const bytecode = buildProgram({
      slots: [{ type: 'aggregate', aggType: AggType.COUNT }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_AGG_COUNT, 0],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // COUNT ignores values, just counts rows
    const values = new Float64Array([1, 2, 3, 4, 5, 6, 7]);
    const columns: ColumnInput[] = [{ data: values, type: ValueType.FLOAT64 }];

    const result = backend.executeBatch(state, program, columns, 7);
    expect(result).toBe(ErrorCode.OK);

    expect(backend.getAggregateValue(state, program, 0)).toBe(7);
  });

  it.skipIf(!WASM_EXISTS)('executes HashMap + Aggregate SUM combined program', async () => {
    // Build: 1 HashMap (cap 64) + 1 Aggregate SUM, 3 inputs (key, val, amount)
    const bytecode = buildProgram({
      slots: [
        { type: 'hashmap', capacity: 64 },
        { type: 'aggregate', aggType: AggType.SUM },
      ],
      numInputs: 3,
      reduceOps: [
        Opcode.BATCH_MAP_UPSERT_LAST,
        0,
        0,
        1, // map[key] = val
        Opcode.BATCH_AGG_SUM,
        1,
        2, // sum += amount
      ],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    const keys = new Uint32Array([1, 2, 1, 3]);
    const values = new Uint32Array([100, 200, 150, 300]);
    const amounts = new Float64Array([10.0, 20.0, 30.0, 40.0]);
    const columns: ColumnInput[] = [
      { data: keys, type: ValueType.UINT32 },
      { data: values, type: ValueType.UINT32 },
      { data: amounts, type: ValueType.FLOAT64 },
    ];

    const result = backend.executeBatch(state, program, columns, 4);
    expect(result).toBe(ErrorCode.OK);

    // HashMap: 3 unique keys
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 1)).toBe(150); // last write
    expect(backend.mapGet(state, program, 0, 2)).toBe(200);
    expect(backend.mapGet(state, program, 0, 3)).toBe(300);

    // Aggregate: sum = 10 + 20 + 30 + 40 = 100
    expect(backend.getAggregateValue(state, program, 1)).toBe(100);
  });

  it.skipIf(!WASM_EXISTS)('grows HashMap and HashSet without duplicating partial batch mutations', async () => {
    const keys = Uint32Array.from({ length: 12 }, (_, index) => index + 1);
    const values = Uint32Array.from(keys, (key) => key * 10);
    const amounts = Float64Array.from(keys);
    const mapProgram = await backend.loadProgram(
      buildProgram({
        slots: [
          { type: 'hashmap', capacity: 4 },
          { type: 'aggregate', aggType: AggType.SUM },
        ],
        numInputs: 3,
        reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1, Opcode.BATCH_AGG_SUM, 1, 2],
      }),
    );
    const mapState = backend.createState(mapProgram);

    expect(
      backend.executeBatch(
        mapState,
        mapProgram,
        [
          { data: keys, type: ValueType.UINT32 },
          { data: values, type: ValueType.UINT32 },
          { data: amounts, type: ValueType.FLOAT64 },
        ],
        keys.length,
      ),
    ).toBe(ErrorCode.OK);
    expect(backend.getMapSize(mapState, mapProgram, 0)).toBe(keys.length);
    expect(backend.getAggregateValue(mapState, mapProgram, 1)).toBe(78);
    for (const key of keys) expect(backend.mapGet(mapState, mapProgram, 0, key)).toBe(key * 10);

    const setProgram = await backend.loadProgram(
      buildProgram({
        slots: [{ type: 'hashset', capacity: 4 }],
        numInputs: 1,
        reduceOps: [Opcode.BATCH_SET_INSERT, 0, 0],
      }),
    );
    const setState = backend.createState(setProgram);
    expect(backend.executeBatch(setState, setProgram, [{ data: keys, type: ValueType.UINT32 }], keys.length)).toBe(
      ErrorCode.OK,
    );
    expect(backend.getSetSize(setState, setProgram, 0)).toBe(keys.length);
    for (const key of keys) expect(backend.setContains(setState, setProgram, 0, key)).toBeTrue();
  });

  it.skipIf(!WASM_EXISTS)('reads U32, F64, and I64 scalar slots losslessly with explicit empty state', async () => {
    const scalarProgram = await backend.loadProgram(
      buildProgram({
        slots: [
          { type: 'scalar', aggType: AggType.SCALAR_U32 },
          { type: 'scalar', aggType: AggType.SCALAR_F64 },
          { type: 'scalar', aggType: AggType.SCALAR_I64 },
        ],
        numInputs: 4,
        reduceOps: [0x48, 0, 0, 3, 0x48, 1, 1, 3, 0x48, 2, 2, 3],
      }),
    );
    const state = backend.createState(scalarProgram);
    expect(backend.getScalarValue(state, scalarProgram, 0)).toEqual({ kind: 'empty' });
    expect(backend.getScalarValue(state, scalarProgram, 1)).toEqual({ kind: 'empty' });
    expect(backend.getScalarValue(state, scalarProgram, 2)).toEqual({ kind: 'empty' });

    const exactI64 = 9_007_199_254_740_993n;
    expect(
      backend.executeBatch(
        state,
        scalarProgram,
        [
          { data: new Uint32Array([0xffff_fffe]), type: ValueType.UINT32 },
          { data: new Float64Array([Math.PI]), type: ValueType.FLOAT64 },
          { data: new BigInt64Array([exactI64]), type: ValueType.UINT32 },
          { data: new Float64Array([100]), type: ValueType.FLOAT64 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);
    expect(backend.getScalarValue(state, scalarProgram, 0)).toEqual({ kind: 'u32', value: 0xffff_fffe });
    expect(backend.getScalarValue(state, scalarProgram, 1)).toEqual({ kind: 'f64', value: Math.PI });
    expect(backend.getScalarValue(state, scalarProgram, 2)).toEqual({ kind: 'i64', value: exactI64 });
  });

  it.skipIf(!WASM_EXISTS)('returns an unambiguous four-row TTL eviction result and copied trigger rows', async () => {
    const program = await backend.loadProgram(
      buildProgram({
        slots: [{ type: 'hashmap', capacity: 8, ttl: { seconds: 10, trigger: true, timestampField: 2 } }],
        numInputs: 3,
        reduceOps: [Opcode.BATCH_MAP_UPSERT_LATEST, 0, 0, 1, 2, 1],
      }),
    );
    const state = backend.createState(program);
    const keys = new Uint32Array([1, 2, 3, 4]);
    const values = new Uint32Array([10, 20, 30, 40]);
    const timestamps = new Float64Array([100, 101, 102, 103]);
    expect(
      backend.executeBatch(
        state,
        program,
        [
          { data: keys, type: ValueType.UINT32 },
          { data: values, type: ValueType.UINT32 },
          { data: timestamps, type: ValueType.FLOAT64 },
        ],
        4,
      ),
    ).toBe(ErrorCode.OK);

    const result = backend.evictExpired(state, program, 114);
    expect(result.ok).toBeTrue();
    if (!result.ok) throw new Error(`unexpected eviction error ${result.error}`);
    expect(result.count).toBe(4);
    expect(result.rows).toEqual([
      { slot: 0, timestamp: 100, key: 1, value: 10 },
      { slot: 0, timestamp: 101, key: 2, value: 20 },
      { slot: 0, timestamp: 102, key: 3, value: 30 },
      { slot: 0, timestamp: 103, key: 4, value: 40 },
    ]);
    expect(backend.getMapSize(state, program, 0)).toBe(0);
    const restored = backend.deserialize(program, backend.serialize(state, program));
    expect(backend.getMapSize(restored, program, 0)).toBe(0);
    for (const key of keys) expect(backend.mapGet(restored, program, 0, key)).toBeUndefined();
  });

  it.skipIf(!WASM_EXISTS)('parses and initializes CONDITION_TREE slots as first-class slot types', async () => {
    const bytecode = buildProgram({
      slots: [{ type: 'condition-tree' }, { type: 'aggregate', aggType: AggType.COUNT }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_AGG_COUNT, 1],
    });

    const program = await backend.loadProgram(bytecode);
    expect(program.slotDefs[0]).toEqual({ type: SlotType.CONDITION_TREE });
    expect(program.slotDefs[1]).toEqual({ type: SlotType.AGGREGATE, aggType: AggType.COUNT });

    const state = backend.createState(program);
    const serialized = backend.serialize(state, program);

    // vm.zig slot metadata layout: TYPE_FLAGS at byte 12 of each 48-byte slot meta block.
    const STATE_HEADER_SIZE = 32;
    const SLOT_META_SIZE = 48;
    const OFFSET_OFFSET = 0;
    const TYPE_FLAGS_OFFSET = 12;
    const slot0MetaBase = STATE_HEADER_SIZE;
    const slot1MetaBase = STATE_HEADER_SIZE + SLOT_META_SIZE;
    const stateView = new DataView(serialized.buffer, serialized.byteOffset, serialized.byteLength);

    const typeFlags = serialized[slot0MetaBase + TYPE_FLAGS_OFFSET];
    const conditionTreeOffset = stateView.getUint32(slot0MetaBase + OFFSET_OFFSET, true);
    const aggregateOffset = stateView.getUint32(slot1MetaBase + OFFSET_OFFSET, true);

    expect(typeFlags & 0x0f).toBe(SlotType.CONDITION_TREE);
    expect(aggregateOffset - conditionTreeOffset).toBe(8);
    expect(stateView.getUint32(conditionTreeOffset, true)).toBe(1);
    expect(stateView.getUint32(conditionTreeOffset + 4, true)).toBe(0xffffffff);

    // Verify the state can execute and round-trip with CONDITION_TREE metadata intact.
    const result = backend.executeBatch(state, program, [{ data: new Float64Array([1]), type: ValueType.FLOAT64 }], 1);
    expect(result).toBe(ErrorCode.OK);

    const restored = backend.deserialize(program, serialized);
    const restoredBytes = backend.serialize(restored, program);
    const restoredTypeFlags = restoredBytes[STATE_HEADER_SIZE + TYPE_FLAGS_OFFSET];
    const restoredView = new DataView(restoredBytes.buffer, restoredBytes.byteOffset, restoredBytes.byteLength);
    const restoredConditionTreeOffset = restoredView.getUint32(slot0MetaBase + OFFSET_OFFSET, true);

    expect(restoredTypeFlags & 0x0f).toBe(SlotType.CONDITION_TREE);
    expect(restoredView.getUint32(restoredConditionTreeOffset, true)).toBe(1);
    expect(restoredView.getUint32(restoredConditionTreeOffset + 4, true)).toBe(0xffffffff);
  });
});

// =============================================================================
// SC2: Undo stage for event cancellation
// =============================================================================

describe('SC2: Undo stage for event cancellation', () => {
  let backend: ColumineBackend;

  beforeAll(async () => {
    if (!WASM_EXISTS) return;
    backend = await loadColumineWasm(WASM_PATH);
  });

  it.skipIf(!WASM_EXISTS)('checkpoint and rollback restores pre-batch state', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    // Build a simple SUM program (SUM reads Float64 values)
    const bytecode = buildProgram({
      slots: [{ type: 'aggregate', aggType: AggType.SUM }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_AGG_SUM, 0, 0],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // First batch: sum = 100
    const batch1 = new Float64Array([10, 20, 30, 40]);
    stages.reduce.executeBatch(state, program, [{ data: batch1, type: ValueType.FLOAT64 }], 4);
    expect(backend.getAggregateValue(state, program, 0)).toBe(100);

    // Checkpoint before speculative batch
    const token = stages.undo.checkpoint(state);

    // Speculative batch: sum should be 100 + 500 = 600
    const batch2 = new Float64Array([100, 200, 200]);
    stages.reduce.executeBatch(state, program, [{ data: batch2, type: ValueType.FLOAT64 }], 3);
    expect(backend.getAggregateValue(state, program, 0)).toBe(600);

    // Rollback: should restore to 100
    stages.undo.rollback(state, token);
    expect(backend.getAggregateValue(state, program, 0)).toBe(100);
  });

  it.skipIf(!WASM_EXISTS)('commit makes changes permanent', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    const bytecode = buildProgram({
      slots: [{ type: 'aggregate', aggType: AggType.SUM }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_AGG_SUM, 0, 0],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // First batch: sum = 50
    const batch1 = new Float64Array([20, 30]);
    stages.reduce.executeBatch(state, program, [{ data: batch1, type: ValueType.FLOAT64 }], 2);

    // Checkpoint
    const token = stages.undo.checkpoint(state);

    // Second batch: sum = 50 + 100 = 150
    const batch2 = new Float64Array([100]);
    stages.reduce.executeBatch(state, program, [{ data: batch2, type: ValueType.FLOAT64 }], 1);
    expect(backend.getAggregateValue(state, program, 0)).toBe(150);

    // Commit: changes become permanent
    stages.undo.commit(state, token);
    expect(backend.getAggregateValue(state, program, 0)).toBe(150);
  });
});

// =============================================================================
// SC3: Pipeline composition (TypeScript-only, no WASM needed)
// =============================================================================

describe('SC3: Pipeline composition', () => {
  it('createPipeline returns all four stages', () => {
    // Supply the concrete backend owned by this pipeline.
    const mockBackend = createMockColumineBackend();

    const stages = createPipeline({ backend: mockBackend, parseBackend: TEST_PARSE_BACKEND });

    // All four stages exist
    expect(stages.parse).toBeDefined();
    expect(stages.reduce).toBeDefined();
    expect(stages.compact).toBeDefined();
    expect(stages.undo).toBeDefined();

    // Each stage has correct name
    expect(stages.parse.name).toBe('parse');
    expect(stages.reduce.name).toBe('reduce');
    expect(stages.compact.name).toBe('compact');
    expect(stages.undo.name).toBe('undo');
  });

  it('reduce stage delegates to the supplied backend', () => {
    let executeBatchCalled = false;

    const mockBackend = createMockColumineBackend({
      executeBatch: () => {
        executeBatchCalled = true;
        return ErrorCode.OK;
      },
    });

    const stages = createPipeline({ backend: mockBackend, parseBackend: TEST_PARSE_BACKEND });

    const mockState = { _brand: 'ColumineStateHandle' as const };
    const mockProgram: ReducerProgram = { bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] };
    const mockColumns: ColumnInput[] = [];

    const result = stages.reduce.executeBatch(mockState, mockProgram, mockColumns, 10);

    expect(executeBatchCalled).toBe(true);
    expect(result).toBe(ErrorCode.OK);
  });

  it('keeps independently constructed pipelines isolated', () => {
    const calls: string[] = [];
    const createNamedBackend = (name: string): ColumineBackend =>
      createMockColumineBackend({
        backend: name,
        executeBatch: () => {
          calls.push(name);
          return ErrorCode.OK;
        },
      });
    const first = createPipeline({ backend: createNamedBackend('first'), parseBackend: TEST_PARSE_BACKEND });
    const second = createPipeline({ backend: createNamedBackend('second'), parseBackend: TEST_PARSE_BACKEND });
    const state = { _brand: 'ColumineStateHandle' as const };
    const program: ReducerProgram = { bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] };

    first.reduce.executeBatch(state, program, [], 0);
    second.reduce.executeBatch(state, program, [], 0);
    first.reduce.executeBatch(state, program, [], 0);

    expect(calls).toEqual(['first', 'second', 'first']);
  });

  it('parse stage delegates to the supplied parse backend', () => {
    let parseCalls = 0;
    const parseBackend: ParseCompactBackend = {
      ...TEST_PARSE_BACKEND,
      parse: () => {
        parseCalls += 1;
        return { arrowIpc: new Uint8Array([1]), eventCount: 1 };
      },
    };
    const mockBackend = createMockColumineBackend();
    const stages = createPipeline({ backend: mockBackend, parseBackend });

    const result = stages.parse.parse('[]', {
      schemaBytes: new Uint8Array(0),
      fieldMetadata: new Uint8Array(0),
    });

    expect(parseCalls).toBe(1);
    expect(result.eventCount).toBe(1);
  });

  it('stages are independently usable', () => {
    let reduceCalled = false;

    const mockBackend = createMockColumineBackend({
      executeBatch: () => {
        reduceCalled = true;
        return ErrorCode.OK;
      },
    });

    const stages = createPipeline({ backend: mockBackend, parseBackend: TEST_PARSE_BACKEND });

    // Reduce and Parse use their respective explicit backends.
    const mockState = { _brand: 'ColumineStateHandle' as const };
    const mockProgram: ReducerProgram = { bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] };
    stages.reduce.executeBatch(mockState, mockProgram, [], 0);
    expect(reduceCalled).toBe(true);

    expect(
      stages.parse.parse('[]', { schemaBytes: new Uint8Array(0), fieldMetadata: new Uint8Array(0) }).eventCount,
    ).toBe(0);
  });
});

// =============================================================================
// SC4: Streaming processing (multiple batches accumulate state)
// =============================================================================

describe('SC4: Streaming processing', () => {
  let backend: ColumineBackend;

  beforeAll(async () => {
    if (!WASM_EXISTS) return;
    backend = await loadColumineWasm(WASM_PATH);
  });

  it.skipIf(!WASM_EXISTS)('multiple batches accumulate SUM correctly', async () => {
    const bytecode = buildProgram({
      slots: [{ type: 'aggregate', aggType: AggType.SUM }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_AGG_SUM, 0, 0],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Batch 1: sum = 10 + 20 = 30 (SUM reads Float64 values)
    backend.executeBatch(state, program, [{ data: new Float64Array([10, 20]), type: ValueType.FLOAT64 }], 2);
    expect(backend.getAggregateValue(state, program, 0)).toBe(30);

    // Batch 2: sum = 30 + 30 + 40 = 100
    backend.executeBatch(state, program, [{ data: new Float64Array([30, 40]), type: ValueType.FLOAT64 }], 2);
    expect(backend.getAggregateValue(state, program, 0)).toBe(100);

    // Batch 3: sum = 100 + 50 = 150
    backend.executeBatch(state, program, [{ data: new Float64Array([50]), type: ValueType.FLOAT64 }], 1);
    expect(backend.getAggregateValue(state, program, 0)).toBe(150);
  });

  it.skipIf(!WASM_EXISTS)('multiple batches accumulate HashMap entries', async () => {
    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Batch 1: key 1 -> 100, key 2 -> 200
    backend.executeBatch(
      state,
      program,
      [
        { data: new Uint32Array([1, 2]), type: ValueType.UINT32 },
        { data: new Uint32Array([100, 200]), type: ValueType.UINT32 },
      ],
      2,
    );
    expect(backend.getMapSize(state, program, 0)).toBe(2);

    // Batch 2: key 3 -> 300, key 1 -> 150 (overwrite)
    backend.executeBatch(
      state,
      program,
      [
        { data: new Uint32Array([3, 1]), type: ValueType.UINT32 },
        { data: new Uint32Array([300, 150]), type: ValueType.UINT32 },
      ],
      2,
    );
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 1)).toBe(150); // Updated
    expect(backend.mapGet(state, program, 0, 2)).toBe(200); // Unchanged
    expect(backend.mapGet(state, program, 0, 3)).toBe(300); // New
  });

  it.skipIf(!WASM_EXISTS)('state serialization and deserialization preserves accumulated state', async () => {
    const bytecode = buildProgram({
      slots: [{ type: 'aggregate', aggType: AggType.SUM }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_AGG_SUM, 0, 0],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Accumulate some state (SUM reads Float64 values)
    backend.executeBatch(state, program, [{ data: new Float64Array([10, 20, 30]), type: ValueType.FLOAT64 }], 3);
    expect(backend.getAggregateValue(state, program, 0)).toBe(60);

    // Serialize and deserialize
    const serialized = backend.serialize(state, program);
    const restored = backend.deserialize(program, serialized);

    // Restored state has same value
    expect(backend.getAggregateValue(restored, program, 0)).toBe(60);

    // Continue accumulating on restored state
    backend.executeBatch(restored, program, [{ data: new Float64Array([40]), type: ValueType.FLOAT64 }], 1);
    expect(backend.getAggregateValue(restored, program, 0)).toBe(100);
  });
});
