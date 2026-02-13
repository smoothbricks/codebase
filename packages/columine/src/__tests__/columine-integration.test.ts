/**
 * Integration tests for @smoothbricks/columine standalone usage.
 *
 * Verifies all Phase 28 success criteria:
 *   SC1: Reduce stage for aggregation (HashMap + Aggregate slots)
 *   SC2: Undo stage for event cancellation (checkpoint/rollback/commit)
 *   SC3: Pipeline composition (all four stages, independently usable)
 *   SC4: DI pattern (setBackend/getBackend/resetBackend/hasBackend)
 *   SC5: Streaming processing (multiple batches accumulate state)
 *
 * SC3 and SC4 are TypeScript-only tests (no WASM needed).
 * SC1, SC2, SC5 require the columine WASM binary and skip gracefully if unavailable.
 */

import { afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { existsSync } from 'node:fs';

import {
  AggType,
  type ColumineBackend,
  type ColumineStages,
  type ColumnInput,
  createPipeline,
  ErrorCode,
  getBackend,
  HEADER_SIZE,
  hasBackend,
  MAGIC,
  Opcode,
  PROGRAM_HASH_PREFIX,
  type ReducerProgram,
  resetBackend,
  SlotType,
  type StateHandle,
  setBackend,
  setBackendLoader,
  ValueType,
} from '../index.js';
import { loadColumineWasm } from '../wasm-backend.js';

// =============================================================================
// WASM Binary Detection
// =============================================================================

const WASM_PATH = new URL('../../dist/columine.wasm', import.meta.url);
const WASM_EXISTS = existsSync(WASM_PATH.pathname);

// Skip message shown when WASM is unavailable
const WASM_SKIP_MSG = 'columine.wasm not found. Build with: cd packages/columine && zig build';

// =============================================================================
// =============================================================================

/**
 * Minimal bytecode builder for test programs.
 */
function buildProgram(opts: {
  slots: Array<
    | { type: 'hashmap'; capacity: number }
    | { type: 'hashset'; capacity: number }
    | { type: 'aggregate'; aggType: AggType }
    | { type: 'condition-tree' }
  >;
  numInputs: number;
  reduceOps: number[];
}): Uint8Array {
  const initCode: number[] = [];
  for (let i = 0; i < opts.slots.length; i++) {
    const slot = opts.slots[i];
    switch (slot.type) {
      case 'hashmap':
        // SLOT_DEF: slot, type_flags(HASHMAP), cap_lo, cap_hi
        initCode.push(Opcode.SLOT_DEF, i, SlotType.HASHMAP, slot.capacity & 0xff, (slot.capacity >> 8) & 0xff);
        break;
      case 'hashset':
        initCode.push(Opcode.SLOT_DEF, i, SlotType.HASHSET, slot.capacity & 0xff, (slot.capacity >> 8) & 0xff);
        break;
      case 'aggregate':
        // SLOT_DEF: slot, type_flags(AGGREGATE), aggType in cap_lo, 0
        initCode.push(Opcode.SLOT_DEF, i, SlotType.AGGREGATE, slot.aggType, 0);
        break;
      case 'condition-tree':
        initCode.push(Opcode.SLOT_DEF, i, SlotType.CONDITION_TREE, 0, 0);
        break;
    }
  }

  const reduceCode = [...opts.reduceOps, Opcode.HALT];
  const contentLen = HEADER_SIZE + initCode.length + reduceCode.length;
  const program = new Uint8Array(PROGRAM_HASH_PREFIX + contentLen);
  // [0..31] hash prefix (zeros - filled by fillProgramHash at async use)
  // [32..] content header + init + reduce

  const base = PROGRAM_HASH_PREFIX;
  // Magic "CLM1" (little-endian)
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

  afterEach(() => {
    resetBackend();
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
    const TYPE_FLAGS_OFFSET = 12;
    const typeFlags = serialized[STATE_HEADER_SIZE + TYPE_FLAGS_OFFSET];
    expect(typeFlags & 0x0f).toBe(SlotType.CONDITION_TREE);

    // Verify the state can execute and round-trip with CONDITION_TREE metadata intact.
    const result = backend.executeBatch(state, program, [{ data: new Float64Array([1]), type: ValueType.FLOAT64 }], 1);
    expect(result).toBe(ErrorCode.OK);

    const restored = backend.deserialize(program, serialized);
    const restoredBytes = backend.serialize(restored, program);
    const restoredTypeFlags = restoredBytes[STATE_HEADER_SIZE + TYPE_FLAGS_OFFSET];
    expect(restoredTypeFlags & 0x0f).toBe(SlotType.CONDITION_TREE);
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

  afterEach(() => {
    resetBackend();
  });

  it.skipIf(!WASM_EXISTS)('checkpoint and rollback restores pre-batch state', async () => {
    // Must set backend before each test since afterEach resets it
    setBackend(backend);
    const stages = await createPipeline();

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
    setBackend(backend);
    const stages = await createPipeline();

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
  afterEach(() => {
    resetBackend();
  });

  it('createPipeline returns all four stages', async () => {
    // Inject a mock backend so createPipeline resolves
    const mockBackend: ColumineBackend = {
      backend: 'mock',
      loadProgram: () => ({ bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] }),
      createState: () => ({ _brand: 'ColumineStateHandle' }),
      resetState: () => {},
      executeBatch: () => 0,
      getMapSize: () => 0,
      getSetSize: () => 0,
      getAggregateValue: () => 0,
      mapGet: () => undefined,
      setContains: () => false,
      serialize: () => new Uint8Array(0),
      deserialize: () => ({ _brand: 'ColumineStateHandle' }),
    };
    setBackend(mockBackend);

    const stages = await createPipeline();

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

  it('reduce stage delegates to backend.executeBatch', async () => {
    let executeBatchCalled = false;
    let executeBatchArgs: unknown[] = [];

    const mockBackend: ColumineBackend = {
      backend: 'mock',
      loadProgram: () => ({ bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] }),
      createState: () => ({ _brand: 'ColumineStateHandle' }),
      resetState: () => {},
      executeBatch: (...args: unknown[]) => {
        executeBatchCalled = true;
        executeBatchArgs = args;
        return ErrorCode.OK;
      },
      getMapSize: () => 0,
      getSetSize: () => 0,
      getAggregateValue: () => 0,
      mapGet: () => undefined,
      setContains: () => false,
      serialize: () => new Uint8Array(0),
      deserialize: () => ({ _brand: 'ColumineStateHandle' }),
    };
    setBackend(mockBackend);

    const stages = await createPipeline();

    const mockState = { _brand: 'ColumineStateHandle' as const };
    const mockProgram: ReducerProgram = { bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] };
    const mockColumns: ColumnInput[] = [];

    const result = stages.reduce.executeBatch(mockState, mockProgram, mockColumns, 10);

    expect(executeBatchCalled).toBe(true);
    expect(result).toBe(ErrorCode.OK);
  });

  it('parse stage throws helpful error without parse backend', async () => {
    const mockBackend: ColumineBackend = {
      backend: 'mock',
      loadProgram: () => ({ bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] }),
      createState: () => ({ _brand: 'ColumineStateHandle' }),
      resetState: () => {},
      executeBatch: () => 0,
      getMapSize: () => 0,
      getSetSize: () => 0,
      getAggregateValue: () => 0,
      mapGet: () => undefined,
      setContains: () => false,
      serialize: () => new Uint8Array(0),
      deserialize: () => ({ _brand: 'ColumineStateHandle' }),
    };
    setBackend(mockBackend);

    // No parse backend provided
    const stages = await createPipeline();

    expect(() => {
      stages.parse.parse('[]', {
        schemaBytes: new Uint8Array(0),
        fieldMetadata: new Uint8Array(0),
      });
    }).toThrow(/parse backend/i);
  });

  it('stages are independently usable (reduce works without parse)', async () => {
    let reduceCalled = false;

    const mockBackend: ColumineBackend = {
      backend: 'mock',
      loadProgram: () => ({ bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] }),
      createState: () => ({ _brand: 'ColumineStateHandle' }),
      resetState: () => {},
      executeBatch: () => {
        reduceCalled = true;
        return ErrorCode.OK;
      },
      getMapSize: () => 0,
      getSetSize: () => 0,
      getAggregateValue: () => 0,
      mapGet: () => undefined,
      setContains: () => false,
      serialize: () => new Uint8Array(0),
      deserialize: () => ({ _brand: 'ColumineStateHandle' }),
    };
    setBackend(mockBackend);

    const stages = await createPipeline();

    // Reduce works without parse backend
    const mockState = { _brand: 'ColumineStateHandle' as const };
    const mockProgram: ReducerProgram = { bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] };
    stages.reduce.executeBatch(mockState, mockProgram, [], 0);
    expect(reduceCalled).toBe(true);

    // Parse throws because no parse backend provided
    expect(() => {
      stages.parse.parse('[]', { schemaBytes: new Uint8Array(0), fieldMetadata: new Uint8Array(0) });
    }).toThrow();
  });
});

// =============================================================================
// SC4: DI pattern (setBackend/getBackend/resetBackend/hasBackend)
// =============================================================================

describe('SC4: DI pattern', () => {
  afterEach(() => {
    resetBackend();
  });

  it('hasBackend returns false initially after reset', () => {
    resetBackend();
    expect(hasBackend()).toBe(false);
  });

  it('setBackend makes hasBackend return true', () => {
    resetBackend();
    expect(hasBackend()).toBe(false);

    const mockBackend: ColumineBackend = {
      backend: 'mock',
      loadProgram: () => ({ bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] }),
      createState: () => ({ _brand: 'ColumineStateHandle' }),
      resetState: () => {},
      executeBatch: () => 0,
      getMapSize: () => 0,
      getSetSize: () => 0,
      getAggregateValue: () => 0,
      mapGet: () => undefined,
      setContains: () => false,
      serialize: () => new Uint8Array(0),
      deserialize: () => ({ _brand: 'ColumineStateHandle' }),
    };

    setBackend(mockBackend);
    expect(hasBackend()).toBe(true);
  });

  it('getBackend returns the injected backend', async () => {
    resetBackend();

    const mockBackend: ColumineBackend = {
      backend: 'test-mock',
      loadProgram: () => ({ bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] }),
      createState: () => ({ _brand: 'ColumineStateHandle' }),
      resetState: () => {},
      executeBatch: () => 0,
      getMapSize: () => 0,
      getSetSize: () => 0,
      getAggregateValue: () => 0,
      mapGet: () => undefined,
      setContains: () => false,
      serialize: () => new Uint8Array(0),
      deserialize: () => ({ _brand: 'ColumineStateHandle' }),
    };

    setBackend(mockBackend);

    const retrieved = await getBackend();
    expect(retrieved.backend).toBe('test-mock');
  });

  it('resetBackend clears the backend', () => {
    const mockBackend: ColumineBackend = {
      backend: 'mock',
      loadProgram: () => ({ bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] }),
      createState: () => ({ _brand: 'ColumineStateHandle' }),
      resetState: () => {},
      executeBatch: () => 0,
      getMapSize: () => 0,
      getSetSize: () => 0,
      getAggregateValue: () => 0,
      mapGet: () => undefined,
      setContains: () => false,
      serialize: () => new Uint8Array(0),
      deserialize: () => ({ _brand: 'ColumineStateHandle' }),
    };

    setBackend(mockBackend);
    expect(hasBackend()).toBe(true);

    resetBackend();
    expect(hasBackend()).toBe(false);
  });

  it('getBackend throws when no backend and no loader set', async () => {
    resetBackend();

    expect(getBackend()).rejects.toThrow(/no columine backend/i);
  });

  it('setBackendLoader enables lazy loading', async () => {
    resetBackend();

    let loaderCalled = false;
    const mockBackend: ColumineBackend = {
      backend: 'lazy-mock',
      loadProgram: () => ({ bytecode: new Uint8Array(0), numSlots: 0, numInputs: 0, slotDefs: [] }),
      createState: () => ({ _brand: 'ColumineStateHandle' }),
      resetState: () => {},
      executeBatch: () => 0,
      getMapSize: () => 0,
      getSetSize: () => 0,
      getAggregateValue: () => 0,
      mapGet: () => undefined,
      setContains: () => false,
      serialize: () => new Uint8Array(0),
      deserialize: () => ({ _brand: 'ColumineStateHandle' }),
    };

    setBackendLoader(async () => {
      loaderCalled = true;
      return mockBackend;
    });

    expect(hasBackend()).toBe(false); // Not loaded yet

    const retrieved = await getBackend();
    expect(loaderCalled).toBe(true);
    expect(retrieved.backend).toBe('lazy-mock');
    expect(hasBackend()).toBe(true); // Now cached
  });
});

// =============================================================================
// SC5: Streaming processing (multiple batches accumulate state)
// =============================================================================

describe('SC5: Streaming processing', () => {
  let backend: ColumineBackend;

  beforeAll(async () => {
    if (!WASM_EXISTS) return;
    backend = await loadColumineWasm(WASM_PATH);
  });

  afterEach(() => {
    resetBackend();
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
