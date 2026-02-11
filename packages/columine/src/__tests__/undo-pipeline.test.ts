/**
 * Pipeline undo integration tests.
 *
 * Exercises the native undo log through the columine pipeline API.
 * Verifies: HashMap rollback, HashSet rollback, Aggregate rollback,
 * checkpoint/commit lifecycle, and multiple checkpoint/rollback cycles.
 *
 * Phase 29-05: Proves undo correctness via pipeline integration.
 */

import { afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { existsSync } from 'node:fs';

import {
  AggType,
  type ColumineBackend,
  type ColumnInput,
  createPipeline,
  HEADER_SIZE,
  MAGIC,
  Opcode,
  PROGRAM_HASH_PREFIX,
  resetBackend,
  setBackend,
  ValueType,
} from '../index.js';
import { loadColumineWasm } from '../wasm-backend.js';

// =============================================================================
// WASM Binary Detection
// =============================================================================

const WASM_PATH = new URL('../../dist/columine.wasm', import.meta.url);
const WASM_EXISTS = existsSync(WASM_PATH.pathname);

// =============================================================================
// BytecodeBuilder (same helper used in columine-integration.test.ts)
// =============================================================================

function buildProgram(opts: {
  slots: Array<
    | { type: 'hashmap'; capacity: number }
    | { type: 'hashset'; capacity: number }
    | { type: 'aggregate'; aggType: AggType }
  >;
  numInputs: number;
  reduceOps: number[];
}): Uint8Array {
  const initCode: number[] = [];
  for (let i = 0; i < opts.slots.length; i++) {
    const slot = opts.slots[i];
    switch (slot.type) {
      case 'hashmap':
        initCode.push(
          Opcode.SLOT_HASHMAP,
          i,
          slot.capacity & 0xff,
          (slot.capacity >> 8) & 0xff,
          ValueType.UINT32,
          ValueType.UINT32,
        );
        break;
      case 'hashset':
        initCode.push(Opcode.SLOT_HASHSET, i, slot.capacity & 0xff, (slot.capacity >> 8) & 0xff, ValueType.UINT32);
        break;
      case 'aggregate':
        initCode.push(Opcode.SLOT_AGGREGATE, i, slot.aggType);
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
// Pipeline Undo Tests
// =============================================================================

describe('Pipeline undo integration', () => {
  let backend: ColumineBackend;

  beforeAll(async () => {
    if (!WASM_EXISTS) return;
    backend = await loadColumineWasm(WASM_PATH);
  });

  afterEach(() => {
    resetBackend();
  });

  // ---------------------------------------------------------------------------
  // 1. checkpoint + commit (no rollback)
  // ---------------------------------------------------------------------------

  it.skipIf(!WASM_EXISTS)('checkpoint + commit keeps HashMap inserts', async () => {
    setBackend(backend);
    const stages = await createPipeline();

    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Checkpoint
    const token = stages.undo.checkpoint(state);

    // Insert entries
    const keys = new Uint32Array([10, 20, 30]);
    const values = new Uint32Array([100, 200, 300]);
    const columns: ColumnInput[] = [
      { data: keys, type: ValueType.UINT32 },
      { data: values, type: ValueType.UINT32 },
    ];
    stages.reduce.executeBatch(state, program, columns, 3);

    // Commit — changes should be permanent
    stages.undo.commit(state, token);

    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 10)).toBe(100);
    expect(backend.mapGet(state, program, 0, 20)).toBe(200);
    expect(backend.mapGet(state, program, 0, 30)).toBe(300);
  });

  // ---------------------------------------------------------------------------
  // 2. checkpoint + rollback restores state
  // ---------------------------------------------------------------------------

  it.skipIf(!WASM_EXISTS)('rollback restores HashMap to pre-checkpoint state', async () => {
    setBackend(backend);
    const stages = await createPipeline();

    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Insert initial entries
    const initKeys = new Uint32Array([1, 2]);
    const initValues = new Uint32Array([10, 20]);
    stages.reduce.executeBatch(
      state,
      program,
      [
        { data: initKeys, type: ValueType.UINT32 },
        { data: initValues, type: ValueType.UINT32 },
      ],
      2,
    );

    // Snapshot initial state
    expect(backend.getMapSize(state, program, 0)).toBe(2);
    expect(backend.mapGet(state, program, 0, 1)).toBe(10);
    expect(backend.mapGet(state, program, 0, 2)).toBe(20);

    // Checkpoint before speculative batch
    const token = stages.undo.checkpoint(state);

    // Speculative batch: insert new key 3, update existing key 1
    const specKeys = new Uint32Array([3, 1]);
    const specValues = new Uint32Array([30, 999]);
    stages.reduce.executeBatch(
      state,
      program,
      [
        { data: specKeys, type: ValueType.UINT32 },
        { data: specValues, type: ValueType.UINT32 },
      ],
      2,
    );

    // Verify speculative state
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 1)).toBe(999); // updated
    expect(backend.mapGet(state, program, 0, 3)).toBe(30); // new

    // Rollback
    stages.undo.rollback(state, token);

    // State matches initial: key 3 gone, key 1 restored
    expect(backend.getMapSize(state, program, 0)).toBe(2);
    expect(backend.mapGet(state, program, 0, 1)).toBe(10);
    expect(backend.mapGet(state, program, 0, 2)).toBe(20);
    expect(backend.mapGet(state, program, 0, 3)).toBeUndefined();
  });

  // ---------------------------------------------------------------------------
  // 3. rollback restores Aggregate values
  // ---------------------------------------------------------------------------

  it.skipIf(!WASM_EXISTS)('rollback restores Aggregate SUM to pre-checkpoint value', async () => {
    setBackend(backend);
    const stages = await createPipeline();

    const bytecode = buildProgram({
      slots: [{ type: 'aggregate', aggType: AggType.SUM }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_AGG_SUM, 0, 0],
    });

    const program = backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // First batch: sum = 10 + 20 + 30 = 60
    const batch1 = new Float64Array([10, 20, 30]);
    stages.reduce.executeBatch(state, program, [{ data: batch1, type: ValueType.FLOAT64 }], 3);
    expect(backend.getAggregateValue(state, program, 0)).toBe(60);

    // Checkpoint
    const token = stages.undo.checkpoint(state);

    // Speculative batch: sum should be 60 + 100 + 200 = 360
    const batch2 = new Float64Array([100, 200]);
    stages.reduce.executeBatch(state, program, [{ data: batch2, type: ValueType.FLOAT64 }], 2);
    expect(backend.getAggregateValue(state, program, 0)).toBe(360);

    // Rollback: should restore to 60
    stages.undo.rollback(state, token);
    expect(backend.getAggregateValue(state, program, 0)).toBe(60);
  });

  // ---------------------------------------------------------------------------
  // 4. rollback restores HashSet
  // ---------------------------------------------------------------------------

  it.skipIf(!WASM_EXISTS)('rollback restores HashSet to pre-checkpoint elements', async () => {
    setBackend(backend);
    const stages = await createPipeline();

    const bytecode = buildProgram({
      slots: [{ type: 'hashset', capacity: 64 }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_SET_INSERT, 0, 0],
    });

    const program = backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Insert initial elements
    const elems1 = new Uint32Array([100, 200, 300]);
    stages.reduce.executeBatch(state, program, [{ data: elems1, type: ValueType.UINT32 }], 3);

    expect(backend.getSetSize(state, program, 0)).toBe(3);
    expect(backend.setContains(state, program, 0, 100)).toBe(true);
    expect(backend.setContains(state, program, 0, 200)).toBe(true);
    expect(backend.setContains(state, program, 0, 300)).toBe(true);

    // Checkpoint
    const token = stages.undo.checkpoint(state);

    // Speculative batch: insert more elements
    const elems2 = new Uint32Array([400, 500]);
    stages.reduce.executeBatch(state, program, [{ data: elems2, type: ValueType.UINT32 }], 2);

    expect(backend.getSetSize(state, program, 0)).toBe(5);
    expect(backend.setContains(state, program, 0, 400)).toBe(true);
    expect(backend.setContains(state, program, 0, 500)).toBe(true);

    // Rollback
    stages.undo.rollback(state, token);

    // Only original elements remain
    expect(backend.getSetSize(state, program, 0)).toBe(3);
    expect(backend.setContains(state, program, 0, 100)).toBe(true);
    expect(backend.setContains(state, program, 0, 200)).toBe(true);
    expect(backend.setContains(state, program, 0, 300)).toBe(true);
    expect(backend.setContains(state, program, 0, 400)).toBe(false);
    expect(backend.setContains(state, program, 0, 500)).toBe(false);
  });

  // ---------------------------------------------------------------------------
  // 5. multiple checkpoint/rollback cycles
  // ---------------------------------------------------------------------------

  it.skipIf(!WASM_EXISTS)('commit then rollback: committed changes persist, rolled-back do not', async () => {
    setBackend(backend);
    const stages = await createPipeline();

    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // === Cycle 1: checkpoint, execute, COMMIT ===
    const token1 = stages.undo.checkpoint(state);

    const keys1 = new Uint32Array([1, 2]);
    const vals1 = new Uint32Array([10, 20]);
    stages.reduce.executeBatch(
      state,
      program,
      [
        { data: keys1, type: ValueType.UINT32 },
        { data: vals1, type: ValueType.UINT32 },
      ],
      2,
    );

    stages.undo.commit(state, token1);

    // Committed: key 1=10, key 2=20
    expect(backend.getMapSize(state, program, 0)).toBe(2);
    expect(backend.mapGet(state, program, 0, 1)).toBe(10);
    expect(backend.mapGet(state, program, 0, 2)).toBe(20);

    // === Cycle 2: checkpoint, execute, ROLLBACK ===
    const token2 = stages.undo.checkpoint(state);

    const keys2 = new Uint32Array([3, 1]);
    const vals2 = new Uint32Array([30, 99]);
    stages.reduce.executeBatch(
      state,
      program,
      [
        { data: keys2, type: ValueType.UINT32 },
        { data: vals2, type: ValueType.UINT32 },
      ],
      2,
    );

    // Before rollback: 3 keys, key 1 updated to 99
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 1)).toBe(99);

    stages.undo.rollback(state, token2);

    // After rollback: only committed changes remain
    expect(backend.getMapSize(state, program, 0)).toBe(2);
    expect(backend.mapGet(state, program, 0, 1)).toBe(10);
    expect(backend.mapGet(state, program, 0, 2)).toBe(20);
    expect(backend.mapGet(state, program, 0, 3)).toBeUndefined();
  });

  // ---------------------------------------------------------------------------
  // 6. commit after partial execution (multiple batches)
  // ---------------------------------------------------------------------------

  it.skipIf(!WASM_EXISTS)('commit after multiple batches preserves all effects', async () => {
    setBackend(backend);
    const stages = await createPipeline();

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

    const program = backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Checkpoint
    const token = stages.undo.checkpoint(state);

    // Batch 1
    stages.reduce.executeBatch(
      state,
      program,
      [
        { data: new Uint32Array([1, 2]), type: ValueType.UINT32 },
        { data: new Uint32Array([100, 200]), type: ValueType.UINT32 },
        { data: new Float64Array([10, 20]), type: ValueType.FLOAT64 },
      ],
      2,
    );

    // Batch 2
    stages.reduce.executeBatch(
      state,
      program,
      [
        { data: new Uint32Array([3, 1]), type: ValueType.UINT32 },
        { data: new Uint32Array([300, 150]), type: ValueType.UINT32 },
        { data: new Float64Array([30, 40]), type: ValueType.FLOAT64 },
      ],
      2,
    );

    // Commit — both batches' effects should be visible
    stages.undo.commit(state, token);

    // HashMap: 3 unique keys, key 1 overwritten to 150
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 1)).toBe(150);
    expect(backend.mapGet(state, program, 0, 2)).toBe(200);
    expect(backend.mapGet(state, program, 0, 3)).toBe(300);

    // Aggregate: sum = 10 + 20 + 30 + 40 = 100
    expect(backend.getAggregateValue(state, program, 1)).toBe(100);
  });
});
