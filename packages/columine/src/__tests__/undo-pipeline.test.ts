/**
 * Pipeline undo integration tests.
 *
 * Exercises the native undo log through the columine pipeline API.
 * Verifies: HashMap rollback, HashSet rollback, Aggregate rollback,
 * checkpoint/commit lifecycle, and multiple checkpoint/rollback cycles.
 *
 * Phase 29-05: Proves undo correctness via pipeline integration.
 */

import { beforeEach, describe, expect, it } from 'bun:test';

import {
  AggType,
  type ColumineBackend,
  type ColumnInput,
  createPipeline,
  ErrorCode,
  encodeProgramHeader,
  HEADER_SIZE,
  Opcode,
  type ParseCompactBackend,
  PROGRAM_HASH_PREFIX,
  PROGRAM_MAGIC,
  SlotType,
  UNDO_POSITION_OVERFLOWED,
  ValueType,
} from '../index.js';
import { loadColumineWasm } from '../wasm-backend.js';

// =============================================================================
// WASM Binary
// =============================================================================

// A prerequisite, not a condition — see columine-integration.test.ts.
const WASM_PATH = new URL('../../dist/columine.wasm', import.meta.url);
const TEST_PARSE_BACKEND: ParseCompactBackend = {
  backend: 'test-parse',
  parse: () => ({ arrowIpc: new Uint8Array(0), eventCount: 0 }),
  encode: () => new Uint8Array(0),
  dispose: () => {},
};

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
        initCode.push(Opcode.SLOT_DEF, i, SlotType.HASHMAP, slot.capacity & 0xff, (slot.capacity >> 8) & 0xff);
        break;
      case 'hashset':
        initCode.push(Opcode.SLOT_DEF, i, SlotType.HASHSET, slot.capacity & 0xff, (slot.capacity >> 8) & 0xff);
        break;
      case 'aggregate':
        initCode.push(Opcode.SLOT_DEF, i, SlotType.AGGREGATE, slot.aggType, 0);
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
  program.set(
    encodeProgramHeader({
      magic: PROGRAM_MAGIC,
      numSlots: opts.slots.length,
      numInputs: opts.numInputs,
      initCodeLength: initCode.length,
      reduceCodeLength: reduceCode.length,
    }),
    base,
  );

  // Init section
  program.set(initCode, base + HEADER_SIZE);

  // Reduce section
  program.set(reduceCode, base + HEADER_SIZE + initCode.length);

  return program;
}

function getUndoPosition(token: unknown): number {
  if (typeof token !== 'object' || token === null || !('position' in token) || typeof token.position !== 'number') {
    throw new Error('invariant: undo token missing numeric position');
  }
  return token.position;
}

// =============================================================================
// Pipeline Undo Tests
// =============================================================================

describe('Pipeline undo integration', () => {
  let backend: ColumineBackend;

  // A fresh backend per test: the undo window flag lives on the backend instance and a
  // rollback leaves it open, so a shared backend carries one test's window into the next.
  beforeEach(async () => {
    backend = await loadColumineWasm(WASM_PATH);
  });

  // ---------------------------------------------------------------------------
  // 1. checkpoint + commit (no rollback)
  // ---------------------------------------------------------------------------

  it('checkpoint + commit keeps HashMap inserts', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Checkpoint
    const begun = stages.undo.checkpoint(state);
    if (!begun.ok) throw new Error(`unexpected undo checkpoint error ${begun.error}`);
    const token = begun.value;

    // Insert entries
    const keys = new Uint32Array([10, 20, 30]);
    const values = new Uint32Array([100, 200, 300]);
    const columns: ColumnInput[] = [
      { data: keys, type: ValueType.UINT32 },
      { data: values, type: ValueType.UINT32 },
    ];
    expect(stages.reduce.executeBatch(state, program, columns, 3)).toBe(ErrorCode.OK);

    // Commit — changes should be permanent
    stages.undo.commit(state, token);

    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 10)).toStrictEqual({ ok: true, value: 100 });
    expect(backend.mapGet(state, program, 0, 20)).toStrictEqual({ ok: true, value: 200 });
    expect(backend.mapGet(state, program, 0, 30)).toStrictEqual({ ok: true, value: 300 });
  });

  // ---------------------------------------------------------------------------
  // 2. checkpoint + rollback restores state
  // ---------------------------------------------------------------------------

  it('rollback restores HashMap to pre-checkpoint state', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Insert initial entries
    const initKeys = new Uint32Array([1, 2]);
    const initValues = new Uint32Array([10, 20]);
    expect(
      stages.reduce.executeBatch(
        state,
        program,
        [
          { data: initKeys, type: ValueType.UINT32 },
          { data: initValues, type: ValueType.UINT32 },
        ],
        2,
      ),
    ).toBe(ErrorCode.OK);

    // Snapshot initial state
    expect(backend.getMapSize(state, program, 0)).toBe(2);
    expect(backend.mapGet(state, program, 0, 1)).toStrictEqual({ ok: true, value: 10 });
    expect(backend.mapGet(state, program, 0, 2)).toStrictEqual({ ok: true, value: 20 });

    // Checkpoint before speculative batch
    const begun = stages.undo.checkpoint(state);
    if (!begun.ok) throw new Error(`unexpected undo checkpoint error ${begun.error}`);
    const token = begun.value;

    // Speculative batch: insert new key 3, update existing key 1
    const specKeys = new Uint32Array([3, 1]);
    const specValues = new Uint32Array([30, 999]);
    expect(
      stages.reduce.executeBatch(
        state,
        program,
        [
          { data: specKeys, type: ValueType.UINT32 },
          { data: specValues, type: ValueType.UINT32 },
        ],
        2,
      ),
    ).toBe(ErrorCode.OK);

    // Verify speculative state
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 1)).toStrictEqual({ ok: true, value: 999 }); // updated
    expect(backend.mapGet(state, program, 0, 3)).toStrictEqual({ ok: true, value: 30 }); // new

    // Rollback
    stages.undo.rollback(state, token);

    // State matches initial: key 3 gone, key 1 restored
    expect(backend.getMapSize(state, program, 0)).toBe(2);
    expect(backend.mapGet(state, program, 0, 1)).toStrictEqual({ ok: true, value: 10 });
    expect(backend.mapGet(state, program, 0, 2)).toStrictEqual({ ok: true, value: 20 });
    expect(backend.mapGet(state, program, 0, 3)).toStrictEqual({ ok: true, value: undefined });
  });

  // ---------------------------------------------------------------------------
  // 3. rollback restores Aggregate values
  // ---------------------------------------------------------------------------

  it('rollback restores Aggregate SUM to pre-checkpoint value', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    const bytecode = buildProgram({
      slots: [{ type: 'aggregate', aggType: AggType.SUM }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_AGG_SUM, 0, 0],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // First batch: sum = 10 + 20 + 30 = 60
    const batch1 = new Float64Array([10, 20, 30]);
    expect(stages.reduce.executeBatch(state, program, [{ data: batch1, type: ValueType.FLOAT64 }], 3)).toBe(
      ErrorCode.OK,
    );
    expect(backend.getAggregateValue(state, program, 0)).toBe(60);

    // Checkpoint
    const begun = stages.undo.checkpoint(state);
    if (!begun.ok) throw new Error(`unexpected undo checkpoint error ${begun.error}`);
    const token = begun.value;

    // Speculative batch: sum should be 60 + 100 + 200 = 360
    const batch2 = new Float64Array([100, 200]);
    expect(stages.reduce.executeBatch(state, program, [{ data: batch2, type: ValueType.FLOAT64 }], 2)).toBe(
      ErrorCode.OK,
    );
    expect(backend.getAggregateValue(state, program, 0)).toBe(360);

    // Rollback: should restore to 60
    stages.undo.rollback(state, token);
    expect(backend.getAggregateValue(state, program, 0)).toBe(60);
  });

  // ---------------------------------------------------------------------------
  // 4. rollback restores HashSet
  // ---------------------------------------------------------------------------

  it('rollback restores HashSet to pre-checkpoint elements', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    const bytecode = buildProgram({
      slots: [{ type: 'hashset', capacity: 64 }],
      numInputs: 1,
      reduceOps: [Opcode.BATCH_SET_INSERT, 0, 0],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Insert initial elements
    const elems1 = new Uint32Array([100, 200, 300]);
    expect(stages.reduce.executeBatch(state, program, [{ data: elems1, type: ValueType.UINT32 }], 3)).toBe(
      ErrorCode.OK,
    );

    expect(backend.getSetSize(state, program, 0)).toBe(3);
    expect(backend.setContains(state, program, 0, 100)).toStrictEqual({ ok: true, value: true });
    expect(backend.setContains(state, program, 0, 200)).toStrictEqual({ ok: true, value: true });
    expect(backend.setContains(state, program, 0, 300)).toStrictEqual({ ok: true, value: true });

    // Checkpoint
    const begun = stages.undo.checkpoint(state);
    if (!begun.ok) throw new Error(`unexpected undo checkpoint error ${begun.error}`);
    const token = begun.value;

    // Speculative batch: insert more elements
    const elems2 = new Uint32Array([400, 500]);
    expect(stages.reduce.executeBatch(state, program, [{ data: elems2, type: ValueType.UINT32 }], 2)).toBe(
      ErrorCode.OK,
    );

    expect(backend.getSetSize(state, program, 0)).toBe(5);
    expect(backend.setContains(state, program, 0, 400)).toStrictEqual({ ok: true, value: true });
    expect(backend.setContains(state, program, 0, 500)).toStrictEqual({ ok: true, value: true });

    // Rollback
    stages.undo.rollback(state, token);

    // Only original elements remain
    expect(backend.getSetSize(state, program, 0)).toBe(3);
    expect(backend.setContains(state, program, 0, 100)).toStrictEqual({ ok: true, value: true });
    expect(backend.setContains(state, program, 0, 200)).toStrictEqual({ ok: true, value: true });
    expect(backend.setContains(state, program, 0, 300)).toStrictEqual({ ok: true, value: true });
    expect(backend.setContains(state, program, 0, 400)).toStrictEqual({ ok: true, value: false });
    expect(backend.setContains(state, program, 0, 500)).toStrictEqual({ ok: true, value: false });
  });

  // ---------------------------------------------------------------------------
  // 5. multiple checkpoint/rollback cycles
  // ---------------------------------------------------------------------------

  it('commit then rollback: committed changes persist, rolled-back do not', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // === Cycle 1: checkpoint, execute, COMMIT ===
    const begun1 = stages.undo.checkpoint(state);
    if (!begun1.ok) throw new Error(`unexpected undo checkpoint error ${begun1.error}`);
    const token1 = begun1.value;

    const keys1 = new Uint32Array([1, 2]);
    const vals1 = new Uint32Array([10, 20]);
    expect(
      stages.reduce.executeBatch(
        state,
        program,
        [
          { data: keys1, type: ValueType.UINT32 },
          { data: vals1, type: ValueType.UINT32 },
        ],
        2,
      ),
    ).toBe(ErrorCode.OK);

    stages.undo.commit(state, token1);

    // Committed: key 1=10, key 2=20
    expect(backend.getMapSize(state, program, 0)).toBe(2);
    expect(backend.mapGet(state, program, 0, 1)).toStrictEqual({ ok: true, value: 10 });
    expect(backend.mapGet(state, program, 0, 2)).toStrictEqual({ ok: true, value: 20 });

    // === Cycle 2: checkpoint, execute, ROLLBACK ===
    const begun2 = stages.undo.checkpoint(state);
    if (!begun2.ok) throw new Error(`unexpected undo checkpoint error ${begun2.error}`);
    const token2 = begun2.value;

    const keys2 = new Uint32Array([3, 1]);
    const vals2 = new Uint32Array([30, 99]);
    expect(
      stages.reduce.executeBatch(
        state,
        program,
        [
          { data: keys2, type: ValueType.UINT32 },
          { data: vals2, type: ValueType.UINT32 },
        ],
        2,
      ),
    ).toBe(ErrorCode.OK);

    // Before rollback: 3 keys, key 1 updated to 99
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 1)).toStrictEqual({ ok: true, value: 99 });

    stages.undo.rollback(state, token2);

    // After rollback: only committed changes remain
    expect(backend.getMapSize(state, program, 0)).toBe(2);
    expect(backend.mapGet(state, program, 0, 1)).toStrictEqual({ ok: true, value: 10 });
    expect(backend.mapGet(state, program, 0, 2)).toStrictEqual({ ok: true, value: 20 });
    expect(backend.mapGet(state, program, 0, 3)).toStrictEqual({ ok: true, value: undefined });
  });

  // ---------------------------------------------------------------------------
  // 6. commit after partial execution (multiple batches)
  // ---------------------------------------------------------------------------

  it('commit after multiple batches preserves all effects', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

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

    // Checkpoint
    const begun = stages.undo.checkpoint(state);
    if (!begun.ok) throw new Error(`unexpected undo checkpoint error ${begun.error}`);
    const token = begun.value;

    // Batch 1
    expect(
      stages.reduce.executeBatch(
        state,
        program,
        [
          { data: new Uint32Array([1, 2]), type: ValueType.UINT32 },
          { data: new Uint32Array([100, 200]), type: ValueType.UINT32 },
          { data: new Float64Array([10, 20]), type: ValueType.FLOAT64 },
        ],
        2,
      ),
    ).toBe(ErrorCode.OK);

    // Batch 2
    expect(
      stages.reduce.executeBatch(
        state,
        program,
        [
          { data: new Uint32Array([3, 1]), type: ValueType.UINT32 },
          { data: new Uint32Array([300, 150]), type: ValueType.UINT32 },
          { data: new Float64Array([30, 40]), type: ValueType.FLOAT64 },
        ],
        2,
      ),
    ).toBe(ErrorCode.OK);

    // Commit — both batches' effects should be visible
    stages.undo.commit(state, token);

    // HashMap: 3 unique keys, key 1 overwritten to 150
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 1)).toStrictEqual({ ok: true, value: 150 });
    expect(backend.mapGet(state, program, 0, 2)).toStrictEqual({ ok: true, value: 200 });
    expect(backend.mapGet(state, program, 0, 3)).toStrictEqual({ ok: true, value: 300 });

    // Aggregate: sum = 10 + 20 + 30 + 40 = 100
    expect(backend.getAggregateValue(state, program, 1)).toBe(100);
  });

  // ---------------------------------------------------------------------------
  // 7. undo overflow: shadow lands writes, sticky window, exact rollback, fitting retry
  // ---------------------------------------------------------------------------

  it('overflow lands the batch in a shadow, keeps the window rollbackable, and a fitting retry commits', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    // Bytecode cap 16384 -> actual cap 32768 -> ~524KB state.
    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 16384 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    // Initial entries land before any window opens, so they are not journaled.
    const initKeys = new Uint32Array([1, 2, 3]);
    const initValues = new Uint32Array([10, 20, 30]);
    expect(
      stages.reduce.executeBatch(
        state,
        program,
        [
          { data: initKeys, type: ValueType.UINT32 },
          { data: initValues, type: ValueType.UINT32 },
        ],
        3,
      ),
    ).toBe(ErrorCode.OK);
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.mapGet(state, program, 0, 1)).toStrictEqual({ ok: true, value: 10 });

    const begun = stages.undo.checkpoint(state);
    if (!begun.ok) throw new Error(`unexpected undo checkpoint error ${begun.error}`);
    const token = begun.value;
    const checkpointImage = backend.serialize(state, program);

    // Every new key journals at least one entry, so 16385 rows exceed UNDO_CAPACITY (16384) whatever the
    // per-row count. The journal cannot record the window, so the batch overflows it into the shadow: the
    // batch returns OK, its writes land, and the window stays overflowed until a rollback or commit.
    const OVERFLOW_COUNT = 16385;
    const keys = new Uint32Array(OVERFLOW_COUNT);
    const values = new Uint32Array(OVERFLOW_COUNT);
    for (let j = 0; j < OVERFLOW_COUNT; j++) {
      keys[j] = 100 + j;
      values[j] = 1000 + j;
    }
    expect(
      stages.reduce.executeBatch(
        state,
        program,
        [
          { data: keys, type: ValueType.UINT32 },
          { data: values, type: ValueType.UINT32 },
        ],
        OVERFLOW_COUNT,
      ),
    ).toBe(ErrorCode.OK);

    // The window is overflowed: its checkpoint is the u32::MAX sentinel, and every row landed in the shadow.
    expect(backend.undoHasOverflow()).toBe(true);
    expect(backend.undoCheckpoint(state)).toBe(UNDO_POSITION_OVERFLOWED);
    expect(backend.getMapSize(state, program, 0)).toBe(3 + OVERFLOW_COUNT);
    expect(backend.mapGet(state, program, 0, 1)).toStrictEqual({ ok: true, value: 10 });
    expect(backend.mapGet(state, program, 0, 2)).toStrictEqual({ ok: true, value: 20 });
    expect(backend.mapGet(state, program, 0, 3)).toStrictEqual({ ok: true, value: 30 });
    expect(backend.mapGet(state, program, 0, 100)).toStrictEqual({ ok: true, value: 1000 });
    expect(backend.mapGet(state, program, 0, 100 + OVERFLOW_COUNT - 1)).toStrictEqual({
      ok: true,
      value: 1000 + OVERFLOW_COUNT - 1,
    });

    // Rolling back to the pre-overflow checkpoint goes through the shadow, restores the exact image and ends the overflow.
    expect(stages.undo.rollback(state, token)).toBe('overflow');
    expect(backend.serialize(state, program)).toEqual(checkpointImage);
    expect(backend.getMapSize(state, program, 0)).toBe(3);
    expect(backend.undoHasOverflow()).toBe(false);
    expect(backend.undoCheckpoint(state)).toBe(getUndoPosition(token));

    // Ending the window closes it.
    stages.undo.commit(state, token);

    // Retry the same rows in windows that fit. Each chunk's OK status is asserted, so a chunk that
    // overflows fails here instead of being assumed to fit from a per-row entry count.
    const CHUNK = 1024;
    for (let start = 0; start < OVERFLOW_COUNT; start += CHUNK) {
      const end = Math.min(start + CHUNK, OVERFLOW_COUNT);
      const chunk = stages.undo.checkpoint(state);
      if (!chunk.ok) throw new Error(`unexpected undo checkpoint error ${chunk.error}`);
      const windowStart = backend.undoCheckpoint(state);
      const extent = backend.serialize(state, program).byteLength;
      expect(
        stages.reduce.executeBatch(
          state,
          program,
          [
            { data: keys.slice(start, end), type: ValueType.UINT32 },
            { data: values.slice(start, end), type: ValueType.UINT32 },
          ],
          end - start,
        ),
      ).toBe(ErrorCode.OK);
      expect(backend.undoHasOverflow()).toBe(false);
      const windowEnd = backend.undoCheckpoint(state);
      expect(windowEnd).toBeGreaterThan(windowStart);
      expect(windowEnd).toBeLessThanOrEqual(16384);
      expect(backend.serialize(state, program).byteLength).toBe(extent);
      stages.undo.commit(state, chunk.value);
      expect(backend.undoCheckpoint(state)).toBe(0);
    }
    expect(backend.getMapSize(state, program, 0)).toBe(3 + OVERFLOW_COUNT);
    expect(backend.mapGet(state, program, 0, 1)).toStrictEqual({ ok: true, value: 10 });
    expect(backend.mapGet(state, program, 0, 100)).toStrictEqual({ ok: true, value: 1000 });
    expect(backend.mapGet(state, program, 0, 100 + OVERFLOW_COUNT - 1)).toStrictEqual({
      ok: true,
      value: 1000 + OVERFLOW_COUNT - 1,
    });
  });

  it('delta segments support rollback and rollforward without event replay', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    const begun = stages.undo.checkpoint(state);
    if (!begun.ok) throw new Error(`unexpected undo checkpoint error ${begun.error}`);
    const fromPos = getUndoPosition(begun.value);

    expect(
      backend.executeBatchDelta(
        state,
        program,
        [
          { data: new Uint32Array([42]), type: ValueType.UINT32 },
          { data: new Uint32Array([900]), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);

    expect(backend.getMapSize(state, program, 0)).toBe(1);
    expect(backend.mapGet(state, program, 0, 42)).toStrictEqual({ ok: true, value: 900 });

    const toPos = backend.undoCheckpoint(state);
    const exported = backend.deltaExportSegment(state, fromPos, toPos);
    if (!exported.ok) throw new Error(`unexpected delta export error ${exported.error}`);
    const segment = exported.value;
    expect(segment.undoBytes.byteLength).toBeGreaterThan(0);
    expect(segment.redoBytes.byteLength).toBe(segment.undoBytes.byteLength);
    expect(segment.entrySize).toBeGreaterThan(0);
    expect(segment.overflow).toBeFalse();

    backend.deltaApplyRollbackSegment(state, segment.undoBytes, segment.entrySize);
    expect(backend.getMapSize(state, program, 0)).toBe(0);
    expect(backend.mapGet(state, program, 0, 42)).toStrictEqual({ ok: true, value: undefined });

    backend.deltaApplyRollforwardSegment(state, segment.redoBytes, segment.entrySize);
    expect(backend.getMapSize(state, program, 0)).toBe(1);
    expect(backend.mapGet(state, program, 0, 42)).toStrictEqual({ ok: true, value: 900 });
  });

  it('delta execution grows from pre-attempt state and preserves exact rollback and rollforward journals', async () => {
    const program = await backend.loadProgram(
      buildProgram({
        slots: [{ type: 'hashmap', capacity: 4 }],
        numInputs: 2,
        reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
      }),
    );
    const state = backend.createState(program);
    const keys = Uint32Array.from({ length: 12 }, (_, index) => index + 1);
    const values = Uint32Array.from(keys, (key) => key * 100);
    const columns: ColumnInput[] = [
      { data: keys, type: ValueType.UINT32 },
      { data: values, type: ValueType.UINT32 },
    ];

    // Capacity 4 cannot hold the batch. An active undo window refuses growth, so the owner rolls back the whole
    // window, ends it, grows the committed image, and retries the whole batch under a newly enabled window.
    const MAX_COMMITTED_GROWTHS = 16;
    let committed = backend.serialize(state, program);
    let growths = 0;
    let fromPos = 0;
    for (;;) {
      if (growths > MAX_COMMITTED_GROWTHS) {
        throw new Error(`batch still needs growth after ${growths} committed growths`);
      }
      expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
      fromPos = backend.undoCheckpoint(state);
      const beforeAttempt = backend.serialize(state, program);
      const result = backend.executeBatchDelta(state, program, columns, keys.length);
      if (result === ErrorCode.OK) break;
      expect(result).toBe(ErrorCode.NEEDS_GROWTH);
      expect(backend.undoCheckpoint(state)).toBe(fromPos);
      expect(backend.serialize(state, program)).toEqual(beforeAttempt);
      // An active window refuses growth and leaves the image untouched.
      expect(backend.growState(state, program)).toBe(ErrorCode.NEEDS_GROWTH);
      expect(backend.serialize(state, program)).toEqual(beforeAttempt);
      backend.undoRollback(state, fromPos);
      expect(backend.serialize(state, program)).toEqual(committed);
      expect(backend.growState(state, program)).toBe(ErrorCode.NEEDS_GROWTH);
      backend.undoCommit(state, fromPos);
      expect(backend.growState(state, program)).toBe(ErrorCode.OK);
      const grownCommitted = backend.serialize(state, program);
      expect(grownCommitted.byteLength).toBeGreaterThan(committed.byteLength);
      committed = grownCommitted;
      growths++;
    }
    expect(growths).toBeGreaterThan(0);

    // The accepted retry is the whole original batch in its window. The VM journals the entries its rows need,
    // so the exported extent is the physical range [fromPos, toPos), not one entry per key.
    expect(backend.getMapSize(state, program, 0)).toBe(keys.length);
    const windowed = backend.serialize(state, program);
    const toPos = backend.undoCheckpoint(state);
    const exported = backend.deltaExportSegment(state, fromPos, toPos);
    if (!exported.ok) throw new Error(`unexpected delta export error ${exported.error}`);
    const segment = exported.value;
    expect(segment.undoBytes.byteLength).toBe((toPos - fromPos) * segment.entrySize);
    expect(segment.redoBytes.byteLength).toBe(segment.undoBytes.byteLength);

    // Ending the window keeps the accepted image; delta lanes then move the same grown extent exactly.
    backend.undoCommit(state, fromPos);
    expect(backend.serialize(state, program)).toEqual(windowed);

    backend.deltaApplyRollbackSegment(state, segment.undoBytes, segment.entrySize);
    expect(backend.serialize(state, program)).toEqual(committed);
    expect(backend.getMapSize(state, program, 0)).toBe(0);
    for (const key of keys) {
      const read = backend.mapGet(state, program, 0, key);
      if (!read.ok) throw new Error(`unexpected mapGet error ${read.error}`);
      expect(read.value).toBeUndefined();
    }

    backend.deltaApplyRollforwardSegment(state, segment.redoBytes, segment.entrySize);
    expect(backend.serialize(state, program)).toEqual(windowed);
    expect(backend.getMapSize(state, program, 0)).toBe(keys.length);
    for (const key of keys) {
      const read = backend.mapGet(state, program, 0, key);
      if (!read.ok) throw new Error(`unexpected mapGet error ${read.error}`);
      expect(read.value).toBe(key * 100);
    }
  });

  it('without delta recording, paired export is refused and state is untouched', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    const begun = stages.undo.checkpoint(state);
    if (!begun.ok) throw new Error(`unexpected undo checkpoint error ${begun.error}`);
    const fromPos = getUndoPosition(begun.value);

    // Production profile: executes mutations but does NOT record redo deltas.
    expect(
      stages.reduce.executeBatch(
        state,
        program,
        [
          { data: new Uint32Array([7]), type: ValueType.UINT32 },
          { data: new Uint32Array([77]), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);

    expect(backend.getMapSize(state, program, 0)).toBe(1);
    expect(backend.mapGet(state, program, 0, 7)).toStrictEqual({ ok: true, value: 77 });

    const toPos = backend.undoCheckpoint(state);
    // The undo-only mutation has no redo lane, so a paired export is refused instead of returned as empty bytes.
    expect(backend.deltaExportSegment(state, fromPos, toPos)).toStrictEqual({
      ok: false,
      error: ErrorCode.DELTA_LANE_MISMATCH,
    });

    // A refused export must leave the state untouched.
    expect(backend.getMapSize(state, program, 0)).toBe(1);
    expect(backend.mapGet(state, program, 0, 7)).toStrictEqual({ ok: true, value: 77 });
  });

  // ---------------------------------------------------------------------------
  // Begin refusal: an active window refuses a second begin as value 14 and moves nothing
  // ---------------------------------------------------------------------------

  it('refuses a begin inside an active window and keeps savepoint, image, and export', async () => {
    const stages = createPipeline({ backend, parseBackend: TEST_PARSE_BACKEND });

    const bytecode = buildProgram({
      slots: [{ type: 'hashmap', capacity: 64 }],
      numInputs: 2,
      reduceOps: [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
    });

    const program = await backend.loadProgram(bytecode);
    const state = backend.createState(program);

    const begun = stages.undo.checkpoint(state);
    if (!begun.ok) throw new Error(`unexpected undo checkpoint error ${begun.error}`);
    const token = begun.value;
    const fromPos = backend.undoCheckpoint(state);
    const imageBefore = backend.serialize(state, program);

    // A paired delta write gives the open window an export selection that a refused begin must not move.
    expect(
      backend.executeBatchDelta(
        state,
        program,
        [
          { data: new Uint32Array([42]), type: ValueType.UINT32 },
          { data: new Uint32Array([900]), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);
    const toPos = backend.undoCheckpoint(state);
    const imageMid = backend.serialize(state, program);
    const exported = backend.deltaExportSegment(state, fromPos, toPos);
    if (!exported.ok) throw new Error(`unexpected delta export error ${exported.error}`);

    // Both begins are refused as values while the window is active; neither takes a savepoint.
    expect(backend.undoEnable(state)).toStrictEqual({ ok: false, error: ErrorCode.SPECULATION_ACTIVE });
    expect(stages.undo.checkpoint(state)).toStrictEqual({ ok: false, error: ErrorCode.SPECULATION_ACTIVE });
    expect(backend.undoCheckpoint(state)).toBe(toPos);
    expect(backend.serialize(state, program)).toEqual(imageMid);
    expect(backend.deltaExportSegment(state, fromPos, toPos)).toStrictEqual(exported);

    // The original token still rolls back to the image before its checkpoint.
    expect(stages.undo.rollback(state, token)).toBe('ok');
    expect(backend.serialize(state, program)).toEqual(imageBefore);
    expect(backend.getMapSize(state, program, 0)).toBe(0);
  });
});
