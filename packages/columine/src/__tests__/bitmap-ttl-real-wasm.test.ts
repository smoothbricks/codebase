import { describe, expect, it } from 'bun:test';
import { encodeProgramHeader } from '../reducer-bytecode.js';
import type { ColumineBackend, DeltaSegmentExport, ReducerProgram, StateHandle } from '../types.js';
import {
  AggType,
  ErrorCode,
  HEADER_SIZE,
  Opcode,
  PROGRAM_HASH_PREFIX,
  PROGRAM_MAGIC,
  SlotType,
  SlotTypeFlag,
  UNDO_POSITION_OVERFLOWED,
  ValueType,
} from '../types.js';
import { loadColumineWasm, VmProgramAdmissionError } from '../wasm-backend.js';

// Bitmap TTL algebra, slot-type TTL admission and undo-log overflow, run against
// the real built artifact, never a bitmap/TTL implementation substituted for it.
// The model is ordinary JS sets and exact optional timestamp bits, independent
// of the native join/planner.
const WASM_PATH = new URL('../../dist/columine.wasm', import.meta.url);
const META_BASE = 32;
const META_SIZE = 48;
const INDEX_OFFSET = 24;
const INDEX_CAPACITY = 28;
const INDEX_SIZE = 32;
const CHANGE_FLAGS = 14;
const TRIGGER_OFFSET = 36;
const TRIGGER_CAPACITY = 1024;
const ENTRY_SIZE = 16;
const UPDATED = 0x02;
const JOURNAL_CAPACITY = 16384;

const ALGEBRA = [Opcode.BATCH_BITMAP_OR, Opcode.BATCH_BITMAP_AND, Opcode.BATCH_BITMAP_ANDNOT, Opcode.BATCH_BITMAP_XOR];

type Member = { readonly key: number; readonly expiry: bigint | undefined };

function bits(value: number): bigint {
  const bytes = new ArrayBuffer(8);
  const view = new DataView(bytes);
  view.setFloat64(0, value, true);
  return view.getBigUint64(0, true);
}

function timestamp(value: bigint): number {
  const bytes = new ArrayBuffer(8);
  const view = new DataView(bytes);
  view.setBigUint64(0, value, true);
  return view.getFloat64(0, true);
}

function timestamps(values: readonly bigint[]): Float64Array {
  const data = new Float64Array(values.length);
  const view = new DataView(data.buffer);
  for (let index = 0; index < values.length; index++) view.setBigUint64(index * 8, values[index], true);
  return data;
}

function initCode(capacity = 64, sourceTtl = true, trigger = false): number[] {
  const ttl = new Uint8Array(8);
  new DataView(ttl.buffer).setFloat32(0, 10, true);
  const init: number[] = [];
  for (const slot of [0, 1]) {
    const tracked = slot === 0 || sourceTtl;
    const flags =
      SlotType.BITMAP |
      (tracked ? SlotTypeFlag.HAS_TTL : 0) |
      (slot === 0 && trigger ? SlotTypeFlag.HAS_EVICT_TRIGGER : 0);
    init.push(Opcode.SLOT_DEF, slot, flags, capacity & 0xff, capacity >>> 8);
    if (tracked) init.push(...ttl, 1, 0);
  }
  init.push(Opcode.SLOT_DEF, 2, SlotType.AGGREGATE, AggType.COUNT, 0, Opcode.HALT);
  return init;
}

function program(init: readonly number[], reduce: readonly number[], numInputs = 2): Uint8Array {
  const body = [...reduce, Opcode.HALT];
  const bytecode = new Uint8Array(PROGRAM_HASH_PREFIX + HEADER_SIZE + init.length + body.length);
  bytecode.set(
    encodeProgramHeader({
      magic: PROGRAM_MAGIC,
      numSlots: 3,
      numInputs,
      initCodeLength: init.length,
      reduceCodeLength: body.length,
    }),
    PROGRAM_HASH_PREFIX,
  );
  bytecode.set(init, PROGRAM_HASH_PREFIX + HEADER_SIZE);
  bytecode.set(body, PROGRAM_HASH_PREFIX + HEADER_SIZE + init.length);
  return bytecode;
}

function indexRows(image: Uint8Array, slot: number): Map<number, bigint> {
  const view = new DataView(image.buffer, image.byteOffset, image.byteLength);
  const meta = META_BASE + slot * META_SIZE;
  const rows = new Map<number, bigint>();
  if ((view.getUint8(meta + 12) & SlotTypeFlag.HAS_TTL) === 0) return rows;
  const offset = view.getUint32(meta + INDEX_OFFSET, true);
  const size = view.getUint32(meta + INDEX_SIZE, true);
  for (let index = 0; index < size; index++) {
    const entry = offset + index * ENTRY_SIZE;
    rows.set(view.getUint32(entry + 8, true), view.getBigUint64(entry, true));
  }
  return rows;
}

// A retained checkpoint may legitimately have members without expiry. Omit
// only their index rows, retaining actual init geometry, original row bytes,
// order and dead tails. This is not a fake timestamp or a forged capacity.
function omitExpiryRows(image: Uint8Array, slot: number, absent: ReadonlySet<number>): void {
  const view = new DataView(image.buffer, image.byteOffset, image.byteLength);
  const meta = META_BASE + slot * META_SIZE;
  const offset = view.getUint32(meta + INDEX_OFFSET, true);
  const size = view.getUint32(meta + INDEX_SIZE, true);
  let kept = 0;
  for (let index = 0; index < size; index++) {
    const entry = offset + index * ENTRY_SIZE;
    if (absent.has(view.getUint32(entry + 8, true))) continue;
    image.copyWithin(offset + kept * ENTRY_SIZE, entry, entry + ENTRY_SIZE);
    kept++;
  }
  view.setUint32(meta + INDEX_SIZE, kept, true);
}

function seedDeadIndexTails(image: Uint8Array): void {
  const view = new DataView(image.buffer, image.byteOffset, image.byteLength);
  for (const slot of [0, 1]) {
    const meta = META_BASE + slot * META_SIZE;
    if ((view.getUint8(meta + 12) & SlotTypeFlag.HAS_TTL) === 0) continue;
    const offset = view.getUint32(meta + INDEX_OFFSET, true);
    const size = view.getUint32(meta + INDEX_SIZE, true);
    const capacity = view.getUint32(meta + INDEX_CAPACITY, true);
    image.fill(0xa7, offset + size * ENTRY_SIZE, offset + capacity * ENTRY_SIZE);
    if ((view.getUint8(meta + 12) & SlotTypeFlag.HAS_EVICT_TRIGGER) !== 0) {
      const trigger = view.getUint32(meta + TRIGGER_OFFSET, true);
      image.fill(0xcd, trigger, trigger + TRIGGER_CAPACITY * ENTRY_SIZE);
    }
  }
}

async function seed(
  backend: ColumineBackend,
  init: readonly number[],
  a: readonly Member[],
  b: readonly Member[],
  sourceTtl = true,
): Promise<{ state: StateHandle; reducer: ReducerProgram }> {
  const reducer = await backend.loadProgram(program(init, []));
  let state = backend.createState(reducer);
  for (const [slot, members] of [
    [0, a],
    [1, b],
  ] as const) {
    const tracked = slot === 0 || sourceTtl;
    const insert = await backend.loadProgram(
      program(init, tracked ? [Opcode.BATCH_SET_INSERT_TTL, slot, 0, 1] : [Opcode.BATCH_BITMAP_ADD, slot, 0]),
    );
    // Native insertion creates the membership and every present timestamp.
    // Missing timestamps are explicitly absent rows in the later checkpoint.
    const stamps = timestamps(members.map((member) => member.expiry ?? bits(123)));
    expect(
      backend.executeBatch(
        state,
        insert,
        [
          { data: Uint32Array.from(members, (member) => member.key), type: ValueType.UINT32 },
          { data: stamps, type: ValueType.FLOAT64 },
        ],
        members.length,
      ),
    ).toBe(ErrorCode.OK);
  }
  const image = backend.serialize(state, reducer);
  omitExpiryRows(image, 0, new Set(a.filter((member) => member.expiry === undefined).map((member) => member.key)));
  if (sourceTtl) {
    omitExpiryRows(image, 1, new Set(b.filter((member) => member.expiry === undefined).map((member) => member.key)));
  }
  seedDeadIndexTails(image);
  state = backend.deserialize(reducer, image);
  return { state, reducer };
}

function model(op: Opcode, a: readonly Member[], b: readonly Member[]): Member[] {
  const left = new Map(a.map((member) => [member.key, member.expiry]));
  const right = new Map(b.map((member) => [member.key, member.expiry]));
  const out: Member[] = [];
  for (const key of new Set([...left.keys(), ...right.keys()])) {
    const inA = left.has(key);
    const inB = right.has(key);
    let present: boolean;
    switch (op) {
      case Opcode.BATCH_BITMAP_OR:
        present = inA || inB;
        break;
      case Opcode.BATCH_BITMAP_AND:
        present = inA && inB;
        break;
      case Opcode.BATCH_BITMAP_ANDNOT:
        present = inA && !inB;
        break;
      case Opcode.BATCH_BITMAP_XOR:
        present = inA !== inB;
        break;
      default:
        throw new Error(`not an algebra opcode: ${op}`);
    }
    if (!present) continue;
    const x = left.get(key);
    const y = right.get(key);
    let expiry: bigint | undefined;
    if (op === Opcode.BATCH_BITMAP_ANDNOT) expiry = x;
    else if (op === Opcode.BATCH_BITMAP_XOR) expiry = inA ? x : y;
    else if (x === undefined) expiry = y;
    else if (y === undefined) expiry = x;
    else if (op === Opcode.BATCH_BITMAP_OR) expiry = timestamp(x) < timestamp(y) ? y : x;
    else expiry = timestamp(x) > timestamp(y) ? y : x;
    out.push({ key, expiry });
  }
  return out.sort((x, y) => x.key - y.key);
}

function assertModel(
  backend: ColumineBackend,
  state: StateHandle,
  reducer: ReducerProgram,
  expected: readonly Member[],
): void {
  const wanted = new Map(expected.map((member) => [member.key, member.expiry]));
  expect(backend.getSetSize(state, reducer, 0)).toBe(expected.length);
  for (let key = 1; key <= 32; key++) {
    expect(backend.setContains(state, reducer, 0, key)).toStrictEqual({ ok: true, value: wanted.has(key) });
  }
  const actual = indexRows(backend.serialize(state, reducer), 0);
  const tracked = new Map<number, bigint>();
  for (const member of expected) if (member.expiry !== undefined) tracked.set(member.key, member.expiry);
  expect(actual).toEqual(tracked);
}

function exportSegment(backend: ColumineBackend, state: StateHandle, from: number, to: number): DeltaSegmentExport {
  const result = backend.deltaExportSegment(state, from, to);
  if (!result.ok) throw new Error(`real paired export refused ${result.error}`);
  expect(result.value.overflow).toBe(false);
  expect(result.value.undoBytes.byteLength).toBe((to - from) * result.value.entrySize);
  expect(result.value.redoBytes.byteLength).toBe(result.value.undoBytes.byteLength);
  for (let at = 0; at < result.value.undoBytes.byteLength; at += result.value.entrySize) {
    expect(result.value.undoBytes[at]).toBe(15);
    expect(result.value.redoBytes[at]).toBe(15);
  }
  return result.value;
}

function replay(
  backend: ColumineBackend,
  state: StateHandle,
  reducer: ReducerProgram,
  segment: DeltaSegmentExport,
  before: Uint8Array,
  after: Uint8Array,
): void {
  backend.deltaApplyRollforwardSegment(state, segment.redoBytes, segment.entrySize);
  expect(backend.serialize(state, reducer)).toEqual(after);
  backend.deltaApplyRollbackSegment(state, segment.undoBytes, segment.entrySize);
  expect(backend.serialize(state, reducer)).toEqual(before);
}

describe('real WASM bitmap TTL algebra', () => {
  for (const op of ALGEBRA) {
    for (const delta of [false, true]) {
      it(`matches exact optional-bit model and complete-image rollback/replay, op ${op}, paired ${delta}`, async () => {
        const backend = await loadColumineWasm(WASM_PATH);
        const init = initCode();
        const a: Member[] = [
          { key: 1, expiry: undefined },
          { key: 2, expiry: bits(-0) },
          { key: 3, expiry: 0x7ff8_0000_0000_00a1n },
          { key: 5, expiry: bits(100) },
          { key: 7, expiry: undefined },
          { key: 8, expiry: bits(30) },
          { key: 9, expiry: undefined },
        ];
        const b: Member[] = [
          { key: 2, expiry: bits(0) },
          { key: 3, expiry: 0x7ff8_0000_0000_00b2n },
          { key: 4, expiry: undefined },
          { key: 6, expiry: bits(50) },
          { key: 7, expiry: bits(40) },
          { key: 8, expiry: undefined },
          { key: 9, expiry: undefined },
        ];
        const { state, reducer } = await seed(backend, init, a, b);
        const algebra = await backend.loadProgram(program(init, [op, 0, 1]));
        const before = backend.serialize(state, reducer);
        expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
        const from = backend.undoCheckpoint(state);
        const run = delta ? backend.executeBatchDelta.bind(backend) : backend.executeBatch.bind(backend);
        expect(run(state, algebra, [], 1)).toBe(ErrorCode.OK);
        assertModel(backend, state, reducer, model(op, a, b));
        const after = backend.serialize(state, reducer);
        const segment = delta ? exportSegment(backend, state, from, backend.undoCheckpoint(state)) : undefined;
        backend.undoRollback(state, from);
        expect(backend.serialize(state, reducer)).toEqual(before);
        backend.undoCommit(state, from);
        if (segment) replay(backend, state, reducer, segment, before, after);
      });

      it(`merges finite expiry and restores trigger bytes at nested marks, op ${op}, paired ${delta}`, async () => {
        const backend = await loadColumineWasm(WASM_PATH);
        const init = initCode(64, true, true);
        const a = [0, 10, 20].map((time, index) => ({ key: index + 1, expiry: bits(time) }));
        const b = [
          { key: 2, expiry: bits(30) },
          { key: 3, expiry: bits(5) },
          { key: 4, expiry: bits(15) },
        ];
        const { state, reducer } = await seed(backend, init, a, b);
        const algebra = await backend.loadProgram(program(init, [op, 0, 1]));
        const expected = model(op, a, b);
        const before = backend.serialize(state, reducer);
        expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
        const from = backend.undoCheckpoint(state);
        const run = delta ? backend.executeBatchDelta.bind(backend) : backend.executeBatch.bind(backend);
        expect(run(state, algebra, [], 1)).toBe(ErrorCode.OK);
        assertModel(backend, state, reducer, expected);
        const merged = backend.serialize(state, reducer);
        const middle = backend.undoCheckpoint(state);
        const first = delta ? exportSegment(backend, state, from, middle) : undefined;
        const expired = expected.flatMap((member) => {
          if (member.expiry === undefined || timestamp(member.expiry) >= 15) return [];
          return [{ key: member.key, expiry: member.expiry }];
        });
        const remaining = expected.filter((member) => member.expiry === undefined || timestamp(member.expiry) >= 15);
        const result = backend.evictExpired(state, reducer, 25, delta);
        if (!result.ok) throw new Error(`real expiry refused ${result.error}`);
        expect(result.count).toBe(expired.length + 1);
        expect(result.rows).toEqual(
          expired
            .map((member) => ({
              slot: 0,
              key: member.key,
              value: member.key,
              timestamp: timestamp(member.expiry),
            }))
            .sort((x, y) => x.timestamp - y.timestamp),
        );
        assertModel(backend, state, reducer, remaining);
        const after = backend.serialize(state, reducer);
        const second = delta ? exportSegment(backend, state, middle, backend.undoCheckpoint(state)) : undefined;
        backend.undoRollback(state, middle);
        expect(backend.serialize(state, reducer)).toEqual(merged);
        backend.undoRollback(state, from);
        expect(backend.serialize(state, reducer)).toEqual(before);
        backend.undoCommit(state, from);
        if (first && second) {
          backend.deltaApplyRollforwardSegment(state, first.redoBytes, first.entrySize);
          expect(backend.serialize(state, reducer)).toEqual(merged);
          replay(backend, state, reducer, second, merged, after);
          backend.deltaApplyRollbackSegment(state, first.undoBytes, first.entrySize);
          expect(backend.serialize(state, reducer)).toEqual(before);
        }
      });
    }
  }

  it('timestamp-only union sets UPDATED and plain source carries no invented expiry', async () => {
    for (const sourceTtl of [false, true]) {
      const backend = await loadColumineWasm(WASM_PATH);
      const init = initCode(64, sourceTtl);
      const a = [{ key: 1, expiry: bits(10) }];
      const b = [{ key: 1, expiry: sourceTtl ? bits(20) : undefined }];
      const seeded = await seed(backend, init, a, b, sourceTtl);
      // Seeding inserted the members and left INSERTED set; clear it so the
      // flags read below are exactly the ones the algebra raised.
      const seededImage = backend.serialize(seeded.state, seeded.reducer);
      seededImage[META_BASE + CHANGE_FLAGS] = 0;
      const reducer = seeded.reducer;
      const state = backend.deserialize(reducer, seededImage);
      const algebra = await backend.loadProgram(program(init, [Opcode.BATCH_BITMAP_OR, 0, 1]));
      expect(backend.executeBatch(state, algebra, [], 1)).toBe(ErrorCode.OK);
      assertModel(backend, state, reducer, model(Opcode.BATCH_BITMAP_OR, a, b));
      const image = backend.serialize(state, reducer);
      expect(image[META_BASE + CHANGE_FLAGS]).toBe(sourceTtl ? UPDATED : 0);
    }
  });

  for (const delta of [false, true]) {
    it(`admits the complete TTL batch before a full-index payload write, paired ${delta}`, async () => {
      const backend = await loadColumineWasm(WASM_PATH);
      const init = initCode(8);
      const a = Array.from({ length: 16 }, (_, index) => ({ key: index + 1, expiry: bits(index + 1) }));
      const b = [
        { key: 1, expiry: bits(200) },
        { key: 17, expiry: bits(30) },
      ];
      const { state, reducer } = await seed(backend, init, a, b);
      const algebra = await backend.loadProgram(program(init, [Opcode.BATCH_BITMAP_OR, 0, 1]));
      const before = backend.serialize(state, reducer);
      expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
      const checkpoint = backend.undoCheckpoint(state);
      const run = delta ? backend.executeBatchDelta.bind(backend) : backend.executeBatch.bind(backend);
      expect(run(state, algebra, [], 1)).toBe(ErrorCode.NEEDS_GROWTH);
      expect(backend.undoCheckpoint(state)).toBe(checkpoint);
      expect(backend.serialize(state, reducer)).toEqual(before);
      expect(backend.undoHasOverflow()).toBe(false);
      backend.undoRollback(state, checkpoint);
      expect(backend.serialize(state, reducer)).toEqual(before);
      backend.undoCommit(state, checkpoint);
    });

    it(`shadows a TTL-algebra capture that overflows the journal, rolls back exactly, paired ${delta}`, async () => {
      const backend = await loadColumineWasm(WASM_PATH);
      const init = initCode();
      const a = Array.from({ length: 12 }, (_, index) => ({ key: index + 1, expiry: bits(index + 1) }));
      const b = a.map((member) => ({ key: member.key, expiry: bits(timestamp(member.expiry) + 100) }));
      const { state, reducer } = await seed(backend, init, a, b);
      const count = await backend.loadProgram(program(init, [Opcode.BATCH_AGG_COUNT, 2], 0));
      const algebra = await backend.loadProgram(program(init, [Opcode.BATCH_BITMAP_OR, 0, 1]));
      const before = backend.serialize(state, reducer);
      expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
      const from = backend.undoCheckpoint(state);
      const run = delta ? backend.executeBatchDelta.bind(backend) : backend.executeBatch.bind(backend);
      let checkpoint = from;
      while (checkpoint < JOURNAL_CAPACITY - 8) {
        expect(run(state, count, [], 1)).toBe(ErrorCode.OK);
        const next = backend.undoCheckpoint(state);
        expect(next).toBeGreaterThan(checkpoint);
        expect(next).toBeLessThan(JOURNAL_CAPACITY);
        checkpoint = next;
      }
      const prefix = backend.serialize(state, reducer);
      const segment = delta ? exportSegment(backend, state, from, checkpoint) : undefined;
      expect(run(state, algebra, [], 1)).toBe(ErrorCode.OK);
      expect(backend.undoHasOverflow()).toBe(true);
      expect(backend.undoCheckpoint(state)).toBe(UNDO_POSITION_OVERFLOWED);
      // The overflow lands the algebra's writes in the shadow.
      assertModel(backend, state, reducer, model(Opcode.BATCH_BITMAP_OR, a, b));
      // A pre-overflow savepoint restores the exact prefix image and ends the overflow.
      backend.undoRollback(state, checkpoint);
      expect(backend.serialize(state, reducer)).toEqual(prefix);
      expect(backend.undoHasOverflow()).toBe(false);
      expect(backend.undoCheckpoint(state)).toBe(checkpoint);
      backend.undoRollback(state, from);
      expect(backend.serialize(state, reducer)).toEqual(before);
      // The rollback emptied the journal, so the same capture fits and journals again.
      expect(run(state, algebra, [], 1)).toBe(ErrorCode.OK);
      expect(backend.undoHasOverflow()).toBe(false);
      expect(backend.undoCheckpoint(state)).toBeGreaterThan(from);
      backend.undoRollback(state, from);
      expect(backend.serialize(state, reducer)).toEqual(before);
      backend.undoCommit(state, from);
      expect(backend.undoHasOverflow()).toBe(false);
      if (segment) replay(backend, state, reducer, segment, before, prefix);
    });
  }

  it('refuses the two actual program causes before state construction', async () => {
    const backend = await loadColumineWasm(WASM_PATH);
    const invalid = program(initCode(), [0xff]);
    await expect(backend.loadProgram(invalid)).rejects.toBeInstanceOf(VmProgramAdmissionError);
    await expect(backend.loadProgram(invalid)).rejects.toMatchObject({
      code: ErrorCode.INVALID_PROGRAM,
      reason: { kind: 'MalformedProgram' },
    });
    const ttl = new Uint8Array(8);
    new DataView(ttl.buffer).setFloat32(0, 10, true);
    const init = [
      Opcode.SLOT_DEF,
      0,
      SlotType.BITMAP,
      64,
      0,
      Opcode.SLOT_DEF,
      1,
      SlotType.BITMAP | SlotTypeFlag.HAS_TTL,
      64,
      0,
      ...ttl,
      1,
      0,
      Opcode.SLOT_DEF,
      2,
      SlotType.AGGREGATE,
      AggType.COUNT,
      0,
      Opcode.HALT,
    ];
    await expect(backend.loadProgram(program(init, [Opcode.BATCH_BITMAP_OR, 0, 1]))).rejects.toMatchObject({
      code: ErrorCode.INVALID_PROGRAM,
      reason: { kind: 'BitmapTtlSourceToUntrackedTarget', source: 1, target: 0 },
    });
  });
});
