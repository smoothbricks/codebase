import { describe, expect, it } from 'bun:test';
import { encodeProgramHeader } from '../reducer-bytecode.js';
import { AggType, ErrorCode, HEADER_SIZE, Opcode, PROGRAM_HASH_PREFIX, PROGRAM_MAGIC, SlotType } from '../types.js';
import { loadColumineWasm } from '../wasm-backend.js';

const WASM_PATH = new URL('../../.cache/wasm-dev/columine.wasm', import.meta.url);

/** Slot 0 is an i64 sum of input column 0. */
function sumProgram(): Uint8Array {
  const initCode = new Uint8Array([Opcode.SLOT_DEF, 0, SlotType.AGGREGATE, AggType.SUM_I64, 0, Opcode.HALT]);
  const reduceCode = new Uint8Array([Opcode.BATCH_AGG_SUM_I64, 0, 0, Opcode.HALT]);
  const bytes = new Uint8Array(PROGRAM_HASH_PREFIX + HEADER_SIZE + initCode.length + reduceCode.length);
  bytes.set(
    encodeProgramHeader({
      magic: PROGRAM_MAGIC,
      numSlots: 1,
      numInputs: 1,
      initCodeLength: initCode.length,
      reduceCodeLength: reduceCode.length,
    }),
    PROGRAM_HASH_PREFIX,
  );
  bytes.set(initCode, PROGRAM_HASH_PREFIX + HEADER_SIZE);
  bytes.set(reduceCode, PROGRAM_HASH_PREFIX + HEADER_SIZE + initCode.length);
  return bytes;
}

//#region smoo/columine!n/wide-i64-sum-refuses
describe('an i64 sum through the wasm host', () => {
  it('refuses a running total past i64 with SUM_OVERFLOW and names its slot', async () => {
    const backend = await loadColumineWasm(WASM_PATH);
    const reducer = await backend.loadProgram(sumProgram());
    const state = backend.createState(reducer);
    const max = 0x7fff_ffff_ffff_ffffn;
    expect(backend.executeBatch(state, reducer, [{ data: new BigInt64Array([max, 1n]) }], 2)).toBe(
      ErrorCode.SUM_OVERFLOW,
    );
    expect(backend.sumOverflowSlot(reducer)).toBe(0);
    expect(backend.executeBatch(state, reducer, [{ data: new BigInt64Array([max]) }], 1)).toBe(ErrorCode.OK);
    expect(backend.sumOverflowSlot(reducer)).toBeUndefined();
  });
});
//#endregion smoo/columine!n/wide-i64-sum-refuses
