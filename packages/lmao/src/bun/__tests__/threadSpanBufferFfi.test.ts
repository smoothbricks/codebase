import { ptr } from 'bun:ffi';
import { describe, expect, it } from 'bun:test';
import { S } from '../../lib/schema/builder.js';
import { defineLogSchema } from '../../lib/schema/defineLogSchema.js';
import { ENTRY_TYPE_INFO, ENTRY_TYPE_SPAN_OK } from '../../lib/schema/systemSchema.js';
import { ThreadSpanCells } from '../../lib/wasm/threadSpanView.js';
import { createThreadSpanBuffer, threadSpanBufferFfiAvailable } from '../threadSpanBufferFfi.js';

describe('Bun native thread span buffer ABI', () => {
  it('writes interned and static rows and preserves packed failures', () => {
    expect(threadSpanBufferFfiAvailable).toBe(true);
    const binding = createThreadSpanBuffer(7n, 8);
    expect(binding).toBeDefined();
    if (binding === undefined) return;

    try {
      const nameId = binding.intern('root');
      expect(nameId).not.toBe(0);
      expect(binding.intern('root')).toBe(nameId);

      const opened = binding.openSpan('trace', 0n, 0, nameId, 10n, 1);
      expect(opened).not.toBe(0n);
      const spanId = Number(opened >> 32n);
      expect(Number(opened & 0xffff_ffffn)).toBe(0);

      expect(binding.appendLog(spanId, ENTRY_TYPE_INFO, nameId, 11n, 2)).not.toBe(0n);
      expect(binding.appendLogStatic(spanId, ENTRY_TYPE_INFO, 1, 12n, 3)).not.toBe(0n);
      expect(binding.end(spanId, ENTRY_TYPE_SPAN_OK, 14n)).toBe(0);

      expect(binding.appendLog(spanId + 1, ENTRY_TYPE_INFO, nameId, 15n, 5)).toBe(0n);
      expect(binding.attributeCells(0)).toBeUndefined();
    } finally {
      binding.free();
    }
  });

  it("aliases the native store's attribute cells instead of copying them", () => {
    const schema = defineLogSchema({ count: S.number() });
    const binding = createThreadSpanBuffer(7n, 8, schema);
    if (binding === undefined) throw new Error('native thread span buffer unavailable');
    try {
      const opened = binding.openSpan('trace', 0n, 0, binding.intern('root'), 10n, 1);
      const row = Number(opened & 0xffff_ffffn);
      const cells = new ThreadSpanCells(binding);
      cells.storeNumber(0, row, 2.5);

      // A fresh wrap of the same block sees the store: one allocation, two names.
      const again = binding.attributeCells(0);
      if (again === undefined) throw new Error('block 0 has no cells');
      expect(new Float64Array(again.buffer, again.byteOffset, 1 + row)[row]).toBe(2.5);
      expect(ptr(cells.views(row).f64)).toBe(ptr(again));
    } finally {
      binding.free();
    }
  });
});
