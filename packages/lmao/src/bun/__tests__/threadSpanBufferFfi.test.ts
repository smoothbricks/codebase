import { describe, expect, it } from 'bun:test';
import { ENTRY_TYPE_INFO, ENTRY_TYPE_SPAN_OK } from '../../lib/schema/systemSchema.js';
import { createThreadSpanBuffer, threadSpanBufferFfiAvailable } from '../threadSpanBufferFfi.js';

describe('Bun native thread span buffer ABI', () => {
  it('writes interned, static, and dynamic rows and preserves packed failures', () => {
    expect(threadSpanBufferFfiAvailable).toBe(true);
    const binding = createThreadSpanBuffer(7n, 8);
    expect(binding).toBeDefined();
    if (binding === undefined) return;

    try {
      const nameId = binding.intern('root');
      expect(nameId).not.toBe(0);
      expect(binding.intern('root')).toBe(nameId);

      const opened = binding.openSpanText('trace', 0n, 0, nameId, 10n, 1);
      expect(opened).not.toBe(0n);
      const spanId = Number(opened >> 32n);
      expect(Number(opened & 0xffff_ffffn)).toBe(0);

      expect(binding.appendLog(spanId, ENTRY_TYPE_INFO, nameId, 11n, 2)).not.toBe(0n);
      expect(binding.appendLogStatic(spanId, ENTRY_TYPE_INFO, 1, 12n, 3)).not.toBe(0n);
      expect(binding.appendLogDynamicText(spanId, ENTRY_TYPE_INFO, 'dynamic', 13n, 4)).not.toBe(0n);
      expect(binding.end(spanId, ENTRY_TYPE_SPAN_OK, 14n)).toBe(0);

      expect(binding.appendLog(spanId + 1, ENTRY_TYPE_INFO, nameId, 15n, 5)).toBe(0n);
    } finally {
      binding.free();
    }
  });
});
