/**
 * A host registers a conforming AOT runtime BEFORE the v1 module evaluates.
 * v1 must adopt it: no throw, and the slot keeps the host's object identity.
 */

import { checkCapacityTuning } from '../../capacityTuning.js';
import { SPAN_BUFFER_AOT_ABI_SYMBOL, type SpanBufferAotRuntime } from '../../span-buffer/aot/abi.js';
import { EMPTY_SCOPE, materializeCompiledSpanBufferClass } from '../../spanBuffer.js';
import { copyThreadIdTo, getThreadId } from '../../threadId.js';

const hostRuntime: SpanBufferAotRuntime = Object.freeze({
  EMPTY_SCOPE,
  checkCapacityTuning,
  copyThreadIdTo,
  getThreadId,
  materializeCompiledSpanBufferClass,
});
Object.defineProperty(globalThis, SPAN_BUFFER_AOT_ABI_SYMBOL, {
  value: hostRuntime,
  enumerable: false,
  configurable: false,
  writable: false,
});

// Dynamic on purpose: this fixture exercises module-evaluation ORDER — the slot
// state above must exist before v1 evaluates, which a static import would defeat.
await import('../../span-buffer/aot/v1.js');

const occupant: unknown = Reflect.get(globalThis, SPAN_BUFFER_AOT_ABI_SYMBOL);
process.stdout.write(JSON.stringify({ adopted: occupant === hostRuntime }));
