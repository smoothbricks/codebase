import { checkCapacityTuning } from '../../capacityTuning.js';
import { EMPTY_SCOPE, materializeCompiledSpanBufferClass } from '../../spanBuffer.js';
import { copyThreadIdTo, getThreadId } from '../../threadId.js';

const SPAN_BUFFER_AOT_ABI_SYMBOL = Symbol.for('@smoothbricks/lmao/span-buffer/aot/v1');

const spanBufferAotRuntime = Object.freeze({
  EMPTY_SCOPE,
  checkCapacityTuning,
  copyThreadIdTo,
  getThreadId,
  materializeCompiledSpanBufferClass,
});

const existingRuntime = Reflect.get(globalThis, SPAN_BUFFER_AOT_ABI_SYMBOL);
if (existingRuntime === undefined) {
  Object.defineProperty(globalThis, SPAN_BUFFER_AOT_ABI_SYMBOL, {
    value: spanBufferAotRuntime,
    enumerable: false,
    configurable: false,
    writable: false,
  });
} else if (existingRuntime !== spanBufferAotRuntime) {
  // invariant throw: one realm must expose exactly one canonical SpanBuffer AOT ABI.
  throw new TypeError('Conflicting LMAO SpanBuffer AOT runtime registrations');
}
