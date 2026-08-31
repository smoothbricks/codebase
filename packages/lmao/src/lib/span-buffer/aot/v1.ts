import { checkCapacityTuning } from '../../capacityTuning.js';
import { EMPTY_SCOPE, materializeCompiledSpanBufferClass } from '../../spanBuffer.js';
import { copyThreadIdTo, getThreadId } from '../../threadId.js';
import { isSpanBufferAotRuntime, SPAN_BUFFER_AOT_ABI_SYMBOL, type SpanBufferAotRuntime } from './abi.js';

const spanBufferAotRuntime: SpanBufferAotRuntime = Object.freeze({
  EMPTY_SCOPE,
  checkCapacityTuning,
  copyThreadIdTo,
  getThreadId,
  materializeCompiledSpanBufferClass,
});

const existingRuntime: unknown = Reflect.get(globalThis, SPAN_BUFFER_AOT_ABI_SYMBOL);
if (existingRuntime === undefined) {
  // Nothing registered: lmao's own JS-heap runtime is the default, installed
  // exactly because no host claimed the realm first.
  Object.defineProperty(globalThis, SPAN_BUFFER_AOT_ABI_SYMBOL, {
    value: spanBufferAotRuntime,
    enumerable: false,
    configurable: false,
    writable: false,
  });
} else if (!isSpanBufferAotRuntime(existingRuntime)) {
  // invariant throw: the slot is the realm's one SpanBuffer AOT ABI. An
  // occupant that does not implement it is a mis-registered host, and every
  // compiled writer in this realm would break at first use — fail at setup,
  // in the registering stack, not at the first trace call.
  throw new TypeError('Conflicting LMAO SpanBuffer AOT runtime registrations');
}
// A conforming occupant is a host-supplied runtime registered before this
// module evaluated (preload / pre-authored host script). Adopt it: generated
// code reads the slot, so installing nothing IS the inversion.
