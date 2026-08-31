/**
 * Nothing registered: v1 installs lmao's own runtime, frozen and
 * non-replaceable, and a later conflicting host registration throws in the
 * later registrar's stack.
 */
import { isSpanBufferAotRuntime, SPAN_BUFFER_AOT_ABI_SYMBOL } from '../../span-buffer/aot/abi.js';

// Dynamic on purpose: this fixture exercises module-evaluation ORDER — the slot
// state above must exist before v1 evaluates, which a static import would defeat.
await import('../../span-buffer/aot/v1.js');

const occupant: unknown = Reflect.get(globalThis, SPAN_BUFFER_AOT_ABI_SYMBOL);
let lateRegistrationThrew = false;
try {
  Object.defineProperty(globalThis, SPAN_BUFFER_AOT_ABI_SYMBOL, {
    value: { late: true },
    configurable: false,
    writable: false,
  });
} catch {
  lateRegistrationThrew = true;
}
process.stdout.write(
  JSON.stringify({
    conforming: isSpanBufferAotRuntime(occupant),
    frozen: typeof occupant === 'object' && occupant !== null && Object.isFrozen(occupant),
    lateRegistrationThrew,
  }),
);
