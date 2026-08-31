/**
 * A non-conforming occupant holds the ABI slot when v1 evaluates. v1 must
 * refuse the realm with the registration-conflict TypeError.
 */
import { SPAN_BUFFER_AOT_ABI_SYMBOL } from '../../span-buffer/aot/abi.js';

Object.defineProperty(globalThis, SPAN_BUFFER_AOT_ABI_SYMBOL, {
  value: { notARuntime: true },
  enumerable: false,
  configurable: false,
  writable: false,
});

try {
  // Dynamic on purpose: the occupant above must precede v1's evaluation.
  await import('../../span-buffer/aot/v1.js');
  process.stdout.write(JSON.stringify({ threw: false }));
} catch (error) {
  process.stdout.write(JSON.stringify({ threw: true, typeError: error instanceof TypeError, message: String(error) }));
}
