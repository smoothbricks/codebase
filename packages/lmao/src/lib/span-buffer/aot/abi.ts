/**
 * The SpanBuffer AOT ABI contract, from the host's side of the seam.
 *
 * `@smoothbricks/lmao/span-buffer/aot/v1` is a side-effect module: evaluating
 * it guarantees the realm global holds exactly one runtime under
 * {@link SPAN_BUFFER_AOT_ABI_SYMBOL}. Compiler-generated code (lmao-ttsc's
 * `$$_lmaoSpanBufferAot` binding) reads that slot and nothing else, which makes
 * the slot the ONE inversion point a host has: install a conforming runtime
 * BEFORE the v1 module evaluates (a Bun preload, containium's pre-authored host
 * script) and every compiled writer in the realm is the host's. lmao never
 * detects a host — the dependency direction is host → this published contract.
 *
 * Registration is decided once, at realm setup:
 * - empty slot → v1 installs lmao's own runtime (frozen, non-configurable);
 * - conforming occupant → v1 adopts it and installs nothing;
 * - non-conforming occupant → `TypeError`. One realm carrying two answers for
 *   the same ABI is a deploy invariant violation, not an operational failure,
 *   so it throws rather than returning a Result.
 * A second HOST cannot displace a first: the slot is defined non-configurable
 * and non-writable by whichever registration won, so the conflicting
 * `defineProperty` throws in the loser's stack, where the misconfiguration is.
 */

import type { checkCapacityTuning } from '../../capacityTuning.js';
import type { EMPTY_SCOPE, materializeCompiledSpanBufferClass } from '../../spanBuffer.js';
import type { copyThreadIdTo, getThreadId } from '../../threadId.js';

export const SPAN_BUFFER_AOT_ABI_SYMBOL = Symbol.for('@smoothbricks/lmao/span-buffer/aot/v1');

/**
 * The five members generated writers reach through the slot. Signatures are
 * the canonical lmao implementations' own types, so a host implementation
 * cannot drift from the compiled call sites without failing to typecheck.
 */
export interface SpanBufferAotRuntime {
  readonly EMPTY_SCOPE: typeof EMPTY_SCOPE;
  readonly checkCapacityTuning: typeof checkCapacityTuning;
  readonly copyThreadIdTo: typeof copyThreadIdTo;
  readonly getThreadId: typeof getThreadId;
  readonly materializeCompiledSpanBufferClass: typeof materializeCompiledSpanBufferClass;
}

/**
 * Structural conformance for a slot occupant.
 *
 * This is a capability object of functions, so the boundary check is member
 * presence and callability — function SIGNATURES are not runtime-checkable, and
 * Typia has nothing extra to validate on a function-typed member. Behavioral
 * conformance is the host's contract obligation, pinned by the type above.
 */
export function isSpanBufferAotRuntime(value: unknown): value is SpanBufferAotRuntime {
  if (typeof value !== 'object' || value === null) return false;
  const scope = Reflect.get(value, 'EMPTY_SCOPE');
  return (
    typeof scope === 'object' &&
    scope !== null &&
    typeof Reflect.get(value, 'checkCapacityTuning') === 'function' &&
    typeof Reflect.get(value, 'copyThreadIdTo') === 'function' &&
    typeof Reflect.get(value, 'getThreadId') === 'function' &&
    typeof Reflect.get(value, 'materializeCompiledSpanBufferClass') === 'function'
  );
}
