export const RUNTIME_HINT_INITIAL_CAPACITY_MASK = 0x0000ffff;

export const RUNTIME_HINT_TAG = 0x00010000;
export const RUNTIME_HINT_LOG = 0x00020000;
export const RUNTIME_HINT_FF = 0x00040000;
export const RUNTIME_HINT_SPAN = 0x00080000;
export const RUNTIME_HINT_RESULT = 0x00100000;
export const RUNTIME_HINT_SCOPE = 0x00200000;
export const RUNTIME_HINT_DEPS = 0x00400000;
export const RUNTIME_HINT_CAPABILITIES_MASK = 0x007f0000;
export const RUNTIME_HINT_ANALYZED_VALID = 0x00800000;
export const RUNTIME_HINT_RESERVED_MASK = 0xff000000;

export const RUNTIME_HINT_FULL_CAPABILITIES = RUNTIME_HINT_CAPABILITIES_MASK;

export interface DecodedRuntimeHint {
  readonly analyzed: boolean;
  readonly initialCapacity: number | undefined;
  readonly capabilities: number;
}

export function isRuntimeHintAnalyzed(runtimeHint: number): boolean {
  return (
    Number.isInteger(runtimeHint) &&
    runtimeHint >= 0 &&
    runtimeHint <= 0xffffffff &&
    (runtimeHint & RUNTIME_HINT_RESERVED_MASK) === 0 &&
    (runtimeHint & RUNTIME_HINT_ANALYZED_VALID) !== 0
  );
}

export function runtimeHintInitialCapacity(runtimeHint: number): number | undefined {
  if (!isRuntimeHintAnalyzed(runtimeHint)) return undefined;
  const capacity = runtimeHint & RUNTIME_HINT_INITIAL_CAPACITY_MASK;
  return capacity === 0 ? undefined : Math.max(2, capacity);
}

export function runtimeHintHasCapability(runtimeHint: number, capability: number): boolean {
  return !isRuntimeHintAnalyzed(runtimeHint) || (runtimeHint & capability) !== 0;
}

export function decodeRuntimeHint(runtimeHint: number): DecodedRuntimeHint {
  if (!isRuntimeHintAnalyzed(runtimeHint)) {
    return {
      analyzed: false,
      initialCapacity: undefined,
      capabilities: RUNTIME_HINT_FULL_CAPABILITIES,
    };
  }

  return {
    analyzed: true,
    initialCapacity: runtimeHintInitialCapacity(runtimeHint),
    capabilities: runtimeHint & RUNTIME_HINT_CAPABILITIES_MASK,
  };
}
