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
export const RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY = 0x01000000;
export const RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY = 0x02000000;
export const RUNTIME_HINT_MESSAGE_LAYOUT_MIXED = 0x03000000;
export const RUNTIME_HINT_MESSAGE_LAYOUT_MASK = 0x03000000;
export const RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED = 0x04000000;
export const RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED = 0x08000000;
export const RUNTIME_HINT_MESSAGE_PHYSICAL_MASK = 0x0c000000;
export const RUNTIME_HINT_RESERVED_MASK = 0xf0000000;

export const RUNTIME_HINT_FULL_CAPABILITIES = RUNTIME_HINT_CAPABILITIES_MASK;
export type MessageLayoutFamily = 'static-only' | 'mixed' | 'dynamic-only';
export type MessagePhysicalLayout = 'current' | 'specialized' | 'packed';

export interface DecodedRuntimeHint {
  readonly analyzed: boolean;
  readonly initialCapacity: number | undefined;
  readonly capabilities: number;
  readonly messageLayoutFamily: MessageLayoutFamily;
  readonly messagePhysicalLayout: MessagePhysicalLayout;
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
  return capacity < 2 ? undefined : capacity;
}

export function runtimeHintHasCapability(runtimeHint: number, capability: number): boolean {
  return !isRuntimeHintAnalyzed(runtimeHint) || (runtimeHint & capability) !== 0;
}

export function runtimeHintMessageLayoutFamily(runtimeHint: number): MessageLayoutFamily {
  if (!isRuntimeHintAnalyzed(runtimeHint)) return 'mixed';

  switch (runtimeHint & RUNTIME_HINT_MESSAGE_LAYOUT_MASK) {
    case RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY:
      return 'static-only';
    case RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY:
      return 'dynamic-only';
    case RUNTIME_HINT_MESSAGE_LAYOUT_MIXED:
      return 'mixed';
    default:
      return 'mixed';
  }
}

export function runtimeHintMessagePhysicalLayout(runtimeHint: number): MessagePhysicalLayout {
  if (!isRuntimeHintAnalyzed(runtimeHint)) return 'current';
  switch (runtimeHint & RUNTIME_HINT_MESSAGE_PHYSICAL_MASK) {
    case RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED:
      return 'packed';
    case RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED:
      return 'specialized';
    default:
      return 'current';
  }
}

export function decodeRuntimeHint(runtimeHint: number): DecodedRuntimeHint {
  if (!isRuntimeHintAnalyzed(runtimeHint)) {
    return {
      analyzed: false,
      initialCapacity: undefined,
      capabilities: RUNTIME_HINT_FULL_CAPABILITIES,
      messageLayoutFamily: 'mixed',
      messagePhysicalLayout: 'current',
    };
  }

  return {
    analyzed: true,
    initialCapacity: runtimeHintInitialCapacity(runtimeHint),
    capabilities: runtimeHint & RUNTIME_HINT_CAPABILITIES_MASK,
    messageLayoutFamily: runtimeHintMessageLayoutFamily(runtimeHint),
    messagePhysicalLayout: runtimeHintMessagePhysicalLayout(runtimeHint),
  };
}
