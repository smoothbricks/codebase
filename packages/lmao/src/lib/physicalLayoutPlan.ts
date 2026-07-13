import type { Nanoseconds } from '@smoothbricks/arrow-builder';
import {
  getResultWriterClass,
  getTagWriterClass,
  type ResultWriter,
  type TagWriter,
} from './codegen/fixedPositionWriterGenerator.js';
import { createSpanLoggerClass, type SpanLoggerImpl } from './codegen/spanLoggerGenerator.js';
import type { RemapDescriptor } from './logBinding.js';
import {
  isRuntimeHintAnalyzed,
  RUNTIME_HINT_CAPABILITIES_MASK,
  RUNTIME_HINT_FULL_CAPABILITIES,
  runtimeHintInitialCapacity,
} from './runtimeHint.js';
import type { LogSchema } from './schema/LogSchema.js';
import type { SpanBufferConstructor } from './spanBuffer.js';
import type { AnySpanBuffer, SpanBuffer } from './types.js';

export const PHYSICAL_LAYOUT_VERSION = 1;

/** Concrete backends may bind the same physical schema to distinct immutable plans. */
export type PhysicalBackendKind = 'strategy-selected' | 'js-heap' | 'wasm';

export interface PhysicalClock {
  readonly kind: 'trace-root';
  now(buffer: AnySpanBuffer): Nanoseconds;
}

export interface PhysicalAppenders {
  writeSpanStart(buffer: AnySpanBuffer, name: string): void;
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void;
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number;
}

export interface PhysicalLayoutPlan<T extends LogSchema = LogSchema> {
  readonly version: typeof PHYSICAL_LAYOUT_VERSION;
  readonly backendKind: PhysicalBackendKind;
  readonly schema: T;
  readonly runtimeHint: number;
  readonly capabilities: number;
  /** Fixed transformer tier, or undefined to retain adaptive strategy capacity. */
  readonly capacityTier: number | undefined;
  readonly SpanBufferClass: SpanBufferConstructor<T>;
  readonly SpanLoggerClass: new (buffer: AnySpanBuffer) => SpanLoggerImpl<T>;
  readonly TagWriterClass: new (buffer: AnySpanBuffer) => TagWriter<T>;
  readonly ResultWriterClass: new <R = unknown, E = unknown>(
    buffer: AnySpanBuffer,
    resultOrError: R | E,
    isError: boolean,
  ) => ResultWriter<T, R, E>;
  readonly clock: PhysicalClock;
  readonly appenders: PhysicalAppenders;
  /** Vocabulary binding is a later startup phase; this stable slot owns no binding yet. */
  readonly vocabularyBindingSlot: null;
  /** Reserved immutable ownership slot; buffer pooling is a later task. */
  readonly poolRef: null;
  readonly remapDescriptor: RemapDescriptor | null;
  createSpanLogger(buffer: SpanBuffer<T>): SpanLoggerImpl<T>;
  createTagWriter(buffer: AnySpanBuffer): TagWriter<T>;
}

const TRACE_ROOT_CLOCK: PhysicalClock = Object.freeze({
  kind: 'trace-root' as const,
  now(buffer: AnySpanBuffer): Nanoseconds {
    return buffer._traceRoot.getTimestampNanos();
  },
});

const TRACE_ROOT_APPENDERS: PhysicalAppenders = Object.freeze({
  writeSpanStart(buffer: AnySpanBuffer, name: string): void {
    buffer._traceRoot.writeSpanStart(buffer, name);
  },
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    buffer._traceRoot.writeSpanEnd(buffer, entryType);
  },
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
    return buffer._traceRoot.writeLogEntry(buffer, entryType);
  },
});

const basePlans = new WeakMap<LogSchema, Map<string, object>>();
const remappedPlans = new WeakMap<object, WeakMap<RemapDescriptor, object>>();

function createBasePlan<T extends LogSchema>(
  SpanBufferClass: SpanBufferConstructor<T>,
  runtimeHint: number,
  backendKind: PhysicalBackendKind,
): PhysicalLayoutPlan<T> {
  const schema = SpanBufferClass.schema;
  const SpanLoggerClass = createSpanLoggerClass(schema);
  const TagWriterClass = getTagWriterClass(schema);
  const ResultWriterClass = getResultWriterClass(schema);

  return Object.freeze({
    version: PHYSICAL_LAYOUT_VERSION,
    backendKind,
    schema,
    runtimeHint,
    capabilities: isRuntimeHintAnalyzed(runtimeHint)
      ? runtimeHint & RUNTIME_HINT_CAPABILITIES_MASK
      : RUNTIME_HINT_FULL_CAPABILITIES,
    capacityTier: runtimeHintInitialCapacity(runtimeHint),
    SpanBufferClass,
    SpanLoggerClass,
    TagWriterClass,
    ResultWriterClass,
    clock: TRACE_ROOT_CLOCK,
    appenders: TRACE_ROOT_APPENDERS,
    vocabularyBindingSlot: null,
    poolRef: null,
    remapDescriptor: null,
    createSpanLogger(buffer: SpanBuffer<T>): SpanLoggerImpl<T> {
      return new SpanLoggerClass(buffer);
    },
    createTagWriter(buffer: AnySpanBuffer): TagWriter<T> {
      return new TagWriterClass(buffer);
    },
  });
}

export function getPhysicalLayoutPlan<T extends LogSchema>(
  SpanBufferClass: SpanBufferConstructor<T>,
  runtimeHint = 0,
  remapDescriptor?: RemapDescriptor,
  backendKind: PhysicalBackendKind = 'strategy-selected',
): PhysicalLayoutPlan<T> {
  const schema = SpanBufferClass.schema;
  let byKey = basePlans.get(schema);
  if (!byKey) {
    byKey = new Map();
    basePlans.set(schema, byKey);
  }

  const key = `${PHYSICAL_LAYOUT_VERSION}:${backendKind}:${runtimeHint}`;
  let base = byKey.get(key) as PhysicalLayoutPlan<T> | undefined;
  if (!base) {
    base = createBasePlan(SpanBufferClass, runtimeHint, backendKind);
    byKey.set(key, base);
  } else if (base.SpanBufferClass !== SpanBufferClass) {
    throw new TypeError('Physical layout cache key resolved to a different SpanBuffer constructor');
  }

  if (!remapDescriptor) return base;

  let bindings = remappedPlans.get(base);
  if (!bindings) {
    bindings = new WeakMap();
    remappedPlans.set(base, bindings);
  }
  const cached = bindings.get(remapDescriptor) as PhysicalLayoutPlan<T> | undefined;
  if (cached) return cached;

  const bound: PhysicalLayoutPlan<T> = Object.freeze({
    ...base,
    remapDescriptor,
  });
  bindings.set(remapDescriptor, bound);
  return bound;
}
