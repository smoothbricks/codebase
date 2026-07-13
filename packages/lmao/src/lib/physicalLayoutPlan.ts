import type { Nanoseconds } from '@smoothbricks/arrow-builder';
import {
  getResultWriterClass,
  getTagWriterClass,
  type ResultWriterConstructor,
  type TagWriter,
} from './codegen/fixedPositionWriterGenerator.js';
import { createSpanLoggerClass, type SpanLoggerImpl } from './codegen/spanLoggerGenerator.js';
import type { RemapDescriptor } from './logBinding.js';
import type { OpMetadata } from './opContext/opTypes.js';
import type { OpContext } from './opContext/types.js';
import {
  isRuntimeHintAnalyzed,
  RUNTIME_HINT_CAPABILITIES_MASK,
  RUNTIME_HINT_FF,
  RUNTIME_HINT_FULL_CAPABILITIES,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_SCOPE,
  RUNTIME_HINT_TAG,
  runtimeHintInitialCapacity,
} from './runtimeHint.js';
import type { LogSchema } from './schema/LogSchema.js';
import { ENTRY_TYPE_SPAN_START } from './schema/systemSchema.js';
import type { SpanBufferConstructor } from './spanBuffer.js';
import type { AnySpanBuffer, SpanBuffer } from './types.js';
import type { SpanContextClass } from './spanContext.js';
import type { ITraceRoot, TimestampAppendPrimitive } from './traceRoot.js';
import { getVocabularyGeneration, type VocabularyGeneration } from './vocabularyRegistry.js';
import { createWasmLayoutTemplate, type WasmLayoutTemplate } from './wasm/wasmPhysicalLayout.js';

export const PHYSICAL_LAYOUT_VERSION = 1;

/** Concrete backends may bind the same physical schema to distinct immutable plans. */
export type PhysicalBackendKind = 'strategy-selected' | 'js-heap' | 'wasm';


export interface PhysicalClock {
  readonly kind: 'trace-root';
  now(buffer: AnySpanBuffer): Nanoseconds;
}

export interface PhysicalAppenders {
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void;
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void;
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number;
}

export interface PhysicalLayoutPlan<
  T extends LogSchema = LogSchema,
  Ctx extends OpContext<T> = OpContext<T>,
> {
  readonly version: typeof PHYSICAL_LAYOUT_VERSION;
  readonly backendKind: PhysicalBackendKind;
  readonly schema: T;
  readonly runtimeHint: number;
  readonly capabilities: number;
  /** Fixed transformer tier, or undefined to retain adaptive strategy capacity. */
  readonly capacityTier: number | undefined;
  /** Canonical user-context key layout used by the generated context constructor. */
  readonly contextLayoutKey: string;
  /** Exact constructor selected at startup for this plan's capability/layout signature. */
  readonly SpanContextClass: SpanContextClass<Ctx>;
  readonly SpanBufferClass: SpanBufferConstructor<T>;
  readonly SpanLoggerClass: new (
    buffer: AnySpanBuffer,
    traceRoot: ITraceRoot,
    appendLogEntry: TimestampAppendPrimitive,
  ) => SpanLoggerImpl<T>;
  readonly TagWriterClass: new (buffer: AnySpanBuffer) => TagWriter<T>;
  readonly ResultWriterClass: ResultWriterConstructor<T>;
  readonly clock: PhysicalClock;
  readonly appenders: PhysicalAppenders;
  /** Immutable global vocabulary generation used by dense row identities in this plan. */
  readonly vocabularyGeneration: VocabularyGeneration;
  /** Reserved immutable ownership slot; buffer pooling is a later task. */
  readonly poolRef: null;
  readonly remapDescriptor: RemapDescriptor | null;
  readonly newCtx0: (parent: object) => object;
  readonly newCtx1: (parent: object, overrides: object) => object;
  readonly newSpanLogger: ((buffer: SpanBuffer<T>) => SpanLoggerImpl<T>) | undefined;
  readonly newTagWriter: ((buffer: AnySpanBuffer) => TagWriter<T>) | undefined;
  readonly wasmLayout: WasmLayoutTemplate;
}

/** Fully resolved immutable operands for one operation callsite. */
export interface CallsitePlan<
  T extends LogSchema = LogSchema,
  Ctx extends OpContext<T> = OpContext<T>,
> extends PhysicalLayoutPlan<T, Ctx> {
  readonly metadata: OpMetadata;
}

export function sealCallsitePlan<T extends LogSchema, Ctx extends OpContext<T>>(
  physicalLayoutPlan: PhysicalLayoutPlan<T, Ctx>,
  metadata: OpMetadata,
): CallsitePlan<T, Ctx> {
  return Object.freeze({ ...physicalLayoutPlan, metadata });
}

const newCtx0 = (parent: object): object => parent;

function createNewCtx1(contextLayoutKey: string): (parent: object, overrides: object) => object {
  const keys = contextLayoutKey === '' ? [] : contextLayoutKey.split('\u0000');
  return (parent: object, overrides: object): object => {
    const context: Record<string, unknown> = {};
    for (const key of keys) {
      context[key] = Object.hasOwn(overrides, key) ? Reflect.get(overrides, key) : Reflect.get(parent, key);
    }
    return context;
  };
}

const TRACE_ROOT_CLOCK: PhysicalClock = Object.freeze({
  kind: 'trace-root' as const,
  now(buffer: AnySpanBuffer): Nanoseconds {
    const traceRoot = buffer._traceRoot;
    return traceRoot._timestampNow(traceRoot);
  },
});

const TRACE_ROOT_APPENDERS: PhysicalAppenders = Object.freeze({
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    if (typeof name === 'number') {
      const traceRoot = buffer._traceRoot;
      buffer.timestamp[0] = traceRoot._timestampNow(traceRoot);
      buffer.entry_type[0] = ENTRY_TYPE_SPAN_START;
      buffer._logHeaders[0] = ((name << 8) | ENTRY_TYPE_SPAN_START) >>> 0;
      if (buffer.message_nulls) buffer.message_nulls[0] |= 1;
      return;
    }
    const traceRoot = buffer._traceRoot;
    traceRoot._writeSpanStart(traceRoot, buffer, name);
  },
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    const traceRoot = buffer._traceRoot;
    traceRoot._writeSpanEnd(traceRoot, buffer, entryType);
  },
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
    const traceRoot = buffer._traceRoot;
    return traceRoot._appendLogEntry(traceRoot, buffer, entryType);
  },
});

const basePlans = new WeakMap<LogSchema, Map<string, object>>();
const remappedPlans = new WeakMap<object, WeakMap<RemapDescriptor, object>>();


function createBasePlan<T extends LogSchema, Ctx extends OpContext<T>>(
  SpanBufferClass: SpanBufferConstructor<T>,
  runtimeHint: number,
  backendKind: PhysicalBackendKind,
  SpanContextClass: SpanContextClass<Ctx>,
  contextLayoutKey: string,
  vocabularyGeneration: VocabularyGeneration,
): PhysicalLayoutPlan<T, Ctx> {
  const schema = SpanBufferClass.schema;
  const SpanLoggerClass = createSpanLoggerClass(schema);
  const TagWriterClass = getTagWriterClass(schema);
  const ResultWriterClass = getResultWriterClass(schema);
  const capabilities = isRuntimeHintAnalyzed(runtimeHint)
    ? runtimeHint & RUNTIME_HINT_CAPABILITIES_MASK
    : RUNTIME_HINT_FULL_CAPABILITIES;
  const needsLogger = (capabilities & (RUNTIME_HINT_LOG | RUNTIME_HINT_FF | RUNTIME_HINT_SCOPE)) !== 0;
  const needsTag = (capabilities & RUNTIME_HINT_TAG) !== 0;
  const newSpanLogger = needsLogger
    ? (buffer: SpanBuffer<T>): SpanLoggerImpl<T> => {
        const traceRoot = buffer._traceRoot;
        return new SpanLoggerClass(buffer, traceRoot, traceRoot._appendLogEntry);
      }
    : undefined;
  const newTagWriter = needsTag ? (buffer: AnySpanBuffer): TagWriter<T> => new TagWriterClass(buffer) : undefined;
  const wasmLayout = createWasmLayoutTemplate(schema);

  return Object.freeze({
    version: PHYSICAL_LAYOUT_VERSION,
    backendKind,
    schema,
    runtimeHint,
    capabilities,
    contextLayoutKey,
    SpanContextClass,
    capacityTier: runtimeHintInitialCapacity(runtimeHint),
    SpanBufferClass,
    SpanLoggerClass,
    TagWriterClass,
    ResultWriterClass,
    clock: TRACE_ROOT_CLOCK,
    appenders: TRACE_ROOT_APPENDERS,
    vocabularyGeneration,
    poolRef: null,
    remapDescriptor: null,
    newCtx0,
    newCtx1: createNewCtx1(contextLayoutKey),
    newSpanLogger,
    newTagWriter,
    wasmLayout,
  });
}

export function getPhysicalLayoutPlan<T extends LogSchema, Ctx extends OpContext<T>>(
  SpanBufferClass: SpanBufferConstructor<T>,
  runtimeHint: number,
  SpanContextClass: SpanContextClass<Ctx>,
  remapDescriptor?: RemapDescriptor,
  backendKind: PhysicalBackendKind = 'strategy-selected',
  contextLayoutKey = '',
): PhysicalLayoutPlan<T, Ctx> {
  const schema = SpanBufferClass.schema;
  let byKey = basePlans.get(schema);
  if (!byKey) {
    byKey = new Map();
    basePlans.set(schema, byKey);
  }

  const vocabularyGeneration = getVocabularyGeneration();
  const key = `${PHYSICAL_LAYOUT_VERSION}:${backendKind}:${runtimeHint}:${contextLayoutKey}:${vocabularyGeneration.generation}`;
  let base = byKey.get(key) as PhysicalLayoutPlan<T, Ctx> | undefined;
  if (!base) {
    base = createBasePlan(
      SpanBufferClass,
      runtimeHint,
      backendKind,
      SpanContextClass,
      contextLayoutKey,
      vocabularyGeneration,
    );
    byKey.set(key, base);
  } else if (base.SpanBufferClass !== SpanBufferClass) {
    throw new TypeError('Physical layout cache key resolved to a different SpanBuffer constructor');
  } else if (base.SpanContextClass !== SpanContextClass) {
    throw new TypeError('Physical layout cache key resolved to a different SpanContext constructor');
  }
  if (!remapDescriptor) return base;

  let bindings = remappedPlans.get(base);
  if (!bindings) {
    bindings = new WeakMap();
    remappedPlans.set(base, bindings);
  }
  const cached = bindings.get(remapDescriptor) as PhysicalLayoutPlan<T, Ctx> | undefined;
  if (cached) return cached;

  const bound: PhysicalLayoutPlan<T, Ctx> = Object.freeze({
    ...base,
    remapDescriptor,
  });
  bindings.set(remapDescriptor, bound);
  return bound;
}
