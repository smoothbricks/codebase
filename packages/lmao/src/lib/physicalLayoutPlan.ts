import type { Nanoseconds } from '@smoothbricks/arrow-builder';
import {
  getResultWriterClass,
  getTagWriterClass,
  type ResultWriterConstructor,
  type TagWriter,
  type TagWriterConstructor,
  type WriterState,
} from './codegen/fixedPositionWriterGenerator.js';
import {
  createSpanLoggerClass,
  type SpanLoggerConstructor,
  type SpanLoggerImpl,
} from './codegen/spanLoggerGenerator.js';
import { resolveEnumLookupDescriptor, type SchemaEnumLookupDescriptor } from './enumMetadata.js';
import type { RemapDescriptor } from './logBinding.js';
import type { OpMetadata } from './opContext/opTypes.js';
import type { OpContext } from './opContext/types.js';
import { MAX_PACKED_MESSAGE_DENSE_INDEX } from './resolveMessage.js';
import {
  isRuntimeHintAnalyzed,
  type MessageLayoutFamily,
  type MessagePhysicalLayout,
  RUNTIME_HINT_CAPABILITIES_MASK,
  RUNTIME_HINT_FF,
  RUNTIME_HINT_FULL_CAPABILITIES,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_SCOPE,
  RUNTIME_HINT_TAG,
  runtimeHintInitialCapacity,
  runtimeHintMessageLayoutFamily,
  runtimeHintMessagePhysicalLayout,
} from './runtimeHint.js';
import type { LogSchema } from './schema/LogSchema.js';
import { getSpanBufferClass, type SpanBufferConstructor } from './spanBuffer.js';
import type { SpanContextClass } from './spanContext.js';
import type { AnySpanBuffer } from './types.js';
import { getVocabularyGeneration, type VocabularyGeneration } from './vocabularyRegistry.js';
import { createWasmLayoutTemplate, type WasmLayoutTemplate } from './wasm/wasmPhysicalLayout.js';

export const PHYSICAL_LAYOUT_VERSION = 1;

/** Canonical schema-ordered eager column selection for generated storage and cache identity. */
export interface EagerColumnDescriptor {
  readonly names: readonly string[];
  readonly words: readonly number[];
  readonly key: string;
}

const EMPTY_EAGER_COLUMNS: EagerColumnDescriptor = Object.freeze({
  names: Object.freeze([]),
  words: Object.freeze([]),
  key: '',
});

export function resolveEagerColumns(schema: LogSchema, requestedNames: readonly string[] = []): EagerColumnDescriptor {
  if (requestedNames.length === 0) return EMPTY_EAGER_COLUMNS;
  const requested = new Set(requestedNames);
  const names: string[] = [];
  const words = new Array<number>(Math.ceil(schema._columnNames.length / 32)).fill(0);
  for (let columnIndex = 0; columnIndex < schema._columnNames.length; columnIndex++) {
    const name = schema._columnNames[columnIndex];
    if (!requested.delete(name)) continue;
    names.push(name);
    const wordIndex = columnIndex >>> 5;
    words[wordIndex] = (words[wordIndex] | (1 << (columnIndex & 31))) >>> 0;
  }
  if (requested.size !== 0) {
    throw new TypeError(`Unknown eager column${requested.size === 1 ? '' : 's'}: ${[...requested].join(', ')}`);
  }
  while (words.length !== 0 && words[words.length - 1] === 0) words.pop();
  const frozenWords = Object.freeze(words);
  return Object.freeze({
    names: Object.freeze(names),
    words: frozenWords,
    key: frozenWords.map((word) => word.toString(16).padStart(8, '0')).join(''),
  });
}

export interface PhysicalClock {
  readonly kind: 'trace-root';
  now(buffer: AnySpanBuffer): Nanoseconds;
}

export interface PhysicalLayoutPlan<T extends LogSchema = LogSchema, Ctx extends OpContext<T> = OpContext<T>> {
  readonly version: typeof PHYSICAL_LAYOUT_VERSION;
  readonly schema: T;
  readonly runtimeHint: number;
  readonly capabilities: number;
  readonly messageLayoutFamily: MessageLayoutFamily;
  readonly messagePhysicalLayout: MessagePhysicalLayout;
  readonly eagerColumns: EagerColumnDescriptor;
  /** Immutable schema-order enum metadata shared by every plan and generated writer. */
  readonly enumLookup: SchemaEnumLookupDescriptor;
  /** Fixed transformer tier, or undefined to retain adaptive strategy capacity. */
  readonly capacityTier: number | undefined;
  /** Canonical user-context key layout used by the generated context constructor. */
  readonly contextLayoutKey: string;
  /** Exact constructor selected at startup for this plan's capability/layout signature. */
  readonly SpanContextClass: SpanContextClass<Ctx>;
  readonly SpanBufferClass: SpanBufferConstructor<T>;
  readonly SpanLoggerClass: SpanLoggerConstructor<T> | undefined;
  readonly TagWriterClass: TagWriterConstructor<T> | undefined;
  readonly ResultWriterClass: ResultWriterConstructor;
  readonly clock: PhysicalClock;
  /** Immutable global vocabulary generation used by dense row identities in this plan. */
  readonly vocabularyGeneration: VocabularyGeneration;
  /** Current-mode local ID minus one maps to a global vocabulary dense index. */
  readonly localMessageDictionary: readonly number[];
  /** Allocation-free hot lookup from global dense identity to 1-based local ID. */
  readonly encodeLocalMessage: (globalDenseIndex: number) => number;
  /** Reserved immutable ownership slot; buffer pooling is a later task. */
  readonly poolRef: null;
  readonly remapDescriptor: RemapDescriptor | null;
  readonly newCtx0: (parent: object) => object;
  readonly newCtx1: (parent: object, overrides: object) => object;
  readonly newSpanLogger: ((state: WriterState) => SpanLoggerImpl<T>) | undefined;
  readonly newTagWriter: ((state: WriterState) => TagWriter<T>) | undefined;
  readonly wasmLayout: WasmLayoutTemplate;
}

/** Fully resolved immutable operands for one operation callsite. */
export interface CallsitePlan<T extends LogSchema = LogSchema, Ctx extends OpContext<T> = OpContext<T>>
  extends PhysicalLayoutPlan<T, Ctx> {
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

const EMPTY_LOCAL_MESSAGE_DICTIONARY: readonly number[] = Object.freeze([]);
const NO_LOCAL_MESSAGE = (_globalDenseIndex: number): number => 0;

function createLocalMessageEncoder(dictionary: readonly number[]): (globalDenseIndex: number) => number {
  if (dictionary.length === 0) return NO_LOCAL_MESSAGE;
  const localByDense = new Map<number, number>();
  let localId = 1;
  for (const globalDenseIndex of dictionary) localByDense.set(globalDenseIndex, localId++);
  return (globalDenseIndex: number): number => localByDense.get(globalDenseIndex) ?? 0;
}

const basePlans = new WeakMap<LogSchema, WeakMap<object, Map<string, object>>>();
const remappedPlans = new WeakMap<object, WeakMap<RemapDescriptor, object>>();

function createBasePlan<T extends LogSchema, Ctx extends OpContext<T>>(
  SpanBufferClass: SpanBufferConstructor<T>,
  runtimeHint: number,
  SpanContextClass: SpanContextClass<Ctx>,
  contextLayoutKey: string,
  vocabularyGeneration: VocabularyGeneration,
  eagerColumns: EagerColumnDescriptor,
  localMessageDictionary: readonly number[],
  messagePhysicalLayout: MessagePhysicalLayout,
): PhysicalLayoutPlan<T, Ctx> {
  const schema = SpanBufferClass.schema;
  const enumLookup = resolveEnumLookupDescriptor(schema);
  const messageLayoutFamily = runtimeHintMessageLayoutFamily(runtimeHint);
  const PlannedSpanBufferClass = getSpanBufferClass(schema, messageLayoutFamily, messagePhysicalLayout, eagerColumns);
  const capabilities = isRuntimeHintAnalyzed(runtimeHint)
    ? runtimeHint & RUNTIME_HINT_CAPABILITIES_MASK
    : RUNTIME_HINT_FULL_CAPABILITIES;
  const needsLogger = (capabilities & (RUNTIME_HINT_LOG | RUNTIME_HINT_FF | RUNTIME_HINT_SCOPE)) !== 0;
  const SpanLoggerClass = needsLogger
    ? createSpanLoggerClass(schema, messageLayoutFamily, messagePhysicalLayout, eagerColumns.names, enumLookup)
    : undefined;
  const ResultWriterClass = getResultWriterClass(
    schema,
    messageLayoutFamily,
    messagePhysicalLayout,
    eagerColumns.names,
    enumLookup,
  );
  const needsTag = (capabilities & RUNTIME_HINT_TAG) !== 0;
  const TagWriterClass = needsTag ? getTagWriterClass(schema, eagerColumns.names, enumLookup) : undefined;
  const newSpanLogger =
    SpanLoggerClass === undefined ? undefined : (state: WriterState): SpanLoggerImpl<T> => new SpanLoggerClass(state);
  const newTagWriter =
    TagWriterClass === undefined ? undefined : (state: WriterState): TagWriter<T> => new TagWriterClass(state);
  const wasmLayout = createWasmLayoutTemplate(schema, messageLayoutFamily, messagePhysicalLayout, eagerColumns);

  return Object.freeze({
    version: PHYSICAL_LAYOUT_VERSION,
    schema,
    runtimeHint,
    capabilities,
    messageLayoutFamily,
    messagePhysicalLayout,
    eagerColumns,
    enumLookup,
    encodeLocalMessage: createLocalMessageEncoder(localMessageDictionary),
    contextLayoutKey,
    SpanContextClass,
    capacityTier: runtimeHintInitialCapacity(runtimeHint),
    SpanBufferClass: PlannedSpanBufferClass,
    SpanLoggerClass,
    TagWriterClass,
    ResultWriterClass,
    clock: TRACE_ROOT_CLOCK,
    localMessageDictionary,
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
  contextLayoutKey = '',
  eagerColumnNames: readonly string[] = [],
  localMessageDictionary: readonly number[] = Object.freeze([]),
): PhysicalLayoutPlan<T, Ctx> {
  const schema = SpanBufferClass.schema;
  const eagerColumns = resolveEagerColumns(schema, eagerColumnNames);
  const resolvedLocalMessageDictionary =
    localMessageDictionary.length === 0
      ? EMPTY_LOCAL_MESSAGE_DICTIONARY
      : Object.isFrozen(localMessageDictionary)
        ? localMessageDictionary
        : Object.freeze([...localMessageDictionary]);
  let byContextClass = basePlans.get(schema);
  if (!byContextClass) {
    byContextClass = new WeakMap();
    basePlans.set(schema, byContextClass);
  }
  let byKey = byContextClass.get(SpanContextClass);
  if (!byKey) {
    byKey = new Map();
    byContextClass.set(SpanContextClass, byKey);
  }

  const vocabularyGeneration = getVocabularyGeneration();
  const messageLayoutFamily = runtimeHintMessageLayoutFamily(runtimeHint);
  const requestedPhysicalLayout = runtimeHintMessagePhysicalLayout(runtimeHint);
  const messagePhysicalLayout =
    requestedPhysicalLayout === 'packed' && vocabularyGeneration.ids.length - 1 > MAX_PACKED_MESSAGE_DENSE_INDEX
      ? 'specialized'
      : requestedPhysicalLayout;
  const key = `${PHYSICAL_LAYOUT_VERSION}:${runtimeHint}:${messageLayoutFamily}:${messagePhysicalLayout}:${contextLayoutKey}:${vocabularyGeneration.generation}:${eagerColumns.key}:${resolvedLocalMessageDictionary.join(',')}`;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- cache keys encode schema/context/runtime identity for this erased store.
  let base = byKey.get(key) as PhysicalLayoutPlan<T, Ctx> | undefined;
  if (!base) {
    base = createBasePlan(
      SpanBufferClass,
      runtimeHint,
      SpanContextClass,
      contextLayoutKey,
      vocabularyGeneration,
      eagerColumns,
      resolvedLocalMessageDictionary,
      messagePhysicalLayout,
    );
    byKey.set(key, base);
  } else if (
    base.SpanBufferClass.messageLayoutFamily !== messageLayoutFamily ||
    base.SpanBufferClass.messagePhysicalLayout !== messagePhysicalLayout
  ) {
    throw new TypeError('Physical layout cache key resolved to a different message layout');
  } else if (base.SpanContextClass !== SpanContextClass) {
    throw new TypeError('Physical layout cache key resolved to a different SpanContext constructor');
  }
  if (!remapDescriptor) return base;

  let bindings = remappedPlans.get(base);
  if (!bindings) {
    bindings = new WeakMap();
    remappedPlans.set(base, bindings);
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- remapped plans are stored under the exact base+descriptor pair created below.
  const cached = bindings.get(remapDescriptor) as PhysicalLayoutPlan<T, Ctx> | undefined;
  if (cached) return cached;

  const bound: PhysicalLayoutPlan<T, Ctx> = Object.freeze({
    ...base,
    remapDescriptor,
  });
  bindings.set(remapDescriptor, bound);
  return bound;
}
