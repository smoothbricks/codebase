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
import { decodeVocabularyMessage, MAX_PACKED_MESSAGE_DENSE_INDEX } from './resolveMessage.js';
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
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from './schema/systemSchema.js';
import { getSpanBufferClass, type SpanBufferConstructor } from './spanBuffer.js';
import type { SpanContextClass } from './spanContext.js';
import type { TimestampAppendPrimitive } from './traceRoot.js';
import type { AnySpanBuffer } from './types.js';
import { getVocabularyGeneration, type VocabularyGeneration } from './vocabularyRegistry.js';
import { createWasmLayoutTemplate, type WasmLayoutTemplate } from './wasm/wasmPhysicalLayout.js';

export const PHYSICAL_LAYOUT_VERSION = 1;

/** Concrete backends may bind the same physical schema to distinct immutable plans. */
export type PhysicalBackendKind = 'strategy-selected' | 'js-heap' | 'wasm';

/** Canonical schema-ordered eager column selection for generated storage and cache identity. */
export interface EagerColumnDescriptor {
  readonly names: readonly string[];
  readonly words: readonly number[];
  readonly key: string;
}

export interface ArrowExposurePlan {
  readonly version: 1;
  readonly primitiveStorage: 'borrowed-chunks' | 'owned-copy';
  readonly dictionaryStorage: 'pinned-generation-prefix';
  readonly entryTypeStorage: 'borrowed-u8' | 'derived-row-headers' | 'owned-copy';
  readonly messageIdentityStorage: 'local-u16' | 'global-u32' | 'packed-row-headers';
}

const JS_ARROW_EXPOSURE_BY_LAYOUT: Readonly<Record<MessagePhysicalLayout, ArrowExposurePlan>> = Object.freeze({
  current: Object.freeze({
    version: 1,
    primitiveStorage: 'borrowed-chunks',
    dictionaryStorage: 'pinned-generation-prefix',
    entryTypeStorage: 'borrowed-u8',
    messageIdentityStorage: 'local-u16',
  }),
  specialized: Object.freeze({
    version: 1,
    primitiveStorage: 'borrowed-chunks',
    dictionaryStorage: 'pinned-generation-prefix',
    entryTypeStorage: 'borrowed-u8',
    messageIdentityStorage: 'global-u32',
  }),
  packed: Object.freeze({
    version: 1,
    primitiveStorage: 'borrowed-chunks',
    dictionaryStorage: 'pinned-generation-prefix',
    entryTypeStorage: 'derived-row-headers',
    messageIdentityStorage: 'packed-row-headers',
  }),
});
const WASM_ARROW_EXPOSURE_BY_LAYOUT: Readonly<Record<MessagePhysicalLayout, ArrowExposurePlan>> = Object.freeze({
  current: Object.freeze({
    version: 1,
    primitiveStorage: 'owned-copy',
    dictionaryStorage: 'pinned-generation-prefix',
    entryTypeStorage: 'owned-copy',
    messageIdentityStorage: 'local-u16',
  }),
  specialized: Object.freeze({
    version: 1,
    primitiveStorage: 'owned-copy',
    dictionaryStorage: 'pinned-generation-prefix',
    entryTypeStorage: 'owned-copy',
    messageIdentityStorage: 'global-u32',
  }),
  packed: Object.freeze({
    version: 1,
    primitiveStorage: 'owned-copy',
    dictionaryStorage: 'pinned-generation-prefix',
    entryTypeStorage: 'derived-row-headers',
    messageIdentityStorage: 'packed-row-headers',
  }),
});

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

export interface PhysicalAppenders {
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void;
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void;
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number;
}

export interface PhysicalLayoutPlan<T extends LogSchema = LogSchema, Ctx extends OpContext<T> = OpContext<T>> {
  readonly version: typeof PHYSICAL_LAYOUT_VERSION;
  readonly backendKind: PhysicalBackendKind;
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
  readonly SpanLoggerClass: SpanLoggerConstructor<T>;
  readonly TagWriterClass: TagWriterConstructor<T> | undefined;
  readonly ResultWriterClass: ResultWriterConstructor;
  readonly clock: PhysicalClock;
  readonly appenders: PhysicalAppenders;
  readonly appendLogEntry: TimestampAppendPrimitive;
  /** Immutable global vocabulary generation used by dense row identities in this plan. */
  readonly vocabularyGeneration: VocabularyGeneration;
  /** Startup-fixed ownership policy used by leased Arrow conversion. */
  /** Current-mode local ID minus one maps to a global vocabulary dense index. */
  readonly localMessageDictionary: readonly number[];
  readonly arrowExposure: ArrowExposurePlan;
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

const SPLIT_APPEND_LOG_ENTRY: TimestampAppendPrimitive = (traceRoot, buffer, entryType) =>
  traceRoot._appendLogEntry(traceRoot, buffer, entryType);

const PACKED_APPEND_LOG_ENTRY: TimestampAppendPrimitive = (traceRoot, buffer, entryType) => {
  const row = buffer._writeIndex;
  const headers = buffer._rowHeaders;
  if (headers === undefined) throw new TypeError('Packed layout is missing row headers');
  buffer.timestamp[row] = traceRoot._timestampNow(traceRoot);
  headers[row] = entryType;
  buffer._writeIndex = row + 1;
  return row;
};

const CURRENT_BASE_APPENDERS = {
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    const traceRoot = buffer._traceRoot;
    traceRoot._writeSpanEnd(traceRoot, buffer, entryType);
  },
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
    return SPLIT_APPEND_LOG_ENTRY(buffer._traceRoot, buffer, entryType);
  },
};

function initializeCurrentSpan(buffer: AnySpanBuffer): Uint8Array {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('Current layout is missing entry types');
  const traceRoot = buffer._traceRoot;
  buffer.timestamp[0] = traceRoot._timestampNow(traceRoot);
  entryTypes[0] = ENTRY_TYPE_SPAN_START;
  entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
  buffer.timestamp[1] = 0n;
  buffer._writeIndex = 2;
  return entryTypes;
}

const CURRENT_MIXED_APPENDERS: PhysicalAppenders = Object.freeze({
  ...CURRENT_BASE_APPENDERS,
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    initializeCurrentSpan(buffer);
    if (typeof name === 'string') {
      buffer.message(0, name);
      return;
    }
    const localId = buffer._opMetadata._physicalLayoutPlan?.encodeLocalMessage(name) ?? 0;
    if (localId === 0) {
      const rawMessages = buffer.message_values;
      if (rawMessages === undefined) throw new TypeError('Current mixed layout is missing raw message storage');
      rawMessages[0] = decodeVocabularyMessage(buffer._vocabularyGeneration, name);
    } else {
      const messageIds = buffer._messageIds;
      if (messageIds === undefined) throw new TypeError('Current mixed layout is missing local message storage');
      messageIds[0] = localId;
    }
  },
});

const CURRENT_STATIC_APPENDERS: PhysicalAppenders = Object.freeze({
  ...CURRENT_BASE_APPENDERS,
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    initializeCurrentSpan(buffer);
    if (typeof name === 'string') {
      buffer._spanName = name;
      return;
    }
    const localId = buffer._opMetadata._physicalLayoutPlan?.encodeLocalMessage(name) ?? 0;
    if (localId === 0) {
      buffer._spanName = name;
      return;
    }
    const messageIds = buffer._messageIds;
    if (messageIds === undefined) throw new TypeError('Current static layout is missing local message storage');
    messageIds[0] = localId;
  },
});

const CURRENT_DYNAMIC_APPENDERS: PhysicalAppenders = Object.freeze({
  ...CURRENT_BASE_APPENDERS,
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    initializeCurrentSpan(buffer);
    buffer._spanName = name;
  },
});

const SPLIT_MIXED_APPENDERS: PhysicalAppenders = Object.freeze({
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    if (typeof name === 'number') {
      const entryTypes = buffer.entry_type;
      const headers = buffer._logHeaders;
      if (entryTypes === undefined || headers === undefined) throw new TypeError('Split mixed layout is incomplete');
      const traceRoot = buffer._traceRoot;
      buffer.timestamp[0] = traceRoot._timestampNow(traceRoot);
      entryTypes[0] = ENTRY_TYPE_SPAN_START;
      headers[0] = name + 1;
      entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
      buffer.timestamp[1] = 0n;
      buffer._writeIndex = 2;
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
    return SPLIT_APPEND_LOG_ENTRY(buffer._traceRoot, buffer, entryType);
  },
});

const SPLIT_STATIC_APPENDERS: PhysicalAppenders = Object.freeze({
  ...SPLIT_MIXED_APPENDERS,
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    const entryTypes = buffer.entry_type;
    const headers = buffer._logHeaders;
    if (entryTypes === undefined || headers === undefined) throw new TypeError('Split static layout is incomplete');
    const traceRoot = buffer._traceRoot;
    buffer.timestamp[0] = traceRoot._timestampNow(traceRoot);
    entryTypes[0] = ENTRY_TYPE_SPAN_START;
    if (typeof name === 'number') {
      headers[0] = name + 1;
    } else {
      buffer._spanName = name;
    }
    entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
    buffer.timestamp[1] = 0n;
    buffer._writeIndex = 2;
  },
});

const SPLIT_DYNAMIC_APPENDERS: PhysicalAppenders = Object.freeze({
  ...SPLIT_MIXED_APPENDERS,
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    const entryTypes = buffer.entry_type;
    if (entryTypes === undefined) throw new TypeError('Split dynamic layout is missing entry types');
    const traceRoot = buffer._traceRoot;
    buffer.timestamp[0] = traceRoot._timestampNow(traceRoot);
    entryTypes[0] = ENTRY_TYPE_SPAN_START;
    buffer._spanName = name;
    entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
    buffer.timestamp[1] = 0n;
    buffer._writeIndex = 2;
  },
});

function packedAppenders(messageLayoutFamily: MessageLayoutFamily): PhysicalAppenders {
  return Object.freeze({
    writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
      const headers = buffer._rowHeaders;
      if (headers === undefined) throw new TypeError('Packed layout is missing row headers');
      const traceRoot = buffer._traceRoot;
      buffer.timestamp[0] = traceRoot._timestampNow(traceRoot);
      if (typeof name === 'number') {
        if (name > MAX_PACKED_MESSAGE_DENSE_INDEX) throw new RangeError('Packed message dense index exceeds 0xFFFFFE');
        headers[0] = (((name + 1) << 8) | ENTRY_TYPE_SPAN_START) >>> 0;
      } else {
        headers[0] = ENTRY_TYPE_SPAN_START;
        if (messageLayoutFamily === 'static-only' || messageLayoutFamily === 'dynamic-only') {
          buffer._spanName = name;
        } else {
          const rawMessages = buffer.message_values;
          if (rawMessages === undefined) throw new TypeError('Packed mixed layout is missing raw message storage');
          if (typeof name !== 'string') throw new TypeError('Packed mixed numeric span name was not encoded');
          rawMessages[0] = name;
        }
      }
      headers[1] = ENTRY_TYPE_SPAN_EXCEPTION;
      buffer.timestamp[1] = 0n;
      buffer._writeIndex = 2;
    },
    writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
      const headers = buffer._rowHeaders;
      if (headers === undefined) throw new TypeError('Packed layout is missing row headers');
      const traceRoot = buffer._traceRoot;
      buffer.timestamp[1] = traceRoot._timestampNow(traceRoot);
      headers[1] = entryType;
      buffer._sealStatsChain();
    },
    writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
      return PACKED_APPEND_LOG_ENTRY(buffer._traceRoot, buffer, entryType);
    },
  });
}

const APPENDERS_BY_MESSAGE_LAYOUT: Readonly<Record<string, PhysicalAppenders>> = Object.freeze({
  'static-only:current': CURRENT_STATIC_APPENDERS,
  'mixed:current': CURRENT_MIXED_APPENDERS,
  'dynamic-only:current': CURRENT_DYNAMIC_APPENDERS,
  'static-only:specialized': SPLIT_STATIC_APPENDERS,
  'mixed:specialized': SPLIT_MIXED_APPENDERS,
  'dynamic-only:specialized': SPLIT_DYNAMIC_APPENDERS,
  'static-only:packed': packedAppenders('static-only'),
  'mixed:packed': packedAppenders('mixed'),
  'dynamic-only:packed': packedAppenders('dynamic-only'),
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
  backendKind: PhysicalBackendKind,
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
  const SpanLoggerClass = createSpanLoggerClass(
    schema,
    messageLayoutFamily,
    messagePhysicalLayout,
    eagerColumns.names,
    enumLookup,
  );
  const ResultWriterClass = getResultWriterClass(
    schema,
    messageLayoutFamily,
    messagePhysicalLayout,
    eagerColumns.names,
    enumLookup,
  );
  const capabilities = isRuntimeHintAnalyzed(runtimeHint)
    ? runtimeHint & RUNTIME_HINT_CAPABILITIES_MASK
    : RUNTIME_HINT_FULL_CAPABILITIES;
  const needsLogger = (capabilities & (RUNTIME_HINT_LOG | RUNTIME_HINT_FF | RUNTIME_HINT_SCOPE)) !== 0;
  const needsTag = (capabilities & RUNTIME_HINT_TAG) !== 0;
  const TagWriterClass = needsTag ? getTagWriterClass(schema, eagerColumns.names, enumLookup) : undefined;
  const newSpanLogger = needsLogger ? (state: WriterState): SpanLoggerImpl<T> => new SpanLoggerClass(state) : undefined;
  const newTagWriter =
    TagWriterClass === undefined ? undefined : (state: WriterState): TagWriter<T> => new TagWriterClass(state);
  const wasmLayout = createWasmLayoutTemplate(schema, messageLayoutFamily, messagePhysicalLayout, eagerColumns);

  return Object.freeze({
    version: PHYSICAL_LAYOUT_VERSION,
    backendKind,
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
    appendLogEntry: messagePhysicalLayout === 'packed' ? PACKED_APPEND_LOG_ENTRY : SPLIT_APPEND_LOG_ENTRY,
    appenders: APPENDERS_BY_MESSAGE_LAYOUT[`${messageLayoutFamily}:${messagePhysicalLayout}`],
    localMessageDictionary,
    vocabularyGeneration,
    arrowExposure:
      backendKind === 'wasm'
        ? WASM_ARROW_EXPOSURE_BY_LAYOUT[messagePhysicalLayout]
        : JS_ARROW_EXPOSURE_BY_LAYOUT[messagePhysicalLayout],
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
  const key = `${PHYSICAL_LAYOUT_VERSION}:${backendKind}:${runtimeHint}:${messageLayoutFamily}:${messagePhysicalLayout}:${contextLayoutKey}:${vocabularyGeneration.generation}:${eagerColumns.key}:${resolvedLocalMessageDictionary.join(',')}`;
  let base = byKey.get(key) as PhysicalLayoutPlan<T, Ctx> | undefined;
  if (!base) {
    base = createBasePlan(
      SpanBufferClass,
      runtimeHint,
      backendKind,
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
  const cached = bindings.get(remapDescriptor) as PhysicalLayoutPlan<T, Ctx> | undefined;
  if (cached) return cached;

  const bound: PhysicalLayoutPlan<T, Ctx> = Object.freeze({
    ...base,
    remapDescriptor,
  });
  bindings.set(remapDescriptor, bound);
  return bound;
}
