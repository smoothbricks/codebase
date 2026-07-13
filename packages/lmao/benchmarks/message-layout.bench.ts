import { bench, group, run, summary } from 'mitata';
import { defineOpContext } from '../src/lib/defineOpContext.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  RUNTIME_HINT_RESULT,
} from '../src/lib/runtimeHint.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import {
  createChildSpanBuffer,
  createOverflowBuffer,
  createSpanBuffer,
} from '../src/lib/spanBuffer.js';
import type { AnySpanBuffer } from '../src/lib/types.js';
import { createTestTraceRoot } from '../src/lib/__tests__/test-helpers.js';

const CAPACITIES = [8, 64, 1024] as const;
const STATIC_RATIOS = [0, 25, 50, 75, 100] as const;
const ENTRY_TYPE_INFO = 3;
const GLOBAL_TEMPLATE_BASE = 1_000;
const REFERENCE_BYTES = 8;
const QUICK = process.argv.includes('--quick');
const FORMAT = parseFormat(argumentValue('--format'));
const TARGET_HOT_WRITES = QUICK ? 64 : 8_192;
const FILTER_TEXT = argumentValue('--filter');
const FILTER = FILTER_TEXT === undefined ? undefined : new RegExp(FILTER_TEXT);

const SPECIALIZED_LABEL = 'specialized-split-u8+u32-dense-index';
const PACKED_LABEL = 'packed-u32-header';

type Message = string | undefined;

type Workload = {
  readonly capacity: number;
  readonly staticRatio: number;
  readonly staticRows: number;
  readonly nullRows: number;
  readonly isStatic: Uint8Array;
  readonly isValid: Uint8Array;
  readonly dynamicMessages: readonly string[];
  readonly templates: readonly string[];
};

type PersistentCounts = {
  readonly arrayBuffers: number;
  readonly views: number;
  readonly referenceSlots: number;
  readonly allocatedBytes: number;
  readonly objects: number;
};

interface MessageLayout {
  readonly counts: PersistentCounts;
  write(repetitions: number): void;
  messageAt(row: number): Message;
}

function argumentValue(name: string): string | undefined {
  const prefix = `${name}=`;
  const inline = process.argv.find((argument) => argument.startsWith(prefix));
  if (inline) return inline.slice(prefix.length);
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

type RunFormat = 'json' | 'markdown' | 'mitata' | 'quiet';

function parseFormat(value: string | undefined): RunFormat {
  if (value === undefined || value === 'text') return 'mitata';
  if (value === 'json' || value === 'markdown' || value === 'mitata' || value === 'quiet') return value;
  throw new Error(`Unsupported --format=${value}; expected json, markdown, mitata, quiet, or text`);
}

function createWorkload(capacity: number, staticRatio: number): Workload {
  const staticRows = Math.round((capacity * staticRatio) / 100);
  const isStatic = new Uint8Array(capacity);
  const isValid = new Uint8Array(capacity);
  const dynamicMessages = new Array<string>(capacity);
  const templates = Array.from({ length: capacity }, (_, row) => `static-message-${row}`);
  let nullRows = 0;

  for (let row = 0; row < capacity; row++) {
    isStatic[row] = row < staticRows ? 1 : 0;
    // Exercise message validity even at capacity 8, while keeping most rows valid.
    const valid = row % 11 !== 0;
    isValid[row] = valid ? 1 : 0;
    if (!valid) nullRows++;
    dynamicMessages[row] = `dynamic-message-${row}`;
  }

  return {
    capacity,
    staticRatio,
    staticRows,
    nullRows,
    isStatic,
    isValid,
    dynamicMessages,
    templates,
  };
}

function setValidity(bitmap: Uint8Array, row: number, valid: boolean): void {
  const mask = 1 << (row & 7);
  if (valid) bitmap[row >>> 3] |= mask;
  else bitmap[row >>> 3] &= ~mask;
}

function isValid(bitmap: Uint8Array, row: number): boolean {
  return (bitmap[row >>> 3]! & (1 << (row & 7))) !== 0;
}

/** Mirrors the current runtime's shared i64 timestamp + u8 entry-type ArrayBuffer
 * and its optional Op-local u16 template lane. The string reference lane and
 * message validity bitmap model the current eager message column.
 */
class CurrentOptionalLocalU16Layout implements MessageLayout {
  readonly counts: PersistentCounts;
  private readonly system: ArrayBuffer;
  private readonly timestamps: BigInt64Array;
  private readonly entryTypes: Uint8Array;
  private readonly localTemplateIds: Uint16Array | undefined;
  private readonly messageValidity: Uint8Array;
  private readonly messageReferences: Message[];

  constructor(private readonly workload: Workload) {
    const { capacity, staticRows } = workload;
    const hasTemplates = staticRows !== 0;
    const templateOffset = (capacity * 9 + 1) & ~1;
    const rawBytes = hasTemplates ? templateOffset + capacity * 2 : capacity * 9;
    this.system = new ArrayBuffer((rawBytes + 7) & ~7);
    this.timestamps = new BigInt64Array(this.system, 0, capacity);
    this.entryTypes = new Uint8Array(this.system, capacity * 8, capacity);
    this.localTemplateIds = hasTemplates ? new Uint16Array(this.system, templateOffset, capacity) : undefined;
    this.messageValidity = new Uint8Array(Math.ceil(capacity / 8));
    this.messageReferences = new Array<Message>(capacity);
    const views = 3 + (this.localTemplateIds ? 1 : 0);
    this.counts = {
      arrayBuffers: 2,
      views,
      referenceSlots: capacity,
      allocatedBytes: this.system.byteLength + this.messageValidity.byteLength + capacity * REFERENCE_BYTES,
      objects: 1 + 2 + views + 1,
    };
  }

  write(repetitions: number): void {
    const { capacity, isStatic, isValid: validRows, dynamicMessages } = this.workload;
    const templateIds = this.localTemplateIds;
    for (let repetition = 0; repetition < repetitions; repetition++) {
      for (let row = 0; row < capacity; row++) {
        const valid = validRows[row] !== 0;
        this.timestamps[row] = BigInt(row + 1);
        this.entryTypes[row] = ENTRY_TYPE_INFO;
        setValidity(this.messageValidity, row, valid);
        if (!valid) {
          if (templateIds) templateIds[row] = 0;
          this.messageReferences[row] = undefined;
        } else if (isStatic[row] !== 0) {
          templateIds![row] = row + 1;
          this.messageReferences[row] = undefined;
        } else {
          if (templateIds) templateIds[row] = 0;
          this.messageReferences[row] = dynamicMessages[row];
        }
      }
    }
  }

  messageAt(row: number): Message {
    if (!isValid(this.messageValidity, row)) return undefined;
    const templateId = this.localTemplateIds?.[row] ?? 0;
    return templateId === 0 ? this.messageReferences[row] : this.workload.templates[templateId - 1];
  }
}

/** Modeled candidate: independent i64, u8, and dense global-u32 lanes. A
 * static-only specialization omits the JavaScript reference lane.
 */
class SpecializedSplitDenseU32Layout implements MessageLayout {
  readonly counts: PersistentCounts;
  private readonly timestamps: BigInt64Array;
  private readonly entryTypes: Uint8Array;
  private readonly denseTemplateIds: Uint32Array;
  private readonly messageValidity: Uint8Array;
  private readonly messageReferences: Message[] | undefined;

  constructor(private readonly workload: Workload) {
    const { capacity, staticRows } = workload;
    this.timestamps = new BigInt64Array(capacity);
    this.entryTypes = new Uint8Array(capacity);
    this.denseTemplateIds = new Uint32Array(capacity);
    this.messageValidity = new Uint8Array(Math.ceil(capacity / 8));
    this.messageReferences = staticRows === capacity ? undefined : new Array<Message>(capacity);
    const referenceSlots = this.messageReferences ? capacity : 0;
    this.counts = {
      arrayBuffers: 4,
      views: 4,
      referenceSlots,
      allocatedBytes:
        this.timestamps.byteLength +
        this.entryTypes.byteLength +
        this.denseTemplateIds.byteLength +
        this.messageValidity.byteLength +
        referenceSlots * REFERENCE_BYTES,
      objects: 1 + 4 + 4 + (this.messageReferences ? 1 : 0),
    };
  }

  write(repetitions: number): void {
    const { capacity, isStatic, isValid: validRows, dynamicMessages } = this.workload;
    const references = this.messageReferences;
    for (let repetition = 0; repetition < repetitions; repetition++) {
      for (let row = 0; row < capacity; row++) {
        const valid = validRows[row] !== 0;
        this.timestamps[row] = BigInt(row + 1);
        this.entryTypes[row] = ENTRY_TYPE_INFO;
        setValidity(this.messageValidity, row, valid);
        if (!valid) {
          this.denseTemplateIds[row] = 0;
          if (references) references[row] = undefined;
        } else if (isStatic[row] !== 0) {
          this.denseTemplateIds[row] = GLOBAL_TEMPLATE_BASE + row + 1;
          if (references) references[row] = undefined;
        } else {
          this.denseTemplateIds[row] = 0;
          references![row] = dynamicMessages[row];
        }
      }
    }
  }

  messageAt(row: number): Message {
    if (!isValid(this.messageValidity, row)) return undefined;
    const globalId = this.denseTemplateIds[row]!;
    return globalId === 0 ? this.messageReferences![row] : this.workload.templates[globalId - GLOBAL_TEMPLATE_BASE - 1];
  }
}

/** Modeled candidate: one u32 message header packs validity, the u8 entry type,
 * and the global template ID. Zero is invalid; a nonzero header with ID zero is
 * a valid dynamic row. Static-only storage omits JavaScript reference slots.
 */
class PackedU32HeaderLayout implements MessageLayout {
  readonly counts: PersistentCounts;
  private readonly timestamps: BigInt64Array;
  private readonly headers: Uint32Array;
  private readonly messageReferences: Message[] | undefined;

  constructor(private readonly workload: Workload) {
    const { capacity, staticRows } = workload;
    const storage = new ArrayBuffer(capacity * 12);
    this.timestamps = new BigInt64Array(storage, 0, capacity);
    this.headers = new Uint32Array(storage, capacity * 8, capacity);
    this.messageReferences = staticRows === capacity ? undefined : new Array<Message>(capacity);
    const referenceSlots = this.messageReferences ? capacity : 0;
    this.counts = {
      arrayBuffers: 1,
      views: 2,
      referenceSlots,
      allocatedBytes: storage.byteLength + referenceSlots * REFERENCE_BYTES,
      objects: 1 + 1 + 2 + (this.messageReferences ? 1 : 0),
    };
  }

  write(repetitions: number): void {
    const { capacity, isStatic, isValid: validRows, dynamicMessages } = this.workload;
    const references = this.messageReferences;
    for (let repetition = 0; repetition < repetitions; repetition++) {
      for (let row = 0; row < capacity; row++) {
        this.timestamps[row] = BigInt(row + 1);
        if (validRows[row] === 0) {
          this.headers[row] = 0;
          if (references) references[row] = undefined;
        } else if (isStatic[row] !== 0) {
          const globalId = GLOBAL_TEMPLATE_BASE + row + 1;
          this.headers[row] = globalId * 8 + ENTRY_TYPE_INFO;
          if (references) references[row] = undefined;
        } else {
          this.headers[row] = ENTRY_TYPE_INFO;
          references![row] = dynamicMessages[row];
        }
      }
    }
  }

  messageAt(row: number): Message {
    const header = this.headers[row]!;
    if (header === 0) return undefined;
    const globalId = Math.floor(header / 8);
    return globalId === 0 ? this.messageReferences![row] : this.workload.templates[globalId - GLOBAL_TEMPLATE_BASE - 1];
  }
}

function checksumRows(layout: MessageLayout, capacity: number): number {
  let hash = 0x811c9dc5;
  for (let row = 0; row < capacity; row++) {
    const message = layout.messageAt(row);
    if (message === undefined) {
      hash = Math.imul(hash ^ 0xff, 0x01000193);
      continue;
    }
    for (let index = 0; index < message.length; index++) {
      hash = Math.imul(hash ^ message.charCodeAt(index), 0x01000193);
    }
    hash = Math.imul(hash ^ 0, 0x01000193);
  }
  return hash >>> 0;
}

function createLayouts(workload: Workload): readonly MessageLayout[] {
  return [
    new CurrentOptionalLocalU16Layout(workload),
    new SpecializedSplitDenseU32Layout(workload),
    new PackedU32HeaderLayout(workload),
  ];
}

function scenarioKind(staticRatio: number): string {
  if (staticRatio === 0) return 'dynamic-only';
  if (staticRatio === 100) return 'static-only';
  return 'mixed';
}

function currentPhysicalLabel(staticRatio: number): string {
  return staticRatio === 0 ? 'split-i64+u8-dynamic-only' : 'split-i64+u8+optional-local-u16';
}

type ScenarioMetadata = {
  capacity: number;
  staticRatioPercent: number;
  scenarioKind: string;
  staticRows: number;
  dynamicRows: number;
  nullRows: number;
  currentPhysicalLayout: string;
  hotWritesPerInvocation: number;
  persistent: Record<string, PersistentCounts>;
  expectedSelector: string;
};

function assertEquivalent(layouts: readonly MessageLayout[], capacity: number, staticRatio: number): number {
  const expected = checksumRows(layouts[0]!, capacity);
  for (let index = 1; index < layouts.length; index++) {
    const actual = checksumRows(layouts[index]!, capacity);
    if (actual !== expected) {
      throw new Error(
        `Pre-timing decoded checksum mismatch at capacity=${capacity}, staticRatio=${staticRatio}: ` +
          `${SPECIALIZED_LABEL} or ${PACKED_LABEL} produced ${actual}; current produced ${expected}`,
      );
    }
  }
  return expected;
}

const SHARING_BUFFER_PAIRS = QUICK ? 8 : 128;
const sharingSchema = defineLogSchema({ marker: S.category() });
const sharingContext = defineOpContext({ logSchema: sharingSchema });
const sharingOp = sharingContext.defineOp('benchmark-local-message-dictionary', (ctx) => ctx.ok(null), undefined, {
  runtimeHint:
    RUNTIME_HINT_ANALYZED_VALID |
    RUNTIME_HINT_LOG |
    RUNTIME_HINT_RESULT |
    RUNTIME_HINT_MESSAGE_LAYOUT_MIXED |
    8,
  localMessageDictionary: [0],
});
const sharingPlan = sharingOp.metadata._physicalLayoutPlan;
if (!sharingPlan) throw new Error('Expected benchmark callsite to own a PhysicalLayoutPlan');
sharingPlan.SpanBufferClass.stats.capacity = 8;
const sharingRoot = createSpanBuffer(
  sharingContext.logBinding.logSchema,
  createTestTraceRoot('message-layout-sharing'),
  sharingOp.metadata,
  8,
  sharingPlan.SpanBufferClass,
);
const sharingBuffers: AnySpanBuffer[] = [sharingRoot];
for (let index = 0; index < SHARING_BUFFER_PAIRS; index++) {
  const child = createChildSpanBuffer(
    sharingRoot,
    sharingPlan.SpanBufferClass,
    sharingOp.metadata,
    sharingOp.metadata,
    8,
  );
  sharingBuffers.push(child, createOverflowBuffer(child));
}

const sharedDictionary = sharingPlan.localMessageDictionary;
const sharedEncoder = sharingPlan.encodeLocalMessage;
const uniquePlans = new Set<object>();
const uniqueDictionaries = new Set<readonly number[]>();
const uniqueEncoders = new Set<(globalDenseIndex: number) => number>();
let bufferOwnedDictionaryObjects = 0;
if (!Object.isFrozen(sharedDictionary)) throw new Error('Callsite local message dictionary must be frozen');
if (sharedDictionary.length === 0) throw new Error('Sharing gate requires a nonempty local message dictionary');
if (sharedEncoder(sharedDictionary[0]!) !== 1 || sharedEncoder(0xffff_ffff) !== 0) {
  throw new Error('Callsite local message encoder violated its 1-based ID contract');
}
for (const buffer of sharingBuffers) {
  const plan = buffer._opMetadata._physicalLayoutPlan;
  if (!plan) throw new Error('Buffer did not retain its callsite PhysicalLayoutPlan');
  uniquePlans.add(plan);
  uniqueDictionaries.add(plan.localMessageDictionary);
  uniqueEncoders.add(plan.encodeLocalMessage);
  if (plan !== sharingPlan || plan.localMessageDictionary !== sharedDictionary || plan.encodeLocalMessage !== sharedEncoder) {
    throw new Error('Root, child, or overflow buffer did not reuse the callsite dictionary and encoder identities');
  }
  for (const key of ['_messageDictionary', 'localMessageDictionary', 'encodeLocalMessage']) {
    if (Object.hasOwn(buffer, key)) bufferOwnedDictionaryObjects++;
  }
}
if (
  uniquePlans.size !== 1 ||
  uniqueDictionaries.size !== 1 ||
  uniqueEncoders.size !== 1 ||
  bufferOwnedDictionaryObjects !== 0
) {
  throw new Error('Per-buffer message dictionary or encoder allocation detected');
}
const sharingMetadata = {
  bufferCount: sharingBuffers.length,
  rootBuffers: 1,
  childBuffers: SHARING_BUFFER_PAIRS,
  overflowBuffers: SHARING_BUFFER_PAIRS,
  physicalLayoutPlanObjects: uniquePlans.size,
  localMessageDictionaryObjects: uniqueDictionaries.size,
  localMessageEncoderObjects: uniqueEncoders.size,
  bufferOwnedDictionaryObjects,
  dictionaryFrozen: Object.isFrozen(sharedDictionary),
  dictionaryLength: sharedDictionary.length,
  knownDenseLocalId: sharedEncoder(sharedDictionary[0]!),
  missingDenseLocalId: sharedEncoder(0xffff_ffff),
} as const;

group('message-layout/callsite-dictionary-sharing', () => {
  bench('callsite-local-dictionary-shared-identity', () => {
    let identityMatches = 0;
    for (const buffer of sharingBuffers) {
      const plan = buffer._opMetadata._physicalLayoutPlan;
      if (
        plan === sharingPlan &&
        plan.localMessageDictionary === sharedDictionary &&
        plan.encodeLocalMessage === sharedEncoder
      ) {
        identityMatches++;
      }
    }
    return identityMatches;
  }).baseline(true);
});

const labels = ['current', SPECIALIZED_LABEL, PACKED_LABEL] as const;
const expectedSelector: Readonly<Record<number, Readonly<Record<number, (typeof labels)[number]>>>> = Object.freeze({
  8: Object.freeze({
    0: 'current',
    25: 'current',
    50: 'current',
    75: 'current',
    100: 'current',
  }),
  64: Object.freeze({
    0: 'current',
    25: 'current',
    50: SPECIALIZED_LABEL,
    75: 'current',
    100: 'current',
  }),
  1024: Object.freeze({
    0: 'current',
    25: 'current',
    50: 'current',
    75: 'current',
    100: 'current',
  }),
});
const persistentMetadata: ScenarioMetadata[] = [];

for (const capacity of CAPACITIES) {
  for (const staticRatio of STATIC_RATIOS) {
    const workload = createWorkload(capacity, staticRatio);
    const repetitions = Math.max(1, Math.ceil(TARGET_HOT_WRITES / capacity));

    const hotLayouts = createLayouts(workload);
    for (const layout of hotLayouts) layout.write(1);
    assertEquivalent(hotLayouts, capacity, staticRatio);

    const coldLayouts = createLayouts(workload);
    for (const layout of coldLayouts) layout.write(1);
    assertEquivalent(coldLayouts, capacity, staticRatio);

    persistentMetadata.push({
      capacity,
      staticRatioPercent: staticRatio,
      scenarioKind: scenarioKind(staticRatio),
      staticRows: workload.staticRows,
      dynamicRows: capacity - workload.staticRows,
      nullRows: workload.nullRows,
      currentPhysicalLayout: currentPhysicalLabel(staticRatio),
      hotWritesPerInvocation: repetitions * capacity,
      persistent: Object.fromEntries(labels.map((label, index) => [label, hotLayouts[index]!.counts])),
      expectedSelector: expectedSelector[capacity]![staticRatio]!,
    });

    group(`message-layout/hot-write/capacity-${capacity}/static-${staticRatio}%`, () => {
      summary(() => {
        bench(labels[0], () => hotLayouts[0]!.write(repetitions)).baseline(true);
        bench(labels[1], () => hotLayouts[1]!.write(repetitions));
        bench(labels[2], () => hotLayouts[2]!.write(repetitions));
      });
    });

    // Extraction is isolated from writes: these persistent layouts are fully
    // populated before Mitata begins warmup and measurement.
    group(`message-layout/cold-extraction/capacity-${capacity}/static-${staticRatio}%`, () => {
      summary(() => {
        bench(labels[0], () => checksumRows(coldLayouts[0]!, capacity)).baseline(true);
        bench(labels[1], () => checksumRows(coldLayouts[1]!, capacity));
        bench(labels[2], () => checksumRows(coldLayouts[2]!, capacity));
      });
    });
  }
}

// These physical-allocation counts are computed during setup and emitted on
// stderr, never sampled inside a timed callback. Mitata owns heap/GC telemetry.
if (!process.argv.includes('--no-metadata')) {
  console.error(`message-layout persistent metadata: ${JSON.stringify(persistentMetadata)}`);
  console.error(`message-layout sharing metadata: ${JSON.stringify(sharingMetadata)}`);
}

await run({
  format: FORMAT,
  colors: FORMAT !== 'json' && FORMAT !== 'quiet',
  throw: true,
  ...(FILTER === undefined ? {} : { filter: FILTER }),
});
