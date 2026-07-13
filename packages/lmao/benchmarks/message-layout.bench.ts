import { bench, do_not_optimize, group, run, summary } from 'mitata';
import { defineOpContext } from '../src/lib/defineOpContext.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import type { CallsitePlan } from '../src/lib/physicalLayoutPlan.js';
import type { OpContextOf } from '../src/lib/opContext/types.js';
import { resolveMessage } from '../src/lib/resolveMessage.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY,
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY,
  RUNTIME_HINT_RESULT,
  type MessageLayoutFamily,
} from '../src/lib/runtimeHint.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ENTRY_TYPE_INFO } from '../src/lib/schema/systemSchema.js';
import { createSpanBuffer } from '../src/lib/spanBuffer.js';
import { createTraceId } from '../src/lib/traceId.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';
import type { AnySpanBuffer } from '../src/lib/types.js';
import { registerBenchmarkVocabulary } from './vocabularyFixture.js';

const CAPACITIES: readonly number[] = Object.freeze([8, 64, 1024]);
const STATIC_RATIOS: readonly number[] = Object.freeze([0, 25, 50, 75, 100]);
const QUICK = process.argv.includes('--quick');
const FORMAT = parseFormat(argumentValue('--format'));
const TARGET_HOT_WRITES = QUICK ? 64 : 8_192;
const TEMPLATES: readonly string[] = Object.freeze([
  'request accepted',
  'cache lookup complete',
  'response committed',
  'request cleanup complete',
]);
const TEMPLATE_BINDING = registerBenchmarkVocabulary(TEMPLATES);
const USER_SCHEMA = defineLogSchema({ marker: S.category() });
const CONTEXT = defineOpContext({ logSchema: USER_SCHEMA });
const RUNTIME_SCHEMA = CONTEXT.logBinding.logSchema;
type RuntimeContext = OpContextOf<typeof CONTEXT>;
const TRACER = new TestTracer(CONTEXT, { bufferStrategy: new JsBufferStrategy(), createTraceRoot });

type RunFormat = 'json' | 'markdown' | 'mitata' | 'quiet';

type Workload = {
  readonly messages: readonly (string | undefined)[];
  readonly staticRows: Uint8Array;
  readonly templateOrdinals: Uint8Array;
};

type PersistentCounts = {
  readonly arrayBuffers: number;
  readonly views: number;
  readonly referenceSlots: number;
  readonly allocatedBytes: number;
};

type ScenarioMetadata = {
  readonly capacity: number;
  readonly staticRatioPercent: number;
  readonly selectedFamily: MessageLayoutFamily;
  readonly hotWritesPerInvocation: number;
  readonly persistent: Readonly<Record<string, PersistentCounts>>;
};

function argumentValue(name: string): string | undefined {
  const prefix = `${name}=`;
  for (let index = 0; index < process.argv.length; index++) {
    const argument = process.argv[index];
    if (argument === undefined) continue;
    if (argument.startsWith(prefix)) return argument.slice(prefix.length);
    if (argument === name) return process.argv[index + 1];
  }
  return undefined;
}

function parseFormat(value: string | undefined): RunFormat {
  if (value === undefined) return 'mitata';
  if (value === 'json' || value === 'markdown' || value === 'mitata' || value === 'quiet') return value;
  throw new Error(`Unknown Mitata format: ${value}`);
}

function familyHint(familyBits: number, capacity: number): number {
  return RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | familyBits | capacity;
}

function defineFamilyPlan(
  family: MessageLayoutFamily,
  familyBits: number,
  capacity: number,
): CallsitePlan<typeof RUNTIME_SCHEMA, RuntimeContext> {
  return CONTEXT.defineOp(
    `message-layout-${family}-${capacity}`,
    (ctx) => ctx.ok(null),
    undefined,
    { runtimeHint: familyHint(familyBits, capacity) },
  ).callsitePlan;
}

function createWorkload(capacity: number, staticRatio: number): Workload {
  const messages = new Array<string | undefined>(capacity);
  const staticRows = new Uint8Array(capacity);
  const templateOrdinals = new Uint8Array(capacity);
  const staticCount = Math.round((capacity * staticRatio) / 100);
  for (let row = 0; row < capacity; row++) {
    if (row % 11 === 0) {
      messages[row] = undefined;
      continue;
    }
    if (row < staticCount) {
      const ordinal = row % TEMPLATES.length;
      const template = TEMPLATES[ordinal];
      if (template === undefined) throw new RangeError(`Missing template ${ordinal}`);
      staticRows[row] = 1;
      templateOrdinals[row] = ordinal;
      messages[row] = template;
    } else {
      messages[row] = `dynamic-request-${row % 17}`;
    }
  }
  return { messages, staticRows, templateOrdinals };
}

function selectedFamily(staticRatio: number): MessageLayoutFamily {
  if (staticRatio === 0) return 'dynamic-only';
  if (staticRatio === 100) return 'static-only';
  return 'mixed';
}

function requireHeaderLane(buffer: AnySpanBuffer): Uint32Array {
  const headers = buffer._logHeaders;
  if (headers === undefined) throw new Error(`${buffer._messageLayoutFamily} buffer omitted its required header lane`);
  return headers;
}

function requireMessageLane(buffer: AnySpanBuffer): (unknown | undefined)[] {
  const messages = buffer.message_values;
  if (messages === undefined) throw new Error(`${buffer._messageLayoutFamily} buffer omitted its required message lane`);
  return messages;
}

function createPlannedBuffer(
  plan: CallsitePlan<typeof RUNTIME_SCHEMA, RuntimeContext>,
  capacity: number,
  label: string,
): AnySpanBuffer {
  if (plan.capacityTier !== capacity) {
    throw new Error(`${label}: CallsitePlan capacity ${String(plan.capacityTier)} did not match ${capacity}`);
  }
  const traceRoot = createTraceRoot(createTraceId(`message-layout-${label}-${capacity}`), TRACER);
  return createSpanBuffer(RUNTIME_SCHEMA, traceRoot, plan.metadata, capacity, plan.SpanBufferClass);
}

function writeWorkload(
  plan: CallsitePlan<typeof RUNTIME_SCHEMA, RuntimeContext>,
  buffer: AnySpanBuffer,
  workload: Workload,
  repetitions: number,
): number {
  let checksum = 2_166_136_261;
  for (let repetition = 0; repetition < repetitions; repetition++) {
    buffer._writeIndex = 0;
    const headers = buffer._logHeaders;
    const references = buffer.message_values;
    headers?.fill(0);
    if (references !== undefined) {
      for (let row = 0; row < references.length; row++) Reflect.deleteProperty(references, row);
    }
    for (let row = 0; row < workload.messages.length; row++) {
      const outputRow = plan.appenders.writeLogEntry(buffer, ENTRY_TYPE_INFO);
      if (outputRow !== row) throw new Error(`Appender produced row ${outputRow}, expected ${row}`);
      const message = workload.messages[row];
      if (message !== undefined) {
        if (workload.staticRows[row] !== 0) {
          const ordinal = workload.templateOrdinals[row];
          const denseIndex = ordinal === undefined ? undefined : TEMPLATE_BINDING[ordinal];
          if (denseIndex === undefined) throw new RangeError(`Missing dense template binding for row ${row}`);
          requireHeaderLane(buffer)[row] = ((denseIndex << 8) | ENTRY_TYPE_INFO) >>> 0;
        } else {
          requireMessageLane(buffer)[row] = message;
        }
      }
      checksum = Math.imul(checksum ^ outputRow ^ (message?.length ?? 0), 16_777_619) >>> 0;
    }
  }
  return checksum;
}

function checksumRows(buffer: AnySpanBuffer, expected: readonly (string | undefined)[]): number {
  let checksum = 2_166_136_261;
  for (let row = 0; row < expected.length; row++) {
    const actual = resolveMessage(buffer, row);
    if (actual !== expected[row]) {
      throw new Error(
        `${buffer._messageLayoutFamily} row ${row}: expected ${String(expected[row])}, received ${String(actual)}`,
      );
    }
    checksum = Math.imul(checksum ^ row ^ (actual?.length ?? 0), 16_777_619) >>> 0;
  }
  return checksum;
}

function persistentCounts(buffer: AnySpanBuffer): PersistentCounts {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new Error('Message layout requires the entry_type lane');
  const arrays: ArrayBufferView[] = [buffer.timestamp, entryTypes];
  if (buffer._logHeaders !== undefined) arrays.push(buffer._logHeaders);
  let referenceSlots = 0;
  if (buffer.message_values !== undefined) referenceSlots += buffer.message_values.length;
  const uniqueBuffers = new Set<ArrayBufferLike>(arrays.map((view) => view.buffer));
  let allocatedBytes = referenceSlots * 8;
  for (const backing of uniqueBuffers) allocatedBytes += backing.byteLength;
  return {
    arrayBuffers: uniqueBuffers.size,
    views: arrays.length,
    referenceSlots,
    allocatedBytes,
  };
}

function familyBits(family: MessageLayoutFamily): number {
  if (family === 'static-only') return RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY;
  if (family === 'dynamic-only') return RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY;
  return RUNTIME_HINT_MESSAGE_LAYOUT_MIXED;
}

const persistentMetadata: ScenarioMetadata[] = [];

for (const capacity of CAPACITIES) {
  const plans: Record<MessageLayoutFamily, CallsitePlan<typeof RUNTIME_SCHEMA, RuntimeContext>> = {
    'static-only': defineFamilyPlan('static-only', familyBits('static-only'), capacity),
    mixed: defineFamilyPlan('mixed', familyBits('mixed'), capacity),
    'dynamic-only': defineFamilyPlan('dynamic-only', familyBits('dynamic-only'), capacity),
  };
  for (const staticRatio of STATIC_RATIOS) {
    const workload = createWorkload(capacity, staticRatio);
    const family = selectedFamily(staticRatio);
    const plan = plans[family];
    const mixedPlan = plans.mixed;
    const repetitions = Math.max(1, Math.ceil(TARGET_HOT_WRITES / capacity));
    const selectedBuffer = createPlannedBuffer(plan, capacity, `${family}-${staticRatio}`);
    const mixedBuffer = createPlannedBuffer(mixedPlan, capacity, `mixed-${staticRatio}`);
    const selectedChecksum = writeWorkload(plan, selectedBuffer, workload, 1);
    const mixedChecksum = writeWorkload(mixedPlan, mixedBuffer, workload, 1);
    if (selectedChecksum !== mixedChecksum) throw new Error(`Write checksum mismatch at ${capacity}/${staticRatio}`);
    const expectedChecksum = checksumRows(selectedBuffer, workload.messages);
    if (checksumRows(mixedBuffer, workload.messages) !== expectedChecksum) {
      throw new Error(`Decoded checksum mismatch at ${capacity}/${staticRatio}`);
    }
    const selectedLabel = `selected/${family}`;
    persistentMetadata.push({
      capacity,
      staticRatioPercent: staticRatio,
      selectedFamily: family,
      hotWritesPerInvocation: repetitions * capacity,
      persistent: {
        [selectedLabel]: persistentCounts(selectedBuffer),
        'conservative/mixed': persistentCounts(mixedBuffer),
      },
    });

    group(`message-layout/hot-write/capacity-${capacity}/static-${staticRatio}%`, () => {
      summary(() => {
        bench('conservative/mixed', () =>
          do_not_optimize(writeWorkload(mixedPlan, mixedBuffer, workload, repetitions)),
        ).baseline(true);
        if (family !== 'mixed') {
          bench(selectedLabel, () =>
            do_not_optimize(writeWorkload(plan, selectedBuffer, workload, repetitions)),
          );
        }
      });
    });

    group(`message-layout/cold-read/capacity-${capacity}/static-${staticRatio}%`, () => {
      summary(() => {
        bench('conservative/mixed', () => do_not_optimize(checksumRows(mixedBuffer, workload.messages))).baseline(true);
        if (family !== 'mixed') {
          bench(selectedLabel, () => do_not_optimize(checksumRows(selectedBuffer, workload.messages)));
        }
      });
    });
  }
}

if (!process.argv.includes('--no-metadata')) {
  console.error(`message-layout persistent metadata: ${JSON.stringify(persistentMetadata)}`);
}
await run({ format: FORMAT, colors: FORMAT !== 'json' && FORMAT !== 'quiet', throw: true });
