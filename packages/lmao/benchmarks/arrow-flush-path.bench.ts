/**
 * Position-balanced Arrow flush benchmarks.
 *
 * The `current/*` variants call the production conversion functions. The other
 * variants deliberately isolate message resolution and output ownership; their
 * labels say "model" because they do not claim to be complete Arrow converters.
 * Copy/allocation counters are computed during semantic preflight, never in the
 * timed callbacks. Production conversion is explicitly not labeled zero-copy
 * because its internal copies are not instrumented here.
 *
 * Run: bun packages/lmao/benchmarks/arrow-flush-path.bench.ts [--quick]
 *      [--json] [--markdown]
 */

import type { Table } from '@uwdata/flechette';
import { bench, group, run, summary } from 'mitata';
import { walkSpanTree } from '../src/lib/traceTopology.js';
import type { SpanLoggerImpl } from '../src/lib/codegen/spanLoggerGenerator.js';
import {
  convertSpanTreeToArrowTable,
  convertSpanTreeToLeasedArrowTable,
  convertToArrowTable,
  convertToLeasedArrowTable,
} from '../src/lib/convertToArrow.js';
import { getVocabularyDictionaryPrefix } from '../src/lib/arrow/vocabularyDictionary.js';
import { defineOpContext } from '../src/lib/defineOpContext.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import { createOpMetadata } from '../src/lib/opContext/defineOp.js';
import { resolveMessage } from '../src/lib/resolveMessage.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ENTRY_TYPE_SPAN_OK, ENTRY_TYPE_SPAN_START } from '../src/lib/schema/systemSchema.js';
import {
  createChildSpanBuffer,
  createOverflowBuffer,
  createSpanBuffer,
  getSpanBufferClass,
} from '../src/lib/spanBuffer.js';
import { createTraceId } from '../src/lib/traceId.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';
import type { AnySpanBuffer, SpanBuffer } from '../src/lib/types.js';
import { registerBenchmarkVocabulary } from './vocabularyFixture.js';

const MESSAGE_KINDS = ['static', 'dynamic', 'mixed'] as const;
const LOG_COUNTS = [0, 1, 50] as const;
const TOPOLOGIES = ['single', 'depth-3-tree', 'overflow-capacity'] as const;
const TEMPLATES = Object.freeze([
  'request accepted',
  'cache lookup complete',
  'response committed',
  'request cleanup complete',
]);
const TEMPLATE_BINDING = registerBenchmarkVocabulary(TEMPLATES);
const USER_SCHEMA = defineLogSchema({ marker: S.category() });
const OP_CONTEXT = defineOpContext({ logSchema: USER_SCHEMA });
const FULL_SCHEMA = OP_CONTEXT.logBinding.logSchema;
const textEncoder = new TextEncoder();

interface Counters {
  modeledAllocationCountEstimate?: number;
  modeledAllocatedBytesEstimate?: number;
  modeledCopyCountEstimate?: number;
  modeledCopiedBytesEstimate?: number;
  modeledDictionaryLookupCountEstimate?: number;
  modeledResolvedRowCountEstimate?: number;
  modeledLeasedChunkCountEstimate?: number;
}

interface LogicalOutput {
  schema: readonly string[];
  rows: number;
  messages: readonly (string | null)[] | readonly (readonly (string | null)[])[];
  nulls: readonly number[];
}

interface VariantOutput {
  logical: LogicalOutput;
  counters: Counters;
}

type MessageKind = (typeof MESSAGE_KINDS)[number];
type Topology = (typeof TOPOLOGIES)[number];

interface Scenario {
  name: string;
  kind: MessageKind;
  logCount: number;
  topology: Topology;
  capacity: number;
  depth: number;
  root: AnySpanBuffer;
  buffers: AnySpanBuffer[];
  staticDictionary: ReadonlyMap<string, number>;
  staticDenseIndices: Uint32Array;
}

function metadataForScenario(name: string) {
  return createOpMetadata(name, '@smoothbricks/arrow-flush-bench', 'arrow-flush-path.bench.ts', 'bench', 1);
}


function writeLogs(logger: SpanLoggerImpl<typeof FULL_SCHEMA>, kind: MessageKind, count: number): void {
  for (let index = 0; index < count; index++) {
    if (kind === 'static' || (kind === 'mixed' && (index & 1) === 0)) {
      logger._infoTemplate(TEMPLATE_BINDING[index % TEMPLATES.length]!);
    } else {
      logger.info(`dynamic request ${index % 11}`);
    }
  }
}

function collectBuffers(root: AnySpanBuffer): AnySpanBuffer[] {
  const buffers: AnySpanBuffer[] = [];
  walkSpanTree(root, (buffer) => {
    if (buffer._writeIndex > 0) buffers.push(buffer);
  });
  return buffers;
}

async function makeScenario(kind: MessageKind, logCount: number, topology: Topology): Promise<Scenario> {
  const requestedCapacity = topology === 'overflow-capacity' ? 8 : 64;
  const depth = topology === 'depth-3-tree' ? 3 : 1;
  const scenarioName = `${kind}-${logCount}-${topology}`;
  const metadata = metadataForScenario(scenarioName);
  const tracer = new TestTracer(OP_CONTEXT, { bufferStrategy: new JsBufferStrategy(), createTraceRoot });
  const op = OP_CONTEXT.defineOp(`arrow-flush-${scenarioName}`, async (ctx) => {
    writeLogs(ctx.log as SpanLoggerImpl<typeof FULL_SCHEMA>, kind, logCount);
    if (depth === 3) {
      await ctx.span('child-span-1', async (child1) => {
        writeLogs(child1.log as SpanLoggerImpl<typeof FULL_SCHEMA>, kind, logCount);
        await child1.span('child-span-2', (child2) => {
          writeLogs(child2.log as SpanLoggerImpl<typeof FULL_SCHEMA>, kind, logCount);
          return child2.ok(null);
        });
        return child1.ok(null);
      });
    }
    return ctx.ok(null);
  }, metadata);
  const previousCapacity = op.callsitePlan.SpanBufferClass.stats.capacity;
  op.callsitePlan.SpanBufferClass.stats.capacity = requestedCapacity;
  try {
    await tracer.trace('root-span', op);
  } finally {
    op.callsitePlan.SpanBufferClass.stats.capacity = previousCapacity;
  }
  const root = tracer.rootBuffers[0];
  if (!root) throw new Error(`${scenarioName}: production trace did not retain a root buffer`);

  const buffers = collectBuffers(root);
  const staticValues = new Set<string>(TEMPLATES);
  for (let level = 0; level < depth; level++) staticValues.add(level === 0 ? 'root-span' : `child-span-${level}`);
  const staticDictionary = new Map<string, number>();
  for (const value of staticValues) staticDictionary.set(value, staticDictionary.size);
  const staticDenseIndices = new Uint32Array(Math.max(...TEMPLATE_BINDING) + 1);
  for (let ordinal = 0; ordinal < TEMPLATES.length; ordinal++) {
    staticDenseIndices[TEMPLATE_BINDING[ordinal]!] = staticDictionary.get(TEMPLATES[ordinal]!)!;
  }

  return {
    name: `arrow-flush/${topology}/${kind}/${logCount}-logs`,
    kind,
    logCount,
    topology,
    capacity: root._capacity,
    depth,
    root,
    buffers,
    staticDictionary,
    staticDenseIndices,
  };
}

function logicalFromTable(table: Table): LogicalOutput {
  const schema = table.schema.fields.map(
    (field) => `${field.name}:${String(field.type.typeId)}:${String(field.nullable)}`,
  );
  const messagesColumn = table.getChild('message');
  if (!messagesColumn) throw new Error('Arrow conversion omitted the message column');
  const messages = Array.from({ length: table.numRows }, (_, row) => {
    const value = messagesColumn.get(row);
    return value == null ? null : String(value);
  });
  const nulls = table.schema.fields.map((field) => {
    const column = table.getChild(field.name);
    if (!column) throw new Error(`Arrow conversion omitted schema field ${field.name}`);
    return column.nullCount;
  });
  return { schema, rows: table.numRows, messages, nulls };
}

function flattenMessages(logical: LogicalOutput): readonly (string | null)[] {
  const messages = logical.messages;
  if (messages.length === 0) return [];
  return Array.isArray(messages[0])
    ? (messages as readonly (readonly (string | null)[])[]).flat()
    : (messages as readonly (string | null)[]);
}

function byteLength(value: string | null): number {
  return value === null ? 0 : textEncoder.encode(value).byteLength;
}

function currentConversion(scenario: Scenario): VariantOutput {
  const table =
    scenario.topology === 'depth-3-tree'
      ? convertSpanTreeToArrowTable(scenario.root)
      : convertToArrowTable(scenario.root);
  return { logical: logicalFromTable(table), counters: {} };
}

function leasedConversion(scenario: Scenario): VariantOutput {
  const lease =
    scenario.topology === 'depth-3-tree'
      ? convertSpanTreeToLeasedArrowTable(scenario.root)
      : convertToLeasedArrowTable(scenario.root);
  try {
    return { logical: logicalFromTable(lease.table), counters: {} };
  } finally {
    lease.release();
  }
}

function modelLogical(
  messages: readonly (string | null)[] | readonly (readonly (string | null)[])[],
  rows: number,
): LogicalOutput {
  const flat =
    messages.length > 0 && Array.isArray(messages[0])
      ? (messages as readonly (readonly (string | null)[])[]).flat()
      : (messages as readonly (string | null)[]);
  return {
    schema: ['model-only:decoded-message-values'],
    rows,
    messages,
    nulls: [flat.filter((value) => value === null).length],
  };
}

function currentTwoPassModel(scenario: Scenario, collectEstimates = false): VariantOutput {
  const messages: (string | null)[] = [];
  let allocatedBytes = 0;
  for (const buffer of scenario.buffers) {
    for (let row = 0; row < buffer._writeIndex; row++) {
      const value = resolveMessage(buffer, row) ?? null;
      messages.push(value);
      if (collectEstimates) allocatedBytes += byteLength(value);
    }
  }
  const dictionary = new Map<string, number>();
  const indices = new Uint32Array(messages.length);
  for (let row = 0; row < messages.length; row++) {
    const value = messages[row];
    if (value === null) continue;
    let index = dictionary.get(value);
    if (index === undefined) {
      index = dictionary.size;
      dictionary.set(value, index);
    }
    indices[row] = index;
  }
  return {
    logical: modelLogical(messages, messages.length),
    counters: collectEstimates
      ? {
          modeledAllocationCountEstimate: 3,
          modeledAllocatedBytesEstimate: allocatedBytes + indices.byteLength,
          modeledCopyCountEstimate: messages.length,
          modeledCopiedBytesEstimate: allocatedBytes,
          modeledDictionaryLookupCountEstimate: messages.length,
          modeledResolvedRowCountEstimate: messages.length,
        }
      : {},
  };
}

function prebuiltStaticDictionaryModel(scenario: Scenario, collectEstimates = false): VariantOutput {
  const messages: (string | null)[] = [];
  const rowCount = scenario.buffers.reduce((count, buffer) => count + buffer._writeIndex, 0);
  const indices = new Uint32Array(rowCount);
  const suffix = new Map<string, number>();
  let allocatedBytes = 0;
  for (const buffer of scenario.buffers) {
    for (let row = 0; row < buffer._writeIndex; row++) {
      const value = resolveMessage(buffer, row) ?? null;
      messages.push(value);
      if (value === null) continue;
      let index = scenario.staticDictionary.get(value);
      if (index === undefined) {
        index = suffix.get(value);
        if (index === undefined) {
          index = scenario.staticDictionary.size + suffix.size;
          suffix.set(value, index);
          if (collectEstimates) allocatedBytes += byteLength(value);
        }
      }
      indices[messages.length - 1] = index;
    }
  }
  return {
    logical: modelLogical(messages, rowCount),
    counters: collectEstimates
      ? {
          modeledAllocationCountEstimate: 3,
          modeledAllocatedBytesEstimate: allocatedBytes + indices.byteLength,
          modeledCopyCountEstimate: messages.length,
          modeledCopiedBytesEstimate: messages.reduce((sum, value) => sum + byteLength(value), 0),
          modeledDictionaryLookupCountEstimate: messages.length,
          modeledResolvedRowCountEstimate: messages.length,
        }
      : {},
  };
}

interface DirectNumericResult {
  readonly indices: Uint32Array;
  readonly suffix: ReadonlyMap<string, number>;
  readonly dynamicLookupCount: number;
}

function directNumericStaticSuffixModel(scenario: Scenario): DirectNumericResult {
  const rowCount = scenario.buffers.reduce((count, buffer) => count + buffer._writeIndex, 0);
  const indices = new Uint32Array(rowCount);
  const suffix = new Map<string, number>();
  let outputRow = 0;
  let dynamicLookupCount = 0;
  for (const buffer of scenario.buffers) {
    for (let row = 0; row < buffer._writeIndex; row++, outputRow++) {
      const header = buffer._logHeaders[row]!;
      const denseIndex = header >>> 8;
      if (denseIndex !== 0) {
        indices[outputRow] = denseIndex;
        continue;
      }

      const value = buffer.message_values[row];
      if (value == null) continue;
      dynamicLookupCount++;
      let index = suffix.get(value);
      if (index === undefined) {
        index = getVocabularyDictionaryPrefix(buffer._vocabularyGeneration).length + suffix.size;
        suffix.set(value, index);
      }
      indices[outputRow] = index;
    }
  }
  return { indices, suffix, dynamicLookupCount };
}

function validateDirectNumericResult(
  scenario: Scenario,
  result: DirectNumericResult,
  expectedMessages: readonly (string | null)[],
): Counters {
  const prefix = getVocabularyDictionaryPrefix(scenario.root._vocabularyGeneration);
  const prefixValues = Array.from(prefix.valueToDenseIndex.entries())
    .sort((left, right) => left[1] - right[1])
    .map(([value]) => value);
  const dictionary = [...prefixValues, ...result.suffix.keys()];
  const decoded = Array.from(result.indices, (index, row) =>
    expectedMessages[row] === null ? null : (dictionary[index] ?? null),
  );
  if (JSON.stringify(decoded) !== JSON.stringify(expectedMessages)) {
    throw new Error(
      `${scenario.name}: direct numeric static/suffix kernel mismatch ${JSON.stringify({ decoded, expectedMessages, indices: Array.from(result.indices), dictionary })}`,
    );
  }
  const suffixBytes = Array.from(result.suffix.keys()).reduce((sum, value) => sum + byteLength(value), 0);
  return {
    modeledAllocationCountEstimate: 2,
    modeledAllocatedBytesEstimate: suffixBytes + result.indices.byteLength,
    modeledCopyCountEstimate: result.indices.length,
    modeledCopiedBytesEstimate: suffixBytes + result.indices.byteLength,
    modeledDictionaryLookupCountEstimate: result.dynamicLookupCount,
    modeledResolvedRowCountEstimate: result.indices.length,
  };
}

function resolvedChunks(scenario: Scenario): readonly (readonly (string | null)[])[] {
  return scenario.buffers.map((buffer) =>
    Array.from({ length: buffer._writeIndex }, (_, row) => resolveMessage(buffer, row) ?? null),
  );
}

function flattenedOwnershipModel(
  source: readonly (readonly (string | null)[])[],
  collectEstimates = false,
): VariantOutput {
  const messages = source.flat();
  const copiedBytes = collectEstimates ? messages.reduce((sum, value) => sum + byteLength(value), 0) : 0;
  return {
    logical: modelLogical(messages, messages.length),
    counters: collectEstimates
      ? {
          modeledAllocationCountEstimate: 1,
          modeledAllocatedBytesEstimate: copiedBytes,
          modeledCopyCountEstimate: messages.length,
          modeledCopiedBytesEstimate: copiedBytes,
          modeledResolvedRowCountEstimate: messages.length,
        }
      : {},
  };
}

function chunkedArraySliceModel(
  source: readonly (readonly (string | null)[])[],
  collectEstimates = false,
): VariantOutput {
  const chunks = source.slice();
  const rows = chunks.reduce((count, chunk) => count + chunk.length, 0);
  return {
    logical: modelLogical(chunks, rows),
    counters: collectEstimates
      ? {
          modeledAllocationCountEstimate: 1,
          modeledAllocatedBytesEstimate: chunks.length * 8,
          modeledCopyCountEstimate: chunks.length,
          modeledCopiedBytesEstimate: chunks.length * 8,
          modeledResolvedRowCountEstimate: rows,
          modeledLeasedChunkCountEstimate: chunks.length,
        }
      : {},
  };
}

interface Preflight {
  readonly chunks: readonly (readonly (string | null)[])[];
  readonly counters: readonly Counters[];
}

function assertTopologySchemaContract(table: Table, topology: Topology, scenarioName: string): string {
  const fields = table.schema.fields.map(
    (field) => `${field.name}:${String(field.type.typeId)}:${String(field.nullable)}`,
  );
  const uint64ValueFields = fields.filter((field) => field.startsWith('uint64_value:'));
  if (topology === 'depth-3-tree') {
    if (JSON.stringify(uint64ValueFields) !== JSON.stringify(['uint64_value:2:true'])) {
      throw new Error(`${scenarioName}: expected exact tree-only uint64_value:2:true schema field`);
    }
  } else if (uint64ValueFields.length !== 0) {
    throw new Error(`${scenarioName}: unexpected uint64_value field outside tree conversion`);
  }
  // The production tree API intentionally adds the asserted uint64_value field;
  // every other field must match the single-buffer and overflow schema exactly.
  return fields.filter((field) => !field.startsWith('uint64_value:')).join('|');
}

const canonicalMessageSchemas = new Map<string, string>();

function assertMessageArrow(scenario: Scenario, table: Table, expectedMessages: readonly (string | null)[]): void {
  const logical = logicalFromTable(table);
  const schemaSignature = assertTopologySchemaContract(table, scenario.topology, scenario.name);
  const schemaKey = `${scenario.kind}/${scenario.logCount}`;
  const canonicalSchema = canonicalMessageSchemas.get(schemaKey);
  if (canonicalSchema === undefined) canonicalMessageSchemas.set(schemaKey, schemaSignature);
  else if (schemaSignature !== canonicalSchema) {
    throw new Error(
      `${scenario.name}: cross-topology schema/type mismatch actual=${schemaSignature} canonical=${canonicalSchema}`,
    );
  }
  if (
    logical.rows !== expectedMessages.length ||
    JSON.stringify(flattenMessages(logical)) !== JSON.stringify(expectedMessages)
  ) {
    throw new Error(`${scenario.name}: production Arrow decoded rows mismatch`);
  }

  const snapshot = dictionarySnapshot(table, 'message');
  const prefix = getVocabularyDictionaryPrefix(scenario.root._vocabularyGeneration);
  const prefixValues = Array.from(prefix.valueToDenseIndex.entries())
    .sort((left, right) => left[1] - right[1])
    .map(([value]) => value);
  const dynamicSuffix = Array.from(
    new Set(
      expectedMessages.filter(
        (value): value is string => value !== null && !prefix.valueToDenseIndex.has(value),
      ),
    ),
  );
  const dictionary = [...prefixValues, ...dynamicSuffix];
  const expectedIndices = expectedMessages.map((value) => (value === null ? 0 : dictionary.indexOf(value)));
  const expectedBitmapBytes = new Uint8Array(Math.ceil(expectedMessages.length / 8));
  for (let index = 0; index < expectedMessages.length; index++) {
    if (expectedMessages[index] !== null) expectedBitmapBytes[index >> 3]! |= 1 << (index & 7);
  }
  const expectedBitmap = Array.from(expectedBitmapBytes);
  if (JSON.stringify(snapshot.values) !== JSON.stringify(dictionary)) {
    throw new Error(
      `${scenario.name}: message dictionary-values mismatch ${JSON.stringify({ actual: snapshot.values, expected: dictionary })}`,
    );
  }
  if (JSON.stringify(snapshot.indices) !== JSON.stringify(expectedIndices)) {
    throw new Error(`${scenario.name}: message dictionary-indices mismatch`);
  }
  if (JSON.stringify(snapshot.validity) !== JSON.stringify(expectedBitmap)) {
    throw new Error(`${scenario.name}: message validity mismatch`);
  }
  if (snapshot.nullCount !== expectedMessages.filter((value) => value === null).length) {
    throw new Error(`${scenario.name}: message null-count mismatch`);
  }
  if (snapshot.indexArrayName !== 'Uint32Array') {
    throw new Error(`${scenario.name}: message index-width/type mismatch`);
  }
}

function preflightScenario(scenario: Scenario): Preflight {
  const chunks = resolvedChunks(scenario);
  const expectedMessages = chunks.flat();
  const table =
    scenario.topology === 'depth-3-tree'
      ? convertSpanTreeToArrowTable(scenario.root)
      : convertToArrowTable(scenario.root);
  assertMessageArrow(scenario, table, expectedMessages);
  const leased = leasedConversion(scenario).logical;
  const owned = logicalFromTable(table);
  if (JSON.stringify(leased) !== JSON.stringify(owned)) {
    throw new Error(`${scenario.name}: leased Arrow schema/null/topology semantics differ from owned conversion`);
  }
  const currentTwoPass = currentTwoPassModel(scenario, true);
  const prebuiltStatic = prebuiltStaticDictionaryModel(scenario, true);
  const directNumeric = directNumericStaticSuffixModel(scenario);
  const flattened = flattenedOwnershipModel(chunks, true);
  const chunked = chunkedArraySliceModel(chunks, true);
  for (const [index, output] of [currentTwoPass, prebuiltStatic, flattened, chunked].entries()) {
    const logical = output.logical;
    if (
      logical.rows !== expectedMessages.length ||
      JSON.stringify(flattenMessages(logical)) !== JSON.stringify(expectedMessages)
    ) {
      throw new Error(`${scenario.name}: decoded model mismatch for variant ${index + 1}`);
    }
  }
  return {
    chunks,
    counters: [
      {},
      currentTwoPass.counters,
      prebuiltStatic.counters,
      validateDirectNumericResult(scenario, directNumeric, expectedMessages),
      flattened.counters,
      chunked.counters,
    ],
  };
}

function counterLabel(label: string, counters: Counters): string {
  if (counters.modeledCopyCountEstimate === undefined || counters.modeledCopiedBytesEstimate === undefined) {
    return `${label} [copies=not-instrumented]`;
  }
  return `${label} [modeled-copy-estimate=${counters.modeledCopyCountEstimate}, modeled-copied-bytes-estimate=${counters.modeledCopiedBytesEstimate}, modeled-allocation-estimate=${counters.modeledAllocationCountEstimate ?? 0}]`;
}

function registerScenario(scenario: Scenario, preflight: Preflight): void {
  group(`${scenario.name} | buffers=${scenario.buffers.length} | capacity=${scenario.capacity}`, () => {
    bench(counterLabel('current/production-arrow-conversion', preflight.counters[0]!), () =>
      currentConversion(scenario),
    ).baseline(true);
    bench('current/production-leased-arrow [copies=non-borrowable-columns, release=per-iteration]', () =>
      leasedConversion(scenario),
    );
    bench(counterLabel('model/current-two-pass-resolution', preflight.counters[1]!), () =>
      currentTwoPassModel(scenario),
    );
    bench(counterLabel('model/prebuilt-static-dictionary', preflight.counters[2]!), () =>
      prebuiltStaticDictionaryModel(scenario),
    );
    bench(counterLabel('model/direct-numeric-static+explicit-dynamic-suffix', preflight.counters[3]!), () =>
      directNumericStaticSuffixModel(scenario),
    );
    bench(counterLabel('model/js-array-flat-only/not-arrow-ownership', preflight.counters[4]!), () =>
      flattenedOwnershipModel(preflight.chunks),
    );
    bench(counterLabel('model/js-array-slice-only/not-recordbatch-ipc-or-lease', preflight.counters[5]!), () =>
      chunkedArraySliceModel(preflight.chunks),
    );
  });
}

const CLOSED_VALUES = ['OPEN', 'BUSY', 'DONE'] as const;
const STRING_SCHEMA = defineLogSchema({
  closedValue: S.enum(CLOSED_VALUES),
  categoryValue: S.category(),
  textValue: S.text(),
});
const STRING_CONTEXT = defineOpContext({ logSchema: STRING_SCHEMA });
const STRING_FULL_SCHEMA = STRING_CONTEXT.logBinding.logSchema;
const STRING_TRACER = new TestTracer(STRING_CONTEXT, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});
const STRING_ROW_COUNT = 12;

type StringProfileName =
  | 'enum-closed-values'
  | 'category-low-cardinality-repeated'
  | 'category-high-cardinality'
  | 'text-unique'
  | 'null-sparse-strings'
  | 'short-ascii'
  | 'multibyte-utf8'
  | 'long-utf8'
  | 'category-cardinality-0'
  | 'category-cardinality-1'
  | 'category-cardinality-255'
  | 'category-cardinality-256'
  | 'category-cardinality-65535'
  | 'category-cardinality-65536';

interface StringRow {
  readonly closedValue: (typeof CLOSED_VALUES)[number] | null;
  readonly categoryValue: string | null;
  readonly textValue: string | null;
}

interface StringProfile {
  readonly name: StringProfileName;
  readonly rowCount?: number;
  readonly row: (index: number) => StringRow;
}

interface StringScenario {
  readonly name: string;
  readonly topology: Topology;
  readonly profileName: StringProfileName;
  readonly root: AnySpanBuffer;
  readonly buffers: readonly AnySpanBuffer[];
  readonly rows: readonly StringRow[];
  readonly table: Table;
}

const STRING_PROFILES: readonly StringProfile[] = [
  {
    name: 'enum-closed-values',
    row: (index) => ({
      closedValue: CLOSED_VALUES[index % CLOSED_VALUES.length]!,
      categoryValue: `region-${index % 2}`,
      textValue: `enum-row-${index}`,
    }),
  },
  {
    name: 'category-low-cardinality-repeated',
    row: (index) => ({
      closedValue: CLOSED_VALUES[index % CLOSED_VALUES.length]!,
      categoryValue: `region-${index % 3}`,
      textValue: `low-cardinality-row-${index}`,
    }),
  },
  {
    name: 'category-high-cardinality',
    row: (index) => ({
      closedValue: CLOSED_VALUES[index % CLOSED_VALUES.length]!,
      categoryValue: `session-${index.toString().padStart(3, '0')}`,
      textValue: `high-cardinality-row-${index}`,
    }),
  },
  {
    name: 'text-unique',
    row: (index) => ({
      closedValue: CLOSED_VALUES[index % CLOSED_VALUES.length]!,
      categoryValue: `text-bucket-${index % 2}`,
      textValue: `unique-text-value-${index.toString().padStart(3, '0')}`,
    }),
  },
  {
    name: 'null-sparse-strings',
    row: (index) => ({
      closedValue: index % 4 === 0 ? null : CLOSED_VALUES[index % CLOSED_VALUES.length]!,
      categoryValue: index % 3 === 0 ? null : `sparse-category-${index % 2}`,
      textValue: index % 2 === 0 ? null : `sparse-text-${index}`,
    }),
  },
  {
    name: 'short-ascii',
    row: (index) => ({
      closedValue: CLOSED_VALUES[index % CLOSED_VALUES.length]!,
      categoryValue: `c${index % 4}`,
      textValue: `t${index}`,
    }),
  },
  {
    name: 'multibyte-utf8',
    row: (index) => ({
      closedValue: CLOSED_VALUES[index % CLOSED_VALUES.length]!,
      categoryValue: `東京-${index % 3}`,
      textValue: `résumé-🙂-東京-${index}`,
    }),
  },
  {
    name: 'long-utf8',
    row: (index) => ({
      closedValue: CLOSED_VALUES[index % CLOSED_VALUES.length]!,
      categoryValue: `long-${index % 2}`,
      textValue: `payload-${index}-${'λ🙂'.repeat(512)}`,
    }),
  },
];

const CARDINALITY_PROFILES: readonly StringProfile[] = [0, 1, 255, 256, 65_535, 65_536].map((cardinality) => ({
  name: `category-cardinality-${cardinality}` as StringProfileName,
  rowCount: cardinality,
  row: (index: number) => ({
    closedValue: 'OPEN',
    categoryValue: `category-${index.toString().padStart(5, '0')}`,
    textValue: 'constant-text',
  }),
}));

function writeStringRows(buffer: SpanBuffer, rows: readonly StringRow[], offset: number): void {
  for (let localRow = 0; localRow < rows.length; localRow++) {
    const row = rows[localRow]!;
    buffer.timestamp[localRow] = BigInt(offset + localRow + 1);
    buffer.entry_type[localRow] = ENTRY_TYPE_SPAN_START;
    buffer.message(localRow, `string-row-${offset + localRow}`);
    if (row.closedValue !== null) buffer.closedValue(localRow, CLOSED_VALUES.indexOf(row.closedValue));
    if (row.categoryValue !== null) buffer.categoryValue(localRow, row.categoryValue);
    if (row.textValue !== null) buffer.textValue(localRow, row.textValue);
  }
  buffer._writeIndex = rows.length;
}

function makeStringScenario(profile: StringProfile, topology: Topology): StringScenario {
  const rows = Array.from({ length: profile.rowCount ?? STRING_ROW_COUNT }, (_, index) => profile.row(index));
  const metadata = metadataForScenario(`strings-${profile.name}-${topology}`);
  const traceRoot = createTraceRoot(createTraceId(`arrow-strings-${profile.name}-${topology}`), STRING_TRACER);
  const capacity = topology === 'overflow-capacity' ? 8 : Math.max(64, rows.length);
  const root = createSpanBuffer(STRING_FULL_SCHEMA, traceRoot, metadata, capacity);
  const buffers: AnySpanBuffer[] = [root];

  if (topology === 'single') {
    writeStringRows(root, rows, 0);
  } else if (topology === 'overflow-capacity') {
    writeStringRows(root, rows.slice(0, capacity), 0);
    const overflow = createOverflowBuffer(root);
    writeStringRows(overflow, rows.slice(capacity), capacity);
    buffers.push(overflow);
  } else {
    const BufferClass = getSpanBufferClass(STRING_FULL_SCHEMA);
    writeStringRows(root, rows.slice(0, 4), 0);
    let parent = root;
    for (let depth = 1; depth < 3; depth++) {
      const child = createChildSpanBuffer(parent, BufferClass, metadata, metadata, capacity) as SpanBuffer;
      writeStringRows(child, rows.slice(depth * 4, depth * 4 + 4), depth * 4);
      buffers.push(child);
      parent = child;
    }
  }

  const table = topology === 'depth-3-tree' ? convertSpanTreeToArrowTable(root) : convertToArrowTable(root);
  return {
    name: `arrow-strings/${topology}/${profile.name}`,
    profileName: profile.name,
    topology,
    root,
    buffers,
    rows,
    table,
  };
}

function dictionarySnapshot(
  table: Table,
  columnName: string,
): {
  readonly typeId: string;
  readonly nullable: boolean;
  readonly values: readonly string[];
  readonly indices: readonly number[];
  readonly indexArrayName: string;
  readonly indexByteWidth: number;
  readonly validity: readonly number[];
  readonly nullCount: number;
  readonly decoded: readonly (string | null)[];
  readonly utf8Bytes: readonly number[];
  readonly utf8Offsets: readonly number[];
} {
  const field = table.schema.fields.find((candidate) => candidate.name === columnName);
  const column = table.getChild(columnName);
  if (!field || !column) throw new Error(`Missing Arrow string column ${columnName}`);
  const batch = column.data[0];
  if (!batch) throw new Error(`Missing Arrow data batch for ${columnName}`);
  const dictionary = Reflect.get(batch, 'dictionary');
  if (!dictionary || typeof dictionary.length !== 'number' || typeof dictionary.get !== 'function') {
    throw new Error(`Expected dictionary Arrow type for ${columnName}`);
  }
  if (!ArrayBuffer.isView(batch.values)) throw new Error(`Missing dictionary indices for ${columnName}`);
  const dictionaryBatches = dictionary.data as readonly {
    readonly length: number;
    readonly values: Uint8Array;
    readonly offsets: Int32Array;
  }[];
  if (
    dictionaryBatches.length === 0 ||
    dictionaryBatches.some(
      (dictionaryBatch) =>
        !(dictionaryBatch.values instanceof Uint8Array) || !(dictionaryBatch.offsets instanceof Int32Array),
    )
  ) {
    throw new Error(`Missing raw UTF-8 dictionary buffers for ${columnName}`);
  }
  const values = Array.from({ length: dictionary.length }, (_, index) => String(dictionary.get(index)));
  const expectedOffsets = new Int32Array(values.length + 1);
  const encoded = values.map((value) => textEncoder.encode(value));
  let totalBytes = 0;
  for (let index = 0; index < encoded.length; index++) {
    totalBytes += encoded[index]!.byteLength;
    expectedOffsets[index + 1] = totalBytes;
  }
  const expectedBytes = new Uint8Array(totalBytes);
  const physicalBytes = new Uint8Array(totalBytes);
  const physicalOffsets = new Int32Array(values.length + 1);
  let byteOffset = 0;
  let valueOffset = 0;
  for (const dictionaryBatch of dictionaryBatches) {
    const batchByteLength = dictionaryBatch.offsets[dictionaryBatch.length];
    if (batchByteLength === undefined) throw new Error(`${columnName}: missing dictionary byte boundary`);
    physicalBytes.set(dictionaryBatch.values.subarray(0, batchByteLength), byteOffset);
    for (let index = 1; index <= dictionaryBatch.length; index++) {
      physicalOffsets[valueOffset + index] = byteOffset + dictionaryBatch.offsets[index]!;
    }
    byteOffset += batchByteLength;
    valueOffset += dictionaryBatch.length;
  }
  byteOffset = 0;
  for (const bytes of encoded) {
    expectedBytes.set(bytes, byteOffset);
    byteOffset += bytes.byteLength;
  }
  if (JSON.stringify(Array.from(physicalBytes)) !== JSON.stringify(Array.from(expectedBytes))) {
    throw new Error(`${columnName}: raw UTF-8 dictionary bytes mismatch`);
  }
  if (JSON.stringify(Array.from(physicalOffsets)) !== JSON.stringify(Array.from(expectedOffsets))) {
    throw new Error(`${columnName}: raw UTF-8 dictionary offsets mismatch`);
  }
  const validity = batch.validity instanceof Uint8Array ? Array.from(batch.validity) : [];
  return {
    typeId: String(field.type.typeId),
    nullable: field.nullable,
    values,
    indices: Array.from(batch.values as Uint8Array | Uint16Array | Uint32Array),
    indexArrayName: batch.values.constructor.name,
    indexByteWidth: batch.values.BYTES_PER_ELEMENT,
    validity,
    nullCount: column.nullCount,
    decoded: Array.from({ length: table.numRows }, (_, index) => {
      const value = column.get(index);
      return value == null ? null : String(value);
    }),
    utf8Bytes: Array.from(physicalBytes),
    utf8Offsets: Array.from(physicalOffsets),
  };
}

function expectedDictionary(values: readonly (string | null)[], sorted: boolean): readonly string[] {
  const unique = Array.from(new Set(values.filter((value): value is string => value !== null)));
  return sorted ? unique.sort() : unique;
}

function expectedValidity(values: readonly (string | null)[]): readonly number[] {
  const bitmap = new Uint8Array(Math.ceil(values.length / 8));
  bitmap.fill(0xff);
  for (let index = 0; index < values.length; index++) {
    if (values[index] === null) bitmap[index >> 3]! &= ~(1 << (index & 7));
  }
  return Array.from(bitmap);
}

const canonicalStringSchemas = new Map<StringProfileName, string>();

function assertStringScenario(scenario: StringScenario): readonly (readonly StringRow[])[] {
  const schema = assertTopologySchemaContract(scenario.table, scenario.topology, scenario.name);
  const canonicalSchema = canonicalStringSchemas.get(scenario.profileName);
  if (canonicalSchema === undefined) canonicalStringSchemas.set(scenario.profileName, schema);
  else if (schema !== canonicalSchema) throw new Error(`${scenario.name}: cross-topology Arrow schema/type mismatch`);
  if (scenario.table.numRows !== scenario.rows.length) throw new Error(`${scenario.name}: Arrow row-count mismatch`);
  if (scenario.rows.length === 0) {
    for (const columnName of ['closedValue', 'categoryValue', 'textValue'] as const) {
      if (scenario.table.getChild(columnName)) {
        throw new Error(`${scenario.name}/${columnName}: zero-row lazy column should be omitted`);
      }
    }
    return scenario.buffers.map(() => []);
  }

  const expectedColumns = {
    closedValue: scenario.rows.map((row) => row.closedValue),
    categoryValue: scenario.rows.map((row) => row.categoryValue),
    textValue: scenario.rows.map((row) => row.textValue),
  };
  const snapshots = {
    closedValue: dictionarySnapshot(scenario.table, 'closedValue'),
    categoryValue: dictionarySnapshot(scenario.table, 'categoryValue'),
    textValue: dictionarySnapshot(scenario.table, 'textValue'),
  };
  const dictionaryTypeId = snapshots.closedValue.typeId;
  for (const columnName of ['closedValue', 'categoryValue', 'textValue'] as const) {
    const expectedValues = expectedColumns[columnName];
    const snapshot = snapshots[columnName];
    if (snapshot.typeId !== dictionaryTypeId) throw new Error(`${scenario.name}/${columnName}: Arrow type mismatch`);
    const expectedNullCount = expectedValues.filter((value) => value === null).length;
    if (snapshot.nullCount !== expectedNullCount)
      throw new Error(`${scenario.name}/${columnName}: null-count mismatch`);
    const expectedBitmap = expectedValidity(expectedValues);
    const bitmapMatches =
      JSON.stringify(snapshot.validity) === JSON.stringify(expectedBitmap) ||
      (expectedNullCount === 0 && snapshot.validity.length === 0);
    if (!bitmapMatches) {
      throw new Error(
        `${scenario.name}/${columnName}: null-bitmap mismatch actual=${JSON.stringify(snapshot.validity)} expected=${JSON.stringify(expectedBitmap)}`,
      );
    }
    if (JSON.stringify(snapshot.decoded) !== JSON.stringify(expectedValues)) {
      throw new Error(`${scenario.name}/${columnName}: decoded row-order mismatch`);
    }
  }

  const expectedDictionaries = {
    closedValue: CLOSED_VALUES,
    categoryValue: expectedDictionary(expectedColumns.categoryValue, true),
    textValue: expectedDictionary(expectedColumns.textValue, false),
  };
  for (const columnName of ['closedValue', 'categoryValue', 'textValue'] as const) {
    const snapshot = snapshots[columnName];
    const dictionary = expectedDictionaries[columnName];
    if (JSON.stringify(snapshot.values) !== JSON.stringify(dictionary)) {
      throw new Error(`${scenario.name}/${columnName}: dictionary-values mismatch`);
    }
    const expectedIndices = expectedColumns[columnName].map((value) =>
      value === null ? 0 : dictionary.indexOf(value),
    );
    if (JSON.stringify(snapshot.indices) !== JSON.stringify(expectedIndices)) {
      throw new Error(`${scenario.name}/${columnName}: dictionary-indices mismatch`);
    }
    const expectedIndexArray =
      columnName === 'textValue' && dictionary.length === 0
        ? 'Uint32Array'
        : dictionary.length <= 255
          ? 'Uint8Array'
          : dictionary.length <= 65_535
            ? 'Uint16Array'
            : 'Uint32Array';
    if (snapshot.indexArrayName !== expectedIndexArray) {
      throw new Error(`${scenario.name}/${columnName}: dictionary index-width/type mismatch`);
    }
    const expectedByteWidth = expectedIndexArray === 'Uint8Array' ? 1 : expectedIndexArray === 'Uint16Array' ? 2 : 4;
    if (snapshot.indexByteWidth !== expectedByteWidth) {
      throw new Error(`${scenario.name}/${columnName}: dictionary index byte-width mismatch`);
    }
  }

  const messages = scenario.table.getChild('message');
  if (!messages) throw new Error(`${scenario.name}: missing message column`);
  for (let index = 0; index < scenario.rows.length; index++) {
    if (messages.get(index) !== `string-row-${index}`) {
      throw new Error(`${scenario.name}: tree/chain row-order mismatch`);
    }
  }

  let offset = 0;
  return scenario.buffers.map((buffer) => {
    const chunk = scenario.rows.slice(offset, offset + buffer._writeIndex);
    offset += buffer._writeIndex;
    return chunk;
  });
}

function registerStringScenario(scenario: StringScenario, chunks: readonly (readonly StringRow[])[]): void {
  const copiedRows = scenario.rows.length;
  const leasedChunks = chunks.length;
  group(`${scenario.name} | buffers=${scenario.buffers.length}`, () => {
    bench('current/production-arrow-conversion [copies=not-instrumented]', () =>
      scenario.topology === 'depth-3-tree'
        ? convertSpanTreeToArrowTable(scenario.root)
        : convertToArrowTable(scenario.root),
    ).baseline(true);
    bench(`model/js-array-flat-only/not-arrow-ownership [modeled-copy-estimate=${copiedRows} row-references]`, () =>
      chunks.flat(),
    );
    bench(
      `model/js-array-slice-only/not-recordbatch-ipc-or-lease [modeled-copy-estimate=${leasedChunks} chunk-references]`,
      () => chunks.slice(),
    );
  });
}

const QUICK = process.argv.includes('--quick');
const JSON_OUTPUT = process.argv.some((argument) => argument === '--json' || argument.startsWith('--json='));
const MARKDOWN_OUTPUT = process.argv.includes('--markdown');
const MITATA_FORMAT = JSON_OUTPUT ? 'json' : MARKDOWN_OUTPUT ? 'markdown' : 'mitata';
const selectedLogCounts = QUICK ? ([1] as const) : LOG_COUNTS;
const selectedTopologies = TOPOLOGIES; // Quick still exercises schema parity across single, overflow, and tree.

for (const topology of selectedTopologies) {
  for (const kind of MESSAGE_KINDS) {
    for (const logCount of selectedLogCounts) {
      const scenario = await makeScenario(kind, logCount, topology);
      const preflight = preflightScenario(scenario);
      summary(() => registerScenario(scenario, preflight));
    }
  }
}

const stringSelections: readonly (readonly [StringProfile, Topology])[] = QUICK
  ? [
      [STRING_PROFILES[0]!, 'single'],
      [STRING_PROFILES[0]!, 'overflow-capacity'],
      [STRING_PROFILES[0]!, 'depth-3-tree'],
      [STRING_PROFILES[1]!, 'single'],
      [STRING_PROFILES[2]!, 'overflow-capacity'],
      [STRING_PROFILES[3]!, 'depth-3-tree'],
      [STRING_PROFILES[4]!, 'overflow-capacity'],
      [STRING_PROFILES[5]!, 'single'],
      [STRING_PROFILES[6]!, 'depth-3-tree'],
      [STRING_PROFILES[7]!, 'overflow-capacity'],
      [CARDINALITY_PROFILES[0]!, 'single'],
      [CARDINALITY_PROFILES[1]!, 'single'],
      [CARDINALITY_PROFILES[2]!, 'single'],
      [CARDINALITY_PROFILES[3]!, 'single'],
    ]
  : [
      ...STRING_PROFILES.flatMap((profile) => TOPOLOGIES.map((topology) => [profile, topology] as const)),
      ...CARDINALITY_PROFILES.map((profile) => [profile, 'single'] as const),
    ];

for (const [profile, topology] of stringSelections) {
  const scenario = makeStringScenario(profile, topology);
  const chunks = assertStringScenario(scenario);
  summary(() => registerStringScenario(scenario, chunks));
}

await run({ format: MITATA_FORMAT, throw: true });
