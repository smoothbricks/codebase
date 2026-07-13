import type { Table } from '@uwdata/flechette';
import { bench, do_not_optimize, group, run, summary } from 'mitata';
import type { ArrowLease } from '../src/lib/arrow/lease.js';
import {
  convertSpanTreeToLeasedArrowTable,
  convertToLeasedArrowTable,
} from '../src/lib/convertToArrow.js';
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
  RUNTIME_HINT_SPAN,
  type MessageLayoutFamily,
} from '../src/lib/runtimeHint.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ENTRY_TYPE_SPAN_START } from '../src/lib/schema/systemSchema.js';
import { createTraceId } from '../src/lib/traceId.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { iterateSpanTree } from '../src/lib/traceTopology.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';
import type { AnySpanBuffer, SpanBuffer } from '../src/lib/types.js';
import { registerBenchmarkVocabulary } from './vocabularyFixture.js';

const MESSAGE_KINDS: readonly MessageKind[] = Object.freeze(['static', 'dynamic', 'mixed']);
const LOG_COUNTS: readonly number[] = Object.freeze([0, 1, 50]);
const TOPOLOGIES: readonly Topology[] = Object.freeze(['single', 'depth-3-tree', 'overflow-capacity']);
const TEMPLATES: readonly string[] = Object.freeze([
  'request accepted',
  'cache lookup complete',
  'response committed',
  'request cleanup complete',
]);
const TEMPLATE_BINDING = registerBenchmarkVocabulary(TEMPLATES);
const USER_SCHEMA = defineLogSchema({ marker: S.category() });
const OP_CONTEXT = defineOpContext({ logSchema: USER_SCHEMA });
const RUNTIME_SCHEMA = OP_CONTEXT.logBinding.logSchema;
type RuntimeContext = OpContextOf<typeof OP_CONTEXT>;
const textEncoder = new TextEncoder();

type MessageKind = 'static' | 'dynamic' | 'mixed';
type Topology = 'single' | 'depth-3-tree' | 'overflow-capacity';
type RunFormat = 'json' | 'markdown' | 'mitata';

type DenseTemplateLogger = {
  _infoTemplate(vocabularyIndex: number): unknown;
};

type LogicalOutput = {
  readonly schema: readonly string[];
  readonly rows: number;
  readonly messages: readonly (string | null)[];
  readonly nulls: readonly number[];
};

type Counters = {
  readonly modeledCopyCountEstimate?: number;
  readonly modeledCopiedBytesEstimate?: number;
  readonly modeledLookupCountEstimate?: number;
  readonly modeledLeasedChunkCountEstimate?: number;
};

type Scenario = {
  readonly name: string;
  readonly kind: MessageKind;
  readonly logCount: number;
  readonly topology: Topology;
  readonly capacity: number;
  readonly root: AnySpanBuffer;
  readonly buffers: readonly AnySpanBuffer[];
  readonly plan: CallsitePlan<typeof RUNTIME_SCHEMA, RuntimeContext>;
};

type Preflight = {
  readonly expectedMessages: readonly (string | null)[];
  readonly chunks: readonly (readonly (string | null)[])[];
  readonly modelCounters: Counters;
  readonly chunkCounters: Counters;
};

function requireDenseTemplateLogger(context: object): DenseTemplateLogger {
  const logger = Reflect.get(context, '_spanLogger');
  if (typeof logger !== 'object' || logger === null || typeof Reflect.get(logger, '_infoTemplate') !== 'function') {
    throw new Error('Expected a real SpanContext-owned dense-template logger');
  }
  return logger;
}

function valueAt<T>(values: readonly T[], index: number, label: string): T {
  const value = values[index];
  if (value === undefined) throw new RangeError(`${label} index ${index} is out of range`);
  return value;
}

function familyFor(kind: MessageKind): MessageLayoutFamily {
  if (kind === 'static') return 'static-only';
  if (kind === 'dynamic') return 'dynamic-only';
  return 'mixed';
}

function familyBits(family: MessageLayoutFamily): number {
  if (family === 'static-only') return RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY;
  if (family === 'dynamic-only') return RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY;
  return RUNTIME_HINT_MESSAGE_LAYOUT_MIXED;
}

function runtimeHint(kind: MessageKind, capacity: number): number {
  return (
    RUNTIME_HINT_ANALYZED_VALID |
    RUNTIME_HINT_LOG |
    RUNTIME_HINT_RESULT |
    RUNTIME_HINT_SPAN |
    familyBits(familyFor(kind)) |
    capacity
  );
}

function writeLogs(context: object, kind: MessageKind, count: number): void {
  const logger = Reflect.get(context, 'log');
  if (typeof logger !== 'object' || logger === null) throw new Error('SpanContext omitted its production logger');
  const dynamicInfo = Reflect.get(logger, 'info');
  if (typeof dynamicInfo !== 'function') throw new Error('SpanContext logger omitted info()');
  const dense = kind === 'dynamic' ? undefined : requireDenseTemplateLogger(context);
  for (let index = 0; index < count; index++) {
    if (kind === 'static' || (kind === 'mixed' && (index & 1) === 0)) {
      const binding = TEMPLATE_BINDING[index % TEMPLATES.length];
      if (binding === undefined || dense === undefined) throw new Error(`Missing dense template binding ${index}`);
      dense._infoTemplate(binding);
    } else {
      Reflect.apply(dynamicInfo, logger, [`dynamic request ${index % 11}`]);
    }
  }
}

async function makeScenario(kind: MessageKind, logCount: number, topology: Topology): Promise<Scenario> {
  const capacity = topology === 'overflow-capacity' ? 8 : 64;
  const hint = runtimeHint(kind, capacity);
  const leaf = OP_CONTEXT.defineOp(
    `${kind}-${logCount}-${topology}-leaf`,
    (ctx) => {
      writeLogs(ctx, kind, logCount);
      return ctx.ok(null).message('leaf-complete');
    },
    undefined,
    { runtimeHint: hint },
  );
  const middle = OP_CONTEXT.defineOp(
    `${kind}-${logCount}-${topology}-middle`,
    async (ctx) => {
      writeLogs(ctx, kind, logCount);
      const result = await ctx.span('leaf-span', leaf);
      if (!result.success) throw new Error('Leaf scenario span failed');
      return ctx.ok(null).message('middle-complete');
    },
    undefined,
    { runtimeHint: hint },
  );
  const rootOp = OP_CONTEXT.defineOp(
    `${kind}-${logCount}-${topology}-root`,
    async (ctx) => {
      writeLogs(ctx, kind, logCount);
      if (topology === 'depth-3-tree') {
        const result = await ctx.span('middle-span', middle);
        if (!result.success) throw new Error('Middle scenario span failed');
      }
      return ctx.ok(null).message('root-complete');
    },
    undefined,
    { runtimeHint: hint },
  );
  const tracer = new TestTracer(OP_CONTEXT, { bufferStrategy: new JsBufferStrategy(), createTraceRoot });
  const result = await tracer.trace(`root-${kind}-${logCount}-${topology}`, rootOp);
  if (!result.success) throw new Error(`Trace failed for ${kind}/${logCount}/${topology}`);
  const root = tracer.rootBuffers[0];
  if (root === undefined) throw new Error(`Trace produced no root buffer for ${kind}/${logCount}/${topology}`);
  const buffers = Array.from(iterateSpanTree(root)).filter((buffer) => buffer._writeIndex !== 0);
  if (rootOp.callsitePlan.messageLayoutFamily !== familyFor(kind)) {
    throw new Error(`${kind}: CallsitePlan selected ${rootOp.callsitePlan.messageLayoutFamily}`);
  }
  return {
    name: `arrow-flush/${topology}/${kind}/${logCount}-logs`,
    kind,
    logCount,
    topology,
    capacity,
    root,
    buffers,
    plan: rootOp.callsitePlan,
  };
}

function leaseFor(root: AnySpanBuffer, topology: Topology): ArrowLease {
  return topology === 'depth-3-tree'
    ? convertSpanTreeToLeasedArrowTable(root)
    : convertToLeasedArrowTable(root);
}

function logicalFromTable(table: Table): LogicalOutput {
  const schema = table.schema.fields.map(
    (field) => `${field.name}:${String(field.type.typeId)}:${String(field.nullable)}`,
  );
  const messagesColumn = table.getChild('message');
  if (messagesColumn === null) throw new Error('Arrow conversion omitted the message column');
  const messages = Array.from({ length: table.numRows }, (_, row) => {
    const value = messagesColumn.get(row);
    return value == null ? null : String(value);
  });
  const nulls = table.schema.fields.map((field) => {
    const column = table.getChild(field.name);
    if (column === null) throw new Error(`Arrow conversion omitted schema field ${field.name}`);
    return column.nullCount;
  });
  return { schema, rows: table.numRows, messages, nulls };
}

function productionConversion(scenario: Scenario): LogicalOutput {
  const lease = leaseFor(scenario.root, scenario.topology);
  try {
    return logicalFromTable(lease.table);
  } finally {
    lease.release();
  }
}

function resolvedChunks(buffers: readonly AnySpanBuffer[]): readonly (readonly (string | null)[])[] {
  return buffers.map((buffer) =>
    Array.from({ length: buffer._writeIndex }, (_, row) => resolveMessage(buffer, row) ?? null),
  );
}

function twoPassModel(buffers: readonly AnySpanBuffer[]): readonly (string | null)[] {
  const messages: (string | null)[] = [];
  for (const buffer of buffers) {
    for (let row = 0; row < buffer._writeIndex; row++) messages.push(resolveMessage(buffer, row) ?? null);
  }
  const dictionary = new Map<string, number>();
  const indices = new Uint32Array(messages.length);
  for (let row = 0; row < messages.length; row++) {
    const message = messages[row];
    if (message === undefined || message === null) continue;
    let index = dictionary.get(message);
    if (index === undefined) {
      index = dictionary.size;
      dictionary.set(message, index);
    }
    indices[row] = index;
  }
  do_not_optimize(indices);
  return messages;
}

function byteLength(values: readonly (string | null)[]): number {
  let bytes = 0;
  for (const value of values) if (value !== null) bytes += textEncoder.encode(value).byteLength;
  return bytes;
}

function assertMessageScenario(scenario: Scenario): Preflight {
  const chunks = resolvedChunks(scenario.buffers);
  const expectedMessages = chunks.flat();
  const topology = scenario.root._traceRoot._topology;
  const leasesBefore = topology.leaseCount;
  const lease = leaseFor(scenario.root, scenario.topology);
  if (lease.released) throw new Error(`${scenario.name}: new ArrowLease was already released`);
  const logical = logicalFromTable(lease.table);
  if (topology.leaseCount !== leasesBefore + 1) throw new Error(`${scenario.name}: ArrowLease did not pin topology`);
  if (JSON.stringify(logical.messages) !== JSON.stringify(expectedMessages)) {
    throw new Error(`${scenario.name}: leased Arrow row order or messages mismatch`);
  }
  const messageColumn = lease.table.getChild('message');
  if (messageColumn === null) throw new Error(`${scenario.name}: leased table omitted message column`);
  lease.release();
  lease.release();
  if (!lease.released || topology.leaseCount !== leasesBefore) {
    throw new Error(`${scenario.name}: idempotent ArrowLease release did not unpin topology`);
  }
  if (messageColumn.length !== expectedMessages.length) {
    throw new Error(`${scenario.name}: released Arrow table lost owned output`);
  }
  const modeled = twoPassModel(scenario.buffers);
  if (JSON.stringify(modeled) !== JSON.stringify(expectedMessages)) {
    throw new Error(`${scenario.name}: two-pass model checksum mismatch`);
  }
  return {
    expectedMessages,
    chunks,
    modelCounters: {
      modeledCopyCountEstimate: expectedMessages.length,
      modeledCopiedBytesEstimate: byteLength(expectedMessages),
      modeledLookupCountEstimate: expectedMessages.length,
    },
    chunkCounters: {
      modeledCopyCountEstimate: chunks.length,
      modeledCopiedBytesEstimate: chunks.length * 8,
      modeledLeasedChunkCountEstimate: chunks.length,
    },
  };
}

function counterLabel(label: string, counters: Counters): string {
  if (counters.modeledCopyCountEstimate === undefined) return `${label} [copies=not-instrumented]`;
  return `${label} [modeled-copy-estimate=${counters.modeledCopyCountEstimate}, modeled-copied-bytes-estimate=${counters.modeledCopiedBytesEstimate ?? 0}]`;
}

function registerScenario(scenario: Scenario, preflight: Preflight): void {
  group(`${scenario.name} | buffers=${scenario.buffers.length} | family=${scenario.plan.messageLayoutFamily}`, () => {
    summary(() => {
      bench('production/leased-arrow-conversion+release', () => {
        do_not_optimize(productionConversion(scenario));
      }).baseline(true);
      bench(counterLabel('model/two-pass-resolution', preflight.modelCounters), () => {
        do_not_optimize(twoPassModel(scenario.buffers));
      });
      bench(counterLabel('model/chunk-reference-slice', preflight.chunkCounters), () => {
        do_not_optimize(preflight.chunks.slice());
      });
    });
  });
}

const CLOSED_VALUES: readonly string[] = Object.freeze(['OPEN', 'BUSY', 'DONE']);
const STRING_SCHEMA = defineLogSchema({
  closedValue: S.enum(CLOSED_VALUES),
  categoryValue: S.category(),
  textValue: S.text(),
});
const STRING_CONTEXT = defineOpContext({ logSchema: STRING_SCHEMA });
const STRING_RUNTIME_SCHEMA = STRING_CONTEXT.logBinding.logSchema;
const STRING_STRATEGY = new JsBufferStrategy<typeof STRING_RUNTIME_SCHEMA>();
const STRING_TRACER = new TestTracer(STRING_CONTEXT, { bufferStrategy: STRING_STRATEGY, createTraceRoot });
const STRING_ROW_COUNT = 12;

type StringRow = {
  readonly closedValue: string | null;
  readonly categoryValue: string | null;
  readonly textValue: string | null;
};

type StringProfile = {
  readonly name: string;
  readonly rowCount?: number;
  row(index: number): StringRow;
};

type StringScenario = {
  readonly name: string;
  readonly topology: Topology;
  readonly root: SpanBuffer<typeof STRING_RUNTIME_SCHEMA>;
  readonly buffers: readonly SpanBuffer<typeof STRING_RUNTIME_SCHEMA>[];
  readonly rows: readonly StringRow[];
};

function closedValue(index: number): string {
  return valueAt(CLOSED_VALUES, index % CLOSED_VALUES.length, 'closed values');
}

const STRING_PROFILES: readonly StringProfile[] = Object.freeze([
  { name: 'enum-closed-values', row: (index) => ({ closedValue: closedValue(index), categoryValue: `region-${index % 2}`, textValue: `enum-${index}` }) },
  { name: 'category-low-cardinality-repeated', row: (index) => ({ closedValue: closedValue(index), categoryValue: `region-${index % 3}`, textValue: `low-${index}` }) },
  { name: 'category-high-cardinality', row: (index) => ({ closedValue: closedValue(index), categoryValue: `session-${index}`, textValue: `high-${index}` }) },
  { name: 'text-unique', row: (index) => ({ closedValue: closedValue(index), categoryValue: `bucket-${index % 2}`, textValue: `unique-${index}` }) },
  { name: 'null-sparse-strings', row: (index) => ({ closedValue: index % 4 === 0 ? null : closedValue(index), categoryValue: index % 3 === 0 ? null : `sparse-${index % 2}`, textValue: index % 2 === 0 ? null : `text-${index}` }) },
  { name: 'short-ascii', row: (index) => ({ closedValue: closedValue(index), categoryValue: `c${index % 4}`, textValue: `t${index}` }) },
  { name: 'multibyte-utf8', row: (index) => ({ closedValue: closedValue(index), categoryValue: `東京-${index % 3}`, textValue: `résumé-🙂-東京-${index}` }) },
  { name: 'long-utf8', row: (index) => ({ closedValue: closedValue(index), categoryValue: `long-${index % 2}`, textValue: `payload-${index}-${'λ🙂'.repeat(512)}` }) },
]);

const CARDINALITY_PROFILES: readonly StringProfile[] = Object.freeze(
  [0, 1, 255, 256, 65_535, 65_536].map((cardinality) => ({
    name: `category-cardinality-${cardinality}`,
    rowCount: cardinality,
    row: (index: number) => ({ closedValue: 'OPEN', categoryValue: `category-${index}`, textValue: 'constant-text' }),
  })),
);

function stringPlan(
  capacity: number,
): CallsitePlan<typeof STRING_RUNTIME_SCHEMA, OpContextOf<typeof STRING_CONTEXT>> {
  const encodedCapacity = capacity <= 0xffff ? capacity : 0;
  return STRING_CONTEXT.defineOp(
    `arrow-string-layout-${capacity}`,
    (ctx) => ctx.ok(null),
    undefined,
    {
      runtimeHint:
        RUNTIME_HINT_ANALYZED_VALID |
        RUNTIME_HINT_RESULT |
        RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY |
        encodedCapacity,
    },
  ).callsitePlan;
}

function writeStringRows(
  buffer: SpanBuffer<typeof STRING_RUNTIME_SCHEMA>,
  rows: readonly StringRow[],
  offset: number,
): void {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new Error('String scenario requires the entry_type lane');
  for (let localRow = 0; localRow < rows.length; localRow++) {
    const row = rows[localRow];
    if (row === undefined) throw new RangeError(`Missing string row ${localRow}`);
    buffer.timestamp[localRow] = BigInt(offset + localRow + 1);
    entryTypes[localRow] = ENTRY_TYPE_SPAN_START;
    buffer.message(localRow, `string-row-${offset + localRow}`);
    if (row.closedValue !== null) buffer.closedValue(localRow, CLOSED_VALUES.indexOf(row.closedValue));
    if (row.categoryValue !== null) buffer.categoryValue(localRow, row.categoryValue);
    if (row.textValue !== null) buffer.textValue(localRow, row.textValue);
  }
  buffer._writeIndex = rows.length;
}

function makeStringScenario(profile: StringProfile, topology: Topology): StringScenario {
  const rows = Array.from({ length: profile.rowCount ?? STRING_ROW_COUNT }, (_, index) => profile.row(index));
  const capacity = topology === 'overflow-capacity' ? 8 : Math.max(64, rows.length);
  const plan = stringPlan(capacity);
  const traceRoot = createTraceRoot(createTraceId(`arrow-strings-${profile.name}-${topology}`), STRING_TRACER);
  const root = STRING_STRATEGY.createSpanBuffer(
    STRING_RUNTIME_SCHEMA,
    traceRoot,
    plan.metadata,
    capacity,
    plan.SpanBufferClass,
  );
  const buffers: Array<SpanBuffer<typeof STRING_RUNTIME_SCHEMA>> = [root];
  if (topology === 'single') {
    writeStringRows(root, rows, 0);
  } else if (topology === 'overflow-capacity') {
    writeStringRows(root, rows.slice(0, capacity), 0);
    if (rows.length > capacity) {
      const overflow = STRING_STRATEGY.createOverflowBuffer(root);
      writeStringRows(overflow, rows.slice(capacity), capacity);
      buffers.push(overflow);
    }
  } else {
    writeStringRows(root, rows.slice(0, 4), 0);
    let parent = root;
    for (let depth = 1; depth < 3; depth++) {
      const child = STRING_STRATEGY.createChildSpanBuffer(
        parent,
        plan.metadata,
        plan.metadata,
        capacity,
        STRING_RUNTIME_SCHEMA,
        plan.SpanBufferClass,
      );
      writeStringRows(child, rows.slice(depth * 4, depth * 4 + 4), depth * 4);
      buffers.push(child);
      parent = child;
    }
  }
  return { name: `arrow-strings/${topology}/${profile.name}`, topology, root, buffers, rows };
}

function requireDictionaryValues(table: Table, columnName: string): readonly string[] {
  const column = table.getChild(columnName);
  if (column === null) throw new Error(`Missing Arrow string column ${columnName}`);
  const batch = column.data[0];
  if (batch === undefined) throw new Error(`Missing Arrow data batch for ${columnName}`);
  const dictionary = Reflect.get(batch, 'dictionary');
  if (typeof dictionary !== 'object' || dictionary === null) throw new Error(`${columnName} is not dictionary encoded`);
  const length = Reflect.get(dictionary, 'length');
  const getter = Reflect.get(dictionary, 'get');
  if (typeof length !== 'number' || typeof getter !== 'function') throw new Error(`${columnName} dictionary is invalid`);
  return Array.from({ length }, (_, index) => String(Reflect.apply(getter, dictionary, [index])));
}

function assertStringScenario(scenario: StringScenario): readonly (readonly StringRow[])[] {
  const lease = leaseFor(scenario.root, scenario.topology);
  try {
    const table = lease.table;
    if (table.numRows !== scenario.rows.length) throw new Error(`${scenario.name}: Arrow row count mismatch`);
    if (scenario.rows.length === 0) return scenario.buffers.map(() => []);
    const columns: readonly (readonly [string, readonly (string | null)[]])[] = Object.freeze([
      ['closedValue', scenario.rows.map((row) => row.closedValue)],
      ['categoryValue', scenario.rows.map((row) => row.categoryValue)],
      ['textValue', scenario.rows.map((row) => row.textValue)],
    ]);
    for (const [name, expected] of columns) {
      const column = table.getChild(name);
      if (column === null) throw new Error(`${scenario.name}: missing ${name}`);
      const decoded = Array.from({ length: table.numRows }, (_, row) => {
        const value = column.get(row);
        return value == null ? null : String(value);
      });
      if (JSON.stringify(decoded) !== JSON.stringify(expected)) throw new Error(`${scenario.name}: ${name} mismatch`);
      const dictionary = requireDictionaryValues(table, name);
      const expectedDictionary = Array.from(new Set(expected.filter((value): value is string => value !== null)));
      if (name === 'categoryValue') expectedDictionary.sort();
      if (name === 'closedValue') {
        expectedDictionary.length = 0;
        expectedDictionary.push(...CLOSED_VALUES);
      }
      if (JSON.stringify(dictionary) !== JSON.stringify(expectedDictionary)) {
        throw new Error(`${scenario.name}: ${name} dictionary mismatch`);
      }
    }
    const messages = table.getChild('message');
    if (messages === null) throw new Error(`${scenario.name}: missing message column`);
    for (let row = 0; row < scenario.rows.length; row++) {
      if (messages.get(row) !== `string-row-${row}`) throw new Error(`${scenario.name}: message order mismatch at ${row}`);
    }
  } finally {
    lease.release();
  }
  let offset = 0;
  return scenario.buffers.map((buffer) => {
    const chunk = scenario.rows.slice(offset, offset + buffer._writeIndex);
    offset += buffer._writeIndex;
    return chunk;
  });
}

function registerStringScenario(scenario: StringScenario, chunks: readonly (readonly StringRow[])[]): void {
  group(`${scenario.name} | buffers=${scenario.buffers.length}`, () => {
    summary(() => {
      bench('production/leased-arrow-conversion+release', () => {
        const lease = leaseFor(scenario.root, scenario.topology);
        do_not_optimize(lease.table.numRows);
        lease.release();
      }).baseline(true);
      bench('model/js-array-flat-only', () => do_not_optimize(chunks.flat()));
      bench('model/chunk-reference-slice', () => do_not_optimize(chunks.slice()));
    });
  });
}

const QUICK = process.argv.includes('--quick');
const JSON_OUTPUT = process.argv.some((argument) => argument === '--json' || argument.startsWith('--json='));
const MARKDOWN_OUTPUT = process.argv.includes('--markdown');
const FORMAT: RunFormat = JSON_OUTPUT ? 'json' : MARKDOWN_OUTPUT ? 'markdown' : 'mitata';
const selectedLogCounts: readonly number[] = QUICK ? Object.freeze([1, 50]) : LOG_COUNTS;

for (const topology of TOPOLOGIES) {
  for (const kind of MESSAGE_KINDS) {
    for (const logCount of selectedLogCounts) {
      const scenario = await makeScenario(kind, logCount, topology);
      const preflight = assertMessageScenario(scenario);
      summary(() => registerScenario(scenario, preflight));
    }
  }
}

const quickStringSelections: readonly (readonly [StringProfile, Topology])[] = Object.freeze([
  [valueAt(STRING_PROFILES, 0, 'string profiles'), 'single'],
  [valueAt(STRING_PROFILES, 1, 'string profiles'), 'overflow-capacity'],
  [valueAt(STRING_PROFILES, 4, 'string profiles'), 'depth-3-tree'],
  [valueAt(STRING_PROFILES, 6, 'string profiles'), 'single'],
  [valueAt(CARDINALITY_PROFILES, 2, 'cardinality profiles'), 'single'],
  [valueAt(CARDINALITY_PROFILES, 3, 'cardinality profiles'), 'single'],
]);
const fullStringSelections: Array<readonly [StringProfile, Topology]> = [];
for (const profile of STRING_PROFILES) for (const topology of TOPOLOGIES) fullStringSelections.push([profile, topology]);
for (const profile of CARDINALITY_PROFILES) fullStringSelections.push([profile, 'single']);
const stringSelections = QUICK ? quickStringSelections : fullStringSelections;

for (const [profile, topology] of stringSelections) {
  const scenario = makeStringScenario(profile, topology);
  const chunks = assertStringScenario(scenario);
  registerStringScenario(scenario, chunks);
}

await run({ format: FORMAT, colors: FORMAT === 'mitata', throw: true });
