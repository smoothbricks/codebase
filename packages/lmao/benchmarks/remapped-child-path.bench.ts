import { bench, do_not_optimize, group, run, summary } from 'mitata';
import {
  defineLogSchema,
  defineOpContext,
  JsBufferStrategy,
  S,
  type SchemaWithMetadata,
  TestTracer,
} from '../src/index.js';
import { convertSpanTreeToLeasedArrowTable } from '../src/lib/convertToArrow.js';
import { createRemapDescriptor } from '../src/lib/library.js';
import type { RemapDescriptor } from '../src/lib/logBinding.js';
import type { LogSchema } from '../src/lib/schema/LogSchema.js';
import { createChildSpanBuffer, createSpanBuffer } from '../src/lib/spanBuffer.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { iterateSpanChildren } from '../src/lib/traceTopology.js';
import type { AnySpanBuffer } from '../src/lib/types.js';

const QUICK = process.argv.includes('--quick');
const JSON_OUTPUT = process.argv.includes('--json');
const MARKDOWN_OUTPUT = process.argv.includes('--markdown');
const MITATA_FORMAT = JSON_OUTPUT ? 'json' : MARKDOWN_OUTPUT ? 'markdown' : 'mitata';

const REGISTRATION_ITERATIONS = QUICK ? 40 : 4_000;
const TRAVERSAL_ITERATIONS = QUICK ? 200 : 20_000;
const CAPACITY = 8;
const MANY_MAPPING_COUNT = 8;

const LABELS = {
  unprefixed: 'unprefixed-raw+topology',
  current: 'legacy-remapped-view+topology',
  modeled: 'CallsitePlan-descriptor+topology',
} as const;

type LegacyRemapWrapper = (buffer: AnySpanBuffer) => AnySpanBuffer;
type BenchmarkValue = Readonly<{ checksum: number; allocationCount: number; visitedObjects: number }>;
type TreeMode = 'unprefixed' | 'current' | 'modeled';

interface BufferPlan {
  readonly schema: LogSchema;
  createRoot(): AnySpanBuffer;
  createChild(parent: AnySpanBuffer, spanName: string): AnySpanBuffer;
}

interface Workload {
  readonly depth: number;
  readonly mappingCount: number;
  readonly depthLabel: 'shallow' | 'depth-3';
  readonly mappingLabel: 'one-mapping' | 'many-mappings';
  readonly publicNames: readonly string[];
  readonly rawNames: readonly string[];
  readonly descriptor: RemapDescriptor;
  readonly publicPlan: BufferPlan;
  readonly rawPlan: BufferPlan;
  readonly wrapLegacyBuffer: LegacyRemapWrapper;
}

interface TreeFixture {
  readonly root: AnySpanBuffer;
  readonly descriptor: RemapDescriptor;
  readonly mode: TreeMode;
  readonly depth: number;
  readonly mappingCount: number;
  readonly allocationCount: number;
  readonly legacyViews: ReadonlyMap<AnySpanBuffer, AnySpanBuffer> | undefined;
}

function numberFields(names: readonly string[]): Record<string, SchemaWithMetadata> {
  const fields: Record<string, SchemaWithMetadata> = {};
  for (const name of names) fields[name] = S.number();
  return fields;
}

function createBufferPlan(fields: Record<string, SchemaWithMetadata>, label: string): BufferPlan {
  const context = defineOpContext({ logSchema: defineLogSchema(fields) });
  const op = context.defineOp(label, (ctx) => ctx.ok(undefined));
  const plan = op.callsitePlan;
  const tracer = new TestTracer(context, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  });

  return {
    schema: plan.schema,
    createRoot() {
      const root = createSpanBuffer(
        plan.schema,
        createTraceRoot(`${label}-trace`, tracer),
        plan.metadata,
        CAPACITY,
        plan.SpanBufferClass,
      );
      plan.appenders.writeSpanStart(root, `${label}-root`);
      root._writeIndex = 1;
      return root;
    },
    createChild(parent, spanName) {
      const child = createChildSpanBuffer(parent, plan.SpanBufferClass, plan.metadata, plan.metadata, CAPACITY);
      plan.appenders.writeSpanStart(child, spanName);
      return child;
    },
  };
}

function createLegacyRemapWrapper(outputToSourceMapping: Readonly<Record<string, string>>): LegacyRemapWrapper {
  return (buffer) => {
    const columns: Array<[string, unknown]> = [];
    for (const [outputName, sourceName] of Object.entries(outputToSourceMapping)) {
      const schema = buffer._logSchema.fields[sourceName];
      if (schema !== undefined) columns.push([outputName, schema]);
    }
    const view: AnySpanBuffer = Object.create(buffer);
    Object.defineProperties(view, {
      _columns: { value: columns },
      getColumnIfAllocated: {
        value(name: string) {
          return buffer.getColumnIfAllocated(outputToSourceMapping[name] ?? name);
        },
      },
      getNullsIfAllocated: {
        value(name: string) {
          return buffer.getNullsIfAllocated(outputToSourceMapping[name] ?? name);
        },
      },
    });
    return view;
  };
}

function createWorkload(depth: number, mappingCount: number): Workload {
  const publicNames = Object.freeze(Array.from({ length: mappingCount }, (_, index) => `lib_value_${index}`));
  const rawNames = Object.freeze(Array.from({ length: mappingCount }, (_, index) => `value_${index}`));
  const outputToSourceMapping: Record<string, string> = {};
  for (let index = 0; index < publicNames.length; index++) {
    const publicName = publicNames[index];
    const rawName = rawNames[index];
    if (publicName === undefined || rawName === undefined) throw new Error('Invalid remap workload');
    outputToSourceMapping[publicName] = rawName;
  }
  const publicPlan = createBufferPlan(numberFields(publicNames), `public-${depth}-${mappingCount}`);
  const rawPlan = createBufferPlan(numberFields(rawNames), `raw-${depth}-${mappingCount}`);
  return {
    depth,
    mappingCount,
    depthLabel: depth === 1 ? 'shallow' : 'depth-3',
    mappingLabel: mappingCount === 1 ? 'one-mapping' : 'many-mappings',
    publicNames,
    rawNames,
    descriptor: createRemapDescriptor(rawPlan.schema, outputToSourceMapping),
    publicPlan,
    rawPlan,
    wrapLegacyBuffer: createLegacyRemapWrapper(outputToSourceMapping),
  };
}

function writeDeterministicRow(buffer: AnySpanBuffer, names: readonly string[], level: number): void {
  for (let columnIndex = 0; columnIndex < names.length; columnIndex++) {
    const name = names[columnIndex];
    if (name === undefined) throw new Error('Missing deterministic column name');
    const writer = Reflect.get(buffer, name);
    if (typeof writer !== 'function') throw new TypeError(`Missing generated writer for ${name}`);
    Reflect.apply(writer, buffer, [0, level * 100 + columnIndex + 1]);
  }
  buffer._writeIndex = 1;
}

function appendTree(root: AnySpanBuffer, workload: Workload, mode: TreeMode): TreeFixture {
  let parent = root;
  const bufferPlan = mode === 'unprefixed' ? workload.publicPlan : workload.rawPlan;
  const childNames = mode === 'unprefixed' ? workload.publicNames : workload.rawNames;
  const legacyViews = mode === 'current' ? new Map<AnySpanBuffer, AnySpanBuffer>() : undefined;
  let allocationCount = 0;

  for (let level = 1; level <= workload.depth; level++) {
    const child = bufferPlan.createChild(parent, `child-${level}`);
    allocationCount++;
    writeDeterministicRow(child, childNames, level);
    if (mode !== 'unprefixed') child._remapDescriptor = workload.descriptor;
    if (legacyViews !== undefined) {
      legacyViews.set(child, workload.wrapLegacyBuffer(child));
      allocationCount++;
    }
    parent = child;
  }

  return {
    root,
    descriptor: workload.descriptor,
    mode,
    depth: workload.depth,
    mappingCount: workload.mappingCount,
    allocationCount,
    legacyViews,
  };
}

function readNumber(values: unknown, label: string): number {
  if (typeof values !== 'object' || values === null) throw new Error(`Missing ${label}`);
  const value = Reflect.get(values, '0');
  if (typeof value !== 'number') throw new Error(`Missing ${label}`);
  return value;
}

function onlyChild(parent: AnySpanBuffer): AnySpanBuffer | undefined {
  let child: AnySpanBuffer | undefined;
  for (const candidate of iterateSpanChildren(parent)) {
    if (child !== undefined) throw new Error('Benchmark tree unexpectedly has sibling spans');
    child = candidate;
  }
  return child;
}

function mappedTreeChecksum(tree: TreeFixture): BenchmarkValue {
  let checksum = 0;
  let visitedObjects = 0;
  let level = 0;
  let parent = tree.root;

  for (;;) {
    const rawChild = onlyChild(parent);
    if (rawChild === undefined) break;
    level++;
    let visibleChild = rawChild;
    if (tree.legacyViews !== undefined) {
      const legacyView = tree.legacyViews.get(rawChild);
      if (legacyView === undefined) throw new Error(`Missing legacy remap view at depth ${level}`);
      visibleChild = legacyView;
    }
    visitedObjects++;
    let mappedColumnIndex = 0;
    if (tree.mode === 'modeled') {
      for (const entry of tree.descriptor.columns) {
        const value = readNumber(Reflect.get(rawChild, entry[3]), `modeled value for ${entry[0]} at depth ${level}`);
        checksum = (checksum * 1_000_003 + value + mappedColumnIndex) >>> 0;
        mappedColumnIndex++;
      }
    } else {
      for (const [publicName] of visibleChild._columns) {
        if (!publicName.startsWith('lib_value_')) continue;
        const value = readNumber(
          visibleChild.getColumnIfAllocated(publicName),
          `visible value for ${publicName} at depth ${level}`,
        );
        checksum = (checksum * 1_000_003 + value + mappedColumnIndex) >>> 0;
        mappedColumnIndex++;
      }
    }
    if (mappedColumnIndex !== tree.mappingCount) {
      throw new Error(`Expected ${tree.mappingCount} mapped columns, visited ${mappedColumnIndex}`);
    }
    parent = rawChild;
  }

  if (level !== tree.depth) throw new Error(`Expected depth ${tree.depth}, visited ${level}`);
  return { checksum, allocationCount: tree.allocationCount, visitedObjects };
}

function registrationSample(workload: Workload, mode: TreeMode): BenchmarkValue {
  const bufferPlan = mode === 'unprefixed' ? workload.publicPlan : workload.rawPlan;
  const root = bufferPlan.createRoot();
  let last: TreeFixture | undefined;
  let allocationCount = 1;
  for (let iteration = 0; iteration < REGISTRATION_ITERATIONS; iteration++) {
    last = appendTree(root, workload, mode);
    allocationCount += last.allocationCount;
  }
  if (last === undefined) throw new Error('Registration sample did not execute');
  const checksum = root._traceRoot._topology.count + workload.depth + workload.mappingCount;
  return { checksum, allocationCount, visitedObjects: 0 };
}

function createTraversalFixture(workload: Workload, mode: TreeMode): TreeFixture {
  const bufferPlan = mode === 'unprefixed' ? workload.publicPlan : workload.rawPlan;
  return appendTree(bufferPlan.createRoot(), workload, mode);
}

function traversalSample(tree: TreeFixture): BenchmarkValue {
  let checksum = 0;
  let visitedObjects = 0;
  for (let iteration = 0; iteration < TRAVERSAL_ITERATIONS; iteration++) {
    const result = mappedTreeChecksum(tree);
    checksum = result.checksum;
    visitedObjects += result.visitedObjects;
  }
  return { checksum, allocationCount: 0, visitedObjects };
}

function arrowOutputChecksum(tree: TreeFixture, publicNames: readonly string[]): number {
  const lease = convertSpanTreeToLeasedArrowTable(tree.root);
  try {
    const { table } = lease;
    let checksum = table.numRows;
    for (const name of publicNames) {
      const column = table.getChild(name);
      if (column === null) throw new Error(`Arrow output is missing mapped column ${name}`);
      for (let row = 0; row < table.numRows; row++) {
        const value = column.get(row);
        if (typeof value === 'number') checksum = (checksum * 1_000_003 + value) >>> 0;
      }
    }
    return checksum;
  } finally {
    lease.release();
  }
}

function validateArrowSemantics(workload: Workload): void {
  const unprefixed = createTraversalFixture(workload, 'unprefixed');
  const current = createTraversalFixture(workload, 'current');
  const modeled = createTraversalFixture(workload, 'modeled');
  const expected = arrowOutputChecksum(unprefixed, workload.publicNames);
  const currentChecksum = arrowOutputChecksum(current, workload.publicNames);
  const modeledChecksum = arrowOutputChecksum(modeled, workload.publicNames);
  if (currentChecksum !== expected || modeledChecksum !== expected) {
    throw new Error(
      `Mapped Arrow checksum mismatch: unprefixed=${expected}, current=${currentChecksum}, modeled=${modeledChecksum}`,
    );
  }
}

function validateMappedChecksums(workload: Workload): void {
  const expected = mappedTreeChecksum(createTraversalFixture(workload, 'unprefixed')).checksum;
  const current = mappedTreeChecksum(createTraversalFixture(workload, 'current')).checksum;
  const modeled = mappedTreeChecksum(createTraversalFixture(workload, 'modeled')).checksum;
  if (current !== expected || modeled !== expected) {
    throw new Error(`Mapped traversal checksum mismatch: ${expected}, ${current}, ${modeled}`);
  }
}

function registerRegistrationGroup(workload: Workload): void {
  group(
    `remapped child registration | ${workload.depthLabel} | ${workload.mappingLabel} | capacity=${CAPACITY} | inner=${REGISTRATION_ITERATIONS}`,
    () => {
      bench(LABELS.unprefixed, () => {
        const value = registrationSample(workload, 'unprefixed');
        do_not_optimize(value.checksum);
        return value;
      });
      bench(LABELS.current, () => {
        const value = registrationSample(workload, 'current');
        do_not_optimize(value.checksum);
        return value;
      }).baseline();
      bench(LABELS.modeled, () => {
        const value = registrationSample(workload, 'modeled');
        do_not_optimize(value.checksum);
        return value;
      }).highlight('cyan');
    },
  );
}

function registerTraversalGroup(workload: Workload): void {
  const fixtures = {
    unprefixed: createTraversalFixture(workload, 'unprefixed'),
    current: createTraversalFixture(workload, 'current'),
    modeled: createTraversalFixture(workload, 'modeled'),
  };
  group(
    `remapped Arrow traversal | ${workload.depthLabel} | ${workload.mappingLabel} | capacity=${CAPACITY} | inner=${TRAVERSAL_ITERATIONS}`,
    () => {
      bench(LABELS.unprefixed, () => {
        const value = traversalSample(fixtures.unprefixed);
        do_not_optimize(value.checksum);
        return value;
      });
      bench(LABELS.current, () => {
        const value = traversalSample(fixtures.current);
        do_not_optimize(value.checksum);
        return value;
      }).baseline();
      bench(LABELS.modeled, () => {
        const value = traversalSample(fixtures.modeled);
        do_not_optimize(value.checksum);
        return value;
      }).highlight('cyan');
    },
  );
}

const workloads = [
  createWorkload(1, 1),
  createWorkload(1, MANY_MAPPING_COUNT),
  createWorkload(3, 1),
  createWorkload(3, MANY_MAPPING_COUNT),
];
for (const workload of workloads) {
  validateMappedChecksums(workload);
  validateArrowSemantics(workload);
  summary(() => registerRegistrationGroup(workload));
  summary(() => registerTraversalGroup(workload));
}

await run({ format: MITATA_FORMAT, throw: true });
