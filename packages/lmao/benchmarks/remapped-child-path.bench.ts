import { bench, do_not_optimize, group, run, summary } from 'mitata';
import { createTestOpMetadata, createTestSchema, createTestTraceRoot } from '../src/lib/__tests__/test-helpers.js';
import { convertSpanTreeToArrowTable } from '../src/lib/convertToArrow.js';
import { generateRemappedBufferViewClass } from '../src/lib/library.js';
import { S } from '../src/lib/schema/builder.js';
import type { LogSchema } from '../src/lib/schema/LogSchema.js';
import { createChildSpanBuffer, createSpanBuffer, getSpanBufferClass } from '../src/lib/spanBuffer.js';
import { writeSpanStart } from '../src/lib/spanContext.js';
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
  unprefixed: 'unprefixed-raw-current',
  current: 'current-remapped-view+_columns',
  modeled: 'modeled-raw-child+immutable-cold-descriptor',
} as const;

type MutableBuffer = AnySpanBuffer & Record<string, unknown>;
type RemappedViewConstructor = new (buffer: AnySpanBuffer) => AnySpanBuffer;
type MappingEntry = readonly [publicName: string, rawName: string];
type ColdRemapDescriptor = Readonly<{ entries: readonly MappingEntry[] }>;
type BenchmarkValue = Readonly<{ checksum: number; allocationCount: number; visitedObjects: number }>;
type TreeMode = 'unprefixed' | 'current' | 'modeled';

interface Workload {
  readonly depth: number;
  readonly mappingCount: number;
  readonly depthLabel: 'shallow' | 'depth-3';
  readonly mappingLabel: 'one-mapping' | 'many-mappings';
  readonly publicNames: readonly string[];
  readonly rawNames: readonly string[];
  readonly descriptor: ColdRemapDescriptor;
  readonly publicSchema: LogSchema;
  readonly rawSchema: LogSchema;
  readonly remappedViewClass: RemappedViewConstructor;
}

interface TreeFixture {
  readonly root: AnySpanBuffer;
  readonly descriptor: ColdRemapDescriptor;
  readonly mode: TreeMode;
  readonly depth: number;
  readonly mappingCount: number;
  readonly allocationCount: number;
}

const METADATA = createTestOpMetadata({
  name: 'remapped-child-path-benchmark',
  package_name: '@smoothbricks/lmao',
  package_file: 'benchmarks/remapped-child-path.bench.ts',
  git_sha: 'benchmark',
  line: 1,
});

function numberFields(names: readonly string[]): Record<string, ReturnType<typeof S.number>> {
  const fields: Record<string, ReturnType<typeof S.number>> = {};
  for (const name of names) fields[name] = S.number();
  return fields;
}

function createDescriptor(publicNames: readonly string[], rawNames: readonly string[]): ColdRemapDescriptor {
  const entries = publicNames.map((publicName, index) => Object.freeze([publicName, rawNames[index]!] as const));
  return Object.freeze({ entries: Object.freeze(entries) });
}

function createWorkload(depth: number, mappingCount: number): Workload {
  const publicNames = Object.freeze(Array.from({ length: mappingCount }, (_, index) => `lib_value_${index}`));
  const rawNames = Object.freeze(Array.from({ length: mappingCount }, (_, index) => `value_${index}`));
  const descriptor = createDescriptor(publicNames, rawNames);
  const reverseMapping = Object.fromEntries(descriptor.entries);
  return {
    depth,
    mappingCount,
    depthLabel: depth === 1 ? 'shallow' : 'depth-3',
    mappingLabel: mappingCount === 1 ? 'one-mapping' : 'many-mappings',
    publicNames,
    rawNames,
    descriptor,
    publicSchema: createTestSchema(numberFields(publicNames)),
    rawSchema: createTestSchema(numberFields(rawNames)),
    remappedViewClass: generateRemappedBufferViewClass(reverseMapping),
  };
}

function createRoot(schema: LogSchema): AnySpanBuffer {
  return createSpanBuffer(schema, createTestTraceRoot('remapped-child-benchmark'), METADATA, CAPACITY);
}

function writeDeterministicRow(buffer: AnySpanBuffer, names: readonly string[], level: number): void {
  writeSpanStart(buffer, `child-${level}`);
  const writable = buffer as MutableBuffer;
  for (let columnIndex = 0; columnIndex < names.length; columnIndex++) {
    const writer = writable[names[columnIndex]!];
    if (typeof writer !== 'function') throw new TypeError(`Missing generated writer for ${names[columnIndex]}`);
    Reflect.apply(writer, buffer, [0, level * 100 + columnIndex + 1]);
  }
  buffer._writeIndex = 1;
}

function appendTree(root: AnySpanBuffer, workload: Workload, mode: TreeMode): TreeFixture {
  let rawParent = root;
  const childSchema = mode === 'unprefixed' ? workload.publicSchema : workload.rawSchema;
  const childNames = mode === 'unprefixed' ? workload.publicNames : workload.rawNames;
  const ChildBufferClass = getSpanBufferClass(childSchema);
  let allocationCount = 0;

  for (let level = 1; level <= workload.depth; level++) {
    const child = createChildSpanBuffer(rawParent, ChildBufferClass, METADATA, METADATA, CAPACITY);
    allocationCount++;
    writeDeterministicRow(child, childNames, level);
    if (mode === 'current') {
      rawParent._children.push(new workload.remappedViewClass(child));
      allocationCount++;
    } else {
      // Modeled future path: registration keeps the raw child. The immutable descriptor
      // is created once with the workload and is consulted only by cold traversal.
      rawParent._children.push(child);
    }
    rawParent = child;
  }

  return {
    root,
    descriptor: workload.descriptor,
    mode,
    depth: workload.depth,
    mappingCount: workload.mappingCount,
    allocationCount,
  };
}

function mappedTreeChecksum(tree: TreeFixture): BenchmarkValue {
  let checksum = 0;
  let visitedObjects = 0;
  let level = 0;
  let children = tree.root._children;

  while (children.length > 0) {
    const visibleChild = children[0]!;
    level++;
    visitedObjects++;
    let mappedColumnIndex = 0;
    if (tree.mode === 'modeled') {
      for (const [publicName, rawName] of tree.descriptor.entries) {
        const values = visibleChild.getColumnIfAllocated(rawName) as ArrayLike<number> | undefined;
        const value = values?.[0];
        if (value === undefined) throw new Error(`Missing modeled value for ${publicName} at depth ${level}`);
        checksum = (checksum * 1_000_003 + value + mappedColumnIndex) >>> 0;
        mappedColumnIndex++;
      }
    } else {
      // Mirror Arrow pass 0: current buffers expose their schema-backed _columns,
      // while RemappedBufferView exposes the constructor-built prefixed _columns.
      for (const [publicName] of visibleChild._columns) {
        if (!publicName.startsWith('lib_value_')) continue;
        const values = visibleChild.getColumnIfAllocated(publicName) as ArrayLike<number> | undefined;
        const value = values?.[0];
        if (value === undefined) throw new Error(`Missing current value for ${publicName} at depth ${level}`);
        checksum = (checksum * 1_000_003 + value + mappedColumnIndex) >>> 0;
        mappedColumnIndex++;
      }
    }
    if (mappedColumnIndex !== tree.mappingCount) {
      throw new Error(`Expected ${tree.mappingCount} mapped columns, visited ${mappedColumnIndex}`);
    }
    children = visibleChild._children;
  }

  if (level !== tree.depth) throw new Error(`Expected depth ${tree.depth}, visited ${level}`);
  return { checksum, allocationCount: tree.allocationCount, visitedObjects };
}

function registrationSample(workload: Workload, mode: TreeMode): BenchmarkValue {
  const root = createRoot(mode === 'unprefixed' ? workload.publicSchema : workload.rawSchema);
  let last: TreeFixture | undefined;
  let allocationCount = 1;
  for (let iteration = 0; iteration < REGISTRATION_ITERATIONS; iteration++) {
    root._children.length = 0;
    last = appendTree(root, workload, mode);
    allocationCount += last.allocationCount;
  }
  if (!last) throw new Error('Registration sample did not execute');
  // Registration timing intentionally stops at ownership/linkage. Semantic mapped
  // traversal is validated once in validateMappedChecksums before Mitata runs.
  const checksum = (last.root === root ? 1 : 0) + root._children.length + workload.depth + workload.mappingCount;
  root._children.length = 0;
  return { checksum, allocationCount, visitedObjects: 0 };
}

function createTraversalFixture(workload: Workload, mode: TreeMode): TreeFixture {
  const root = createRoot(mode === 'unprefixed' ? workload.publicSchema : workload.rawSchema);
  return appendTree(root, workload, mode);
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
  const table = convertSpanTreeToArrowTable(tree.root);
  let checksum = table.numRows;
  for (const name of publicNames) {
    const column = table.getChild(name);
    if (!column) throw new Error(`Arrow output is missing mapped column ${name}`);
    for (let row = 0; row < table.numRows; row++) {
      const value = column.get(row);
      if (typeof value === 'number') checksum = (checksum * 1_000_003 + value) >>> 0;
    }
  }
  return checksum;
}

function validateArrowSemantics(workload: Workload): void {
  const unprefixed = createTraversalFixture(workload, 'unprefixed');
  const current = createTraversalFixture(workload, 'current');
  // The modeled representation deliberately avoids wrappers on registration. Materialize
  // current views only for this untimed compatibility check until Arrow accepts descriptors.
  const modeledForArrow = createTraversalFixture(workload, 'current');
  const expected = arrowOutputChecksum(unprefixed, workload.publicNames);
  const currentChecksum = arrowOutputChecksum(current, workload.publicNames);
  const modeledChecksum = arrowOutputChecksum(modeledForArrow, workload.publicNames);
  if (currentChecksum !== expected || modeledChecksum !== expected) {
    throw new Error(
      `Mapped Arrow checksum mismatch: unprefixed=${expected}, current=${currentChecksum}, modeled=${modeledChecksum}`,
    );
  }
}

function validateMappedChecksums(workload: Workload): void {
  const fixtures = [
    createTraversalFixture(workload, 'unprefixed'),
    createTraversalFixture(workload, 'current'),
    createTraversalFixture(workload, 'modeled'),
  ];
  const checksums = fixtures.map((fixture) => mappedTreeChecksum(fixture).checksum);
  if (checksums.some((checksum) => checksum !== checksums[0])) {
    throw new Error(`Mapped traversal checksum mismatch: ${checksums.join(', ')}`);
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
