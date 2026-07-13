import { bench, group, run, summary } from 'mitata';

const LAYOUT_KEY = 'span-model:v1:f64+u8+2xrefs+2xvalidity';
const REFERENCE_BYTES = 8;
const MODELED_WRAPPER_BYTES = 64;
const DEFAULT_CAPACITY = 64;
const POOL_LIMIT = 16;

const MODEL_LABELS = {
  fresh: 'modeled:fresh-js-backing+fresh-wrapper',
  backingReuse: 'modeled:exact-layout-backing-reuse+fresh-wrapper',
  wrapperReuse: 'modeled:whole-wrapper-reuse',
} as const;

type Model = keyof typeof MODEL_LABELS;
type WorkloadName = 'steady' | 'burst' | 'overflow' | 'idle-after-burst';

interface Backing {
  readonly layoutKey: typeof LAYOUT_KEY;
  readonly capacity: number;
  readonly timestamps: Float64Array;
  readonly entryTypes: Uint8Array;
  readonly messageValues: (string | undefined)[];
  readonly payloadReferences: ({ request: number; row: number } | undefined)[];
  readonly messageValidity: Uint8Array;
  readonly payloadValidity: Uint8Array;
  highWaterMark: number;
}

interface SpanWrapper {
  backing: Backing;
  parent: SpanWrapper | undefined;
  overflow: SpanWrapper | undefined;
  children: SpanWrapper[];
  writeIndex: number;
}

interface Telemetry {
  backingAllocations: number;
  wrapperAllocations: number;
  payloadObjectAllocations: number;
  allocatedBytes: number;
  poolHits: number;
  poolMisses: number;
  referenceSlotsCleared: number;
  validityBytesCleared: number;
  topologyLinksCleared: number;
  staleReferenceSlots: number;
  staleValidityBits: number;
  staleTopologyLinks: number;
  evictedUnits: number;
  peakRetainedCapacityRows: number;
}

interface RunOutcome extends Telemetry {
  semanticChecksum: string;
  staleDataChecksum: number;
  retainedCapacityRows: number;
  retainedUnits: number;
}

interface WorkloadSpec {
  readonly name: WorkloadName;
  readonly requestCount: number;
  readonly operationCount: number;
  readonly description: string;
  run(pool: ModelPool, telemetry?: Telemetry): number;
}

function emptyTelemetry(): Telemetry {
  return {
    backingAllocations: 0,
    wrapperAllocations: 0,
    payloadObjectAllocations: 0,
    allocatedBytes: 0,
    poolHits: 0,
    poolMisses: 0,
    referenceSlotsCleared: 0,
    validityBytesCleared: 0,
    topologyLinksCleared: 0,
    staleReferenceSlots: 0,
    staleValidityBits: 0,
    staleTopologyLinks: 0,
    evictedUnits: 0,
    peakRetainedCapacityRows: 0,
  };
}

function validityBytes(capacity: number): number {
  return Math.ceil(capacity / 8);
}

function modeledBackingBytes(capacity: number): number {
  return (
    capacity * Float64Array.BYTES_PER_ELEMENT +
    capacity * Uint8Array.BYTES_PER_ELEMENT +
    validityBytes(capacity) * 2 +
    capacity * REFERENCE_BYTES * 2
  );
}

function allocateBacking(capacity: number, telemetry?: Telemetry): Backing {
  if (telemetry) {
    telemetry.backingAllocations++;
    telemetry.allocatedBytes += modeledBackingBytes(capacity);
  }
  return {
    layoutKey: LAYOUT_KEY,
    capacity,
    timestamps: new Float64Array(capacity),
    entryTypes: new Uint8Array(capacity),
    messageValues: new Array<string | undefined>(capacity),
    payloadReferences: new Array<{ request: number; row: number } | undefined>(capacity),
    messageValidity: new Uint8Array(validityBytes(capacity)),
    payloadValidity: new Uint8Array(validityBytes(capacity)),
    highWaterMark: 0,
  };
}

function allocateWrapper(backing: Backing, telemetry?: Telemetry): SpanWrapper {
  if (telemetry) {
    telemetry.wrapperAllocations++;
    telemetry.allocatedBytes += MODELED_WRAPPER_BYTES;
  }
  return {
    backing,
    parent: undefined,
    overflow: undefined,
    children: [],
    writeIndex: 0,
  };
}

function countBits(bytes: Uint8Array): number {
  let count = 0;
  for (let index = 0; index < bytes.length; index++) {
    let value = bytes[index]!;
    while (value !== 0) {
      value &= value - 1;
      count++;
    }
  }
  return count;
}

function auditClean(wrapper: SpanWrapper, telemetry: Telemetry): void {
  const backing = wrapper.backing;
  for (let row = 0; row < backing.capacity; row++) {
    if (backing.messageValues[row] !== undefined) telemetry.staleReferenceSlots++;
    if (backing.payloadReferences[row] !== undefined) telemetry.staleReferenceSlots++;
  }
  telemetry.staleValidityBits += countBits(backing.messageValidity) + countBits(backing.payloadValidity);
  telemetry.staleTopologyLinks +=
    (wrapper.parent === undefined ? 0 : 1) +
    (wrapper.overflow === undefined ? 0 : 1) +
    wrapper.children.length +
    (wrapper.writeIndex === 0 ? 0 : 1) +
    (backing.highWaterMark === 0 ? 0 : 1);
}

function clearBacking(backing: Backing, telemetry?: Telemetry): void {
  const highWaterMark = backing.highWaterMark;
  if (telemetry) telemetry.referenceSlotsCleared += highWaterMark * 2;
  for (let row = 0; row < highWaterMark; row++) {
    backing.messageValues[row] = undefined;
    backing.payloadReferences[row] = undefined;
  }
  backing.messageValidity.fill(0);
  backing.payloadValidity.fill(0);
  if (telemetry) telemetry.validityBytesCleared += backing.messageValidity.length + backing.payloadValidity.length;
  backing.highWaterMark = 0;
}

function resetWrapper(wrapper: SpanWrapper, telemetry?: Telemetry): void {
  if (telemetry) {
    telemetry.topologyLinksCleared +=
      (wrapper.parent === undefined ? 0 : 1) + (wrapper.overflow === undefined ? 0 : 1) + wrapper.children.length;
  }
  wrapper.parent = undefined;
  wrapper.overflow = undefined;
  wrapper.children.length = 0;
  wrapper.writeIndex = 0;
}

function setValid(validity: Uint8Array, row: number): void {
  validity[row >>> 3] |= 1 << (row & 7);
}

function isValid(validity: Uint8Array, row: number): boolean {
  return (validity[row >>> 3]! & (1 << (row & 7))) !== 0;
}

function mixChecksum(checksum: number, value: number): number {
  return Math.imul(checksum ^ value, 16_777_619) >>> 0;
}

class ModelPool {
  private readonly backings = new Map<string, Backing[]>();
  private readonly wrappers = new Map<string, SpanWrapper[]>();

  constructor(
    private readonly model: Model,
    private readonly telemetry?: Telemetry,
  ) {}

  acquire(capacity: number): SpanWrapper {
    const key = `${LAYOUT_KEY}:${capacity}`;
    let wrapper: SpanWrapper;
    if (this.model === 'wrapperReuse') {
      const bucket = this.wrappers.get(key);
      const reused = bucket?.pop();
      if (reused) {
        if (this.telemetry) this.telemetry.poolHits++;
        wrapper = reused;
      } else {
        if (this.telemetry) this.telemetry.poolMisses++;
        wrapper = allocateWrapper(allocateBacking(capacity, this.telemetry), this.telemetry);
      }
    } else if (this.model === 'backingReuse') {
      const bucket = this.backings.get(key);
      const backing = bucket?.pop();
      if (backing) {
        if (this.telemetry) this.telemetry.poolHits++;
        wrapper = allocateWrapper(backing, this.telemetry);
      } else {
        if (this.telemetry) this.telemetry.poolMisses++;
        wrapper = allocateWrapper(allocateBacking(capacity, this.telemetry), this.telemetry);
      }
    } else {
      if (this.telemetry) this.telemetry.poolMisses++;
      wrapper = allocateWrapper(allocateBacking(capacity, this.telemetry), this.telemetry);
    }
    if (this.telemetry) auditClean(wrapper, this.telemetry);
    this.capturePeak();
    return wrapper;
  }

  release(wrapper: SpanWrapper): void {
    if (this.model === 'fresh') return;
    clearBacking(wrapper.backing, this.telemetry);
    resetWrapper(wrapper, this.telemetry);
    const key = `${wrapper.backing.layoutKey}:${wrapper.backing.capacity}`;
    if (this.retainedUnits() >= POOL_LIMIT) {
      if (this.telemetry) this.telemetry.evictedUnits++;
      return;
    }
    if (this.model === 'wrapperReuse') {
      const bucket = this.wrappers.get(key) ?? [];
      bucket.push(wrapper);
      this.wrappers.set(key, bucket);
    } else {
      const bucket = this.backings.get(key) ?? [];
      bucket.push(wrapper.backing);
      this.backings.set(key, bucket);
    }
    this.capturePeak();
  }

  trimTo(maxUnits: number): void {
    const maps: Map<string, unknown[]>[] =
      this.model === 'wrapperReuse'
        ? [this.wrappers as Map<string, unknown[]>]
        : [this.backings as Map<string, unknown[]>];
    let retained = this.retainedUnits();
    for (const buckets of maps) {
      for (const bucket of buckets.values()) {
        while (retained > maxUnits && bucket.length > 0) {
          bucket.pop();
          retained--;
          if (this.telemetry) this.telemetry.evictedUnits++;
        }
      }
    }
  }

  retainedUnits(): number {
    let count = 0;
    const buckets = this.model === 'wrapperReuse' ? this.wrappers.values() : this.backings.values();
    for (const bucket of buckets) count += bucket.length;
    return count;
  }

  retainedCapacityRows(): number {
    let rows = 0;
    if (this.model === 'wrapperReuse') {
      for (const bucket of this.wrappers.values()) {
        for (const wrapper of bucket) rows += wrapper.backing.capacity;
      }
    } else {
      for (const bucket of this.backings.values()) {
        for (const backing of bucket) rows += backing.capacity;
      }
    }
    return rows;
  }

  private capturePeak(): void {
    if (!this.telemetry) return;
    this.telemetry.peakRetainedCapacityRows = Math.max(
      this.telemetry.peakRetainedCapacityRows,
      this.retainedCapacityRows(),
    );
  }
}

function writeRows(wrapper: SpanWrapper, request: number, rows: number, telemetry?: Telemetry): number {
  const backing = wrapper.backing;
  const terminalRow = rows - 1;
  for (let row = 0; row < rows; row++) {
    const payload = { request, row };
    if (telemetry) {
      telemetry.payloadObjectAllocations++;
      telemetry.allocatedBytes += 32;
    }
    backing.timestamps[row] = request * 1_000 + row;
    backing.entryTypes[row] = (row % 7) + 1;
    backing.messageValues[row] = `request-${request}-row-${row}`;
    backing.payloadReferences[row] = payload;
    setValid(backing.messageValidity, row);
    setValid(backing.payloadValidity, row);
    wrapper.writeIndex = row + 1;
    backing.highWaterMark = row + 1;

    if (telemetry) {
      if (!isValid(backing.messageValidity, row) || !isValid(backing.payloadValidity, row)) {
        throw new Error(`Validity lane lost row ${row}`);
      }
      if (
        backing.timestamps[row] !== request * 1_000 + row ||
        backing.entryTypes[row] !== (row % 7) + 1 ||
        backing.messageValues[row] !== `request-${request}-row-${row}` ||
        backing.payloadReferences[row]?.request !== request ||
        backing.payloadReferences[row]?.row !== row
      ) {
        throw new Error(`Backing state corrupted row ${row}`);
      }
    }
  }
  return mixChecksum(mixChecksum(request, wrapper.writeIndex), backing.entryTypes[terminalRow]!);
}

function runConcurrentBatch(
  pool: ModelPool,
  telemetry: Telemetry | undefined,
  startRequest: number,
  width: number,
  rows: number,
): number {
  const wrappers: SpanWrapper[] = [];
  let checksum = 2_166_136_261;
  for (let offset = 0; offset < width; offset++) {
    const wrapper = pool.acquire(rows <= 16 ? 16 : rows <= 32 ? 32 : DEFAULT_CAPACITY);
    wrappers.push(wrapper);
    checksum = mixChecksum(checksum, writeRows(wrapper, startRequest + offset, rows, telemetry));
  }
  for (let index = wrappers.length - 1; index >= 0; index--) pool.release(wrappers[index]!);
  return checksum;
}

function runOverflowRequest(pool: ModelPool, telemetry: Telemetry | undefined, request: number, rows: number): number {
  const wrappers: SpanWrapper[] = [];
  let previous: SpanWrapper | undefined;
  let checksum = 2_166_136_261;
  let written = 0;
  while (written < rows) {
    const wrapper = pool.acquire(DEFAULT_CAPACITY);
    if (previous) {
      previous.overflow = wrapper;
      previous.children.push(wrapper);
      wrapper.parent = previous;
    }
    wrappers.push(wrapper);
    const chunkRows = Math.min(DEFAULT_CAPACITY, rows - written);
    checksum = mixChecksum(checksum, writeRows(wrapper, request * 10 + wrappers.length, chunkRows, telemetry));
    written += chunkRows;
    previous = wrapper;
  }
  for (let index = wrappers.length - 1; index >= 0; index--) pool.release(wrappers[index]!);
  return checksum;
}

function buildWorkloads(quick: boolean): WorkloadSpec[] {
  const steadyRequests = quick ? 6 : 48;
  const burstBatches = quick ? 2 : 8;
  const burstWidth = quick ? 4 : 16;
  const overflowRequests = quick ? 3 : 16;
  const idleBurstWidth = quick ? 6 : 32;
  const idleRequests = quick ? 2 : 12;
  return [
    {
      name: 'steady',
      requestCount: steadyRequests,
      operationCount: steadyRequests * 24,
      description: 'single in-flight request with a stable 32-row exact layout',
      run(pool, telemetry) {
        let checksum = 2_166_136_261;
        for (let request = 0; request < steadyRequests; request++) {
          checksum = mixChecksum(checksum, runConcurrentBatch(pool, telemetry, request, 1, 24));
        }
        return checksum;
      },
    },
    {
      name: 'burst',
      requestCount: burstBatches * burstWidth,
      operationCount: burstBatches * burstWidth * 28,
      description: 'synchronous bursts retained and released as a batch with a 32-row exact layout',
      run(pool, telemetry) {
        let checksum = 2_166_136_261;
        for (let batch = 0; batch < burstBatches; batch++) {
          checksum = mixChecksum(checksum, runConcurrentBatch(pool, telemetry, batch * burstWidth, burstWidth, 28));
        }
        return checksum;
      },
    },
    {
      name: 'overflow',
      requestCount: overflowRequests,
      operationCount: overflowRequests * 150,
      description: '150-row requests chained across three 64-row exact-layout wrappers',
      run(pool, telemetry) {
        let checksum = 2_166_136_261;
        for (let request = 0; request < overflowRequests; request++) {
          checksum = mixChecksum(checksum, runOverflowRequest(pool, telemetry, request, 150));
        }
        return checksum;
      },
    },
    {
      name: 'idle-after-burst',
      requestCount: idleBurstWidth + idleRequests,
      operationCount: idleBurstWidth * 48 + idleRequests * 12,
      description: 'large 64-row burst, idle trim to two retained units, then 16-row steady traffic',
      run(pool, telemetry) {
        let checksum = runConcurrentBatch(pool, telemetry, 0, idleBurstWidth, 48);
        pool.trimTo(2);
        for (let request = 0; request < idleRequests; request++) {
          checksum = mixChecksum(checksum, runConcurrentBatch(pool, telemetry, idleBurstWidth + request, 1, 12));
        }
        return checksum;
      },
    },
  ];
}

function runModel(model: Model, workload: WorkloadSpec): RunOutcome {
  const telemetry = emptyTelemetry();
  const pool = new ModelPool(model, telemetry);
  const activeChecksum = workload.run(pool, telemetry);
  const staleDataChecksum = telemetry.staleReferenceSlots + telemetry.staleValidityBits + telemetry.staleTopologyLinks;
  return {
    ...telemetry,
    semanticChecksum: `${activeChecksum.toString(16)}:stale=${staleDataChecksum}`,
    staleDataChecksum,
    retainedCapacityRows: pool.retainedCapacityRows(),
    retainedUnits: pool.retainedUnits(),
  };
}

function formatTelemetry(workload: WorkloadSpec, model: Model, outcome: RunOutcome): string {
  const allocations = outcome.backingAllocations + outcome.wrapperAllocations + outcome.payloadObjectAllocations;
  return [
    workload.name,
    MODEL_LABELS[model],
    `allocations=${allocations}`,
    `allocatedBytes=${outcome.allocatedBytes}`,
    `poolHit/Miss=${outcome.poolHits}/${outcome.poolMisses}`,
    `refsCleared=${outcome.referenceSlotsCleared}`,
    `validityBytesReset=${outcome.validityBytesCleared}`,
    `topologyLinksReset=${outcome.topologyLinksCleared}`,
    `retainedRows=${outcome.retainedCapacityRows}`,
    `peakRetainedRows=${outcome.peakRetainedCapacityRows}`,
    `evicted=${outcome.evictedUnits}`,
    `staleChecksum=${outcome.staleDataChecksum}`,
  ].join('\t');
}

function assertPreflight(workload: WorkloadSpec): string[] {
  let expected: string | undefined;
  const lines: string[] = [];
  for (const model of Object.keys(MODEL_LABELS) as Model[]) {
    const outcome = runModel(model, workload);
    if (outcome.staleDataChecksum !== 0) {
      throw new Error(`${workload.name}/${MODEL_LABELS[model]} leaked stale rows or references`);
    }
    expected ??= outcome.semanticChecksum;
    if (outcome.semanticChecksum !== expected) {
      throw new Error(`${workload.name}/${MODEL_LABELS[model]} failed semantic preflight`);
    }
    lines.push(formatTelemetry(workload, model, outcome));
  }
  return lines;
}

interface CliOptions {
  quick: boolean;
  json: boolean;
  markdown: boolean;
  scenario: WorkloadName | undefined;
}

function parseCli(argv: readonly string[]): CliOptions {
  const options: CliOptions = {
    quick: argv.includes('--quick'),
    json: argv.includes('--json'),
    markdown: argv.includes('--markdown'),
    scenario: undefined,
  };
  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index]!;
    if (argument === '--scenario') options.scenario = argv[++index] as WorkloadName;
    else if (argument.startsWith('--scenario='))
      options.scenario = argument.slice('--scenario='.length) as WorkloadName;
  }
  if (options.scenario && !['steady', 'burst', 'overflow', 'idle-after-burst'].includes(options.scenario)) {
    throw new Error(`Unknown scenario: ${options.scenario}`);
  }
  return options;
}

const cli = parseCli(process.argv.slice(2));
const workloads = buildWorkloads(cli.quick).filter(
  (workload) => cli.scenario === undefined || workload.name === cli.scenario,
);
const telemetryLines = [
  '# untimed modeled telemetry (preflight)',
  '# workload\tmodel\tallocation/pool/reset/retention counters',
];
for (const workload of workloads) {
  telemetryLines.push(...assertPreflight(workload));
  const timedPools: Record<Model, ModelPool> = {
    fresh: new ModelPool('fresh'),
    backingReuse: new ModelPool('backingReuse'),
    wrapperReuse: new ModelPool('wrapperReuse'),
  };
  workload.run(timedPools.backingReuse);
  workload.run(timedPools.wrapperReuse);
  group(`span pooling [modeled, steady-state pool]: ${workload.name} (${workload.operationCount} row writes)`, () => {
    summary(() => {
      bench(MODEL_LABELS.fresh, () => workload.run(timedPools.fresh)).baseline();
      bench(MODEL_LABELS.backingReuse, () => workload.run(timedPools.backingReuse));
      bench(MODEL_LABELS.wrapperReuse, () => workload.run(timedPools.wrapperReuse));
    });
  });
}

const telemetryOutput = `${telemetryLines.join('\n')}\n`;
if (cli.json) process.stderr.write(telemetryOutput);
else process.stdout.write(telemetryOutput);

await run({
  colors: false,
  format: cli.json ? { json: { samples: true } } : cli.markdown ? 'markdown' : 'mitata',
});
