import { bench, do_not_optimize, group, run, summary } from 'mitata';
import { defineOpContext } from '../src/lib/defineOpContext.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import type { OpContextOf } from '../src/lib/opContext/types.js';
import type { CallsitePlan } from '../src/lib/physicalLayoutPlan.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY,
  RUNTIME_HINT_RESULT,
} from '../src/lib/runtimeHint.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ENTRY_TYPE_INFO } from '../src/lib/schema/systemSchema.js';
import { createTraceId } from '../src/lib/traceId.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';
import { iterateSpanTree } from '../src/lib/traceTopology.js';
import type { SpanBuffer } from '../src/lib/types.js';

type WorkloadName = 'steady' | 'burst' | 'overflow' | 'idle-after-burst';

type WorkloadSpec = {
  readonly name: WorkloadName;
  readonly requests: number;
  readonly batchWidth: number;
  readonly rows: number;
  readonly overflowSegments: number;
  readonly childRows: number;
  readonly description: string;
};

type CliOptions = {
  readonly quick: boolean;
  readonly json: boolean;
  readonly scenario: WorkloadName | undefined;
};

type RunOutcome = {
  readonly checksum: number;
  readonly roots: number;
  readonly physicalSegments: number;
  readonly releasedTopologies: number;
  readonly poolRef: null;
};

const SCHEMA = defineLogSchema({ metric: S.number() });
const CONTEXT = defineOpContext({ logSchema: SCHEMA });
const RUNTIME_SCHEMA = CONTEXT.logBinding.logSchema;
type RuntimeContext = OpContextOf<typeof CONTEXT>;
const STRATEGY = new JsBufferStrategy<typeof RUNTIME_SCHEMA>();
const TRACER = new TestTracer(CONTEXT, { bufferStrategy: STRATEGY, createTraceRoot });

function isWorkloadName(value: string): value is WorkloadName {
  return value === 'steady' || value === 'burst' || value === 'overflow' || value === 'idle-after-burst';
}

function parseCli(argv: readonly string[]): CliOptions {
  let scenario: WorkloadName | undefined;
  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index];
    if (argument === undefined) continue;
    let candidate: string | undefined;
    if (argument === '--scenario') candidate = argv[index + 1];
    else if (argument.startsWith('--scenario=')) candidate = argument.slice('--scenario='.length);
    if (candidate === undefined) continue;
    if (!isWorkloadName(candidate)) throw new Error(`Unknown scenario: ${candidate}`);
    scenario = candidate;
  }
  return {
    quick: argv.includes('--quick'),
    json: argv.includes('--json'),
    scenario,
  };
}

function workloads(quick: boolean): readonly WorkloadSpec[] {
  const scale = quick ? 1 : 16;
  return Object.freeze([
    {
      name: 'steady',
      requests: 6 * scale,
      batchWidth: 1,
      rows: 24,
      overflowSegments: 0,
      childRows: 0,
      description: 'one live trace at a time with no retained production pool',
    },
    {
      name: 'burst',
      requests: 8 * scale,
      batchWidth: 4,
      rows: 28,
      overflowSegments: 0,
      childRows: 0,
      description: 'synchronous trace bursts retained and released as a batch',
    },
    {
      name: 'overflow',
      requests: 3 * scale,
      batchWidth: 1,
      rows: 32,
      overflowSegments: 2,
      childRows: 18,
      description: 'root, child, and overflow topology release',
    },
    {
      name: 'idle-after-burst',
      requests: 8 * scale,
      batchWidth: 7,
      rows: 26,
      overflowSegments: 0,
      childRows: 0,
      description: 'large burst followed by one small trace with no retained pool owner',
    },
  ]);
}

function mix(checksum: number, value: number): number {
  return Math.imul(checksum ^ value, 16_777_619) >>> 0;
}

function planForCapacity(capacity: number): CallsitePlan<typeof RUNTIME_SCHEMA, RuntimeContext> {
  return CONTEXT.defineOp(`span-pooling-capacity-${capacity}`, (ctx) => ctx.ok(null), undefined, {
    runtimeHint:
      RUNTIME_HINT_ANALYZED_VALID |
      RUNTIME_HINT_LOG |
      RUNTIME_HINT_RESULT |
      RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY |
      capacity,
  }).callsitePlan;
}

function writeRows(
  plan: CallsitePlan<typeof RUNTIME_SCHEMA, RuntimeContext>,
  buffer: SpanBuffer<typeof RUNTIME_SCHEMA>,
  rows: number,
  request: number,
): number {
  let checksum = 2_166_136_261;
  for (let row = 0; row < rows; row++) {
    const outputRow = plan.appenders.writeLogEntry(buffer, ENTRY_TYPE_INFO);
    const message = `request-${request}/row-${row}`;
    buffer.message(outputRow, message);
    buffer.metric(outputRow, request * 1_000 + row + 0.25);
    checksum = mix(checksum, outputRow);
    checksum = mix(checksum, message.length);
  }
  return checksum;
}

function buildRoot(
  plan: CallsitePlan<typeof RUNTIME_SCHEMA, RuntimeContext>,
  workload: WorkloadSpec,
  request: number,
): { readonly root: SpanBuffer<typeof RUNTIME_SCHEMA>; readonly checksum: number; readonly segments: number } {
  const capacity = plan.capacityTier;
  if (capacity === undefined) throw new Error('Span pooling CallsitePlan must freeze a capacity tier');
  const traceRoot = createTraceRoot(createTraceId(`span-pooling-${workload.name}-${request}`), TRACER);
  const root = STRATEGY.createSpanBuffer(RUNTIME_SCHEMA, traceRoot, plan.metadata, capacity, plan.SpanBufferClass);
  let checksum = writeRows(plan, root, Math.min(workload.rows, capacity), request);
  let active: SpanBuffer<typeof RUNTIME_SCHEMA> = root;
  for (let segment = 0; segment < workload.overflowSegments; segment++) {
    const overflow = STRATEGY.createOverflowBuffer(active);
    checksum = mix(checksum, writeRows(plan, overflow, Math.min(workload.rows, capacity), request + segment + 1));
    active = overflow;
  }
  if (workload.childRows !== 0) {
    const child = STRATEGY.createChildSpanBuffer(
      root,
      plan.metadata,
      plan.metadata,
      capacity,
      RUNTIME_SCHEMA,
      plan.SpanBufferClass,
    );
    checksum = mix(checksum, writeRows(plan, child, Math.min(workload.childRows, capacity), request + 17));
  }
  const segments = Array.from(iterateSpanTree(root)).length;
  return { root, checksum, segments };
}

function runWorkload(plan: CallsitePlan<typeof RUNTIME_SCHEMA, RuntimeContext>, workload: WorkloadSpec): RunOutcome {
  let checksum = 2_166_136_261;
  let roots = 0;
  let physicalSegments = 0;
  let releasedTopologies = 0;
  const retained: Array<SpanBuffer<typeof RUNTIME_SCHEMA>> = [];
  const releaseRetained = (): void => {
    for (const root of retained) {
      const topology = root._traceRoot._topology;
      const generation = topology.generation;
      STRATEGY.releaseBuffer(root);
      if (topology.generation !== generation + 1) throw new Error('Topology release did not advance its generation');
      releasedTopologies++;
    }
    retained.length = 0;
  };

  for (let request = 0; request < workload.requests; request++) {
    const built = buildRoot(plan, workload, request);
    retained.push(built.root);
    roots++;
    physicalSegments += built.segments;
    checksum = mix(checksum, built.checksum);
    checksum = mix(checksum, built.segments);
    if (retained.length === workload.batchWidth) releaseRetained();
  }
  releaseRetained();

  if (workload.name === 'idle-after-burst') {
    const idle = buildRoot(plan, { ...workload, rows: 1, overflowSegments: 0, childRows: 0 }, workload.requests);
    roots++;
    physicalSegments += idle.segments;
    checksum = mix(checksum, idle.checksum);
    const topology = idle.root._traceRoot._topology;
    const generation = topology.generation;
    STRATEGY.releaseBuffer(idle.root);
    if (topology.generation !== generation + 1) throw new Error('Idle topology release did not advance generation');
    releasedTopologies++;
  }

  return { checksum, roots, physicalSegments, releasedTopologies, poolRef: plan.poolRef };
}

function assertPreflight(
  plan: CallsitePlan<typeof RUNTIME_SCHEMA, RuntimeContext>,
  workload: WorkloadSpec,
): RunOutcome {
  if (plan.messageLayoutFamily !== 'dynamic-only') throw new Error('Span pooling plan chose the wrong message family');
  if (plan.poolRef !== null)
    throw new Error('Current PhysicalLayoutPlan unexpectedly retained a production buffer pool');
  const first = runWorkload(plan, workload);
  const second = runWorkload(plan, workload);
  if (
    first.checksum !== second.checksum ||
    first.roots !== second.roots ||
    first.physicalSegments !== second.physicalSegments ||
    first.releasedTopologies !== first.roots
  ) {
    throw new Error(`${workload.name}: production lifecycle preflight was not deterministic`);
  }
  return first;
}

const cli = parseCli(process.argv.slice(2));
const selected = workloads(cli.quick).filter(
  (workload) => cli.scenario === undefined || workload.name === cli.scenario,
);
const capacity = cli.quick ? 32 : 64;
const plan = planForCapacity(capacity);
const telemetry: string[] = [
  '# untimed production topology telemetry (preflight)',
  '# workload\troots\tphysicalSegments\treleasedTopologies\tpoolRef\tdescription',
];

for (const workload of selected) {
  const outcome = assertPreflight(plan, workload);
  telemetry.push(
    `${workload.name}\t${outcome.roots}\t${outcome.physicalSegments}\t${outcome.releasedTopologies}\t${String(outcome.poolRef)}\t${workload.description}`,
  );
  group(`span pooling production baseline | ${workload.name} | capacity=${capacity}`, () => {
    summary(() => {
      bench('production/fresh-layout+topology-release', () => {
        const result = runWorkload(plan, workload);
        do_not_optimize(result.checksum);
      }).baseline(true);
    });
  });
}

const output = `${telemetry.join('\n')}\n`;
if (cli.json) process.stderr.write(output);
else process.stdout.write(output);
await run({ format: cli.json ? 'json' : 'mitata', colors: !cli.json, throw: true });
