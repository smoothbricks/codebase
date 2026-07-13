import {
  convertSpanTreeToArrowTable,
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  JsBufferStrategy,
  S,
  TestTracer,
} from '@smoothbricks/lmao/node';
import { bench, do_not_optimize, run } from 'mitata';

const schema = defineLogSchema({
  userId: S.category(),
  operation: S.enum(['READ', 'WRITE']),
  index: S.number(),
  detail: S.text(),
});

const opContext = defineOpContext({ logSchema: schema });
const { defineOp } = opContext;
const childSpanName: string = 'plugin-baseline-child';


const baselineOp = defineOp('plugin-baseline', async (ctx, logCount: number) => {
  ctx.tag.userId('user-42').operation('READ').index(0).detail('root-tag');

  for (let index = 0; index < logCount; index++) {
    ctx.log
      .info('Processing complete')
      .userId(`user-${index % 3}`)
      .operation(index % 2 === 0 ? 'READ' : 'WRITE')
      .index(index)
      .detail(`detail-${index}`);
  }

  const child = await ctx.span(childSpanName, async (childCtx) => {
    childCtx.tag.userId('child-user').operation('READ').index(99).detail('child-tag');
    childCtx.log.info('Validation passed').index(100).detail('child-detail');
    return childCtx.ok({ child: true });
  });

  if (!child.success) return ctx.err('CHILD_FAILED', child.error);
  return ctx.ok({ processed: logCount, child: child.value.child });
});

const argv = process.argv.slice(2);
const args = new Set(argv);
const LOG_COUNT = args.has('--quick') ? 32 : 200;

function createWorkloadTracer() {
  return new TestTracer(opContext, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  });
}

async function executeWorkload(tracer: TestTracer<typeof opContext>) {
  const result = await tracer.trace('plugin-baseline-trace', baselineOp, LOG_COUNT);
  return { tracer, result };
}

function normalizeArrowValue(value: unknown): unknown {
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Uint8Array) return Array.from(value);
  if (Array.isArray(value)) return value.map(normalizeArrowValue);
  if (value !== null && typeof value === 'object') return JSON.parse(JSON.stringify(value));
  return value;
}

async function semanticSnapshot() {
  const { tracer, result } = await executeWorkload(createWorkloadTracer());
  if (!result.success || result.value.processed !== LOG_COUNT || !result.value.child) {
    throw new Error('The baseline workload returned an unexpected result');
  }

  const root = tracer.rootBuffers[0];
  if (!root) throw new Error('The baseline workload produced no root trace buffer');
  if (!root._overflow) throw new Error('The baseline workload did not exercise log overflow');
  if (root._children.length !== 1) throw new Error('The baseline workload did not produce exactly one child span');

  const table = convertSpanTreeToArrowTable(root);
  // These are all workload-observable Arrow values. Trace identity, timestamps,
  // and compiler-injected source/template metadata are intentionally excluded.
  const columns = ['entry_type', 'message', 'userId', 'operation', 'index', 'detail'] as const;
  const rows = Array.from({ length: table.numRows }, (_, rowIndex) => {
    const row: Record<string, unknown> = {};
    for (const name of columns) {
      const column = table.getChild(name);
      row[name] = column ? normalizeArrowValue(column.get(rowIndex)) : null;
    }
    return row;
  });

  const entryTypes = rows.map((row) => row.entry_type);
  if (entryTypes.filter((value) => value === 'info').length !== LOG_COUNT + 1) {
    throw new Error('Decoded Arrow rows did not preserve all root and child logs');
  }
  if (entryTypes.filter((value) => value === 'span-start').length !== 2) {
    throw new Error('Decoded Arrow rows did not preserve root and child tag rows');
  }
  if (entryTypes.filter((value) => value === 'span-ok').length !== 2) {
    throw new Error('Decoded Arrow rows did not preserve root and child result rows');
  }

  const canonical = JSON.stringify({ result: result.value, rows });
  const checksum = new Bun.CryptoHasher('sha256').update(canonical).digest('hex');
  return { checksum, rowCount: rows.length, rows };
}

if (args.has('--semantic')) {
  const outputIndex = argv.indexOf('--semantic-output');
  const outputPath = outputIndex < 0 ? undefined : argv[outputIndex + 1];
  if (!outputPath) throw new Error('--semantic requires --semantic-output <path>');
  await Bun.write(outputPath, `${JSON.stringify(await semanticSnapshot())}\n`);
} else if (args.has('--benchmark')) {
  // Semantic validation and runtime construction are deliberately untimed.
  await semanticSnapshot();
  const benchmarkTracer = createWorkloadTracer();
  bench('typed LMAO trace workload', async () => {
    benchmarkTracer.clear();
    const { result } = await executeWorkload(benchmarkTracer);
    do_not_optimize(result);
  }).gc('inner');
  await run({ colors: false, format: { json: { samples: true } }, throw: true });
} else {
  throw new Error('Expected --semantic or --benchmark');
}
