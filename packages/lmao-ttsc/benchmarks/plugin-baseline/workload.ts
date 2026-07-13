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
const ROOT_LOG_COUNT = 62;

const baselineOp = defineOp(
  'plugin-baseline',
  async (ctx, dynamicMessage: string, dynamicOperation: 'READ' | 'WRITE') => {
    ctx.tag.userId('user-42').operation('READ').index(0).detail('root-tag');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(0).detail('detail-0');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(1).detail('detail-1');

    ctx.log.info('Processing complete').userId('user-2').operation('READ').index(2).detail('detail-2');

    ctx.log.debug(dynamicMessage).userId('user-0').operation(dynamicOperation).index(3).detail('detail-3');

    ctx.log.info('Processing complete').userId('user-1').operation('READ').index(4).detail('detail-4');

    ctx.log.debug(dynamicMessage).userId('user-2').operation(dynamicOperation).index(5).detail('detail-5');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(6).detail('detail-6');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(7).detail('detail-7');

    ctx.log.info('Processing complete').userId('user-2').operation('READ').index(8).detail('detail-8');

    ctx.log.debug(dynamicMessage).userId('user-0').operation(dynamicOperation).index(9).detail('detail-9');

    ctx.log.info('Processing complete').userId('user-1').operation('READ').index(10).detail('detail-10');

    ctx.log.debug(dynamicMessage).userId('user-2').operation(dynamicOperation).index(11).detail('detail-11');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(12).detail('detail-12');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(13).detail('detail-13');

    ctx.log.info('Processing complete').userId('user-2').operation('READ').index(14).detail('detail-14');

    ctx.log.debug(dynamicMessage).userId('user-0').operation(dynamicOperation).index(15).detail('detail-15');

    ctx.log.info('Processing complete').userId('user-1').operation('READ').index(16).detail('detail-16');

    ctx.log.debug(dynamicMessage).userId('user-2').operation(dynamicOperation).index(17).detail('detail-17');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(18).detail('detail-18');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(19).detail('detail-19');

    ctx.log.info('Processing complete').userId('user-2').operation('READ').index(20).detail('detail-20');

    ctx.log.debug(dynamicMessage).userId('user-0').operation(dynamicOperation).index(21).detail('detail-21');

    ctx.log.info('Processing complete').userId('user-1').operation('READ').index(22).detail('detail-22');

    ctx.log.debug(dynamicMessage).userId('user-2').operation(dynamicOperation).index(23).detail('detail-23');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(24).detail('detail-24');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(25).detail('detail-25');

    ctx.log.info('Processing complete').userId('user-2').operation('READ').index(26).detail('detail-26');

    ctx.log.debug(dynamicMessage).userId('user-0').operation(dynamicOperation).index(27).detail('detail-27');

    ctx.log.info('Processing complete').userId('user-1').operation('READ').index(28).detail('detail-28');

    ctx.log.debug(dynamicMessage).userId('user-2').operation(dynamicOperation).index(29).detail('detail-29');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(30).detail('detail-30');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(31).detail('detail-31');

    ctx.log.info('Processing complete').userId('user-2').operation('READ').index(32).detail('detail-32');

    ctx.log.debug(dynamicMessage).userId('user-0').operation(dynamicOperation).index(33).detail('detail-33');

    ctx.log.info('Processing complete').userId('user-1').operation('READ').index(34).detail('detail-34');

    ctx.log.debug(dynamicMessage).userId('user-2').operation(dynamicOperation).index(35).detail('detail-35');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(36).detail('detail-36');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(37).detail('detail-37');

    ctx.log.info('Processing complete').userId('user-2').operation('READ').index(38).detail('detail-38');

    ctx.log.debug(dynamicMessage).userId('user-0').operation(dynamicOperation).index(39).detail('detail-39');

    ctx.log.info('Processing complete').userId('user-1').operation('READ').index(40).detail('detail-40');

    ctx.log.debug(dynamicMessage).userId('user-2').operation(dynamicOperation).index(41).detail('detail-41');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(42).detail('detail-42');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(43).detail('detail-43');

    ctx.log.info('Processing complete').userId('user-2').operation('READ').index(44).detail('detail-44');

    ctx.log.debug(dynamicMessage).userId('user-0').operation(dynamicOperation).index(45).detail('detail-45');

    ctx.log.info('Processing complete').userId('user-1').operation('READ').index(46).detail('detail-46');

    ctx.log.debug(dynamicMessage).userId('user-2').operation(dynamicOperation).index(47).detail('detail-47');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(48).detail('detail-48');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(49).detail('detail-49');

    ctx.log.info('Processing complete').userId('user-2').operation('READ').index(50).detail('detail-50');

    ctx.log.debug(dynamicMessage).userId('user-0').operation(dynamicOperation).index(51).detail('detail-51');

    ctx.log.info('Processing complete').userId('user-1').operation('READ').index(52).detail('detail-52');

    ctx.log.debug(dynamicMessage).userId('user-2').operation(dynamicOperation).index(53).detail('detail-53');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(54).detail('detail-54');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(55).detail('detail-55');

    ctx.log.info('Processing complete').userId('user-2').operation('READ').index(56).detail('detail-56');

    ctx.log.debug(dynamicMessage).userId('user-0').operation(dynamicOperation).index(57).detail('detail-57');

    ctx.log.info('Processing complete').userId('user-1').operation('READ').index(58).detail('detail-58');

    ctx.log.debug(dynamicMessage).userId('user-2').operation(dynamicOperation).index(59).detail('detail-59');

    ctx.log.info('Processing complete').userId('user-0').operation('READ').index(60).detail('detail-60');

    ctx.log.debug(dynamicMessage).userId('user-1').operation(dynamicOperation).index(61).detail('detail-61');
    return ctx.ok({ processed: ROOT_LOG_COUNT });
  },
);

const childCoverageOp = defineOp('plugin-baseline-child-coverage', async (ctx) => {
  for (let index = 0; index < 12; index++) {
    ctx.log
      .info('Processing complete')
      .index(200 + index)
      .detail('overflow-coverage');
  }
  const child = await ctx.span('validate-items', async (childCtx) => {
    childCtx.tag.userId('child-user').operation('READ').index(99).detail('child-tag');
    childCtx.log.info('Validation passed').index(100).detail('child-detail');
    return childCtx.ok({ child: true });
  });
  if (!child.success) return ctx.err(child.error);
  return ctx.ok({ child: child.value.child });
});

const argv = process.argv.slice(2);
const args = new Set(argv);
const TRACE_REPETITIONS = args.has('--quick') ? 1 : 4;

function createWorkloadTracer() {
  return new TestTracer(opContext, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  });
}

async function executeWorkload(tracer: TestTracer<typeof opContext>) {
  const result = await tracer.trace('plugin-baseline-trace', baselineOp, 'Processing items', 'WRITE');
  const childResult = await tracer.trace('plugin-baseline-child-coverage', childCoverageOp);
  return { tracer, result, childResult };
}

function normalizeArrowValue(value: unknown): unknown {
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Uint8Array) return Array.from(value);
  if (Array.isArray(value)) return value.map(normalizeArrowValue);
  if (value !== null && typeof value === 'object') return JSON.parse(JSON.stringify(value));
  return value;
}

async function semanticSnapshot() {
  const { tracer, result, childResult } = await executeWorkload(createWorkloadTracer());
  if (
    !result.success ||
    result.value.processed !== ROOT_LOG_COUNT ||
    !childResult.success ||
    !childResult.value.child
  ) {
    throw new Error('The baseline workload returned an unexpected result');
  }

  const root = tracer.rootBuffers[0];
  const overflowRoot = tracer.rootBuffers[1];
  if (!root || !overflowRoot) throw new Error('The baseline workload produced incomplete root trace buffers');
  if (!overflowRoot._overflow) throw new Error('The baseline workload did not exercise log overflow');

  const columns = ['entry_type', 'message', 'userId', 'operation', 'index', 'detail'] as const;
  const rows = tracer.rootBuffers.flatMap((buffer) => {
    const table = convertSpanTreeToArrowTable(buffer);
    return Array.from({ length: table.numRows }, (_, rowIndex) => {
      const row: Record<string, unknown> = {};
      for (const name of columns) {
        const column = table.getChild(name);
        row[name] = column ? normalizeArrowValue(column.get(rowIndex)) : null;
      }
      return row;
    });
  });

  const entryTypes = rows.map((row) => row.entry_type);
  if (entryTypes.filter((value) => value === 'info' || value === 'debug').length !== ROOT_LOG_COUNT + 13) {
    throw new Error('Decoded Arrow rows did not preserve all root and child logs');
  }
  if (entryTypes.filter((value) => value === 'span-start').length !== 3) {
    throw new Error('Decoded Arrow rows did not preserve root and child tag rows');
  }
  if (entryTypes.filter((value) => value === 'span-ok').length !== 3) {
    throw new Error('Decoded Arrow rows did not preserve root and child result rows');
  }

  const canonical = JSON.stringify({ result: result.value, childResult: childResult.value, rows });
  const checksum = new Bun.CryptoHasher('sha256').update(canonical).digest('hex');
  return { checksum, rowCount: rows.length, rows };
}

if (args.has('--semantic')) {
  const outputIndex = argv.indexOf('--semantic-output');
  const outputPath = outputIndex < 0 ? undefined : argv[outputIndex + 1];
  if (!outputPath) throw new Error('--semantic requires --semantic-output <path>');
  await Bun.write(
    outputPath,
    `${JSON.stringify(await semanticSnapshot())}
`,
  );
} else if (args.has('--benchmark')) {
  await semanticSnapshot();
  const benchmarkTracer = createWorkloadTracer();
  bench('typed LMAO trace workload', async () => {
    benchmarkTracer.clear();
    let result: unknown;
    for (let repetition = 0; repetition < TRACE_REPETITIONS; repetition++) {
      result = (await executeWorkload(benchmarkTracer)).result;
    }
    do_not_optimize(result);
  }).gc('inner');
  await run({ colors: false, format: { json: { samples: true } }, throw: true });
} else {
  throw new Error('Expected --semantic or --benchmark');
}
