import {
  convertSpanTreeToArrowTable,
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  getSpanBufferClass,
  JsBufferStrategy,
  S,
  TestTracer,
} from '@smoothbricks/lmao/node';
import { bench, do_not_optimize, run } from 'mitata';

const SEMANTIC_SCHEMA_VERSION = 1;
const BUFFER_CAPACITY = 32;
const ROOT_TRACE_NAME = 'scenario request';
const ROOT_USER_ID = 'user-constant';
const ROOT_OPERATION = 'READ';
const ROOT_INDEX = 7;
const ROOT_DETAIL = 'detail-constant';
const ROOT_OUTCOME = 'failure';
const ROOT_DYNAMIC_MESSAGE = 'request 7: detail-constant';
const SUCCESS_CHILD_INDEX = 10;
const FAILURE_CHILD_INDEX = 11;
const LIFECYCLE_WARMUP_ITERATIONS = 2_048;
const ARROW_WARMUP_ITERATIONS = 256;

const scenarioSchema = defineLogSchema({
  userId: S.category(),
  operation: S.enum(['READ', 'WRITE']),
  index: S.number(),
  detail: S.text(),
  outcome: S.enum(['failure', 'success']),
});

const scenarioContext = defineOpContext({
  logSchema: scenarioSchema,
});

const { defineOp } = scenarioContext;

const childOp = defineOp('scenario-child', async (ctx, index: number, shouldFail: boolean) => {
  ctx.tag.index(index);

  if (shouldFail) {
    ctx.log.warn('child failed as expected').index(index).outcome('failure');
    return ctx
      .err('EXPECTED_CHILD_FAILURE')
      .with({ index, detail: 'child-failure', outcome: 'failure' })
      .message('child failed as expected');
  }

  ctx.log.info('child completed successfully').index(index).outcome('success');
  return ctx
    .ok(index * 2)
    .with({ index, detail: 'child-success', outcome: 'success' })
    .message('child completed');
});

const rootOp = defineOp(
  'scenario-root',
  async (
    ctx,
    userId: string,
    operation: 'READ' | 'WRITE',
    index: number,
    detail: string,
    outcome: 'success' | 'failure',
    dynamicMessage: string,
  ) => {
    ctx.tag.userId(userId).operation(operation).index(index).detail(detail).outcome(outcome);

    ctx.log.info('request accepted').userId(userId).operation(operation);
    ctx.log.info('request queued').index(index).detail(detail);
    ctx.log.warn('request benchmark warning').outcome(outcome);
    ctx.log.debug(dynamicMessage).userId(userId).index(index);

    const successfulChild = await ctx.span('scenario child success', childOp, SUCCESS_CHILD_INDEX, false);
    if (!successfulChild.success) {
      throw new Error('Scenario invariant failed: success child returned an error');
    }

    const failedChild = await ctx.span('scenario child expected failure', childOp, FAILURE_CHILD_INDEX, true);
    if (failedChild.success) {
      throw new Error('Scenario invariant failed: failure child returned success');
    }

    return ctx
      .ok(successfulChild.value + 41)
      .with({ index, detail: 'root-result', outcome: 'success' })
      .message('request completed');
  },
);

function resetEffectiveSpanBufferStats(): void {
  const stats = getSpanBufferClass(scenarioContext.logBinding.logSchema).stats;
  stats.capacity = BUFFER_CAPACITY;
  stats.totalWrites = 0;
  stats.spansCreated = 0;
}

function createScenarioTracer() {
  return new TestTracer(scenarioContext, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  });
}

async function executeScenario(tracer: TestTracer<typeof scenarioContext>) {
  return tracer.trace(
    ROOT_TRACE_NAME,
    {},
    rootOp,
    ROOT_USER_ID,
    ROOT_OPERATION,
    ROOT_INDEX,
    ROOT_DETAIL,
    ROOT_OUTCOME,
    ROOT_DYNAMIC_MESSAGE,
  );
}

function assertEqual(actual: unknown, expected: unknown, label: string): void {
  if (!Object.is(actual, expected)) {
    throw new Error(`${label}: expected ${String(expected)}, received ${String(actual)}`);
  }
}

function assertDeepEqual(actual: unknown, expected: unknown, label: string): void {
  const actualJson = JSON.stringify(actual);
  const expectedJson = JSON.stringify(expected);
  if (actualJson !== expectedJson) {
    throw new Error(`${label}: expected ${expectedJson}, received ${actualJson}`);
  }
}

async function semanticPreflight(outputPath?: string): Promise<string> {
  const tracer = createScenarioTracer();
  resetEffectiveSpanBufferStats();

  const result = await executeScenario(tracer);
  if (!result.success) {
    throw new Error(`Scenario result invariant failed: ${String(result.error)}`);
  }
  assertEqual(result.value, 61, 'Scenario result');
  assertEqual(tracer.rootBuffers.length, 1, 'Root buffer count');

  const tables = tracer.rootBuffers.map((rootBuffer) => convertSpanTreeToArrowTable(rootBuffer));
  const table = tables[0];
  if (!table) {
    throw new Error('Semantic preflight produced no Arrow table');
  }

  const deterministicColumnNames = ['entry_type', 'message', 'userId', 'operation', 'index', 'detail', 'outcome'];
  const deterministicColumns = deterministicColumnNames.map((columnName) => {
    const column = table.getChild(columnName);
    if (!column) {
      throw new Error(`Semantic preflight is missing deterministic Arrow column: ${columnName}`);
    }
    return column;
  });

  const rows = Array.from({ length: table.numRows }, (_, rowIndex) => {
    const row: Record<string, unknown> = {};
    for (let columnIndex = 0; columnIndex < deterministicColumnNames.length; columnIndex++) {
      const columnName = deterministicColumnNames[columnIndex];
      const column = deterministicColumns[columnIndex];
      if (columnName === undefined || column === undefined) {
        throw new Error(`Semantic preflight column index ${columnIndex} is unavailable`);
      }
      const value = column.get(rowIndex);
      row[columnName] = value === undefined ? null : value;
    }
    return row;
  });

  const counts = {
    'span-start': 0,
    'span-ok': 0,
    'span-err': 0,
    info: 0,
    debug: 0,
    warn: 0,
    error: 0,
  };

  for (const row of rows) {
    switch (row.entry_type) {
      case 'span-start':
        counts['span-start']++;
        break;
      case 'span-ok':
        counts['span-ok']++;
        break;
      case 'span-err':
        counts['span-err']++;
        break;
      case 'info':
        counts.info++;
        break;
      case 'debug':
        counts.debug++;
        break;
      case 'warn':
        counts.warn++;
        break;
      case 'error':
        counts.error++;
        break;
      default:
        throw new Error(`Unexpected semantic entry type: ${String(row.entry_type)}`);
    }
  }

  const expectedCounts = {
    'span-start': 3,
    'span-ok': 2,
    'span-err': 1,
    info: 3,
    debug: 1,
    warn: 2,
    error: 0,
  };
  assertEqual(table.numRows, 12, 'Arrow row count');
  assertDeepEqual(counts, expectedCounts, 'Entry counts');

  const expectedRows = [
    {
      entry_type: 'span-start',
      message: 'scenario request',
      userId: 'user-constant',
      operation: 'READ',
      index: 7,
      detail: 'detail-constant',
      outcome: 'failure',
    },
    {
      entry_type: 'span-ok',
      message: 'request completed',
      userId: null,
      operation: null,
      index: 7,
      detail: 'root-result',
      outcome: 'success',
    },
    {
      entry_type: 'info',
      message: 'request accepted',
      userId: 'user-constant',
      operation: 'READ',
      index: null,
      detail: null,
      outcome: null,
    },
    {
      entry_type: 'info',
      message: 'request queued',
      userId: null,
      operation: null,
      index: 7,
      detail: 'detail-constant',
      outcome: null,
    },
    {
      entry_type: 'warn',
      message: 'request benchmark warning',
      userId: null,
      operation: null,
      index: null,
      detail: null,
      outcome: 'failure',
    },
    {
      entry_type: 'debug',
      message: 'request 7: detail-constant',
      userId: 'user-constant',
      operation: null,
      index: 7,
      detail: null,
      outcome: null,
    },
    {
      entry_type: 'span-start',
      message: 'scenario child success',
      userId: null,
      operation: null,
      index: 10,
      detail: null,
      outcome: null,
    },
    {
      entry_type: 'span-ok',
      message: 'child completed',
      userId: null,
      operation: null,
      index: 10,
      detail: 'child-success',
      outcome: 'success',
    },
    {
      entry_type: 'info',
      message: 'child completed successfully',
      userId: null,
      operation: null,
      index: 10,
      detail: null,
      outcome: 'success',
    },
    {
      entry_type: 'span-start',
      message: 'scenario child expected failure',
      userId: null,
      operation: null,
      index: 11,
      detail: null,
      outcome: null,
    },
    {
      entry_type: 'span-err',
      message: 'child failed as expected',
      userId: null,
      operation: null,
      index: 11,
      detail: 'child-failure',
      outcome: 'failure',
    },
    {
      entry_type: 'warn',
      message: 'child failed as expected',
      userId: null,
      operation: null,
      index: 11,
      detail: null,
      outcome: 'failure',
    },
  ];
  assertDeepEqual(rows, expectedRows, 'Deterministic Arrow rows');

  const canonicalJson = `${JSON.stringify({
    schemaVersion: SEMANTIC_SCHEMA_VERSION,
    result: result.value,
    counts,
    rows,
  })}\n`;

  if (outputPath !== undefined) {
    await Bun.write(outputPath, canonicalJson);
  }

  tracer.clear();
  return canonicalJson;
}

async function benchmarkScenario(): Promise<void> {
  await semanticPreflight();

  const lifecycleTracer = createScenarioTracer();
  const arrowTracer = createScenarioTracer();
  resetEffectiveSpanBufferStats();

  for (let index = 0; index < LIFECYCLE_WARMUP_ITERATIONS; index++) {
    lifecycleTracer.clear();
    do_not_optimize(await executeScenario(lifecycleTracer));
  }
  for (let index = 0; index < ARROW_WARMUP_ITERATIONS; index++) {
    arrowTracer.clear();
    const result = await executeScenario(arrowTracer);
    const rootBuffer = arrowTracer.rootBuffers[0];
    if (!rootBuffer) {
      throw new Error('Arrow warmup did not capture a root buffer');
    }
    const table = convertSpanTreeToArrowTable(rootBuffer);
    do_not_optimize(result);
    do_not_optimize(table.numRows);
  }
  lifecycleTracer.clear();
  arrowTracer.clear();

  bench('request lifecycle', async () => {
    lifecycleTracer.clear();
    const result = await executeScenario(lifecycleTracer);
    do_not_optimize(result);
  }).gc('inner');

  bench('request lifecycle + Arrow', async () => {
    arrowTracer.clear();
    const result = await executeScenario(arrowTracer);
    const rootBuffer = arrowTracer.rootBuffers[0];
    if (!rootBuffer) {
      throw new Error('Arrow benchmark did not capture a root buffer');
    }
    const table = convertSpanTreeToArrowTable(rootBuffer);
    do_not_optimize(result);
    do_not_optimize(table.numRows);
  }).gc('inner');

  await run({ colors: false, format: { json: { samples: true } }, throw: true });
}

function optionValue(args: string[], name: string): string | undefined {
  const index = args.indexOf(name);
  if (index === -1) {
    return undefined;
  }
  const value = args[index + 1];
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${name} requires an explicit path`);
  }
  return value;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const semanticOutput = optionValue(args, '--semantic-output');
  const allowedArgumentCount = semanticOutput === undefined ? 0 : 2;
  if (args.length !== allowedArgumentCount) {
    throw new Error(`Unknown or duplicate workload arguments: ${args.join(' ')}`);
  }

  if (semanticOutput !== undefined) {
    await semanticPreflight(semanticOutput);
    return;
  }

  await benchmarkScenario();
}

await main();
