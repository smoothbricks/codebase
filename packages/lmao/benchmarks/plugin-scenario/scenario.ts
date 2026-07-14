import {
  convertSpanTreeToArrowTable,
  defineLogSchema,
  defineOpContext,
  getSpanBufferClass,
  JsBufferStrategy,
  S,
  TestTracer,
  type TraceRootFactory,
} from '@smoothbricks/lmao';

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

const scenarioSchema = defineLogSchema({
  userId: S.category(),
  operation: S.enum(['READ', 'WRITE']),
  index: S.number(),
  detail: S.text(),
  outcome: S.enum(['failure', 'success']),
});

export type ScenarioTraceRootFactory = TraceRootFactory<typeof scenarioSchema>;

const scenarioContext = defineOpContext({
  logSchema: scenarioSchema,
});
export type ScenarioTracer = TestTracer<typeof scenarioContext>;

const { defineOp } = scenarioContext;

function defineScenarioChild(index: number, shouldFail: boolean) {
  return defineOp('scenario-child', (ctx) => {
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
}

const successfulChildOp = defineScenarioChild(SUCCESS_CHILD_INDEX, false);
const failedChildOp = defineScenarioChild(FAILURE_CHILD_INDEX, true);

const rootOp = defineOp(
  'scenario-root',
  (
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

    const successfulChild = ctx.spanSync('scenario child success', successfulChildOp);
    if (!successfulChild.success) {
      throw new Error('Scenario invariant failed: success child returned an error');
    }

    const failedChild = ctx.spanSync('scenario child expected failure', failedChildOp);
    if (failedChild.success) {
      throw new Error('Scenario invariant failed: failure child returned success');
    }

    return ctx
      .ok(successfulChild.value + 41)
      .with({ index, detail: 'root-result', outcome: 'success' })
      .message('request completed');
  },
);

export function resetScenarioBufferStats(): void {
  const stats = getSpanBufferClass(scenarioContext.logBinding.logSchema).stats;
  stats.capacity = BUFFER_CAPACITY;
  stats.totalWrites = 0;
  stats.spansCreated = 0;
}

export function createScenarioTracer(createTraceRoot: ScenarioTraceRootFactory): ScenarioTracer {
  return new TestTracer(scenarioContext, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  });
}

export function executeScenario(tracer: ScenarioTracer) {
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

export function generateCanonicalSemanticSnapshot(createTraceRoot: TraceRootFactory<typeof scenarioSchema>): string {
  const tracer = createScenarioTracer(createTraceRoot);
  resetScenarioBufferStats();

  const result = executeScenario(tracer);
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

  tracer.clear();
  return canonicalJson;
}
