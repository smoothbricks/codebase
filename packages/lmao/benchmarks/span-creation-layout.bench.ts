import { bench, group, run, summary } from 'mitata';
import {
  createTestLogBinding,
  createTestOpMetadata,
  createTestSchema,
  createTestTraceRoot,
} from '../src/lib/__tests__/test-helpers.js';
import {
  createResultWriter,
  createTagWriter,
  getResultWriterClass,
  getTagWriterClass,
} from '../src/lib/codegen/fixedPositionWriterGenerator.js';
import { createSpanLogger, createSpanLoggerClass } from '../src/lib/codegen/spanLoggerGenerator.js';
import type { OpContext } from '../src/lib/opContext/types.js';
import { Ok } from '../src/lib/result.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_FULL_CAPABILITIES,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_SPAN,
} from '../src/lib/runtimeHint.js';
import {
  createChildSpanBuffer,
  createOverflowBuffer,
  createSpanBuffer,
  getSpanBufferClass,
} from '../src/lib/spanBuffer.js';
import {
  createSpanContextClass,
  type SpanContextInstance,
  writeSpanEnd,
  writeSpanStart,
} from '../src/lib/spanContext.js';
import type { AnySpanBuffer, SpanBuffer } from '../src/lib/types.js';

const CAPACITIES = [8, 64, 1024] as const;

const schema = createTestSchema({});
const logBinding = createTestLogBinding(schema);
const metadata = createTestOpMetadata({ name: 'span-creation-layout' });
const SpanBufferClass = getSpanBufferClass(schema);
const SpanLoggerClass = createSpanLoggerClass(schema);
const TagWriterClass = getTagWriterClass(schema);
const ResultWriterClass = getResultWriterClass(schema);

type BenchmarkContext = OpContext & { logSchema: typeof schema };
type Context = SpanContextInstance<BenchmarkContext>;
const SpanContextClass = createSpanContextClass<BenchmarkContext>(schema, logBinding);

type Outcome = {
  root: SpanBuffer<typeof schema>;
  contexts: readonly Context[];
  retained: readonly object[];
};

type Scenario = {
  label: string;
  run: () => Outcome | Promise<Outcome>;
};

function createRoot(capacity: number): SpanBuffer<typeof schema> {
  return createSpanBuffer(schema, createTestTraceRoot(), metadata, capacity);
}

function createRootDirectModeled(capacity: number): SpanBuffer<typeof schema> {
  const traceRoot = createTestTraceRoot();
  return new SpanBufferClass(capacity, SpanBufferClass.stats, undefined, false, metadata, metadata, traceRoot);
}

function createRootContext(root: SpanBuffer<typeof schema>): { context: Context; retained: object[] } {
  const logger = createSpanLogger(schema, root);
  const tag = createTagWriter(schema, root);
  const context = new SpanContextClass(root, schema, logger, tag);
  return { context, retained: [logger, tag] };
}

function finishRoot(root: SpanBuffer<typeof schema>, result = new Ok(1, schema, root)): void {
  writeSpanEnd(root, result);
}

function childrenOf(buffer: AnySpanBuffer): AnySpanBuffer[] {
  return buffer._children.map((child) => {
    const direct = child as AnySpanBuffer;
    const wrapped = Reflect.get(child, '_buffer');
    return wrapped && typeof wrapped === 'object' ? (wrapped as AnySpanBuffer) : direct;
  });
}

function messageAt(buffer: AnySpanBuffer, row: number): string {
  const values = Reflect.get(buffer, 'message_values');
  return Array.isArray(values) && typeof values[row] === 'string' ? values[row] : '';
}

function lifecycleChecksum(outcome: Outcome): string {
  const encode = (buffer: AnySpanBuffer, parent: AnySpanBuffer | undefined): string => {
    const rows = Array.from(buffer.entry_type.slice(0, buffer._writeIndex)).join(',');
    const messages = Array.from({ length: buffer._writeIndex }, (_, row) => messageAt(buffer, row)).join(',');
    const overflow: string[] = [];
    let next = buffer._overflow;
    while (next) {
      overflow.push(`${next.span_id === buffer.span_id ? 'same' : 'different'}:${next._writeIndex}`);
      next = next._overflow;
    }
    const children = childrenOf(buffer)
      .map((child) => encode(child, buffer))
      .join('|');
    const parentIdentity =
      parent === undefined
        ? buffer.parent_span_id === 0
        : buffer.parent_span_id === parent.span_id && buffer.trace_id === parent.trace_id;
    return `[${parentIdentity ? 'parent-ok' : 'parent-bad'};${rows};${messages};${overflow.join(',')};${children}]`;
  };
  return encode(outcome.root, undefined);
}

function rootCreationScenarios(capacity: number): Scenario[] {
  const complete = (root: SpanBuffer<typeof schema>): Outcome => {
    writeSpanStart(root, 'root');
    const result = new Ok(1, schema, root);
    finishRoot(root, result);
    return { root, contexts: [], retained: [result] };
  };
  return [
    { label: 'production factory', run: () => complete(createRoot(capacity)) },
    {
      label: 'production strategy',
      run: () => {
        const traceRoot = createTestTraceRoot();
        return complete(traceRoot.tracer.bufferStrategy.createSpanBuffer(schema, traceRoot, metadata, capacity));
      },
    },
    { label: 'modeled direct constructor', run: () => complete(createRootDirectModeled(capacity)) },
  ];
}

function setupScenarios(capacity: number): Scenario[] {
  const setup = (kind: 'buffer' | 'logger' | 'tag' | 'result' | 'context'): Outcome => {
    const root = createRoot(capacity);
    writeSpanStart(root, 'root');
    const retained: object[] = [];
    const contexts: Context[] = [];
    if (kind === 'logger') retained.push(new SpanLoggerClass(root));
    if (kind === 'tag') retained.push(new TagWriterClass(root));
    if (kind === 'result') retained.push(new ResultWriterClass(root, 1, false));
    if (kind === 'context') {
      const logger = new SpanLoggerClass(root);
      const tag = new TagWriterClass(root);
      const context = new SpanContextClass(root, schema, logger, tag);
      retained.push(logger, tag);
      contexts.push(context);
    }
    const result = new Ok(1, schema, root);
    retained.push(result);
    finishRoot(root, result);
    return { root, contexts, retained };
  };
  return [
    { label: 'production span buffer', run: () => setup('buffer') },
    { label: 'production span logger', run: () => setup('logger') },
    { label: 'production tag writer', run: () => setup('tag') },
    { label: 'production result writer', run: () => setup('result') },
    { label: 'production span context', run: () => setup('context') },
  ];
}

function childCreationScenarios(capacity: number): Scenario[] {
  const complete = (factory: (root: SpanBuffer<typeof schema>) => SpanBuffer<typeof schema>): Outcome => {
    const root = createRoot(capacity);
    writeSpanStart(root, 'root');
    const child = factory(root);
    root._children.push(child);
    writeSpanStart(child, 'child');
    const childResult = new Ok(1, schema, child);
    finishRoot(child, childResult);
    const rootResult = new Ok(1, schema, root);
    finishRoot(root, rootResult);
    return { root, contexts: [], retained: [childResult, rootResult] };
  };
  return [
    {
      label: 'production factory',
      run: () => complete((root) => createChildSpanBuffer(root, SpanBufferClass, metadata, metadata, capacity)),
    },
    {
      label: 'production strategy',
      run: () =>
        complete((root) =>
          root._traceRoot.tracer.bufferStrategy.createChildSpanBuffer(root, metadata, metadata, capacity, schema),
        ),
    },
  ];
}

function overflowCreationScenarios(capacity: number): Scenario[] {
  const complete = (factory: (root: SpanBuffer<typeof schema>) => SpanBuffer<typeof schema>): Outcome => {
    (SpanBufferClass.stats as { capacity: number }).capacity = capacity;
    const root = createRoot(capacity);
    writeSpanStart(root, 'root');
    factory(root);
    const result = new Ok(1, schema, root);
    finishRoot(root, result);
    return { root, contexts: [], retained: [result] };
  };
  return [
    { label: 'production factory', run: () => complete(createOverflowBuffer) },
    {
      label: 'production strategy',
      run: () => complete((root) => root._traceRoot.tracer.bufferStrategy.createOverflowBuffer(root)),
    },
    { label: 'production instance method', run: () => complete((root) => root.getOrCreateOverflow()) },
  ];
}

function makeRootFixture(capacity: number): { root: SpanBuffer<typeof schema>; context: Context; retained: object[] } {
  const root = createRoot(capacity);
  writeSpanStart(root, 'root');
  const { context, retained } = createRootContext(root);
  const featureFlags = { forContext: () => featureFlags };
  context.ff = featureFlags as Context['ff'];
  context.deps = {};
  return { root, context, retained };
}

function finalizeFixture(root: SpanBuffer<typeof schema>, contexts: Context[], retained: object[]): Outcome {
  const result = new Ok(1, schema, root);
  retained.push(result);
  finishRoot(root, result);
  return { root, contexts, retained };
}

function contextShapeScenarios(capacity: number, depth: 0 | 1 | 3): Scenario[] {
  if (depth === 0) {
    return [
      {
        label: 'production root lifecycle',
        run: () => {
          const fixture = makeRootFixture(capacity);
          return finalizeFixture(fixture.root, [fixture.context], fixture.retained);
        },
      },
    ];
  }

  const generic = async (): Promise<Outcome> => {
    const { root, context, retained } = makeRootFixture(capacity);
    const contexts: Context[] = [context];
    if (depth === 1) {
      await context.span('child', (child) => {
        contexts.push(child as Context);
        const result = child.ok(1);
        retained.push(result);
        return result;
      });
    } else {
      await context.span('child-1', async (child1) => {
        contexts.push(child1 as Context);
        await child1.span('child-2', (child2) => {
          contexts.push(child2 as Context);
          const result = child2.ok(1);
          retained.push(result);
          return result;
        });
        const result = child1.ok(1);
        retained.push(result);
        return result;
      });
    }
    return finalizeFixture(root, contexts, retained);
  };

  const direct = async (): Promise<Outcome> => {
    const { root, context, retained } = makeRootFixture(capacity);
    const contexts: Context[] = [context];
    if (depth === 1) {
      await context.span0(0, 'child', context._newCtx0(), SpanBufferClass, undefined, metadata, (child) => {
        contexts.push(child as Context);
        const result = child.ok(1);
        retained.push(result);
        return result;
      });
    } else {
      await context.span0(0, 'child-1', context._newCtx0(), SpanBufferClass, undefined, metadata, async (child1) => {
        const directChild1 = child1 as Context;
        contexts.push(directChild1);
        await directChild1.span0(
          0,
          'child-2',
          directChild1._newCtx0(),
          SpanBufferClass,
          undefined,
          metadata,
          (child2) => {
            contexts.push(child2 as Context);
            const result = child2.ok(1);
            retained.push(result);
            return result;
          },
        );
        const result = child1.ok(1);
        retained.push(result);
        return result;
      });
    }
    return finalizeFixture(root, contexts, retained);
  };

  return [
    { label: 'production generic dispatch', run: generic },
    { label: 'production direct span0', run: direct },
  ];
}

function capabilityScenarios(capacity: number): Scenario[] {
  const hints = [
    { label: 'production unanalysed full', hint: 0 },
    {
      label: 'production analysed full mask',
      hint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_FULL_CAPABILITIES | capacity,
    },
    {
      label: 'production analysed result only',
      hint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | capacity,
    },
    {
      label: 'production analysed result and span',
      hint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | RUNTIME_HINT_SPAN | capacity,
    },
  ];
  return hints.map(({ label, hint }) => ({
    label,
    run: async (): Promise<Outcome> => {
      const { root, context, retained } = makeRootFixture(capacity);
      const contexts: Context[] = [context];
      await context.span0(
        0,
        'child',
        context._newCtx0(),
        SpanBufferClass,
        undefined,
        metadata,
        (child) => {
          contexts.push(child as Context);
          const result = child.ok(1);
          retained.push(result);
          return result;
        },
        hint,
      );
      return finalizeFixture(root, contexts, retained);
    },
  }));
}

async function registerScenario(name: string, scenarios: readonly Scenario[]): Promise<void> {
  const checksums = await Promise.all(scenarios.map(async (scenario) => lifecycleChecksum(await scenario.run())));
  const expected = checksums[0];
  for (let index = 1; index < checksums.length; index++) {
    if (checksums[index] !== expected) {
      throw new Error(`${name}: lifecycle checksum mismatch for ${scenarios[index]?.label}`);
    }
  }

  summary(() => {
    group(name, () => {
      scenarios.forEach((scenario, index) => {
        bench(scenario.label, scenario.run).baseline(index === 0);
      });
    });
  });
}

for (const capacity of CAPACITIES) {
  await registerScenario(`span creation / root / capacity ${capacity}`, rootCreationScenarios(capacity));
  await registerScenario(`span creation / setup / capacity ${capacity}`, setupScenarios(capacity));
  await registerScenario(`span creation / child / capacity ${capacity}`, childCreationScenarios(capacity));
  await registerScenario(`span creation / overflow / capacity ${capacity}`, overflowCreationScenarios(capacity));
  await registerScenario(
    `span creation / context shape / root only / capacity ${capacity}`,
    contextShapeScenarios(capacity, 0),
  );
  await registerScenario(
    `span creation / context shape / one child / capacity ${capacity}`,
    contextShapeScenarios(capacity, 1),
  );
  await registerScenario(
    `span creation / context shape / nested children / capacity ${capacity}`,
    contextShapeScenarios(capacity, 3),
  );
  await registerScenario(`span creation / capability mask / capacity ${capacity}`, capabilityScenarios(capacity));
}

const format = process.argv.includes('--markdown') ? 'markdown' : process.argv.includes('--json') ? 'json' : undefined;

if (format) await run({ format });
else await run();
