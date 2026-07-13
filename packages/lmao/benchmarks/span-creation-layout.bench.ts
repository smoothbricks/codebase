import { bench, group, run, summary } from 'mitata';
import {
  createTestLogBinding,
  createTestOpMetadata,
  createTestSchema,
  createTestTraceRoot,
} from '../src/lib/__tests__/test-helpers.js';
import {
  getResultWriterClass,
  getTagWriterClass,
  type WriterState,
} from '../src/lib/codegen/fixedPositionWriterGenerator.js';
import { createSpanLoggerClass } from '../src/lib/codegen/spanLoggerGenerator.js';
import type { OpMetadata } from '../src/lib/op.js';
import {
  getPhysicalLayoutPlan,
  sealCallsitePlan,
  type CallsitePlan,
} from '../src/lib/physicalLayoutPlan.js';
import type { OpContext } from '../src/lib/opContext/types.js';
import { Ok } from '../src/lib/result.js';
import {
  decodeRuntimeHint,
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
import { iterateSpanChildren } from '../src/lib/traceTopology.js';
import type { AnySpanBuffer, SpanBuffer } from '../src/lib/types.js';

const CAPACITIES = [8, 64, 1024] as const;
const QUICK = process.argv.includes('--quick');
const CAPACITY_ARGUMENT = process.argv.find((argument) => argument.startsWith('--capacity='));
const SELECTED_CAPACITY = CAPACITY_ARGUMENT === undefined
  ? undefined
  : Number(CAPACITY_ARGUMENT.slice('--capacity='.length));
const MEMORY_BATCH_SIZE = QUICK ? 4 : 32;
const SCENARIO_ARGUMENT = process.argv.find((argument) => argument.startsWith('--scenario='));
const SELECTED_SCENARIO = SCENARIO_ARGUMENT?.slice('--scenario='.length);
const CAPACITY_FILTER = SELECTED_CAPACITY === undefined
  ? CAPACITIES
  : CAPACITIES.filter((capacity) => capacity === SELECTED_CAPACITY);
if (CAPACITY_ARGUMENT !== undefined && CAPACITY_FILTER.length === 0) {
  throw new Error(`Unsupported capacity ${CAPACITY_ARGUMENT}; expected 8, 64, or 1024`);
}

const schema = createTestSchema({});
const logBinding = createTestLogBinding(schema);
const SpanBufferClass = getSpanBufferClass(schema);
const SpanLoggerClass = createSpanLoggerClass(schema);
const TagWriterClass = getTagWriterClass(schema);
const ResultWriterClass = getResultWriterClass(schema);

type BenchmarkContext = OpContext & { logSchema: typeof schema };
type Context = SpanContextInstance<BenchmarkContext>;
const SpanContextClass = createSpanContextClass<BenchmarkContext>(schema, logBinding);

interface BenchmarkPlan {
  metadata: OpMetadata;
  callsitePlan: CallsitePlan<typeof schema, BenchmarkContext>;
}

const benchmarkPlans = new Map<number, BenchmarkPlan>();

function benchmarkPlan(capacity: number, runtimeHint?: number): BenchmarkPlan {
  const resolvedHint = runtimeHint ?? (RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_FULL_CAPABILITIES | capacity);
  const cached = benchmarkPlans.get(resolvedHint);
  if (cached) return cached;
  const planSpanContextClass = createSpanContextClass<BenchmarkContext>(
    schema,
    logBinding,
    decodeRuntimeHint(resolvedHint).capabilities,
  );
  const physicalLayoutPlan = getPhysicalLayoutPlan<typeof schema, BenchmarkContext>(
    SpanBufferClass,
    resolvedHint,
    planSpanContextClass,
  );
  const metadata = Object.freeze({
    ...createTestOpMetadata({ name: 'span-creation-layout' }),
    _physicalLayoutPlan: physicalLayoutPlan,
  });
  const plan = { metadata, callsitePlan: sealCallsitePlan(physicalLayoutPlan, metadata) };
  benchmarkPlans.set(resolvedHint, plan);
  return plan;
}

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
  return createSpanBuffer(schema, createTestTraceRoot(), benchmarkPlan(capacity).metadata, capacity);
}


function createRootContext(root: SpanBuffer<typeof schema>): { context: Context; retained: object[] } {
  const context = new SpanContextClass(root, schema, benchmarkPlan(root._capacity).callsitePlan);
  return { context, retained: [context.log, context.tag] };
}

function finishRoot(root: SpanBuffer<typeof schema>, result = new Ok(1, schema, root)): void {
  writeSpanEnd(root, result);
}

function createWriterState(root: SpanBuffer<typeof schema>): WriterState {
  const callsitePlan = benchmarkPlan(root._capacity).callsitePlan;
  return {
    _spanBuffer: root,
    _buffer: root,
    _appendLogEntry: callsitePlan.appendLogEntry,
    _physicalLayoutPlan: callsitePlan,
  };
}

function childrenOf(buffer: AnySpanBuffer): AnySpanBuffer[] {
  return Array.from(iterateSpanChildren(buffer));
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

function collectBuffers(root: AnySpanBuffer): AnySpanBuffer[] {
  const buffers: AnySpanBuffer[] = [];
  const visit = (buffer: AnySpanBuffer): void => {
    buffers.push(buffer);
    let overflow = buffer._overflow;
    while (overflow) {
      buffers.push(overflow);
      overflow = overflow._overflow;
    }
    for (const child of childrenOf(buffer)) visit(child);
  };
  visit(root);
  return buffers;
}

function exactRetainedStorage(outcomes: readonly Outcome[]): {
  backingStores: number;
  backingStoreBytes: number;
  typedArrayViews: number;
  typedArrayViewBytes: number;
  reachableDomainObjects: number;
} {
  const buffers = new Set<AnySpanBuffer>();
  const contexts = new Set<Context>();
  const retained = new Set<object>();
  const views = new Set<ArrayBufferView>();
  const backingStores = new Set<ArrayBufferLike>();

  for (const outcome of outcomes) {
    for (const buffer of collectBuffers(outcome.root)) {
      buffers.add(buffer);
      for (const key of Object.getOwnPropertyNames(buffer)) {
        const value = Reflect.get(buffer, key);
        if (!ArrayBuffer.isView(value)) continue;
        views.add(value);
        backingStores.add(value.buffer);
      }
    }
    for (const context of outcome.contexts) contexts.add(context);
    for (const value of outcome.retained) retained.add(value);
  }

  let backingStoreBytes = 0;
  for (const backingStore of backingStores) backingStoreBytes += backingStore.byteLength;
  let typedArrayViewBytes = 0;
  for (const view of views) typedArrayViewBytes += view.byteLength;

  return {
    backingStores: backingStores.size,
    backingStoreBytes,
    typedArrayViews: views.size,
    typedArrayViewBytes,
    reachableDomainObjects: buffers.size + contexts.size + retained.size,
  };
}

function forceGarbageCollection(): void {
  const bun = Reflect.get(globalThis, 'Bun') as { gc?: (force?: boolean) => void } | undefined;
  const gc = Reflect.get(globalThis, 'gc');
  if (bun?.gc) bun.gc(true);
  else if (typeof gc === 'function') Reflect.apply(gc, globalThis, []);
}

async function measureEffectiveMemory(name: string, scenario: Scenario): Promise<void> {
  forceGarbageCollection();
  const before = process.memoryUsage();
  const outcomes: Outcome[] = [];
  for (let index = 0; index < MEMORY_BATCH_SIZE; index++) outcomes.push(await scenario.run());
  forceGarbageCollection();
  const after = process.memoryUsage();
  const storage = exactRetainedStorage(outcomes);
  const heapUsedDelta = after.heapUsed - before.heapUsed;
  const externalDelta = after.external - before.external;
  const arrayBuffersBefore = before.arrayBuffers ?? 0;
  const arrayBuffersAfter = after.arrayBuffers ?? 0;

  process.stderr.write(`effective-memory ${JSON.stringify({
    scenario: name,
    variant: scenario.label,
    retainedOutcomes: outcomes.length,
    processDeltaBytes: {
      heapUsed: heapUsedDelta,
      external: externalDelta,
      arrayBuffers: arrayBuffersAfter - arrayBuffersBefore,
      rss: after.rss - before.rss,
      effectiveHeapPlusExternal: heapUsedDelta + externalDelta,
    },
    exactRetained: storage,
    perOutcome: {
      effectiveHeapPlusExternalBytes: (heapUsedDelta + externalDelta) / outcomes.length,
      backingStoreBytes: storage.backingStoreBytes / outcomes.length,
      reachableDomainObjects: storage.reachableDomainObjects / outcomes.length,
    },
    components: {
      wasm: 'not-exercised',
      arrowLease: 'not-exercised',
      arrowTemporary: 'not-exercised',
      dictionary: 'not-exercised',
      cache: 'not-exercised',
    },
  })}\n`);
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
        return complete(traceRoot.tracer.bufferStrategy.createSpanBuffer(schema, traceRoot, benchmarkPlan(capacity).metadata, capacity));
      },
    },
  ];
}

function setupScenarios(capacity: number): Scenario[] {
  const setup = (kind: 'buffer' | 'logger' | 'tag' | 'result' | 'context'): Outcome => {
    const root = createRoot(capacity);
    writeSpanStart(root, 'root');
    const retained: object[] = [];
    const contexts: Context[] = [];
    if (kind === 'logger') retained.push(new SpanLoggerClass(createWriterState(root)));
    if (kind === 'tag') retained.push(new TagWriterClass(createWriterState(root)));
    if (kind === 'result') retained.push(new ResultWriterClass(createWriterState(root)));
    if (kind === 'context') {
      const context = new SpanContextClass(root, schema, benchmarkPlan(capacity).callsitePlan);
      retained.push(context.log, context.tag);
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
      run: () => complete((root) => createChildSpanBuffer(root, SpanBufferClass, benchmarkPlan(capacity).metadata, benchmarkPlan(capacity).metadata, capacity)),
    },
    {
      label: 'production strategy',
      run: () =>
        complete((root) =>
          root._traceRoot.tracer.bufferStrategy.createChildSpanBuffer(root, benchmarkPlan(capacity).metadata, benchmarkPlan(capacity).metadata, capacity, schema),
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
      await context.span0(0, 'child', context._newCtx0(), benchmarkPlan(capacity).callsitePlan, (child) => {
        contexts.push(child as Context);
        const result = child.ok(1);
        retained.push(result);
        return result;
      });
    } else {
      await context.span0(
        0,
        'child-1',
        context._newCtx0(),
        benchmarkPlan(capacity).callsitePlan,
        async (child1) => {
          const directChild1 = child1 as Context;
          contexts.push(directChild1);
          await directChild1.span0(
            0,
            'child-2',
            directChild1._newCtx0(),
            benchmarkPlan(capacity).callsitePlan,
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
        },
      );
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
      const childPlan = benchmarkPlan(capacity, hint).callsitePlan;
      await context.span0(0, 'child', childPlan.newCtx0(context), childPlan, (child) => {
        contexts.push(child as Context);
        const result = child.ok(1);
        retained.push(result);
        return result;
      });
      return finalizeFixture(root, contexts, retained);
    },
  }));
}

async function registerScenario(name: string, scenarios: readonly Scenario[]): Promise<void> {
  const checksums: string[] = [];
  for (const scenario of scenarios) {
    const outcome = await scenario.run();
    if (!outcome?.root) throw new Error(`${name}: ${scenario.label} produced no root buffer`);
    checksums.push(lifecycleChecksum(outcome));
  }
  const expected = checksums[0];
  for (let index = 1; index < checksums.length; index++) {
    if (checksums[index] !== expected) {
      throw new Error(`${name}: lifecycle checksum mismatch for ${scenarios[index]?.label}`);
    }
  }

  if (process.argv.includes('--memory')) {
    for (const scenario of scenarios) await measureEffectiveMemory(name, scenario);
  }

  summary(() => {
    group(name, () => {
      scenarios.forEach((scenario, index) => {
        bench(scenario.label, scenario.run).baseline(index === 0);
      });
    });
  });
}

for (const capacity of CAPACITY_FILTER) {
  if (SELECTED_SCENARIO === undefined || SELECTED_SCENARIO === 'root') {
    await registerScenario(`span creation / root / capacity ${capacity}`, rootCreationScenarios(capacity));
  }
  if (SELECTED_SCENARIO === undefined || SELECTED_SCENARIO === 'setup') {
    await registerScenario(`span creation / setup / capacity ${capacity}`, setupScenarios(capacity));
  }
  if (SELECTED_SCENARIO === undefined || SELECTED_SCENARIO === 'child') {
    await registerScenario(`span creation / child / capacity ${capacity}`, childCreationScenarios(capacity));
  }
  if (SELECTED_SCENARIO === undefined || SELECTED_SCENARIO === 'overflow') {
    await registerScenario(`span creation / overflow / capacity ${capacity}`, overflowCreationScenarios(capacity));
  }
  if (SELECTED_SCENARIO === undefined || SELECTED_SCENARIO === 'context') {
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
  }
  if (SELECTED_SCENARIO === undefined || SELECTED_SCENARIO === 'capability') {
    await registerScenario(`span creation / capability mask / capacity ${capacity}`, capabilityScenarios(capacity));
  }
}

const format = process.argv.includes('--markdown') ? 'markdown' : process.argv.includes('--json') ? 'json' : undefined;
const filterArgument = process.argv.find((argument) => argument.startsWith('--filter='));
const filter = filterArgument === undefined ? undefined : new RegExp(filterArgument.slice('--filter='.length));

if (format === 'json') await run({ format: { json: { samples: true } }, filter });
else if (format) await run({ format, filter });
else await run({ filter });
