/**
 * Phase-2 jurisdiction arm: nested dynamic span tree with message reuse.
 *
 * 4-deep tree, 128 log rows, 8 distinct messages. Half go through
 * `_infoTemplate` (static vocabulary / `_messageIds`); half through
 * `log.info` (repeated dynamic literals). Cold arm: unique string per row.
 */
import { bench, group, run } from 'mitata';
import { defineOpContext } from '../src/lib/defineOpContext.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_SPAN,
} from '../src/lib/runtimeHint.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ThreadBufferStrategy } from '../src/lib/ThreadBufferStrategy.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';
import { registerBenchmarkVocabulary } from './vocabularyFixture.js';

const schema = defineLogSchema({ n: S.number() });
const context = defineOpContext({ logSchema: schema });
type Binding = typeof context;

const REUSED_MESSAGES = [
  'reduce-tick',
  'rete-fire',
  'decide-start',
  'op-enter',
  'user-event',
  'attr-write',
  'child-ok',
  'retry-wait',
] as const;
const VOCAB = registerBenchmarkVocabulary(REUSED_MESSAGES.slice(0, 4));
const CAPACITY = 64;
const HINT =
  RUNTIME_HINT_ANALYZED_VALID |
  RUNTIME_HINT_LOG |
  RUNTIME_HINT_RESULT |
  RUNTIME_HINT_SPAN |
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED |
  CAPACITY;

function writeWarmRows(span: { log: { info(message: string): { n(value: number): unknown } } }, start: number): void {
  const logger = Reflect.get(span, '_spanLogger') as {
    _infoTemplate(vocabularyIndex: number): { n(value: number): unknown };
  };
  for (let i = 0; i < 32; i++) {
    const slot = (start + i) & 7;
    if (slot < 4) {
      const dense = VOCAB[slot];
      if (dense === undefined) throw new Error(`missing vocab slot ${slot}`);
      logger._infoTemplate(dense).n(i);
    } else {
      const message = REUSED_MESSAGES[slot];
      if (message === undefined) throw new Error('reused message slot missing');
      span.log.info(message).n(i);
    }
  }
}

function nestedWarm(span: {
  span(name: string, fn: (child: never) => unknown): unknown;
  ok(value: number): unknown;
  log: { info(message: string): { n(value: number): unknown } };
}): unknown {
  writeWarmRows(span, 0);
  return span.span('l1', (a: typeof span) => {
    writeWarmRows(a, 1);
    return a.span('l2', (b: typeof span) => {
      writeWarmRows(b, 2);
      return b.span('l3', (c: typeof span) => {
        writeWarmRows(c, 3);
        return c.ok(1);
      });
    });
  });
}

let coldSeq = 0;
function writeColdRows(span: { log: { info(message: string): { n(value: number): unknown } } }): void {
  for (let i = 0; i < 32; i++) {
    span.log.info(`cold-${coldSeq++}`).n(i);
  }
}

function nestedCold(span: {
  span(name: string, fn: (child: never) => unknown): unknown;
  ok(value: number): unknown;
  log: { info(message: string): { n(value: number): unknown } };
}): unknown {
  writeColdRows(span);
  return span.span('l1', (a: typeof span) => {
    writeColdRows(a);
    return a.span('l2', (b: typeof span) => {
      writeColdRows(b);
      return b.span('l3', (c: typeof span) => {
        writeColdRows(c);
        return c.ok(1);
      });
    });
  });
}

const opOptions = {
  runtimeHint: HINT,
  localMessageDictionary: Array.from(VOCAB),
};

const warmOp = context.defineOp('phase2-warm', (ctx) => nestedWarm(ctx) ?? ctx.ok(1), undefined, opOptions);
const coldOp = context.defineOp('phase2-cold', (ctx) => nestedCold(ctx) ?? ctx.ok(1), undefined, opOptions);

async function makeTracers(): Promise<{
  js: TestTracer<Binding>;
  thread: TestTracer<Binding>;
  threadStrategy: ThreadBufferStrategy<Binding['logBinding']['logSchema']>;
}> {
  const js = new JsBufferStrategy<Binding['logBinding']['logSchema']>();
  const threadStrategy = await ThreadBufferStrategy.create<Binding['logBinding']['logSchema']>({
    capacity: CAPACITY,
  });
  return {
    js: new TestTracer(context, { bufferStrategy: js, createTraceRoot }),
    thread: new TestTracer(context, { bufferStrategy: threadStrategy, createTraceRoot }),
    threadStrategy,
  };
}

function wrapCounts(strategy: ThreadBufferStrategy<Binding['logBinding']['logSchema']>): Record<string, number> {
  const hits = { appendLog: 0, appendLogStatic: 0, appendLogDynamic: 0, intern: 0, writeAttr: 0 };
  const runtime = strategy.runtime as unknown as {
    createBinding: (...args: never[]) => {
      appendLog: (...a: never[]) => unknown;
      appendLogStatic: (...a: never[]) => unknown;
      appendLogDynamic: (...a: never[]) => unknown;
      intern: (...a: never[]) => unknown;
      writeAttr: (...a: never[]) => unknown;
    };
  };
  const original = runtime.createBinding.bind(runtime);
  runtime.createBinding = ((...args: never[]) => {
    const binding = original(...args);
    const wrap = (key: keyof typeof hits, fn: (...a: never[]) => unknown) =>
      ((...a: never[]) => {
        hits[key] += 1;
        return fn(...a);
      }) as typeof fn;
    binding.appendLog = wrap('appendLog', binding.appendLog.bind(binding));
    binding.appendLogStatic = wrap('appendLogStatic', binding.appendLogStatic.bind(binding));
    binding.appendLogDynamic = wrap('appendLogDynamic', binding.appendLogDynamic.bind(binding));
    binding.intern = wrap('intern', binding.intern.bind(binding));
    binding.writeAttr = wrap('writeAttr', binding.writeAttr.bind(binding));
    return binding;
  }) as typeof runtime.createBinding;
  return hits;
}

const { js: jsWarm, thread: threadWarm, threadStrategy } = await makeTracers();
const { js: jsCold, thread: threadCold } = await makeTracers();
const hits = wrapCounts(threadStrategy);

{
  const before = { ...threadStrategy.runtime.internStats };
  threadWarm.clear();
  threadWarm.trace_op(0, 'phase2-warm', {}, warmOp);
  const afterFirst = { ...threadStrategy.runtime.internStats };
  threadWarm.clear();
  threadWarm.trace_op(0, 'phase2-warm', {}, warmOp);
  const afterSecond = { ...threadStrategy.runtime.internStats };
  console.log('phase2 ABI counts after 1 warm tree', hits);
  console.log('internStats first tree', {
    hits: afterFirst.hits - before.hits,
    misses: afterFirst.misses - before.misses,
  });
  console.log('internStats second tree', {
    hits: afterSecond.hits - afterFirst.hits,
    misses: afterSecond.misses - afterFirst.misses,
  });
}

group('phase2-warm nested-reuse js-heap vs thread-buffer', () => {
  bench('js-heap', () => {
    jsWarm.clear();
    jsWarm.trace_op(0, 'phase2-warm', {}, warmOp);
  });
  bench('thread-buffer', () => {
    threadWarm.clear();
    threadWarm.trace_op(0, 'phase2-warm', {}, warmOp);
  });
});

group('phase2-cold unique-messages js-heap vs thread-buffer', () => {
  bench('js-heap', () => {
    jsCold.clear();
    jsCold.trace_op(0, 'phase2-cold', {}, coldOp);
  });
  bench('thread-buffer', () => {
    threadCold.clear();
    threadCold.trace_op(0, 'phase2-cold', {}, coldOp);
  });
});

await run({ colors: false });
