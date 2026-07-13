import { describe, expect, it, spyOn } from 'bun:test';
import { defineOpContext } from '../../defineOpContext.js';
import { JsBufferStrategy } from '../../JsBufferStrategy.js';
import { resolveMessage } from '../../resolveMessage.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { ENTRY_TYPE_SPAN_OK, ENTRY_TYPE_SPAN_START } from '../../schema/systemSchema.js';
import { createTraceRoot } from '../../traceRoot.node.js';
import { ArrayQueueTracer } from '../../tracers/ArrayQueueTracer.js';
import { StdioTracer } from '../../tracers/StdioTracer.js';
import { extractFacts } from '../extractFacts.js';
import { querySpan } from '../queryable-span.js';
import { extractFactsFor, findAllSpans, findSpan, spanNames } from '../span-query.js';
import { replayTraceToStdio } from '../stdio-replay.js';

const schema = defineLogSchema({});
const binding = defineOpContext({ logSchema: schema });

function createRuntimeRoot(name = 'root') {
  const tracer = new ArrayQueueTracer(binding, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  });
  tracer.trace(name, (ctx) => ctx.ok(null));
  const root = tracer.queue[0];
  if (!root) throw new Error(`Expected traced root '${name}'`);
  return { root, tracer };
}

function addCompletedChild(
  runtime: ReturnType<typeof createRuntimeRoot>,
  parent: ReturnType<typeof createRuntimeRoot>['root'],
  name: string,
) {
  const child = runtime.tracer.bufferStrategy.createChildSpanBuffer(
    parent,
    parent._opMetadata,
    parent._opMetadata,
    8,
  );
  child.message(0, name);
  {
    const entryTypes = child.entry_type;
    if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
    entryTypes[0] = ENTRY_TYPE_SPAN_START;
  };
  {
    const entryTypes = child.entry_type;
    if (entryTypes === undefined) throw new Error('Expected split entry-type lane');
    entryTypes[1] = ENTRY_TYPE_SPAN_OK;
  };
  child._writeIndex = 2;
  return child;
}

const factOptions = {
  includeScope: false,
  includeTags: false,
  includeLogs: false,
  includeFF: false,
  includeMetrics: false,
};

describe('iterative test-support span traversal', () => {
  it('preserves depth-first insertion order across facts, queries, and stdio replay', () => {
    const runtime = createRuntimeRoot();
    const first = addCompletedChild(runtime, runtime.root, 'first');
    addCompletedChild(runtime, first, 'target');
    addCompletedChild(runtime, first, 'nested-last');
    addCompletedChild(runtime, runtime.root, 'target');

    const expectedNames = ['first', 'target', 'nested-last', 'target'];
    expect(spanNames(runtime.root)).toEqual(expectedNames);
    expect(querySpan(runtime.root).names()).toEqual(expectedNames);
    expect(findAllSpans(runtime.root, 'target').map((span) => resolveMessage(span, 0))).toEqual([
      'target',
      'target',
    ]);
    expect(querySpan(runtime.root).findAll('target').map((span) => span.name)).toEqual(['target', 'target']);
    expect(findSpan(runtime.root, 'nested-last')).toBe(querySpan(runtime.root).find('nested-last')?.buffer);
    expect(extractFactsFor(runtime.root, 'first', factOptions)).toEqual(querySpan(first).facts(factOptions));

    expect(Array.from(extractFacts(runtime.root, factOptions))).toEqual([
      'span:root: started',
      'span:first: started',
      'span:target: started',
      'span:target: ok',
      'span:nested-last: started',
      'span:nested-last: ok',
      'span:first: ok',
      'span:target: started',
      'span:target: ok',
      'span:root: ok',
    ]);

    const replayEvents: string[] = [];
    const traceStart = spyOn(StdioTracer.prototype, 'onTraceStart').mockImplementation((buffer) => {
      replayEvents.push(`trace-start:${resolveMessage(buffer, 0)}`);
    });
    const spanStart = spyOn(StdioTracer.prototype, 'onSpanStart').mockImplementation((buffer) => {
      replayEvents.push(`span-start:${resolveMessage(buffer, 0)}`);
    });
    const spanEnd = spyOn(StdioTracer.prototype, 'onSpanEnd').mockImplementation((buffer) => {
      replayEvents.push(`span-end:${resolveMessage(buffer, 0)}`);
    });
    const traceEnd = spyOn(StdioTracer.prototype, 'onTraceEnd').mockImplementation((buffer) => {
      replayEvents.push(`trace-end:${resolveMessage(buffer, 0)}`);
    });

    try {
      replayTraceToStdio(binding, runtime.root);
    } finally {
      traceStart.mockRestore();
      spanStart.mockRestore();
      spanEnd.mockRestore();
      traceEnd.mockRestore();
    }

    expect(replayEvents).toEqual([
      'trace-start:root',
      'span-start:first',
      'span-start:target',
      'span-end:target',
      'span-start:nested-last',
      'span-end:nested-last',
      'span-end:first',
      'span-start:target',
      'span-end:target',
      'trace-end:root',
    ]);
  });

  it('handles a tree deeper than the JavaScript call stack in every public test-support traversal', () => {
    const runtime = createRuntimeRoot();
    const edgeCount = 20_000;
    let parent = runtime.root;

    for (let depth = 0; depth < edgeCount; depth++) {
      parent = addCompletedChild(runtime, parent, depth === edgeCount - 1 ? 'deepest' : 'level');
    }

    const names = spanNames(runtime.root);
    expect(names).toHaveLength(edgeCount);
    expect(names[0]).toBe('level');
    expect(names[edgeCount - 1]).toBe('deepest');
    expect(findSpan(runtime.root, 'deepest')).toBe(parent);
    expect(querySpan(runtime.root).find('deepest')?.buffer).toBe(parent);

    const facts = Array.from(extractFacts(runtime.root, factOptions));
    expect(facts).toHaveLength((edgeCount + 1) * 2);
    expect(facts[0]).toBe('span:root: started');
    expect(facts[edgeCount]).toBe('span:deepest: started');
    expect(facts[edgeCount + 1]).toBe('span:deepest: ok');
    expect(facts[facts.length - 1]).toBe('span:root: ok');

    let replayedStarts = 0;
    let replayedEnds = 0;
    const spanStart = spyOn(StdioTracer.prototype, 'onSpanStart').mockImplementation(() => {
      replayedStarts++;
    });
    const spanEnd = spyOn(StdioTracer.prototype, 'onSpanEnd').mockImplementation(() => {
      replayedEnds++;
    });
    const traceStart = spyOn(StdioTracer.prototype, 'onTraceStart').mockImplementation(() => {});
    const traceEnd = spyOn(StdioTracer.prototype, 'onTraceEnd').mockImplementation(() => {});

    try {
      replayTraceToStdio(binding, runtime.root);
    } finally {
      spanStart.mockRestore();
      spanEnd.mockRestore();
      traceStart.mockRestore();
      traceEnd.mockRestore();
    }

    expect(replayedStarts).toBe(edgeCount);
    expect(replayedEnds).toBe(edgeCount);
  });
});
