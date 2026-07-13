import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { createTraceRoot } from '../traceRoot.node.js';
import {
  NO_NODE,
  TraceTopology,
  iterateSpanChildren,
  iterateSpanTree,
  walkSpanTree,
} from '../traceTopology.js';
import { extractFacts } from '../testing/extractFacts.js';
import { querySpan } from '../testing/queryable-span.js';
import { ArrayQueueTracer } from '../tracers/ArrayQueueTracer.js';

const topologyContext = defineOpContext({ logSchema: defineLogSchema({}) });

function createTracer() {
  return new ArrayQueueTracer(topologyContext, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  });
}

function createRuntimeRoot(name = 'root') {
  const tracer = createTracer();
  tracer.trace(name, (ctx) => ctx.ok(null));
  const root = tracer.queue[0];
  if (!root) throw new Error(`Expected traced root '${name}'`);
  return { root, tracer };
}

function addChild(
  runtime: ReturnType<typeof createRuntimeRoot>,
  parent: ReturnType<typeof createRuntimeRoot>['root'],
) {
  return runtime.tracer.bufferStrategy.createChildSpanBuffer(
    parent,
    parent._opMetadata,
    parent._opMetadata,
    8,
  );
}

function collectWalk(root: Parameters<typeof walkSpanTree>[0]) {
  const visited = new Array<Parameters<typeof walkSpanTree>[0]>();
  walkSpanTree(root, (buffer) => visited.push(buffer));
  return visited;
}

function expectNoChildrenArray(buffer: Parameters<typeof walkSpanTree>[0]): void {
  expect(Object.hasOwn(buffer, '_children')).toBe(false);
  expect(Array.isArray(Reflect.get(buffer, '_children'))).toBe(false);
}

function requireColumn(table: ReturnType<typeof convertSpanTreeToArrowTable>, name: string) {
  const column = table.getChild(name);
  if (!column) throw new Error(`Expected Arrow column '${name}'`);
  return column;
}

function spanStartNames(table: ReturnType<typeof convertSpanTreeToArrowTable>): unknown[] {
  const entryTypes = requireColumn(table, 'entry_type');
  const messages = requireColumn(table, 'message');
  const names: unknown[] = [];
  for (let row = 0; row < table.numRows; row++) {
    if (entryTypes.get(row) === 'span-start') names.push(messages.get(row));
  }
  return names;
}

describe('trace topology arena properties', () => {
  it('registers a root, children, and grandchild in insertion order without per-span child arrays', () => {
    const runtime = createRuntimeRoot();
    const { root } = runtime;
    const firstChild = addChild(runtime, root);
    const secondChild = addChild(runtime, root);
    const grandchild = addChild(runtime, firstChild);
    const topology = root._traceRoot._topology;

    expect(topology).toBeInstanceOf(TraceTopology);
    expect(topology.root).toBe(0);
    expect(topology.count).toBe(4);
    expect([root._nodeIndex, firstChild._nodeIndex, secondChild._nodeIndex, grandchild._nodeIndex]).toEqual([
      0, 1, 2, 3,
    ]);
    expect(topology.buffers.slice(0, topology.count)).toEqual([root, firstChild, secondChild, grandchild]);

    expect(topology.firstChild[root._nodeIndex]).toBe(firstChild._nodeIndex);
    expect(topology.lastChild[root._nodeIndex]).toBe(secondChild._nodeIndex);
    expect(topology.nextSibling[firstChild._nodeIndex]).toBe(secondChild._nodeIndex);
    expect(topology.nextSibling[secondChild._nodeIndex]).toBe(NO_NODE);
    expect(topology.firstChild[firstChild._nodeIndex]).toBe(grandchild._nodeIndex);
    expect(topology.lastChild[firstChild._nodeIndex]).toBe(grandchild._nodeIndex);
    expect(topology.nextSibling[grandchild._nodeIndex]).toBe(NO_NODE);
    expect(topology.firstChild[secondChild._nodeIndex]).toBe(NO_NODE);
    expect(topology.lastChild[secondChild._nodeIndex]).toBe(NO_NODE);

    expect(Array.from(iterateSpanChildren(root))).toEqual([firstChild, secondChild]);
    expect(Array.from(iterateSpanChildren(firstChild))).toEqual([grandchild]);
    expect(collectWalk(root)).toEqual([root, firstChild, grandchild, secondChild]);
    expect(Array.from(iterateSpanTree(root))).toEqual([root, firstChild, grandchild, secondChild]);

    for (const buffer of [root, firstChild, secondChild, grandchild]) {
      topology.assertLive(buffer);
      expect(buffer._topologyGeneration).toBe(topology.generation);
      expectNoChildrenArray(buffer);
    }
  });

  it('grows every arena lane together and preserves arbitrary sibling insertion order', () => {
    fc.assert(
      fc.property(fc.integer({ min: 9, max: 384 }), (siblingCount) => {
        const runtime = createRuntimeRoot(`siblings-${siblingCount}`);
        const { root } = runtime;
        const siblings = [];
        for (let index = 0; index < siblingCount; index++) siblings.push(addChild(runtime, root));
        const topology = root._traceRoot._topology;

        expect(topology.count).toBe(siblingCount + 1);
        expect(topology.root).toBe(root._nodeIndex);
        expect(topology.buffers.length).toBeGreaterThanOrEqual(topology.count);
        expect(topology.buffers.length & (topology.buffers.length - 1)).toBe(0);
        expect(topology.firstChild).toHaveLength(topology.buffers.length);
        expect(topology.lastChild).toHaveLength(topology.buffers.length);
        expect(topology.nextSibling).toHaveLength(topology.buffers.length);
        expect(topology.firstChild[root._nodeIndex]).toBe(1);
        expect(topology.lastChild[root._nodeIndex]).toBe(siblingCount);

        for (let index = 0; index < siblings.length; index++) {
          const sibling = siblings[index];
          if (!sibling) throw new Error(`Missing sibling ${index}`);
          const nodeIndex = index + 1;
          expect(sibling._nodeIndex).toBe(nodeIndex);
          expect(topology.buffers[nodeIndex]).toBe(sibling);
          expect(topology.firstChild[nodeIndex]).toBe(NO_NODE);
          expect(topology.lastChild[nodeIndex]).toBe(NO_NODE);
          expect(topology.nextSibling[nodeIndex]).toBe(index + 1 < siblingCount ? nodeIndex + 1 : NO_NODE);
          expectNoChildrenArray(sibling);
        }

        expect(Array.from(iterateSpanChildren(root))).toEqual(siblings);
        expect(collectWalk(root)).toEqual([root, ...siblings]);
        expectNoChildrenArray(root);
        topology.release();
      }),
      { numRuns: 60 },
    );
  });

  it('walks a 20,000-edge chain iteratively without consuming the JavaScript call stack', () => {
    const runtime = createRuntimeRoot('deep-root');
    const { root } = runtime;
    const nodes = [root];
    let parent = root;
    const edgeCount = 20_000;

    for (let depth = 0; depth < edgeCount; depth++) {
      const child = addChild(runtime, parent);
      nodes.push(child);
      parent = child;
    }

    let visited = 0;
    walkSpanTree(root, (buffer) => {
      if (buffer !== nodes[visited]) throw new Error(`Preorder mismatch at depth ${visited}`);
      visited++;
    });
    expect(visited).toBe(edgeCount + 1);

    let iterated = 0;
    for (const buffer of iterateSpanTree(root)) {
      if (buffer !== nodes[iterated]) throw new Error(`Iterator preorder mismatch at depth ${iterated}`);
      iterated++;
    }
    expect(iterated).toBe(edgeCount + 1);

    const topology = root._traceRoot._topology;
    for (let depth = 0; depth < nodes.length; depth++) {
      const buffer = nodes[depth];
      if (!buffer) throw new Error(`Missing buffer at depth ${depth}`);
      expectNoChildrenArray(buffer);
      expect(topology.nextSibling[buffer._nodeIndex]).toBe(NO_NODE);
      const expectedChild = depth < edgeCount ? depth + 1 : NO_NODE;
      expect(topology.firstChild[buffer._nodeIndex]).toBe(expectedChild);
      expect(topology.lastChild[buffer._nodeIndex]).toBe(expectedChild);
    }
  });

  it('matches iterative model preorder and lane links for arbitrary trees', () => {
    fc.assert(
      fc.property(fc.array(fc.nat(), { maxLength: 192 }), (parentSeeds) => {
        const runtime = createRuntimeRoot('property-root');
        const { root } = runtime;
        const buffers = [root];
        const modelChildren: number[][] = [[]];

        for (let offset = 0; offset < parentSeeds.length; offset++) {
          const seed = parentSeeds[offset];
          if (seed === undefined) throw new Error(`Missing parent seed ${offset}`);
          const childNode = offset + 1;
          const parentNode = seed % childNode;
          const parentBuffer = buffers[parentNode];
          const siblings = modelChildren[parentNode];
          if (!parentBuffer || !siblings) throw new Error(`Missing generated parent ${parentNode}`);
          const child = addChild(runtime, parentBuffer);
          buffers.push(child);
          siblings.push(childNode);
          modelChildren.push([]);
        }

        const expectedPreorder: number[] = [];
        const pending = [0];
        while (pending.length > 0) {
          const node = pending.pop();
          if (node === undefined) throw new Error('Pending preorder node disappeared');
          expectedPreorder.push(node);
          const children = modelChildren[node];
          if (!children) throw new Error(`Missing model lane ${node}`);
          for (let index = children.length - 1; index >= 0; index--) {
            const child = children[index];
            if (child === undefined) throw new Error(`Missing modeled child ${index} of ${node}`);
            pending.push(child);
          }
        }

        const topology = root._traceRoot._topology;
        expect(topology.count).toBe(buffers.length);
        expect(collectWalk(root).map((buffer) => buffer._nodeIndex)).toEqual(expectedPreorder);
        expect(Array.from(iterateSpanTree(root), (buffer) => buffer._nodeIndex)).toEqual(expectedPreorder);

        for (let node = 0; node < buffers.length; node++) {
          const buffer = buffers[node];
          const children = modelChildren[node];
          if (!buffer || !children) throw new Error(`Missing generated node ${node}`);
          const first = children[0] ?? NO_NODE;
          const last = children[children.length - 1] ?? NO_NODE;
          expect(buffer._nodeIndex).toBe(node);
          expect(topology.buffers[node]).toBe(buffer);
          expect(topology.firstChild[node]).toBe(first);
          expect(topology.lastChild[node]).toBe(last);
          expect(Array.from(iterateSpanChildren(buffer), (child) => child._nodeIndex)).toEqual(children);
          expectNoChildrenArray(buffer);

          for (let position = 0; position < children.length; position++) {
            const child = children[position];
            if (child === undefined) throw new Error(`Missing child ${position} of ${node}`);
            expect(topology.nextSibling[child]).toBe(children[position + 1] ?? NO_NODE);
          }
        }

        topology.release();
      }),
      { numRuns: 100 },
    );
  });

  it('keeps every overflow chain contiguous on its logical node without arena registration', () => {
    const runtime = createRuntimeRoot('overflow-root');
    const { root } = runtime;
    const rootOverflow = runtime.tracer.bufferStrategy.createOverflowBuffer(root);
    const rootOverflowTail = runtime.tracer.bufferStrategy.createOverflowBuffer(rootOverflow);
    const firstChild = addChild(runtime, root);
    const secondChild = addChild(runtime, root);
    const grandchild = addChild(runtime, firstChild);
    const childOverflow = runtime.tracer.bufferStrategy.createOverflowBuffer(firstChild);
    const grandchildOverflow = runtime.tracer.bufferStrategy.createOverflowBuffer(grandchild);
    const topology = root._traceRoot._topology;

    expect(topology.count).toBe(4);
    expect(rootOverflow._nodeIndex).toBe(root._nodeIndex);
    expect(rootOverflowTail._nodeIndex).toBe(root._nodeIndex);
    expect(childOverflow._nodeIndex).toBe(firstChild._nodeIndex);
    expect(grandchildOverflow._nodeIndex).toBe(grandchild._nodeIndex);
    expect(topology.buffers[root._nodeIndex]).toBe(root);
    expect(topology.buffers[firstChild._nodeIndex]).toBe(firstChild);
    expect(topology.buffers[grandchild._nodeIndex]).toBe(grandchild);
    expect(topology.buffers.slice(0, topology.count)).toEqual([root, firstChild, secondChild, grandchild]);
    expect(Array.from(iterateSpanChildren(root))).toEqual([firstChild, secondChild]);
    expect(collectWalk(root)).toEqual([
      root,
      rootOverflow,
      rootOverflowTail,
      firstChild,
      childOverflow,
      grandchild,
      grandchildOverflow,
      secondChild,
    ]);

    for (const buffer of [rootOverflow, rootOverflowTail, childOverflow, grandchildOverflow]) {
      topology.assertLive(buffer);
      expect(buffer._topologyGeneration).toBe(topology.generation);
      expectNoChildrenArray(buffer);
    }
  });

  it('clears the arena on release, rejects every stale handle, and registers a fresh generation', () => {
    const runtime = createRuntimeRoot('release-root');
    const { root } = runtime;
    const child = addChild(runtime, root);
    const overflow = runtime.tracer.bufferStrategy.createOverflowBuffer(child);
    const topology = root._traceRoot._topology;
    const laneCapacity = topology.buffers.length;
    const releasedGeneration = topology.generation;

    topology.release();

    expect(topology.count).toBe(0);
    expect(topology.root).toBe(NO_NODE);
    expect(topology.generation).toBe(releasedGeneration + 1);
    expect(topology.buffers).toHaveLength(laneCapacity);
    for (let index = 0; index < laneCapacity; index++) {
      expect(topology.buffers[index]).toBeUndefined();
      expect(topology.firstChild[index]).toBe(NO_NODE);
      expect(topology.lastChild[index]).toBe(NO_NODE);
      expect(topology.nextSibling[index]).toBe(NO_NODE);
    }

    expect(() => topology.assertLive(root)).toThrow('stale');
    expect(() => topology.assertLive(child)).toThrow('stale');
    expect(() => topology.assertLive(overflow)).toThrow('stale');
    expect(() => topology.registerChild(root, child)).toThrow('stale');
    expect(() => topology.adoptOverflow(root, overflow)).toThrow('stale');
    expect(() => walkSpanTree(root, () => undefined)).toThrow('stale');
    expect(() => iterateSpanChildren(root).next()).toThrow('stale');
    expect(() => iterateSpanTree(root).next()).toThrow('stale');

    const freshRoot = runtime.tracer.bufferStrategy.createSpanBuffer(
      root._logSchema,
      root._traceRoot,
      root._opMetadata,
      8,
    );
    expect(freshRoot._nodeIndex).toBe(0);
    expect(freshRoot._topologyGeneration).toBe(releasedGeneration + 1);
    expect(topology.root).toBe(0);
    expect(topology.count).toBe(1);
    expect(topology.buffers[0]).toBe(freshRoot);
    topology.assertLive(freshRoot);

    const freshChild = addChild(runtime, freshRoot);
    expect(freshChild._nodeIndex).toBe(1);
    expect(topology.firstChild[0]).toBe(1);
    expect(topology.lastChild[0]).toBe(1);
    expect(topology.nextSibling[1]).toBe(NO_NODE);
    expect(() => topology.registerRoot(freshChild)).toThrow('already registered');
    expect(() => topology.assertLive(root)).toThrow('stale');
  });

  it('keeps Arrow, facts, and queryable spans in the real traced arena preorder', async () => {
    const tracer = createTracer();
    await tracer.trace('root', async (root) => {
      root.log.info('root-event');
      await root.span('child-a', async (child) => {
        child.log.info('child-a-event');
        await child.span('grandchild', (grandchild) => {
          grandchild.log.info('grandchild-event');
          return grandchild.ok(null);
        });
        return child.ok(null);
      });
      await root.span('child-b', (child) => {
        child.log.info('child-b-event');
        return child.ok(null);
      });
      return root.ok(null);
    });

    const root = tracer.queue[0];
    if (!root) throw new Error('Expected completed traced root');
    expect(collectWalk(root).map((buffer) => buffer._nodeIndex)).toEqual([0, 1, 2, 3]);
    for (const buffer of collectWalk(root)) expectNoChildrenArray(buffer);

    const table = convertSpanTreeToArrowTable(root);
    expect(spanStartNames(table)).toEqual(['root', 'child-a', 'grandchild', 'child-b']);

    const startedFacts = Array.from(extractFacts(root, { includeMetrics: false })).filter((fact) =>
      fact.endsWith(': started'),
    );
    expect(startedFacts).toEqual([
      'span:root: started',
      'span:child-a: started',
      'span:grandchild: started',
      'span:child-b: started',
    ]);

    const queryable = querySpan(root);
    expect(queryable.name).toBe('root');
    expect(queryable.children.map((child) => child.name)).toEqual(['child-a', 'child-b']);
    expect(queryable.names()).toEqual(['child-a', 'grandchild', 'child-b']);
    expect(queryable.find('grandchild')?.name).toBe('grandchild');
  });
});
