import type { RemapDescriptor } from './logBinding.js';
import type { LogSchema } from './schema/LogSchema.js';
import type { AnySpanBuffer, SpanBuffer } from './types.js';

export const NO_NODE = 0xffff_ffff;
const INITIAL_CAPACITY = 8;
const MAX_NODE_COUNT = NO_NODE;

export type SpanTreeVisitor = (buffer: AnySpanBuffer, remapDescriptor: RemapDescriptor | undefined) => void;

/** Trace-owned O(1) span topology arena. Overflow segments share their logical node. */
export class TraceTopology {
  buffers: Array<AnySpanBuffer | undefined> = new Array(INITIAL_CAPACITY);
  firstChild = new Uint32Array(INITIAL_CAPACITY);
  lastChild = new Uint32Array(INITIAL_CAPACITY);
  nextSibling = new Uint32Array(INITIAL_CAPACITY);
  count = 0;
  root = NO_NODE;
  generation = 1;
  private activeLeases = 0;
  private releasePending = false;
  private pendingRelease: (() => void) | undefined;

  constructor() {
    this.firstChild.fill(NO_NODE);
    this.lastChild.fill(NO_NODE);
    this.nextSibling.fill(NO_NODE);
  }

  registerRoot(buffer: AnySpanBuffer): number {
    if (this.count !== 0 || this.root !== NO_NODE) throw new Error('Trace topology root is already registered');
    const index = this.register(buffer);
    if (index !== 0) throw new Error('Trace topology root must have index 0');
    this.root = index;
    return index;
  }

  registerChild(parent: AnySpanBuffer, child: AnySpanBuffer): number {
    this.assertLive(parent);
    const parentIndex = parent._nodeIndex;
    const childIndex = this.register(child);
    const previous = this.lastChild[parentIndex];
    if (previous === NO_NODE) {
      this.firstChild[parentIndex] = childIndex;
    } else {
      this.nextSibling[previous] = childIndex;
    }
    this.lastChild[parentIndex] = childIndex;
    return childIndex;
  }

  adoptOverflow(logicalBuffer: AnySpanBuffer, overflow: AnySpanBuffer): void {
    this.assertLive(logicalBuffer);
    overflow._nodeIndex = logicalBuffer._nodeIndex;
    overflow._topologyGeneration = this.generation;
  }

  assertLive(buffer: AnySpanBuffer): void {
    if (
      this.releasePending ||
      buffer._traceRoot._topology !== this ||
      buffer._topologyGeneration !== this.generation ||
      buffer._nodeIndex >= this.count ||
      this.buffers[buffer._nodeIndex] === undefined
    ) {
      throw new Error('Span buffer topology handle is stale');
    }
  }

  /** Pin this topology generation until the returned idempotent release runs. */
  acquireLease(): () => void {
    if (this.releasePending || this.count === 0 || this.root === NO_NODE) {
      throw new Error('Cannot lease a released span topology');
    }
    this.activeLeases++;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.activeLeases--;
      if (this.activeLeases === 0 && this.releasePending) this.finishRelease();
    };
  }

  get leaseCount(): number {
    return this.activeLeases;
  }

  release(finalizer?: () => void): void {
    if (this.releasePending) throw new Error('Span buffer topology release is already pending');
    if (this.activeLeases !== 0) {
      this.releasePending = true;
      this.pendingRelease = finalizer;
      return;
    }
    finalizer?.();
    this.clearReleasedTopology();
  }

  private finishRelease(): void {
    const finalizer = this.pendingRelease;
    this.pendingRelease = undefined;
    finalizer?.();
    this.clearReleasedTopology();
  }

  private clearReleasedTopology(): void {
    this.releasePending = false;
    for (let index = 0; index < this.count; index++) this.buffers[index] = undefined;
    this.firstChild.fill(NO_NODE);
    this.lastChild.fill(NO_NODE);
    this.nextSibling.fill(NO_NODE);
    this.count = 0;
    this.root = NO_NODE;
    this.generation++;
    if (this.generation === 0) this.generation = 1;
  }

  private register(buffer: AnySpanBuffer): number {
    if (this.count === MAX_NODE_COUNT) throw new RangeError('Trace topology node limit exceeded');
    if (this.count === this.buffers.length) this.grow();
    const index = this.count++;
    this.buffers[index] = buffer;
    buffer._nodeIndex = index;
    buffer._topologyGeneration = this.generation;
    return index;
  }

  private grow(): void {
    const current = this.buffers.length;
    const next = current * 2;
    if (!Number.isSafeInteger(next) || next > MAX_NODE_COUNT) throw new RangeError('Trace topology capacity overflow');

    this.buffers.length = next;
    const firstChild = new Uint32Array(next);
    const lastChild = new Uint32Array(next);
    const nextSibling = new Uint32Array(next);
    firstChild.fill(NO_NODE);
    lastChild.fill(NO_NODE);
    nextSibling.fill(NO_NODE);
    firstChild.set(this.firstChild);
    lastChild.set(this.lastChild);
    nextSibling.set(this.nextSibling);
    this.firstChild = firstChild;
    this.lastChild = lastChild;
    this.nextSibling = nextSibling;
  }
}

/** Allocation-free iterative preorder: logical node overflow chain, then children in insertion order. */
export function walkSpanTree(root: AnySpanBuffer, visitor: SpanTreeVisitor): void {
  const topology = root._traceRoot._topology;
  topology.assertLive(root);
  const subtreeRoot = root._nodeIndex;
  let nodeIndex = subtreeRoot;

  for (;;) {
    const logical = topology.buffers[nodeIndex];
    if (!logical) throw new Error('Trace topology references a released span buffer');
    let segment: AnySpanBuffer | undefined = logical;
    while (segment) {
      visitor(segment, logical._remapDescriptor);
      segment = segment._overflow;
    }

    const child = topology.firstChild[nodeIndex];
    if (child !== NO_NODE) {
      nodeIndex = child;
      continue;
    }

    for (;;) {
      if (nodeIndex === subtreeRoot) return;
      const sibling = topology.nextSibling[nodeIndex];
      if (sibling !== NO_NODE) {
        nodeIndex = sibling;
        break;
      }
      const current = topology.buffers[nodeIndex];
      if (!current?._parent) throw new Error('Trace topology parent link is missing');
      nodeIndex = current._parent._nodeIndex;
    }
  }
}
/** Allocation-free logical-tree depth-first walk with balanced enter/leave callbacks. */
export function walkLogicalSpanTree(
  root: AnySpanBuffer,
  enter: (buffer: AnySpanBuffer) => void,
  leave: (buffer: AnySpanBuffer) => void,
): void {
  const topology = root._traceRoot._topology;
  topology.assertLive(root);
  const subtreeRoot = root._nodeIndex;
  let nodeIndex = subtreeRoot;
  let logical = topology.buffers[nodeIndex];
  if (!logical) throw new Error('Trace topology references a released span buffer');
  enter(logical);

  for (;;) {
    const child = topology.firstChild[nodeIndex];
    if (child !== NO_NODE) {
      nodeIndex = child;
      logical = topology.buffers[nodeIndex];
      if (!logical) throw new Error('Trace topology references a released span buffer');
      enter(logical);
      continue;
    }

    for (;;) {
      logical = topology.buffers[nodeIndex];
      if (!logical) throw new Error('Trace topology references a released span buffer');
      leave(logical);
      if (nodeIndex === subtreeRoot) return;
      const sibling = topology.nextSibling[nodeIndex];
      if (sibling !== NO_NODE) {
        nodeIndex = sibling;
        logical = topology.buffers[nodeIndex];
        if (!logical) throw new Error('Trace topology references a released span buffer');
        enter(logical);
        break;
      }
      if (!logical._parent) throw new Error('Trace topology parent link is missing');
      nodeIndex = logical._parent._nodeIndex;
    }
  }
}


/** Iterate logical child buffers without materializing a child array. */
export function iterateSpanChildren<T extends LogSchema>(root: SpanBuffer<T>): Generator<SpanBuffer<T>>;
export function iterateSpanChildren(root: AnySpanBuffer): Generator<AnySpanBuffer>;
export function* iterateSpanChildren(root: AnySpanBuffer): Generator<AnySpanBuffer> {
  const topology = root._traceRoot._topology;
  topology.assertLive(root);
  let child = topology.firstChild[root._nodeIndex];
  while (child !== NO_NODE) {
    const buffer = topology.buffers[child];
    if (!buffer) throw new Error('Trace topology references a released span buffer');
    yield buffer;
    child = topology.nextSibling[child];
  }
}

/** Iterate physical buffers in canonical preorder without recursion or temporary arrays. */
export function iterateSpanTree<T extends LogSchema>(root: SpanBuffer<T>): Generator<SpanBuffer<T>>;
export function iterateSpanTree(root: AnySpanBuffer): Generator<AnySpanBuffer>;
export function* iterateSpanTree(root: AnySpanBuffer): Generator<AnySpanBuffer> {
  const topology = root._traceRoot._topology;
  topology.assertLive(root);
  const subtreeRoot = root._nodeIndex;
  let nodeIndex = subtreeRoot;

  for (;;) {
    const logical = topology.buffers[nodeIndex];
    if (!logical) throw new Error('Trace topology references a released span buffer');
    let segment: AnySpanBuffer | undefined = logical;
    while (segment) {
      yield segment;
      segment = segment._overflow;
    }

    const child = topology.firstChild[nodeIndex];
    if (child !== NO_NODE) {
      nodeIndex = child;
      continue;
    }

    for (;;) {
      if (nodeIndex === subtreeRoot) return;
      const sibling = topology.nextSibling[nodeIndex];
      if (sibling !== NO_NODE) {
        nodeIndex = sibling;
        break;
      }
      const current = topology.buffers[nodeIndex];
      if (!current?._parent) throw new Error('Trace topology parent link is missing');
      nodeIndex = current._parent._nodeIndex;
    }
  }
}
