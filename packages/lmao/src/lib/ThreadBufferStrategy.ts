/**
 * BufferStrategy backed by the shared per-thread ThreadSpanBuffer.
 *
 * One provider per strategy instance (one logical thread). Each schema gets its
 * own row store, so the schema's attribute order is the store's column order.
 * The provider is how the lane reaches the store — allocator.wasm, Bun FFI, or
 * a host runtime that embeds the store natively — and is chosen once, when the
 * strategy is built, never per call.
 */

import type { Table } from '@uwdata/flechette';
import type { BufferStrategy } from './bufferStrategy.js';
import { convertSpanTreeToArrowTable } from './convertToArrow.js';
import type { OpMetadata } from './opContext/opTypes.js';
import type { LogSchema } from './schema/LogSchema.js';
import type { SpanBufferConstructor } from './spanBuffer.js';
import type { SpanBufferStats } from './spanBufferStats.js';
import { getThreadId } from './threadId.js';
import type { ITraceRoot } from './traceRoot.js';
import type { AnySpanBuffer, SpanBuffer } from './types.js';
import { THREAD_SPAN_BUFFER_OK, type ThreadSpanBufferBinding } from './wasm/threadSpanBuffer.js';
import { createThreadSpanBufferRuntime, type ThreadSpanBufferRuntime } from './wasm/threadSpanBufferHost.js';
import {
  createThreadSpanView,
  isThreadSpanView,
  requireThreadSpanView,
  ThreadSpanCells,
  type ThreadSpanView,
} from './wasm/threadSpanView.js';

/** How a thread lane reaches its row stores. */
export interface ThreadSpanBufferProvider {
  /** A new row store for one schema, bound for writing. */
  createBinding(threadId: bigint, capacity: number, schema: LogSchema): ThreadSpanBufferBinding;
  /**
   * The JavaScript Arrow conversion of a view's store. A provider whose host
   * converts rows natively refuses: its rows are never read back into JS.
   */
  toArrowTable(view: ThreadSpanView): Table;
}

const statsBySchema = new WeakMap<LogSchema, SpanBufferStats>();

function statsFor(schema: LogSchema, capacity: number): SpanBufferStats {
  const existing = statsBySchema.get(schema);
  if (existing) return existing;
  const created: SpanBufferStats = { capacity, totalWrites: 0, spansCreated: 0 };
  statsBySchema.set(schema, created);
  return created;
}

export class ThreadBufferStrategy<
  T extends LogSchema = LogSchema,
  P extends ThreadSpanBufferProvider = ThreadSpanBufferProvider,
> implements BufferStrategy<T>
{
  readonly provider: P;
  readonly capacity: number;
  readonly threadId: bigint;
  private readonly cells = new WeakMap<LogSchema, ThreadSpanCells>();
  /**
   * Bindings reachable for `reset`. The WeakMap above is the lookup; this is
   * the iteration order, and it is what makes the row store releasable — a
   * store outlives every span written through it, so without an explicit
   * reset a long-lived thread grows its row store without bound.
   */
  private readonly liveBindings: ThreadSpanBufferBinding[] = [];

  private constructor(provider: P, capacity: number, threadId: bigint) {
    this.provider = provider;
    this.capacity = capacity;
    this.threadId = threadId;
  }

  /** A strategy over allocator.wasm's row stores. */
  static async create<TSchema extends LogSchema>(options?: {
    capacity?: number;
    threadId?: bigint;
    initialPages?: number;
    maxPages?: number;
    /** Pre-compiled allocator.wasm for bundled environments; see createThreadSpanBufferRuntime. */
    module?: WebAssembly.Module;
  }): Promise<ThreadBufferStrategy<TSchema, ThreadSpanBufferRuntime>> {
    const runtime = await createThreadSpanBufferRuntime({
      initialPages: options?.initialPages,
      maxPages: options?.maxPages,
      module: options?.module,
    });
    return ThreadBufferStrategy.fromProvider<TSchema, ThreadSpanBufferRuntime>(runtime, options);
  }

  /** A strategy over any provider's row stores. */
  static fromProvider<TSchema extends LogSchema, TProvider extends ThreadSpanBufferProvider>(
    provider: TProvider,
    options?: { capacity?: number; threadId?: bigint },
  ): ThreadBufferStrategy<TSchema, TProvider> {
    return new ThreadBufferStrategy<TSchema, TProvider>(
      provider,
      options?.capacity ?? 64,
      options?.threadId ?? getThreadId(),
    );
  }

  /** The cell writer of `schema`'s row store, creating the store on first use. */
  cellsFor(schema: LogSchema): ThreadSpanCells {
    const existing = this.cells.get(schema);
    if (existing) return existing;
    const binding = this.provider.createBinding(this.threadId, this.capacity, schema);
    const created = new ThreadSpanCells(binding);
    this.cells.set(schema, created);
    this.liveBindings.push(binding);
    return created;
  }

  createSpanBuffer(
    schema: T,
    traceRoot: ITraceRoot,
    opMetadata: OpMetadata,
    _capacity?: number,
    _plannedClass?: SpanBufferConstructor<T>,
  ): SpanBuffer<T> {
    const buffer = createThreadSpanView({
      provider: this.provider,
      cells: this.cellsFor(schema),
      schema,
      traceRoot,
      opMetadata,
      callsiteMetadata: opMetadata,
      stats: statsFor(schema, this.capacity),
    });
    traceRoot._topology.registerRoot(buffer);
    return buffer;
  }

  createChildSpanBuffer(
    parentBuffer: SpanBuffer<T>,
    callsiteMetadata: OpMetadata,
    opMetadata: OpMetadata,
    _capacity?: number,
    schema?: T,
    _plannedClass?: SpanBufferConstructor<T>,
  ): SpanBuffer<T> {
    const childSchema = schema ?? parentBuffer._logSchema;
    const child = createThreadSpanView({
      provider: this.provider,
      cells: this.cellsFor(childSchema),
      schema: childSchema,
      traceRoot: parentBuffer._traceRoot,
      opMetadata,
      callsiteMetadata,
      parent: parentBuffer,
      stats: statsFor(childSchema, this.capacity),
    });
    parentBuffer._traceRoot._topology.registerChild(parentBuffer, child);
    const parent = requireThreadSpanView(parentBuffer);
    if (isThreadSpanView(parent) && Object.keys(parent._scopeValues).length > 0) {
      requireThreadSpanView(child)._scopeValues = parent._scopeValues;
    }
    return child;
  }

  createOverflowBuffer(buffer: SpanBuffer<T>): SpanBuffer<T> {
    return buffer;
  }

  toArrowTable(buffer: AnySpanBuffer): Table {
    return convertSpanTreeToArrowTable(buffer);
  }

  releaseBuffer(buffer: AnySpanBuffer): void {
    buffer._traceRoot._topology.release();
  }

  /** Release every row and span on this thread, keeping interned vocabularies. */
  reset(): void {
    for (const binding of this.liveBindings) {
      if (binding.reset() !== THREAD_SPAN_BUFFER_OK) {
        throw new Error('thread_span_buffer_reset failed');
      }
    }
  }
}
