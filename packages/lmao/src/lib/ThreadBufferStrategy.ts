/**
 * BufferStrategy backed by the shared per-thread ThreadSpanBuffer ABI.
 *
 * One WASM runtime per strategy instance (one logical thread). Each schema gets
 * its own handle so the blob ordinals match the writers.
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
import { convertThreadViewToArrowTable } from './wasm/convertThreadBuffer.js';
import { THREAD_SPAN_BUFFER_OK, type ThreadSpanBufferBinding } from './wasm/threadSpanBuffer.js';
import { createThreadSpanBufferRuntime, type ThreadSpanBufferRuntime } from './wasm/threadSpanBufferHost.js';
import { createThreadSpanView, isThreadSpanView, requireThreadSpanView } from './wasm/threadSpanView.js';

const statsBySchema = new WeakMap<LogSchema, SpanBufferStats>();

function statsFor(schema: LogSchema, capacity: number): SpanBufferStats {
  const existing = statsBySchema.get(schema);
  if (existing) return existing;
  const created: SpanBufferStats = { capacity, totalWrites: 0, spansCreated: 0 };
  statsBySchema.set(schema, created);
  return created;
}

export class ThreadBufferStrategy<T extends LogSchema = LogSchema> implements BufferStrategy<T> {
  readonly physicalBackend = 'thread-buffer' as const;
  readonly runtime: ThreadSpanBufferRuntime;
  readonly capacity: number;
  readonly threadId: bigint;
  private readonly bindings = new WeakMap<LogSchema, ThreadSpanBufferBinding>();
  /**
   * Bindings reachable for `reset`. The WeakMap above is the lookup; this is
   * the iteration order, and it is what makes the row store releasable — a
   * handle outlives every span written through it, so without an explicit
   * reset a long-lived thread grows its row store without bound.
   */
  private readonly liveBindings: ThreadSpanBufferBinding[] = [];

  private constructor(runtime: ThreadSpanBufferRuntime, capacity: number, threadId: bigint) {
    this.runtime = runtime;
    this.capacity = capacity;
    this.threadId = threadId;
  }

  static async create<TSchema extends LogSchema>(options?: {
    capacity?: number;
    threadId?: bigint;
    initialPages?: number;
    maxPages?: number;
  }): Promise<ThreadBufferStrategy<TSchema>> {
    const runtime = await createThreadSpanBufferRuntime({
      initialPages: options?.initialPages,
      maxPages: options?.maxPages,
    });
    return new ThreadBufferStrategy<TSchema>(runtime, options?.capacity ?? 64, options?.threadId ?? getThreadId());
  }

  bindingFor(schema: LogSchema): ThreadSpanBufferBinding {
    const existing = this.bindings.get(schema);
    if (existing) return existing;
    const created = this.runtime.createBinding(this.threadId, this.capacity, schema);
    this.bindings.set(schema, created);
    this.liveBindings.push(created);
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
      runtime: this.runtime,
      binding: this.bindingFor(schema),
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
      runtime: this.runtime,
      binding: this.bindingFor(childSchema),
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
    if (isThreadSpanView(buffer)) return convertThreadViewToArrowTable(buffer);
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
