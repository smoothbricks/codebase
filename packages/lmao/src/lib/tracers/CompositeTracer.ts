import type { OpContextBinding } from '../opContext/types.js';
import { Tracer, type TracerOptions } from '../tracer.js';
import type { SpanBuffer } from '../types.js';

type MaybeClose = { close?: () => void | Promise<void> };

export interface CompositeTracerOptions<
  T extends import('../schema/LogSchema.js').LogSchema = import('../schema/LogSchema.js').LogSchema,
> extends TracerOptions<T> {
  delegates: Tracer[];
}

/**
 * Composite tracer that fans lifecycle hooks out to delegate tracers.
 *
 * Useful when you want multiple outputs from a single trace run,
 * for example Stdio + SQLite.
 */
export class CompositeTracer<B extends OpContextBinding = OpContextBinding> extends Tracer<B> {
  private readonly delegates: Tracer<B>[];

  constructor(binding: B, options: CompositeTracerOptions<B['logBinding']['logSchema']>) {
    super(binding, options);
    this.delegates = options.delegates as Tracer<B>[];
  }

  onTraceStart(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    for (const tracer of this.delegates) {
      tracer.onTraceStart(rootBuffer);
    }
  }

  onTraceEnd(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    for (const tracer of this.delegates) {
      tracer.onTraceEnd(rootBuffer);
    }
  }

  onSpanStart(childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    for (const tracer of this.delegates) {
      tracer.onSpanStart(childBuffer);
    }
  }

  onSpanEnd(childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    for (const tracer of this.delegates) {
      tracer.onSpanEnd(childBuffer);
    }
  }

  onStatsWillResetFor(buffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    for (const tracer of this.delegates) {
      tracer.onStatsWillResetFor(buffer);
    }
  }

  override async flush(): Promise<void> {
    for (const tracer of this.delegates) {
      await tracer.flush();
    }
  }

  async close(): Promise<void> {
    for (const tracer of this.delegates) {
      const close = (tracer as MaybeClose).close;
      if (typeof close === 'function') {
        await close.call(tracer);
      }
    }
  }
}
