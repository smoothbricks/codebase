import { cleanupDebug } from '../cleanupDiagnostics.js';
import type { OpContextBinding } from '../opContext/types.js';
import { Tracer, type TracerOptions } from '../tracer.js';
import type { SpanBuffer } from '../types.js';

type MaybeClose = { close?: () => void | Promise<void> };

function hasClose(value: unknown): value is MaybeClose {
  return typeof value === 'object' && value !== null && typeof Reflect.get(value, 'close') === 'function';
}

export interface CompositeTracerOptions<B extends OpContextBinding = OpContextBinding>
  extends TracerOptions<B['logBinding']['logSchema']> {
  delegates: Tracer<B>[];
}

/**
 * Composite tracer that fans lifecycle hooks out to delegate tracers.
 *
 * Useful when you want multiple outputs from a single trace run,
 * for example Stdio + SQLite.
 */
export class CompositeTracer<B extends OpContextBinding = OpContextBinding> extends Tracer<B> {
  private readonly delegates: Tracer<B>[];

  constructor(binding: B, options: CompositeTracerOptions<B>) {
    super(binding, options);
    this.delegates = options.delegates;
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
      cleanupDebug('compositeTracer.flush:delegate:start', { tracer: tracer.constructor.name });
      await tracer.flush();
      cleanupDebug('compositeTracer.flush:delegate:end', { tracer: tracer.constructor.name });
    }
  }

  async close(): Promise<void> {
    for (const tracer of this.delegates) {
      if (hasClose(tracer)) {
        const { close } = tracer;
        if (close) {
          cleanupDebug('compositeTracer.close:delegate:start', { tracer: tracer.constructor.name });
          await close.call(tracer);
          cleanupDebug('compositeTracer.close:delegate:end', { tracer: tracer.constructor.name });
        }
      }
    }
  }
}
