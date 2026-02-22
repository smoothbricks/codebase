import type { OpContextBinding } from '../opContext/types.js';
import { SQLiteAsyncTraceWriter } from '../sqlite/sqlite-async-writer.js';
import type { AsyncSQLiteDatabase, SyncSQLiteDatabase } from '../sqlite/sqlite-db.js';
import { SQLiteTraceWriter } from '../sqlite/sqlite-writer.js';
import { Tracer, type TracerOptions } from '../tracer.js';
import type { SpanBuffer } from '../types.js';

export interface SQLiteTracerOptions<
  T extends import('../schema/LogSchema.js').LogSchema = import('../schema/LogSchema.js').LogSchema,
> extends TracerOptions<T> {
  db: SyncSQLiteDatabase;
}

export interface SQLiteAsyncTracerOptions<
  T extends import('../schema/LogSchema.js').LogSchema = import('../schema/LogSchema.js').LogSchema,
> extends TracerOptions<T> {
  db: AsyncSQLiteDatabase | Promise<AsyncSQLiteDatabase>;
}

/**
 * Tracer that persists completed root traces to SQLite synchronously.
 */
export class SQLiteTracer<B extends OpContextBinding = OpContextBinding> extends Tracer<B> {
  private readonly writer: SQLiteTraceWriter;

  constructor(binding: B, options: SQLiteTracerOptions<B['logBinding']['logSchema']>) {
    super(binding, options);
    this.writer = new SQLiteTraceWriter(options.db);
  }

  onTraceStart(_rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  onTraceEnd(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    this.writer.flush(rootBuffer);
  }

  onSpanStart(_childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  onSpanEnd(_childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  onStatsWillResetFor(_buffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  async close(): Promise<void> {
    this.writer.close();
  }
}

/**
 * Tracer that persists completed root traces to SQLite asynchronously.
 *
 * onTraceEnd starts a background write pipeline; flush() awaits pipeline completion.
 */
export class SQLiteAsyncTracer<B extends OpContextBinding = OpContextBinding> extends Tracer<B> {
  private readonly writerPromise: Promise<SQLiteAsyncTraceWriter>;
  private pendingWrites: Promise<void> = Promise.resolve();
  private pendingErrors: unknown[] = [];

  constructor(binding: B, options: SQLiteAsyncTracerOptions<B['logBinding']['logSchema']>) {
    super(binding, options);
    this.writerPromise = Promise.resolve(options.db).then((db) => new SQLiteAsyncTraceWriter(db));
  }

  onTraceStart(_rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  onTraceEnd(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    this.pendingWrites = this.pendingWrites.then(async () => {
      try {
        const writer = await this.writerPromise;
        await writer.flush(rootBuffer);
      } catch (error) {
        this.pendingErrors.push(error);
      }
    });
  }

  onSpanStart(_childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  onSpanEnd(_childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  onStatsWillResetFor(_buffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  override async flush(): Promise<void> {
    await this.pendingWrites;
    if (this.pendingErrors.length > 0) {
      const errors = this.pendingErrors;
      this.pendingErrors = [];
      if (errors.length === 1) {
        throw errors[0];
      }
      throw new AggregateError(errors, `SQLiteAsyncTracer flush failed for ${errors.length} trace writes`);
    }
  }

  async close(): Promise<void> {
    await this.flush();
    const writer = await this.writerPromise;
    await writer.close();
  }
}
