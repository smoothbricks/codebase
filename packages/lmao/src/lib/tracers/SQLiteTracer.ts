import { cleanupDebug } from '../cleanupDiagnostics.js';
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
  /**
   * Write the start row of the root span and of each of its direct children when it starts, and a direct child's
   * completion row when it ends, instead of only when the trace ends. A test run sets it: each test is a direct child
   * of the run's root, so a run that is killed has left which tests finished and which one was running (a start row
   * with no completion row). Deeper spans and log rows are written when the trace ends, as without it.
   */
  liveChildRows?: boolean;
}

export interface SQLiteAsyncTracerOptions<
  T extends import('../schema/LogSchema.js').LogSchema = import('../schema/LogSchema.js').LogSchema,
> extends TracerOptions<T> {
  db: AsyncSQLiteDatabase | Promise<AsyncSQLiteDatabase>;
}

/**
 * Tracer that persists completed root traces to SQLite synchronously.
 */
//#region smoo/lmao!n/tracer-implementations.sqlite
export class SQLiteTracer<B extends OpContextBinding = OpContextBinding> extends Tracer<B> {
  private readonly writer: SQLiteTraceWriter;
  private readonly liveChildRows: boolean;
  /** Span id of each running trace's root, by trace id; held only under `liveChildRows`. */
  private readonly liveRoots = new Map<string, number>();

  constructor(binding: B, options: SQLiteTracerOptions<B['logBinding']['logSchema']>) {
    super(binding, options);
    this.writer = new SQLiteTraceWriter(options.db);
    this.liveChildRows = options.liveChildRows === true;
  }

  onTraceStart(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    if (!this.liveChildRows) return;
    this.liveRoots.set(rootBuffer.trace_id, rootBuffer.span_id);
    this.writer.writeRow(rootBuffer, 0);
  }

  onTraceEnd(rootBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    cleanupDebug('sqliteTracer.onTraceEnd:flush:start', { rows: rootBuffer._writeIndex });
    this.liveRoots.delete(rootBuffer.trace_id);
    this.writer.flush(rootBuffer);
    cleanupDebug('sqliteTracer.onTraceEnd:flush:end');
  }

  onSpanStart(childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    if (this.isLiveChild(childBuffer)) this.writer.writeRow(childBuffer, 0);
  }

  onSpanEnd(childBuffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    if (this.isLiveChild(childBuffer)) this.writer.writeRow(childBuffer, 1);
  }

  private isLiveChild(childBuffer: SpanBuffer<B['logBinding']['logSchema']>): boolean {
    return this.liveChildRows && this.liveRoots.get(childBuffer.trace_id) === childBuffer.parent_span_id;
  }

  onStatsWillResetFor(_buffer: SpanBuffer<B['logBinding']['logSchema']>): void {
    // No-op
  }

  async close(): Promise<void> {
    cleanupDebug('sqliteTracer.close:start');
    this.writer.close();
    cleanupDebug('sqliteTracer.close:end');
  }
}
//#endregion smoo/lmao!n/tracer-implementations.sqlite

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
    cleanupDebug('sqliteAsyncTracer.flush:start');
    await this.pendingWrites;
    if (this.pendingErrors.length > 0) {
      const errors = this.pendingErrors;
      this.pendingErrors = [];
      if (errors.length === 1) {
        throw errors[0];
      }
      throw new AggregateError(errors, `SQLiteAsyncTracer flush failed for ${errors.length} trace writes`);
    }
    cleanupDebug('sqliteAsyncTracer.flush:end');
  }

  async close(): Promise<void> {
    cleanupDebug('sqliteAsyncTracer.close:start');
    try {
      await this.flush();
    } finally {
      const writer = await this.writerPromise;
      await writer.close();
    }
    cleanupDebug('sqliteAsyncTracer.close:end');
  }
}
