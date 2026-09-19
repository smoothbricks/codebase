import { describe, expect, it } from 'bun:test';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { SQLiteAsyncTracer } from '../../tracers/SQLiteTracer.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { SQLiteAsyncTraceWriter } from '../sqlite-async-writer.js';
import { SPANS_TABLE_INIT_SQL } from '../sqlite-common.js';
import type { AsyncSQLiteDatabase, SyncSQLiteDatabase } from '../sqlite-db.js';
import { createNodeSQLiteDatabase } from '../sqlite-node.js';

const binding = defineOpContext({ logSchema: defineLogSchema({ label: S.category() }) });

/** Real SQLite driver with an optional atomic bulk capability for the differential oracle. */
function asyncDatabase(db: SyncSQLiteDatabase, batchSizes?: number[]): AsyncSQLiteDatabase {
  return {
    async exec(sql) {
      db.exec(sql);
    },
    prepare(sql) {
      const statement = db.prepare(sql);
      const single = {
        async run(...params: unknown[]) {
          statement.run(...params);
        },
        async all(...params: unknown[]) {
          return statement.all(...params);
        },
        async get(...params: unknown[]) {
          return statement.get(...params);
        },
      };
      if (!batchSizes) return single;
      return {
        ...single,
        async runMany(rows: readonly (readonly unknown[])[]) {
          batchSizes.push(rows.length);
          db.exec('SAVEPOINT writer_batch');
          try {
            for (const row of rows) statement.run(...row);
            db.exec('RELEASE writer_batch');
          } catch (error) {
            db.exec('ROLLBACK TO writer_batch; RELEASE writer_batch');
            throw error;
          }
        },
      };
    },
    async close() {
      db.close();
    },
  };
}

describe('SQLiteAsyncTraceWriter bulk persistence', () => {
  it('persists every span and overflow row identically to the single-statement driver', async () => {
    const tracer = new TestTracer(binding, createTestTracerOptions());
    await tracer.trace('bulk-root', async (ctx) => {
      for (let index = 0; index < 300; index++) {
        await ctx.span('child', (child) => {
          child.tag.label(`child-${index}`);
          const logRows = index === 0 ? child.buffer._capacity + 1 : 10;
          for (let row = 0; row < logRows; row++) child.log.info(`row-${row}`);
          return child.ok(undefined);
        });
      }
      return ctx.ok(undefined);
    });
    const root = tracer.rootBuffers[0];
    if (!root) throw new Error('test tracer did not capture its root');
    const singleDb = createNodeSQLiteDatabase(':memory:');
    const bulkDb = createNodeSQLiteDatabase(':memory:');
    const batchSizes: number[] = [];
    const single = new SQLiteAsyncTraceWriter(asyncDatabase(singleDb));
    const bulk = new SQLiteAsyncTraceWriter(asyncDatabase(bulkDb, batchSizes));
    try {
      await single.flush(root);
      await bulk.flush(root);
      // Node SQLite rejects int64 timestamps outside JS's safe-number range.
      // Compare their exact stored decimal representation, not a lossy Number.
      const sql = `SELECT trace_id, span_id, parent_span_id, row_index, entry_type,
        CAST(timestamp_ns AS TEXT) AS timestamp_ns, message, label
        FROM spans ORDER BY span_id, row_index`;
      expect(bulkDb.prepare(sql).all()).toEqual(singleDb.prepare(sql).all());
      expect(batchSizes[0]).toBe(256);
      expect(batchSizes.every((size) => size > 0 && size <= 256)).toBe(true);
      expect(bulkDb.prepare('SELECT count(*) AS count FROM spans WHERE row_index = 0').get()).toEqual({ count: 301 });
    } finally {
      await single.close();
      await bulk.close();
    }
  });

  it('close rejects persistence loss but still releases the underlying SQLite connection', async () => {
    const db = createNodeSQLiteDatabase(':memory:');
    db.exec(SPANS_TABLE_INIT_SQL);
    db.exec("CREATE TRIGGER reject_trace BEFORE INSERT ON spans BEGIN SELECT RAISE(ABORT, 'trace-row-refused'); END");
    const tracer = new SQLiteAsyncTracer(binding, { ...createTestTracerOptions(), db: asyncDatabase(db, []) });
    await tracer.trace('refused-root', (ctx) => ctx.ok(undefined));
    await expect(tracer.close()).rejects.toThrow('trace-row-refused');
    expect(() => db.prepare('SELECT count(*) FROM spans')).toThrow();
  });
});
