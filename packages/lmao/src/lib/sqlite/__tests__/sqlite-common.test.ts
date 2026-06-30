import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import type { AnySpanBuffer } from '../../types.js';
import {
  buildInsertParams,
  buildInsertSql,
  parseSqliteTableInfoRows,
  SPANS_TABLE_INIT_SQL,
  type SpanSegment,
} from '../sqlite-common.js';

const opContext = defineOpContext({
  logSchema: defineLogSchema({
    scoped_text: S.category(),
    scoped_num: S.number(),
    mixed: S.category(),
  }),
});

async function createSegment(): Promise<{ buffer: AnySpanBuffer; segment: SpanSegment }> {
  const tracer = new TestTracer(opContext, createTestTracerOptions());

  await tracer.trace('sqlite-scope-fallback', async (ctx) => {
    ctx.setScope({
      scoped_text: 'scope-text',
      scoped_num: 7,
      mixed: 'scope-mixed',
    });

    // Direct row-0 write should override scope for row 0 only.
    ctx.tag.mixed('direct-mixed');
    return ctx.ok(null);
  });

  const buffer = tracer.rootBuffers[0] as AnySpanBuffer;
  const segment: SpanSegment = {
    buffer,
    traceId: buffer.trace_id,
    spanId: buffer.span_id,
    parentSpanId: buffer.parent_span_id,
    rowOffset: 0,
  };

  return { buffer, segment };
}

describe('sqlite-common scope fallback', () => {
  it('fills user columns from scope when row value is missing', async () => {
    const { segment } = await createSegment();

    const activeUserFields = ['scoped_text', 'scoped_num', 'mixed'];

    const row0 = buildInsertParams(segment, 0, activeUserFields);
    const row1 = buildInsertParams(segment, 1, activeUserFields);

    expect(row0.slice(7)).toEqual(['scope-text', 7, 'direct-mixed']);
    expect(row1.slice(7)).toEqual(['scope-text', 7, 'scope-mixed']);
  });
});

describe('parseSqliteTableInfoRows', () => {
  it('accepts bigint PRAGMA integers from SQLite adapters', () => {
    expect(
      parseSqliteTableInfoRows([
        {
          cid: 0n,
          name: 'trace_id',
          type: 'TEXT',
          notnull: 1n,
          dflt_value: null,
          pk: 1n,
        },
      ]),
    ).toEqual([
      {
        cid: 0,
        name: 'trace_id',
        type: 'TEXT',
        notnull: 1,
        dflt_value: null,
        pk: 1,
      },
    ]);
  });

  it('throws a descriptive error for invalid row shapes', () => {
    expect(() =>
      parseSqliteTableInfoRows([{ cid: 0, name: 'trace_id', type: 1, notnull: 1, dflt_value: null, pk: 1 }]),
    ).toThrow('PRAGMA table_info(spans) returned an invalid SQLite row 0: type expected string, got number');
  });
});

describe('buildInsertSql', () => {
  it('quotes user column identifiers so reserved words do not break the INSERT', () => {
    // `check` and `order` are SQLite reserved words; a LogSchema field may use
    // either (e.g. a `check` column). Unquoted, these produce
    // `near "check": syntax error` at flush time.
    const sql = buildInsertSql(['check', 'order', 'node']);
    expect(sql).toContain('"check"');
    expect(sql).toContain('"order"');
    expect(sql).toContain('"node"');
  });

  it('omits the user-column clause entirely when there are no active fields', () => {
    const sql = buildInsertSql([]);
    expect(sql).toContain(
      'INSERT INTO spans (trace_id, span_id, parent_span_id, row_index, entry_type, timestamp_ns, message)',
    );
  });

  it('prepares against a real SQLite spans table with a reserved-word column', () => {
    const db = new Database(':memory:');
    db.exec(SPANS_TABLE_INIT_SQL);
    db.exec('ALTER TABLE spans ADD COLUMN "check" TEXT');
    // The bug: db.prepare(buildInsertSql(['check'])) threw `near "check": syntax error`.
    const stmt = db.prepare(buildInsertSql(['check']));
    stmt.run('trace-1', 1, 0, 0, 0, 0, 'msg', 'pass');
    // .values() yields raw cell arrays (no object shape to assert / no cast).
    const rows = db.query('SELECT "check" FROM spans').values();
    expect(rows).toEqual([['pass']]);
    db.close();
  });
});
