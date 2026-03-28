/**
 * TraceQuery - Post-run query API for the SQLite trace database.
 *
 * Queries a single flat `spans` table where each row is a buffer entry.
 * Status and duration are derived from entry_type and timestamps via self-joins.
 * Tree structure uses span_id / parent_span_id (no depth column).
 *
 * Platform-agnostic: accepts a SyncSQLiteDatabase instance from either
 * bun:sqlite or better-sqlite3.
 *
 * @example
 * ```typescript
 * import { Database } from 'bun:sqlite';
 * import { TraceQuery } from '@smoothbricks/lmao/testing';
 *
 * const query = new TraceQuery(new Database('.trace-results.db'));
 * console.log(query.failures());
 * console.log(query.slowest(undefined, 10));
 * query.close();
 * ```
 *
 * @module trace-query
 */

import typia from 'typia';
import type { SyncSQLiteDatabase } from '../sqlite/sqlite-db.js';

export interface TraceQueryResult {
  spanName: string;
  status: 'ok' | 'err' | 'exception' | 'running';
  errorCode?: string;
  durationNs?: number;
  describe?: string;
  parentSpanId?: number;
}

type TraceRun = { traceId: string; spanName: string; startedAt: number };
type LatestTraceRun = { traceId: string };

type TraceRunRow = {
  readonly traceId: string;
  readonly spanName: string;
  readonly startedAt: number;
};

type LatestTraceRunRow = {
  readonly traceId: string;
};

type RootSpanRow = {
  readonly span_id: number;
};

type TraceQueryResultRow = {
  readonly spanName: string;
  readonly status: 'ok' | 'err' | 'exception' | 'running';
  readonly durationNs: number | null;
  readonly describe: string | null;
  readonly parentSpanId: number;
};

let validateTraceRunRow: ((input: unknown) => typia.IValidation<TraceRunRow>) | undefined;
let validateLatestTraceRunRow: ((input: unknown) => typia.IValidation<LatestTraceRunRow>) | undefined;
let validateRootSpanRow: ((input: unknown) => typia.IValidation<RootSpanRow>) | undefined;
let validateTraceQueryResultRow: ((input: unknown) => typia.IValidation<TraceQueryResultRow>) | undefined;

function getValidateTraceRunRow(): (input: unknown) => typia.IValidation<TraceRunRow> {
  validateTraceRunRow ??= typia.createValidateEquals<TraceRunRow>();
  return validateTraceRunRow;
}

function getValidateLatestTraceRunRow(): (input: unknown) => typia.IValidation<LatestTraceRunRow> {
  validateLatestTraceRunRow ??= typia.createValidateEquals<LatestTraceRunRow>();
  return validateLatestTraceRunRow;
}

function getValidateRootSpanRow(): (input: unknown) => typia.IValidation<RootSpanRow> {
  validateRootSpanRow ??= typia.createValidateEquals<RootSpanRow>();
  return validateRootSpanRow;
}

function getValidateTraceQueryResultRow(): (input: unknown) => typia.IValidation<TraceQueryResultRow> {
  validateTraceQueryResultRow ??= typia.createValidateEquals<TraceQueryResultRow>();
  return validateTraceQueryResultRow;
}

function describeSqliteBoundaryError(
  boundary: string,
  errors: readonly { path: string; expected: string }[],
  rowIndex?: number,
): string {
  const firstError = errors[0];
  const rowDetail = rowIndex === undefined ? '' : ` row ${rowIndex}`;
  if (!firstError) {
    return `${boundary} returned an invalid SQLite${rowDetail} result`;
  }

  return `${boundary} returned an invalid SQLite${rowDetail} result at ${firstError.path}: expected ${firstError.expected}`;
}

function parseOptionalSqliteRow<T>(
  boundary: string,
  row: unknown,
  validate: (input: unknown) => typia.IValidation<T>,
): T | undefined {
  if (row == null) {
    return undefined;
  }

  const validation = validate(row);
  if (!validation.success) {
    throw new Error(describeSqliteBoundaryError(boundary, validation.errors));
  }

  return validation.data;
}

function parseSqliteRows<T>(
  boundary: string,
  rows: unknown[],
  validate: (input: unknown) => typia.IValidation<T>,
): T[] {
  return rows.map((row, rowIndex) => {
    const validation = validate(row);
    if (!validation.success) {
      throw new Error(describeSqliteBoundaryError(boundary, validation.errors, rowIndex));
    }

    return validation.data;
  });
}

function toTraceQueryResult(row: TraceQueryResultRow): TraceQueryResult {
  return {
    spanName: row.spanName,
    status: row.status,
    durationNs: row.durationNs ?? undefined,
    describe: row.describe ?? undefined,
    parentSpanId: row.parentSpanId,
  };
}

export class TraceQuery {
  constructor(private db: SyncSQLiteDatabase) {}

  /** Get all trace runs (root spans), most recent first */
  runs(): TraceRun[] {
    return parseSqliteRows(
      'TraceQuery.runs',
      this.db
        .prepare(
          `SELECT trace_id AS traceId, message AS spanName, timestamp_ns AS startedAt
         FROM spans
         WHERE parent_span_id = 0 AND row_index = 0
         ORDER BY timestamp_ns DESC`,
        )
        .all(),
      getValidateTraceRunRow(),
    );
  }

  /** Get the most recent trace_id (= run_id) */
  latestRun(): LatestTraceRun | undefined {
    return parseOptionalSqliteRow(
      'TraceQuery.latestRun',
      this.db
        .prepare(
          `SELECT trace_id AS traceId
         FROM spans
         WHERE parent_span_id = 0 AND row_index = 0
         ORDER BY timestamp_ns DESC LIMIT 1`,
        )
        .get(),
      getValidateLatestTraceRunRow(),
    );
  }

  /** Get all failed tests in a run (defaults to latest run) */
  failures(traceId?: string): TraceQueryResult[] {
    const id = traceId ?? this.latestRun()?.traceId;
    if (!id) return [];

    // Find the root span_id for this trace
    const root = parseOptionalSqliteRow(
      'TraceQuery.failures root span',
      this.db.prepare('SELECT span_id FROM spans WHERE trace_id = ? AND parent_span_id = 0 AND row_index = 0').get(id),
      getValidateRootSpanRow(),
    );
    if (!root) return [];

    // Self-join: row_index=0 for name, row_index=1 for status/duration
    return parseSqliteRows(
      'TraceQuery.failures rows',
      this.db
        .prepare(
          `SELECT
           s0.message AS spanName,
           CASE WHEN s1.entry_type = 2 THEN 'ok'
                WHEN s1.entry_type = 3 THEN 'err'
                WHEN s1.entry_type = 4 THEN 'exception'
                ELSE 'running' END AS status,
           s1.timestamp_ns - s0.timestamp_ns AS durationNs,
           s0.describe,
           s0.parent_span_id AS parentSpanId
         FROM spans s0
         LEFT JOIN spans s1 ON s1.trace_id = s0.trace_id AND s1.span_id = s0.span_id AND s1.row_index = 1
          WHERE s0.trace_id = ? AND s0.parent_span_id = ? AND s0.row_index = 0
            AND s1.entry_type IN (3, 4)
          ORDER BY s0.timestamp_ns ASC`,
        )
        .all(id, root.span_id),
      getValidateTraceQueryResultRow(),
    ).map(toTraceQueryResult);
  }

  /** Get slowest tests in a run (defaults to latest run) */
  slowest(traceId?: string, limit = 20): TraceQueryResult[] {
    const id = traceId ?? this.latestRun()?.traceId;
    if (!id) return [];

    // Find the root span_id for this trace
    const root = parseOptionalSqliteRow(
      'TraceQuery.slowest root span',
      this.db.prepare('SELECT span_id FROM spans WHERE trace_id = ? AND parent_span_id = 0 AND row_index = 0').get(id),
      getValidateRootSpanRow(),
    );
    if (!root) return [];

    return parseSqliteRows(
      'TraceQuery.slowest rows',
      this.db
        .prepare(
          `SELECT
           s0.message AS spanName,
           CASE WHEN s1.entry_type = 2 THEN 'ok'
                WHEN s1.entry_type = 3 THEN 'err'
                WHEN s1.entry_type = 4 THEN 'exception'
                ELSE 'running' END AS status,
           s1.timestamp_ns - s0.timestamp_ns AS durationNs,
           s0.describe,
           s0.parent_span_id AS parentSpanId
         FROM spans s0
         LEFT JOIN spans s1 ON s1.trace_id = s0.trace_id AND s1.span_id = s0.span_id AND s1.row_index = 1
         WHERE s0.trace_id = ? AND s0.parent_span_id = ? AND s0.row_index = 0
            AND s1.entry_type IS NOT NULL
          ORDER BY durationNs DESC
          LIMIT ?`,
        )
        .all(id, root.span_id, limit),
      getValidateTraceQueryResultRow(),
    ).map(toTraceQueryResult);
  }

  /** Find spans by name pattern (SQL LIKE, defaults to latest run) */
  findSpans(pattern: string, traceId?: string): TraceQueryResult[] {
    const id = traceId ?? this.latestRun()?.traceId;
    if (!id) return [];

    return parseSqliteRows(
      'TraceQuery.findSpans',
      this.db
        .prepare(
          `SELECT
           s0.message AS spanName,
           CASE WHEN s1.entry_type = 2 THEN 'ok'
                WHEN s1.entry_type = 3 THEN 'err'
                WHEN s1.entry_type = 4 THEN 'exception'
                ELSE 'running' END AS status,
           s1.timestamp_ns - s0.timestamp_ns AS durationNs,
           s0.describe,
           s0.parent_span_id AS parentSpanId
         FROM spans s0
          LEFT JOIN spans s1 ON s1.trace_id = s0.trace_id AND s1.span_id = s0.span_id AND s1.row_index = 1
          WHERE s0.trace_id = ? AND s0.row_index = 0 AND s0.message LIKE ?
          ORDER BY s0.timestamp_ns ASC`,
        )
        .all(id, pattern),
      getValidateTraceQueryResultRow(),
    ).map(toTraceQueryResult);
  }

  /** Get full span tree for a specific test (all descendants, defaults to latest run) */
  testTree(testName: string, traceId?: string): TraceQueryResult[] {
    const id = traceId ?? this.latestRun()?.traceId;
    if (!id) return [];

    // Recursive CTE on span_id/parent_span_id
    return parseSqliteRows(
      'TraceQuery.testTree',
      this.db
        .prepare(
          `WITH RECURSIVE tree AS (
           -- Anchor: find the span by name
           SELECT span_id
           FROM spans
           WHERE trace_id = ? AND row_index = 0 AND message = ?
           LIMIT 1

           UNION ALL

           -- Recurse: find children by parent_span_id
           SELECT s.span_id
           FROM spans s
           INNER JOIN tree t ON s.parent_span_id = t.span_id
           WHERE s.trace_id = ? AND s.row_index = 0
         )
         SELECT
           s0.message AS spanName,
           CASE WHEN s1.entry_type = 2 THEN 'ok'
                WHEN s1.entry_type = 3 THEN 'err'
                WHEN s1.entry_type = 4 THEN 'exception'
                ELSE 'running' END AS status,
           s1.timestamp_ns - s0.timestamp_ns AS durationNs,
           s0.describe,
           s0.parent_span_id AS parentSpanId
         FROM tree
          JOIN spans s0 ON s0.trace_id = ? AND s0.span_id = tree.span_id AND s0.row_index = 0
          LEFT JOIN spans s1 ON s1.trace_id = s0.trace_id AND s1.span_id = s0.span_id AND s1.row_index = 1
          ORDER BY s0.timestamp_ns ASC`,
        )
        .all(id, testName, id, id),
      getValidateTraceQueryResultRow(),
    ).map(toTraceQueryResult);
  }

  close(): void {
    this.db.close();
  }
}
