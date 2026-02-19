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

export class TraceQuery {
  constructor(private db: SyncSQLiteDatabase) {}

  /** Get all trace runs (root spans), most recent first */
  runs(): TraceRun[] {
    return this.db
      .prepare(
        `SELECT trace_id AS traceId, message AS spanName, timestamp_ns AS startedAt
         FROM spans
         WHERE parent_span_id = 0 AND row_index = 0
         ORDER BY timestamp_ns DESC`,
      )
      .all() as TraceRun[];
  }

  /** Get the most recent trace_id (= run_id) */
  latestRun(): LatestTraceRun | undefined {
    return this.db
      .prepare(
        `SELECT trace_id AS traceId
         FROM spans
         WHERE parent_span_id = 0 AND row_index = 0
         ORDER BY timestamp_ns DESC LIMIT 1`,
      )
      .get() as LatestTraceRun | undefined;
  }

  /** Get all failed tests in a run (defaults to latest run) */
  failures(traceId?: string): TraceQueryResult[] {
    const id = traceId ?? this.latestRun()?.traceId;
    if (!id) return [];

    // Find the root span_id for this trace
    const root = this.db
      .prepare('SELECT span_id FROM spans WHERE trace_id = ? AND parent_span_id = 0 AND row_index = 0')
      .get(id) as { span_id: number } | undefined;
    if (!root) return [];

    // Self-join: row_index=0 for name, row_index=1 for status/duration
    return this.db
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
      .all(id, root.span_id) as TraceQueryResult[];
  }

  /** Get slowest tests in a run (defaults to latest run) */
  slowest(traceId?: string, limit = 20): TraceQueryResult[] {
    const id = traceId ?? this.latestRun()?.traceId;
    if (!id) return [];

    // Find the root span_id for this trace
    const root = this.db
      .prepare('SELECT span_id FROM spans WHERE trace_id = ? AND parent_span_id = 0 AND row_index = 0')
      .get(id) as { span_id: number } | undefined;
    if (!root) return [];

    return this.db
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
      .all(id, root.span_id, limit) as TraceQueryResult[];
  }

  /** Find spans by name pattern (SQL LIKE, defaults to latest run) */
  findSpans(pattern: string, traceId?: string): TraceQueryResult[] {
    const id = traceId ?? this.latestRun()?.traceId;
    if (!id) return [];

    return this.db
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
      .all(id, pattern) as TraceQueryResult[];
  }

  /** Get full span tree for a specific test (all descendants, defaults to latest run) */
  testTree(testName: string, traceId?: string): TraceQueryResult[] {
    const id = traceId ?? this.latestRun()?.traceId;
    if (!id) return [];

    // Recursive CTE on span_id/parent_span_id
    return this.db
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
      .all(id, testName, id, id) as TraceQueryResult[];
  }

  close(): void {
    this.db.close();
  }
}
