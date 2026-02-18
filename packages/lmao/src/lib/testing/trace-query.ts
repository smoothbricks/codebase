/**
 * TraceQuery - Post-run query API for the SQLite trace database.
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

import type { SyncSQLiteDatabase } from './sqlite-db.js';

export interface TraceQueryResult {
  spanName: string;
  status: 'ok' | 'err' | 'exception' | 'running';
  errorCode?: string;
  durationNs?: number;
  depth: number;
  parentSpanName?: string;
}

interface RunRecord {
  runId: string;
  startedAt: number;
  completedAt?: number;
  testCount?: number;
  passCount?: number;
  failCount?: number;
}

export class TraceQuery {
  constructor(private db: SyncSQLiteDatabase) {}

  /** Get all runs, most recent first */
  runs(): RunRecord[] {
    return this.db
      .prepare(
        `SELECT run_id as runId, started_at as startedAt, completed_at as completedAt,
                test_count as testCount, pass_count as passCount, fail_count as failCount
         FROM runs ORDER BY started_at DESC`,
      )
      .all() as RunRecord[];
  }

  /** Get the most recent run */
  latestRun(): { runId: string } | undefined {
    return this.db.prepare('SELECT run_id as runId FROM runs ORDER BY started_at DESC LIMIT 1').get() as
      | { runId: string }
      | undefined;
  }

  /** Get all failed tests in a run (defaults to latest run) */
  failures(runId?: string): TraceQueryResult[] {
    const id = runId ?? this.latestRun()?.runId;
    if (!id) return [];

    return this.db
      .prepare(
        `SELECT span_name as spanName, status, error_code as errorCode,
                duration_ns as durationNs, depth, parent_span_name as parentSpanName
         FROM spans
         WHERE run_id = ? AND status IN ('err', 'exception')
         ORDER BY started_at ASC`,
      )
      .all(id) as TraceQueryResult[];
  }

  /** Get slowest tests in a run (defaults to latest run) */
  slowest(runId?: string, limit = 20): TraceQueryResult[] {
    const id = runId ?? this.latestRun()?.runId;
    if (!id) return [];

    return this.db
      .prepare(
        `SELECT span_name as spanName, status, error_code as errorCode,
                duration_ns as durationNs, depth, parent_span_name as parentSpanName
         FROM spans
         WHERE run_id = ? AND duration_ns IS NOT NULL
         ORDER BY duration_ns DESC
         LIMIT ?`,
      )
      .all(id, limit) as TraceQueryResult[];
  }

  /** Find spans by name pattern (SQL LIKE, defaults to latest run) */
  findSpans(pattern: string, runId?: string): TraceQueryResult[] {
    const id = runId ?? this.latestRun()?.runId;
    if (!id) return [];

    return this.db
      .prepare(
        `SELECT span_name as spanName, status, error_code as errorCode,
                duration_ns as durationNs, depth, parent_span_name as parentSpanName
         FROM spans
         WHERE run_id = ? AND span_name LIKE ?
         ORDER BY started_at ASC`,
      )
      .all(id, pattern) as TraceQueryResult[];
  }

  /** Get full span tree for a specific test (all descendants, defaults to latest run) */
  testTree(testName: string, runId?: string): TraceQueryResult[] {
    const id = runId ?? this.latestRun()?.runId;
    if (!id) return [];

    // Get the test span and all its descendants via recursive CTE
    return this.db
      .prepare(
        `WITH RECURSIVE tree AS (
           SELECT span_name, parent_span_name, status, error_code, duration_ns, depth, started_at
           FROM spans
           WHERE run_id = ? AND span_name = ? AND depth = (
             SELECT MIN(depth) FROM spans WHERE run_id = ? AND span_name = ?
           )
           UNION ALL
           SELECT s.span_name, s.parent_span_name, s.status, s.error_code, s.duration_ns, s.depth, s.started_at
           FROM spans s
           INNER JOIN tree t ON s.parent_span_name = t.span_name AND s.run_id = ?
         )
         SELECT span_name as spanName, status, error_code as errorCode,
                duration_ns as durationNs, depth, parent_span_name as parentSpanName
         FROM tree
         ORDER BY started_at ASC`,
      )
      .all(id, testName, id, testName, id) as TraceQueryResult[];
  }

  close(): void {
    this.db.close();
  }
}
