import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ENTRY_TYPE_SPAN_OK, ENTRY_TYPE_SPAN_START } from '../../schema/systemSchema.js';

const preload = fileURLToPath(new URL('./fixtures/bun-harness-worker-runs.preload.ts', import.meta.url));
const fixtureA = fileURLToPath(new URL('./fixtures/bun-harness-worker-runs-a.fixture.ts', import.meta.url));
const fixtureB = fileURLToPath(new URL('./fixtures/bun-harness-worker-runs-b.fixture.ts', import.meta.url));

interface SinkRow {
  trace_id: string;
  span_id: number;
  parent_span_id: number;
  row_index: number;
  entry_type: number;
  message: string | null;
}

interface TraceRunSummary {
  rootRows: Array<{ row_index: number; entry_type: number; message: string | null }>;
  children: Array<{ name: string | null; rows: Array<{ row_index: number; entry_type: number }> }>;
}

/** Runs both fixture files in a child `bun test` process in a temp directory with no bunfig. */
async function runFixtures(entry: 'instance' | 'global', flags: readonly string[]) {
  const directory = await mkdtemp(join(tmpdir(), 'lmao-bun-worker-runs-'));
  try {
    const sink = join(directory, 'sink', 'trace-results.db');
    const command = Bun.spawn([process.execPath, 'test', '--preload', preload, ...flags, fixtureA, fixtureB], {
      cwd: directory,
      env: { ...process.env, NO_COLOR: '1', LMAO_FIXTURE_ENTRY: entry, LMAO_FIXTURE_SINK: sink },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      command.exited,
      new Response(command.stdout).text(),
      new Response(command.stderr).text(),
    ]);
    const output = stdout + stderr;

    const db = new Database(sink, { readonly: true });
    let rows: SinkRow[] = [];
    try {
      rows = db
        .query<SinkRow, []>(
          'SELECT trace_id, span_id, parent_span_id, row_index, entry_type, message FROM spans ORDER BY trace_id, span_id, row_index',
        )
        .all();
    } finally {
      db.close();
    }

    return { exitCode, output, runs: summarizeRuns(rows) };
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

function summarizeRuns(rows: readonly SinkRow[]): TraceRunSummary[] {
  const byTrace = new Map<string, SinkRow[]>();
  for (const row of rows) {
    const group = byTrace.get(row.trace_id);
    if (group) group.push(row);
    else byTrace.set(row.trace_id, [row]);
  }

  const summaries: TraceRunSummary[] = [];
  for (const traceRows of byTrace.values()) {
    const rootRows = traceRows.filter((row) => row.parent_span_id === 0);
    const rootSpanId = rootRows[0]?.span_id;
    const bySpan = new Map<number, SinkRow[]>();
    for (const row of traceRows) {
      if (row.parent_span_id === 0) continue;
      if (row.parent_span_id !== rootSpanId) {
        throw new Error(`unexpected grandchild span ${row.span_id} under parent ${row.parent_span_id}`);
      }
      const spanRows = bySpan.get(row.span_id);
      if (spanRows) spanRows.push(row);
      else bySpan.set(row.span_id, [row]);
    }

    const children = [...bySpan.values()]
      .map((spanRows) => ({
        name: spanRows.find((row) => row.row_index === 0)?.message ?? null,
        rows: spanRows.map((row) => ({ row_index: row.row_index, entry_type: row.entry_type })),
      }))
      .sort((left, right) => String(left.name).localeCompare(String(right.name)));

    summaries.push({
      rootRows: rootRows.map((row) => ({
        row_index: row.row_index,
        entry_type: row.entry_type,
        message: row.message,
      })),
      children,
    });
  }

  return summaries.sort((left, right) => String(left.children[0]?.name).localeCompare(String(right.children[0]?.name)));
}

const completeRootRows = [
  { row_index: 0, entry_type: ENTRY_TYPE_SPAN_START, message: 'test-run' },
  { row_index: 1, entry_type: ENTRY_TYPE_SPAN_OK, message: null },
];

const completeChildRows = [
  { row_index: 0, entry_type: ENTRY_TYPE_SPAN_START },
  { row_index: 1, entry_type: ENTRY_TYPE_SPAN_OK },
];

function countLinesContaining(output: string, marker: string): number {
  return output.split('\n').filter((line) => line.includes(marker)).length;
}

describe('bun harness ends one trace run per preload afterAll and opens the next', () => {
  for (const entry of ['instance', 'global'] as const) {
    // Bun 1.4.4 treats `--parallel=1 --no-isolate` as an in-process sequential run (one afterAll at exit), so
    // `--parallel=2 --parallel-delay=600000 --no-isolate` enters the worker loop while keeping the second worker
    // unspawned so one `--no-isolate` worker runs both files with one preload load and one preload afterAll per file.
    it(`${entry}: one --no-isolate worker produces one complete test-run per file with no EBADF`, async () => {
      const { exitCode, output, runs } = await runFixtures(entry, [
        '--parallel=2',
        '--parallel-delay=600000',
        '--no-isolate',
      ]);
      expect(exitCode, output).toBe(0);
      expect(output).not.toContain('EBADF');
      expect(countLinesContaining(output, 'FIXTURE preload'), output).toBe(1);
      expect(countLinesContaining(output, 'FIXTURE later preload afterAll'), output).toBe(2);
      expect(runs).toEqual([
        {
          rootRows: completeRootRows,
          children: [
            { name: 'file a first', rows: completeChildRows },
            { name: 'file a second', rows: completeChildRows },
          ],
        },
        {
          rootRows: completeRootRows,
          children: [
            { name: 'file b first', rows: completeChildRows },
            { name: 'file b second', rows: completeChildRows },
          ],
        },
      ]);
    });

    it(`${entry}: --parallel=1 --isolate produces one complete test-run per file`, async () => {
      const { exitCode, output, runs } = await runFixtures(entry, ['--parallel=1', '--isolate']);
      expect(exitCode, output).toBe(0);
      expect(output).not.toContain('EBADF');
      expect(countLinesContaining(output, 'FIXTURE preload'), output).toBe(2);
      expect(countLinesContaining(output, 'FIXTURE later preload afterAll'), output).toBe(2);
      expect(runs).toEqual([
        {
          rootRows: completeRootRows,
          children: [
            { name: 'file a first', rows: completeChildRows },
            { name: 'file a second', rows: completeChildRows },
          ],
        },
        {
          rootRows: completeRootRows,
          children: [
            { name: 'file b first', rows: completeChildRows },
            { name: 'file b second', rows: completeChildRows },
          ],
        },
      ]);
    });

    it(`${entry}: plain bun test keeps one test-run holding all four tests`, async () => {
      const { exitCode, output, runs } = await runFixtures(entry, []);
      expect(exitCode, output).toBe(0);
      expect(output).not.toContain('EBADF');
      expect(countLinesContaining(output, 'FIXTURE preload'), output).toBe(1);
      expect(countLinesContaining(output, 'FIXTURE later preload afterAll'), output).toBe(1);
      expect(runs).toEqual([
        {
          rootRows: completeRootRows,
          children: [
            { name: 'file a first', rows: completeChildRows },
            { name: 'file a second', rows: completeChildRows },
            { name: 'file b first', rows: completeChildRows },
            { name: 'file b second', rows: completeChildRows },
          ],
        },
      ]);
    });
  }
});
