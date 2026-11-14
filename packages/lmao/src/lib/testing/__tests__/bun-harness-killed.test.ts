import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ENTRY_TYPE_SPAN_OK, ENTRY_TYPE_SPAN_START } from '../../schema/systemSchema.js';

const fixture = fileURLToPath(new URL('./fixtures/bun-harness-killed.fixture.ts', import.meta.url));

interface SinkRow {
  message: string | null;
  row_index: number;
  entry_type: number;
  parent_span_id: number;
}

describe('a traced Bun run that is killed while a test runs', () => {
  it('has left the finished test whole and the start of the test that was running', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'lmao-bun-killed-'));
    try {
      const sink = join(directory, 'sink', 'trace-results.db');
      // Its own `bun test` process, in a directory with no bunfig, so no preload traces it as well.
      const command = Bun.spawn([process.execPath, 'test', fixture], {
        cwd: directory,
        env: { ...process.env, NO_COLOR: '1', LMAO_FIXTURE_SINK: sink },
        stdout: 'ignore',
        stderr: 'ignore',
      });
      await command.exited;
      expect(command.signalCode).toBe('SIGKILL');

      const db = new Database(sink, { readonly: true });
      const rows = db
        .query<SinkRow, []>(
          'SELECT message, row_index, entry_type, parent_span_id FROM spans ORDER BY span_id, row_index',
        )
        .all();
      db.close();

      const root = rows.filter((row) => row.parent_span_id === 0);
      expect(root).toEqual([
        { message: 'test-run', row_index: 0, entry_type: ENTRY_TYPE_SPAN_START, parent_span_id: 0 },
      ]);

      const tests = rows.filter((row) => row.parent_span_id !== 0);
      expect(tests.map((row) => [row.message, row.row_index, row.entry_type])).toEqual([
        ['finishes before the kill', 0, ENTRY_TYPE_SPAN_START],
        [null, 1, ENTRY_TYPE_SPAN_OK],
        ['is running when the process is killed', 0, ENTRY_TYPE_SPAN_START],
      ]);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});
