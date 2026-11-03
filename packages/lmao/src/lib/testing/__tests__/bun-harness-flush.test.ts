import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const fixture = fileURLToPath(new URL('./fixtures/bun-harness-flush.fixture.ts', import.meta.url));

/** Runs the fixture as its own `bun test` process, in a directory with no bunfig so no preload traces it as well. */
async function runFixture(entry: 'instance' | 'global') {
  const directory = await mkdtemp(join(tmpdir(), 'lmao-bun-flush-'));
  try {
    const sink = join(directory, 'sink', 'trace-results.db');
    const command = Bun.spawn([process.execPath, 'test', fixture], {
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
    return { exitCode, output: stdout + stderr, sink };
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

describe('bun harness teardown when the root trace cannot be written', () => {
  for (const entry of ['instance', 'global'] as const) {
    it(`${entry}: reports the sink and trace id, still closes the tracer, and fails the run`, async () => {
      const { exitCode, output, sink } = await runFixture(entry);
      expect(exitCode, output).not.toBe(0);
      const report = output.split('\n').find((line) => line.includes('[lmao/testing] SQLite flush error:'));
      expect(report, output).toBeDefined();
      expect(report, output).toMatch(/\[lmao\/testing\] SQLite flush error: trace_id: [0-9a-f]{32} -> /);
      expect(report, output).toContain(` -> ${sink}`);
      expect(output).toContain('no such table');
      expect(output).toContain('FIXTURE tracer closed');
    });
  }
});
