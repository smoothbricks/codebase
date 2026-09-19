import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { ViteUserConfig } from 'vitest/config';

/** Bun owns this unit suite; the child exercises the real Vitest consumer API. */
async function runFixture(name: string) {
  const directory = await mkdtemp(join(tmpdir(), 'lmao-vitest-contract-'));
  try {
    const fixture = fileURLToPath(new URL(`./fixtures/${name}.fixture.ts`, import.meta.url));
    const root = fileURLToPath(new URL('../../../../', import.meta.url));
    const config = join(directory, 'vitest.config.mjs');
    const settings = { test: { include: [fixture], maxWorkers: 1 } } satisfies ViteUserConfig;
    await writeFile(
      config,
      `import { precompiledTestPlugin } from ${JSON.stringify(import.meta.resolve('@smoothbricks/validation/test-build'))};\n` +
        `export default { ...${JSON.stringify(settings)}, plugins: [precompiledTestPlugin()] };`,
    );
    const command = Bun.spawn(
      [
        process.execPath,
        fileURLToPath(new URL('../vitest.mjs', import.meta.resolve('vitest'))),
        'run',
        '--root',
        root,
        '--config',
        config,
      ],
      { stdout: 'pipe', stderr: 'pipe' },
    );
    const [exitCode, stdout, stderr] = await Promise.all([
      command.exited,
      new Response(command.stdout).text(),
      new Response(command.stderr).text(),
    ]);
    return { exitCode, output: stdout + stderr };
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

describe('vitest harness framework contract', () => {
  it('traces real each, conditional, fixture, retry and concurrent callbacks across await', async () => {
    const { exitCode, output } = await runFixture('vitest-harness');
    expect(exitCode, output).toBe(0);
  });

  it('fails the framework run when afterAll cannot persist trace output', async () => {
    const { exitCode, output } = await runFixture('vitest-harness-flush');
    expect(exitCode, output).not.toBe(0);
    expect(output).toContain('trace-persistence-refused');
  });
});
