import { expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createBunTtscPlugin } from '../index.js';

const fixturePath = fileURLToPath(new URL('./fixtures/enum-metadata-parity.ts', import.meta.url));
const projectPath = fileURLToPath(new URL('../../tsconfig.test.json', import.meta.url));

interface FixtureResult {
  storage: Array<{ writeIndex: number; valueBytes: number[]; nullBytes: number[] }>;
  decoded: Array<string | null>;
  spanBufferCompilerCalls: number;
  spanBufferCompilerSources: string[];
}

interface BuiltFixture {
  path: string;
  source: string;
}

async function buildFixture(pluginEnabled: boolean, outputDir: string): Promise<BuiltFixture> {
  const result = await Bun.build({
    entrypoints: [fixturePath],
    outdir: outputDir,
    target: 'bun',
    external: ['@smoothbricks/lmao', '@smoothbricks/lmao/node'],
    plugins: pluginEnabled
      ? [
          createBunTtscPlugin({
            project: projectPath,
            plugins: [
              {
                transform: '@smoothbricks/lmao-ttsc/ttsc-plugin',
              },
            ],
          }),
        ]
      : [],
  });
  if (!result.success) throw new Error(result.logs.map((log) => log.message).join('\n'));
  const output = result.outputs.find((candidate) => candidate.path.endsWith('.js'));
  if (!output) throw new Error('Enum metadata fixture emitted no JavaScript');
  return { path: output.path, source: await output.text() };
}

async function executeFixture(entrypoint: string): Promise<FixtureResult> {
  const child = Bun.spawn([process.execPath, entrypoint], {
    cwd: process.cwd(),
    stdout: 'pipe',
    stderr: 'pipe',
    env: process.env,
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) throw new Error(`Enum metadata fixture exited ${exitCode}:\n${stderr || stdout}`);
  return JSON.parse(stdout);
}

test('plugin OFF and ON preserve exact enum value/null bytes while ON reuses plan-bound encoders', async () => {
  const temporaryRoot = await mkdtemp(join(dirname(fixturePath), '.enum-metadata-parity-'));
  try {
    const [offBuild, onBuild] = await Promise.all([
      buildFixture(false, join(temporaryRoot, 'off')),
      buildFixture(true, join(temporaryRoot, 'on')),
    ]);
    const [pluginOff, pluginOn] = await Promise.all([executeFixture(offBuild.path), executeFixture(onBuild.path)]);

    expect(pluginOn.storage).toEqual(pluginOff.storage);
    expect(pluginOn.decoded).toEqual(pluginOff.decoded);
    expect(pluginOn.storage.length).toBeGreaterThan(0);
    expect(pluginOn.decoded).toContain('READ');
    expect(pluginOn.decoded).toContain('WRITE');
    expect(pluginOff.spanBufferCompilerCalls).toBeGreaterThan(0);
    expect(pluginOn.spanBufferCompilerSources).toEqual([]);
    expect(onBuild.source).toContain('@smoothbricks/lmao/span-buffer/aot/v1');
    expect(onBuild.source).toContain('class $$LmaoSpanBuffer_');
    expect(onBuild.source).toContain('.enumLookup.byField["operation"].encode(');
    expect(onBuild.source).not.toContain('case "READ":');
    expect(onBuild.source).not.toContain('case "WRITE":');
  } finally {
    await rm(temporaryRoot, { recursive: true, force: true });
  }
});
