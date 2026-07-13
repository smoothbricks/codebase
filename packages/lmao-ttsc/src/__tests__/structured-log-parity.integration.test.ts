import { expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createBunTtscPlugin } from '../index.js';

const fixturePath = fileURLToPath(new URL('./fixtures/structured-log-parity.ts', import.meta.url));
const projectPath = fileURLToPath(new URL('../../tsconfig.test.json', import.meta.url));

interface FixtureResult {
  result: { ok: boolean; value: string };
  effects: string[];
  decodedFacts: string[];
  logHeaders: number[];
  rawMessages: (string | null)[];
}

async function buildFixture(pluginEnabled: boolean, outputDir: string): Promise<string> {
  const result = await Bun.build({
    entrypoints: [fixturePath],
    outdir: outputDir,
    target: 'bun',
    external: ['@smoothbricks/lmao', '@smoothbricks/lmao/node', '@smoothbricks/lmao/testing'],
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
  if (!result.success) {
    throw new Error(result.logs.map((log) => log.message).join('\n'));
  }
  const output = result.outputs.find((candidate) => candidate.path.endsWith('.js'));
  if (!output) throw new Error('Structured logging fixture emitted no JavaScript');
  return output.path;
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
  if (exitCode !== 0) {
    throw new Error(`Structured logging fixture exited ${exitCode}:\n${stderr || stdout}`);
  }
  return JSON.parse(stdout);
}

test('plugin OFF and ON preserve decoded facts and effects while ON uses dense vocabulary rows', async () => {
  const temporaryRoot = await mkdtemp(join(dirname(fixturePath), '.structured-log-parity-'));
  try {
    const offDir = join(temporaryRoot, 'off');
    const onDir = join(temporaryRoot, 'on');
    const [offEntrypoint, onEntrypoint] = await Promise.all([buildFixture(false, offDir), buildFixture(true, onDir)]);
    const [pluginOff, pluginOn] = await Promise.all([executeFixture(offEntrypoint), executeFixture(onEntrypoint)]);

    const expectedSemantics = {
      result: { ok: true, value: 'complete' },
      effects: [
        'info.userId',
        'info.elapsedMs',
        'warn.region',
        'error.userId',
        'error.elapsedMs',
        'debug.message',
        'trace.message',
      ],
      decodedFacts: [
        'log:info: loaded {userId} in {elapsedMs}ms',
        'log:warn: literal braces: {ok} for {region}',
        'log:error: loaded {userId} in {elapsedMs}ms',
        'log:debug: debug-state-ready',
        'log:trace: trace-state-ready',
        'tag:userId: user-42',
        'tag:elapsedMs: 17',
        'tag:region: iad',
        'tag:userId: user-99',
        'tag:elapsedMs: 29',
      ],
    };
    const offSemantics = {
      result: pluginOff.result,
      effects: pluginOff.effects,
      decodedFacts: pluginOff.decodedFacts,
    };
    const onSemantics = {
      result: pluginOn.result,
      effects: pluginOn.effects,
      decodedFacts: pluginOn.decodedFacts,
    };
    expect(offSemantics).toEqual(expectedSemantics);
    expect(onSemantics).toEqual(expectedSemantics);
    expect(onSemantics).toEqual(offSemantics);

    expect(pluginOff.logHeaders.every((header) => header === 0)).toBe(true);
    expect(pluginOn.logHeaders.filter((header) => header !== 0)).toHaveLength(3);
    expect(pluginOn.rawMessages).toContain('debug-state-ready');
    expect(pluginOn.rawMessages).toContain('trace-state-ready');
    expect(pluginOn.rawMessages).not.toContain('loaded {userId} in {elapsedMs}ms');
    expect(pluginOn.rawMessages).not.toContain('literal braces: {ok} for {region}');
  } finally {
    await rm(temporaryRoot, { recursive: true, force: true });
  }
});
