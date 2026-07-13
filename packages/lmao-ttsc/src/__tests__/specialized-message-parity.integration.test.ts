import { expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createBunTtscPlugin } from '../index.js';

const fixturePath = fileURLToPath(new URL('./fixtures/specialized-message-parity.ts', import.meta.url));
const projectPath = fileURLToPath(new URL('../../tsconfig.test.json', import.meta.url));
const vocabularySyncPath = fileURLToPath(new URL('../../scripts/sync-vocabulary.mjs', import.meta.url));

interface FixtureResult {
  result: { ok: boolean; value: string };
  effects: number[];
  logFacts: string[];
  checksum: string;
  segments: Array<{ capacity: number; writeIndex: number; physicalLayout: 'current' | 'specialized' }>;
  physicalLayout: 'current' | 'specialized';
  hasRowHeaders: boolean;
  hasEntryType: boolean;
  hasLogHeaders: boolean;
  hasMessageIds: boolean;
  hasMessageValidity: boolean;
  hasRawMessages: boolean;
  encodedMessageIdentities: [number, number];
  rawMessageSentinels: [string | null, string | null];
  localMessageDictionaryLength: number;
}

async function buildFixture(
  pluginEnabled: boolean,
  outputDir: string,
  vocabularyManifest: string,
  project: string,
): Promise<string> {
  const result = await Bun.build({
    entrypoints: [fixturePath],
    outdir: outputDir,
    target: 'bun',
    external: ['@smoothbricks/lmao', '@smoothbricks/lmao/node', '@smoothbricks/lmao/testing'],
    plugins: pluginEnabled
      ? [
          createBunTtscPlugin({
            project,
            plugins: [
              {
                transform: '@smoothbricks/lmao-ttsc/ttsc-plugin',
                vocabularyManifest,
              },
            ],
          }),
        ]
      : [],
  });
  if (!result.success) throw new Error(result.logs.map((log) => log.message).join('\n'));
  const output = result.outputs.find((candidate) => candidate.path.endsWith('.js'));
  if (!output) throw new Error('Specialized message parity fixture emitted no JavaScript');
  return output.path;
}

async function syncVocabulary(project: string, vocabularyManifest: string): Promise<void> {
  const child = Bun.spawn(
    [
      process.execPath,
      vocabularySyncPath,
      '--write',
      `--cwd=${dirname(projectPath)}`,
      `--tsconfig=${project}`,
      `--lmao-vocabulary-manifest=${vocabularyManifest}`,
    ],
    { cwd: dirname(projectPath), stdout: 'pipe', stderr: 'pipe', env: process.env },
  );
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) throw new Error(`Specialized message vocabulary sync exited ${exitCode}:\n${stderr || stdout}`);
}

async function executeFixture(entrypoint: string): Promise<FixtureResult> {
  const child = Bun.spawn([process.execPath, entrypoint], {
    cwd: dirname(projectPath),
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
    const source = await Bun.file(entrypoint).text();
    const runtimeHints = Array.from(source.matchAll(/runtimeHint:\s*(\d+)/g), (match) => match[1]);
    throw new Error(
      `Specialized message parity fixture exited ${exitCode} with runtime hints ${runtimeHints.join(', ')}:\n${stderr || stdout}`,
    );
  }
  return JSON.parse(stdout);
}

test('plugin ON selects specialized capacity-64 static-50 while OFF stays current with identical semantics', async () => {
  const temporaryRoot = await mkdtemp(join(dirname(fixturePath), '.specialized-message-parity-'));
  try {
    const vocabularyManifest = join(temporaryRoot, 'lmao.vocabulary.json');
    const isolatedProject = join(temporaryRoot, 'tsconfig.json');
    await Bun.write(
      isolatedProject,
      JSON.stringify({ extends: projectPath, include: [], files: [fixturePath] }),
    );
    await syncVocabulary(isolatedProject, vocabularyManifest);
    const [offEntrypoint, onEntrypoint] = await Promise.all([
      buildFixture(false, join(temporaryRoot, 'off'), vocabularyManifest, isolatedProject),
      buildFixture(true, join(temporaryRoot, 'on'), vocabularyManifest, isolatedProject),
    ]);
    const [pluginOff, pluginOn] = await Promise.all([
      executeFixture(offEntrypoint),
      executeFixture(onEntrypoint),
    ]);

    const expectedEffects = Array.from({ length: 31 }, (_, index) => index);
    expect(pluginOff.effects).toEqual(expectedEffects);
    expect(pluginOn.effects).toEqual(expectedEffects);
    expect(pluginOn.result).toEqual(pluginOff.result);
    expect(pluginOn.logFacts).toEqual(pluginOff.logFacts);
    expect(pluginOn.logFacts).toHaveLength(62);
    expect(pluginOn.checksum).toBe('570359dd6fc493e7d2ff54f46a29e7218c5b399cc3827d81185e8a181ab0a2d3');
    expect(pluginOff.checksum).toBe(pluginOn.checksum);
    expect(pluginOff.segments).toEqual([{ capacity: 64, writeIndex: 64, physicalLayout: 'current' }]);
    expect(pluginOn.segments).toEqual([{ capacity: 64, writeIndex: 64, physicalLayout: 'specialized' }]);
    expect(pluginOff).toMatchObject({
      physicalLayout: 'current',
      hasRowHeaders: false,
      hasEntryType: true,
      hasLogHeaders: false,
      hasMessageIds: true,
      hasMessageValidity: false,
      hasRawMessages: true,
      encodedMessageIdentities: [0, 0],
      rawMessageSentinels: ['static-00', 'dynamic-00'],
      localMessageDictionaryLength: 0,
    });
    expect(pluginOn).toMatchObject({
      physicalLayout: 'specialized',
      hasRowHeaders: false,
      hasEntryType: true,
      hasLogHeaders: true,
      hasMessageIds: false,
      hasMessageValidity: false,
      hasRawMessages: true,
      rawMessageSentinels: [null, 'dynamic-00'],
      localMessageDictionaryLength: 0,
    });
    expect(pluginOn.encodedMessageIdentities[0]).toBeGreaterThan(0);
    expect(pluginOn.encodedMessageIdentities[1]).toBe(0);
  } finally {
    await rm(temporaryRoot, { recursive: true, force: true });
  }
});
