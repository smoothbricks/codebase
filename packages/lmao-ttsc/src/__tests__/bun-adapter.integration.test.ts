import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { rm } from 'node:fs/promises';
import { basename, dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import * as adapterExports from '@smoothbricks/lmao-ttsc';
import {
  getExportConditions,
  importDeclaredModule,
  pickTarget,
  readPackageManifest,
} from '@smoothbricks/validation/testing';
import { makeBuildOutputRoot } from './build-output-root.js';

// Every fixture is bundled by one plugin-ON and one plugin-OFF Bun.build, through the package's exported adapter:
// the ON build's whole-project ttsc compile is paid once, and each test reads its own entry's output.
const projectPath = fileURLToPath(new URL('../../tsconfig.test.json', import.meta.url));
const packageUrl = new URL('../../package.json', import.meta.url);
const PARITY = ['structured-log-parity', 'specialized-message-parity', 'enum-metadata-parity'] as const;
type Fixture = (typeof PARITY)[number] | 'typed-op';

interface Built {
  readonly path: string;
  readonly source: string;
}

async function buildFixtures(
  pluginEnabled: boolean,
  fixtures: readonly Fixture[],
  outdir: string,
): Promise<ReadonlyMap<Fixture, Built>> {
  const result = await Bun.build({
    entrypoints: fixtures.map((name) => fileURLToPath(new URL(`./fixtures/${name}.ts`, import.meta.url))),
    outdir,
    target: 'bun',
    external: ['@smoothbricks/lmao', '@smoothbricks/lmao/node', '@smoothbricks/lmao/testing'],
    plugins: pluginEnabled
      ? [
          adapterExports.createBunTtscPlugin({
            project: projectPath,
            plugins: [{ transform: '@smoothbricks/lmao-ttsc/ttsc-plugin' }],
          }),
        ]
      : [],
  });
  if (!result.success) throw new Error(result.logs.map((log) => log.message).join('\n'));
  const built = new Map<Fixture, Built>();
  for (const name of fixtures) {
    const output = result.outputs.find((candidate) => basename(candidate.path) === `${name}.js`);
    if (output === undefined) throw new Error(`The ${name} fixture emitted no JavaScript`);
    built.set(name, { path: output.path, source: await output.text() });
  }
  return built;
}

async function executeFixture<T>(built: Built, cwd: string): Promise<T> {
  const child = Bun.spawn([process.execPath, built.path], { cwd, stdout: 'pipe', stderr: 'pipe', env: process.env });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) {
    const runtimeHints = Array.from(built.source.matchAll(/runtimeHint:\s*(\d+)/g), (match) => match[1]);
    throw new Error(
      `${basename(built.path)} exited ${exitCode} with runtime hints ${runtimeHints.join(', ')}:\n${stderr || stdout}`,
    );
  }
  return JSON.parse(stdout);
}

let outputRoot = '';
let pluginOn: ReadonlyMap<Fixture, Built> = new Map();
let pluginOff: ReadonlyMap<Fixture, Built> = new Map();

beforeAll(async () => {
  outputRoot = await makeBuildOutputRoot('bun-adapter');
  [pluginOn, pluginOff] = await Promise.all([
    buildFixtures(true, [...PARITY, 'typed-op'], join(outputRoot, 'on')),
    buildFixtures(false, PARITY, join(outputRoot, 'off')),
  ]);
});

afterAll(async () => {
  if (outputRoot !== '') await rm(outputRoot, { recursive: true, force: true });
});

function output(builds: ReadonlyMap<Fixture, Built>, name: Fixture): Built {
  const built = builds.get(name);
  if (built === undefined) throw new Error(`No build of the ${name} fixture`);
  return built;
}

test('the exported Bun adapter applies the explicitly selected native plugin', async () => {
  expect(typeof adapterExports.createBunTtscPlugin).toBe('function');
  // The build output path is created per run, so a static import cannot address this module boundary.
  const builtModule = await import(pathToFileURL(output(pluginOn, 'typed-op').path).href);
  const typedOp: unknown = builtModule.typedOp;
  if ((typeof typedOp !== 'object' && typeof typedOp !== 'function') || typedOp === null) {
    throw new TypeError('Bun adapter output did not export the transformed typed Op');
  }
  const callsitePlan: unknown = Reflect.get(typedOp, 'callsitePlan');
  if (typeof callsitePlan !== 'object' || callsitePlan === null) {
    throw new TypeError('Bun adapter output did not expose the transformed Op callsite plan');
  }
  const runtimeHint: unknown = Reflect.get(callsitePlan, 'runtimeHint');
  const localMessageDictionary: unknown = Reflect.get(callsitePlan, 'localMessageDictionary');
  if (typeof runtimeHint !== 'number' || !Array.isArray(localMessageDictionary)) {
    throw new TypeError('Bun adapter output exposed an invalid transformed Op callsite plan');
  }
  expect(runtimeHint).not.toBe(0);
  expect(localMessageDictionary).toHaveLength(2);
  expect('createLmaoTransformer' in adapterExports).toBe(false);
});

interface StructuredResult {
  result: { ok: boolean; value: string };
  effects: string[];
  decodedFacts: string[];
  logHeaders: number[];
  rawMessages: (string | null)[];
}

test('plugin OFF and ON preserve decoded facts and effects while ON uses dense vocabulary rows', async () => {
  const [off, on] = await Promise.all([
    executeFixture<StructuredResult>(output(pluginOff, 'structured-log-parity'), process.cwd()),
    executeFixture<StructuredResult>(output(pluginOn, 'structured-log-parity'), process.cwd()),
  ]);

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
  const offSemantics = { result: off.result, effects: off.effects, decodedFacts: off.decodedFacts };
  const onSemantics = { result: on.result, effects: on.effects, decodedFacts: on.decodedFacts };
  expect(offSemantics).toEqual(expectedSemantics);
  expect(onSemantics).toEqual(expectedSemantics);
  expect(onSemantics).toEqual(offSemantics);

  expect(off.logHeaders.every((header) => header === 0)).toBe(true);
  expect(on.logHeaders.filter((header) => header !== 0)).toHaveLength(3);
  expect(on.rawMessages).toContain('debug-state-ready');
  expect(on.rawMessages).toContain('trace-state-ready');
  expect(on.rawMessages).not.toContain('loaded {userId} in {elapsedMs}ms');
  expect(on.rawMessages).not.toContain('literal braces: {ok} for {region}');
});

interface SpecializedResult {
  result: { ok: boolean; value: string };
  effects: number[];
  logFacts: string[];
  checksum: string;
  segments: Array<{ capacity: number; writeIndex: number; physicalLayout: 'current' | 'specialized' }>;
  physicalLayout: 'current' | 'specialized';
  messageIdentityStorage: 'local-u16' | 'global-u32' | 'packed-row-headers';
  hasRowHeaders: boolean;
  hasEntryType: boolean;
  hasLogHeaders: boolean;
  hasMessageIds: boolean;
  hasMessageValidity: boolean;
  hasRawMessages: boolean;
  rawMessageSentinels: [string | null, string | null];
}

test('plugin ON selects specialized capacity-64 static-50 while OFF stays current with identical semantics', async () => {
  const [off, on] = await Promise.all([
    executeFixture<SpecializedResult>(output(pluginOff, 'specialized-message-parity'), dirname(projectPath)),
    executeFixture<SpecializedResult>(output(pluginOn, 'specialized-message-parity'), dirname(projectPath)),
  ]);

  const expectedEffects = Array.from({ length: 31 }, (_, index) => index);
  expect(off.effects).toEqual(expectedEffects);
  expect(on.effects).toEqual(expectedEffects);
  expect(on.result).toEqual(off.result);
  expect(on.logFacts).toEqual(off.logFacts);
  expect(on.logFacts).toHaveLength(62);
  expect(on.checksum).toBe('570359dd6fc493e7d2ff54f46a29e7218c5b399cc3827d81185e8a181ab0a2d3');
  expect(off.checksum).toBe(on.checksum);
  expect(off.segments).toEqual([{ capacity: 64, writeIndex: 64, physicalLayout: 'current' }]);
  expect(on.segments).toEqual([{ capacity: 64, writeIndex: 64, physicalLayout: 'specialized' }]);
  expect(off).toMatchObject({
    physicalLayout: 'current',
    messageIdentityStorage: 'local-u16',
    hasRowHeaders: false,
    hasEntryType: true,
    hasLogHeaders: false,
    hasMessageIds: true,
    hasMessageValidity: false,
    hasRawMessages: true,
    rawMessageSentinels: ['static-00', 'dynamic-00'],
  });
  expect(on).toMatchObject({
    physicalLayout: 'specialized',
    messageIdentityStorage: 'global-u32',
    hasRowHeaders: false,
    hasEntryType: true,
    hasLogHeaders: true,
    hasMessageIds: false,
    hasMessageValidity: false,
    hasRawMessages: true,
    rawMessageSentinels: [null, 'dynamic-00'],
  });
});

interface EnumResult {
  storage: Array<{ writeIndex: number; valueBytes: number[]; nullBytes: number[] }>;
  decoded: Array<string | null>;
  spanBufferCompilerCalls: number;
  spanBufferCompilerSources: string[];
}

test('plugin OFF and ON preserve exact enum value/null bytes while ON reuses plan-bound encoders', async () => {
  const onBuild = output(pluginOn, 'enum-metadata-parity');
  const [off, on] = await Promise.all([
    executeFixture<EnumResult>(output(pluginOff, 'enum-metadata-parity'), process.cwd()),
    executeFixture<EnumResult>(onBuild, process.cwd()),
  ]);

  expect(on.storage).toEqual(off.storage);
  expect(on.decoded).toEqual(off.decoded);
  expect(on.storage.length).toBeGreaterThan(0);
  expect(on.decoded).toContain('READ');
  expect(on.decoded).toContain('WRITE');
  expect(off.spanBufferCompilerCalls).toBeGreaterThan(0);
  expect(on.spanBufferCompilerSources).toEqual([]);
  expect(onBuild.source).toContain('@smoothbricks/lmao/span-buffer/aot/v1');
  expect(onBuild.source).toContain('class $$LmaoSpanBuffer_');
  expect(onBuild.source).toContain('.enumLookup.byField["operation"].encode(');
  expect(onBuild.source).not.toContain('case "READ":');
  expect(onBuild.source).not.toContain('case "WRITE":');
});

describe('package export contract', () => {
  test('declares Bun adapter and runtime-register targets', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const rootConditions = getExportConditions(manifest, '.');
    const bunConditions = getExportConditions(manifest, './bun');
    const registerConditions = getExportConditions(manifest, './bun-register');
    const pluginConditions = getExportConditions(manifest, './ttsc-plugin');

    expect(manifest.name).toBe('@smoothbricks/lmao-ttsc');
    expect(manifest.exports['./package.json']).toBe('./package.json');
    expect(await Bun.file(new URL(pickTarget(pluginConditions, './ttsc-plugin', ['types']), packageUrl)).exists()).toBe(
      true,
    );
    expect(
      await Bun.file(new URL(pickTarget(pluginConditions, './ttsc-plugin', ['require']), packageUrl)).exists(),
    ).toBe(true);
    expect(await Bun.file(new URL(pickTarget(rootConditions, '.', ['types']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(rootConditions, '.', ['import']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(bunConditions, '.', ['types']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(bunConditions, '.', ['import']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(registerConditions, '.', ['types']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(registerConditions, '.', ['import']), packageUrl)).exists()).toBe(true);
  });

  test('loads the declared Bun build adapter', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, '.', ['import']);

    expect(typeof mod.default).toBe('function');
    expect(mod.createBunTtscPlugin).toBe(mod.default);
  });

  test('loads the declared ttsc plugin descriptor', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, './ttsc-plugin', ['require']);

    expect(typeof mod.default).toBe('function');
  });
});
