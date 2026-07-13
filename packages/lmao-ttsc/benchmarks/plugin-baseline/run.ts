#!/usr/bin/env bun

import { mkdir, mkdtemp, rm } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createBunTtscPlugin } from '../../src/index.js';

type Variant = 'OFF' | 'ON';

interface CliOptions {
  quick: boolean;
  preflight: boolean;
  offOutput?: string;
  onOutput?: string;
  abbaOutputDir?: string;
}

interface SemanticOutput {
  checksum: string;
  rowCount: number;
  rows: Array<Record<string, unknown>>;
}

function parseCli(argv: readonly string[]): CliOptions {
  const options: CliOptions = { quick: false, preflight: false };
  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index];
    if (argument === undefined) throw new RangeError(`Missing argument at index ${index}`);
    if (argument === '--quick') options.quick = true;
    else if (argument === '--preflight') options.preflight = true;
    else if (argument === '--off-output') options.offOutput = argv[++index];
    else if (argument.startsWith('--off-output=')) options.offOutput = argument.slice('--off-output='.length);
    else if (argument === '--on-output') options.onOutput = argv[++index];
    else if (argument.startsWith('--on-output=')) options.onOutput = argument.slice('--on-output='.length);
    else if (argument === '--abba-output-dir') options.abbaOutputDir = argv[++index];
    else if (argument.startsWith('--abba-output-dir=')) {
      options.abbaOutputDir = argument.slice('--abba-output-dir='.length);
    } else throw new Error(`Unknown argument: ${argument}`);
  }
  if (argv.at(-1) === '--off-output' || argv.at(-1) === '--on-output' || argv.at(-1) === '--abba-output-dir') {
    throw new Error('Output path option requires a path');
  }
  if (options.abbaOutputDir && (options.offOutput || options.onOutput)) {
    throw new Error('--abba-output-dir cannot be combined with --off-output or --on-output');
  }
  return options;
}

async function buildVariant(
  variant: Variant,
  sourcePath: string,
  projectPath: string,
  outputDir: string,
): Promise<string> {
  const plugins =
    variant === 'ON'
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
      : [];

  const result = await Bun.build({
    entrypoints: [sourcePath],
    outdir: outputDir,
    target: 'bun',
    external: ['@smoothbricks/lmao', '@smoothbricks/lmao/node'],
    minify: false,
    sourcemap: 'none',
    plugins,
  });
  if (!result.success) {
    throw new Error(`${variant} build failed:\n${result.logs.map((log) => log.message).join('\n')}`);
  }
  const output = result.outputs.find((candidate) => candidate.path.endsWith('.js'));
  if (!output) throw new Error(`${variant} build emitted no JavaScript`);
  return output.path;
}
async function assertTransformProof(offEntrypoint: string, onEntrypoint: string) {
  const [offSource, onSource] = await Promise.all([Bun.file(offEntrypoint).text(), Bun.file(onEntrypoint).text()]);
  const runtimeHintSignature = /runtimeHint:\s*\d+/g;
  const denseHeaderSignature = /_logHeaders\[/g;
  const registrationSignature = /registerLmaoVocabulary\w*\(\{/g;
  const offRuntimeHints = offSource.match(runtimeHintSignature)?.length ?? 0;
  const onRuntimeHints = onSource.match(runtimeHintSignature)?.length ?? 0;
  const onRuntimeHintValues = Array.from(onSource.matchAll(/runtimeHint:\s*(\d+)/g), (match) => Number(match[1]));
  const offDenseHeaders = offSource.match(denseHeaderSignature)?.length ?? 0;
  const onDenseHeaders = onSource.match(denseHeaderSignature)?.length ?? 0;
  const offRegistrations = offSource.match(registrationSignature)?.length ?? 0;
  const onRegistrations = onSource.match(registrationSignature)?.length ?? 0;
  if (
    offRuntimeHints !== 0 ||
    onRuntimeHints !== 2 ||
    onRuntimeHintValues[0] !== 194183232 ||
    onRuntimeHintValues[1] !== 0 ||
    offDenseHeaders !== 0 ||
    onDenseHeaders === 0 ||
    offRegistrations !== 0 ||
    onRegistrations !== 1
  ) {
    throw new Error(
      `Plugin transform proof failed: expected OFF runtime/dense/registration=0/0/0 and ON hints=194183232,0/dense>0/registration=1; received OFF=${offRuntimeHints}/${offDenseHeaders}/${offRegistrations}, ON hints=${onRuntimeHintValues.join(',')}/dense=${onDenseHeaders}/registration=${onRegistrations}`,
    );
  }
  return {
    offRuntimeHints,
    onRuntimeHints,
    onRuntimeHint: onRuntimeHintValues.join(','),
    offDenseHeaders,
    onDenseHeaders,
    offRegistrations,
    onRegistrations,
  };
}

async function runProcess(entrypoint: string, args: readonly string[]): Promise<string> {
  const child = Bun.spawn([process.execPath, entrypoint, ...args], {
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
    throw new Error(`Bun subprocess exited ${exitCode}:\n${stderr || stdout}`);
  }
  if (stderr.trim()) process.stderr.write(stderr);
  return stdout;
}

function parseJson<T>(variant: Variant, phase: string, text: string): T {
  try {
    return JSON.parse(text) as T;
  } catch (error) {
    throw new Error(`${variant} ${phase} did not emit valid JSON: ${(error as Error).message}\n${text}`);
  }
}

async function persist(path: string | undefined, contents: string): Promise<void> {
  if (!path) return;
  const absolute = resolve(path);
  await mkdir(dirname(absolute), { recursive: true });
  await Bun.write(absolute, contents);
}

const cli = parseCli(process.argv.slice(2));
const benchmarkDir = dirname(fileURLToPath(import.meta.url));
const sourcePath = join(benchmarkDir, 'workload.ts');
const temporaryRoot = await mkdtemp(join(benchmarkDir, '.tmp-'));
const projectPath = join(temporaryRoot, 'tsconfig.json');

try {
  await Bun.write(
    projectPath,
    JSON.stringify({
      extends: resolve(benchmarkDir, '../../../../tsconfig.base.json'),
      compilerOptions: {
        composite: false,
        declaration: false,
        declarationMap: false,
        emitDeclarationOnly: false,
        noEmit: true,
        rootDir: benchmarkDir,
        types: ['bun', 'node'],
      },
      files: [sourcePath],
    }),
  );
  const offDir = join(temporaryRoot, 'off');
  const onDir = join(temporaryRoot, 'on');
  await Promise.all([mkdir(offDir), mkdir(onDir)]);

  const [offEntrypoint, onEntrypoint] = await Promise.all([
    buildVariant('OFF', sourcePath, projectPath, offDir),
    buildVariant('ON', sourcePath, projectPath, onDir),
  ]);
  const transformProof = await assertTransformProof(offEntrypoint, onEntrypoint);

  const offSemanticPath = join(temporaryRoot, 'off-semantic.json');
  const onSemanticPath = join(temporaryRoot, 'on-semantic.json');
  const semanticArgs = cli.quick ? ['--semantic', '--quick'] : ['--semantic'];
  await Promise.all([
    runProcess(offEntrypoint, [...semanticArgs, '--semantic-output', offSemanticPath]),
    runProcess(onEntrypoint, [...semanticArgs, '--semantic-output', onSemanticPath]),
  ]);
  const [offSemanticText, onSemanticText] = await Promise.all([
    Bun.file(offSemanticPath).text(),
    Bun.file(onSemanticPath).text(),
  ]);
  const offSemantic = parseJson<SemanticOutput>('OFF', 'semantic check', offSemanticText);
  const onSemantic = parseJson<SemanticOutput>('ON', 'semantic check', onSemanticText);
  if (offSemantic.checksum !== onSemantic.checksum) {
    const differingRow = offSemantic.rows.findIndex(
      (row, index) => JSON.stringify(row) !== JSON.stringify(onSemantic.rows[index]),
    );
    const transformedLines = (await Bun.file(onEntrypoint).text()).split('\n');
    const operationWriteIndex = transformedLines.findIndex((line) => line.includes('operation_values[$$i] ='));
    const operationWriteBlock = transformedLines
      .slice(Math.max(0, operationWriteIndex - 24), operationWriteIndex + 3)
      .map((line) => line.trim());
    throw new Error(
      `Semantic parity failed: source expression=.operation(index % 2 === 0 ? 'READ' : 'WRITE'); OFF=${offSemantic.checksum}, ON=${onSemantic.checksum}, first differing row=${differingRow}, OFF row=${JSON.stringify(offSemantic.rows[differingRow])}, ON row=${JSON.stringify(onSemantic.rows[differingRow])}, transformed operation block=${JSON.stringify(operationWriteBlock)}`,
    );
  }
  if (offSemantic.rowCount !== onSemantic.rowCount) {
    throw new Error(`Semantic row counts differ: OFF=${offSemantic.rowCount}, ON=${onSemantic.rowCount}`);
  }

  if (!cli.preflight) {
    const benchmarkArgs = cli.quick ? ['--benchmark', '--quick'] : ['--benchmark'];
    if (cli.abbaOutputDir) {
      const outputDir = resolve(cli.abbaOutputDir);
      await mkdir(outputDir, { recursive: true });
      const positions = [
        ['OFF', offEntrypoint, 'off-pos1.json'],
        ['ON', onEntrypoint, 'on-pos2.json'],
        ['ON', onEntrypoint, 'on-pos1.json'],
        ['OFF', offEntrypoint, 'off-pos2.json'],
      ] as const;
      for (const [variant, entrypoint, filename] of positions) {
        const json = await runProcess(entrypoint, benchmarkArgs);
        parseJson<unknown>(variant, `Mitata benchmark ${filename}`, json);
        await persist(join(outputDir, filename), json);
      }
    } else {
      const [offJson, onJson] = await Promise.all([
        runProcess(offEntrypoint, benchmarkArgs),
        runProcess(onEntrypoint, benchmarkArgs),
      ]);
      parseJson<unknown>('OFF', 'Mitata benchmark', offJson);
      parseJson<unknown>('ON', 'Mitata benchmark', onJson);
      await Promise.all([persist(cli.offOutput, offJson), persist(cli.onOutput, onJson)]);
    }
  }

  process.stderr.write(
    `transform proof: OFF runtime/dense/registration=${transformProof.offRuntimeHints}/${transformProof.offDenseHeaders}/${transformProof.offRegistrations}, ON hint=${transformProof.onRuntimeHint}/dense=${transformProof.onDenseHeaders}/registration=${transformProof.onRegistrations}\n` +
      `semantic parity: ${offSemantic.checksum} (${offSemantic.rowCount} decoded Arrow rows)\n` +
      `${cli.abbaOutputDir ? `ABBA Mitata JSON directory: ${resolve(cli.abbaOutputDir)}\n` : ''}` +
      `${cli.offOutput ? `OFF Mitata JSON: ${resolve(cli.offOutput)}\n` : ''}` +
      `${cli.onOutput ? `ON Mitata JSON: ${resolve(cli.onOutput)}\n` : ''}`,
  );
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}
