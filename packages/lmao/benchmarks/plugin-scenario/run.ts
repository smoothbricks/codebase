import { mkdir, mkdtemp, rm } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import mitataPackage from 'mitata/package.json' with { type: 'json' };

const MANIFEST_SCHEMA_VERSION = 1;
const scenarioDirectory = dirname(fileURLToPath(import.meta.url));
const workloadPath = join(scenarioDirectory, 'workload.ts');
const projectRoot = resolve(scenarioDirectory, '../../../..');
const lmaoNodeUrl = pathToFileURL(resolve(projectRoot, 'packages/lmao/src/node.ts')).href;
const nativePluginPath = resolve(projectRoot, 'packages/lmao-ttsc/plugin.cjs');
const nativeAdapterUrl = pathToFileURL(resolve(projectRoot, 'packages/lmao-ttsc/src/index.ts')).href;
const lmaoTtscPackage = await Bun.file(resolve(projectRoot, 'packages/lmao-ttsc/package.json')).json();
const { createBunTtscPlugin } = await import(nativeAdapterUrl);
const textDecoder = new TextDecoder();

type Variant = 'off' | 'on';

interface RunnerOptions {
  preflight: boolean;
  quick: boolean;
  abbaOutputDirectory?: string;
  offOutput?: string;
  onOutput?: string;
  semanticOutput?: string;
  manifestOutput?: string;
}

interface TransformSignatureCounts {
  runtimeHintSignatures: number;
  directBufferWriteSignatures: number;
  vocabularyRegistrationSignatures: number;
}

interface TransformProof {
  off: TransformSignatureCounts;
  on: TransformSignatureCounts;
}

interface Launch {
  variant: Variant;
  filename: string;
  outputPath: string;
}

function requireOptionValue(args: string[], index: number, option: string): string {
  const value = args[index + 1];
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${option} requires an explicit path`);
  }
  return value;
}

function parseArguments(args: string[]): RunnerOptions {
  const options: RunnerOptions = { preflight: false, quick: false };

  for (let index = 0; index < args.length; index++) {
    const argument = args[index];
    switch (argument) {
      case '--preflight':
        if (options.preflight) {
          throw new Error('--preflight may only be specified once');
        }
        options.preflight = true;
        break;
      case '--quick':
        if (options.quick) {
          throw new Error('--quick may only be specified once');
        }
        options.quick = true;
        break;
      case '--abba-output-dir':
        if (options.abbaOutputDirectory !== undefined) {
          throw new Error('--abba-output-dir may only be specified once');
        }
        options.abbaOutputDirectory = requireOptionValue(args, index, argument);
        index++;
        break;
      case '--off-output':
        if (options.offOutput !== undefined) {
          throw new Error('--off-output may only be specified once');
        }
        options.offOutput = requireOptionValue(args, index, argument);
        index++;
        break;
      case '--on-output':
        if (options.onOutput !== undefined) {
          throw new Error('--on-output may only be specified once');
        }
        options.onOutput = requireOptionValue(args, index, argument);
        index++;
        break;
      case '--semantic-output':
        if (options.semanticOutput !== undefined) {
          throw new Error('--semantic-output may only be specified once');
        }
        options.semanticOutput = requireOptionValue(args, index, argument);
        index++;
        break;
      case '--manifest-output':
        if (options.manifestOutput !== undefined) {
          throw new Error('--manifest-output may only be specified once');
        }
        options.manifestOutput = requireOptionValue(args, index, argument);
        index++;
        break;
      default:
        throw new Error(`Unknown runner argument: ${String(argument)}`);
    }
  }

  const hasAbbaOutput = options.abbaOutputDirectory !== undefined;
  const hasOffOutput = options.offOutput !== undefined;
  const hasOnOutput = options.onOutput !== undefined;

  if (hasAbbaOutput && (hasOffOutput || hasOnOutput)) {
    throw new Error('--abba-output-dir is mutually exclusive with --off-output and --on-output');
  }
  if (hasOffOutput !== hasOnOutput) {
    throw new Error('--off-output and --on-output must be specified together');
  }
  if (!options.preflight && !hasAbbaOutput && !hasOffOutput) {
    throw new Error('Benchmark mode requires --abba-output-dir or both --off-output and --on-output');
  }
  if (
    options.preflight &&
    !hasAbbaOutput &&
    !hasOffOutput &&
    options.semanticOutput === undefined &&
    options.manifestOutput === undefined
  ) {
    throw new Error('Preflight mode requires an explicit artifact path');
  }

  return options;
}

function outputBaseDirectory(options: RunnerOptions): string {
  if (options.abbaOutputDirectory !== undefined) {
    return resolve(options.abbaOutputDirectory);
  }
  if (options.offOutput !== undefined) {
    return dirname(resolve(options.offOutput));
  }
  if (options.semanticOutput !== undefined) {
    return dirname(resolve(options.semanticOutput));
  }
  if (options.manifestOutput !== undefined) {
    return dirname(resolve(options.manifestOutput));
  }
  throw new Error('Unable to determine artifact output directory');
}

function createLaunches(options: RunnerOptions): Launch[] {
  if (options.preflight) {
    return [];
  }

  if (options.abbaOutputDirectory !== undefined) {
    const directory = resolve(options.abbaOutputDirectory);
    return [
      { variant: 'off', filename: 'off-pos1.json', outputPath: join(directory, 'off-pos1.json') },
      { variant: 'on', filename: 'on-pos2.json', outputPath: join(directory, 'on-pos2.json') },
      { variant: 'on', filename: 'on-pos1.json', outputPath: join(directory, 'on-pos1.json') },
      { variant: 'off', filename: 'off-pos2.json', outputPath: join(directory, 'off-pos2.json') },
    ];
  }

  if (options.offOutput === undefined || options.onOutput === undefined) {
    throw new Error('Non-ABBA benchmark mode requires both output paths');
  }

  return [
    { variant: 'off', filename: basename(options.offOutput), outputPath: resolve(options.offOutput) },
    { variant: 'on', filename: basename(options.onOutput), outputPath: resolve(options.onOutput) },
  ];
}

function assertDistinctOutputPaths(paths: string[]): void {
  const seen = new Set<string>();
  for (const path of paths) {
    if (seen.has(path)) {
      throw new Error(`Output paths must be distinct: ${path}`);
    }
    seen.add(path);
  }
}

async function compileWorkload(variant: Variant, projectPath: string, outputDirectory: string): Promise<string> {
  const result = await Bun.build({
    entrypoints: [workloadPath],
    outdir: outputDirectory,
    target: 'bun',
    external: ['@smoothbricks/lmao', '@smoothbricks/lmao/node', 'mitata'],
    minify: false,
    sourcemap: 'none',
    ...(variant === 'on'
      ? {
          plugins: [
            createBunTtscPlugin({
              project: projectPath,
              plugins: [
                {
                  transform: nativePluginPath,
                },
              ],
            }),
          ],
        }
      : {}),
  });

  if (!result.success) {
    throw new Error(
      `${variant.toUpperCase()} native build failed:\n${result.logs.map((log) => log.message).join('\n')}`,
    );
  }
  const emittedJavaScript = result.outputs.find((candidate) => candidate.path.endsWith('.js'));
  if (emittedJavaScript === undefined) {
    throw new Error(`${variant.toUpperCase()} native build emitted no JavaScript`);
  }

  const outputText = await emittedJavaScript.text();
  const runtimeJavaScript = outputText
    .replaceAll('"@smoothbricks/lmao/node"', JSON.stringify(lmaoNodeUrl))
    .replaceAll("'@smoothbricks/lmao/node'", JSON.stringify(lmaoNodeUrl));
  if (runtimeJavaScript === outputText) {
    throw new Error(`${variant.toUpperCase()} compiled workload did not retain the expected LMAO Node import`);
  }
  return runtimeJavaScript;
}

function proveTransformation(offJavaScript: string, onJavaScript: string): TransformProof {
  const runtimeHintPattern = /\bruntimeHint:\s*\d+\b/g;
  const directBufferWritePattern = /\b(?:_logHeaders|[A-Za-z_$][\w$]*_values)\[\$\$i\]\s*=/g;
  const vocabularyRegistrationPattern = /\bregisterLmaoVocabulary\w*\(\{/g;

  const countSignatures = (source: string): TransformSignatureCounts => ({
    runtimeHintSignatures: source.match(runtimeHintPattern)?.length ?? 0,
    directBufferWriteSignatures: source.match(directBufferWritePattern)?.length ?? 0,
    vocabularyRegistrationSignatures: source.match(vocabularyRegistrationPattern)?.length ?? 0,
  });
  const proof: TransformProof = {
    off: countSignatures(offJavaScript),
    on: countSignatures(onJavaScript),
  };

  if (
    proof.off.runtimeHintSignatures !== 0 ||
    proof.off.directBufferWriteSignatures !== 0 ||
    proof.off.vocabularyRegistrationSignatures !== 0
  ) {
    throw new Error(
      `OFF compilation unexpectedly contains native transform signatures: runtime hints=${proof.off.runtimeHintSignatures}, direct buffer writes=${proof.off.directBufferWriteSignatures}, vocabulary registrations=${proof.off.vocabularyRegistrationSignatures}`,
    );
  }
  if (
    proof.on.runtimeHintSignatures !== 2 ||
    proof.on.directBufferWriteSignatures < 1 ||
    proof.on.vocabularyRegistrationSignatures !== 1
  ) {
    throw new Error(
      `ON compilation is missing native transform signatures: expected runtime hints=2, direct buffer writes>=1, vocabulary registrations=1; received ${proof.on.runtimeHintSignatures}/${proof.on.directBufferWriteSignatures}/${proof.on.vocabularyRegistrationSignatures}`,
    );
  }

  return proof;
}

async function runSubprocess(compiledWorkload: string, args: string[], label: string) {
  const child = Bun.spawn({
    cmd: [process.execPath, compiledWorkload, ...args],
    cwd: projectRoot,
    env: process.env,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const stdoutPromise = new Response(child.stdout).arrayBuffer();
  const stderrPromise = new Response(child.stderr).text();
  const [exitCode, stdoutBuffer, stderr] = await Promise.all([child.exited, stdoutPromise, stderrPromise]);
  const stdout = new Uint8Array(stdoutBuffer);

  if (exitCode !== 0) {
    throw new Error(
      `${label} failed with exit code ${exitCode}\nstdout:\n${textDecoder.decode(stdout)}\nstderr:\n${stderr}`,
    );
  }
  if (stderr.length > 0) {
    throw new Error(`${label} wrote unexpected stderr:\n${stderr}`);
  }

  return stdout;
}

async function semanticPreflight(offWorkload: string, onWorkload: string, temporaryDirectory: string): Promise<string> {
  const offSemanticPath = join(temporaryDirectory, 'semantic.off.json');
  const onSemanticPath = join(temporaryDirectory, 'semantic.on.json');

  const offStdout = await runSubprocess(offWorkload, ['--semantic-output', offSemanticPath], 'OFF semantic preflight');
  if (offStdout.byteLength !== 0) {
    throw new Error(`OFF semantic preflight wrote unexpected stdout:\n${textDecoder.decode(offStdout)}`);
  }

  const onStdout = await runSubprocess(onWorkload, ['--semantic-output', onSemanticPath], 'ON semantic preflight');
  if (onStdout.byteLength !== 0) {
    throw new Error(`ON semantic preflight wrote unexpected stdout:\n${textDecoder.decode(onStdout)}`);
  }

  const [offSemantic, onSemantic] = await Promise.all([
    Bun.file(offSemanticPath).text(),
    Bun.file(onSemanticPath).text(),
  ]);
  if (offSemantic !== onSemantic) {
    throw new Error('OFF and ON semantic JSON differ byte-for-byte');
  }
  if (!offSemantic.endsWith('\n')) {
    throw new Error('Semantic JSON must end with one newline');
  }

  return offSemantic;
}

function commitSha(): string {
  const result = Bun.spawnSync({
    cmd: ['git', '-C', projectRoot, 'rev-parse', 'HEAD'],
    stdout: 'pipe',
    stderr: 'pipe',
  });
  if (result.exitCode !== 0) {
    throw new Error(`Unable to read commit SHA:\n${textDecoder.decode(result.stderr)}`);
  }
  const sha = textDecoder.decode(result.stdout).trim();
  if (!/^[0-9a-f]{40}$/.test(sha)) {
    throw new Error(`git rev-parse returned an invalid commit SHA: ${sha}`);
  }
  return sha;
}

function workloadSha256(sourceBytes: Uint8Array): string {
  return new Bun.CryptoHasher('sha256').update(sourceBytes).digest('hex');
}

async function ensureParentDirectory(path: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
}

async function main(): Promise<void> {
  const options = parseArguments(process.argv.slice(2));
  const baseDirectory = outputBaseDirectory(options);
  const semanticOutput = resolve(options.semanticOutput ?? join(baseDirectory, 'semantic.json'));
  const manifestOutput = resolve(options.manifestOutput ?? join(baseDirectory, 'manifest.json'));
  const launches = createLaunches(options);
  assertDistinctOutputPaths([semanticOutput, manifestOutput, ...launches.map((launch) => launch.outputPath)]);

  const sourceBuffer = new Uint8Array(await Bun.file(workloadPath).arrayBuffer());
  const temporaryDirectory = await mkdtemp(join(scenarioDirectory, '.plugin-scenario-'));

  try {
    const projectPath = join(temporaryDirectory, 'tsconfig.json');
    await Bun.write(
      projectPath,
      `${JSON.stringify(
        {
          extends: resolve(projectRoot, 'tsconfig.base.json'),
          compilerOptions: {
            composite: false,
            declaration: false,
            declarationMap: false,
            emitDeclarationOnly: false,
            noEmit: true,
            rootDir: scenarioDirectory,
            types: ['bun', 'node'],
          },
          files: [workloadPath],
        },
        null,
        2,
      )}\n`,
    );

    const offBuildDirectory = join(temporaryDirectory, 'build.off');
    const onBuildDirectory = join(temporaryDirectory, 'build.on');
    await Promise.all([mkdir(offBuildDirectory), mkdir(onBuildDirectory)]);
    const [offJavaScript, onJavaScript] = await Promise.all([
      compileWorkload('off', projectPath, offBuildDirectory),
      compileWorkload('on', projectPath, onBuildDirectory),
    ]);
    const transformProof = proveTransformation(offJavaScript, onJavaScript);
    const offWorkload = join(temporaryDirectory, 'workload.off.mjs');
    const onWorkload = join(temporaryDirectory, 'workload.on.mjs');
    await Promise.all([Bun.write(offWorkload, offJavaScript), Bun.write(onWorkload, onJavaScript)]);

    const semanticJson = await semanticPreflight(offWorkload, onWorkload, temporaryDirectory);
    await ensureParentDirectory(semanticOutput);
    await Bun.write(semanticOutput, semanticJson);

    for (const launch of launches) {
      const compiledWorkload = launch.variant === 'off' ? offWorkload : onWorkload;
      const stdout = await runSubprocess(
        compiledWorkload,
        [],
        `${launch.variant.toUpperCase()} benchmark ${launch.filename}`,
      );
      await ensureParentDirectory(launch.outputPath);
      await Bun.write(launch.outputPath, stdout);
    }

    const manifest = {
      schemaVersion: MANIFEST_SCHEMA_VERSION,
      commitSha: commitSha(),
      workloadSha256: workloadSha256(sourceBuffer),
      versions: {
        bun: Bun.version,
        typescript: lmaoTtscPackage.dependencies.typescript,
        mitata: mitataPackage.version,
        lmaoTtsc: lmaoTtscPackage.version,
      },
      compilerKind: 'native-ttsc',
      transformProof,
      quick: options.quick,
      quickBehavior: options.quick ? 'label-only' : 'standard',
      preflightOnly: options.preflight,
      launchOrder: launches.map((launch, index) => ({
        position: index + 1,
        variant: launch.variant,
        filename: launch.filename,
      })),
      filenames: {
        semantic: basename(semanticOutput),
        manifest: basename(manifestOutput),
        benchmarks: launches.map((launch) => launch.filename),
      },
    };
    await ensureParentDirectory(manifestOutput);
    await Bun.write(manifestOutput, `${JSON.stringify(manifest, null, 2)}\n`);
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
}

await main();
