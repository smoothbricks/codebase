import { mkdir, rm, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { $ } from 'bun';
import { Command, InvalidArgumentError } from 'commander';
import { Bench, type FnOptions, type Statistics, type Task } from 'tinybench';

type ImageFormat = 'SPARSE' | 'ASIF';

interface Options {
  files: number;
  fileSize: number;
  imageSize: string;
  ioMiB: number;
  iterations: number;
  metadataFiles: number;
  root: string;
  keep: boolean;
  format: ImageFormat;
}

interface Timing {
  name: string;
  milliseconds: number;
}

interface StatisticalResult {
  samples: number[];
  samplesCount: number;
  meanMs: number;
  medianMs: number;
  p75Ms: number;
  p99Ms: number;
  minMs: number;
  maxMs: number;
  standardDeviationMs: number;
  standardErrorMs: number;
  marginOfErrorMs: number;
  relativeMarginOfErrorPercent: number;
}

interface BenchmarkTask {
  name: string;
  operation: () => Promise<unknown>;
  hooks?: FnOptions;
}

const projectDir = import.meta.dir;
const mounted = new Set<string>();

function parsePositiveInteger(value: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new InvalidArgumentError('must be a positive integer');
  }
  return parsed;
}

function parseImageSize(value: string): string {
  if (!/^\d+[kmgt]$/i.test(value)) {
    throw new InvalidArgumentError('must look like 32g or 500g');
  }
  return value;
}

function parseRoot(value: string): string {
  const root = resolve(value);
  if (!root.startsWith('/private/tmp/')) {
    throw new InvalidArgumentError('must be a directory strictly under /private/tmp');
  }
  return root;
}

function parseFormat(value: string): ImageFormat {
  const upper = value.toUpperCase();
  if (upper !== 'SPARSE' && upper !== 'ASIF') {
    throw new InvalidArgumentError('must be SPARSE or ASIF');
  }
  return upper;
}

function parseOptions(argv: string[]): Options {
  const program = new Command()
    .name('bench.ts')
    .description('Benchmarks APFS sparse-image workspace strategies: shadow mounts versus clonefile copies.')
    .option('--files <count>', 'synthetic hot-workspace files', parsePositiveInteger, 100_000)
    .option('--file-size <bytes>', 'bytes per synthetic file', parsePositiveInteger, 256)
    .option('--image-size <size>', 'sparse image capacity, e.g. 32g', parseImageSize, '32g')
    .option('--io-mib <mib>', 'direct sequential I/O per sample', parsePositiveInteger, 128)
    .option('--iterations <count>', 'samples per benchmark task', parsePositiveInteger, 20)
    .option('--metadata-files <count>', 'small files per metadata sample', parsePositiveInteger, 10_000)
    .option(
      '--root <path>',
      'temporary root under /private/tmp',
      parseRoot,
      `/private/tmp/apfs-workspace-bench-${process.pid}`,
    )
    .option('--format <fmt>', 'image format: SPARSE (hdiutil) or ASIF (diskutil, macOS 26+)', parseFormat, 'SPARSE')
    .option('--keep', 'keep images after the benchmark', false)
    .parse(argv);
  const opts = program.opts();
  return {
    files: opts.files,
    fileSize: opts.fileSize,
    imageSize: opts.imageSize,
    ioMiB: opts.ioMib,
    iterations: opts.iterations,
    metadataFiles: opts.metadataFiles,
    root: opts.root,
    keep: opts.keep,
    format: opts.format,
  };
}

async function timed(name: string, operation: () => Promise<unknown>): Promise<Timing> {
  const started = performance.now();
  await operation();
  return { name, milliseconds: performance.now() - started };
}

async function attachImage(image: string, mountpoint: string): Promise<void> {
  // hdiutil cannot attach ASIF ("image format is not supported by hdiutil; use
  // 'diskutil image attach'"), so dispatch on the image extension. diskutil defaults to
  // browsable + noowners, hence the explicit flags. (owners-on ASIF makes the volume root
  // root-owned and unwritable to the user, so the bench mounts ASIF without owners; a real
  // cowshed workspace would chown the volume root after create — see the fork's report.)
  if (image.endsWith('.asif')) {
    await $`diskutil image attach --nobrowse --mountPoint ${mountpoint} ${image}`.quiet();
  } else {
    await $`hdiutil attach -quiet -nobrowse -owners on -mountpoint ${mountpoint} ${image}`.quiet();
  }
  mounted.add(mountpoint);
}

async function createBaseImage(format: ImageFormat, image: string, size: string): Promise<void> {
  if (format === 'ASIF') {
    // diskutil's --fs does not expose case-sensitivity; ASIF base uses plain APFS.
    await $`diskutil image create blank --format ASIF --size ${size} --volumeName APFSBench --fs APFS ${image}`.quiet();
  } else {
    await $`hdiutil create -quiet -size ${size} -type SPARSE -fs ${'Case-sensitive APFS'} -volname APFSBench -nospotlight ${image}`.quiet();
  }
}

async function attachShadow(base: string, shadow: string, mountpoint: string): Promise<void> {
  await $`hdiutil attach -quiet -nobrowse -owners on -shadow ${shadow} -mountpoint ${mountpoint} ${base}`.quiet();
  mounted.add(mountpoint);
}

async function detach(mountpoint: string): Promise<void> {
  await $`hdiutil detach -quiet ${mountpoint}`.quiet();
  mounted.delete(mountpoint);
}

function resultFromTask(task: Task): StatisticalResult {
  if (task.result?.state !== 'completed') {
    throw new Error(`Benchmark ${task.name} did not complete: ${task.result?.state ?? 'missing result'}`);
  }
  const statistics: Statistics = task.result.latency;
  return {
    samples: statistics.samples ? Array.from(statistics.samples) : [],
    samplesCount: statistics.samplesCount,
    meanMs: statistics.mean,
    medianMs: statistics.p50,
    p75Ms: statistics.p75,
    p99Ms: statistics.p99,
    minMs: statistics.min,
    maxMs: statistics.max,
    standardDeviationMs: statistics.sd,
    standardErrorMs: statistics.sem,
    marginOfErrorMs: statistics.moe,
    relativeMarginOfErrorPercent: statistics.rme,
  };
}

async function runStatisticalSuite(
  name: string,
  tasks: BenchmarkTask[],
  iterations: number,
): Promise<Record<string, StatisticalResult>> {
  const bench = new Bench({
    name,
    iterations,
    time: 0,
    warmup: false,
    retainSamples: true,
    throws: true,
    timestampProvider: 'bunNanoseconds',
  });
  for (const task of tasks) {
    bench.add(task.name, task.operation, { async: true, retainSamples: true, ...task.hooks });
  }
  await bench.run();

  const results = Object.fromEntries(bench.tasks.map((task) => [task.name, resultFromTask(task)]));
  console.log(`\n${name}`);
  console.table(
    Object.entries(results).map(([task, result]) => ({
      task,
      samples: result.samplesCount,
      'mean ms': result.meanMs.toFixed(2),
      'median ms': result.medianMs.toFixed(2),
      'p99 ms': result.p99Ms.toFixed(2),
      'sd ms': result.standardDeviationMs.toFixed(2),
      'RME %': result.relativeMarginOfErrorPercent.toFixed(2),
    })),
  );
  return results;
}

async function makeSyntheticFiles(workspace: string, totalFiles: number, fileSize: number): Promise<string[]> {
  const sourceCount = Math.max(1, Math.floor(totalFiles * 0.2));
  const nodeCount = Math.floor(totalFiles * 0.5);
  const rustCount = totalFiles - sourceCount - nodeCount;
  const filesPerDirectory = 200;
  const payload = Buffer.alloc(fileSize, 0x61);
  const sourceFiles: string[] = [];
  const paths: string[] = [];

  const categories = [
    { root: join(workspace, 'src'), count: sourceCount, extension: 'ts', source: true },
    {
      root: join(workspace, 'node_modules'),
      count: nodeCount,
      extension: 'js',
      source: false,
    },
    {
      root: join(workspace, 'target', 'debug', 'incremental'),
      count: rustCount,
      extension: 'o',
      source: false,
    },
  ];

  for (const category of categories) {
    for (let index = 0; index < category.count; index += 1) {
      const directory = join(
        category.root,
        `group-${Math.floor(index / filesPerDirectory)
          .toString()
          .padStart(6, '0')}`,
      );
      const path = join(directory, `file-${index.toString().padStart(8, '0')}.${category.extension}`);
      paths.push(path);
      if (category.source) sourceFiles.push(path);
    }
  }

  const directories = [...new Set(paths.map(dirname))];
  for (let offset = 0; offset < directories.length; offset += 256) {
    await Promise.all(
      directories.slice(offset, offset + 256).map((directory) => mkdir(directory, { recursive: true })),
    );
  }

  for (let offset = 0; offset < paths.length; offset += 512) {
    await Promise.all(paths.slice(offset, offset + 512).map((path) => Bun.write(path, payload)));
    const populated = Math.min(offset + 512, paths.length);
    if (Math.floor(populated / 50_000) > Math.floor(offset / 50_000)) {
      console.log(`  populated ${populated.toLocaleString()} / ${paths.length.toLocaleString()} files`);
    }
  }

  return sourceFiles;
}

async function initialiseGitWorkspace(workspace: string, sourceFiles: string[]): Promise<void> {
  await writeFile(join(workspace, '.gitignore'), 'node_modules/\ntarget/\nbench-*.bin\nmetadata-bench/\n');
  await $`git -C ${workspace} init -q -b main`.quiet();
  await $`git -C ${workspace} add src .gitignore`.quiet();
  await $`git -C ${workspace} -c user.name=Benchmark -c user.email=benchmark@invalid commit -q -m main`.quiet();
  await $`git -C ${workspace} checkout -q -b bench-feature`.quiet();

  const changedCount = Math.min(1_000, Math.max(1, Math.floor(sourceFiles.length * 0.05)));
  const changedPayload = Buffer.from('feature branch\n');
  for (let offset = 0; offset < changedCount; offset += 256) {
    await Promise.all(
      sourceFiles.slice(offset, Math.min(offset + 256, changedCount)).map((path) => Bun.write(path, changedPayload)),
    );
  }

  await $`git -C ${workspace} add src`.quiet();
  await $`git -C ${workspace} -c user.name=Benchmark -c user.email=benchmark@invalid commit -q -m feature`.quiet();
  await $`git -C ${workspace} checkout -q main`.quiet();
}

async function createMetadataFiles(root: string, count: number): Promise<void> {
  const filesPerDirectory = 200;
  const payload = Buffer.from('metadata\n');
  const paths: string[] = [];
  for (let index = 0; index < count; index += 1) {
    paths.push(
      join(
        root,
        `group-${Math.floor(index / filesPerDirectory)
          .toString()
          .padStart(5, '0')}`,
        `file-${index.toString().padStart(8, '0')}`,
      ),
    );
  }

  const directories = [...new Set(paths.map(dirname))];
  await Promise.all(directories.map((directory) => mkdir(directory, { recursive: true })));
  for (let offset = 0; offset < paths.length; offset += 512) {
    await Promise.all(paths.slice(offset, offset + 512).map((path) => Bun.write(path, payload)));
  }
}

function freshSessionTasks(base: string, options: Options): BenchmarkTask[] {
  let shadowIndex = 0;
  let currentShadow = '';
  let shadowMount = '';
  let cloneOnlyIndex = 0;
  let currentCloneOnly = '';
  let cloneTotalIndex = 0;
  let currentCloneTotal = '';
  let cloneTotalMount = '';
  let cloneAttachIndex = 0;
  let currentCloneAttach = '';
  const cloneExt = options.format === 'ASIF' ? 'asif' : 'sparseimage';
  const cloneAttachMount = join(options.root, 'mount-clone-attach-only');
  const attachOnlyClones = Array.from({ length: options.iterations }, (_, index) =>
    join(options.root, `attach-only-${index}.${cloneExt}`),
  );

  const tasks: BenchmarkTask[] = [
    {
      name: 'shadow: create + attach',
      operation: async () => attachShadow(base, currentShadow, shadowMount),
      hooks: {
        beforeAll: async () => {
          await mkdir(join(options.root, 'mount-shadow-fresh'), { recursive: true });
        },
        beforeEach: async () => {
          const index = shadowIndex++;
          currentShadow = join(options.root, `fresh-${index}.shadow`);
          shadowMount = join(options.root, 'mount-shadow-fresh', String(index));
          await mkdir(shadowMount, { recursive: true });
        },
        afterEach: async () => {
          await detach(shadowMount);
          await rm(currentShadow, { force: true });
        },
      },
    },
    {
      name: 'clone: clonefile only',
      operation: async () => {
        await $`/bin/cp -c ${base} ${currentCloneOnly}`.quiet();
      },
      hooks: {
        beforeEach: () => {
          currentCloneOnly = join(options.root, `clone-only-${cloneOnlyIndex++}.${cloneExt}`);
        },
        afterEach: async () => {
          await rm(currentCloneOnly, { force: true });
        },
      },
    },
    {
      name: 'clone: fresh attach only',
      operation: async () => attachImage(currentCloneAttach, cloneAttachMount),
      hooks: {
        beforeAll: async () => {
          await mkdir(cloneAttachMount, { recursive: true });
          for (const clone of attachOnlyClones) {
            await $`/bin/cp -c ${base} ${clone}`.quiet();
          }
        },
        beforeEach: () => {
          currentCloneAttach = attachOnlyClones[cloneAttachIndex++];
        },
        afterEach: async () => {
          await detach(cloneAttachMount);
        },
        afterAll: async () => {
          await Promise.all(attachOnlyClones.map((clone) => rm(clone, { force: true })));
        },
      },
    },
    {
      name: 'clone: clonefile + attach',
      operation: async () => {
        await $`/bin/cp -c ${base} ${currentCloneTotal}`.quiet();
        await attachImage(currentCloneTotal, cloneTotalMount);
      },
      hooks: {
        beforeEach: async () => {
          const index = cloneTotalIndex++;
          currentCloneTotal = join(options.root, `clone-total-${index}.${cloneExt}`);
          cloneTotalMount = join(options.root, 'mount-clone-total', String(index));
          await mkdir(cloneTotalMount, { recursive: true });
        },
        afterEach: async () => {
          await detach(cloneTotalMount);
          await rm(currentCloneTotal, { force: true });
        },
      },
    },
  ];
  // Shadows are hdiutil-only and cannot back an ASIF image, so drop them under --format ASIF.
  return options.format === 'ASIF' ? tasks.filter((task) => !task.name.startsWith('shadow')) : tasks;
}

function warmReattachTasks(
  base: string,
  shadow: string,
  shadowMount: string,
  clone: string,
  cloneMount: string,
  format: ImageFormat,
): BenchmarkTask[] {
  const tasks: BenchmarkTask[] = [
    {
      name: 'shadow: warm reattach',
      operation: async () => attachShadow(base, shadow, shadowMount),
      hooks: { afterEach: async () => detach(shadowMount) },
    },
    {
      name: 'clone: warm reattach',
      operation: async () => attachImage(clone, cloneMount),
      hooks: { afterEach: async () => detach(cloneMount) },
    },
  ];
  return format === 'ASIF' ? tasks.filter((task) => !task.name.startsWith('shadow')) : tasks;
}

function mountedOperationTasks(shadowMount: string, cloneMount: string, options: Options): BenchmarkTask[] {
  const shadowWorkspace = join(shadowMount, 'workspace');
  const cloneWorkspace = join(cloneMount, 'workspace');
  const tasks: BenchmarkTask[] = [];

  const addPair = (
    operation: string,
    shadowOperation: () => Promise<unknown>,
    cloneOperation: () => Promise<unknown>,
    shadowHooks?: FnOptions,
    cloneHooks?: FnOptions,
  ) => {
    // No shadow mount exists under --format ASIF; emit only the clone task.
    if (options.format === 'SPARSE') {
      tasks.push({ name: `${operation}: shadow`, operation: shadowOperation, hooks: shadowHooks });
    }
    tasks.push({ name: `${operation}: clone`, operation: cloneOperation, hooks: cloneHooks });
  };

  addPair(
    'git status',
    async () => $`git -C ${shadowWorkspace} status --porcelain=v1`.quiet(),
    async () => $`git -C ${cloneWorkspace} status --porcelain=v1`.quiet(),
  );
  addPair(
    'branch checkout pair',
    async () => {
      await $`git -C ${shadowWorkspace} checkout -q bench-feature`.quiet();
      await $`git -C ${shadowWorkspace} checkout -q main`.quiet();
    },
    async () => {
      await $`git -C ${cloneWorkspace} checkout -q bench-feature`.quiet();
      await $`git -C ${cloneWorkspace} checkout -q main`.quiet();
    },
  );
  addPair(
    'direct sequential read',
    async () =>
      $`/bin/dd if=${join(shadowWorkspace, 'bench-payload.bin')} of=/dev/null bs=8388608 iflag=direct`.quiet(),
    async () => $`/bin/dd if=${join(cloneWorkspace, 'bench-payload.bin')} of=/dev/null bs=8388608 iflag=direct`.quiet(),
  );

  const shadowWrite = join(shadowWorkspace, 'bench-write.bin');
  const cloneWrite = join(cloneWorkspace, 'bench-write.bin');
  addPair(
    'direct synchronous write',
    async () => $`/bin/dd if=/dev/zero of=${shadowWrite} bs=1048576 count=${options.ioMiB} oflag=direct,fsync`.quiet(),
    async () => $`/bin/dd if=/dev/zero of=${cloneWrite} bs=1048576 count=${options.ioMiB} oflag=direct,fsync`.quiet(),
    { afterEach: async () => rm(shadowWrite, { force: true }) },
    { afterEach: async () => rm(cloneWrite, { force: true }) },
  );

  const shadowMetadata = join(shadowWorkspace, 'metadata-bench');
  const cloneMetadata = join(cloneWorkspace, 'metadata-bench');
  addPair(
    'metadata create',
    async () => createMetadataFiles(shadowMetadata, options.metadataFiles),
    async () => createMetadataFiles(cloneMetadata, options.metadataFiles),
    { afterEach: async () => rm(shadowMetadata, { recursive: true, force: true }) },
    { afterEach: async () => rm(cloneMetadata, { recursive: true, force: true }) },
  );
  addPair(
    'metadata delete',
    async () => $`/bin/rm -rf ${shadowMetadata}`.quiet(),
    async () => $`/bin/rm -rf ${cloneMetadata}`.quiet(),
    { beforeEach: async () => createMetadataFiles(shadowMetadata, options.metadataFiles) },
    { beforeEach: async () => createMetadataFiles(cloneMetadata, options.metadataFiles) },
  );

  return tasks;
}

const options = parseOptions(Bun.argv);
const base = join(options.root, options.format === 'ASIF' ? 'base.asif' : 'base.sparseimage');
const templateMount = join(options.root, 'mount-template');
const resultsDirectory = join(projectDir, 'results');
const startedAt = new Date();

await rm(options.root, { recursive: true, force: true });
await mkdir(options.root, { recursive: true });
await mkdir(templateMount, { recursive: true });
await mkdir(resultsDirectory, { recursive: true });

const setupTimings: Timing[] = [];
let freshSessions: Record<string, StatisticalResult> = {};
let warmReattach: Record<string, StatisticalResult> = {};
let operations: Record<string, StatisticalResult> = {};

try {
  console.log(`Creating ${options.imageSize} ${options.format} APFS image...`);
  setupTimings.push(
    await timed('create image', async () => {
      await createBaseImage(options.format, base, options.imageSize);
    }),
  );
  setupTimings.push(
    await timed('attach template', async () => {
      await attachImage(base, templateMount);
    }),
  );

  const workspace = join(templateMount, 'workspace');
  await mkdir(workspace, { recursive: true });
  console.log(`Populating ${options.files.toLocaleString()} synthetic workspace files...`);
  let sourceFiles: string[] = [];
  setupTimings.push(
    await timed('populate files', async () => {
      sourceFiles = await makeSyntheticFiles(workspace, options.files, options.fileSize);
    }),
  );
  setupTimings.push(
    await timed('initialise git', async () => {
      await initialiseGitWorkspace(workspace, sourceFiles);
    }),
  );
  setupTimings.push(
    await timed('create read payload', async () => {
      await $`/bin/dd if=/dev/zero of=${join(workspace, 'bench-payload.bin')} bs=1048576 count=${options.ioMiB} oflag=direct,fsync`.quiet();
    }),
  );
  await detach(templateMount);

  freshSessions = await runStatisticalSuite(
    'Fresh session lifecycle (Tinybench)',
    freshSessionTasks(base, options),
    options.iterations,
  );

  const useShadow = options.format === 'SPARSE';
  const persistentShadow = join(options.root, 'persistent.shadow');
  const persistentShadowMount = join(options.root, 'mount-shadow');
  const persistentClone = join(options.root, options.format === 'ASIF' ? 'persistent.asif' : 'persistent.sparseimage');
  const persistentCloneMount = join(options.root, 'mount-clone');
  await mkdir(persistentShadowMount, { recursive: true });
  await mkdir(persistentCloneMount, { recursive: true });
  await $`/bin/cp -c ${base} ${persistentClone}`.quiet();

  if (useShadow) {
    await attachShadow(base, persistentShadow, persistentShadowMount);
    await detach(persistentShadowMount);
  }
  await attachImage(persistentClone, persistentCloneMount);
  await detach(persistentCloneMount);

  warmReattach = await runStatisticalSuite(
    'Existing session reattach (Tinybench)',
    warmReattachTasks(
      base,
      persistentShadow,
      persistentShadowMount,
      persistentClone,
      persistentCloneMount,
      options.format,
    ),
    options.iterations,
  );

  if (useShadow) await attachShadow(base, persistentShadow, persistentShadowMount);
  await attachImage(persistentClone, persistentCloneMount);
  operations = await runStatisticalSuite(
    'Mounted workspace operations (Tinybench)',
    mountedOperationTasks(persistentShadowMount, persistentCloneMount, options),
    options.iterations,
  );
  await detach(persistentCloneMount);
  if (useShadow) await detach(persistentShadowMount);

  const statTargets = useShadow ? [base, persistentShadow, persistentClone] : [base, persistentClone];
  const imageStats = await $`/usr/bin/stat -f ${'%N size=%z blocks=%b inode=%i'} ${statTargets}`.text();
  console.log('\nHost image files');
  console.log(imageStats.trim());

  const result = {
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    methodology: {
      library: 'tinybench',
      libraryVersion: '6.0.2',
      timer: 'bunNanoseconds',
      warmup: false,
      rationale:
        'Each sample is a heavyweight filesystem lifecycle or I/O operation; setup and teardown hooks restore comparable state.',
      cacheControl:
        'Sequential I/O uses Darwin dd direct flags (F_NOCACHE); mount tests do not purge global filesystem caches.',
    },
    host: { bun: Bun.version, arch: process.arch, platform: process.platform },
    options,
    setupTimings,
    benchmarks: { freshSessions, warmReattach, operations },
    imageStats: imageStats.trim().split('\n'),
  };
  const resultPath = join(resultsDirectory, `${startedAt.toISOString().replaceAll(':', '-')}.json`);
  await Bun.write(resultPath, `${JSON.stringify(result, null, 2)}\n`);
  await Bun.write(join(resultsDirectory, 'latest.json'), `${JSON.stringify(result, null, 2)}\n`);
  console.log(`\nSaved ${resultPath}`);
} finally {
  for (const mountpoint of [...mounted].reverse()) {
    await $`hdiutil detach -quiet ${mountpoint}`.nothrow().quiet();
  }
  mounted.clear();
  if (!options.keep) {
    await rm(options.root, { recursive: true, force: true });
  } else {
    console.log(`Kept benchmark data at ${options.root}`);
  }
}
