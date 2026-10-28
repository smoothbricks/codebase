/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Assertions cover literal GitHub Actions expressions. */

import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { githubActionPins } from '@smoothbricks/nx-plugin/managed-files/ci-workflow';
import { formatManagedContent } from '@smoothbricks/nx-plugin/managed-files/managed-format';
import {
  definePublishWorkflow,
  type PublishWorkflowBump,
  type PublishWorkflowCallbacks,
  type PublishWorkflowDefinitionOptions,
  type PublishWorkflowNxTarget,
  type PublishWorkflowVersionOutputs,
  type ReleaseVersionMode,
  renderPublishWorkflowYaml,
  runPublishWorkflow,
} from '@smoothbricks/nx-plugin/managed-files/publish-workflow';
import {
  LINUX_PLATFORM_TARGET_GLOBS,
  MACOS_PLATFORM_TARGET_GLOBS,
  PLATFORM_TARGET_GLOBS,
} from '@smoothbricks/nx-plugin/workspace-config-policy';
import typia from 'typia';

const nixosRunsOn = ['nixos-latest-x64', 'self-hosted'] as const;
const REPO_ROOT = join(import.meta.dir, '..', '..', '..', '..', '..');
const PUBLISH_WORKFLOW_TARGET = '.github/workflows/publish.yml';
/**
 * What actually lands on disk: the renderer composes structure, the managed
 * writer formats it with the consuming repository's own Prettier config.
 * Structural assertions below run on these bytes, not the raw template, so a
 * hand-folded line Prettier would join (or a joined line it would fold) does
 * not read as a missing step.
 */
const formattedPublishWorkflow = (rendered: string): Promise<string> =>
  formatManagedContent(REPO_ROOT, PUBLISH_WORKFLOW_TARGET, rendered);

/**
 * What `smoo monorepo` derives from this repository's own Nx graph: macOS and
 * Linux platform families (no iOS targets exist here) built for two
 * architectures. The checked-in workflow copy is rendered from exactly these.
 */
const codebaseWorkflowOptions: PublishWorkflowDefinitionOptions = {
  repoName: '@smoothbricks/codebase',
  platformTargetGlobs: ['*-macos', '*-linux'],
  macosPlatformArchitectures: ['arm64', 'x64'],
  runsOn: [...nixosRunsOn],
};

type ProducerStep = {
  name: string;
  uses?: string;
  env?: Record<string, string>;
  run?: string;
  'working-directory'?: string;
  with?: { 'persist-credentials'?: boolean };
};
type ProducerJob = { env?: Record<string, string>; steps: ProducerStep[] };
type ProducerWorkflow = { jobs: Record<string, ProducerJob> };
/** What the preload probe reports from inside the process that ran it; `bun` is null in a real Node. */
type NodePreloadReport = { pid: number; preloadedBy: number | null; bun: string | null };
/** One real Node process per distinct env a job renders, with the steps that render it. */
type NodeConsumer = { env: Record<string, string>; steps: string[] };

const PRODUCER_PREFLIGHT_NAME = 'Check cross-platform toolchain prerequisites';
const PRODUCER_PREFLIGHT = [
  'printf "%s" "$SDK_ROOT" > preflight-saw-sdk',
  'printf "%s" "$MULTILINE" > preflight-saw-multiline',
  'printf "%s\\n%s\\n%s" "$name" "$delimiter" "$__SMOO_ENV_DELIMITER" > preflight-saw-names',
].join('\n');

/** What generated steps declare for themselves under names a repository's producer also declares. */
const OWN_ENV: Record<string, string> = {
  SMOO_RELEASE_PROJECTS: '${{ inputs.projects }}',
  PUBLISH_ENV: '${{ secrets.PUBLISH_ENV }}',
  SOURCE_READ_TOKEN: '${{ secrets.SOURCE_READ_TOKEN }}',
  SIBLING_READ_TOKEN: '${{ secrets.SIBLING_READ_TOKEN }}',
};

/**
 * Values a repository may legitimately declare, plus text a shell would execute if it ever saw it as source.
 *
 * NODE_OPTIONS is why the env rides every step and not GITHUB_ENV. The workflow-commands documentation ("Setting an
 * environment variable") says: "Due to security restrictions, GITHUB_ENV cannot be used to set the NODE_OPTIONS
 * environment variable." So only a step's own env can put a preload into that step's processes. The quotes around its
 * path, which contains a space, are how Node itself spells a path it must not split. The names after it are also
 * declared by generated steps for themselves, and those steps keep their own value.
 */
const PRODUCER_ENV: Record<string, string> = {
  SDK_ROOT: '${{ runner.temp }}/macos sdk',
  CROSS_FLAGS: '--sysroot "$SDK_ROOT"; touch hostile-marker; $(touch hostile-subst) `touch hostile-tick`',
  MULTILINE: 'first\nghost<<EOF\nlast',
  EMPTY: '',
  APP_CROSS: '1',
  TRUE: 'upper true identifier',
  FALSE: 'upper false identifier',
  NULL: 'upper null identifier',
  // Names that resemble the retired GITHUB_ENV transfer's scratch variables: plain variables now, kept as declared.
  name: 'original name',
  delimiter: 'original delimiter',
  __SMOO_ENV_DELIMITER: 'former scratch name',
  __SMOO_ENV_DELIMITER_: 'another former scratch name',
  NODE_OPTIONS: '--require "${{ runner.temp }}/node preload/probe.cjs"',
  SMOO_RELEASE_PROJECTS: 'producer-chosen projects',
  PUBLISH_ENV: 'producer-chosen credential',
  SOURCE_READ_TOKEN: 'producer-chosen source token',
  SIBLING_READ_TOKEN: 'producer-chosen sibling token',
};

/** A real `--require` preload: it marks the process it runs in and leaves a file no output parsing could forge. */
const NODE_PRELOAD_PROBE = [
  "const { writeFileSync } = require('node:fs');",
  "const { join } = require('node:path');",
  'globalThis.producerPreloadedBy = process.pid;',
  "writeFileSync(join(__dirname, 'witness-' + process.pid), String(process.pid));",
].join('\n');
/** The downstream consumer: a plain process reporting its pid, its runtime, and who, if anyone, preloaded into it. */
const NODE_PRELOAD_CONSUMER = [
  'const report = { pid: process.pid, preloadedBy: globalThis.producerPreloadedBy ?? null };',
  'process.stdout.write(JSON.stringify({ ...report, bun: process.versions.bun ?? null }));',
].join('\n');

const CARGO_GIT_CREDENTIALS = {
  gitOrigins: [{ origin: 'https://git.example.net', tokenEnv: 'SOURCE_READ_TOKEN' }],
};
const SIBLING_SOURCE_CHECKOUTS = [
  { path: '../shared', repository: 'https://git.example.net/org/shared.git', tokenEnv: 'SIBLING_READ_TOKEN' },
];

type ProducerShape = {
  shape: string;
  /** The job that owns the preflight, and with it the producer env. */
  job: string;
  /** Every other job the shape renders: none of them belongs to the producer. */
  unrelatedJobs: string[];
  /** The steps of `job` that declare each OWN_ENV name for themselves; a name no step declares is absent. */
  owners: Record<string, string[]>;
  options: PublishWorkflowDefinitionOptions;
};

const RELEASE_OWNERS: Record<string, string[]> = {
  SMOO_RELEASE_PROJECTS: ['🧭 Plan release', '🔢 Version release'],
  PUBLISH_ENV: ['🧯 Repair pending releases', '📦 Publish release (${{ steps.version.outputs.mode }})'],
};

/** The flattened single job with Apple targets cross-built on its own runner, beside steps with their own env. */
const flattenedShape: ProducerShape = {
  shape: 'flattened single job',
  job: 'publish',
  unrelatedJobs: [],
  owners: {
    ...RELEASE_OWNERS,
    SOURCE_READ_TOKEN: ['Prepare Cargo credentials'],
    SIBLING_READ_TOKEN: ['📦 Check out sibling sources'],
  },
  options: {
    repoName: 'acme/app',
    actionsProvider: 'forgejo',
    runsOn: ['nixos-latest-x64', 'self-hosted'],
    platformTargetGlobs: ['*-macos', '*-linux'],
    macosPlatformArchitectures: ['arm64'],
    platformProducer: { kind: 'linux-cross', preflight: PRODUCER_PREFLIGHT, env: PRODUCER_ENV },
    privateNpm: { scope: '@acme', readTokenEnv: 'READ_ENV', publishTokenEnv: 'PUBLISH_ENV' },
    // Steps that already own an env mapping: the producer env merges into it rather than repeating a key or a header.
    cargoCredentials: CARGO_GIT_CREDENTIALS,
    sourceCheckouts: SIBLING_SOURCE_CHECKOUTS,
  },
};
const emptyFamiliesShape: ProducerShape = {
  shape: 'flattened without platform families',
  job: 'publish',
  unrelatedJobs: [],
  owners: RELEASE_OWNERS,
  options: {
    repoName: 'acme/app',
    actionsProvider: 'forgejo',
    platformTargetGlobs: [],
    platformProducer: { kind: 'linux-cross', preflight: PRODUCER_PREFLIGHT, env: PRODUCER_ENV },
    privateNpm: { scope: '@acme', readTokenEnv: 'READ_ENV', publishTokenEnv: 'PUBLISH_ENV' },
  },
};
const deployOnlyShape: ProducerShape = {
  shape: 'flattened deploy-only job',
  job: 'publish',
  unrelatedJobs: [],
  // Nothing versions or publishes here, so no step declares any of those names: the producer's values reach every step.
  owners: {},
  options: {
    repoName: 'acme/app',
    release: false,
    deploy: true,
    actionsProvider: 'forgejo',
    platformTargetGlobs: [],
    platformProducer: { kind: 'linux-cross', preflight: PRODUCER_PREFLIGHT, env: PRODUCER_ENV },
    privateNpm: { scope: '@acme', readTokenEnv: 'READ_ENV', publishTokenEnv: 'PUBLISH_ENV' },
  },
};
/** The producer is one of three jobs: the Linux candidate before it and the final publisher after it are not its. */
const dedicatedShape: ProducerShape = {
  shape: 'dedicated cross producer job',
  job: 'cross-platform',
  unrelatedJobs: ['linux-release-candidate', 'publish-on-linux'],
  // The publisher credential lives in the final job, which the producer does not own: only the version steps declare.
  owners: { SMOO_RELEASE_PROJECTS: ['🔢 Version release', '🍎 Build selected macOS and iOS release outputs'] },
  options: {
    repoName: '@smoothbricks/codebase',
    platformTargetGlobs: ['*-macos', '*-linux'],
    macosPlatformArchitectures: ['arm64'],
    platformProducer: { kind: 'linux-cross', preflight: PRODUCER_PREFLIGHT, env: PRODUCER_ENV },
    // GitHub provider, so the final job publishes and its publish step declares PUBLISH_ENV for itself.
    privateNpm: { scope: '@acme', readTokenEnv: 'READ_ENV', publishTokenEnv: 'PUBLISH_ENV' },
  },
};
const producerShapes: ProducerShape[] = [flattenedShape, emptyFamiliesShape, deployOnlyShape, dedicatedShape];

/** The shape's own declaration with the given env, and no `env` key at all when there is none. */
function withProducerEnv(
  options: PublishWorkflowDefinitionOptions,
  env?: Record<string, string>,
): PublishWorkflowDefinitionOptions {
  const { platformProducer } = options;
  if (platformProducer === undefined) throw new Error('A producer shape declares platformProducer.');
  const { kind, preflight } = platformProducer;
  return { ...options, platformProducer: { kind, preflight, ...(env === undefined ? {} : { env }) } };
}

function jobSteps(workflow: ProducerWorkflow, job: string): ProducerStep[] {
  const steps = workflow.jobs[job]?.steps;
  if (steps === undefined) throw new Error(`The workflow renders no ${job} job.`);
  return steps;
}

/** One job's raw lines under `jobs:`, from its key to the next job's key. */
function jobSection(rendered: string, job: string): string {
  const lines = rendered.split('\n');
  const start = lines.indexOf(`  ${job}:`);
  if (start === -1) throw new Error(`The workflow renders no ${job} job.`);
  const next = lines.findIndex((line, index) => index > start && /^ {2}[A-Za-z0-9_-]+:$/.test(line));
  return lines.slice(start, next === -1 ? undefined : next).join('\n');
}

/** A top-level step's raw lines with the `# Step N` anchor GitHub deeplinks resolve to. */
type StepBlock = { anchor: number | undefined; name: string; lines: string[] };

function stepBlocks(job: string): StepBlock[] {
  const blocks: StepBlock[] = [];
  let anchor: number | undefined;
  for (const line of job.split('\n')) {
    const anchored = /^ {6}# Step (\d+)/.exec(line)?.[1];
    if (anchored !== undefined) anchor = Number(anchored);
    const name = /^ {6}- name: (.*)$/.exec(line)?.[1];
    if (name !== undefined) {
      blocks.push({ anchor, name, lines: [line] });
      anchor = undefined;
    } else {
      blocks[blocks.length - 1]?.lines.push(line);
    }
  }
  return blocks;
}

/** The keys of every `env:` mapping in one step's raw lines, one list per mapping. */
function envMappingKeys(lines: readonly string[]): string[][] {
  const mappings: string[][] = [];
  let open: string[] | undefined;
  for (const line of lines) {
    if (line === '        env:') {
      open = [];
      mappings.push(open);
    } else if (open !== undefined) {
      const key = /^ {10}"?([A-Za-z_][A-Za-z0-9_]*)"?:/.exec(line)?.[1];
      if (key !== undefined) {
        open.push(key);
      } else if (/^ {0,9}\S/.test(line)) {
        open = undefined;
      }
    }
  }
  return mappings;
}

/** The first line of `expected` that `actual` does not contain after the lines already matched, if any. */
function firstLineMissingInOrder(expected: readonly string[], actual: readonly string[]): string | undefined {
  let from = 0;
  for (const line of expected) {
    const found = actual.indexOf(line, from);
    if (found === -1) return line;
    from = found + 1;
  }
  return undefined;
}

/** The diagnostic a render raises when it must fail. */
function renderFailure(options: PublishWorkflowDefinitionOptions): string {
  try {
    renderPublishWorkflowYaml(options);
  } catch (error) {
    if (error instanceof Error) return error.message;
    throw error;
  }
  throw new Error('The workflow rendered although its producer env cannot be carried.');
}

/** Runs the preflight's script in a real shell under the env its own step renders, as the runner would. */
async function runPreflight(shape: string, preflight: ProducerStep | undefined): Promise<void> {
  const directory = await mkdtemp(join(tmpdir(), 'producer-preflight-'));
  try {
    const githubEnv = join(directory, 'github-env');
    const script = join(directory, 'step.sh');
    await writeFile(githubEnv, '');
    await writeFile(script, preflight?.run ?? '');
    // The runner resolves `runner.temp` before the shell starts, and gives every step a GITHUB_ENV file.
    const stepEnv = Object.fromEntries(
      Object.entries(preflight?.env ?? {}).map(([name, value]) => [
        name,
        value.replaceAll('${{ runner.temp }}', directory),
      ]),
    );
    const proc = Bun.spawn(['bash', '-e', script], {
      cwd: directory,
      env: { PATH: process.env.PATH ?? '', ...stepEnv, GITHUB_ENV: githubEnv },
      stdout: 'ignore',
      stderr: 'pipe',
    });
    const [stderr, exitCode] = await Promise.all([new Response(proc.stderr).text(), proc.exited]);
    expect({ shape, stderr, exitCode }).toEqual({ shape, stderr: '', exitCode: 0 });

    // The runner would hand whatever the step appended there to every later step: the producer appended nothing.
    await expect(readFile(githubEnv, 'utf8')).resolves.toBe('');
    // The preflight saw the declared values through the env of its own step.
    await expect(readFile(join(directory, 'preflight-saw-sdk'), 'utf8')).resolves.toBe(`${directory}/macos sdk`);
    await expect(readFile(join(directory, 'preflight-saw-multiline'), 'utf8')).resolves.toBe('first\nghost<<EOF\nlast');
    await expect(readFile(join(directory, 'preflight-saw-names'), 'utf8')).resolves.toBe(
      'original name\noriginal delimiter\nformer scratch name',
    );
    // Hostile values were data end to end: nothing executed them.
    expect((await readdir(directory)).sort()).toEqual([
      'github-env',
      'preflight-saw-multiline',
      'preflight-saw-names',
      'preflight-saw-sdk',
      'step.sh',
    ]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

/** Spawns a real Node under every distinct env the steps render and proves the NODE_OPTIONS preload ran in each. */
async function runNodeConsumers(shape: string, steps: ProducerStep[]): Promise<void> {
  const node = Bun.which('node');
  if (node === null) throw new Error('This proof needs a real node on PATH.');
  const runnerTemp = await mkdtemp(join(tmpdir(), 'producer-node-'));
  try {
    const preloads = join(runnerTemp, 'node preload');
    await mkdir(preloads);
    await writeFile(join(preloads, 'probe.cjs'), NODE_PRELOAD_PROBE);

    // The Node command, cwd and probe are fixed; run each distinct rendered step env through that same boundary.
    const consumers = new Map<string, NodeConsumer>();
    for (const step of steps) {
      // The runner resolves `runner.temp`; every other expression stays text, as nothing here evaluates one.
      const env = Object.fromEntries(
        Object.entries(step.env ?? {}).map(([name, value]) => [
          name,
          value.replaceAll('${{ runner.temp }}', runnerTemp),
        ]),
      );
      const key = JSON.stringify(Object.entries(env).sort(([a], [b]) => a.localeCompare(b)));
      const consumer: NodeConsumer = consumers.get(key) ?? { env, steps: [] };
      consumer.steps.push(step.name);
      consumers.set(key, consumer);
    }

    for (const { env, steps: consumerSteps } of consumers.values()) {
      // The child gets exactly the env its rendered step declares: nothing of the producer's own configuration.
      const proc = Bun.spawn([node, '-e', NODE_PRELOAD_CONSUMER], {
        cwd: runnerTemp,
        env,
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]);
      const who = { shape, steps: consumerSteps };
      expect({ ...who, stderr, exitCode }).toEqual({ ...who, stderr: '', exitCode: 0 });

      // A real Node, not a shim to Bun: the preload ran inside the consumer's own process, and left a file no output
      // parsing could forge.
      const report = typia.assert<NodePreloadReport>(JSON.parse(stdout));
      expect({ ...who, bun: report.bun, preloadedBy: report.preloadedBy }).toEqual({
        ...who,
        bun: null,
        preloadedBy: report.pid,
      });
      await expect(readFile(join(preloads, `witness-${report.pid}`), 'utf8')).resolves.toBe(String(report.pid));
    }
  } finally {
    await rm(runnerTemp, { recursive: true, force: true });
  }
}

describe('publish workflow definition', () => {
  it('renders the checked-in local publish workflow copy', async () => {
    const rendered = renderPublishWorkflowYaml(codebaseWorkflowOptions);
    // validate builds and pack-checks the release's candidates, not every project:
    // unscoped it compiled binaries the release did not contain.
    expect(rendered).toContain('smoo monorepo validate --projects "${{ steps.version.outputs.projects }}"');
    const packageRoot = join(import.meta.dir, '..', '..', '..');
    await expect(readFile(join(packageRoot, '..', '..', '.github/workflows/publish.yml'), 'utf8')).resolves.toBe(
      await formattedPublishWorkflow(rendered),
    );
  });

  it('writes workflow bytes the repository formatter leaves alone on a second pass', async () => {
    const formatted = await formattedPublishWorkflow(renderPublishWorkflowYaml(codebaseWorkflowOptions));

    // The bytes on disk are a fixed point of the consuming repository's own
    // formatter: `update` writes them once, the commit hook agrees, and
    // `check` afterwards reports only drift that is real.
    await expect(formattedPublishWorkflow(formatted)).resolves.toBe(formatted);
  });

  it('gives every publish job the declared remote cache, in bytes Prettier keeps', async () => {
    const remoteCache = {
      server: 'https://nx-cache.example.net',
      internalServer: 'http://10.89.0.1:8765',
      tokenSecret: 'NX_REMOTE_CACHE_TOKEN',
    };
    const cacheEnv = {
      NX_SELF_HOSTED_REMOTE_CACHE_SERVER: 'http://10.89.0.1:8765',
      NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN: '${{ secrets.NX_REMOTE_CACHE_TOKEN }}',
    };
    const platform = await formattedPublishWorkflow(
      renderPublishWorkflowYaml({ ...codebaseWorkflowOptions, remoteCache }),
    );
    const single = await formattedPublishWorkflow(
      renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase', remoteCache }),
    );

    expect(Bun.YAML.parse(platform)).toMatchObject({
      jobs: {
        'linux-release-candidate': { env: cacheEnv },
        'macos-platform': { env: cacheEnv },
        'publish-on-linux': { env: cacheEnv },
      },
    });
    expect(Bun.YAML.parse(single)).toMatchObject({ jobs: { publish: { env: cacheEnv } } });
    expect(renderPublishWorkflowYaml(codebaseWorkflowOptions)).not.toContain('NX_SELF_HOSTED');
    // A workflow the repository Prettier config would rewrite drifts on the
    // first commit hook, so the cache lines must already be its output.
    await expect(formattedPublishWorkflow(platform)).resolves.toBe(platform);
  });

  it('pins Rust span provenance in every publishing job', async () => {
    const provenanceEnv = { LMAO_GIT_REVISION: '${{ github.sha }}' };
    const platform = await formattedPublishWorkflow(renderPublishWorkflowYaml(codebaseWorkflowOptions));
    const single = await formattedPublishWorkflow(renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' }));
    expect(Bun.YAML.parse(platform)).toMatchObject({
      jobs: {
        'linux-release-candidate': { env: provenanceEnv },
        'macos-platform': { env: provenanceEnv },
        'publish-on-linux': { env: provenanceEnv },
      },
    });
    expect(Bun.YAML.parse(single)).toMatchObject({ jobs: { publish: { env: provenanceEnv } } });
  });

  it('passes the projects selector to the version and platform-output steps', async () => {
    const rendered = renderPublishWorkflowYaml(codebaseWorkflowOptions);

    expect(foldedRunCommand(rendered, '🔢 Version release')).toBe(
      'smoo release version --bump "${{ inputs.bump }}" --projects "$SMOO_RELEASE_PROJECTS" --dry-run "${{ inputs.dry_run }}" --github-output "$GITHUB_OUTPUT"',
    );
    expect(foldedRunCommand(rendered, '🍎 Build selected macOS and iOS release outputs')).toContain(
      '--projects "$SMOO_RELEASE_PROJECTS"',
    );
    expect(rendered).toContain("default: ''");
  });

  it('builds what ships with the release configuration and checks the dev build', () => {
    const shapes = [
      renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' }),
      renderPublishWorkflowYaml({
        repoName: '@smoothbricks/codebase',
        platformTargetGlobs: LINUX_PLATFORM_TARGET_GLOBS,
      }),
      renderPublishWorkflowYaml(codebaseWorkflowOptions),
      // One job that also cross-builds the Apple legs on its own runner.
      renderPublishWorkflowYaml({
        repoName: 'acme/app',
        actionsProvider: 'forgejo',
        runsOn: ['nixos-latest-x64', 'self-hosted'],
        platformTargetGlobs: ['*-macos', '*-linux'],
        macosPlatformArchitectures: ['arm64'],
        platformProducer: { kind: 'linux-cross', preflight: 'sh prepare.sh' },
        privateNpm: { scope: '@acme', readTokenEnv: 'READ_ENV', publishTokenEnv: 'PUBLISH_ENV' },
      }),
    ];
    for (const rendered of shapes) {
      const workflow = typia.assert<{ jobs: Record<string, { steps: Array<{ run?: string }> }> }>(
        Bun.YAML.parse(rendered),
      );
      const runs = Object.values(workflow.jobs)
        .flatMap((job) => job.steps)
        .flatMap((step) => (step.run?.includes('smoo github-ci nx-run-many') ? [step.run] : []));
      const isCheck = (run: string): boolean => /--targets (lint|test) /.test(run);
      const builds = runs.filter((run) => !isCheck(run));
      const checks = runs.filter(isCheck);
      expect(builds.length).toBeGreaterThan(0);
      expect(checks.length).toBeGreaterThan(0);
      // The build aggregate and every platform leg ship; lint and unit tests
      // check the same dev build every local and CI run uses.
      for (const run of builds) {
        expect(run).toContain('--configuration production');
      }
      for (const run of checks) {
        expect(run).not.toContain('--configuration');
      }
    }
  });

  it('a private forge release with cross-built Apple targets publishes from one job', () => {
    const rendered = renderPublishWorkflowYaml({
      repoName: 'acme/app',
      actionsProvider: 'forgejo',
      runsOn: ['nixos-latest-x64', 'self-hosted'],
      platformTargetGlobs: ['*-macos', '*-linux'],
      macosPlatformArchitectures: ['arm64'],
      platformProducer: { kind: 'linux-cross', preflight: 'sh scripts/prepare-macos-sdk.sh', env: { APP_CROSS: '1' } },
      privateNpm: { scope: '@acme', readTokenEnv: 'READ_ENV', publishTokenEnv: 'PUBLISH_ENV' },
    });

    // One job: no producer job to hand outputs over from, so no transfer at all.
    const jobsYaml = rendered.slice(rendered.indexOf('jobs:\n') + 'jobs:\n'.length);
    expect(jobsYaml.match(/^ {2}[a-z][a-z0-9-]*:$/gm)).toEqual(['  publish:']);
    // The hand-off steps specifically; the failure-only trace-DB upload stays.
    expect(rendered).not.toContain('Upload validated release state');
    expect(rendered).not.toContain('Upload validated build outputs');
    expect(rendered).not.toContain('Upload macOS platform outputs');
    expect(rendered).not.toContain('Download candidate artifacts');
    expect(rendered).not.toContain('git bundle');
    // It still builds the Apple targets, with the producer's toolchain env and
    // its preflight ahead of setup.
    expect(rendered).toContain('--targets "*-macos"');
    expect(rendered).not.toMatch(/^ {6}"?APP_CROSS"?:/m);
    expect(rendered).toMatch(/^ {10}"APP_CROSS": ['"]1['"]$/m);
    expect(rendered).toContain('Check cross-platform toolchain prerequisites');
    expect(rendered.indexOf('Check cross-platform toolchain prerequisites')).toBeLessThan(
      rendered.indexOf('Setup Nix/devenv'),
    );
  });

  it('runs and numbers a declared flattened producer even when no platform family matches', () => {
    for (const platformTargetGlobs of [[], ['*-linux']]) {
      const rendered = renderPublishWorkflowYaml({
        repoName: 'acme/app',
        actionsProvider: 'forgejo',
        platformTargetGlobs,
        privateNpm: { scope: '@acme', readTokenEnv: 'READ_ENV', publishTokenEnv: 'PUBLISH_ENV' },
        platformProducer: { kind: 'linux-cross', preflight: 'sh scripts/prepare.sh', env: { APP_CROSS: '1' } },
      });
      const workflow = typia.assert<ProducerWorkflow>(Bun.YAML.parse(rendered));
      const steps = workflow.jobs.publish?.steps ?? [];
      expect(rendered).toContain('# Step 3\n      - name: Check cross-platform toolchain prerequisites');
      expect(rendered).toContain('# Step 4. Composite action internals');
      expect(stepAnchorNumbers(rendered)).toEqual(Array.from({ length: steps.length + 1 }, (_, index) => index + 1));
      expect(rendered).toMatch(/^ {10}"APP_CROSS": ['"]1['"]$/m);
      expect(rendered).not.toMatch(/^ {6}"?APP_CROSS"?:/m);
      expect(rendered.indexOf('Check cross-platform toolchain prerequisites')).toBeLessThan(
        rendered.indexOf('Setup Nix/devenv'),
      );
      expect(rendered).toContain('sh scripts/prepare.sh');
      if (platformTargetGlobs.length === 0) expect(rendered).not.toContain('Build supplemental Linux targets');
    }
  });

  it('keeps the publishing hand-off when provenance needs the hosted runner', () => {
    const publicWithMac = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: ['*-macos', '*-linux'],
      macosPlatformArchitectures: ['arm64'],
      platformProducer: { kind: 'linux-cross', preflight: 'sh prepare.sh' },
    });
    const forgePublicWithMac = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      actionsProvider: 'forgejo',
      runsOn: ['nixos-latest-x64', 'self-hosted'],
      platformTargetGlobs: ['*-macos', '*-linux'],
      macosPlatformArchitectures: ['arm64'],
      platformProducer: { kind: 'linux-cross', preflight: 'sh prepare.sh' },
    });

    // No private registry declared: packages go to npmjs, which mints
    // provenance from the publishing job and refuses a self-hosted runner.
    for (const rendered of [publicWithMac, forgePublicWithMac]) {
      expect(rendered).toContain('publish-on-linux:');
      expect(rendered).toContain('Download candidate artifacts');
    }
  });

  it('preserves the single Ubuntu job and renders no artifact transfer when no Apple targets exist', () => {
    const current = renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' });
    const explicitlyEmpty = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: [],
    });
    const linuxOnly = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: LINUX_PLATFORM_TARGET_GLOBS,
    });

    expect(explicitlyEmpty).toBe(current);
    for (const rendered of [current, linuxOnly]) {
      const jobsYaml = rendered.slice(rendered.indexOf('jobs:\n') + 'jobs:\n'.length);
      expect(jobsYaml).toContain('  publish:\n    runs-on: ubuntu-latest');
      expect(jobsYaml.match(/^ {2}[a-z][a-z-]+:\n/gm)).toHaveLength(1);
      expect(rendered).not.toContain('linux-release-candidate:');
      expect(rendered).not.toContain('macos-platform:');
      expect(rendered).not.toContain('publish-on-linux:');
      expect(rendered).not.toContain('--collect-outputs');
      expect(rendered).not.toContain('Upload validated release state');
      expect(rendered).not.toContain('Upload validated build outputs');
      expect(rendered).not.toContain('Upload supplemental Linux outputs');
      expect(rendered).not.toContain('Upload macOS platform outputs');
      expect(rendered).not.toContain('actions/download-artifact');
      expect(rendered).not.toContain('apply-outputs');
      expect(rendered).not.toContain('git bundle');
    }
  });

  it('keeps job-local deeplink step anchors exact for single-job and parallel platform workflows', () => {
    const singleJob = renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' });
    const platform = renderPublishWorkflowYaml({
      deploy: true,
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    });
    const linuxCandidate = platform.slice(
      platform.indexOf('  linux-release-candidate:'),
      platform.indexOf('  macos-platform:'),
    );
    const macosPlatform = platform.slice(
      platform.indexOf('  macos-platform:'),
      platform.indexOf('  publish-on-linux:'),
    );
    const finalJob = platform.slice(platform.indexOf('  publish-on-linux:'));

    expect(stepAnchorNumbers(singleJob)).toEqual(Array.from({ length: 17 }, (_, index) => index + 1));
    expect(stepAnchorNumbers(linuxCandidate)).toEqual(Array.from({ length: 20 }, (_, index) => index + 1));
    expect(stepAnchorNumbers(macosPlatform)).toEqual(Array.from({ length: 10 }, (_, index) => index + 1));
    expect(stepAnchorNumbers(finalJob)).toEqual(Array.from({ length: 14 }, (_, index) => index + 1));
    expect(singleJob).toContain('# Step 15\n      - name: 🏷️ Tag release');
    expect(singleJob).toContain('# Step 16\n      - name: 📦 Publish release (${{ steps.version.outputs.mode }})');
    expect(singleJob).toContain('# Step 17\n      - name: 🧹 Cleanup and cache Nix/devenv');
    expect(linuxCandidate).toContain('# Step 11\n      - name: 🔒 Capture candidate release SHA');
    expect(linuxCandidate).toContain(
      '# Step 7\n      - name: ✅ Check managed monorepo files (${{ steps.plan.outputs.mode }})',
    );
    expect(linuxCandidate).toContain('# Step 20\n      - name: 🧹 Cleanup and cache Nix/devenv');
    expect(macosPlatform).toContain('# Step 6\n      - name: 🔢 Version release');
    expect(macosPlatform).toContain('# Step 7\n      - name: 🍎 Build selected macOS and iOS release outputs');
    expect(macosPlatform).toContain('# Step 10\n      - name: 🧹 Cleanup and cache Nix/devenv');
    expect(finalJob).toContain('# Step 3\n      - name: 🧱 Setup Nix/devenv');
    expect(finalJob).toContain('# Step 4\n      - name: 📥 Download candidate artifacts');
    expect(finalJob).toContain('# Step 6\n      - name: 🏗️ Build smoo Nx version actions');
    expect(finalJob).toContain('# Step 7\n      - name: 🧯 Repair pending releases');
    expect(finalJob).toContain('# Step 8\n      - name: ♻️ Restore validated release state');
    expect(finalJob).toContain('# Step 9\n      - name: 📦 Apply verified Linux outputs');
    expect(finalJob).toContain('# Step 10\n      - name: 🍎 Apply verified macOS outputs');
    expect(finalJob).toContain('# Step 11\n      - name: 🏷️ Tag release');
    expect(finalJob).toContain(
      '# Step 12\n      - name: 📦 Publish release (${{ needs.linux-release-candidate.outputs.mode }})',
    );
    expect(finalJob).toContain('# Step 13\n      - name: 🚀 Deploy production');
    expect(finalJob).toContain('# Step 14\n      - name: 🧹 Cleanup and cache Nix/devenv');
    expect(finalJob).toContain('uses: ./.github/actions/setup-devenv');
    expect(finalJob).toContain('uses: ./.github/actions/save-nix-devenv');
    expect(finalJob).not.toContain('🥟 Install Bun');
    expect(finalJob).not.toContain('🟢 Install Node');
    expect(finalJob).not.toContain('smoo monorepo validate');
  });

  it('bootstraps only the Nx version actions package, and only for the self-hosting repo', () => {
    const platform = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: PLATFORM_TARGET_GLOBS,
      macosPlatformArchitectures: ['arm64', 'x64'],
    });
    const candidates = [
      platform.slice(platform.indexOf('  linux-release-candidate:'), platform.indexOf('  macos-platform:')),
      platform.slice(platform.indexOf('  macos-platform:'), platform.indexOf('  publish-on-linux:')),
    ];
    const finalJob = platform.slice(platform.indexOf('  publish-on-linux:'));
    const bootstrap = '- name: 🏗️ Build smoo Nx version actions';

    // Nx Release loads the version-actions hook from dist through the export map,
    // in its own node process where no source condition applies. Every job that
    // versions must materialize that one package first -- never the whole CLI
    // dependency chain, which is what this step used to build.
    for (const job of [...candidates, finalJob]) {
      expect(job).toContain('        run: bun ../../tooling/ttsc-emit.ts');
      expect(job).not.toContain('run: nx build');
    }
    for (const candidate of candidates) {
      expect(candidate.indexOf(bootstrap)).toBeLessThan(candidate.indexOf('- name: 🔢 Version release'));
    }
    // The publishing job bumps to the next prerelease, which is also an
    // `nx release version` run and therefore also needs the hook on disk.
    expect(finalJob.indexOf(bootstrap)).toBeLessThan(finalJob.indexOf('- name: 📦 Publish release'));
    // Consumers resolve the hook from the published package instead.
    expect(renderPublishWorkflowYaml({ repoName: '@acme/widgets' })).not.toContain('ttsc -p tsconfig.lib.json');
  });

  it('tags only in the publishing job, after repair and before the npm publish', () => {
    const platform = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: PLATFORM_TARGET_GLOBS,
      macosPlatformArchitectures: ['arm64', 'x64'],
    });
    const linuxCandidate = platform.slice(
      platform.indexOf('  linux-release-candidate:'),
      platform.indexOf('  macos-platform:'),
    );
    const macosPlatform = platform.slice(
      platform.indexOf('  macos-platform:'),
      platform.indexOf('  publish-on-linux:'),
    );
    const finalJob = platform.slice(platform.indexOf('  publish-on-linux:'));

    // Candidates build the versioned tree, so versioning stays ahead of Build,
    // but a candidate that fails Build, Lint, or Unit Tests must leave no ref.
    for (const candidate of [linuxCandidate, macosPlatform]) {
      expect(candidate).toContain('smoo release version');
      expect(candidate).not.toContain('smoo release tag');
      expect(candidate).not.toContain('- name: 🏷️ Tag release');
    }
    expect(linuxCandidate.indexOf('- name: 🔢 Version release')).toBeLessThan(
      linuxCandidate.indexOf('- name: 🔨 Build'),
    );

    // Exactly one tag-creating step exists in the whole workflow.
    expect(platform.match(/- name: 🏷️ Tag release/g)).toHaveLength(1);
    expect(platform.match(/smoo release tag /g)).toHaveLength(1);
    expect(finalJob).toContain('        run: smoo release tag --dry-run "${{ inputs.dry_run }}"');
    expect(finalJob.indexOf('- name: 🧯 Repair pending releases')).toBeLessThan(
      finalJob.indexOf('- name: 🏷️ Tag release'),
    );
    expect(finalJob.indexOf('- name: 🏷️ Tag release')).toBeLessThan(finalJob.indexOf('- name: 📦 Publish release'));
  });

  it('keeps the same version-then-tag-then-publish order in the single-job workflow', () => {
    const singleJob = renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' });

    expect(singleJob.match(/- name: 🏷️ Tag release/g)).toHaveLength(1);
    expect(singleJob).toContain('        run: smoo release tag --dry-run "${{ inputs.dry_run }}"');
    expect(singleJob.indexOf('- name: 🧯 Repair pending releases')).toBeLessThan(
      singleJob.indexOf('- name: 🧭 Plan release'),
    );
    // Lint and test hash the source, so they run BEFORE the version commit and
    // reuse the task hashes ci already computed. Build stays after it: an
    // artifact built from pre-bump sources would carry the previous version.
    expect(singleJob.indexOf('- name: 🧭 Plan release')).toBeLessThan(singleJob.indexOf('- name: 🔍 Lint'));
    expect(singleJob.indexOf('- name: 🧪 Unit Tests')).toBeLessThan(singleJob.indexOf('- name: 🔢 Version release'));
    expect(singleJob.indexOf('- name: 🔢 Version release')).toBeLessThan(singleJob.indexOf('- name: 🔨 Build'));
    expect(singleJob.indexOf('- name: 🔨 Build')).toBeLessThan(singleJob.indexOf('- name: 🏷️ Tag release'));
    // The gates select from the plan, not from a version step that has not run.
    expect(singleJob).toContain('--targets lint --projects "${{ steps.plan.outputs.projects }}"');
    expect(singleJob).toContain("if: steps.plan.outputs.mode != 'none'");
    expect(singleJob.indexOf('- name: 🏷️ Tag release')).toBeLessThan(singleJob.indexOf('- name: 📦 Publish release'));
    // The Release section starts at tagging now, not at publishing.
    expect(singleJob).toContain(
      '# --- Release ------------------------------------------------------------\n\n      # Step 15\n      - name: 🏷️ Tag release',
    );
  });

  it('renders parallel native producers and a dependent publish-only final job', async () => {
    const linuxOnly = await formattedPublishWorkflow(
      renderPublishWorkflowYaml({
        repoName: '@smoothbricks/codebase',
        platformTargetGlobs: LINUX_PLATFORM_TARGET_GLOBS,
      }),
    );
    const native = await formattedPublishWorkflow(
      renderPublishWorkflowYaml({
        repoName: '@smoothbricks/codebase',
        platformTargetGlobs: PLATFORM_TARGET_GLOBS,
        runsOn: [...nixosRunsOn],
      }),
    );
    const linuxCandidate = native.slice(
      native.indexOf('  linux-release-candidate:'),
      native.indexOf('  macos-platform:'),
    );
    const macosPlatform = native.slice(native.indexOf('  macos-platform:'), native.indexOf('  publish-on-linux:'));
    const finalJob = native.slice(native.indexOf('  publish-on-linux:'));

    expect(linuxOnly).not.toContain('macos-platform:');
    expect(linuxOnly).toContain(
      `smoo github-ci nx-run-many --targets "${LINUX_PLATFORM_TARGET_GLOBS.join(',')}" --projects`,
    );
    expect(native).toContain('  linux-release-candidate:');
    expect(native).toContain('  macos-platform:\n    runs-on: macos-latest');
    expect(native).toContain('  publish-on-linux:');
    expect(native).toContain('  publish-on-linux:\n    needs: [linux-release-candidate, macos-platform]');
    expect(linuxCandidate).not.toContain('needs:');
    expect(macosPlatform).not.toContain('needs:');
    expect(linuxCandidate).not.toContain('smoo release repair-pending');
    expect(linuxCandidate).toContain('smoo release version');
    expect(linuxCandidate).toContain('smoo github-ci nx-run-many --targets build --projects');
    expect(linuxCandidate).toContain('smoo github-ci nx-run-many --targets lint --projects');
    expect(linuxCandidate).toContain('smoo github-ci nx-run-many --targets test --projects');
    expect(linuxCandidate).toContain(
      `smoo github-ci nx-run-many --targets "${LINUX_PLATFORM_TARGET_GLOBS.join(',')}" --projects`,
    );
    expect(macosPlatform).toContain('smoo release version');
    expect(macosPlatform.indexOf('- name: 🔢 Version release')).toBeLessThan(
      macosPlatform.indexOf('- name: 🍎 Build selected macOS and iOS release outputs'),
    );
    expect(foldedRunCommand(macosPlatform, '🍎 Build selected macOS and iOS release outputs')).toBe(
      `smoo release build-platform-outputs --bump "\${{ inputs.bump }}" --projects "$SMOO_RELEASE_PROJECTS" --ref "\${{ github.sha }}" --targets "${MACOS_PLATFORM_TARGET_GLOBS.join(
        ',',
      )}" --output "\${{ runner.temp }}/macos-platform-outputs" --github-output "$GITHUB_OUTPUT"`,
    );
    expect(macosPlatform).toContain('id: platform-outputs');
    expect(macosPlatform).toContain("if: steps.platform-outputs.outputs.projects != ''");
    expect(macosPlatform).toContain(
      'run: smoo github-ci nx-run-many --targets test --projects "${{ steps.platform-outputs.outputs.projects }}"',
    );
    expect(macosPlatform.indexOf('- name: 🍎 Build selected macOS and iOS release outputs')).toBeLessThan(
      macosPlatform.indexOf('- name: 🧪 Unit test selected macOS and iOS packages'),
    );
    expect(macosPlatform.indexOf('- name: 🧪 Unit test selected macOS and iOS packages')).toBeLessThan(
      macosPlatform.indexOf('- name: 📤 Upload macOS platform outputs'),
    );
    expect(finalJob).not.toContain('smoo release version');
    expect(finalJob).not.toContain('nx-run-many');
    expect(foldedRunCommand(finalJob, '🧯 Repair pending releases')).toBe(
      'smoo release repair-pending --ref "${{ github.sha }}" --platform-outputs ' +
        '"${{ runner.temp }}/publish-artifacts/publish-macos-outputs-${{ github.run_id }}/repairs" ' +
        '--dry-run "${{ inputs.dry_run }}"',
    );
    expect(native).toContain(
      `uses: ${githubActionPins.uploadArtifact.ref} # ${githubActionPins.uploadArtifact.version}`,
    );
    expect(native).toContain(
      `uses: ${githubActionPins.downloadArtifact.ref} # ${githubActionPins.downloadArtifact.version}`,
    );
    expect(native).toContain('name: publish-release-state-${{ github.run_id }}');
    expect(native).toContain('name: publish-release-outputs-${{ github.run_id }}');
    expect(native).toContain('name: publish-linux-outputs-${{ github.run_id }}');
    expect(native).toContain('name: publish-macos-outputs-${{ github.run_id }}');
    expect(native).toContain('git bundle create');
    expect(finalJob).toContain(
      'smoo github-ci apply-outputs --source-sha "${{ needs.linux-release-candidate.outputs.release-sha }}"',
    );
    expect(finalJob).toContain('smoo github-ci apply-outputs --source-sha "${{ github.sha }}"');
    expect(foldedRunCommand(finalJob, '📦 Apply verified Linux outputs')).toBe(
      'smoo github-ci apply-outputs --source-sha "${{ needs.linux-release-candidate.outputs.release-sha }}" ' +
        '"${{ runner.temp }}/publish-artifacts/publish-release-outputs-${{ github.run_id }}" ' +
        '"${{ runner.temp }}/publish-artifacts/publish-linux-outputs-${{ github.run_id }}"',
    );
    expect(foldedRunCommand(finalJob, '🍎 Apply verified macOS outputs')).toBe(
      'smoo github-ci apply-outputs --source-sha "${{ github.sha }}" ' +
        '"${{ runner.temp }}/publish-artifacts/publish-macos-outputs-${{ github.run_id }}/current"',
    );
  });

  it('pins checkouts, scopes persisted credentials, bounds job time, and keeps the projects selector out of run lines', () => {
    const rendered = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: PLATFORM_TARGET_GLOBS,
      runsOn: [...nixosRunsOn],
    });
    const workflow = typia.assert<{
      jobs: Record<
        string,
        {
          'timeout-minutes'?: number;
          steps: Array<{
            name?: string;
            uses?: string;
            env?: Record<string, string>;
            run?: string;
            with?: { 'persist-credentials'?: boolean };
          }>;
        }
      >;
    }>(Bun.YAML.parse(rendered));
    const expectedTimeouts: Record<string, number> = {
      'linux-release-candidate': 45,
      'macos-platform': 90,
      'publish-on-linux': 45,
    };
    for (const [job, minutes] of Object.entries(expectedTimeouts)) {
      expect(workflow.jobs[job]['timeout-minutes']).toBe(minutes);
    }
    const expectedPersist: Record<string, boolean> = {
      // The candidate reads and writes no remote: the clone is complete.
      'linux-release-candidate': false,
      // build-platform-outputs fetches origin's release branch and tags, which a private origin refuses anonymously.
      'macos-platform': true,
      // smoo publish and pending-release repair push release refs.
      'publish-on-linux': true,
    };
    for (const [job, persist] of Object.entries(expectedPersist)) {
      const checkouts = workflow.jobs[job].steps.filter((step) => step.uses?.startsWith('actions/checkout@'));
      expect(checkouts.length).toBe(1);
      for (const step of checkouts) {
        expect(step.uses).toBe(githubActionPins.checkout.ref);
        expect(step.with?.['persist-credentials']).toBe(persist);
      }
    }
    expect(rendered).not.toContain('blob:none');
    expect(rendered).toContain('# No remote is read or written from this job: the clone is complete and the');
    expect(rendered).toContain('# build-platform-outputs fetches the release branch and tags from origin');
    expect(rendered).toContain('# smoo publish and pending-release repair push release refs.');
    const steps = Object.values(workflow.jobs).flatMap((job) => job.steps);
    expect(steps.some((step) => (step.run ?? '').includes('inputs.projects'))).toBe(false);
    const projectEnvSteps = steps.filter((step) => step.env?.SMOO_RELEASE_PROJECTS !== undefined);
    expect(projectEnvSteps.length).toBeGreaterThan(0);
    for (const step of projectEnvSteps) {
      expect(step.env?.SMOO_RELEASE_PROJECTS).toBe('${{ inputs.projects }}');
    }
  });

  it('sizes the single-job publish timeout by platform targets and keeps release:false credentials off', () => {
    const withPlatforms = typia.assert<{
      jobs: {
        publish: {
          'timeout-minutes': number;
          steps: Array<{ uses?: string; with?: { 'persist-credentials'?: boolean } }>;
        };
      };
    }>(
      Bun.YAML.parse(
        renderPublishWorkflowYaml({
          repoName: '@smoothbricks/codebase',
          platformTargetGlobs: PLATFORM_TARGET_GLOBS,
          release: false,
        }),
      ),
    );
    expect(withPlatforms.jobs.publish['timeout-minutes']).toBe(90);
    const checkout = withPlatforms.jobs.publish.steps.find((step) => step.uses?.startsWith('actions/checkout@'));
    expect(checkout?.with?.['persist-credentials']).toBe(false);
    const withoutPlatforms = typia.assert<{ jobs: { publish: { 'timeout-minutes': number } } }>(
      Bun.YAML.parse(renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase', release: false })),
    );
    expect(withoutPlatforms.jobs.publish['timeout-minutes']).toBe(45);
    const singleReleaseJob = typia.assert<{ jobs: { publish: { 'timeout-minutes': number } } }>(
      Bun.YAML.parse(renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' })),
    );
    expect(singleReleaseJob.jobs.publish['timeout-minutes']).toBe(45);
    const singleReleaseWithPlatforms = typia.assert<{ jobs: { publish: { 'timeout-minutes': number } } }>(
      Bun.YAML.parse(
        renderPublishWorkflowYaml({
          repoName: '@smoothbricks/codebase',
          platformTargetGlobs: LINUX_PLATFORM_TARGET_GLOBS,
        }),
      ),
    );
    expect(singleReleaseWithPlatforms.jobs.publish['timeout-minutes']).toBe(90);
  });

  it('keeps every checkout a full clone with an explicit credential decision', () => {
    const decisions: Array<[string, PublishWorkflowDefinitionOptions, boolean]> = [
      ['single release job pushes', { repoName: '@smoothbricks/codebase' }, true],
      [
        'single release job with platforms pushes',
        { repoName: '@smoothbricks/codebase', platformTargetGlobs: LINUX_PLATFORM_TARGET_GLOBS },
        true,
      ],
      ['release:false never pushes', { repoName: '@smoothbricks/codebase', release: false }, false],
    ];
    for (const [shape, options, persist] of decisions) {
      const rendered = renderPublishWorkflowYaml(options);
      expect(rendered).not.toContain('blob:none');
      const workflow = typia.assert<ProducerWorkflow>(Bun.YAML.parse(rendered));
      const checkouts = workflow.jobs.publish?.steps.filter((step) => step.uses?.startsWith('actions/checkout@')) ?? [];
      expect({ shape, checkouts: checkouts.length }).toEqual({ shape, checkouts: 1 });
      expect({ shape, persist: checkouts[0]?.with?.['persist-credentials'] }).toEqual({ shape, persist });
      expect(rendered).toContain('fetch-depth: 0');
    }
  });

  it('walks each producer shape once: the env rides every step of its job, in no other, and into real processes', async () => {
    for (const { shape, job, unrelatedJobs, owners, options } of producerShapes) {
      const rendered = renderPublishWorkflowYaml(options);
      const bare = renderPublishWorkflowYaml(withProducerEnv(options));
      const workflow = typia.assert<ProducerWorkflow>(Bun.YAML.parse(rendered));
      const bareWorkflow = typia.assert<ProducerWorkflow>(Bun.YAML.parse(bare));
      const steps = jobSteps(workflow, job);
      const bareSteps = jobSteps(bareWorkflow, job);
      const section = jobSection(rendered, job);
      const blocks = stepBlocks(section);

      // The declaration adds environment and nothing else: the workflow rendered without it survives line for line,
      // in order, so no command, comment or `# Step N` anchor was rewritten. An empty declaration adds nothing at all.
      const missing = firstLineMissingInOrder(bare.split('\n'), rendered.split('\n'));
      expect({ shape, missing }).toEqual({ shape, missing: undefined });
      expect(renderPublishWorkflowYaml(withProducerEnv(options, {}))).toBe(bare);
      expect(Object.keys(workflow.jobs).sort()).toEqual([job, ...unrelatedJobs].sort());
      expect({ shape, names: steps.map((step) => step.name) }).toEqual({
        shape,
        names: bareSteps.map((step) => step.name),
      });

      // Every emitted step carries every producer name, under whatever the step declares for itself.
      for (const [index, step] of steps.entries()) {
        const env = { ...PRODUCER_ENV, ...bareSteps[index]?.env };
        expect({ shape, step: step.name, env: step.env }).toEqual({ shape, step: step.name, env });
      }

      // A step's own value wins over a colliding producer value, and only there: the steps that render their own
      // expression are exactly the table's owners, and every other step keeps the producer's value.
      for (const [name, own] of Object.entries(OWN_ENV)) {
        const declaring = steps.filter((step) => step.env?.[name] === own).map((step) => step.name);
        expect({ shape, name, declaring }).toEqual({ shape, name, declaring: owners[name] ?? [] });
        for (const step of steps) {
          const who = { shape, name, step: step.name };
          const expected = declaring.includes(step.name) ? own : PRODUCER_ENV[name];
          expect({ ...who, carried: step.env?.[name] }).toEqual({ ...who, carried: expected });
        }
      }
      // Source selectors reach shell only as step env, so a hostile projects input is data.
      expect(steps.some((step) => (step.run ?? '').includes('inputs.projects'))).toBe(false);

      // The runner context exists for steps, not for a job's env, so no job may carry the declaration.
      for (const [name, definition] of Object.entries(workflow.jobs)) {
        const runnerContext = Object.values(definition.env ?? {}).some((value) => value.includes('runner.'));
        expect({ shape, name, runnerContext }).toEqual({ shape, name, runnerContext: false });
        expect({ shape, name, env: definition.env }).toEqual({ shape, name, env: bareWorkflow.jobs[name]?.env });
      }

      // Jobs that are not the producer's render byte for byte as they do without the declaration: none of its names
      // reached a step there, and a name a step declares for itself keeps that step's value. Each declares one, so
      // the producer's colliding value had a place to leak.
      for (const name of unrelatedJobs) {
        expect(jobSection(rendered, name)).toBe(jobSection(bare, name));
        let declaring = 0;
        for (const step of jobSteps(workflow, name)) {
          const who = { shape, job: name, step: step.name };
          const present = Object.keys(PRODUCER_ENV).filter((producerName) => step.env?.[producerName] !== undefined);
          const leaked = present.filter((producerName) => step.env?.[producerName] !== OWN_ENV[producerName]);
          expect({ ...who, leaked }).toEqual({ ...who, leaked: [] });
          declaring += present.length;
        }
        expect({ shape, job: name, declaring: declaring > 0 }).toEqual({ shape, job: name, declaring: true });
      }

      // GitHub rejects a repeated `env` or a repeated key, and the YAML parser here keeps the last without a word,
      // so only the raw lines can show that every step has one mapping with each name once.
      for (const block of blocks) {
        const who = { shape, step: block.name };
        const mappings = envMappingKeys(block.lines);
        const keys = mappings[0] ?? [];
        const repeated = keys.filter((key, index) => keys.indexOf(key) !== index);
        expect({ ...who, mappings: mappings.length, repeated }).toEqual({ ...who, mappings: 1, repeated: [] });
      }

      // `# Step 1` is GitHub's automatic "Set up job"; every emitted step then owns the next number, in order, and
      // the preflight follows the checkout.
      expect({ shape, numbers: stepAnchorNumbers(section) }).toEqual({
        shape,
        numbers: Array.from({ length: steps.length + 1 }, (_, index) => index + 1),
      });
      const anchored = blocks.map(({ anchor, name }) => ({ anchor, name }));
      expect({ shape, anchored }).toEqual({
        shape,
        anchored: steps.map((step, index) => ({ anchor: index + 2, name: step.name })),
      });
      expect({ shape, preflight: anchored[1] }).toEqual({
        shape,
        preflight: { anchor: 3, name: PRODUCER_PREFLIGHT_NAME },
      });

      // The preflight runs ahead of devenv resolving anything. It is the declared command behind at most the
      // shell-strictness line: no export loop, no random delimiter, no scratch assignment, no GITHUB_ENV append.
      const names = steps.map((step) => step.name);
      const preflight = steps[1];
      expect({ shape, uses: steps[0]?.uses?.startsWith('actions/checkout@') }).toEqual({ shape, uses: true });
      expect(names.filter((name) => name === PRODUCER_PREFLIGHT_NAME)).toHaveLength(1);
      expect(rendered.match(/Check cross-platform toolchain prerequisites/g)).toHaveLength(1);
      const devenv = names.findIndex((name) => name?.includes('Setup Nix/devenv'));
      expect(names.indexOf(PRODUCER_PREFLIGHT_NAME)).toBeLessThan(devenv);
      expect(preflight?.['working-directory']).toBe('.');
      const run = preflight?.run ?? '';
      const lines = run.trimEnd().split('\n');
      const command = lines[0] === 'set -euo pipefail' ? lines.slice(1) : lines;
      expect({ shape, command }).toEqual({ shape, command: PRODUCER_PREFLIGHT.split('\n') });
      for (const machinery of ['GITHUB_ENV', '/dev/urandom', 'smoo_env_']) {
        expect({ shape, machinery, emitted: run.includes(machinery) }).toEqual({ shape, machinery, emitted: false });
      }
      // Only the independent Cargo credential helper, a different mechanism, may write GITHUB_ENV in this job.
      const writers = steps.filter((step) => step.run?.includes('GITHUB_ENV')).map((step) => step.name);
      const helper = (options.cargoCredentials?.gitOrigins?.length ?? 0) > 0 ? ['Prepare Cargo credentials'] : [];
      expect({ shape, writers }).toEqual({ shape, writers: helper });

      // The preflight really runs in a shell under its own step env, and real Node processes honor the NODE_OPTIONS
      // preload under every env the job renders.
      await runPreflight(shape, preflight);
      await runNodeConsumers(shape, steps);
    }
  });

  it('refuses a producer env it cannot carry, naming each rejected name and the step that blocks it', () => {
    const invalid = ['BAD NAME', '1LEADING', 'X\n        run: evil', 'A;touch hostile', ''];
    // Not identifiers; two names GitHub reads as one key, whichever comes first.
    const refusals = [
      ...invalid.map((name) => ({
        env: { APP_CROSS: '1', [name]: '1' },
        named: [name],
        reason: 'is not a valid environment variable name',
      })),
      { env: { FOO: 'upper', foo: 'lower' }, named: ['FOO', 'foo'], reason: 'only by case' },
      { env: { foo: 'lower', FOO: 'upper' }, named: ['foo', 'FOO'], reason: 'only by case' },
    ];
    for (const { shape, owners, options } of producerShapes) {
      // A name that differs only by case from one a step declares for itself can be neither overridden nor repeated
      // there; an exact-case collision is the approved override and renders (the walk above).
      const collisions = Object.entries(owners).map(([own, declaring]) => ({
        env: { [own.toLowerCase()]: 'producer-chosen' },
        named: [own.toLowerCase(), declaring[0] ?? '', own],
        reason: 'only by case',
      }));
      for (const { env, named, reason } of [...refusals, ...collisions]) {
        const message = renderFailure(withProducerEnv(options, env));
        expect({ shape, env, reason: message.includes(reason) }).toEqual({ shape, env, reason: true });
        for (const name of named) expect(message, shape).toContain(JSON.stringify(name));
      }
    }
  });

  it('emits only the selected provider action references before evaluating artifact step eligibility', () => {
    const providers: Array<PublishWorkflowDefinitionOptions['actionsProvider']> = [undefined, 'github', 'forgejo'];
    for (const actionsProvider of providers) {
      const workflow = typia.assert<{
        jobs: Record<
          string,
          {
            steps: Array<{
              uses?: string;
              if?: string;
              with?: { name?: string; 'include-hidden-files'?: boolean };
            }>;
          }
        >;
      }>(Bun.YAML.parse(renderPublishWorkflowYaml({ platformTargetGlobs: PLATFORM_TARGET_GLOBS, actionsProvider })));
      const artifacts = Object.values(workflow.jobs)
        .flatMap((job) => job.steps)
        .filter((step) => step.uses?.includes('-artifact'));
      // Runners resolve actions before evaluating if: even an ineligible
      // foreign action reference would prevent the whole workflow from starting.
      for (const step of artifacts) {
        expect(step.uses?.startsWith('./.github/actions/forgejo-')).toBe(actionsProvider === 'forgejo');
        expect(step.uses?.startsWith('actions/')).toBe(actionsProvider !== 'forgejo');
      }
      const uploads = artifacts.filter((step) => step.uses?.includes('upload-artifact'));
      for (const succeeded of [true, false]) {
        const selected = uploads.filter((step) => {
          if (!step.if) throw new Error('Artifact transport requires an explicit execution condition.');
          // Actions implicitly adds success() when no status function is present.
          if (!/\b(success|failure|always|cancelled)\(/.test(step.if) && !succeeded) return false;
          const matches = new Function('steps', 'success', 'failure', `return ${step.if};`);
          return Boolean(
            matches(
              { version: { outputs: { mode: 'release' } } },
              () => succeeded,
              () => !succeeded,
            ),
          );
        });
        const expectedNames = succeeded
          ? [
              'publish-release-state-${{ github.run_id }}',
              'publish-release-outputs-${{ github.run_id }}',
              'publish-linux-outputs-${{ github.run_id }}',
              'publish-macos-outputs-${{ github.run_id }}',
            ]
          : ['trace-results-${{ github.run_id }}'];
        expect(selected.map((step) => step.with?.name).sort()).toEqual(expectedNames.sort());
        for (const step of selected) {
          if (step.with?.name !== 'publish-release-state-${{ github.run_id }}') {
            expect(step.with?.['include-hidden-files']).toBe(true);
          }
        }
      }
    }
  });

  it('fans the macOS producer out over one runner per architecture', () => {
    const matrixed = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: PLATFORM_TARGET_GLOBS,
      macosPlatformArchitectures: ['arm64', 'x64'],
    });
    const macosPlatform = matrixed.slice(
      matrixed.indexOf('  macos-platform:'),
      matrixed.indexOf('  publish-on-linux:'),
    );
    const finalJob = matrixed.slice(matrixed.indexOf('  publish-on-linux:'));

    expect(macosPlatform).toContain('      fail-fast: false');
    expect(macosPlatform).toContain('        arch: [arm64, x64]');
    expect(foldedRunCommand(macosPlatform, '🍎 Build selected macOS and iOS release outputs')).toBe(
      'smoo release build-platform-outputs --bump "${{ inputs.bump }}" --projects "$SMOO_RELEASE_PROJECTS" --ref "${{ github.sha }}" ' +
        '--targets "*-${{ matrix.arch }}-macos,*-${{ matrix.arch }}-ios" ' +
        '--output "${{ runner.temp }}/macos-platform-outputs" --github-output "$GITHUB_OUTPUT"',
    );
    // Foreign-architecture binaries cannot execute on the runner that built them.
    expect(macosPlatform).toContain("if: matrix.arch == 'arm64' && steps.platform-outputs.outputs.projects != ''");
    expect(macosPlatform).toContain('name: publish-macos-${{ matrix.arch }}-outputs-${{ github.run_id }}');
    expect(macosPlatform).not.toContain('name: publish-macos-outputs-');
    expect(foldedRunCommand(finalJob, '🍎 Apply verified macOS outputs')).toBe(
      'smoo github-ci apply-outputs --source-sha "${{ github.sha }}" ' +
        '"${{ runner.temp }}/publish-artifacts/publish-macos-arm64-outputs-${{ github.run_id }}/current" ' +
        '"${{ runner.temp }}/publish-artifacts/publish-macos-x64-outputs-${{ github.run_id }}/current"',
    );
    expect(foldedRunCommand(finalJob, '🧯 Repair pending releases')).toBe(
      'smoo release repair-pending --ref "${{ github.sha }}" --platform-outputs ' +
        '"${{ runner.temp }}/publish-artifacts/publish-macos-arm64-outputs-${{ github.run_id }}/repairs,' +
        '${{ runner.temp }}/publish-artifacts/publish-macos-x64-outputs-${{ github.run_id }}/repairs" ' +
        '--dry-run "${{ inputs.dry_run }}"',
    );

    const single = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: PLATFORM_TARGET_GLOBS,
      macosPlatformArchitectures: ['arm64'],
    });
    const singleMacos = single.slice(single.indexOf('  macos-platform:'), single.indexOf('  publish-on-linux:'));
    expect(singleMacos).not.toContain('strategy:');
    expect(singleMacos).toContain('name: publish-macos-outputs-${{ github.run_id }}');
    expect(singleMacos).toContain("if: steps.platform-outputs.outputs.projects != ''");
  });

  it('limits trusted publishing permission to final publish and preserves mode, deploy, and dry-run gates', () => {
    const rendered = renderPublishWorkflowYaml({
      deploy: true,
      deployProvider: 'cloudflare',
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    });
    const linuxCandidate = rendered.slice(
      rendered.indexOf('  linux-release-candidate:'),
      rendered.indexOf('  macos-platform:'),
    );
    const macosPlatform = rendered.slice(
      rendered.indexOf('  macos-platform:'),
      rendered.indexOf('  publish-on-linux:'),
    );
    const finalJob = rendered.slice(rendered.indexOf('  publish-on-linux:'));

    expect(linuxCandidate).toContain('id-token: none');
    expect(macosPlatform).toContain('id-token: none');
    expect(finalJob).toContain('id-token: write');
    expect(rendered.match(/id-token: write/g)).toHaveLength(1);
    expect(finalJob.indexOf('🧯 Repair pending releases')).toBeLessThan(
      finalJob.indexOf('♻️ Restore validated release state'),
    );
    expect(finalJob.indexOf('♻️ Restore validated release state')).toBeGreaterThan(
      finalJob.indexOf('🧱 Setup Nix/devenv'),
    );
    expect(rendered).not.toContain('GITHUB_SHA:');
    expect(finalJob).toContain("needs.linux-release-candidate.outputs.mode != 'none'");
    expect(finalJob).toContain("inputs.deploy_stage == 'production'");
    expect(finalJob).toContain("inputs.dry_run != 'true'");
    const publishCommand = foldedRunCommand(
      finalJob,
      '📦 Publish release (${{ needs.linux-release-candidate.outputs.mode }})',
    );
    expect(publishCommand).toContain('smoo release publish --prebuilt');
    expect(publishCommand).toContain('/publish-release-outputs-${{ github.run_id }}"');
    expect(publishCommand).toContain('/publish-linux-outputs-${{ github.run_id }}"');
    expect(publishCommand).toContain('/publish-macos-outputs-${{ github.run_id }}/current"');
    expect(publishCommand).toContain('--bump "${{ inputs.bump }}" --dry-run "${{ inputs.dry_run }}"');
    expect(finalJob).toContain('CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}');
    expect(finalJob).toContain('CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}');
  });

  it('does not wire npm token secrets into repair or publish steps', () => {
    const rendered = renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' });

    expect(rendered).not.toContain('NODE_AUTH_TOKEN');
    expect(rendered).not.toContain('secrets.NPM_TOKEN');
    expect(rendered).toContain('packages must already exist on npm and use trusted publishing/OIDC');
  });

  it('puts the private registry read token on every job and the publisher token only on write steps', () => {
    const rendered = renderPublishWorkflowYaml({
      repoName: '@priv.test/runtime',
      privateNpm: {
        scope: '@priv.test',
        readTokenEnv: 'PRIV_NPM_READ_TOKEN',
        publishTokenEnv: 'PRIV_NPM_PUBLISH_TOKEN',
      },
    });

    expect(rendered).toContain('PRIV_NPM_READ_TOKEN: ${{ secrets.PRIV_NPM_READ_TOKEN }}');
    expect(rendered).not.toContain('PRIV_NPM_REGISTRY');
    expect(rendered).not.toContain('vars.PRIV_NPM_REGISTRY');
    // Job env is six spaces; a publisher token there would leak into setup/build.
    expect(rendered).not.toMatch(/^ {6}PRIV_NPM_PUBLISH_TOKEN:/m);
    expect(rendered).toContain('          PRIV_NPM_PUBLISH_TOKEN: ${{ secrets.PRIV_NPM_PUBLISH_TOKEN }}');

    const setupAt = rendered.indexOf('- name: 🧱 Setup Nix/devenv');
    const repairAt = rendered.indexOf('- name: 🧯 Repair pending releases');
    const publishAt = rendered.indexOf('- name: 📦 Publish release');
    expect(setupAt).toBeGreaterThan(-1);
    expect(repairAt).toBeGreaterThan(setupAt);
    expect(publishAt).toBeGreaterThan(repairAt);

    const beforeSetup = rendered.slice(0, setupAt);
    expect(beforeSetup).toContain('PRIV_NPM_READ_TOKEN: ${{ secrets.PRIV_NPM_READ_TOKEN }}');
    expect(beforeSetup).not.toContain('PRIV_NPM_PUBLISH_TOKEN');

    expect(rendered.slice(repairAt, publishAt)).toContain('PRIV_NPM_PUBLISH_TOKEN');
    expect(rendered.slice(publishAt)).toContain('PRIV_NPM_PUBLISH_TOKEN');
    expect(rendered).not.toContain('NODE_AUTH_TOKEN');
    expect(rendered).not.toContain('secrets.NPM_TOKEN');
  });

  it('does not wire a publish token when the repo only consumes the private scope', () => {
    const rendered = renderPublishWorkflowYaml({
      repoName: '@example/app',
      privateNpm: {
        scope: '@priv.test',
        readTokenEnv: 'PRIV_NPM_READ_TOKEN',
      },
    });

    expect(rendered).toContain('PRIV_NPM_READ_TOKEN: ${{ secrets.PRIV_NPM_READ_TOKEN }}');
    expect(rendered).not.toContain('PRIV_NPM_PUBLISH_TOKEN');
    expect(rendered).not.toContain('          PRIV_NPM_PUBLISH_TOKEN:');
  });

  it('omits production deploy controls when no production deploy target exists', () => {
    const rendered = renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' });

    expect(rendered).not.toContain('deploy_stage');
    expect(rendered).not.toContain('Deploy production');
    expect(rendered).not.toContain('nx-deploy');
  });

  it('renders production deploy controls for repos with production deploy targets', () => {
    const rendered = renderPublishWorkflowYaml({ deploy: true, repoName: '@smoothbricks/codebase' });

    expect(rendered).toContain('deploy_stage:');
    expect(rendered).toContain('- name: 🚀 Deploy production');
    expect(rendered).not.toContain('CLOUDFLARE_API_TOKEN');
    expect(rendered).not.toContain('CLOUDFLARE_ACCOUNT_ID');
    expect(rendered).toContain(
      'smoo github-ci nx-deploy --stage production --mode run-many --verify --name "Deploy Production"',
    );
  });

  it('renders a deploy-only workflow for repos that deploy but own no release packages', () => {
    const rendered = renderPublishWorkflowYaml({ deploy: true, release: false, repoName: '@example/app' });

    // The deploy must not wait on a release that can never happen here.
    expect(rendered).toContain('- name: 🚀 Deploy production');
    expect(rendered).not.toContain('steps.version.outputs.mode');
    expect(rendered).not.toContain('smoo release');
    expect(rendered).not.toContain('🧯 Repair pending releases');
    expect(rendered).not.toContain('🔢 Version release');
    expect(rendered).not.toContain('📦 Publish release');
    expect(rendered).not.toContain('🏷️ Tag release');
    expect(rendered).toContain("inputs.deploy_stage == 'production'");
    expect(rendered).toContain("inputs.dry_run != 'true'");
    // Nothing to bump when the workflow releases nothing.
    expect(rendered).not.toContain('bump:');
    // dry_run gates the deploy here, so it must not describe release commands.
    expect(rendered).toContain('description: Skip the deploy and report what would run.');
  });

  it('adds Cloudflare credentials for Wrangler-backed production deploys', () => {
    const rendered = renderPublishWorkflowYaml({
      deploy: true,
      deployProvider: 'cloudflare',
      repoName: '@smoothbricks/codebase',
    });

    expect(rendered).toContain('CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}');
    expect(rendered).toContain('CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}');
  });

  it('deploys production only after a real publish when requested', async () => {
    const deployed = await publishWorkflowScenario({
      deploy: true,
      repairs: [],
      current: [],
      bump: 'auto',
      deployStage: 'production',
      dryRun: false,
      version: { mode: 'new', projects: ['app'] },
    }).run();
    const dryRun = await publishWorkflowScenario({
      deploy: true,
      repairs: [],
      current: [],
      bump: 'auto',
      deployStage: 'production',
      dryRun: true,
      version: { mode: 'new', projects: ['app'] },
    }).run();

    expect(deployed.productionDeployed).toBe(true);
    expect(dryRun.productionDeployed).toBe(false);
  });

  it('deploys production with no release for repos that own no packages', async () => {
    const outcome = await publishWorkflowScenario({
      deploy: true,
      release: false,
      repairs: [],
      current: [],
      bump: 'auto',
      deployStage: 'production',
      dryRun: false,
      // No release means no version step, so mode stays 'none' — the deploy must
      // still run, which the release-gated condition would have blocked.
      version: { mode: 'none', projects: [] },
    }).run();

    const dryRun = await publishWorkflowScenario({
      deploy: true,
      release: false,
      repairs: [],
      current: [],
      bump: 'auto',
      deployStage: 'production',
      dryRun: true,
      version: { mode: 'none', projects: [] },
    }).run();
    const notRequested = await publishWorkflowScenario({
      deploy: true,
      release: false,
      repairs: [],
      current: [],
      bump: 'auto',
      deployStage: 'none',
      dryRun: false,
      version: { mode: 'none', projects: [] },
    }).run();

    expect(outcome.productionDeployed).toBe(true);
    expect(outcome.publishRan).toBe(false);
    expect(outcome.createdTags).toEqual([]);
    // Dropping the version gate must not drop the other two.
    expect(dryRun.productionDeployed).toBe(false);
    expect(notRequested.productionDeployed).toBe(false);
  });

  it('bootstraps the self-hosted CLI before release commands only for the SmoothBricks repo', async () => {
    const smoothbricks = await publishWorkflowScenario({
      repoName: '@smoothbricks/codebase',
      repairs: [],
      current: [],
      bump: 'patch',
      dryRun: false,
      version: { mode: 'none', projects: [] },
    }).run();
    const downstream = await publishWorkflowScenario({
      repoName: '@other/repo',
      repairs: [],
      current: [],
      bump: 'patch',
      dryRun: false,
      version: { mode: 'none', projects: [] },
    }).run();

    expect(smoothbricks.nxVersionActionsBuilt).toBe(true);
    expect(smoothbricks.repairSawNxVersionActions).toBe(true);
    expect(smoothbricks.versionSawNxVersionActions).toBe(true);
    expect(downstream.nxVersionActionsBuilt).toBe(false);
    expect(downstream.repairSawNxVersionActions).toBe(false);
    expect(downstream.versionSawNxVersionActions).toBe(false);
  });

  it('repairs older gaps and skips validation for mode none', async () => {
    const scenario = publishWorkflowScenario({
      repairs: [
        { tag: '@scope/a@1.1.0', npmMissing: false, githubMissing: true },
        { tag: '@scope/b@2.0.0-beta.1', npmMissing: true, githubMissing: true },
      ],
      // `none` means HEAD carries no release: a HEAD release target reports `complete`.
      current: [],
      bump: 'auto',
      dryRun: false,
      version: { mode: 'none', projects: [] },
    });

    const outcome = await scenario.run();

    expect(outcome.fixtureRepoSetup).toBe(true);
    expect(outcome.releaseAuthorConfigured).toBe(true);
    expect(outcome.repairedTags).toEqual(['@scope/a@1.1.0', '@scope/b@2.0.0-beta.1']);
    expect(outcome.repairBuildArtifacts).toEqual(['@scope/b']);
    expect(outcome.validation).toEqual({ checks: 0, builds: [], lints: [], tests: [], validates: 0 });
    expect(outcome.publishRan).toBe(true);
    expect(outcome.publishCompletedTags).toEqual([]);
    expect(outcome.createdTags).toEqual([]);
    expect(outcome.remainingDurableGaps).toEqual([]);
  });

  it('builds and validates a HEAD release target before the prebuilt publish completes it', async () => {
    // `smoo release retag-unpublished` moved the tags to HEAD; npm still lacks both versions.
    const scenario = publishWorkflowScenario({
      repairs: [],
      current: [
        { tag: '@scope/a@1.2.0', npmMissing: true, githubMissing: true },
        { tag: '@scope/b@2.0.0', npmMissing: true, githubMissing: true },
      ],
      bump: 'auto',
      dryRun: false,
      version: { mode: 'complete', projects: ['a', 'b'] },
    });

    const outcome = await scenario.run();

    expect(outcome.validation).toEqual({
      checks: 1,
      builds: ['a', 'b'],
      lints: ['a', 'b'],
      tests: ['a', 'b'],
      validates: 1,
    });
    expect(outcome.publishSawPrebuiltArtifacts).toBe(true);
    expect(outcome.publishCompletedTags).toEqual(['@scope/a@1.2.0', '@scope/b@2.0.0']);
    expect(outcome.publishSawCreatedTags).toBe(true);
    expect(outcome.remainingDurableGaps).toEqual([]);
  });

  it('opens every gate the prebuilt publish depends on for a HEAD release target, and none for no release', async () => {
    const workflow = typia.assert<{
      jobs: Record<string, { steps: Array<{ name?: string; if?: string; run?: string }> }>;
    }>(Bun.YAML.parse(await formattedPublishWorkflow(renderPublishWorkflowYaml(codebaseWorkflowOptions))));
    const steps = new Map(
      Object.values(workflow.jobs)
        .flatMap((job) => job.steps)
        .map((step) => [step.name, step]),
    );
    const dependencies = [
      '🔍 Lint (${{ steps.plan.outputs.mode }})',
      '🧪 Unit Tests (${{ steps.plan.outputs.mode }})',
      '🔨 Build (${{ steps.version.outputs.mode }})',
      '🐧 Build supplemental Linux targets',
      '📤 Upload validated build outputs',
      '📤 Upload supplemental Linux outputs',
      '📦 Apply verified Linux outputs',
      '🍎 Apply verified macOS outputs',
    ];

    for (const name of dependencies) {
      const gate = steps.get(name)?.if;
      expect({ name, complete: modeGateAdmits(gate, 'complete'), none: modeGateAdmits(gate, 'none') }).toEqual({
        name,
        complete: true,
        none: false,
      });
    }
    const publish = steps.get('📦 Publish release (${{ needs.linux-release-candidate.outputs.mode }})');
    expect(publish?.if).toBeUndefined();
    expect(publish?.run).toContain('smoo release publish --prebuilt');
  });

  it('validates a new release before publishing current HEAD gaps', async () => {
    const scenario = publishWorkflowScenario({
      repairs: [],
      current: [
        { tag: '@scope/a@1.2.0', npmMissing: true, githubMissing: true },
        { tag: '@scope/b@2.0.0', npmMissing: false, githubMissing: true },
      ],
      bump: 'patch',
      dryRun: false,
      version: { mode: 'new', projects: ['a', 'b'] },
    });

    const outcome = await scenario.run();

    expect(outcome.validation).toEqual({
      checks: 1,
      builds: ['a', 'b'],
      lints: ['a', 'b'],
      tests: ['a', 'b'],
      validates: 1,
    });
    expect(outcome.publishSawValidatedRelease).toBe(true);
    expect(outcome.publishCompletedTags).toEqual(['@scope/a@1.2.0', '@scope/b@2.0.0']);
    expect(outcome.createdTags).toEqual(['@scope/a@1.2.0', '@scope/b@2.0.0']);
    expect(outcome.publishSawCreatedTags).toBe(true);
    expect(outcome.remainingDurableGaps).toEqual([]);
  });

  it('dry-run walks the workflow outcomes without mutating durable release state', async () => {
    const scenario = publishWorkflowScenario({
      repairs: [{ tag: '@scope/a@1.1.0', npmMissing: true, githubMissing: true }],
      current: [{ tag: '@scope/a@1.2.0', npmMissing: true, githubMissing: true }],
      bump: 'prerelease',
      dryRun: true,
      version: { mode: 'new', projects: ['a'] },
    });

    const outcome = await scenario.run();

    expect(outcome.fixtureRepoSetup).toBe(true);
    expect(outcome.repairedTags).toEqual([]);
    expect(outcome.repairBuildArtifacts).toEqual([]);
    expect(outcome.validation).toEqual({
      checks: 1,
      builds: ['a'],
      lints: ['a'],
      tests: ['a'],
      validates: 1,
    });
    expect(outcome.publishRan).toBe(true);
    expect(outcome.publishCompletedTags).toEqual([]);
    expect(outcome.createdTags).toEqual([]);
    expect(outcome.remainingDurableGaps).toEqual([
      '@scope/a@1.1.0:github',
      '@scope/a@1.1.0:npm',
      '@scope/a@1.2.0:github',
      '@scope/a@1.2.0:npm',
    ]);
  });
});

interface ReleaseGap {
  tag: string;
  npmMissing: boolean;
  githubMissing: boolean;
}

interface WorkflowScenarioConfig {
  deploy?: boolean;
  release?: boolean;
  repoName?: string;
  repairs: ReleaseGap[];
  current: ReleaseGap[];
  bump: PublishWorkflowBump;
  deployStage?: 'none' | 'production';
  dryRun: boolean;
  version: PublishWorkflowVersionOutputs;
}

interface WorkflowOutcome {
  fixtureRepoSetup: boolean;
  releaseAuthorConfigured: boolean;
  nxVersionActionsBuilt: boolean;
  repairSawNxVersionActions: boolean;
  versionSawNxVersionActions: boolean;
  repairedTags: string[];
  repairBuildArtifacts: string[];
  validation: { checks: number; builds: string[]; lints: string[]; tests: string[]; validates: number };
  publishRan: boolean;
  productionDeployed: boolean;
  publishSawValidatedRelease: boolean;
  publishCompletedTags: string[];
  createdTags: string[];
  publishSawCreatedTags: boolean;
  remainingDurableGaps: string[];
  /** Every npm-missing HEAD package was built before publish: `--prebuilt` refuses one that was not. */
  publishSawPrebuiltArtifacts: boolean;
}

function publishWorkflowScenario(config: WorkflowScenarioConfig): { run(): Promise<WorkflowOutcome> } {
  return {
    async run() {
      const state = new WorkflowScenarioState(config);
      await runPublishWorkflow(
        definePublishWorkflow({ deploy: config.deploy, release: config.release, repoName: config.repoName }),
        {
          inputs: { bump: config.bump, deployStage: config.deployStage ?? 'none', dryRun: config.dryRun },
          callbacks: state.callbacks(),
        },
      );
      return state.outcome();
    },
  };
}

class WorkflowScenarioState {
  private fixtureSetup = false;
  private authorConfigured = false;
  private nxVersionActions = false;
  private repairObservedNxVersionActions = false;
  private versionObservedNxVersionActions = false;
  private publishReleaseRan = false;
  private productionDeployRan = false;
  private publishSawValidation = false;
  private publishSawTags = false;
  private publishSawPrebuilt = false;
  private readonly createdTags = new Set<string>();
  private readonly repaired = new Set<string>();
  private readonly repairBuilds = new Set<string>();
  private readonly publishedCurrent = new Set<string>();
  private readonly durableGaps = new Set<string>();
  private readonly validationState: {
    checks: number;
    builds: string[];
    lints: string[];
    tests: string[];
    validates: number;
  } = {
    checks: 0,
    builds: [],
    lints: [],
    tests: [],
    validates: 0,
  };
  planned = false;
  gatesBeforeVersion: { lints: string[]; tests: string[] } = { lints: [], tests: [] };

  constructor(private readonly config: WorkflowScenarioConfig) {
    for (const gap of [...config.repairs, ...config.current]) {
      if (gap.githubMissing) {
        this.durableGaps.add(`${gap.tag}:github`);
      }
      if (gap.npmMissing) {
        this.durableGaps.add(`${gap.tag}:npm`);
      }
    }
  }

  callbacks(): PublishWorkflowCallbacks {
    return {
      checkout: async () => {},
      setupDevenv: async () => {
        this.fixtureSetup = true;
        return { nixCacheHit: 'false', devenvCacheHit: 'false' };
      },
      configureReleaseAuthor: async () => {
        this.authorConfigured = true;
      },
      buildNxVersionActions: async () => {
        this.nxVersionActions = true;
      },
      repairPendingReleases: async ({ dryRun }) => {
        this.repairObservedNxVersionActions = this.nxVersionActions;
        if (dryRun) {
          return;
        }
        for (const gap of this.config.repairs) {
          if (gap.npmMissing) {
            this.repairBuilds.add(packageNameFromTag(gap.tag));
            this.durableGaps.delete(`${gap.tag}:npm`);
          }
          if (gap.githubMissing) {
            this.durableGaps.delete(`${gap.tag}:github`);
          }
          this.repaired.add(gap.tag);
        }
      },
      planRelease: async ({ bump }) => {
        // The plan runs before anything is written, and the gates that follow
        // it must therefore have seen no version commit yet.
        this.planned = true;
        expect(bump).toBe(this.config.bump);
        return this.config.version;
      },
      versionRelease: async ({ bump, dryRun }) => {
        this.versionObservedNxVersionActions = this.nxVersionActions;
        this.gatesBeforeVersion = { lints: [...this.validationState.lints], tests: [...this.validationState.tests] };
        expect(bump).toBe(this.config.bump);
        expect(dryRun).toBe(this.config.dryRun);
        return this.config.version;
      },
      checkManagedMonorepoFiles: async () => {
        this.validationState.checks += 1;
      },
      nxRunMany: async ({ target, projects }) => {
        this.validationProjects(target).push(...projects);
      },
      uploadTraceDbs: async () => {},
      validateMonorepoConfig: async () => {
        this.validationState.validates += 1;
      },
      tagRelease: async ({ dryRun }) => {
        expect(dryRun).toBe(this.config.dryRun);
        if (dryRun) {
          return;
        }
        for (const gap of this.config.current) {
          this.createdTags.add(gap.tag);
        }
      },
      publishRelease: async ({ bump, dryRun }) => {
        expect(bump).toBe(this.config.bump);
        expect(dryRun).toBe(this.config.dryRun);
        this.publishReleaseRan = true;
        this.publishSawValidation = this.config.version.mode === 'none' || this.validationState.validates > 0;
        // Publishing must never be the first writer of a release tag.
        this.publishSawTags = this.config.current.every((gap) => this.createdTags.has(gap.tag));
        this.publishSawPrebuilt = this.config.current
          .filter((gap) => gap.npmMissing)
          .every((gap) => this.validationState.builds.includes(projectNameFromTag(gap.tag)));
        if (dryRun) {
          return;
        }
        for (const gap of this.config.current) {
          if (gap.npmMissing) {
            this.durableGaps.delete(`${gap.tag}:npm`);
          }
          if (gap.githubMissing) {
            this.durableGaps.delete(`${gap.tag}:github`);
          }
          if (gap.npmMissing || gap.githubMissing) {
            this.publishedCurrent.add(gap.tag);
          }
        }
      },
      deployProduction: async () => {
        this.productionDeployRan = true;
      },
      saveNixDevenv: async () => {},
    };
  }

  outcome(): WorkflowOutcome {
    return {
      fixtureRepoSetup: this.fixtureSetup,
      releaseAuthorConfigured: this.authorConfigured,
      nxVersionActionsBuilt: this.nxVersionActions,
      repairSawNxVersionActions: this.repairObservedNxVersionActions,
      versionSawNxVersionActions: this.versionObservedNxVersionActions,
      repairedTags: [...this.repaired].sort(),
      repairBuildArtifacts: [...this.repairBuilds].sort(),
      validation: {
        checks: this.validationState.checks,
        builds: [...this.validationState.builds],
        lints: [...this.validationState.lints],
        tests: [...this.validationState.tests],
        validates: this.validationState.validates,
      },
      publishRan: this.publishReleaseRan,
      productionDeployed: this.productionDeployRan,
      publishSawValidatedRelease: this.publishSawValidation,
      publishCompletedTags: [...this.publishedCurrent].sort(),
      createdTags: [...this.createdTags].sort(),
      publishSawCreatedTags: this.publishSawTags,
      remainingDurableGaps: [...this.durableGaps].sort(),
      publishSawPrebuiltArtifacts: this.publishSawPrebuilt,
    };
  }

  private validationProjects(target: PublishWorkflowNxTarget): string[] {
    if (target === 'build') {
      return this.validationState.builds;
    }
    if (target === 'lint') {
      return this.validationState.lints;
    }
    return this.validationState.tests;
  }
}

function stepAnchorNumbers(yaml: string): number[] {
  return [...yaml.matchAll(/^\s*# Step (\d+)/gm)].map((match) => Number(match[1]));
}

function foldedRunCommand(yaml: string, stepName: string): string {
  const stepStart = yaml.indexOf(`- name: ${stepName}`);
  expect(stepStart).toBeGreaterThanOrEqual(0);
  const runMarker = '        run:\n';
  const runStart = yaml.indexOf(runMarker, stepStart);
  expect(runStart).toBeGreaterThan(stepStart);
  const commandLines: string[] = [];
  for (const line of yaml.slice(runStart + runMarker.length).split('\n')) {
    if (!line.startsWith('          ')) {
      break;
    }
    commandLines.push(line.trim());
  }
  return commandLines.join(' ');
}

function packageNameFromTag(tag: string): string {
  const versionSeparator = tag.lastIndexOf('@');
  if (versionSeparator <= 0) {
    throw new Error(`Invalid release tag fixture ${tag}.`);
  }
  return tag.slice(0, versionSeparator);
}

/** The fixtures' Nx project is the unscoped package name: `@scope/a@1.2.0` releases project `a`. */
function projectNameFromTag(tag: string): string {
  const packageName = packageNameFromTag(tag);
  return packageName.slice(packageName.lastIndexOf('/') + 1);
}

/**
 * Evaluates a release gate of the one shape the workflow renders,
 * `<source>.outputs.mode != '<mode>'`, for `mode`. Any other shape fails the
 * test rather than being guessed at.
 */
function modeGateAdmits(gate: string | undefined, mode: ReleaseVersionMode): boolean {
  const excluded = gate?.match(
    /^(?:steps\.(?:plan|version)|needs\.linux-release-candidate)\.outputs\.mode != '([a-z-]+)'$/,
  )?.[1];
  if (excluded === undefined) {
    throw new Error(`Not a release mode gate: ${String(gate)}`);
  }
  return mode !== excluded;
}

it('omits sibling checkout steps when no sources are declared', () => {
  const absent = renderPublishWorkflowYaml();
  const empty = renderPublishWorkflowYaml({ sourceCheckouts: [] });

  expect(empty).toBe(absent);
  expect(absent).not.toContain('📦 Check out sibling sources');
});

it('refuses malformed sibling checkout declarations at render time', () => {
  expect(() =>
    renderPublishWorkflowYaml({
      sourceCheckouts: [{ path: '/absolute', repository: 'https://git.example.net/source.git' }],
    }),
  ).toThrow('path relative to the workspace root');
  expect(() =>
    renderPublishWorkflowYaml({
      sourceCheckouts: [{ path: '../source', repository: 'git@example.net:source.git' }],
    }),
  ).toThrow('https repository URL');
  expect(() =>
    renderPublishWorkflowYaml({
      sourceCheckouts: [
        { path: '../source', repository: 'https://git.example.net/source.git', tokenEnv: 'source_read_token' },
      ],
    }),
  ).toThrow('secret env name');
});

it('builds on smoo.github.runsOn but publishes from the GitHub-hosted runner', () => {
  const rendered = renderPublishWorkflowYaml({
    repoName: '@smoothbricks/codebase',
    platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    runsOn: [...nixosRunsOn],
  });
  const selfHostedRunner = `runs-on:
      \${{ (github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository) &&
      fromJSON('["nixos-latest-x64","self-hosted"]') || 'ubuntu-latest' }}`;

  expect(rendered).toContain(`  linux-release-candidate:\n    ${selfHostedRunner}`);
  expect(rendered).toContain('  macos-platform:\n    runs-on: macos-latest');
  // npmjs signs provenance with the job's OIDC token and then rejects the
  // upload (422) unless the runner is GitHub-hosted, so only the build lane
  // may keep the configured labels.
  expect(rendered.split('fromJSON(\'["nixos-latest-x64","self-hosted"]\')').length - 1).toBe(1);
  expect(rendered).toContain(
    '  publish-on-linux:\n    needs: [linux-release-candidate, macos-platform]\n    runs-on: ubuntu-latest',
  );
  // The runner moved; the OIDC permission that mints provenance did not.
  expect(rendered.slice(rendered.indexOf('  publish-on-linux:'))).toContain('id-token: write');
});

it('publishes single-job workflows from the GitHub-hosted runner, private scopes included', () => {
  for (const platformTargetGlobs of [undefined, LINUX_PLATFORM_TARGET_GLOBS]) {
    const rendered = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs,
      runsOn: [...nixosRunsOn],
    });

    // Build and publish share this job, so the whole job follows the publisher.
    expect(rendered).toContain('  publish:\n    runs-on: ubuntu-latest');
    expect(rendered).not.toContain('nixos-latest-x64');
    expect(rendered).toContain('- name: 📦 Publish release');
  }

  const privateScope = renderPublishWorkflowYaml({
    repoName: '@priv.test/runtime',
    runsOn: [...nixosRunsOn],
    privateNpm: {
      scope: '@priv.test',
      readTokenEnv: 'PRIV_NPM_READ_TOKEN',
      publishTokenEnv: 'PRIV_NPM_PUBLISH_TOKEN',
    },
  });

  expect(privateScope).toContain('  publish:\n    runs-on: ubuntu-latest');
  expect(privateScope).toContain('      PRIV_NPM_READ_TOKEN: ${{ secrets.PRIV_NPM_READ_TOKEN }}');
  expect(privateScope).toContain('          PRIV_NPM_PUBLISH_TOKEN: ${{ secrets.PRIV_NPM_PUBLISH_TOKEN }}');
});

it('keeps every job on the configured runner when the workflow is not GitHub Actions', () => {
  const rendered = renderPublishWorkflowYaml({
    repoName: '@example/runtime',
    platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    runsOn: ['linux-x64', 'self-hosted'],
    actionsProvider: 'forgejo',
  });

  // `ubuntu-latest` names no runner on a Forgejo instance, and there is no
  // GitHub OIDC there to mint npmjs provenance from.
  expect(Bun.YAML.parse(rendered)).toMatchObject({
    jobs: {
      'linux-release-candidate': { 'runs-on': ['linux-x64', 'self-hosted'] },
      'publish-on-linux': { 'runs-on': ['linux-x64', 'self-hosted'] },
    },
  });
  expect(rendered).not.toContain('runs-on: ubuntu-latest');
});

it('leaves the deploy-only job on the configured runner', () => {
  const rendered = renderPublishWorkflowYaml({
    deploy: true,
    release: false,
    repoName: '@example/app',
    runsOn: ['linux-x64', 'self-hosted'],
  });

  // Nothing here reaches a registry, so the publisher rule does not apply.
  expect(Bun.YAML.parse(rendered)).toMatchObject({ jobs: { publish: { 'runs-on': ['linux-x64', 'self-hosted'] } } });
  expect(rendered).not.toContain('smoo release');
});

it('runs macOS platform legs on smoo.github.macosRunsOn; default stays macos-latest', () => {
  const configured = renderPublishWorkflowYaml({
    repoName: '@smoothbricks/codebase',
    platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    macosRunsOn: ['macos-arm64', 'self-hosted'],
  });
  expect(Bun.YAML.parse(configured)).toMatchObject({
    jobs: { 'macos-platform': { 'runs-on': ['macos-arm64', 'self-hosted'] } },
  });
  const fallback = renderPublishWorkflowYaml({
    repoName: '@smoothbricks/codebase',
    platformTargetGlobs: PLATFORM_TARGET_GLOBS,
  });
  expect(Bun.YAML.parse(fallback)).toMatchObject({
    jobs: { 'macos-platform': { 'runs-on': 'macos-latest' } },
  });
});

it('installs the Cargo credential helper in every job that fetches crates', () => {
  const rendered = renderPublishWorkflowYaml({
    repoName: '@smoothbricks/codebase',
    platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    cargoCredentials: CARGO_GIT_CREDENTIALS,
  });
  const linuxCandidate = rendered.slice(
    rendered.indexOf('  linux-release-candidate:'),
    rendered.indexOf('  macos-platform:'),
  );
  const macosPlatform = rendered.slice(rendered.indexOf('  macos-platform:'), rendered.indexOf('  publish-on-linux:'));
  const finalJob = rendered.slice(rendered.indexOf('  publish-on-linux:'));

  // The helper is per-job process configuration: a job that builds without it
  // fetches the private dependency anonymously and fails. The macOS leg builds
  // the Apple natives and the final job still repairs pending releases.
  for (const job of [linuxCandidate, macosPlatform, finalJob]) {
    expect(job.indexOf('Prepare Cargo credentials')).toBeLessThan(job.indexOf('🧱 Setup Nix/devenv'));
  }
  expect(Bun.YAML.parse(rendered)).toMatchObject({
    jobs: {
      'linux-release-candidate': { env: { SOURCE_READ_TOKEN: '${{ secrets.SOURCE_READ_TOKEN }}' } },
      'macos-platform': { env: { SOURCE_READ_TOKEN: '${{ secrets.SOURCE_READ_TOKEN }}' } },
      'publish-on-linux': { env: { SOURCE_READ_TOKEN: '${{ secrets.SOURCE_READ_TOKEN }}' } },
    },
  });
  // The token stays a shell expansion of job env: never a URL, never argv.
  expect(rendered).not.toMatch(/https:\/\/[^\s'"]*SOURCE_READ_TOKEN/);

  const withoutCredentials = renderPublishWorkflowYaml({
    repoName: '@smoothbricks/codebase',
    platformTargetGlobs: PLATFORM_TARGET_GLOBS,
  });
  expect(withoutCredentials).not.toContain('Prepare Cargo credentials');
  expect(withoutCredentials).not.toContain('CARGO_NET_GIT_FETCH_WITH_CLI');
  expect(withoutCredentials).not.toContain('SOURCE_READ_TOKEN');
});

it('keeps job-local step anchors contiguous once Cargo credentials add a setup step', () => {
  const singleJob = renderPublishWorkflowYaml({
    repoName: '@smoothbricks/codebase',
    cargoCredentials: CARGO_GIT_CREDENTIALS,
  });
  const platform = renderPublishWorkflowYaml({
    deploy: true,
    repoName: '@smoothbricks/codebase',
    platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    cargoCredentials: CARGO_GIT_CREDENTIALS,
  });
  const linuxCandidate = platform.slice(
    platform.indexOf('  linux-release-candidate:'),
    platform.indexOf('  macos-platform:'),
  );
  const macosPlatform = platform.slice(platform.indexOf('  macos-platform:'), platform.indexOf('  publish-on-linux:'));
  const finalJob = platform.slice(platform.indexOf('  publish-on-linux:'));

  // Each job gains exactly one step over the credential-free counts, and the
  // hand-numbered platform renderers must renumber with it.
  expect(stepAnchorNumbers(singleJob)).toEqual(Array.from({ length: 18 }, (_, index) => index + 1));
  expect(stepAnchorNumbers(linuxCandidate)).toEqual(Array.from({ length: 21 }, (_, index) => index + 1));
  expect(stepAnchorNumbers(macosPlatform)).toEqual(Array.from({ length: 11 }, (_, index) => index + 1));
  expect(stepAnchorNumbers(finalJob)).toEqual(Array.from({ length: 15 }, (_, index) => index + 1));
});

it('preflights registry-only Cargo credentials in every fetching job without installing a git helper', () => {
  const rendered = renderPublishWorkflowYaml({
    repoName: '@smoothbricks/codebase',
    platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    cargoCredentials: { registryTokenEnvs: ['CARGO_REGISTRIES_EXAMPLE_TOKEN', 'CARGO_REGISTRIES_FORGE_TOKEN'] },
  });
  const macosPlatform = rendered.slice(rendered.indexOf('  macos-platform:'), rendered.indexOf('  publish-on-linux:'));

  const registryEnv = {
    CARGO_REGISTRIES_EXAMPLE_TOKEN: '${{ secrets.CARGO_REGISTRIES_EXAMPLE_TOKEN }}',
    CARGO_REGISTRIES_FORGE_TOKEN: '${{ secrets.CARGO_REGISTRIES_FORGE_TOKEN }}',
  };
  expect(Bun.YAML.parse(rendered)).toMatchObject({
    jobs: {
      'linux-release-candidate': { env: registryEnv },
      'macos-platform': { env: registryEnv },
      'publish-on-linux': { env: registryEnv },
    },
  });
  expect(rendered).not.toContain('CARGO_NET_GIT_FETCH_WITH_CLI');
  expect(stepAnchorNumbers(macosPlatform)).toEqual(Array.from({ length: 11 }, (_, index) => index + 1));
});

it('refuses malformed Cargo credential declarations at render time', () => {
  expect(() => renderPublishWorkflowYaml({ cargoCredentials: {} })).toThrow('empty configuration');
  expect(() =>
    renderPublishWorkflowYaml({
      cargoCredentials: { gitOrigins: [{ origin: 'https://git.example.net/codebase', tokenEnv: 'SOURCE_READ_TOKEN' }] },
    }),
  ).toThrow('credential-free https origin without a path');
  expect(() =>
    renderPublishWorkflowYaml({
      cargoCredentials: { gitOrigins: [{ origin: 'https://x:y@git.example.net', tokenEnv: 'SOURCE_READ_TOKEN' }] },
    }),
  ).toThrow('credential-free https origin');
  expect(() =>
    renderPublishWorkflowYaml({
      cargoCredentials: { gitOrigins: [{ origin: 'https://git.example.net', tokenEnv: 'source_read_token' }] },
    }),
  ).toThrow('upper-case secret env name');
  expect(() => renderPublishWorkflowYaml({ cargoCredentials: { registryTokenEnvs: ['cargo_token'] } })).toThrow(
    'upper-case secret env names',
  );
});

/**
 * Deploy credentials are declared once, in `smoo.github.deploySecrets`, and
 * every managed workflow that deploys renders them the same way. This workflow
 * used to render the Cloudflare pair alone, so a repository whose production
 * deploy needs any other credential got a deploy that could not authenticate —
 * and got it silently, since nothing in the generated YAML mentioned the
 * declaration it dropped.
 *
 * Scope is the other half of the contract, and it is why these assertions read
 * the parsed workflow rather than the text: the job that deploys is also the job
 * that repairs pending releases and publishes to npm. Step `env` hands the
 * credentials to the deploy and to nothing else; job `env` would hand them to
 * checkout, setup, repair, and the publish itself.
 */
it('renders declared deploy secrets on the production deploy step of every deploying shape', () => {
  const deploySecrets = { BILLING_API_TOKEN: 'BILLING_API_TOKEN', STRIPE_SECRET_KEY: 'SMOO_STRIPE_SECRET_KEY' };
  const declared = {
    BILLING_API_TOKEN: '${{ secrets.BILLING_API_TOKEN }}',
    STRIPE_SECRET_KEY: '${{ secrets.SMOO_STRIPE_SECRET_KEY }}',
  };
  const cloudflare = {
    CLOUDFLARE_API_TOKEN: '${{ secrets.CLOUDFLARE_API_TOKEN }}',
    CLOUDFLARE_ACCOUNT_ID: '${{ secrets.CLOUDFLARE_ACCOUNT_ID }}',
  };
  const shapes: Array<[string, PublishWorkflowDefinitionOptions, Record<string, string>]> = [
    // Release plus deploy in one job.
    [
      'single-job',
      { deploy: true, deployProvider: 'cloudflare', repoName: '@example/app', deploySecrets },
      { ...cloudflare, ...declared },
    ],
    // Native platform producers, deploy in the final publishing job.
    [
      'platform',
      {
        deploy: true,
        deployProvider: 'cloudflare',
        repoName: '@smoothbricks/codebase',
        platformTargetGlobs: PLATFORM_TARGET_GLOBS,
        deploySecrets,
      },
      { ...cloudflare, ...declared },
    ],
    // Deploys but owns no release packages.
    [
      'deploy-only',
      { deploy: true, release: false, deployProvider: 'cloudflare', repoName: '@example/app', deploySecrets },
      { ...cloudflare, ...declared },
    ],
    // No provider default to fall back on: the declaration is the whole env.
    ['provider-free', { deploy: true, repoName: '@example/app', deploySecrets }, declared],
  ];

  for (const [shape, options, expected] of shapes) {
    const workflow = typia.assert<{
      jobs: Record<
        string,
        { env?: Record<string, string>; steps: Array<{ name?: string; env?: Record<string, string> }> }
      >;
    }>(Bun.YAML.parse(renderPublishWorkflowYaml(options)));
    const steps = Object.values(workflow.jobs).flatMap((job) => job.steps);
    const deploySteps = steps.filter((step) => step.name?.includes('Deploy production'));

    expect(`${shape}: ${deploySteps.length}`).toBe(`${shape}: 1`);
    expect({ shape, env: deploySteps[0]?.env }).toEqual({ shape, env: expected });

    const elsewhere = [
      ...Object.values(workflow.jobs).map((job) => job.env ?? {}),
      ...steps.filter((step) => step !== deploySteps[0]).map((step) => step.env ?? {}),
    ].flatMap((env) => Object.keys(env).filter((name) => name in deploySecrets));
    expect({ shape, elsewhere }).toEqual({ shape, elsewhere: [] });
  }
});
