import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { type ProjectTargets, readProjectTargets } from '../nx/index.js';
import { expandNxTargetDependencyRuns, expandNxTargetRuns } from './index.js';
import { applyCollectedOutputs, collectNxOutputs } from './outputs.js';

const SOURCE_SHA = 'c'.repeat(40);

function cowshedShapedProject(): ProjectTargets {
  return {
    project: 'cowshed',
    root: 'packages/cowshed',
    targets: [
      'build',
      'tsc-js',
      'cli-x64-linux',
      'cli-arm64-linux',
      'cli-arm64-macos',
      'napi-x64-linux',
      'napi-arm64-macos',
    ],
    targetDependencies: new Map([
      ['build', ['tsc-js', 'cli-arm64-macos', 'napi-arm64-macos']],
      ['cli-x64-linux', ['napi-toolchain-x64-linux']],
      ['napi-x64-linux', ['napi-toolchain-x64-linux']],
    ]),
    targetOutputs: new Map([
      ['tsc-js', ['{projectRoot}/dist/ts']],
      ['cli-x64-linux', ['{projectRoot}/dist/bin/linux-x64-gnu']],
      ['cli-arm64-linux', ['{projectRoot}/dist/bin/linux-arm64-gnu']],
      ['cli-arm64-macos', ['{projectRoot}/dist/bin/darwin-arm64']],
      ['napi-x64-linux', ['{projectRoot}/dist/native/linux-x64-gnu']],
      ['napi-arm64-macos', ['{projectRoot}/dist/native/darwin-arm64']],
    ]),
  };
}

async function withProbeRepo(
  run: (fixture: {
    root: string;
    buildArtifact: string;
    linuxArtifact: string;
    macosArmArtifact: string;
  }) => Promise<void>,
): Promise<void> {
  const temp = await mkdtemp(join(tmpdir(), 'containment-probe-'));
  const root = join(temp, 'repo');
  try {
    await mkdir(join(root, 'packages/cowshed/dist/ts'), { recursive: true });
    await mkdir(join(root, 'packages/cowshed/dist/bin/linux-x64-gnu'), { recursive: true });
    await mkdir(join(root, 'packages/cowshed/dist/bin/linux-arm64-gnu'), { recursive: true });
    await mkdir(join(root, 'packages/cowshed/dist/bin/darwin-arm64'), { recursive: true });
    await mkdir(join(root, 'packages/cowshed/dist/native/linux-x64-gnu'), { recursive: true });
    await mkdir(join(root, 'packages/cowshed/dist/native/darwin-arm64'), { recursive: true });
    await writeFile(join(root, 'packages/cowshed/dist/ts/cli.js'), 'ts entry');
    await writeFile(join(root, 'packages/cowshed/dist/bin/linux-x64-gnu/cowshed'), 'linux-x64 cli');
    await writeFile(join(root, 'packages/cowshed/dist/bin/linux-arm64-gnu/cowshed'), 'linux-arm64 cli');
    await writeFile(join(root, 'packages/cowshed/dist/bin/darwin-arm64/cowshed'), 'darwin-arm64 cli');
    await writeFile(join(root, 'packages/cowshed/dist/native/linux-x64-gnu/cowshed.node'), 'linux-x64 napi');
    await writeFile(join(root, 'packages/cowshed/dist/native/darwin-arm64/cowshed.node'), 'darwin-arm64 napi');
    await run({
      root,
      buildArtifact: join(temp, 'release-build-outputs'),
      linuxArtifact: join(temp, 'linux-platform-outputs'),
      macosArmArtifact: join(temp, 'macos-arm64-outputs'),
    });
  } finally {
    await rm(temp, { recursive: true, force: true });
  }
}

describe('publish collect containment probe', () => {
  it('does not expand *-linux or *-macos collects to cowshed:build', () => {
    const project = cowshedShapedProject();
    const linux = expandNxTargetRuns([project], { targets: '*-linux', projects: 'cowshed' });
    const linuxClosure = expandNxTargetDependencyRuns(linux.runs);
    expect(linux.runs.map((run) => run.target).sort()).toEqual(['cli-arm64-linux', 'cli-x64-linux', 'napi-x64-linux']);
    expect(linuxClosure.map((run) => run.target).sort()).toEqual([
      'cli-arm64-linux',
      'cli-x64-linux',
      'napi-x64-linux',
    ]);

    const macosArm = expandNxTargetRuns([project], { targets: '*-arm64-macos', projects: 'cowshed' });
    const macosClosure = expandNxTargetDependencyRuns(macosArm.runs);
    expect(macosArm.runs.map((run) => run.target).sort()).toEqual(['cli-arm64-macos', 'napi-arm64-macos']);
    expect(macosClosure.map((run) => run.target).sort()).toEqual(['cli-arm64-macos', 'napi-arm64-macos']);

    const build = expandNxTargetRuns([project], { targets: 'build', projects: 'cowshed' });
    const buildClosure = expandNxTargetDependencyRuns(build.runs);
    expect(build.runs.map((run) => run.target)).toEqual(['build']);
    expect(buildClosure.map((run) => run.target).sort()).toEqual(['tsc-js']);
  });

  it('applies build and linux trees together once the aggregate claims no dist', async () => {
    await withProbeRepo(async ({ root, buildArtifact, linuxArtifact }) => {
      const project = cowshedShapedProject();
      const buildRuns = expandNxTargetDependencyRuns(
        expandNxTargetRuns([project], { targets: 'build', projects: 'cowshed' }).runs,
      );
      const linuxRuns = expandNxTargetDependencyRuns(
        expandNxTargetRuns([project], { targets: '*-linux', projects: 'cowshed' }).runs,
      );

      const buildManifest = await collectNxOutputs(root, buildArtifact, buildRuns, SOURCE_SHA);
      const linuxManifest = await collectNxOutputs(root, linuxArtifact, linuxRuns, SOURCE_SHA);

      const linuxBin = 'packages/cowshed/dist/bin/linux-x64-gnu/cowshed';
      expect(buildManifest.files.some((file) => file.target === 'build')).toBe(false);
      expect(buildManifest.files.map((file) => `${file.target}:${file.path}`)).toEqual([
        'tsc-js:packages/cowshed/dist/ts/cli.js',
      ]);
      expect(linuxManifest.files.some((file) => file.path === linuxBin && file.target === 'cli-x64-linux')).toBe(true);

      await expect(
        applyCollectedOutputs(root, [buildArtifact, linuxArtifact], SOURCE_SHA, [project]),
      ).resolves.toBeUndefined();
    });
  });

  it('still refuses apply when a leftover aggregate output swallows the platform binary', async () => {
    await withProbeRepo(async ({ root, buildArtifact, linuxArtifact }) => {
      const project = cowshedShapedProject();
      project.targetOutputs?.set('build', ['{projectRoot}/dist']);
      const buildRuns = expandNxTargetDependencyRuns(
        expandNxTargetRuns([project], { targets: 'build', projects: 'cowshed' }).runs,
      );
      const linuxRuns = expandNxTargetDependencyRuns(
        expandNxTargetRuns([project], { targets: '*-linux', projects: 'cowshed' }).runs,
      );

      const buildManifest = await collectNxOutputs(root, buildArtifact, buildRuns, SOURCE_SHA);
      const linuxManifest = await collectNxOutputs(root, linuxArtifact, linuxRuns, SOURCE_SHA);

      const linuxBin = 'packages/cowshed/dist/bin/linux-x64-gnu/cowshed';
      expect(buildManifest.files.some((file) => file.path === linuxBin)).toBe(true);
      expect(linuxManifest.files.some((file) => file.path === linuxBin && file.target === 'cli-x64-linux')).toBe(true);

      await expect(applyCollectedOutputs(root, [buildArtifact, linuxArtifact], SOURCE_SHA, [project])).rejects.toThrow(
        'Output collision across collected trees',
      );
    });
  });

  it('keeps the linux platform binary in the *-linux collect tree', async () => {
    await withProbeRepo(async ({ root, linuxArtifact }) => {
      const project = cowshedShapedProject();
      const linuxRuns = expandNxTargetDependencyRuns(
        expandNxTargetRuns([project], { targets: '*-linux', projects: 'cowshed' }).runs,
      );
      const linuxManifest = await collectNxOutputs(root, linuxArtifact, linuxRuns, SOURCE_SHA);
      expect(linuxManifest.files.map((file) => `${file.target}:${file.path}`)).toContain(
        'cli-x64-linux:packages/cowshed/dist/bin/linux-x64-gnu/cowshed',
      );
    });
  });

  it('still refuses a genuine two-producers-one-file collision', async () => {
    await withProbeRepo(async ({ root, buildArtifact, linuxArtifact }) => {
      const first: ProjectTargets = {
        project: 'alpha',
        root: 'packages/cowshed',
        targets: ['cli-x64-linux'],
        targetOutputs: new Map([['cli-x64-linux', ['{projectRoot}/dist/bin/linux-x64-gnu']]]),
      };
      const second: ProjectTargets = {
        project: 'beta',
        root: 'packages/cowshed',
        targets: ['other-x64-linux'],
        targetOutputs: new Map([['other-x64-linux', ['{projectRoot}/dist/bin/linux-x64-gnu']]]),
      };
      await collectNxOutputs(root, buildArtifact, [{ target: 'cli-x64-linux', projects: [first] }], SOURCE_SHA);
      await collectNxOutputs(root, linuxArtifact, [{ target: 'other-x64-linux', projects: [second] }], SOURCE_SHA);
      await expect(
        applyCollectedOutputs(root, [buildArtifact, linuxArtifact], SOURCE_SHA, [first, second]),
      ).rejects.toThrow('Output collision across collected trees');
    });
  });

  it('does not collide macos arm64 current against a disjoint x64 current tree', async () => {
    await withProbeRepo(async ({ root, macosArmArtifact, linuxArtifact }) => {
      const project = cowshedShapedProject();
      project.targets = [...project.targets, 'cli-x64-macos'];
      project.targetOutputs?.set('cli-x64-macos', ['{projectRoot}/dist/bin/darwin-x64']);
      const macosRuns = expandNxTargetDependencyRuns(
        expandNxTargetRuns([project], { targets: '*-arm64-macos', projects: 'cowshed' }).runs,
      );
      await mkdir(join(root, 'packages/cowshed/dist/bin/darwin-x64'), { recursive: true });
      await writeFile(join(root, 'packages/cowshed/dist/bin/darwin-x64/cowshed'), 'darwin-x64 cli');
      const macosManifest = await collectNxOutputs(root, macosArmArtifact, macosRuns, SOURCE_SHA);
      const x64Manifest = await collectNxOutputs(
        root,
        linuxArtifact,
        [{ target: 'cli-x64-macos', projects: [project] }],
        SOURCE_SHA,
      );
      expect(macosManifest.files.map((file) => file.path)).toEqual([
        'packages/cowshed/dist/bin/darwin-arm64/cowshed',
        'packages/cowshed/dist/native/darwin-arm64/cowshed.node',
      ]);
      expect(x64Manifest.files.map((file) => file.path)).toEqual(['packages/cowshed/dist/bin/darwin-x64/cowshed']);
      await expect(
        applyCollectedOutputs(root, [macosArmArtifact, linuxArtifact], SOURCE_SHA, [project]),
      ).resolves.toBeUndefined();
    });
  });
});

describe('publish collect containment probe against the resolved graph', () => {
  it('collects cowshed tsc-js and macos platform trees without overlap', async () => {
    const root = join(import.meta.dir, '../../../..');
    const projects = await readProjectTargets(root);
    const cowshed = projects.find((project) => project.project === 'cowshed');
    expect(cowshed?.targetOutputs?.get('build')).toBeUndefined();
    expect(cowshed?.targetOutputs?.get('tsc-js')).toEqual(['{projectRoot}/dist/ts']);

    const buildRuns = expandNxTargetDependencyRuns(
      expandNxTargetRuns(projects, { targets: 'build', projects: 'cowshed' }).runs,
    );
    const macosRuns = expandNxTargetDependencyRuns(
      expandNxTargetRuns(projects, { targets: '*-macos', projects: 'cowshed' }).runs,
    );
    expect(buildRuns.some((run) => run.target === 'build')).toBe(false);
    expect(buildRuns.some((run) => run.target === 'tsc-js' && run.projects[0]?.project === 'cowshed')).toBe(true);

    const temp = await mkdtemp(join(tmpdir(), 'containment-real-graph-'));
    const buildArtifact = join(temp, 'rb');
    const macosArtifact = join(temp, 'mp');
    const applyRoot = join(temp, 'apply');
    try {
      const buildManifest = await collectNxOutputs(root, buildArtifact, buildRuns, SOURCE_SHA);
      const macosManifest = await collectNxOutputs(root, macosArtifact, macosRuns, SOURCE_SHA);

      const tsFiles = buildManifest.files.filter(
        (file) =>
          file.project === 'cowshed' && file.target === 'tsc-js' && file.path.startsWith('packages/cowshed/dist/ts/'),
      );
      expect(buildManifest.files.some((file) => file.project === 'cowshed' && file.target === 'build')).toBe(false);
      expect(tsFiles).toHaveLength(19);
      expect(buildRuns.map((run) => run.target).sort()).toContain('tsc-js');
      expect(buildRuns.map((run) => run.target).sort()).not.toContain('build');
      expect(
        buildManifest.files.some((file) => file.path.includes('/dist/bin/') || file.path.includes('/dist/native/')),
      ).toBe(false);

      const macosBins = macosManifest.files.filter((file) => file.path.includes('/dist/bin/'));
      const macosNative = macosManifest.files.filter((file) => file.path.includes('/dist/native/'));
      expect(macosBins.map((file) => file.path).sort()).toEqual([
        'packages/cowshed/dist/bin/darwin-arm64/cowshed',
        'packages/cowshed/dist/bin/darwin-x64/cowshed',
      ]);
      expect(macosNative).toHaveLength(4);
      expect(macosManifest.files).toHaveLength(6);

      await mkdir(applyRoot, { recursive: true });
      await applyCollectedOutputs(applyRoot, [buildArtifact, macosArtifact], SOURCE_SHA, projects);
      expect((await stat(join(applyRoot, 'packages/cowshed/dist/bin/darwin-arm64/cowshed'))).mode & 0o777).toBe(0o755);
    } finally {
      await rm(temp, { recursive: true, force: true });
    }
  });
});
