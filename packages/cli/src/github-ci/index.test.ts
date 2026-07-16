import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { $ } from 'bun';
import { decode } from '../lib/run.js';
import type { ProjectTargets } from '../nx/index.js';
import {
  expandNxTargetDependencyRuns,
  expandNxTargetRuns,
  nxRunManyArgs,
  nxSmartArgs,
  readGitHeadSha,
} from './index.js';
import {
  applyCollectedOutputs,
  type CollectedOutputsManifest,
  collectNxOutputs,
  resolveDeclaredOutput,
} from './outputs.js';

const SOURCE_SHA = 'a'.repeat(40);
const OTHER_SHA = 'b'.repeat(40);

const projects: ProjectTargets[] = [
  { project: 'api', root: 'packages/api', targets: ['build', 'test'] },
  { project: 'desktop', root: 'packages/desktop', targets: ['build-macos', 'package-macos', 'test'] },
  { project: 'mobile', root: 'packages/mobile', targets: ['build-ios'] },
];

describe('GitHub CI Nx target expansion', () => {
  it('groups an exact target with only projects that own it', () => {
    const expanded = expandNxTargetRuns(projects, { targets: 'test' });

    expect(expanded.unmatchedGlobs).toEqual([]);
    expect(expanded.runs.map((run) => [run.target, run.projects.map((project) => project.project)])).toEqual([
      ['test', ['api', 'desktop']],
    ]);
    expect(expanded.runs.map((run) => nxRunManyArgs(run))).toEqual([
      ['run-many', '-t', 'test', '--projects=api,desktop', '--parallel=5'],
    ]);
  });

  it('expands target globs across heterogeneous projects', () => {
    const expanded = expandNxTargetRuns(projects, { targets: '*-macos,*-ios' });

    expect(expanded.unmatchedGlobs).toEqual([]);
    expect(expanded.runs.map((run) => [run.target, run.projects.map((project) => project.project)])).toEqual([
      ['build-macos', ['desktop']],
      ['package-macos', ['desktop']],
      ['build-ios', ['mobile']],
    ]);
  });

  it('reports a zero-match glob as an empty run set', () => {
    expect(expandNxTargetRuns(projects, { targets: '*-windows' })).toEqual({
      runs: [],
      unmatchedGlobs: ['*-windows'],
    });
  });

  it('passes an unknown exact target to Nx so Nx retains failure behavior', () => {
    const expanded = expandNxTargetRuns(projects, { targets: 'missing', projects: 'api' });

    expect(expanded.unmatchedGlobs).toEqual([]);
    expect(expanded.runs.map((run) => nxRunManyArgs(run))).toEqual([
      ['run-many', '-t', 'missing', '--projects=api', '--parallel=5'],
    ]);
  });

  it('scopes glob candidates to selected projects and rejects empty project selections', () => {
    expect(expandNxTargetRuns(projects, { targets: '*-macos', projects: 'api' })).toEqual({
      runs: [],
      unmatchedGlobs: ['*-macos'],
    });
    expect(() => expandNxTargetRuns(projects, { targets: 'test', projects: 'missing-*' })).toThrow(
      'No Nx projects matched',
    );
    expect(expandNxTargetRuns([], { targets: 'missing' })).toEqual({ runs: [], unmatchedGlobs: [] });
    expect(() => nxRunManyArgs({ target: 'test', projects: [] })).toThrow('has no selected projects');
  });

  it('expands same-project target dependencies in dependency-first order exactly once', () => {
    const project: ProjectTargets = {
      project: 'native',
      root: 'packages/native',
      targets: ['build', 'compile-linux', 'package-linux'],
      targetDependencies: new Map([
        ['build', ['compile-*', 'compile-linux']],
        ['compile-linux', ['package-linux', '^build']],
      ]),
      targetOutputs: new Map([
        ['build', ['{projectRoot}/dist']],
        ['compile-linux', ['{projectRoot}/dist/**/*.bin']],
        ['package-linux', ['{projectRoot}/dist/**/*.tar']],
      ]),
    };

    expect(
      expandNxTargetDependencyRuns([{ target: 'build', projects: [project] }]).map(
        (run) => `${run.projects.map((owner) => owner.project).join(',')}:${run.target}`,
      ),
    ).toEqual(['native:package-linux', 'native:compile-linux', 'native:build']);
  });

  it('adds the generic target skip tag only to nx-smart', () => {
    expect(nxSmartArgs('test', 'affected')).toEqual([
      'affected',
      '-t',
      'test',
      '--exclude=tag:ci:skip:test',
      '--parallel=5',
    ]);
    expect(nxRunManyArgs({ target: 'test', projects: projects.slice(0, 1) })).not.toContain(
      '--exclude=tag:ci:skip:test',
    );
  });
});

describe('collected Nx outputs', () => {
  it('derives artifact provenance from the checked-out HEAD', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-output-head-'));
    try {
      await $`git init -q`.cwd(root).quiet();
      await writeFile(join(root, 'artifact-source.txt'), 'release candidate');
      await $`git add artifact-source.txt`.cwd(root).quiet();
      await $`git -c user.name=Test -c user.email=test@example.com commit -q -m initial`.cwd(root).quiet();
      const expected = decode((await $`git rev-parse HEAD`.cwd(root).quiet()).stdout).trim();

      expect(await readGitHeadSha(root)).toBe(expected);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('collects aggregate same-project dependency outputs once with dependency ownership', async () => {
    await withOutputFixture(async ({ root, artifact, outputProject }) => {
      outputProject.targets = ['build', 'compile-linux', 'package-linux'];
      outputProject.targetDependencies = new Map([
        ['build', ['compile-linux']],
        ['compile-linux', ['package-linux']],
      ]);
      outputProject.targetOutputs = new Map([
        ['build', ['{projectRoot}/dist']],
        ['compile-linux', ['{projectRoot}/dist/**/*.bin']],
        ['package-linux', ['{projectRoot}/dist/package/**/*.tar']],
      ]);
      await mkdir(join(root, 'packages/app/dist/package'), { recursive: true });
      await writeFile(join(root, 'packages/app/dist/result.bin'), 'linux binary');
      await writeFile(join(root, 'packages/app/dist/package/release.tar'), 'linux package');

      const runs = expandNxTargetDependencyRuns([{ target: 'build', projects: [outputProject] }]);
      const manifest = await collectNxOutputs(root, artifact, runs, SOURCE_SHA);

      expect(manifest.files.map((file) => `${file.project}:${file.target}:${file.path}`)).toEqual([
        'app:package-linux:packages/app/dist/package/release.tar',
        'app:compile-linux:packages/app/dist/result.bin',
      ]);
    });
  });

  it('collects brace-expanded TypeScript outputs without accepting unresolved Nx placeholders', async () => {
    await withOutputFixture(async ({ root, artifact, outputProject }) => {
      const declaredOutput = '{projectRoot}/dist/**/*.{js,cjs,mjs,jsx,d.ts,d.cts,d.mts}{,.map}';
      outputProject.targetOutputs = new Map([['build-macos', [declaredOutput]]]);
      await writeFile(join(root, 'packages/app/dist/index.js'), 'javascript');
      await writeFile(join(root, 'packages/app/dist/index.d.ts'), 'declaration');
      await writeFile(join(root, 'packages/app/dist/index.d.ts.map'), 'source map');
      await writeFile(join(root, 'packages/app/dist/index.css'), 'not declared');

      const manifest = await collectNxOutputs(
        root,
        artifact,
        [{ target: 'build-macos', projects: [outputProject] }],
        SOURCE_SHA,
      );

      expect(resolveDeclaredOutput(declaredOutput, outputProject)).toBe(
        'packages/app/dist/**/*.{js,cjs,mjs,jsx,d.ts,d.cts,d.mts}{,.map}',
      );
      expect(manifest.files.map((file) => file.path)).toEqual([
        'packages/app/dist/index.d.ts',
        'packages/app/dist/index.d.ts.map',
        'packages/app/dist/index.js',
      ]);
      expect(() => resolveDeclaredOutput('{options.outputPath}', outputProject)).toThrow(
        'Unsupported Nx output placeholder',
      );
    });
  });

  it('rejects overlapping outputs from unrelated targets', async () => {
    await withOutputFixture(async ({ root, artifact, outputProject }) => {
      outputProject.targets = ['first-linux', 'second-linux'];
      outputProject.targetOutputs = new Map([
        ['first-linux', ['{projectRoot}/dist/**/*.bin']],
        ['second-linux', ['{projectRoot}/dist/**/*.bin']],
      ]);
      await writeFile(join(root, 'packages/app/dist/result.bin'), 'collision');

      await expect(
        collectNxOutputs(
          root,
          artifact,
          [
            { target: 'first-linux', projects: [outputProject] },
            { target: 'second-linux', projects: [outputProject] },
          ],
          SOURCE_SHA,
        ),
      ).rejects.toThrow('Output collision');
    });
  });

  it('collects declared files and applies a verified overlay', async () => {
    await withOutputFixture(async ({ root, artifact, outputProject }) => {
      const source = join(root, 'packages/app/dist/result.bin');
      await writeFile(source, 'native artifact');

      const manifest = await collectNxOutputs(
        root,
        artifact,
        [{ target: 'build-macos', projects: [outputProject] }],
        SOURCE_SHA,
      );
      expect(manifest).toMatchObject({
        version: 1,
        sourceSha: SOURCE_SHA,
        files: [
          {
            project: 'app',
            target: 'build-macos',
            output: '{projectRoot}/dist/**/*.bin',
            path: 'packages/app/dist/result.bin',
            size: 15,
          },
        ],
      });
      expect(await readFile(join(artifact, 'workspace/packages/app/dist/result.bin'), 'utf8')).toBe('native artifact');

      await rm(join(root, 'packages/app/dist'), { recursive: true, force: true });
      await applyCollectedOutputs(root, [artifact], SOURCE_SHA, [outputProject]);
      expect(await readFile(source, 'utf8')).toBe('native artifact');
    });
  });

  it('rejects missing, escaping, and symlinked declared outputs', async () => {
    await withOutputFixture(async ({ root, artifact, outputProject, temp }) => {
      await expect(
        collectNxOutputs(root, artifact, [{ target: 'build-macos', projects: [outputProject] }], SOURCE_SHA),
      ).rejects.toThrow('is missing');

      outputProject.targetOutputs = new Map([['build-macos', ['../outside']]]);
      await expect(
        collectNxOutputs(root, artifact, [{ target: 'build-macos', projects: [outputProject] }], SOURCE_SHA),
      ).rejects.toThrow('escapes the workspace');

      const outside = join(temp, 'outside-output');
      await mkdir(join(outside, 'dist'), { recursive: true });
      await writeFile(join(outside, 'dist/result.bin'), 'outside');
      await rm(join(root, 'packages/app'), { recursive: true, force: true });
      await symlink(outside, join(root, 'packages/app'), 'dir');
      outputProject.targetOutputs = new Map([['build-macos', ['{projectRoot}/dist/**/*.bin']]]);
      await expect(
        collectNxOutputs(root, artifact, [{ target: 'build-macos', projects: [outputProject] }], SOURCE_SHA),
      ).rejects.toThrow('symbolic link');
    });
  });

  it('rejects checksum corruption, source mismatches, and undeclared staged files', async () => {
    await withOutputFixture(async ({ root, artifact, outputProject }) => {
      await writeFile(join(root, 'packages/app/dist/result.bin'), 'native artifact');
      await collectNxOutputs(root, artifact, [{ target: 'build-macos', projects: [outputProject] }], SOURCE_SHA);

      await expect(applyCollectedOutputs(root, [artifact], OTHER_SHA, [outputProject])).rejects.toThrow(
        'Source SHA mismatch',
      );

      const staged = join(artifact, 'workspace/packages/app/dist/result.bin');
      await writeFile(staged, 'corrupt artifact');
      await expect(applyCollectedOutputs(root, [artifact], SOURCE_SHA, [outputProject])).rejects.toThrow(/mismatch/);

      await writeFile(staged, 'native artifact');
      await writeFile(join(artifact, 'workspace/undeclared.txt'), 'extra');
      await expect(applyCollectedOutputs(root, [artifact], SOURCE_SHA, [outputProject])).rejects.toThrow(
        'Undeclared staged output',
      );
    });
  });

  it('rejects path injection, exact-shape violations, and collisions before overlaying', async () => {
    await withOutputFixture(async ({ root, artifact, outputProject, temp }) => {
      await writeFile(join(root, 'packages/app/dist/result.bin'), 'native artifact');
      const manifest = await collectNxOutputs(
        root,
        artifact,
        [{ target: 'build-macos', projects: [outputProject] }],
        SOURCE_SHA,
      );

      const malicious: CollectedOutputsManifest = {
        ...manifest,
        files: manifest.files.map((file, index) => (index === 0 ? { ...file, path: '../escape.bin' } : file)),
      };
      await writeFile(join(artifact, 'manifest.json'), `${JSON.stringify(malicious)}\n`);
      await expect(applyCollectedOutputs(root, [artifact], SOURCE_SHA, [outputProject])).rejects.toThrow(
        'escapes the workspace',
      );

      await writeFile(join(artifact, 'manifest.json'), `${JSON.stringify({ ...manifest, unexpected: true })}\n`);
      await expect(applyCollectedOutputs(root, [artifact], SOURCE_SHA, [outputProject])).rejects.toThrow(
        'Invalid collected output manifest',
      );

      await writeFile(
        join(artifact, 'manifest.json'),
        `${JSON.stringify({
          ...manifest,
          files: manifest.files.map((file) => ({ ...file, output: '{workspaceRoot}/**/*' })),
        })}\n`,
      );
      await expect(applyCollectedOutputs(root, [artifact], SOURCE_SHA, [outputProject])).rejects.toThrow(
        'is not declared by Nx target',
      );

      await writeFile(
        join(artifact, 'manifest.json'),
        `${JSON.stringify({
          ...manifest,
          files: manifest.files.map((file) => ({ ...file, project: 'unknown-project' })),
        })}\n`,
      );
      await expect(applyCollectedOutputs(root, [artifact], SOURCE_SHA, [outputProject])).rejects.toThrow(
        'unknown Nx project',
      );

      await writeFile(
        join(artifact, 'manifest.json'),
        `${JSON.stringify({
          ...manifest,
          files: manifest.files.map((file) => ({ ...file, target: '*-macos' })),
        })}\n`,
      );
      await expect(applyCollectedOutputs(root, [artifact], SOURCE_SHA, [outputProject])).rejects.toThrow(
        'must be an exact Nx name',
      );

      await writeFile(join(artifact, 'manifest.json'), `${JSON.stringify(manifest)}\n`);
      const secondArtifact = join(temp, 'artifact-two');
      await mkdir(secondArtifact, { recursive: true });
      await Bun.write(join(secondArtifact, 'manifest.json'), JSON.stringify(manifest));
      await mkdir(join(secondArtifact, 'workspace/packages/app/dist'), { recursive: true });
      await Bun.write(join(secondArtifact, 'workspace/packages/app/dist/result.bin'), 'native artifact');
      await expect(
        applyCollectedOutputs(root, [artifact, secondArtifact], SOURCE_SHA, [outputProject]),
      ).rejects.toThrow('Output collision across collected trees');
    });
  });

  it('rejects non-hex source SHAs', async () => {
    await withOutputFixture(async ({ root, artifact, outputProject }) => {
      await expect(
        collectNxOutputs(root, artifact, [{ target: 'build-macos', projects: [outputProject] }], 'not-a-git-sha'),
      ).rejects.toThrow('40- or 64-character hexadecimal Git SHA');
      await expect(applyCollectedOutputs(root, [artifact], 'not-a-git-sha', [outputProject])).rejects.toThrow(
        '40- or 64-character hexadecimal Git SHA',
      );
    });
  });

  it('rejects symlinks in destination ancestors and destination files', async () => {
    await withOutputFixture(async ({ root, artifact, outputProject, temp }) => {
      await writeFile(join(root, 'packages/app/dist/result.bin'), 'native artifact');
      await collectNxOutputs(root, artifact, [{ target: 'build-macos', projects: [outputProject] }], SOURCE_SHA);

      const outside = join(temp, 'outside');
      await mkdir(outside);
      await rm(join(root, 'packages/app'), { recursive: true, force: true });
      await symlink(outside, join(root, 'packages/app'), 'dir');

      await expect(applyCollectedOutputs(root, [artifact], SOURCE_SHA, [outputProject])).rejects.toThrow(
        'symbolic link',
      );
      expect(await Bun.file(join(outside, 'dist/result.bin')).exists()).toBe(false);
    });

    await withOutputFixture(async ({ root, artifact, outputProject, temp }) => {
      const output = join(root, 'packages/app/dist/result.bin');
      await writeFile(output, 'native artifact');
      await collectNxOutputs(root, artifact, [{ target: 'build-macos', projects: [outputProject] }], SOURCE_SHA);

      const outside = join(temp, 'outside.bin');
      await writeFile(outside, 'must remain unchanged');
      await rm(output);
      await symlink(outside, output, 'file');

      await expect(applyCollectedOutputs(root, [artifact], SOURCE_SHA, [outputProject])).rejects.toThrow(
        'symbolic link',
      );
      expect(await readFile(outside, 'utf8')).toBe('must remain unchanged');
    });
  });
});

async function withOutputFixture(
  run: (fixture: { root: string; artifact: string; outputProject: ProjectTargets; temp: string }) => Promise<void>,
): Promise<void> {
  const temp = await mkdtemp(join(tmpdir(), 'smoo-platform-output-'));
  const root = join(temp, 'repo');
  const artifact = join(temp, 'artifact');
  const outputProject: ProjectTargets = {
    project: 'app',
    root: 'packages/app',
    targets: ['build-macos'],
    targetOutputs: new Map([['build-macos', ['{projectRoot}/dist/**/*.bin']]]),
  };
  try {
    await mkdir(join(root, 'packages/app/dist'), { recursive: true });
    await run({ root, artifact, outputProject, temp });
  } finally {
    await rm(temp, { recursive: true, force: true });
  }
}
