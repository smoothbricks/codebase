import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import { type ProjectTargets, readProjectTargets } from '../nx/index.js';
import { expandNxTargetDependencyRuns, expandNxTargetRuns } from './index.js';
import {
  applyCollectedOutputs,
  type CollectedTree,
  collectNxOutputs,
  judgeOutputFile,
  type OutputClaim,
  type OutputFileFact,
  type OutputStage,
  type Planned,
  planOutputClaims,
  planOverlays,
  resolveDeclaredOutput,
  type StagedObservation,
  type StagedOutputFile,
} from './outputs.js';

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
    targetDependencies: new Map([['build', ['tsc-js', 'cli-arm64-macos', 'napi-arm64-macos']]]),
    targetOutputs: new Map([
      ['tsc-js', ['{projectRoot}/dist/ts']],
      ['cli-x64-linux', ['{projectRoot}/dist/bin/linux-x64']],
      ['cli-arm64-linux', ['{projectRoot}/dist/bin/linux-arm64']],
      ['cli-arm64-macos', ['{projectRoot}/dist/bin/darwin-arm64']],
      ['napi-x64-linux', ['{projectRoot}/dist/native/linux-x64-gnu']],
      ['napi-arm64-macos', ['{projectRoot}/dist/native/darwin-arm64']],
    ]),
  };
}

const DIGEST = 'd'.repeat(64);
const OTHER_DIGEST = 'e'.repeat(64);
const PACKAGES = [{ name: 'cowshed', version: '1.0.0' }];
const TS_ENTRY = 'packages/cowshed/dist/ts/cli.js';
const LINUX_X64_BIN = 'packages/cowshed/dist/bin/linux-x64/cowshed';
const LINUX_ARM64_BIN = 'packages/cowshed/dist/bin/linux-arm64/cowshed';
const LINUX_X64_NAPI = 'packages/cowshed/dist/native/linux-x64-gnu/cowshed.node';
const DARWIN_ARM64_BIN = 'packages/cowshed/dist/bin/darwin-arm64/cowshed';
const DARWIN_ARM64_NAPI = 'packages/cowshed/dist/native/darwin-arm64/cowshed.node';
const DARWIN_X64_BIN = 'packages/cowshed/dist/bin/darwin-x64/cowshed';

/** The one claim `tsc-js` makes, exactly as the collect planner records it. */
const TS_CLAIM: OutputClaim = {
  project: 'cowshed',
  target: 'tsc-js',
  output: '{projectRoot}/dist/ts',
  path: TS_ENTRY,
  size: 4,
  mode: 0o644,
};

/**
 * A file fact as `lstat` would report it. Size and mode are fixed because the planner judges the facts it is handed;
 * the on-disk tests are where real sizes, modes and links are exercised.
 */
function fileFact(path: string, kind: OutputFileFact['kind'] = 'file'): OutputFileFact {
  return { path, kind, size: 4, mode: 0o644 };
}

/** A collect verdict the test expects to be a plan; a refusal fails the test with its reason. */
function expectPlanned<T>(plan: Planned<T>): T {
  if (!plan.ok) {
    throw new Error(`Expected a plan, got a refusal: ${plan.reason}`);
  }
  return plan.value;
}

/**
 * A collected tree as `collectNxOutputs` stages it: every claim is present at its declared size and hashed to DIGEST,
 * so the only judgement left to the planner is the one a test varies.
 */
function stagedTree(name: string, claims: readonly OutputClaim[]): CollectedTree {
  const workspacePath = `/collected/${name}/workspace`;
  return {
    manifestPath: `/collected/${name}/manifest.json`,
    manifest: { sourceSha: SOURCE_SHA, packageVersions: { cowshed: '1.0.0' } },
    workspacePath,
    files: claims.map(
      (claim): StagedOutputFile => ({
        entry: { ...claim, sha256: DIGEST },
        source: `${workspacePath}/${claim.path}`,
        observed: { kind: 'regular', size: claim.size, sha256: DIGEST },
      }),
    ),
    workspace: { kind: 'directory', regularFiles: claims.map((claim) => claim.path) },
  };
}

/** The same tree with every staged file observed as `observed`, so a test varies one on-disk fact at a time. */
function observing(tree: CollectedTree, observed: StagedObservation): CollectedTree {
  return { ...tree, files: tree.files.map((file): StagedOutputFile => ({ ...file, observed })) };
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

  it('applies build and linux trees together once the aggregate claims no dist', () => {
    const project = cowshedShapedProject();
    const buildClaims = expectPlanned(
      planOutputClaims([
        {
          project,
          target: 'tsc-js',
          outputs: [{ declaredOutput: '{projectRoot}/dist/ts', files: [fileFact(TS_ENTRY)] }],
        },
      ]),
    );
    const linuxClaims = expectPlanned(
      planOutputClaims([
        {
          project,
          target: 'cli-x64-linux',
          outputs: [{ declaredOutput: '{projectRoot}/dist/bin/linux-x64', files: [fileFact(LINUX_X64_BIN)] }],
        },
      ]),
    );

    const trees = [stagedTree('build', buildClaims), stagedTree('linux', linuxClaims)];
    expect(planOverlays(trees, SOURCE_SHA, [project], PACKAGES)).toEqual({
      ok: true,
      value: [
        { source: `/collected/build/workspace/${TS_ENTRY}`, destination: TS_ENTRY, mode: 0o644 },
        { source: `/collected/linux/workspace/${LINUX_X64_BIN}`, destination: LINUX_X64_BIN, mode: 0o644 },
      ],
    });
  });

  it('still refuses apply when a leftover aggregate output swallows the platform binary', () => {
    const project = cowshedShapedProject();
    project.targetOutputs?.set('build', ['{projectRoot}/dist']);
    const distFiles = [TS_ENTRY, LINUX_X64_BIN, LINUX_ARM64_BIN, DARWIN_ARM64_BIN, LINUX_X64_NAPI, DARWIN_ARM64_NAPI];
    const buildClaims = expectPlanned(
      planOutputClaims([
        {
          project,
          target: 'build',
          outputs: [{ declaredOutput: '{projectRoot}/dist', files: distFiles.map((path) => fileFact(path)) }],
        },
      ]),
    );
    const linuxClaims = expectPlanned(
      planOutputClaims([
        {
          project,
          target: 'cli-x64-linux',
          outputs: [{ declaredOutput: '{projectRoot}/dist/bin/linux-x64', files: [fileFact(LINUX_X64_BIN)] }],
        },
      ]),
    );
    expect(buildClaims.map((claim) => claim.path)).toContain(LINUX_X64_BIN);

    const trees = [stagedTree('build', buildClaims), stagedTree('linux', linuxClaims)];
    expect(planOverlays(trees, SOURCE_SHA, [project], PACKAGES)).toEqual({
      ok: false,
      reason: `Output collision across collected trees in /collected/linux/manifest.json: ${LINUX_X64_BIN}`,
    });
  });

  it('keeps the linux platform binary in the *-linux collect tree', () => {
    const project = cowshedShapedProject();
    expect(
      planOutputClaims([
        {
          project,
          target: 'cli-x64-linux',
          outputs: [{ declaredOutput: '{projectRoot}/dist/bin/linux-x64', files: [fileFact(LINUX_X64_BIN)] }],
        },
      ]),
    ).toEqual({
      ok: true,
      value: [
        {
          project: 'cowshed',
          target: 'cli-x64-linux',
          output: '{projectRoot}/dist/bin/linux-x64',
          path: LINUX_X64_BIN,
          size: 4,
          mode: 0o644,
        },
      ],
    });
  });

  it('still refuses a genuine two-producers-one-file collision', () => {
    const alpha: ProjectTargets = {
      ...cowshedShapedProject(),
      project: 'alpha',
      targets: ['cli-x64-linux'],
      targetOutputs: new Map([['cli-x64-linux', ['{projectRoot}/dist/bin/linux-x64']]]),
    };
    const beta: ProjectTargets = {
      ...cowshedShapedProject(),
      project: 'beta',
      targets: ['other-x64-linux'],
      targetOutputs: new Map([['other-x64-linux', ['{projectRoot}/dist/bin/linux-x64']]]),
    };
    const outputs = [{ declaredOutput: '{projectRoot}/dist/bin/linux-x64', files: [fileFact(LINUX_X64_BIN)] }];

    expect(
      planOutputClaims([
        { project: alpha, target: 'cli-x64-linux', outputs },
        { project: beta, target: 'other-x64-linux', outputs },
      ]),
    ).toEqual({
      ok: false,
      reason: `Output collision: ${LINUX_X64_BIN} is declared by more than one project target or output.`,
    });

    const alphaClaims = expectPlanned(planOutputClaims([{ project: alpha, target: 'cli-x64-linux', outputs }]));
    const betaClaims = expectPlanned(planOutputClaims([{ project: beta, target: 'other-x64-linux', outputs }]));
    const trees = [stagedTree('alpha', alphaClaims), stagedTree('linux', betaClaims)];
    expect(planOverlays(trees, SOURCE_SHA, [alpha, beta], PACKAGES)).toEqual({
      ok: false,
      reason: `Output collision across collected trees in /collected/linux/manifest.json: ${LINUX_X64_BIN}`,
    });
  });

  it('does not collide macos arm64 current against a disjoint x64 current tree', () => {
    const project = cowshedShapedProject();
    project.targets = [...project.targets, 'cli-x64-macos'];
    project.targetOutputs?.set('cli-x64-macos', ['{projectRoot}/dist/bin/darwin-x64']);
    const macosClaims = expectPlanned(
      planOutputClaims([
        {
          project,
          target: 'cli-arm64-macos',
          outputs: [{ declaredOutput: '{projectRoot}/dist/bin/darwin-arm64', files: [fileFact(DARWIN_ARM64_BIN)] }],
        },
        {
          project,
          target: 'napi-arm64-macos',
          outputs: [{ declaredOutput: '{projectRoot}/dist/native/darwin-arm64', files: [fileFact(DARWIN_ARM64_NAPI)] }],
        },
      ]),
    );
    const x64Claims = expectPlanned(
      planOutputClaims([
        {
          project,
          target: 'cli-x64-macos',
          outputs: [{ declaredOutput: '{projectRoot}/dist/bin/darwin-x64', files: [fileFact(DARWIN_X64_BIN)] }],
        },
      ]),
    );

    const trees = [stagedTree('macos-arm64', macosClaims), stagedTree('x64', x64Claims)];
    expect(planOverlays(trees, SOURCE_SHA, [project], PACKAGES)).toEqual({
      ok: true,
      value: [
        { source: `/collected/macos-arm64/workspace/${DARWIN_ARM64_BIN}`, destination: DARWIN_ARM64_BIN, mode: 0o644 },
        {
          source: `/collected/macos-arm64/workspace/${DARWIN_ARM64_NAPI}`,
          destination: DARWIN_ARM64_NAPI,
          mode: 0o644,
        },
        { source: `/collected/x64/workspace/${DARWIN_X64_BIN}`, destination: DARWIN_X64_BIN, mode: 0o644 },
      ],
    });
  });

  it('refuses a collected target that declares no outputs', () => {
    expect(planOutputClaims([{ project: cowshedShapedProject(), target: 'build', outputs: [] }])).toEqual({
      ok: false,
      reason: 'Nx target cowshed:build declares no outputs.',
    });
  });

  it('refuses a declared output that matched no file', () => {
    const outputs = [{ declaredOutput: '{projectRoot}/dist/ts', files: [] }];
    expect(planOutputClaims([{ project: cowshedShapedProject(), target: 'tsc-js', outputs }])).toEqual({
      ok: false,
      reason: 'Declared output {projectRoot}/dist/ts for cowshed:tsc-js is missing.',
    });
  });

  it('refuses an output file that is not a regular file', () => {
    const outputs = [{ declaredOutput: '{projectRoot}/dist/ts', files: [fileFact(TS_ENTRY, 'symlink')] }];
    expect(planOutputClaims([{ project: cowshedShapedProject(), target: 'tsc-js', outputs }])).toEqual({
      ok: false,
      reason: `Output file must be a regular file: ${TS_ENTRY}`,
    });
  });

  it('reaches a dependency output through its aggregate without a second claim', () => {
    const project = cowshedShapedProject();
    expect(
      planOutputClaims([
        {
          project,
          target: 'tsc-js',
          outputs: [{ declaredOutput: '{projectRoot}/dist/ts', files: [fileFact(TS_ENTRY)] }],
        },
        { project, target: 'build', outputs: [{ declaredOutput: '{projectRoot}/dist', files: [fileFact(TS_ENTRY)] }] },
      ]),
    ).toEqual({ ok: true, value: [TS_CLAIM] });
  });

  it('refuses a tree staged for another source SHA', () => {
    const otherSha = 'f'.repeat(40);
    const tree: CollectedTree = {
      ...stagedTree('build', [TS_CLAIM]),
      manifest: { sourceSha: otherSha, packageVersions: { cowshed: '1.0.0' } },
    };
    expect(planOverlays([tree], SOURCE_SHA, [cowshedShapedProject()], PACKAGES)).toEqual({
      ok: false,
      reason: `Source SHA mismatch in /collected/build/manifest.json: expected ${SOURCE_SHA}, received ${otherSha}.`,
    });
  });

  it('refuses a tree whose package versions disagree with the workspace', () => {
    const tree: CollectedTree = {
      ...stagedTree('build', [TS_CLAIM]),
      manifest: { sourceSha: SOURCE_SHA, packageVersions: { cowshed: '0.9.0' } },
    };
    expect(planOverlays([tree], SOURCE_SHA, [cowshedShapedProject()], PACKAGES)).toEqual({
      ok: false,
      reason: 'Package version mismatch in /collected/build/manifest.json for cowshed: expected 1.0.0, received 0.9.0.',
    });
  });

  it('refuses a staged output file that is missing', () => {
    const cause = new Error('ENOENT: no such file');
    const tree = observing(stagedTree('build', [TS_CLAIM]), { kind: 'missing', cause });
    expect(planOverlays([tree], SOURCE_SHA, [cowshedShapedProject()], PACKAGES)).toEqual({
      ok: false,
      reason: `Staged output file is missing: ${TS_ENTRY}: ENOENT: no such file`,
      cause,
    });
  });

  it('refuses a staged output that is not a regular file', () => {
    const tree = observing(stagedTree('build', [TS_CLAIM]), { kind: 'not-regular' });
    expect(planOverlays([tree], SOURCE_SHA, [cowshedShapedProject()], PACKAGES)).toEqual({
      ok: false,
      reason: `Staged output must be a regular file: ${TS_ENTRY}`,
    });
  });

  it('refuses a staged output whose size differs from the manifest', () => {
    const tree = observing(stagedTree('build', [TS_CLAIM]), { kind: 'regular', size: 9, sha256: DIGEST });
    expect(planOverlays([tree], SOURCE_SHA, [cowshedShapedProject()], PACKAGES)).toEqual({
      ok: false,
      reason: `Size mismatch for staged output ${TS_ENTRY}: expected 4, received 9.`,
    });
  });

  it('refuses a staged output whose bytes differ from the manifest', () => {
    const tree = observing(stagedTree('build', [TS_CLAIM]), { kind: 'regular', size: 4, sha256: OTHER_DIGEST });
    expect(planOverlays([tree], SOURCE_SHA, [cowshedShapedProject()], PACKAGES)).toEqual({
      ok: false,
      reason: `SHA-256 mismatch for staged output ${TS_ENTRY}: expected ${DIGEST}, received ${OTHER_DIGEST}.`,
    });
  });

  it('accepts an empty tree whose workspace was never transferred', () => {
    const cause = new Error('ENOENT');
    const tree: CollectedTree = { ...stagedTree('build', []), workspace: { kind: 'absent', cause } };
    expect(planOverlays([tree], SOURCE_SHA, [cowshedShapedProject()], PACKAGES)).toEqual({ ok: true, value: [] });
  });

  it('refuses a non-empty tree whose workspace is missing', () => {
    const cause = new Error('ENOENT');
    const tree: CollectedTree = { ...stagedTree('build', [TS_CLAIM]), workspace: { kind: 'absent', cause } };
    expect(planOverlays([tree], SOURCE_SHA, [cowshedShapedProject()], PACKAGES)).toEqual({
      ok: false,
      reason: 'Collected output workspace is missing: /collected/build/workspace: ENOENT',
      cause,
    });
  });

  it('refuses a staged workspace that is not a real directory', () => {
    const tree: CollectedTree = { ...stagedTree('build', [TS_CLAIM]), workspace: { kind: 'not-directory' } };
    expect(planOverlays([tree], SOURCE_SHA, [cowshedShapedProject()], PACKAGES)).toEqual({
      ok: false,
      reason: 'Collected output workspace must be a real directory: /collected/build/workspace',
    });
  });

  it('refuses an undeclared file in a staged workspace', () => {
    const extra = 'packages/cowshed/dist/ts/extra.js';
    const tree: CollectedTree = {
      ...stagedTree('build', [TS_CLAIM]),
      workspace: { kind: 'directory', regularFiles: [TS_ENTRY, extra] },
    };
    expect(planOverlays([tree], SOURCE_SHA, [cowshedShapedProject()], PACKAGES)).toEqual({
      ok: false,
      reason: `Undeclared staged output file in /collected/build/manifest.json: ${extra}`,
    });
  });

  it('refuses to collect a declared output directory that holds a symbolic link', async () => {
    const temp = await mkdtemp(join(tmpdir(), 'containment-probe-symlink-'));
    const root = join(temp, 'repo');
    try {
      await mkdir(join(root, 'packages/cowshed/dist/ts'), { recursive: true });
      await symlink('cli.js', join(root, 'packages/cowshed/dist/ts/cli.js'));
      const runs = [{ target: 'tsc-js', projects: [cowshedShapedProject()] }];
      await expect(collectNxOutputs(root, join(temp, 'release-build-outputs'), runs, SOURCE_SHA)).rejects.toThrow(
        'Collected outputs must not contain symbolic links',
      );
    } finally {
      await rm(temp, { recursive: true, force: true });
    }
  });

  it('judges an applied file by the same rules as a staged one', () => {
    const applied: OutputStage = { stage: 'applied', manifestPath: '/collected/build/manifest.json' };
    const expected = { size: 4, sha256: DIGEST };
    const cause = new Error('ENOENT');
    expect(judgeOutputFile(applied, TS_ENTRY, expected, { kind: 'missing', cause })).toEqual({
      reason: `Applied output file is missing: ${TS_ENTRY}: ENOENT`,
      cause,
    });
    expect(judgeOutputFile(applied, TS_ENTRY, expected, { kind: 'not-regular' })).toEqual({
      reason: `Applied output must be a regular file: ${TS_ENTRY}`,
    });
    expect(judgeOutputFile(applied, TS_ENTRY, expected, { kind: 'regular', size: 9, sha256: DIGEST })).toEqual({
      reason: `Size mismatch for applied output ${TS_ENTRY}: expected 4, received 9.`,
    });
    expect(judgeOutputFile(applied, TS_ENTRY, expected, { kind: 'regular', size: 4, sha256: OTHER_DIGEST })).toEqual({
      reason: `SHA-256 mismatch for applied output ${TS_ENTRY} from /collected/build/manifest.json: expected ${DIGEST}, received ${OTHER_DIGEST}.`,
    });
    expect(judgeOutputFile(applied, TS_ENTRY, expected, { kind: 'regular', size: 4, sha256: DIGEST })).toBeUndefined();
  });
});

/**
 * Plant one file inside every output the given runs declare, under `root`.
 *
 * The probe needs the REAL inferred graph but must not need the real build's
 * artifacts: `dist/bin/darwin-arm64` only exists on a macOS host, so collecting
 * from the repo made this a macOS-only test that failed on Linux with
 * "Declared output {projectRoot}/dist/bin/darwin-arm64 for cowshed:cli-arm64-macos
 * is missing." Planting the declared outputs keeps the graph real, the files
 * deterministic, and the assertion identical on every runner.
 */
async function plantDeclaredOutputs(
  root: string,
  runs: readonly { target: string; projects: readonly ProjectTargets[] }[],
): Promise<string[]> {
  const planted: string[] = [];
  for (const run of runs) {
    for (const project of run.projects) {
      // An output resolves against the project's root, so a project that
      // reports none has no place to put one.
      if (project.root === undefined) continue;
      for (const output of project.targetOutputs?.get(run.target) ?? []) {
        // The collector's own resolution, so every placeholder a declaration
        // may use (`{workspaceRoot}` included) lands where it will look.
        const directory = join(root, resolveDeclaredOutput(output, project));
        await mkdir(directory, { recursive: true });
        const file = join(directory, run.target);
        // 0o755 because `applyCollectedOutputs` must carry the executable bit
        // of a platform binary through the collect tree.
        await writeFile(file, `${run.target} artifact`, { mode: 0o755 });
        planted.push(file);
      }
    }
  }
  return planted;
}

describe('publish collect containment probe against the resolved graph', () => {
  it('collects cowshed tsc-js and macos platform trees without overlap', async () => {
    const projects = await readProjectTargets(join(import.meta.dir, '../../../..'));
    const cowshed = projects.find((project) => project.project === 'cowshed');
    // The inferred aggregate declares an explicit empty output list, so a
    // cached replay of `build` can never restore bytes its children own.
    expect(cowshed?.targetOutputs?.get('build')).toEqual([]);
    expect(cowshed?.targetOutputs?.get('tsc-js')).toEqual(['{projectRoot}/dist/ts']);

    const buildRuns = expandNxTargetDependencyRuns(
      expandNxTargetRuns(projects, { targets: 'build', projects: 'cowshed' }).runs,
    );
    const macosRuns = expandNxTargetDependencyRuns(
      expandNxTargetRuns(projects, { targets: '*-macos', projects: 'cowshed' }).runs,
    );
    expect(buildRuns.some((run) => run.target === 'build')).toBe(false);
    expect(buildRuns.some((run) => run.target === 'tsc-js' && run.projects[0]?.project === 'cowshed')).toBe(true);
    // Every expansion member must be a macOS platform target; the exact set
    // follows the package's declared triples, so listing them here would break
    // on the next triple added rather than on a containment regression.
    expect(macosRuns.length).toBeGreaterThan(0);
    for (const run of macosRuns) {
      expect(run.target.endsWith('-macos')).toBe(true);
    }
    expect(macosRuns.map((run) => run.target)).toContain('cli-arm64-macos');

    const temp = await mkdtemp(join(tmpdir(), 'containment-real-graph-'));
    const graphRoot = join(temp, 'repo');
    const buildArtifact = join(temp, 'rb');
    const macosArtifact = join(temp, 'mp');
    const applyRoot = join(temp, 'apply');
    try {
      const plantedBuild = await plantDeclaredOutputs(graphRoot, buildRuns);
      const plantedMacos = await plantDeclaredOutputs(graphRoot, macosRuns);
      expect(plantedBuild.length).toBeGreaterThan(0);
      expect(plantedMacos.length).toBeGreaterThan(0);

      const buildManifest = await collectNxOutputs(graphRoot, buildArtifact, buildRuns, SOURCE_SHA);
      const macosManifest = await collectNxOutputs(graphRoot, macosArtifact, macosRuns, SOURCE_SHA);

      // The aggregate itself declares no outputs, so nothing may be attributed
      // to it, and the platform families must stay out of the build tree.
      expect(buildManifest.files.some((file) => file.project === 'cowshed' && file.target === 'build')).toBe(false);
      expect(
        buildManifest.files.some((file) => file.path.includes('/dist/bin/') || file.path.includes('/dist/native/')),
      ).toBe(false);
      expect(
        buildManifest.files.some((file) => file.target === 'tsc-js' && file.path === 'packages/cowshed/dist/ts/tsc-js'),
      ).toBe(true);

      expect(macosManifest.files.map((file) => file.path).sort()).toEqual(
        plantedMacos.map((file) => relative(graphRoot, file)).sort(),
      );
      const buildPaths = new Set(buildManifest.files.map((file) => file.path));
      for (const file of macosManifest.files) {
        expect(buildPaths.has(file.path)).toBe(false);
      }

      await mkdir(applyRoot, { recursive: true });
      await applyCollectedOutputs(applyRoot, [buildArtifact, macosArtifact], SOURCE_SHA, projects);
      expect((await stat(join(applyRoot, 'packages/cowshed/dist/bin/darwin-arm64/cli-arm64-macos'))).mode & 0o777).toBe(
        0o755,
      );
    } finally {
      await rm(temp, { recursive: true, force: true });
    }
  });
});
