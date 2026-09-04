/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are asserted literally. */
import { describe, expect, it } from 'bun:test';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LINUX_PLATFORM_TARGET_GLOBS, PLATFORM_TARGET_GLOBS } from '@smoothbricks/nx-plugin/workspace-config-policy';
import fc from 'fast-check';
import { type NxProjects, targetNamesFromProjects } from '../nx/index.js';
import {
  DEVENV_MODULE_IMPORT,
  deployTargetInfoFromProjects,
  extractInlineLocalBlocksForTest,
  getManagedContentForTest,
  hasExactTargetForTest,
  INLINE_LOCAL_BEGIN,
  INLINE_LOCAL_END,
  LOCAL_SECTION_MARKER,
  type ManagedFileContext,
  macosPlatformArchitecturesForTest,
  managedFileTargetsForTest,
  platformTargetGlobsForTest,
  publishWorkflowManagedFileForTest,
  reinsertInlineLocalBlocksForTest,
  splitLocalSectionForTest,
  validateDevenvModuleImport,
} from './managed-files.js';

const MANAGED = '# managed content\npath merge=driver\n';

const REPO_ROOT = join(import.meta.dir, '..', '..', '..', '..');
const ARCHITECTURE_SCOPED_PREFIX = '${{ runner.os }}-${{ runner.arch }}-';
const NODE_MODULES_CACHE_KEY = "${{ hashFiles('bun.lock', 'package.json', 'packages/*/package.json') }}";
const CACHE_ACTIONS = [
  { name: 'cache-nix-devenv', osKeyLines: 6 },
  { name: 'cache-node-modules', osKeyLines: 2 },
  { name: 'cache-ttsc-plugins', osKeyLines: 2 },
  { name: 'cache-nx', osKeyLines: 2 },
] as const;

describe('managed-file local sections', () => {
  it('content without a marker is entirely managed', () => {
    const { managed, localTail } = splitLocalSectionForTest(MANAGED);
    expect(managed).toBe(MANAGED);
    expect(localTail).toBe('');
  });

  it('everything from the marker onward is the repo-owned tail', () => {
    const tail = `${LOCAL_SECTION_MARKER}\ncustom/*.jsonl merge=custom-log\n`;
    const { managed, localTail } = splitLocalSectionForTest(`${MANAGED}\n${tail}`);
    expect(managed).toBe(`${MANAGED}\n`);
    expect(localTail).toBe(tail);
  });

  it('a tail directly after the managed content tolerates the separating newline', () => {
    // The compare rule accepts `managed === content + '\n'` when a tail exists,
    // so update → check round-trips as unchanged.
    const written = `${MANAGED}\n${LOCAL_SECTION_MARKER}\nextra\n`;
    const { managed, localTail } = splitLocalSectionForTest(written);
    expect(managed).toBe(`${MANAGED}\n`);
    expect(localTail.startsWith(LOCAL_SECTION_MARKER)).toBe(true);
  });
});

describe('managed-file inline local blocks', () => {
  it('content with no inline markers extracts as fully managed, no blocks', () => {
    const { withoutInline, blocks } = extractInlineLocalBlocksForTest(MANAGED);
    expect(withoutInline).toBe(MANAGED);
    expect(blocks).toEqual([]);
  });

  it('a wrapped block is extracted and removed, anchored on the preceding line', () => {
    const current = ['a:', '  - one', '  - two', INLINE_LOCAL_BEGIN, '  - repo-owned', INLINE_LOCAL_END, 'b:'].join(
      '\n',
    );
    const { withoutInline, blocks } = extractInlineLocalBlocksForTest(current);
    expect(withoutInline).toBe(['a:', '  - one', '  - two', 'b:'].join('\n'));
    expect(blocks).toEqual([{ anchor: '  - two', lines: '  - repo-owned' }]);
  });

  it('multiple blocks each anchor to their own preceding line', () => {
    const current = [
      'x',
      INLINE_LOCAL_BEGIN,
      'first',
      INLINE_LOCAL_END,
      'y',
      INLINE_LOCAL_BEGIN,
      'second',
      INLINE_LOCAL_END,
      'z',
    ].join('\n');
    const { withoutInline, blocks } = extractInlineLocalBlocksForTest(current);
    expect(withoutInline).toBe(['x', 'y', 'z'].join('\n'));
    expect(blocks).toEqual([
      { anchor: 'x', lines: 'first' },
      { anchor: 'y', lines: 'second' },
    ]);
  });

  it('a begin marker with no preceding line throws rather than silently dropping content', () => {
    const current = [INLINE_LOCAL_BEGIN, 'orphan', INLINE_LOCAL_END].join('\n');
    expect(() => extractInlineLocalBlocksForTest(current)).toThrow(/no preceding anchor/);
  });

  it('an unterminated begin marker throws rather than silently dropping content', () => {
    const current = ['a', INLINE_LOCAL_BEGIN, 'unterminated'].join('\n');
    expect(() => extractInlineLocalBlocksForTest(current)).toThrow(/no matching/);
  });

  it('reinserting into fresh content with no blocks is a no-op', () => {
    expect(reinsertInlineLocalBlocksForTest(MANAGED, [])).toBe(MANAGED);
  });

  it('reinserting splices the block back after its anchor in freshly rendered content', () => {
    const fresh = ['a:', '  - one', '  - two', 'b:'].join('\n');
    const result = reinsertInlineLocalBlocksForTest(fresh, [{ anchor: '  - two', lines: '  - repo-owned' }]);
    expect(result).toBe(
      ['a:', '  - one', '  - two', INLINE_LOCAL_BEGIN, '  - repo-owned', INLINE_LOCAL_END, 'b:'].join('\n'),
    );
  });

  it('preserves marker indentation when refreshing nested configuration', () => {
    const markerIndent = '      ';
    const current = [
      'patterns:',
      "      - '*.html'",
      `${markerIndent}${INLINE_LOCAL_BEGIN}`,
      "      - '!**/templates/*.html'",
      `${markerIndent}${INLINE_LOCAL_END}`,
    ].join('\n');
    const { withoutInline, blocks } = extractInlineLocalBlocksForTest(current);
    expect(reinsertInlineLocalBlocksForTest(withoutInline, blocks)).toBe(current);
  });

  it("reinserting refuses when the anchor no longer appears — never silently drops the repo's customization", () => {
    const fresh = ['a:', '  - one', 'b:'].join('\n'); // '  - two' is gone
    expect(() => reinsertInlineLocalBlocksForTest(fresh, [{ anchor: '  - two', lines: '  - repo-owned' }])).toThrow(
      /matches no line/,
    );
  });

  it('reinserting refuses an ambiguous multiply occurring anchor', () => {
    const fresh = ['anchor', 'middle', 'anchor'].join('\n');
    expect(() => reinsertInlineLocalBlocksForTest(fresh, [{ anchor: 'anchor', lines: 'repo-owned' }])).toThrow(
      /matches 2 lines/,
    );
  });

  it('property: extract then reinsert round-trips to the original for unique, single-occurrence anchors', () => {
    // Lines that can serve as anchors: non-empty, not a marker, and each drawn
    // from a small alphabet so uniqueness is checkable — the round-trip only
    // holds when an anchor line occurs exactly once in the managed section
    // because ambiguous anchors are rejected rather than selecting one.
    const linePool = fc.constantFrom('alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta');
    fc.assert(
      fc.property(
        fc.uniqueArray(linePool, { minLength: 1, maxLength: 6 }),
        fc.array(fc.string(), { maxLength: 3 }),
        (anchors, blockLinesFlat) => {
          const blockLines = blockLinesFlat.length > 0 ? [blockLinesFlat.join('|') || 'x'] : ['x'];
          const fresh = anchors.join('\n');
          const withBlocks = anchors
            .map((anchor) => [anchor, INLINE_LOCAL_BEGIN, ...blockLines, INLINE_LOCAL_END].join('\n'))
            .join('\n');
          const { withoutInline, blocks } = extractInlineLocalBlocksForTest(withBlocks);
          expect(withoutInline).toBe(fresh);
          const reinserted = reinsertInlineLocalBlocksForTest(fresh, blocks);
          expect(reinserted).toBe(withBlocks);
        },
      ),
    );
  });
});

describe('managed publish platform discovery', () => {
  it('returns canonical target families from resolved target names without leaking project names', () => {
    const discovered = platformTargetGlobsForTest(['build', 'bundle-linux', 'package-macos', 'simulator-ios', 'test']);

    expect(discovered).toEqual([...PLATFORM_TARGET_GLOBS]);
    expect(discovered).not.toContain('native-app');
  });

  it('selects only supplemental Linux when resolved metadata has no Apple targets', () => {
    expect(platformTargetGlobsForTest(['build', 'bundle-linux', 'test'])).toEqual([...LINUX_PLATFORM_TARGET_GLOBS]);
  });

  it('returns no platform families for ordinary Nx targets', () => {
    expect(platformTargetGlobsForTest(['build', 'lint', 'test', 'typecheck'])).toEqual([]);
  });

  it('derives macOS matrix architectures from the target names that exist', () => {
    expect(
      macosPlatformArchitecturesForTest([
        'build',
        'cli-arm64-macos',
        'cli-x64-macos',
        'napi-arm64-macos',
        'cli-x64-linux',
        'test',
      ]),
    ).toEqual(['arm64', 'x64']);
    expect(macosPlatformArchitecturesForTest(['simulator-arm64-ios', 'cli-arm64-macos'])).toEqual(['arm64']);
    expect(macosPlatformArchitecturesForTest(['build', 'cli-x64-linux', 'test'])).toEqual([]);
  });
});

describe('managed devenv module import', () => {
  it('accepts a devenv.nix that imports the module and reports one that does not', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-devenv-import-'));
    try {
      // No devenv.nix at all: nothing to enforce (a repo may not use devenv).
      expect(validateDevenvModuleImport(root)).toBe(0);

      await mkdir(join(root, 'tooling/direnv'), { recursive: true });
      const target = join(root, 'tooling/direnv/devenv.nix');
      await writeFile(target, '{...}: {\n  packages = [];\n}\n');
      expect(validateDevenvModuleImport(root)).toBe(1);

      await writeFile(target, `{...}: {\n  imports = [${DEVENV_MODULE_IMPORT}];\n}\n`);
      expect(validateDevenvModuleImport(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('nx graph project helpers', () => {
  const sampleProjects: NxProjects = {
    lib: {
      targets: {
        build: {},
        'bundle-linux': {},
        lint: {},
      },
    },
    app: {
      targets: {
        deploy: {
          options: { command: 'echo no' },
          configurations: {
            staging: { command: 'wrangler deploy --env staging' },
            production: { options: { command: 'wrangler deploy --env production' } },
          },
        },
        'package-macos': {},
      },
    },
  };

  it('collects target names from graph nodes without project names', () => {
    expect(targetNamesFromProjects(sampleProjects).sort()).toEqual(
      ['build', 'bundle-linux', 'deploy', 'lint', 'package-macos'].sort(),
    );
  });

  it('detects browser and deployment E2E targets by exact name only', () => {
    const exact = targetNamesFromProjects({
      app: { targets: { 'test-browser': {}, 'e2e-deployment': {} } },
    });
    const nearMisses = targetNamesFromProjects({
      app: { targets: { 'test-browser-extra': {}, 'e2e-deployments': {}, 'pre-e2e-deployment': {} } },
    });

    expect(hasExactTargetForTest(exact, 'test-browser')).toBe(true);
    expect(hasExactTargetForTest(exact, 'e2e-deployment')).toBe(true);
    expect(hasExactTargetForTest(nearMisses, 'test-browser')).toBe(false);
    expect(hasExactTargetForTest(nearMisses, 'e2e-deployment')).toBe(false);
  });

  it('detects deploy configurations and cloudflare provider from graph nodes', () => {
    expect(deployTargetInfoFromProjects(sampleProjects, 'staging')).toEqual({
      exists: true,
      provider: 'cloudflare',
    });
    expect(deployTargetInfoFromProjects(sampleProjects, 'production')).toEqual({
      exists: true,
      provider: 'cloudflare',
    });
    expect(deployTargetInfoFromProjects(sampleProjects, 'preview')).toEqual({ exists: false });
  });

  it('recognizes convention-driven deploy-stage targets without per-stage configurations', () => {
    const projects: NxProjects = {
      app: {
        targets: {
          deploy: {
            options: { command: 'smoo wrangler deploy-stage --stage {args.stage}' },
          },
        },
      },
    };

    expect(deployTargetInfoFromProjects(projects, 'staging')).toEqual({ exists: true, provider: 'cloudflare' });
    expect(deployTargetInfoFromProjects(projects, 'production')).toEqual({ exists: true, provider: 'cloudflare' });
  });
});

describe('managed raw files', () => {
  it('manages the devenv wrapper as an executable byte-exact copy', async () => {
    expect(managedFileTargetsForTest).toContainEqual({ target: 'tooling/devenv', executable: true });

    const [source, generated] = await Promise.all([
      readFile(join(REPO_ROOT, 'packages', 'cli', 'managed', 'raw', 'tooling', 'devenv'), 'utf8'),
      readFile(join(REPO_ROOT, 'tooling', 'devenv'), 'utf8'),
    ]);

    expect(generated).toBe(source);
  });

  it('manages the macOS pre-push linux compile gate as an executable copy', async () => {
    expect(managedFileTargetsForTest).toContainEqual({
      target: 'tooling/git-hooks/pre-push.sh',
      executable: true,
    });

    const [source, generated] = await Promise.all([
      readFile(join(REPO_ROOT, 'packages', 'cli', 'managed', 'raw', 'tooling', 'git-hooks', 'pre-push.sh'), 'utf8'),
      readFile(join(REPO_ROOT, 'tooling', 'git-hooks', 'pre-push.sh'), 'utf8'),
    ]);

    expect(generated).toBe(source);
    expect(generated).toContain('uname -s');
    expect(generated).toContain('cargo-lint-cross');
    expect(generated).toContain('bun run check:linux');
  });

  it('persists the ttsc cache path the shell computes, leaving host overrides untouched', async () => {
    const temp = await mkdtemp(join(tmpdir(), 'smoo-github-bootstrap-'));
    const bin = join(temp, 'bin');
    await mkdir(bin);
    const devenv = join(bin, 'devenv');
    // Emulates `devenv shell [flags] -- cmd...` the way devenv.smoo.nix's
    // enterShell behaves: cd to the workspace root, compute TTSC_CACHE_DIR
    // honoring a host-provided value, then run the command — so build-shell's
    // wholesale environment capture sees real shell exports instead of a
    // hand-copied list.
    await writeFile(
      devenv,
      [
        '#!/usr/bin/env bash',
        'set -euo pipefail',
        'while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do shift; done',
        'shift',
        'cd ../..',
        'export TTSC_CACHE_DIR="${TTSC_CACHE_DIR:-$PWD/.cache/ttsc}"',
        'exec "$@"',
        '',
      ].join('\n'),
    );
    await chmod(devenv, 0o755);

    try {
      const cases = [
        // No host value: the shell computes the repo-local default and the
        // capture persists it for the workflow steps that follow.
        { input: '', expected: `TTSC_CACHE_DIR=${join(REPO_ROOT, '.cache', 'ttsc')}\n` },
        // Host-provided value: the shell keeps it, so nothing changed and
        // nothing is rewritten — the override stays live in the step env.
        { input: join(temp, 'host-ttsc'), expected: undefined },
      ];
      for (const [index, cache] of cases.entries()) {
        const githubEnv = join(temp, `github-env-${index}`);
        const githubPath = join(temp, `github-path-${index}`);
        const process = Bun.spawn(
          [join(REPO_ROOT, 'tooling', 'direnv', 'github-actions-bootstrap.sh'), 'build-shell'],
          {
            cwd: join(REPO_ROOT, 'tooling', 'direnv'),
            env: {
              ...Bun.env,
              GITHUB_ENV: githubEnv,
              GITHUB_PATH: githubPath,
              PATH: `${bin}:${Bun.env.PATH ?? ''}`,
              TTSC_CACHE_DIR: cache.input,
            },
            stderr: 'inherit',
            stdout: 'inherit',
          },
        );
        expect(await process.exited).toBe(0);
        // Nothing to persist leaves GITHUB_ENV untouched — possibly never created.
        const persisted = await readFile(githubEnv, 'utf8').catch(() => '');
        if (cache.expected === undefined) {
          expect(persisted).not.toContain('TTSC_CACHE_DIR=');
        } else {
          expect(persisted).toContain(cache.expected);
        }
      }
    } finally {
      await rm(temp, { recursive: true, force: true });
    }
  });
});

describe('managed cache actions', () => {
  it('renders the checked-in action copies from their managed templates', async () => {
    for (const action of CACHE_ACTIONS) {
      const [template, generated] = await Promise.all([
        readFile(
          join(REPO_ROOT, 'packages', 'cli', 'managed', 'templates', 'github', 'actions', action.name, 'action.yml'),
          'utf8',
        ),
        readFile(join(REPO_ROOT, '.github', 'actions', action.name, 'action.yml'), 'utf8'),
      ]);

      expect(generated).toBe(template.replace('{{NODE_MODULES_CACHE_KEY}}', NODE_MODULES_CACHE_KEY));
    }
  });

  it('scopes every primary, restore, and save key to the runner OS and architecture', async () => {
    for (const action of CACHE_ACTIONS) {
      for (const actionRoot of [
        join(REPO_ROOT, 'packages', 'cli', 'managed', 'templates', 'github', 'actions'),
        join(REPO_ROOT, '.github', 'actions'),
      ]) {
        const content = await readFile(join(actionRoot, action.name, 'action.yml'), 'utf8');
        const osKeyLines = content.split('\n').filter((line) => line.includes('${{ runner.os }}'));

        expect(osKeyLines).toHaveLength(action.osKeyLines);
        expect(osKeyLines.every((line) => line.includes(ARCHITECTURE_SCOPED_PREFIX))).toBe(true);
      }
    }
  });
});

describe('publish workflow rendering by repo shape', () => {
  const context = (overrides: Partial<ManagedFileContext>): ManagedFileContext => ({
    hasReleasePackages: true,
    hasStagingDeployTargets: false,
    hasProductionDeployTargets: false,
    hasBrowserTestTargets: false,
    hasE2eDeploymentTargets: false,
    ciPushBranches: ['main'],
    ciRunsOn: 'ubuntu-latest',
    nodeModulesCacheKey: 'key',
    repoName: '@scope/repo',
    platformTargetGlobs: [],
    macosPlatformArchitectures: [],
    ...overrides,
  });

  it('drops the release half for a repo that deploys production but owns no packages', () => {
    const rendered = getManagedContentForTest(
      publishWorkflowManagedFileForTest,
      context({ hasReleasePackages: false, hasProductionDeployTargets: true }),
    );

    expect(rendered).toContain('- name: 🚀 Deploy production');
    expect(rendered).not.toContain('smoo release');
    expect(rendered).not.toContain('steps.version.outputs.mode');
  });

  it('keeps the release pipeline for a repo that owns packages', () => {
    const rendered = getManagedContentForTest(
      publishWorkflowManagedFileForTest,
      context({ hasReleasePackages: true, hasProductionDeployTargets: true }),
    );

    expect(rendered).toContain('smoo release publish');
    expect(rendered).toContain("steps.version.outputs.mode != 'none'");
  });
});
