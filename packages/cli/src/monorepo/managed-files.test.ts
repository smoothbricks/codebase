/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are asserted literally. */
import { describe, expect, it } from 'bun:test';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LINUX_PLATFORM_TARGET_GLOBS, PLATFORM_TARGET_GLOBS } from '@smoothbricks/nx-plugin/workspace-config-policy';
import { type NxProjects, targetNamesFromProjects } from '../nx/index.js';
import {
  anyProjectHasTagForTest,
  crossTestArchivesForTest,
  DEVENV_MODULE_IMPORT,
  deployTargetInfoFromProjects,
  hasExactTargetForTest,
  type ManagedFileContext,
  macosPlatformArchitecturesForTest,
  managedFileTargetsForContext,
  managedFileTargetsForTest,
  platformTargetGlobsForTest,
  releasePlatformTargetGlobsFor,
  renderManagedWorkflowForTest,
  validateDevenvModuleImport,
} from './managed-files.js';
import { renderPublishWorkflowYaml } from './publish-workflow.js';

const REPO_ROOT = join(import.meta.dir, '..', '..', '..', '..');
const ARCHITECTURE_SCOPED_PREFIX = '${{ runner.os }}-${{ runner.arch }}-';
const NODE_MODULES_CACHE_KEY = "${{ hashFiles('.npmrc', 'bun.lock', 'package.json', 'packages/*/package.json') }}";
const CACHE_ACTIONS = [
  { name: 'setup-devenv', osKeyLines: 2 },
  { name: 'cache-nix-devenv', osKeyLines: 3 },
  { name: 'cache-node-modules', osKeyLines: 2 },
  { name: 'cache-ttsc-plugins', osKeyLines: 2 },
  { name: 'cache-nx', osKeyLines: 2 },
] as const;

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

  it('reads cross-built archives out of the graph, at each project root', () => {
    expect(
      crossTestArchivesForTest({
        '@acme/codebase': {
          root: '.',
          targets: { 'cargo-cross-test-archive-aarch64-apple-darwin': {}, 'cargo-test-archive': {}, test: {} },
        },
        'acme-embedded': {
          root: 'packages/embedded',
          targets: { 'cargo-cross-test-archive-thumbv7em-none-eabihf': {} },
        },
        'acme-web': { root: 'packages/web', targets: { build: {}, 'cargo-cross-test-aarch64-apple-darwin': {} } },
      }),
    ).toEqual([
      { triple: 'aarch64-apple-darwin', path: 'target/nextest/archive-aarch64-apple-darwin.tar.zst' },
      {
        triple: 'thumbv7em-none-eabihf',
        path: 'packages/embedded/target/nextest/archive-thumbv7em-none-eabihf.tar.zst',
      },
    ]);
    // A repository with no cross declaration has no such target, so the CI
    // renderer gets an empty list and renders the workflow it renders today.
    expect(crossTestArchivesForTest({ '@acme/codebase': { root: '.', targets: { test: {} } } })).toEqual([]);
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
      // What the plugin infers for a private wrangler project, and what a published library
      // holding a wrangler manifest used to get: a deploy target no tag enrols in CI.
      targets: {
        deploy: { options: { command: 'smoo wrangler deploy-stage --stage {args.stage}' } },
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

  it('deploys nothing for a wrangler deploy target no tag names', () => {
    expect(deployTargetInfoFromProjects(sampleProjects, 'staging')).toEqual({ exists: false });
    expect(deployTargetInfoFromProjects(sampleProjects, 'production')).toEqual({ exists: false });
  });

  it('reads the tags as precedence: permanent excludes, staging narrows, stage generalizes', () => {
    const deploy = { options: { command: 'smoo wrangler deploy-stage --stage {args.stage}' } };
    const stageFor = (tags: string[]) => ({
      staging: deployTargetInfoFromProjects({ site: { tags, targets: { deploy } } }, 'staging').exists,
      production: deployTargetInfoFromProjects({ site: { tags, targets: { deploy } } }, 'production').exists,
    });

    expect(stageFor(['stage-deploy-target'])).toEqual({ staging: true, production: true });
    expect(stageFor(['staging-deploy-target'])).toEqual({ staging: true, production: false });
    expect(stageFor(['permanent-deploy-target'])).toEqual({ staging: false, production: false });
    // Both spellings on one project is a contradiction; the exclusion wins, which is what
    // `nx show projects --exclude=tag:permanent-deploy-target` does to it in CI too.
    expect(stageFor(['permanent-deploy-target', 'stage-deploy-target'])).toEqual({
      staging: false,
      production: false,
    });
    expect(stageFor(['npm:private'])).toEqual({ staging: false, production: false });
  });

  it('reads the command for the provider only: which credentials the generated job needs', () => {
    const cloudflare: NxProjects = {
      site: { tags: ['stage-deploy-target'], targets: { deploy: { command: 'wrangler deploy --config dist/w.json' } } },
    };
    const elsewhere: NxProjects = {
      site: { tags: ['stage-deploy-target'], targets: { deploy: { command: 'bun tooling/deploy-site.ts' } } },
    };

    expect(deployTargetInfoFromProjects(cloudflare, 'staging')).toEqual({ exists: true, provider: 'cloudflare' });
    expect(deployTargetInfoFromProjects(elsewhere, 'staging')).toEqual({ exists: true, provider: undefined });
  });

  it('finds a tag carried by any project in the graph', () => {
    const tagged: NxProjects = { ...sampleProjects, site: { tags: ['production-push-deploy-target'] } };

    expect(anyProjectHasTagForTest(tagged, 'production-push-deploy-target')).toBe(true);
    expect(anyProjectHasTagForTest(sampleProjects, 'production-push-deploy-target')).toBe(false);
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

const context = (overrides: Partial<ManagedFileContext>): ManagedFileContext => ({
  hasReleasePackages: true,
  hasStagingDeployTargets: false,
  hasProductionDeployTargets: false,
  hasProductionPushDeployTargets: false,
  hasBrowserTestTargets: false,
  hasE2eDeploymentTargets: false,
  ciPushBranches: ['main'],
  ciRunsOn: 'ubuntu-latest',
  macosRunsOn: 'macos-latest',
  ciDeploySecrets: {},
  ciE2eSecrets: {},
  nodeModulesCacheKey: 'key',
  repoName: '@scope/repo',
  platformTargetGlobs: [],
  releasePlatformTargetGlobs: [],
  macosPlatformArchitectures: [],
  crossTestArchives: [],
  ...overrides,
});

describe('publish workflow rendering by repo shape', () => {
  it('drops the release half for a repo that deploys production but owns no packages', () => {
    const rendered = renderManagedWorkflowForTest(
      'publish-workflow',
      context({ hasReleasePackages: false, hasProductionDeployTargets: true }),
    );

    expect(rendered).toContain('- name: 🚀 Deploy production');
    expect(rendered).not.toContain('smoo release');
    expect(rendered).not.toContain('steps.version.outputs.mode');
  });

  it('keeps the release pipeline for a repo that owns packages', () => {
    const rendered = renderManagedWorkflowForTest(
      'publish-workflow',
      context({ hasReleasePackages: true, hasProductionDeployTargets: true }),
    );

    expect(rendered).toContain('smoo release publish');
    expect(rendered).toContain("steps.version.outputs.mode != 'none'");
  });

  it('threads declared privateNpm into the publish workflow as job-level read token', () => {
    const rendered = renderManagedWorkflowForTest(
      'publish-workflow',
      context({
        privateNpm: {
          scope: '@priv.test',
          readTokenEnv: 'PRIV_NPM_READ_TOKEN',
          publishTokenEnv: 'PRIV_NPM_PUBLISH_TOKEN',
        },
      }),
    );

    expect(rendered).toContain('PRIV_NPM_READ_TOKEN: ${{ secrets.PRIV_NPM_READ_TOKEN }}');
    expect(rendered).not.toContain('PRIV_NPM_REGISTRY');
    expect(rendered).not.toMatch(/^ {6}PRIV_NPM_PUBLISH_TOKEN:/m);
    expect(rendered).toContain('          PRIV_NPM_PUBLISH_TOKEN: ${{ secrets.PRIV_NPM_PUBLISH_TOKEN }}');
  });

  it('threads production deploy secrets into the publish deploy step', () => {
    const rendered = renderManagedWorkflowForTest(
      'publish-workflow',
      context({
        hasProductionDeployTargets: true,
        productionDeployProvider: 'cloudflare',
        hasReleasePackages: true,
        ciDeploySecrets: { BILLING_API_TOKEN: 'SMOO_BILLING_API_TOKEN' },
      }),
    );

    expect(rendered).toContain('          BILLING_API_TOKEN: ${{ secrets.SMOO_BILLING_API_TOKEN }}');
    expect(rendered).not.toMatch(/^ {6}BILLING_API_TOKEN:/m);
  });

  it('renders publish.yml byte-identical whether or not the graph declares cross test archives', () => {
    // A repo shape that HAS macOS publish legs, so the invariance is asserted
    // where a cross-test step could plausibly have been grafted on.
    const bare = context({
      platformTargetGlobs: [...PLATFORM_TARGET_GLOBS],
      releasePlatformTargetGlobs: [...PLATFORM_TARGET_GLOBS],
      macosPlatformArchitectures: ['arm64'],
    });
    const crossing: ManagedFileContext = {
      ...bare,
      crossTestArchives: [
        { triple: 'aarch64-apple-darwin', path: 'target/nextest/archive-aarch64-apple-darwin.tar.zst' },
      ],
    };

    expect(renderManagedWorkflowForTest('publish-workflow', crossing)).toBe(
      renderManagedWorkflowForTest('publish-workflow', bare),
    );
    expect(renderManagedWorkflowForTest('publish-workflow', crossing)).not.toContain('cross-test');
    // ...and the declaration is not inert: the same pair of contexts does change
    // ci.yml, which is the file that owns executing those binaries.
    expect(renderManagedWorkflowForTest('ci-workflow', crossing)).not.toBe(
      renderManagedWorkflowForTest('ci-workflow', bare),
    );
    expect(renderManagedWorkflowForTest('ci-workflow', crossing)).toContain('  macos-cross-tests:');
  });
});

describe('CI workflow rendering by repo shape', () => {
  const deploying = context({
    hasStagingDeployTargets: true,
    stagingDeployProvider: 'cloudflare',
    hasE2eDeploymentTargets: true,
  });

  it('adds the production-on-push job only when a project carries the tag', () => {
    expect(
      renderManagedWorkflowForTest('ci-workflow', { ...deploying, hasProductionPushDeployTargets: true }),
    ).toContain('  deploy-production:');
    expect(renderManagedWorkflowForTest('ci-workflow', deploying)).not.toContain('deploy-production');
  });

  it('renders the production job for a repo whose only stage project is tag-based', () => {
    const projects: NxProjects = {
      site: {
        tags: ['stage-deploy-target', 'production-push-deploy-target'],
        targets: { deploy: { command: 'wrangler deploy --config dist/wrangler.json' } },
      },
    };
    const stagingDeploy = deployTargetInfoFromProjects(projects, 'staging');
    const rendered = renderManagedWorkflowForTest(
      'ci-workflow',
      context({
        hasStagingDeployTargets: stagingDeploy.exists,
        stagingDeployProvider: stagingDeploy.provider,
        hasProductionPushDeployTargets: anyProjectHasTagForTest(projects, 'production-push-deploy-target'),
      }),
    );

    expect(rendered).toContain('- name: 🚀 Deploy Stage');
    expect(rendered).toContain('  deploy-production:');
  });

  it('renders no deploy job at all for a repo whose wrangler projects carry no deploy tag', () => {
    // The regression this exists for: published libraries ship a wrangler manifest to document a
    // Durable Object binding for their consumers, the plugin inferred a deploy target from each,
    // and a repository that deploys nothing from CI grew a Deploy Stage step holding Cloudflare
    // credentials, a deployments permission, and a preview-cleanup workflow.
    const deploy = { options: { command: 'smoo wrangler deploy-stage --stage {args.stage}' } };
    const projects: NxProjects = {
      'acme-cloudflare': { tags: ['npm:private'], targets: { deploy } },
      'acme-documents': { tags: ['npm:public'], targets: { deploy } },
      // A private worker, deployed by hand from a developer's shell and never by CI.
      'acme-service': { targets: { deploy } },
    };
    const staging = deployTargetInfoFromProjects(projects, 'staging');
    const production = deployTargetInfoFromProjects(projects, 'production');
    const shape = context({
      hasStagingDeployTargets: staging.exists,
      stagingDeployProvider: staging.provider,
      hasProductionDeployTargets: production.exists,
      productionDeployProvider: production.provider,
      hasE2eDeploymentTargets: true,
    });
    const ci = renderManagedWorkflowForTest('ci-workflow', shape);

    expect(ci).not.toContain('Deploy Stage');
    expect(ci).not.toContain('deployments: write');
    expect(ci).not.toContain('CLOUDFLARE_API_TOKEN');
    expect(ci).not.toContain('e2e-deployment');
    expect(renderManagedWorkflowForTest('publish-workflow', shape)).not.toContain('Deploy production');
    expect(managedFileTargetsForContext(shape)).not.toContain('.github/workflows/pr-preview-cleanup.yml');
  });

  it('adds the preview cleanup workflow only where a cloudflare stage deploy makes preview stages', () => {
    expect(managedFileTargetsForContext(deploying)).toContain('.github/workflows/pr-preview-cleanup.yml');
    expect(managedFileTargetsForContext({ ...deploying, stagingDeployProvider: undefined })).not.toContain(
      '.github/workflows/pr-preview-cleanup.yml',
    );
  });

  it('threads environments and secrets from the context into the workflow', () => {
    const rendered = renderManagedWorkflowForTest('ci-workflow', {
      ...deploying,
      ciEnvironments: { staging: 'staging' },
      ciDeploySecrets: { E2E_CONTROL_TOKEN: 'E2E_CONTROL_TOKEN' },
      ciE2eSecrets: { GIT_CRYPT_KEY_B64: 'GIT_CRYPT_KEY_B64' },
    });

    expect(rendered).toContain('    environment: staging\n');
    expect(rendered).toContain('E2E_CONTROL_TOKEN: ${{ secrets.E2E_CONTROL_TOKEN }}');
    expect(rendered).toContain('GIT_CRYPT_KEY_B64: ${{ secrets.GIT_CRYPT_KEY_B64 }}');
  });
});

describe('release platform families', () => {
  it('an excluded family is not produced by the release workflow, and the rest still are', () => {
    expect(releasePlatformTargetGlobsFor(['*-macos', '*-ios', '*-linux'], ['*-macos', '*-ios'])).toEqual(['*-linux']);
    expect(releasePlatformTargetGlobsFor(PLATFORM_TARGET_GLOBS, undefined)).toEqual([...PLATFORM_TARGET_GLOBS]);
  });

  it('excluding every Apple family renders the single-job Linux publish shape', () => {
    const globs = releasePlatformTargetGlobsFor(['*-macos', '*-linux'], ['*-macos']);
    const rendered = renderPublishWorkflowYaml({
      repoName: 'acme/app',
      platformTargetGlobs: globs,
      macosPlatformArchitectures: [],
      runsOn: ['nixos-latest-x64', 'self-hosted'],
      actionsProvider: 'forgejo',
    });
    expect(rendered).not.toContain('cross-platform:');
    expect(rendered).not.toContain('macos-platform:');
    expect(rendered).not.toContain('SDKROOT');
    expect(rendered).not.toContain('--targets "*-macos"');
    expect(rendered).toContain('  publish:');
  });
});
