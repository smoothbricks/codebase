/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are asserted literally. */
import { describe, expect, it, spyOn } from 'bun:test';
import { chmod, copyFile, mkdir, mkdtemp, readFile, readlink, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import { pathToFileURL } from 'node:url';
import { managedNxPatch } from '@smoothbricks/nx-plugin/managed-files/files';
import { renderPublishWorkflowYaml } from '@smoothbricks/nx-plugin/managed-files/publish-workflow';
import { LINUX_PLATFORM_TARGET_GLOBS, PLATFORM_TARGET_GLOBS } from '@smoothbricks/nx-plugin/workspace-config-policy';
import typia from 'typia';
import { mergeEnv } from '../lib/run.js';
import { type NxProjects, targetNamesFromProjects } from '../nx/index.js';
import {
  anyProjectHasTagForTest,
  applyManagedFilesForContext,
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
  validateDevenvReleaseLock,
} from './managed-files.js';

const REPO_ROOT = join(import.meta.dir, '..', '..', '..', '..');
const ARCHITECTURE_SCOPED_PREFIX = '${{ runner.os }}-${{ runner.arch }}-';
const NODE_MODULES_CACHE_KEY = "${{ hashFiles('.npmrc', 'bun.lock', 'package.json', 'packages/*/package.json') }}";
const CACHE_ACTIONS = [
  { name: 'setup-devenv', osKeyLines: 2 },
  { name: 'cache-nix-devenv', osKeyLines: 3 },
  { name: 'save-nix-devenv', osKeyLines: 0 },
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
          targets: { 'cargo-cross-test-archive-aarch64-apple-darwin': {}, 'cargo-test-build': {}, test: {} },
        },
        'acme-embedded': {
          root: 'packages/embedded',
          targets: { 'cargo-cross-test-archive-thumbv7em-none-eabihf': {} },
        },
        'acme-web': { root: 'packages/web', targets: { build: {}, 'cargo-cross-test-aarch64-apple-darwin': {} } },
      }),
    ).toEqual([
      { triple: 'aarch64-apple-darwin', path: '.cache/nextest/archive-aarch64-apple-darwin.tar.zst' },
      {
        triple: 'thumbv7em-none-eabihf',
        path: 'packages/embedded/.cache/nextest/archive-thumbv7em-none-eabihf.tar.zst',
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

describe('managed devenv release lock', () => {
  const gitEnv = mergeEnv(undefined, ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR']);
  const git = (cwd: string, ...args: string[]): string => {
    const identity = ['-c', 'user.name=smoo', '-c', 'user.email=smoo@example.invalid'];
    const unsigned = ['-c', 'commit.gpgSign=false', '-c', 'tag.gpgSign=false'];
    const result = Bun.spawnSync(['git', ...identity, ...unsigned, ...args], { cwd, env: gitEnv });
    if (result.exitCode !== 0) {
      throw new Error(`git ${args.join(' ')} failed: ${result.stderr.toString()}`);
    }
    return result.stdout.toString().trim();
  };

  interface DevenvRemote {
    readonly root: string;
    readonly url: string;
    readonly v203: string;
    readonly v204: string;
    readonly between: string;
    readonly v210: string;
    readonly head: string;
  }

  /**
   * A stand-in for cachix/devenv's repository: v2.0.3, older than every release whose builds stay cached; v2.0.4,
   * annotated, so ls-remote lists its commit only as refs/tags/v2.0.4^{}; an untagged commit after it; v2.10, which
   * only a numeric comparison puts after v2.0.4; and an untagged head.
   */
  const withDevenvRemote = async (run: (remote: DevenvRemote) => Promise<void>): Promise<void> => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-devenv-remote-'));
    try {
      const repo = join(root, 'devenv');
      await mkdir(repo);
      git(repo, 'init', '--quiet');
      const commit = (message: string): string => {
        git(repo, 'commit', '--quiet', '--allow-empty', '-m', message);
        return git(repo, 'rev-parse', 'HEAD');
      };
      const v203 = commit('2.0.3');
      git(repo, 'tag', 'v2.0.3');
      const v204 = commit('2.0.4');
      git(repo, 'tag', '--annotate', 'v2.0.4', '-m', 'v2.0.4');
      const between = commit('between releases');
      const v210 = commit('2.10');
      git(repo, 'tag', 'v2.10');
      const head = commit('main');
      await run({ root, url: pathToFileURL(repo).href, v203, v204, between, v210, head });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  };

  // The devenv node as `devenv update` writes it. `original` is what devenv.yaml declared; the check reads `locked`.
  const lockWithDevenv = (locked: Record<string, string>): string =>
    `${JSON.stringify(
      {
        nodes: {
          devenv: {
            locked: { dir: 'src/modules', ...locked },
            original: { dir: 'src/modules', owner: 'cachix', repo: 'devenv', type: 'github' },
          },
          root: { inputs: { devenv: 'devenv' } },
        },
        root: 'root',
        version: 7,
      },
      null,
      2,
    )}\n`;

  /** The check's failure count, and everything it printed to say why. */
  const checkLock = async (lock: string | null): Promise<{ failures: number; reason: string }> => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-devenv-release-'));
    const errors = spyOn(console, 'error').mockImplementation(() => {});
    try {
      if (lock !== null) {
        await mkdir(join(root, 'tooling/direnv'), { recursive: true });
        await writeFile(join(root, 'tooling/direnv/devenv.lock'), lock);
      }
      const failures = await validateDevenvReleaseLock(root);
      return { failures, reason: errors.mock.calls.map((call) => String(call[0])).join('\n') };
    } finally {
      errors.mockRestore();
      await rm(root, { recursive: true, force: true });
    }
  };

  it('checks a locked devenv commit against the release tags of its remote', async () => {
    await withDevenvRemote(async ({ root, url, v203, v204, between, v210, head }) => {
      // An annotated tag, matched through the commit it peels to.
      expect(await checkLock(lockWithDevenv({ type: 'git', url, rev: v204 }))).toEqual({ failures: 0, reason: '' });
      // As text, v2.10 would sort before v2.0.4.
      expect(await checkLock(lockWithDevenv({ type: 'git', url, rev: v210 }))).toEqual({ failures: 0, reason: '' });

      // git's own URL rewriting sends the derived GitHub URL to the stand-in, so no test reaches the network.
      const rewrite: Record<string, string> = {
        GIT_CONFIG_COUNT: '1',
        GIT_CONFIG_KEY_0: `url.${url}.insteadOf`,
        GIT_CONFIG_VALUE_0: 'https://github.com/cachix/devenv',
      };
      const saved = Object.fromEntries(Object.keys(rewrite).map((key) => [key, process.env[key]]));
      Object.assign(process.env, rewrite);
      try {
        const locked = { owner: 'cachix', repo: 'devenv', rev: v204, type: 'github' };
        expect(await checkLock(lockWithDevenv(locked))).toEqual({ failures: 0, reason: '' });
      } finally {
        for (const [key, value] of Object.entries(saved)) {
          if (value === undefined) delete process.env[key];
          else process.env[key] = value;
        }
      }

      // `between` is part of v2.10's history, which is not enough: the cache keeps the tag's build,
      // not its history's.
      for (const rev of [between, head, 'f'.repeat(40)]) {
        const { failures, reason } = await checkLock(lockWithDevenv({ type: 'git', url, rev }));
        expect(failures).toBe(1);
        expect(reason).toContain(`locks devenv at ${rev} from ${url}, which no release tag names,`);
        expect(reason).toContain('every cold runner compiles devenv from source');
      }
      const old = await checkLock(lockWithDevenv({ type: 'git', url, rev: v203 }));
      expect(old.failures).toBe(1);
      expect(old.reason).toContain('which only v2.0.3 names, not a release from v2.0.4 on');

      // A remote whose tags cannot be listed is refused, not passed.
      const unreachable = pathToFileURL(join(root, 'unreachable')).href;
      const unlisted = await checkLock(lockWithDevenv({ type: 'git', url: unreachable, rev: v204 }));
      expect(unlisted.failures).toBe(1);
      expect(unlisted.reason).toContain(`cannot list the release tags of ${unreachable}`);
    });
  });

  it('refuses a lock that names no remote or no commit', async () => {
    const rev = 'f'.repeat(40);
    const cases = [
      [{ path: '/nix/store/devenv', rev, type: 'path' }, 'with no remote (a path input, which names no git remote)'],
      [{ repo: 'devenv', rev, type: 'github' }, 'with no remote (a github input without an owner and a repo)'],
      [{ rev, type: 'git' }, 'with no remote (a git input without a url)'],
      [{ rev: 'main', type: 'git', url: 'https://github.com/cachix/devenv' }, 'with no commit hash'],
      [{ type: 'git', url: 'https://github.com/cachix/devenv' }, 'with no commit hash'],
    ] as const;
    for (const [locked, why] of cases) {
      const { failures, reason } = await checkLock(lockWithDevenv(locked));
      expect(failures).toBe(1);
      expect(reason).toContain(`tooling/direnv/devenv.lock locks devenv ${why}`);
    }
  });

  it('has nothing to check without a lock or a devenv node, and reports a lock it cannot read', async () => {
    expect((await checkLock(null)).failures).toBe(0);
    expect((await checkLock('{"nodes":{"root":{"inputs":{}}},"root":"root","version":7}\n')).failures).toBe(0);
    expect((await checkLock('{"nodes":')).failures).toBe(1);
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
        deploy: { executor: '@smoothbricks/nx-plugin:wrangler-deploy' },
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
    const deploy = { executor: '@smoothbricks/nx-plugin:wrangler-deploy' };
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
  it('distributes the exact Nx patch and repairs consumer drift through the managed writer', async () => {
    expect(managedFileTargetsForTest).toContainEqual({ target: managedNxPatch.path, executable: undefined });
    const [authoritative, packaged] = await Promise.all([
      readFile(join(REPO_ROOT, managedNxPatch.path), 'utf8'),
      readFile(join(REPO_ROOT, 'packages/nx-plugin/managed/raw', managedNxPatch.path), 'utf8'),
    ]);
    expect(packaged).toBe(authoritative);
    const root = await mkdtemp(join(tmpdir(), 'smoo-managed-nx-patch-'));
    try {
      const manifest = `${JSON.stringify({
        name: '@acme/managed-patch-consumer',
        private: true,
        workspaces: ['packages/*'],
      })}\n`;
      await writeFile(join(root, 'package.json'), manifest);
      const shape = context({ hasReleasePackages: false });
      await applyManagedFilesForContext(root, 'update', shape);
      const destination = join(root, managedNxPatch.path);
      expect(await readFile(destination, 'utf8')).toBe(authoritative);
      await writeFile(destination, `${authoritative}\nconsumer drift\n`);
      const checked = await applyManagedFilesForContext(root, 'check', shape);
      expect(checked).toContainEqual(
        expect.objectContaining({
          target: managedNxPatch.path,
          action: 'drifted',
        }),
      );
      await applyManagedFilesForContext(root, 'update', shape);
      expect(await readFile(destination, 'utf8')).toBe(authoritative);
      const unchanged = await applyManagedFilesForContext(root, 'check', shape);
      expect(unchanged).toContainEqual(
        expect.objectContaining({
          target: managedNxPatch.path,
          action: 'unchanged',
        }),
      );
      expect(await readFile(join(root, 'package.json'), 'utf8')).toBe(manifest);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('manages the devenv wrapper as an executable byte-exact copy', async () => {
    expect(managedFileTargetsForTest).toContainEqual({ target: 'tooling/devenv', executable: true });

    const [source, generated] = await Promise.all([
      readFile(join(REPO_ROOT, 'packages', 'nx-plugin', 'managed', 'raw', 'tooling', 'devenv'), 'utf8'),
      readFile(join(REPO_ROOT, 'tooling', 'devenv'), 'utf8'),
    ]);

    expect(generated).toBe(source);
  });

  it('keeps local overlay freshness in the owner evaluation and managed direnv watches', async () => {
    const [managedEnvrc, envrc, module, yaml, json, lock] = await Promise.all([
      readFile(join(REPO_ROOT, 'packages/nx-plugin/managed/raw/envrc'), 'utf8'),
      readFile(join(REPO_ROOT, '.envrc'), 'utf8'),
      readFile(join(REPO_ROOT, 'tooling/direnv/devenv.nix'), 'utf8'),
      readFile(join(REPO_ROOT, 'tooling/direnv/devenv.yaml'), 'utf8'),
      readFile(join(REPO_ROOT, 'tooling/direnv/devenv.json'), 'utf8'),
      readFile(join(REPO_ROOT, 'tooling/direnv/devenv.lock'), 'utf8'),
    ]);
    expect(envrc).toBe(managedEnvrc);
    const watches = managedEnvrc.match(/watch_file ([\s\S]*?)\n\n/)?.[1];
    expect(watches).toBeDefined();
    expect(watches).toContain('tooling/direnv/nixpkgs-overlay/flake.nix');
    expect(watches).toContain('tooling/direnv/nixpkgs-overlay/_sources/generated.nix');
    // A directory a devenv module reads is no file devenv hands direnv: the repository names it
    // and the managed shell watches it, without exporting the list.
    expect(managedEnvrc).toContain('watch_dir "$smoo_input_dir"');
    expect(managedEnvrc).toContain('unset smoo_input_dir SMOO_SHELL_INPUT_DIRS');
    expect(module).toContain('((import ./nixpkgs-overlay/flake.nix).outputs {}).overlays.default');
    expect(module).not.toContain('inputs.nixpkgs-overlay');
    expect(yaml).not.toContain('nixpkgs-overlay:');
    expect(json).not.toContain('"nixpkgs-overlay"');
    expect(lock).not.toContain('"nixpkgs-overlay"');
  });

  it('manages the pre-push hook as an executable copy', async () => {
    expect(managedFileTargetsForTest).toContainEqual({
      target: 'tooling/git-hooks/pre-push.sh',
      executable: true,
    });

    const [source, generated] = await Promise.all([
      readFile(
        join(REPO_ROOT, 'packages', 'nx-plugin', 'managed', 'raw', 'tooling', 'git-hooks', 'pre-push.sh'),
        'utf8',
      ),
      readFile(join(REPO_ROOT, 'tooling', 'git-hooks', 'pre-push.sh'), 'utf8'),
    ]);

    expect(generated).toBe(source);
  });

  it('persists the ttsc cache path the shell computes, leaving host overrides untouched', async () => {
    const temp = await mkdtemp(join(tmpdir(), 'smoo-github-bootstrap-'));
    const bin = join(temp, 'bin');
    await mkdir(bin);
    // The workspace the emulated shell enters: a checkout of its own with ttsc-env.sh copied in, so the cache
    // that ttsc-env.sh computes lands under temp rather than in the checkout the suite runs from.
    const checkout = join(temp, 'checkout');
    await mkdir(join(checkout, 'tooling', 'direnv'), { recursive: true });
    await copyFile(
      join(REPO_ROOT, 'tooling', 'direnv', 'ttsc-env.sh'),
      join(checkout, 'tooling', 'direnv', 'ttsc-env.sh'),
    );
    const devenv = join(bin, 'devenv');
    // Emulates `devenv shell [flags] -- cmd...` the way devenv.smoo.nix's
    // enterShell behaves: cd to the workspace root, source ttsc-env.sh, which
    // computes TTSC_CACHE_DIR honoring a host-provided value, then run the
    // command — so build-shell's wholesale environment capture sees real shell
    // exports instead of a hand-copied list.
    await writeFile(
      devenv,
      [
        '#!/usr/bin/env bash',
        'set -euo pipefail',
        // build-shell runs the install tasks first; they are not what this measures.
        '[ "${1:-}" = tasks ] && exit 0',
        'while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do shift; done',
        'shift',
        `cd '${checkout}'`,
        '. tooling/direnv/ttsc-env.sh',
        'exec "$@"',
        '',
      ].join('\n'),
    );
    await chmod(devenv, 0o755);

    try {
      const cases = [
        // No host value: the shell computes the checkout's default and the
        // capture persists it for the workflow steps that follow.
        { input: '', expected: `TTSC_CACHE_DIR=${join(checkout, '.cache', 'ttsc-plugins')}\n` },
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
              // The inherited environment may name the real checkout's cache: every ttsc path points at this fixture.
              NX_WORKSPACE_ROOT_PATH: checkout,
              TTSC_CACHE_DIR: cache.input,
              TTSC_GO_CACHE_DIR: '',
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
          join(
            REPO_ROOT,
            'packages',
            'nx-plugin',
            'managed',
            'templates',
            'github',
            'actions',
            action.name,
            'action.yml',
          ),
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
        join(REPO_ROOT, 'packages', 'nx-plugin', 'managed', 'templates', 'github', 'actions'),
        join(REPO_ROOT, '.github', 'actions'),
      ]) {
        const content = await readFile(join(actionRoot, action.name, 'action.yml'), 'utf8');
        const osKeyLines = content.split('\n').filter((line) => line.includes('${{ runner.os }}'));

        expect(osKeyLines).toHaveLength(action.osKeyLines);
        expect(osKeyLines.every((line) => line.includes(ARCHITECTURE_SCOPED_PREFIX))).toBe(true);
      }
    }
  });

  it('restores isolated Bun aliases with the cache targets they name, never the install stamp', async () => {
    const action = typia.assert<{ runs: { steps: [{ with: { path: string } }, { shell: 'bash'; run: string }] } }>(
      Bun.YAML.parse(await readFile(join(REPO_ROOT, '.github/actions/cache-node-modules/action.yml'), 'utf8')),
    );
    const paths = typia.assert<Array<'node_modules' | '~/.bun/install/cache/links'>>(
      action.runs.steps[0].with.path.trim().split('\n'),
    );
    const dropStamp = action.runs.steps[1].run;
    const temp = await mkdtemp(join(tmpdir(), 'smoo-bun-cache-'));
    const modules = join(temp, 'checkout', 'node_modules');
    const links = join(temp, 'home', '.bun', 'install', 'cache', 'links');
    const entry = join(links, 'fixture@1.0.0-cache');
    const storeAlias = join(modules, '.bun', 'fixture@1.0.0');
    const manifest = join(modules, 'fixture', 'package.json');
    const stamp = join(modules, '.smoo-install');
    const content = `${JSON.stringify({ name: 'fixture', version: '1.0.0' })}\n`;
    const modulesOnly = join(temp, 'modules-only.tar');
    const configured = join(temp, 'configured.tar');
    const tar = (...args: string[]): void => {
      expect(Bun.spawnSync(['tar', ...args], { cwd: temp, stderr: 'inherit' }).exitCode).toBe(0);
    };
    try {
      await mkdir(join(entry, 'node_modules', 'fixture'), { recursive: true });
      await mkdir(join(modules, '.bun'), { recursive: true });
      await writeFile(join(entry, 'node_modules', 'fixture', 'package.json'), content);
      await symlink(entry, storeAlias);
      await symlink(join('.bun', 'fixture@1.0.0', 'node_modules', 'fixture'), join(modules, 'fixture'));
      await writeFile(stamp, '{}\n');
      expect(await readFile(manifest, 'utf8')).toBe(content);

      tar('-cf', modulesOnly, relative(temp, modules));
      tar('-cf', configured, ...paths.map((path) => relative(temp, path === 'node_modules' ? modules : links)));
      await rm(modules, { recursive: true, force: true });
      await rm(links, { recursive: true, force: true });

      tar('-xf', modulesOnly);
      await expect(readFile(manifest, 'utf8')).rejects.toThrow('ENOENT');
      expect(await readlink(storeAlias)).toBe(entry);
      await rm(modules, { recursive: true, force: true });

      tar('-xf', configured);
      expect(await readlink(storeAlias)).toBe(entry);
      expect(await readFile(manifest, 'utf8')).toBe(content);
      expect(paths).toEqual(['node_modules', '~/.bun/install/cache/links']);
      // actions/cache archives node_modules whole, the stamp devenv's smoo:install watches included, and a `!` path
      // inside a listed directory excludes nothing (actions/toolkit#713). Workspace members' own node_modules links
      // are not archived, so the step after the restore drops the stamp and only an install in this checkout can
      // claim the tree current (Publish runs 39132666631 and 39143439571 skipped that install).
      expect(await readFile(stamp, 'utf8')).toBe('{}\n');
      expect(
        Bun.spawnSync(['bash', '-c', dropStamp], { cwd: join(temp, 'checkout'), stderr: 'inherit' }).exitCode,
      ).toBe(0);
      await expect(readFile(stamp, 'utf8')).rejects.toThrow('ENOENT');
      expect(await readFile(manifest, 'utf8')).toBe(content);
    } finally {
      await rm(temp, { recursive: true, force: true });
    }
  });

  it('falls back only to a Nix store saved from the same devenv pins', async () => {
    const template = await readFile(
      join(
        REPO_ROOT,
        'packages',
        'nx-plugin',
        'managed',
        'templates',
        'github',
        'actions',
        'setup-devenv',
        'action.yml',
      ),
      'utf8',
    );
    const input = (name: string) =>
      template
        .split('\n')
        .map((line) => line.trim())
        .find((line) => line.startsWith(`${name}: `))
        ?.slice(name.length + 2);
    const primaryKey = input('primary-key');
    const restorePrefix = input('restore-prefixes-first-match');
    const pinsHash = "${{ hashFiles('tooling/direnv/devenv.lock', 'tooling/direnv/devenv.yaml') }}";

    // A hash has a fixed length, so a prefix that ends right after it matches only stores saved from these pins.
    expect(restorePrefix).toEndWith(`-${pinsHash}-`);
    expect(primaryKey).toStartWith(restorePrefix ?? 'no restore prefix');
    // devenv.lock locks the overlay as a path without a narHash, so only hashing it directly notices an edit.
    expect(primaryKey).toContain("'tooling/direnv/nixpkgs-overlay/**'");
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
        { triple: 'aarch64-apple-darwin', path: '.cache/nextest/archive-aarch64-apple-darwin.tar.zst' },
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
    const deploy = { executor: '@smoothbricks/nx-plugin:wrangler-deploy' };
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
    expect(managedFileTargetsForContext(shape)).not.toContain('.github/workflows/pr-stage-sweep.yml');
  });

  it('adds the preview cleanup and sweep workflows only where a cloudflare stage deploy makes preview stages', () => {
    for (const workflow of ['.github/workflows/pr-preview-cleanup.yml', '.github/workflows/pr-stage-sweep.yml']) {
      expect(managedFileTargetsForContext(deploying)).toContain(workflow);
      expect(managedFileTargetsForContext({ ...deploying, stagingDeployProvider: undefined })).not.toContain(workflow);
    }
  });

  it('threads the declared private npm read token into the preview cleanup job', () => {
    const privateNpm = { scope: '@priv.test', readTokenEnv: 'PRIV_NPM_READ_TOKEN' };
    const cleanup = renderManagedWorkflowForTest('pr-preview-cleanup-workflow', { ...deploying, privateNpm });

    expect(cleanup).toContain('    env:\n      PRIV_NPM_READ_TOKEN: ${{ secrets.PRIV_NPM_READ_TOKEN }}\n    steps:\n');
    expect(renderManagedWorkflowForTest('pr-preview-cleanup-workflow', deploying)).not.toMatch(/^ {4}env:/m);
  });

  it('threads the staging environment, Cloudflare deploy secrets and sibling sources into the preview cleanup job', () => {
    const cleanup = renderManagedWorkflowForTest('pr-preview-cleanup-workflow', {
      ...deploying,
      ciEnvironments: { staging: 'staging' },
      ciDeploySecrets: { CLOUDFLARE_API_TOKEN: 'CF_STAGE_TOKEN' },
      sourceCheckouts: [{ path: '../shared', repository: 'https://git.example.test/shared.git' }],
    });

    expect(cleanup).toContain('    environment: staging\n');
    expect(cleanup).toContain('          CLOUDFLARE_API_TOKEN: ${{ secrets.CF_STAGE_TOKEN }}\n');
    expect(cleanup).toContain('git clone --filter=blob:none https://git.example.test/shared.git "$root/../shared"');
  });

  it('threads the declared Cargo credentials into the preview cleanup job', () => {
    const cleanup = renderManagedWorkflowForTest('pr-preview-cleanup-workflow', {
      ...deploying,
      cargoCredentials: { gitOrigins: [{ origin: 'https://git.example.test', tokenEnv: 'SOURCE_READ_TOKEN' }] },
    });

    expect(cleanup).toContain('    env:\n      SOURCE_READ_TOKEN: ${{ secrets.SOURCE_READ_TOKEN }}\n    steps:\n');
    expect(cleanup).toContain('      - name: Prepare Cargo credentials\n');
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
