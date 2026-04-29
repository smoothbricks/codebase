import { afterEach, describe, expect, it, spyOn } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import {
  applyFixableMonorepoDefaults,
  applyNxProjectNameDefaults,
  applyWorkspaceDependencyDefaults,
  listValidCommitScopes,
  validateNxProjectNames,
  validateNxReleaseConfig,
  validateRootPackagePolicy,
  validateWorkspaceDependencies,
} from './package-policy.js';

describe('root smoo monorepo policy', () => {
  afterEach(() => {
    console.error = originalConsoleError;
  });

  it('fixes root scripts and Nx plugin defaults', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-package-policy-'));
    try {
      await writeJson(join(root, 'package.json'), validRootPackage({ scripts: { lint: 'nx affected -t lint' } }));
      await writeJson(join(root, 'nx.json'), validNxJson());

      expect(validateRootPackagePolicy(root)).toBe(4);
      expect(validateNxReleaseConfig(root)).toBe(5);

      applyFixableMonorepoDefaults(root);

      const rootPackage = await readJson(join(root, 'package.json'));
      const nxJson = await readJson(join(root, 'nx.json'));
      expect(rootPackage.scripts).toEqual({
        'format:changed': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml --also-unstaged',
        'format:staged': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml',
        lint: 'nx run-many -t lint',
        'lint:fix': 'git-format-staged --config tooling/git-hooks/git-format-staged.yml --unstaged',
      });
      expect(nxJson.targetDefaults).toEqual({ build: { cache: true, outputs: ['{projectRoot}/dist'] } });
      expect(nxJson.plugins).toEqual([
        {
          plugin: '@nx/js/typescript',
          options: {
            typecheck: { targetName: 'typecheck' },
            build: {
              targetName: 'tsc-js',
              configName: 'tsconfig.lib.json',
              buildDepsName: 'build-deps',
              watchDepsName: 'watch-deps',
            },
          },
        },
        '@smoothbricks/nx-plugin',
      ]);
      expect(validateRootPackagePolicy(root)).toBe(0);
      expect(validateNxReleaseConfig(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects all colon-style Nx target defaults', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-package-policy-'));
    try {
      await writeJson(join(root, 'package.json'), validRootPackage());
      await writeJson(join(root, 'nx.json'), {
        ...validConfiguredNxJson(),
        targetDefaults: {
          'build:wasm': {},
          'lint:fix': {},
        },
      });

      expect(validateNxReleaseConfig(root)).toBe(4);

      applyFixableMonorepoDefaults(root);

      const nxJson = await readJson(join(root, 'nx.json'));
      expect(nxJson.targetDefaults).toEqual({ build: { cache: true, outputs: ['{projectRoot}/dist'] } });
      expect(validateNxReleaseConfig(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('explains Nx plugin conventions when nx.json is not configured', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-package-policy-'));
    try {
      await writeJson(join(root, 'package.json'), validRootPackage());
      await writeJson(join(root, 'nx.json'), {
        ...validConfiguredNxJson(),
        plugins: [],
      });
      const errors = captureConsoleErrors();

      expect(validateNxReleaseConfig(root)).toBe(2);

      expect(errors.join('\n')).toContain('Official Nx owns TypeScript library inference');
      expect(errors.join('\n')).toContain('tsconfig.lib.json produces tsc-js');
      expect(errors.join('\n')).toContain('Smoo relies on this plugin to infer convention targets');
      expect(errors.join('\n')).not.toContain('Fix:');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('explains why TypeScript build targetName is tsc-js', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-package-policy-'));
    try {
      await writeJson(join(root, 'package.json'), validRootPackage());
      await writeJson(join(root, 'nx.json'), {
        ...validConfiguredNxJson(),
        plugins: [
          {
            plugin: '@nx/js/typescript',
            options: { build: { targetName: 'build', configName: 'tsconfig.lib.json' } },
          },
          '@smoothbricks/nx-plugin',
        ],
      });
      const errors = captureConsoleErrors();

      expect(validateNxReleaseConfig(root)).toBe(1);

      expect(errors.join('\n')).toContain('build.targetName must be tsc-js');
      expect(errors.join('\n')).toContain('build is reserved for aggregate targets');
      expect(errors.join('\n')).not.toContain('Fix:');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('Nx project name policy', () => {
  it('fixes same-scope packages without touching external or unscoped packages', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [
        { dir: 'cli', name: '@smoothbricks/cli', nx: { tags: ['npm:public'] } },
        { dir: 'external', name: '@external/thing' },
        { dir: 'tool', name: 'eslint-stdout' },
      ],
    });
    try {
      expect(validateNxProjectNames(root)).toBe(1);

      applyNxProjectNameDefaults(root);

      const cli = JSON.parse(await readFile(join(root, 'packages/cli/package.json'), 'utf8'));
      const external = JSON.parse(await readFile(join(root, 'packages/external/package.json'), 'utf8'));
      const tool = JSON.parse(await readFile(join(root, 'packages/tool/package.json'), 'utf8'));
      expect(cli.nx).toEqual({ tags: ['npm:public'], name: 'cli' });
      expect(external.nx).toBeUndefined();
      expect(tool.nx).toBeUndefined();
      expect(validateNxProjectNames(root)).toBe(0);
      expect(listValidCommitScopes(root)).toEqual(new Set(['cli', 'release']));
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('workspace package script policy', () => {
  it('rejects all colon-style package Nx targets', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [
        {
          dir: 'native',
          name: '@smoothbricks/native',
          nx: { name: 'native', targets: { 'build:ts': {}, 'lint:fix': {} } },
        },
      ],
    });
    try {
      expect(validateWorkspaceDependencies(root)).toBe(2);

      const resolvedTargetsByProject = new Map([['native', new Set(['build', 'tsc-js', 'zig-wasm'])]]);
      applyWorkspaceDependencyDefaults(root, { resolvedTargetsByProject });

      const native = await readJson(join(root, 'packages/native/package.json'));
      expect(native.nx).toEqual({ name: 'native', targets: {} });
      expect(validateWorkspaceDependencies(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('migrates colon build target aliases before removing colon targets', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [
        { dir: 'lib', name: '@smoothbricks/lib', nx: { name: 'lib' } },
        {
          dir: 'native',
          name: '@smoothbricks/native',
          dependencies: { '@smoothbricks/lib': 'workspace:*' },
          scripts: {
            'build:custom': 'nx run native:build:custom',
            'build:zig': 'nx run native:build:zig',
            'build:ts': 'nx run native:build:ts',
          },
          nx: {
            name: 'native',
            targets: {
              build: { dependsOn: ['^build', 'build:ts', 'build:zig', 'build:custom'] },
              'build:custom': {},
              'build:zig': {
                executor: 'nx:run-commands',
                options: { command: 'zig build wasm', cwd: '{projectRoot}' },
              },
              'build:ts': {
                executor: 'nx:run-commands',
                options: { command: 'tsc --build tsconfig.lib.json', cwd: '{projectRoot}' },
              },
            },
          },
        },
      ],
    });
    try {
      expect(validateWorkspaceDependencies(root)).toBe(7);

      const resolvedTargetsByProject = new Map([['native', new Set(['build', 'custom', 'tsc-js', 'zig-wasm'])]]);
      applyWorkspaceDependencyDefaults(root, { resolvedTargetsByProject });

      const native = await readJson(join(root, 'packages/native/package.json'));
      expect(native.scripts).toEqual({
        'build:custom': 'nx run native:custom',
        'build:zig': 'nx run native:zig-wasm',
        'build:ts': 'nx run native:tsc-js',
      });
      expect(native.nx).toEqual({
        name: 'native',
        targets: {},
      });
      expect(validateWorkspaceDependencies(root, { resolvedTargetsByProject })).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('tells agents to remove explicit aggregate build targets instead of replacing colon targets with build', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [
        {
          dir: 'native',
          name: '@smoothbricks/native',
          nx: {
            name: 'native',
            targets: {
              build: { executor: 'nx:noop', dependsOn: ['^build', 'build:wasm'] },
              'build:wasm': {},
            },
          },
        },
      ],
    });
    try {
      const errors = captureConsoleErrors();

      expect(validateWorkspaceDependencies(root)).toBe(3);

      const output = errors.join('\n');
      expect(output).toContain('package.json nx.targets.build must not define the aggregate build target');
      expect(output).toContain('@smoothbricks/nx-plugin infers build from concrete targets');
      expect(output).toContain('Remove nx.targets.build and fix the concrete target inference instead');
      expect(output).toContain('The aggregate build target is inferred; remove package.json nx.targets.build');
      expect(output).toContain('Remove this target; colon names are only allowed as package-script aliases');

      applyWorkspaceDependencyDefaults(root);

      const native = await readJson(join(root, 'packages/native/package.json'));
      expect(native.nx).toEqual({ name: 'native', targets: {} });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects old test tsconfig output and build.zig without steps', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [{ dir: 'native', name: '@smoothbricks/native', nx: { name: 'native' } }],
    });
    try {
      await writeJson(join(root, 'packages/native/tsconfig.test.json'), {
        compilerOptions: {
          composite: true,
          declaration: true,
          declarationMap: true,
          outDir: 'dist-test',
          tsBuildInfoFile: 'dist-test/tsconfig.test.tsbuildinfo',
        },
      });
      await writeFile(join(root, 'packages/native/build.zig'), 'pub fn build(b: *std.Build) void { _ = b; }\n');

      expect(validateWorkspaceDependencies(root)).toBe(7);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('creates no-emit test typecheck config for packages that use bun test', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [{ dir: 'app', name: '@smoothbricks/app', scripts: { test: 'bun test --pass-with-no-tests' } }],
    });
    try {
      await writeJson(join(root, 'packages/app/tsconfig.json'), {
        files: [],
        include: [],
        references: [{ path: './tsconfig.lib.json' }, { path: './tsconfig.test.json' }],
      });
      await writeJson(join(root, 'packages/app/tsconfig.lib.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: { baseUrl: '.', rootDir: 'src', types: ['node'], outDir: 'dist' },
      });

      expect(validateWorkspaceDependencies(root)).toBe(1);

      applyWorkspaceDependencyDefaults(root);

      const testTsconfig = await readJson(join(root, 'packages/app/tsconfig.test.json'));
      expect(testTsconfig).toEqual({
        extends: '../../tsconfig.base.json',
        compilerOptions: {
          baseUrl: '.',
          rootDir: 'src',
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
          noEmit: true,
          types: ['bun'],
        },
        include: [
          'src/**/*.test.ts',
          'src/**/*.spec.ts',
          'src/**/__tests__/**/*.ts',
          'src/**/__tests__/**/*.tsx',
          'src/test-suite-tracer.ts',
        ],
        references: [{ path: './tsconfig.lib.json' }],
      });
      const projectTsconfig = await readJson(join(root, 'packages/app/tsconfig.json'));
      expect(projectTsconfig.references).toEqual([{ path: './tsconfig.lib.json' }]);
      expect(validateWorkspaceDependencies(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('does not require tsconfig.test.json for non-Bun test runners', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [{ dir: 'app', name: '@smoothbricks/app', scripts: { test: 'vitest run' } }],
    });
    try {
      expect(validateWorkspaceDependencies(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('detects bun test commands configured directly in Nx targets', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [
        {
          dir: 'app',
          name: '@smoothbricks/app',
          nx: {
            name: 'app',
            targets: {
              test: { executor: 'nx:run-commands', options: { command: 'bun test', cwd: '{projectRoot}' } },
            },
          },
        },
      ],
    });
    try {
      expect(validateWorkspaceDependencies(root)).toBe(1);

      applyWorkspaceDependencyDefaults(root);

      const testTsconfig = await readJson(join(root, 'packages/app/tsconfig.test.json'));
      expect(testTsconfig.compilerOptions).toMatchObject({ noEmit: true, types: ['bun'] });
      expect(validateWorkspaceDependencies(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('accepts noEmit test tsconfig and build.zig with an explicit step', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [{ dir: 'native', name: '@smoothbricks/native', nx: { name: 'native', targets: { lint: {} } } }],
    });
    try {
      await writeJson(join(root, 'packages/native/tsconfig.test.json'), { compilerOptions: { noEmit: true } });
      await writeFile(
        join(root, 'packages/native/build.zig'),
        'pub fn build(b: *std.Build) void { _ = b.step("build", "Build native artifact"); }\n',
      );

      expect(validateWorkspaceDependencies(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rewrites safe scripts that use workspace dependencies into Nx aliases', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [
        { dir: 'lib', name: '@smoothbricks/lib', nx: { name: 'lib' } },
        {
          dir: 'app',
          name: '@smoothbricks/app',
          dependencies: { '@smoothbricks/lib': '^0.0.0' },
          scripts: {
            dev: 'astro dev',
            serve: 'vite dev --host 0.0.0.0',
            test: 'bun test --pass-with-no-tests',
            build: 'wrangler build --outdir dist',
            deploy: 'wrangler deploy',
            astro: 'astro',
          },
          nx: { name: 'app' },
        },
      ],
    });
    try {
      expect(validateWorkspaceDependencies(root)).toBe(6);

      applyWorkspaceDependencyDefaults(root);

      const app = await readJson(join(root, 'packages/app/package.json'));
      expect(app.dependencies).toEqual({ '@smoothbricks/lib': 'workspace:*' });
      expect(app.scripts).toEqual({
        dev: 'nx run app:dev --tui=false --outputStyle=stream',
        serve: 'nx run app:serve --tui=false --outputStyle=stream',
        test: 'nx run app:test',
        build: 'nx run app:build',
        deploy: 'wrangler deploy',
        astro: 'astro',
      });
      expect(app.nx).toEqual({
        name: 'app',
        targets: {
          dev: {
            executor: 'nx:run-commands',
            dependsOn: ['^build'],
            continuous: true,
            options: { command: 'astro dev', cwd: '{projectRoot}' },
          },
          serve: {
            executor: 'nx:run-commands',
            dependsOn: ['^build'],
            continuous: true,
            options: { command: 'vite dev --host 0.0.0.0', cwd: '{projectRoot}' },
          },
          test: {
            executor: 'nx:run-commands',
            dependsOn: ['^build'],
            options: { command: 'bun test --pass-with-no-tests', cwd: '{projectRoot}' },
          },
          build: {
            executor: 'nx:run-commands',
            dependsOn: ['^build'],
            options: { command: 'wrangler build --outdir dist', cwd: '{projectRoot}' },
          },
        },
      });
      expect(validateWorkspaceDependencies(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects Nx target commands that recurse through package scripts', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [
        { dir: 'lib', name: '@smoothbricks/lib', nx: { name: 'lib' } },
        {
          dir: 'app',
          name: '@smoothbricks/app',
          dependencies: { '@smoothbricks/lib': 'workspace:*' },
          scripts: { test: 'nx run app:test' },
          nx: {
            name: 'app',
            targets: {
              test: {
                executor: 'nx:run-commands',
                options: { command: 'bun run test', cwd: '{projectRoot}' },
              },
            },
          },
        },
      ],
    });
    try {
      expect(validateWorkspaceDependencies(root)).toBe(2);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('moves simple leading environment assignments into Nx target options', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [
        { dir: 'lib', name: '@smoothbricks/lib', nx: { name: 'lib' } },
        {
          dir: 'website',
          name: '@smoothbricks/website',
          dependencies: { '@smoothbricks/lib': 'workspace:*' },
          scripts: {
            dev: "NODE_OPTIONS='--import=extensionless/register' astro dev",
            build: 'astro build',
            preview: "NODE_OPTIONS='--import=extensionless/register' astro preview",
            astro: 'astro',
          },
          nx: { name: 'website' },
        },
      ],
    });
    try {
      applyWorkspaceDependencyDefaults(root);

      const website = await readJson(join(root, 'packages/website/package.json'));
      expect(website.scripts).toEqual({
        dev: 'nx run website:dev --tui=false --outputStyle=stream',
        build: 'nx run website:build',
        preview: 'nx run website:preview --tui=false --outputStyle=stream',
        astro: 'astro',
      });
      expect(website.nx).toEqual({
        name: 'website',
        targets: {
          dev: {
            executor: 'nx:run-commands',
            dependsOn: ['^build'],
            continuous: true,
            options: {
              command: 'astro dev',
              cwd: '{projectRoot}',
              env: { NODE_OPTIONS: '--import=extensionless/register' },
            },
          },
          build: {
            executor: 'nx:run-commands',
            dependsOn: ['^build'],
            options: { command: 'astro build', cwd: '{projectRoot}' },
          },
          preview: {
            executor: 'nx:run-commands',
            dependsOn: ['build'],
            continuous: true,
            options: {
              command: 'astro preview',
              cwd: '{projectRoot}',
              env: { NODE_OPTIONS: '--import=extensionless/register' },
            },
          },
        },
      });
      expect(validateWorkspaceDependencies(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('materializes nx.name when rewriting scripts for packages with existing nx config', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [
        { dir: 'lib', name: '@smoothbricks/lib', nx: { name: 'lib' } },
        {
          dir: 'app',
          name: '@external/app',
          dependencies: { '@smoothbricks/lib': 'workspace:*' },
          scripts: { test: 'bun test --pass-with-no-tests' },
          nx: { targets: {} },
        },
      ],
    });
    try {
      expect(validateWorkspaceDependencies(root)).toBe(2);

      applyWorkspaceDependencyDefaults(root);

      const app = await readJson(join(root, 'packages/app/package.json'));
      expect(app.scripts).toEqual({ test: 'nx run @external/app:test' });
      expect(app.nx).toEqual({
        name: '@external/app',
        targets: {
          test: {
            executor: 'nx:run-commands',
            dependsOn: ['^build'],
            options: { command: 'bun test --pass-with-no-tests', cwd: '{projectRoot}' },
          },
        },
      });
      expect(validateWorkspaceDependencies(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

async function createWorkspace(input: {
  rootName: string;
  packages: Array<{
    dir: string;
    name: string;
    dependencies?: Record<string, string>;
    scripts?: Record<string, string>;
    nx?: Record<string, unknown>;
  }>;
}): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-package-policy-'));
  await writeJson(join(root, 'package.json'), {
    name: input.rootName,
    version: '0.0.0',
    private: true,
    workspaces: ['packages/*'],
  });
  for (const pkg of input.packages) {
    await writeJson(join(root, `packages/${pkg.dir}/package.json`), {
      name: pkg.name,
      version: '0.0.0',
      ...(pkg.dependencies ? { dependencies: pkg.dependencies } : {}),
      ...(pkg.scripts ? { scripts: pkg.scripts } : {}),
      ...(pkg.nx ? { nx: pkg.nx } : {}),
    });
  }
  return root;
}

async function readJson(path: string): Promise<Record<string, unknown>> {
  return JSON.parse(await readFile(path, 'utf8'));
}

const originalConsoleError = console.error;

function captureConsoleErrors(): string[] {
  const errors: string[] = [];
  spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    errors.push(args.join(' '));
  });
  return errors;
}

function validRootPackage(extra: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    name: '@smoothbricks/codebase',
    version: '0.0.0',
    private: true,
    license: 'MIT',
    repository: { type: 'git', url: 'git+https://github.com/smoothbricks/codebase.git' },
    packageManager: 'bun@1.3.13',
    engines: { node: '>=24.0.0' },
    devDependencies: { '@types/bun': '1.3.13' },
    workspaces: ['packages/*'],
    ...extra,
  };
}

function validNxJson(): Record<string, unknown> {
  return {
    targetDefaults: {
      'lint:fix': { executor: 'nx:run-commands' },
    },
    plugins: [
      {
        plugin: '@nx/js/typescript',
        options: {
          typecheck: { targetName: 'typecheck' },
          build: { targetName: 'build', configName: 'tsconfig.lib.json' },
        },
      },
    ],
    release: {
      projectsRelationship: 'independent',
      version: {
        specifierSource: 'conventional-commits',
        currentVersionResolver: 'git-tag',
        fallbackCurrentVersionResolver: 'disk',
        versionActions: '@smoothbricks/cli/nx-version-actions',
      },
      releaseTag: { pattern: '{projectName}@{version}' },
      changelog: {
        workspaceChangelog: false,
        projectChangelogs: {
          createRelease: false,
          file: false,
          renderOptions: { authors: true, applyUsernameToAuthors: true },
        },
      },
    },
  };
}

function validConfiguredNxJson(): Record<string, unknown> {
  return {
    ...validNxJson(),
    targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } },
    plugins: [
      {
        plugin: '@nx/js/typescript',
        options: {
          typecheck: { targetName: 'typecheck' },
          build: {
            targetName: 'tsc-js',
            configName: 'tsconfig.lib.json',
            buildDepsName: 'build-deps',
            watchDepsName: 'watch-deps',
          },
        },
      },
      '@smoothbricks/nx-plugin',
    ],
  };
}

async function writeJson(path: string, value: Record<string, unknown>): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
