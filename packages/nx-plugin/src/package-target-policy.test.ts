import { beforeEach, describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { addProjectConfiguration, readJson, type Tree } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';

import {
  applyPackageTargetPolicy,
  applyPackageTargetPolicyTree,
  applyPackageTargets,
  checkPackageTargetPolicy,
  checkPackageTargetPolicyTree,
  checkPackageTargets,
  nxRunAlias,
  packageNxProjectName,
  type ResolvedProjectTargets,
} from './package-target-policy.js';

describe('package target policy', () => {
  it('rejects colon-style package Nx targets', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-pkg-target-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        nx: {
          name: 'app',
          targets: {
            'build:ts': {
              executor: 'nx:run-commands',
              options: { command: 'tsc --build tsconfig.lib.json', cwd: '{projectRoot}' },
            },
            'lint:fix': {
              executor: 'nx:run-commands',
              options: { command: 'biome check --apply', cwd: '{projectRoot}' },
            },
          },
        },
      });

      const issues = checkPackageTargetPolicy(root);
      expect(issues.length).toBeGreaterThanOrEqual(2);
      expect(issues.some((i) => i.message.includes('build:ts') && i.message.includes('colon target names'))).toBe(true);
      expect(issues.some((i) => i.message.includes('lint:fix') && i.message.includes('colon target names'))).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('migrates colon build targets to tool-output names', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-pkg-target-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeJson(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
        dependencies: { '@scope/other': 'workspace:*' },
        scripts: {
          'build:ts': 'nx run lib:build:ts',
        },
        nx: {
          name: 'lib',
          targets: {
            'build:ts': {
              executor: 'nx:run-commands',
              options: { command: 'tsc --build tsconfig.lib.json', cwd: '{projectRoot}' },
            },
            build: {
              executor: 'nx:noop',
              dependsOn: ['^build', 'build:ts'],
            },
          },
        },
      });
      await writeJson(join(root, 'packages/other/package.json'), {
        name: '@scope/other',
      });

      expect(applyPackageTargetPolicy(root)).toBe(true);

      const lib = JSON.parse(await readFile(join(root, 'packages/lib/package.json'), 'utf8'));
      const targets = lib.nx.targets;
      // build:ts should be renamed to tsc-js
      expect(targets['build:ts']).toBeUndefined();
      expect(targets['tsc-js']).toBeDefined();
      expect(targets['tsc-js'].options.command).toBe('tsc --build tsconfig.lib.json');
      // dependsOn in build target should be updated
      expect(targets.build.dependsOn).toContain('tsc-js');
      expect(targets.build.dependsOn).not.toContain('build:ts');
      // scripts should be updated
      expect(lib.scripts['build:ts']).toBe('nx run lib:tsc-js');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('removes noop aggregate build targets matching resolved plugin output', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-pkg-target-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeJson(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
        nx: {
          name: 'lib',
          targets: {
            build: {
              executor: 'nx:noop',
              dependsOn: ['^build', '*-js'],
            },
          },
        },
      });

      const resolvedTargetsByProject = new Map<string, ResolvedProjectTargets>([
        [
          'lib',
          {
            targets: new Set(['build', 'tsc-js']),
            buildDependsOn: ['^build', '*-js'],
          },
        ],
      ]);

      expect(applyPackageTargetPolicy(root, { resolvedTargetsByProject })).toBe(true);

      const lib = JSON.parse(await readFile(join(root, 'packages/lib/package.json'), 'utf8'));
      expect(lib.nx.targets.build).toBeUndefined();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('keeps non-noop build targets even when dependsOn matches', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-pkg-target-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeJson(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
        nx: {
          name: 'lib',
          targets: {
            build: {
              executor: 'nx:run-commands',
              dependsOn: ['^build', '*-js'],
              options: { command: 'echo build', cwd: '{projectRoot}' },
            },
          },
        },
      });

      const resolvedTargetsByProject = new Map<string, ResolvedProjectTargets>([
        [
          'lib',
          {
            targets: new Set(['build', 'tsc-js']),
            buildDependsOn: ['^build', '*-js'],
          },
        ],
      ]);

      expect(applyPackageTargetPolicy(root, { resolvedTargetsByProject })).toBe(false);

      const lib = JSON.parse(await readFile(join(root, 'packages/lib/package.json'), 'utf8'));
      expect(lib.nx.targets.build).toBeDefined();
      expect(lib.nx.targets.build.options.command).toBe('echo build');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rewrites safe scripts into Nx aliases', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-pkg-target-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeJson(join(root, 'packages/web/package.json'), {
        name: '@scope/web',
        dependencies: { '@scope/lib': 'workspace:*' },
        scripts: {
          dev: 'astro dev',
          build: 'astro build',
        },
        nx: { name: 'web' },
      });
      await writeJson(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
      });

      expect(applyPackageTargetPolicy(root)).toBe(true);

      const web = JSON.parse(await readFile(join(root, 'packages/web/package.json'), 'utf8'));
      // Vite/Astro dev servers render best with dynamic legacy output.
      expect(web.scripts.dev).toBe('nx run web:dev --outputStyle=dynamic-legacy');
      // build script should become an Nx alias without streaming flags
      expect(web.scripts.build).toBe('nx run web:build');
      // nx.targets.dev should be created with continuous flag
      expect(web.nx.targets.dev.continuous).toBe(true);
      expect(web.nx.targets.dev.executor).toBe('nx:run-commands');
      expect(web.nx.targets.dev.options.command).toBe('astro dev');
      // nx.targets.build should be created
      expect(web.nx.targets.build.executor).toBe('nx:run-commands');
      expect(web.nx.targets.build.options.command).toBe('astro build');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects recursive script commands', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-pkg-target-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        dependencies: { '@scope/lib': 'workspace:*' },
        scripts: {
          dev: 'nx run app:dev --outputStyle=stream',
        },
        nx: {
          name: 'app',
          targets: {
            dev: {
              executor: 'nx:run-commands',
              dependsOn: ['^build'],
              continuous: true,
              options: { command: 'bun run dev', cwd: '{projectRoot}' },
            },
          },
        },
      });
      await writeJson(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
      });

      const issues = checkPackageTargetPolicy(root);
      expect(issues.some((i) => i.message.includes('options.command must not call scripts.dev'))).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('moves env assignments into target options', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-pkg-target-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeJson(join(root, 'packages/web/package.json'), {
        name: '@scope/web',
        dependencies: { '@scope/lib': 'workspace:*' },
        scripts: {
          dev: "NODE_OPTIONS='--max-old-space-size=4096' astro dev",
        },
        nx: { name: 'web' },
      });
      await writeJson(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
      });

      expect(applyPackageTargetPolicy(root)).toBe(true);

      const web = JSON.parse(await readFile(join(root, 'packages/web/package.json'), 'utf8'));
      expect(web.nx.targets.dev.options.command).toBe('astro dev');
      expect(web.nx.targets.dev.options.env).toEqual({ NODE_OPTIONS: '--max-old-space-size=4096' });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('validates build.zig has steps', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-pkg-target-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      const pkgDir = join(root, 'packages/wasm');
      await mkdir(pkgDir, { recursive: true });
      await writeJson(join(pkgDir, 'package.json'), {
        name: '@scope/wasm',
        nx: { name: 'wasm' },
      });
      // build.zig without b.step(...)
      await writeFile(join(pkgDir, 'build.zig'), 'const std = @import("std");\n');

      const issues = checkPackageTargetPolicy(root);
      expect(issues.some((i) => i.message.includes('build.zig must define at least one b.step'))).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('requires test entrypoint when test files exist', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-pkg-target-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      const pkgDir = join(root, 'packages/lib');
      const srcDir = join(pkgDir, 'src');
      await mkdir(srcDir, { recursive: true });
      await writeJson(join(pkgDir, 'package.json'), {
        name: '@scope/lib',
        nx: { name: 'lib' },
      });
      // Create a test file without a scripts.test entry
      await writeFile(join(srcDir, 'example.test.ts'), 'import { test } from "bun:test";\n');

      const issues = checkPackageTargetPolicy(root);
      expect(issues.some((i) => i.message.includes('test files require scripts.test or nx.targets.test'))).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('accepts wildcard aggregate build dependencies', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-pkg-target-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeJson(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
        nx: {
          name: 'lib',
          targets: {
            build: {
              executor: 'nx:noop',
              dependsOn: ['^build', '*-js', '*-wasm'],
            },
          },
        },
      });

      const issues = checkPackageTargetPolicy(root);
      // No issue about missing targets for wildcard dependencies
      const buildIssues = issues.filter((i) => i.message.includes('dependsOn references missing target'));
      expect(buildIssues).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('per-package checkPackageTargets', () => {
  it('detects colon target names', () => {
    const pkg = {
      nx: {
        name: 'lib',
        targets: {
          'build:ts': { executor: 'nx:run-commands', options: { command: 'tsc' } },
        },
      },
    };
    const issues = checkPackageTargets(pkg, 'packages/lib');
    expect(issues.some((i) => i.message.includes('build:ts') && i.message.includes('colon'))).toBe(true);
  });
});

describe('per-package applyPackageTargets', () => {
  it('migrates colon targets and rewrites scripts', () => {
    const pkg: Record<string, unknown> = {
      name: '@scope/lib',
      dependencies: { '@scope/other': 'workspace:*' },
      scripts: {
        'build:ts': 'nx run lib:build:ts',
      },
      nx: {
        name: 'lib',
        targets: {
          'build:ts': {
            executor: 'nx:run-commands',
            options: { command: 'tsc --build tsconfig.lib.json', cwd: '{projectRoot}' },
          },
        },
      },
    };
    const workspaceNames = new Set(['@scope/lib', '@scope/other']);
    expect(applyPackageTargets(pkg, 'packages/lib', workspaceNames)).toBe(true);
    const targets = (pkg.nx as Record<string, unknown>).targets as Record<string, unknown>;
    expect(targets['build:ts']).toBeUndefined();
    expect(targets['tsc-js']).toBeDefined();
  });
});

describe('nxRunAlias', () => {
  it('generates streaming flags for continuous targets', () => {
    expect(nxRunAlias('app', 'dev', true)).toBe('nx run app:dev --outputStyle=stream');
  });

  it('uses dynamic legacy output for Astro and Vite dev targets', () => {
    expect(nxRunAlias('app', 'dev', true, 'astro dev')).toBe('nx run app:dev --outputStyle=dynamic-legacy');
    expect(nxRunAlias('app', 'serve', true, 'vite dev --host 0.0.0.0')).toBe(
      'nx run app:serve --outputStyle=dynamic-legacy',
    );
  });

  it('generates plain alias for non-continuous targets', () => {
    expect(nxRunAlias('app', 'build', false)).toBe('nx run app:build');
  });

  it('delegates test targets to boundedTestScriptAlias', () => {
    expect(nxRunAlias('app', 'test', false)).toBe('nx run app:test --outputStyle=stream');
  });
});

describe('packageNxProjectName', () => {
  it('returns nx.name when present', () => {
    expect(packageNxProjectName({ nx: { name: 'my-lib' } })).toBe('my-lib');
  });

  it('falls back to package name', () => {
    expect(packageNxProjectName({ name: '@scope/my-lib' })).toBe('@scope/my-lib');
  });

  it('returns null for anonymous packages', () => {
    expect(packageNxProjectName({})).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Tree-based tests
// ---------------------------------------------------------------------------

function addProject(tree: Tree, name: string, root: string, options: { keepProjectJson?: boolean } = {}): void {
  addProjectConfiguration(tree, name, {
    root,
    sourceRoot: `${root}/src`,
    projectType: 'library',
    targets: {},
  });
  if (!options.keepProjectJson && tree.exists(`${root}/project.json`)) {
    tree.delete(`${root}/project.json`);
  }
}

function writeJsonFile(tree: Tree, filePath: string, value: unknown): void {
  tree.write(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

describe('checkPackageTargetPolicyTree', () => {
  let tree: Tree;

  beforeEach(() => {
    tree = createTreeWithEmptyWorkspace();
  });

  it('detects colon-style Nx targets', () => {
    addProject(tree, 'app', 'packages/app');
    writeJsonFile(tree, 'packages/app/package.json', {
      name: '@scope/app',
      nx: {
        name: 'app',
        targets: {
          'build:ts': {
            executor: 'nx:run-commands',
            options: { command: 'tsc --build tsconfig.lib.json', cwd: '{projectRoot}' },
          },
          'lint:fix': {
            executor: 'nx:run-commands',
            options: { command: 'biome check --apply', cwd: '{projectRoot}' },
          },
        },
      },
    });

    const issues = checkPackageTargetPolicyTree(tree);
    expect(issues.length).toBeGreaterThanOrEqual(2);
    expect(issues.some((i) => i.message.includes('build:ts') && i.message.includes('colon target names'))).toBe(true);
    expect(issues.some((i) => i.message.includes('lint:fix') && i.message.includes('colon target names'))).toBe(true);
  });

  it('validates build.zig has steps', () => {
    addProject(tree, 'wasm', 'packages/wasm');
    writeJsonFile(tree, 'packages/wasm/package.json', {
      name: '@scope/wasm',
      nx: { name: 'wasm' },
    });
    tree.write('packages/wasm/build.zig', 'const std = @import("std");\n');

    const issues = checkPackageTargetPolicyTree(tree);
    expect(issues.some((i) => i.message.includes('build.zig must define at least one b.step'))).toBe(true);
  });

  it('passes build.zig with valid steps', () => {
    addProject(tree, 'wasm', 'packages/wasm');
    writeJsonFile(tree, 'packages/wasm/package.json', {
      name: '@scope/wasm',
      nx: { name: 'wasm' },
    });
    tree.write('packages/wasm/build.zig', 'const step = b.step("wasm", "Build wasm");\n');

    const issues = checkPackageTargetPolicyTree(tree);
    expect(issues.filter((i) => i.message.includes('build.zig'))).toEqual([]);
  });

  it('requires test entrypoint when test files exist', () => {
    addProject(tree, 'lib', 'packages/lib');
    writeJsonFile(tree, 'packages/lib/package.json', {
      name: '@scope/lib',
      nx: { name: 'lib' },
    });
    tree.write('packages/lib/src/example.test.ts', 'import { test } from "bun:test";\n');

    const issues = checkPackageTargetPolicyTree(tree);
    expect(issues.some((i) => i.message.includes('test files require scripts.test or nx.targets.test'))).toBe(true);
  });

  it('accepts packages with test entrypoint', () => {
    addProject(tree, 'lib', 'packages/lib');
    writeJsonFile(tree, 'packages/lib/package.json', {
      name: '@scope/lib',
      scripts: { test: 'bun test' },
      nx: { name: 'lib' },
    });
    tree.write('packages/lib/src/example.test.ts', 'import { test } from "bun:test";\n');

    const issues = checkPackageTargetPolicyTree(tree);
    expect(issues.filter((i) => i.message.includes('test files require'))).toEqual([]);
  });

  it('accepts wildcard aggregate build dependencies', () => {
    addProject(tree, 'lib', 'packages/lib');
    writeJsonFile(tree, 'packages/lib/package.json', {
      name: '@scope/lib',
      nx: {
        name: 'lib',
        targets: {
          build: {
            executor: 'nx:noop',
            dependsOn: ['^build', '*-js', '*-wasm'],
          },
        },
      },
    });

    const issues = checkPackageTargetPolicyTree(tree);
    const buildIssues = issues.filter((i) => i.message.includes('dependsOn references missing target'));
    expect(buildIssues).toEqual([]);
  });

  it('rejects recursive script commands', () => {
    addProject(tree, 'app', 'packages/app');
    addProject(tree, 'lib', 'packages/lib');
    writeJsonFile(tree, 'packages/app/package.json', {
      name: '@scope/app',
      dependencies: { '@scope/lib': 'workspace:*' },
      scripts: {
        dev: 'nx run app:dev --outputStyle=stream',
      },
      nx: {
        name: 'app',
        targets: {
          dev: {
            executor: 'nx:run-commands',
            dependsOn: ['^build'],
            continuous: true,
            options: { command: 'bun run dev', cwd: '{projectRoot}' },
          },
        },
      },
    });
    writeJsonFile(tree, 'packages/lib/package.json', {
      name: '@scope/lib',
      nx: { name: 'lib' },
    });

    const issues = checkPackageTargetPolicyTree(tree);
    expect(issues.some((i) => i.message.includes('options.command must not call scripts.dev'))).toBe(true);
  });
});

describe('applyPackageTargetPolicyTree', () => {
  let tree: Tree;

  beforeEach(() => {
    tree = createTreeWithEmptyWorkspace();
  });

  it('migrates colon build targets to tool-output names', () => {
    addProject(tree, 'lib', 'packages/lib');
    addProject(tree, 'other', 'packages/other');
    writeJsonFile(tree, 'packages/lib/package.json', {
      name: '@scope/lib',
      dependencies: { '@scope/other': 'workspace:*' },
      scripts: {
        'build:ts': 'nx run lib:build:ts',
      },
      nx: {
        name: 'lib',
        targets: {
          'build:ts': {
            executor: 'nx:run-commands',
            options: { command: 'tsc --build tsconfig.lib.json', cwd: '{projectRoot}' },
          },
          build: {
            executor: 'nx:noop',
            dependsOn: ['^build', 'build:ts'],
          },
        },
      },
    });
    writeJsonFile(tree, 'packages/other/package.json', {
      name: '@scope/other',
    });

    expect(applyPackageTargetPolicyTree(tree)).toBe(true);

    const lib = readJson(tree, 'packages/lib/package.json');
    const targets = lib.nx.targets;
    expect(targets['build:ts']).toBeUndefined();
    expect(targets['tsc-js']).toBeDefined();
    expect(targets['tsc-js'].options.command).toBe('tsc --build tsconfig.lib.json');
    expect(targets.build.dependsOn).toContain('tsc-js');
    expect(targets.build.dependsOn).not.toContain('build:ts');
    expect(lib.scripts['build:ts']).toBe('nx run lib:tsc-js');
  });

  it('removes noop aggregate build targets matching resolved plugin output', () => {
    addProject(tree, 'lib', 'packages/lib');
    writeJsonFile(tree, 'packages/lib/package.json', {
      name: '@scope/lib',
      nx: {
        name: 'lib',
        targets: {
          build: {
            executor: 'nx:noop',
            dependsOn: ['^build', '*-js'],
          },
        },
      },
    });

    const resolvedTargetsByProject = new Map<string, ResolvedProjectTargets>([
      [
        'lib',
        {
          targets: new Set(['build', 'tsc-js']),
          buildDependsOn: ['^build', '*-js'],
        },
      ],
    ]);

    expect(applyPackageTargetPolicyTree(tree, { resolvedTargetsByProject })).toBe(true);

    const lib = readJson(tree, 'packages/lib/package.json');
    expect(lib.nx.targets.build).toBeUndefined();
  });

  it('keeps non-noop build targets even when dependsOn matches', () => {
    addProject(tree, 'lib', 'packages/lib');
    writeJsonFile(tree, 'packages/lib/package.json', {
      name: '@scope/lib',
      nx: {
        name: 'lib',
        targets: {
          build: {
            executor: 'nx:run-commands',
            dependsOn: ['^build', '*-js'],
            options: { command: 'echo build', cwd: '{projectRoot}' },
          },
        },
      },
    });

    const resolvedTargetsByProject = new Map<string, ResolvedProjectTargets>([
      [
        'lib',
        {
          targets: new Set(['build', 'tsc-js']),
          buildDependsOn: ['^build', '*-js'],
        },
      ],
    ]);

    expect(applyPackageTargetPolicyTree(tree, { resolvedTargetsByProject })).toBe(false);

    const lib = readJson(tree, 'packages/lib/package.json');
    expect(lib.nx.targets.build).toBeDefined();
    expect(lib.nx.targets.build.options.command).toBe('echo build');
  });

  it('rewrites safe scripts into Nx aliases', () => {
    addProject(tree, 'web', 'packages/web');
    addProject(tree, 'lib', 'packages/lib');
    writeJsonFile(tree, 'packages/web/package.json', {
      name: '@scope/web',
      dependencies: { '@scope/lib': 'workspace:*' },
      scripts: {
        dev: 'astro dev',
        build: 'astro build',
      },
      nx: { name: 'web' },
    });
    writeJsonFile(tree, 'packages/lib/package.json', {
      name: '@scope/lib',
      nx: { name: 'lib' },
    });

    expect(applyPackageTargetPolicyTree(tree)).toBe(true);

    const web = readJson(tree, 'packages/web/package.json');
    expect(web.scripts.dev).toBe('nx run web:dev --outputStyle=dynamic-legacy');
    expect(web.scripts.build).toBe('nx run web:build');
    expect(web.nx.targets.dev.continuous).toBe(true);
    expect(web.nx.targets.dev.executor).toBe('nx:run-commands');
    expect(web.nx.targets.dev.options.command).toBe('astro dev');
    expect(web.nx.targets.build.executor).toBe('nx:run-commands');
    expect(web.nx.targets.build.options.command).toBe('astro build');
  });

  it('moves env assignments into target options', () => {
    addProject(tree, 'web', 'packages/web');
    addProject(tree, 'lib', 'packages/lib');
    writeJsonFile(tree, 'packages/web/package.json', {
      name: '@scope/web',
      dependencies: { '@scope/lib': 'workspace:*' },
      scripts: {
        dev: "NODE_OPTIONS='--max-old-space-size=4096' astro dev",
      },
      nx: { name: 'web' },
    });
    writeJsonFile(tree, 'packages/lib/package.json', {
      name: '@scope/lib',
      nx: { name: 'lib' },
    });

    expect(applyPackageTargetPolicyTree(tree)).toBe(true);

    const web = readJson(tree, 'packages/web/package.json');
    expect(web.nx.targets.dev.options.command).toBe('astro dev');
    expect(web.nx.targets.dev.options.env).toEqual({ NODE_OPTIONS: '--max-old-space-size=4096' });
  });

  it('returns false when no changes needed', () => {
    addProject(tree, 'lib', 'packages/lib');
    writeJsonFile(tree, 'packages/lib/package.json', {
      name: '@scope/lib',
      nx: { name: 'lib' },
    });

    expect(applyPackageTargetPolicyTree(tree)).toBe(false);
  });

  it('skips projects without package.json', () => {
    addProject(tree, 'native', 'packages/native', { keepProjectJson: true });
    // No package.json written — should not throw
    expect(applyPackageTargetPolicyTree(tree)).toBe(false);
  });
});

async function writeJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
