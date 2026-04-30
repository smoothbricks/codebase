import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  applyPackageTargetPolicy,
  applyPackageTargets,
  checkPackageTargetPolicy,
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
      // dev script should become an Nx alias with streaming flags
      expect(web.scripts.dev).toBe('nx run web:dev --tui=false --outputStyle=stream');
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
          dev: 'nx run app:dev --tui=false --outputStyle=stream',
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
    expect(nxRunAlias('app', 'dev', true)).toBe('nx run app:dev --tui=false --outputStyle=stream');
  });

  it('generates plain alias for non-continuous targets', () => {
    expect(nxRunAlias('app', 'build', false)).toBe('nx run app:build');
  });

  it('delegates test targets to boundedTestScriptAlias', () => {
    expect(nxRunAlias('app', 'test', false)).toBe('nx run app:test --tui=false --outputStyle=stream');
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

async function writeJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
