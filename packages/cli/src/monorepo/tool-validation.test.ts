import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import {
  applyRootDevDependencyDefaults,
  applyToolConfigDefaults,
  applyToolingPackageDefaults,
  readToolContext,
  validateToolConfig,
} from './tool-validation.js';

describe('tool configuration validation', () => {
  it('fixes root, tooling, and devenv tool declarations', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-tool-validation-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@smoothbricks/codebase',
        version: '0.0.0',
        private: true,
        devDependencies: {
          '@smoothbricks/cli': 'workspace:*',
        },
      });
      const devenvPath = join(root, 'tooling/direnv/devenv.nix');
      await mkdir(dirname(devenvPath), { recursive: true });
      await writeFile(
        devenvPath,
        `{
  pkgs,
  ...
}: {
  packages = with pkgs; [
  ];
}
`,
      );

      expect(validateToolConfig(root)).toBeGreaterThan(0);
      await applyToolConfigDefaults(root);
      expect(validateToolConfig(root)).toBe(0);

      const rootPackage = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
      const toolingPackage = JSON.parse(await readFile(join(root, 'tooling/package.json'), 'utf8'));
      const devenv = await readFile(join(root, 'tooling/direnv/devenv.nix'), 'utf8');
      expect(rootPackage.devDependencies['@smoothbricks/cli']).toBeUndefined();
      expect(rootPackage.devDependencies['@smoothbricks/nx-plugin']).toBe('workspace:*');
      expect(rootPackage.devDependencies['eslint-stdout']).toBe('workspace:*');
      expect(rootPackage.devDependencies.nx).toBe('22.5.4');
      expect(rootPackage.workspaces).toContain('tooling');
      expect(toolingPackage.name).toBe('@smoothbricks/tooling');
      expect(toolingPackage.dependencies['@smoothbricks/cli']).toBe('workspace:*');
      expect(devenv).toContain('nodejs_latest');
      expect(devenv).toContain('coreutils');
      expect(devenv).toContain('git-format-staged');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('accepts newer dependency versions above the policy floor', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-tool-validation-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@fixture/app',
        version: '0.0.0',
        private: true,
        workspaces: ['packages/*', 'tooling'],
        devDependencies: {
          '@biomejs/biome': '^3.0.0',
          '@nx/js': '22.6.0',
          '@smoothbricks/nx-plugin': '^0.1.0',
          '@types/bun': '1.3.99',
          eslint: '^10.0.0',
          'eslint-stdout': await currentEslintStdoutRange(),
          nx: '23.0.0',
          prettier: '^3.7.0',
          typescript: '^6.0.0',
        },
      });
      await writeJson(join(root, 'tooling/package.json'), {
        name: '@fixture/tooling',
        private: true,
        dependencies: { '@smoothbricks/cli': await currentCliRange() },
      });
      const devenvPath = join(root, 'tooling/direnv/devenv.nix');
      await mkdir(dirname(devenvPath), { recursive: true });
      await writeFile(
        devenvPath,
        `{
  pkgs,
  ...
}: {
  packages = with pkgs; [
    nodejs_latest
    bun
    git
    git-format-staged
    jq
    alejandra
    coreutils
    gnutar
  ];
}
`,
      );

      expect(validateToolConfig(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('uses workspace smoo in the SmoothBricks codebase repo', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-tool-validation-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@smoothbricks/codebase',
        version: '0.0.0',
        private: true,
        workspaces: ['packages/*', 'tooling'],
      });
      applyToolingPackageDefaults(root, readToolContext(root).policy);

      const toolingPackage = JSON.parse(await readFile(join(root, 'tooling/package.json'), 'utf8'));
      expect(toolingPackage.name).toBe('@smoothbricks/tooling');
      expect(toolingPackage.dependencies['@smoothbricks/cli']).toBe('workspace:*');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('uses published smoo and eslint formatter ranges in user repos', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-tool-validation-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@fixture/app',
        version: '0.0.0',
        private: true,
        workspaces: ['packages/*', 'tooling'],
        devDependencies: {
          '@biomejs/biome': '^3.0.0',
          '@nx/js': '22.6.0',
          eslint: '^10.0.0',
          nx: '23.0.0',
          prettier: '^3.7.0',
          typescript: '^6.0.0',
        },
      });

      const context = readToolContext(root);
      await applyRootDevDependencyDefaults(root, context);
      applyToolingPackageDefaults(root, context.policy);

      const rootPackage = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
      const toolingPackage = JSON.parse(await readFile(join(root, 'tooling/package.json'), 'utf8'));
      expect(rootPackage.devDependencies['eslint-stdout']).toBe(await currentEslintStdoutRange());
      expect(rootPackage.devDependencies['@smoothbricks/nx-plugin']).toBe(await currentNxPluginRange());
      expect(toolingPackage.name).toBe('@fixture/tooling');
      expect(toolingPackage.dependencies['@smoothbricks/cli']).toBe(await currentCliRange());
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

async function currentCliRange(): Promise<string> {
  const pkg = JSON.parse(await readFile(new URL('../../package.json', import.meta.url), 'utf8'));
  return `^${pkg.version}`;
}

async function currentEslintStdoutRange(): Promise<string> {
  const pkg = JSON.parse(await readFile(new URL('../../../eslint-stdout/package.json', import.meta.url), 'utf8'));
  return `^${pkg.version}`;
}

async function currentNxPluginRange(): Promise<string> {
  const pkg = JSON.parse(await readFile(new URL('../../../nx-plugin/package.json', import.meta.url), 'utf8'));
  return `^${pkg.version}`;
}

async function writeJson(path: string, value: Record<string, unknown>): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
