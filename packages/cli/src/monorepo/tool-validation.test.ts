import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { applyToolConfigDefaults, applyToolingPackageDefaults, validateToolConfig } from './tool-validation.js';

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
      expect(rootPackage.devDependencies.nx).toBe('22.5.4');
      expect(rootPackage.workspaces).toContain('tooling');
      expect(toolingPackage.name).toBe('@smoothbricks/tooling');
      expect(toolingPackage.dependencies['@smoothbricks/cli']).toBe(await currentCliRange());
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
        name: '@smoothbricks/codebase',
        version: '0.0.0',
        private: true,
        workspaces: ['packages/*', 'tooling'],
        devDependencies: {
          '@biomejs/biome': '^3.0.0',
          '@nx/js': '22.6.0',
          '@types/bun': '1.3.99',
          eslint: '^10.0.0',
          'eslint-stdout': 'workspace:*',
          nx: '23.0.0',
          prettier: '^3.7.0',
          typescript: '^6.0.0',
        },
      });
      await writeJson(join(root, 'tooling/package.json'), {
        name: '@smoothbricks/tooling',
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

  it('uses workspace cli only when @smoothbricks/cli is a real workspace package', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-tool-validation-'));
    try {
      await writeJson(join(root, 'package.json'), {
        name: '@smoothbricks/codebase',
        version: '0.0.0',
        private: true,
        workspaces: ['packages/*', 'tooling'],
      });
      await writeJson(join(root, 'packages/cli/package.json'), {
        name: '@smoothbricks/cli',
        version: '0.1.1',
      });

      applyToolingPackageDefaults(root);

      const toolingPackage = JSON.parse(await readFile(join(root, 'tooling/package.json'), 'utf8'));
      expect(toolingPackage.name).toBe('@smoothbricks/tooling');
      expect(toolingPackage.dependencies['@smoothbricks/cli']).toBe('workspace:*');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

async function currentCliRange(): Promise<string> {
  const pkg = JSON.parse(await readFile(new URL('../../package.json', import.meta.url), 'utf8'));
  return `^${pkg.version}`;
}

async function writeJson(path: string, value: Record<string, unknown>): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
