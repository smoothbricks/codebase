import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
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

const registryVersions: Record<string, string[]> = {
  '@biomejs/biome': ['2.3.5'],
  '@nx/js': ['23.1.0'],
  '@smoothbricks/cli': ['0.10.3', '0.10.4'],
  '@smoothbricks/nx-plugin': ['0.3.0'],
  eslint: ['9.39.1'],
  'eslint-stdout': ['1.1.1', '1.1.2'],
  nx: ['23.1.0'],
  prettier: ['3.6.1'],
  ttsc: ['0.18.4'],
  typescript: ['6.0.3'],
  // alias package is not fetched from registry under this name when using fallback
  '@typescript/native': ['7.0.2'],
};

const registryLatestTags: Record<string, string> = {
  '@smoothbricks/cli': '0.10.4',
};

const realFetch = globalThis.fetch;

describe('tool configuration validation', () => {
  beforeEach(() => {
    globalThis.fetch = mockRegistryFetch;
  });

  afterEach(() => {
    globalThis.fetch = realFetch;
  });

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

      expect(await validateToolConfig(root)).toBeGreaterThan(0);
      await applyToolConfigDefaults(root);
      expect(await validateToolConfig(root)).toBe(0);

      const rootPackage = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
      const toolingPackage = JSON.parse(await readFile(join(root, 'tooling/package.json'), 'utf8'));
      const devenv = await readFile(join(root, 'tooling/direnv/devenv.nix'), 'utf8');
      expect(rootPackage.devDependencies['@smoothbricks/cli']).toBeUndefined();
      expect(rootPackage.devDependencies['@smoothbricks/nx-plugin']).toBe('workspace:*');
      expect(rootPackage.devDependencies['eslint-stdout']).toBe('workspace:*');
      expect(rootPackage.devDependencies.nx).toBe('23.1.0');
      expect(rootPackage.devDependencies.typescript).toBe('^6.0.3');
      expect(rootPackage.devDependencies['@typescript/native']).toBe('npm:typescript@^7.0.2');
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
          '@nx/js': '23.2.0',
          '@smoothbricks/nx-plugin': '^0.4.0',
          '@types/bun': '1.3.99',
          eslint: '^10.0.0',
          'eslint-stdout': await currentEslintStdoutRange(),
          nx: '24.0.0',
          prettier: '^3.7.0',
          ttsc: '^0.19.0',
          typescript: '^6.0.0',
          '@typescript/native': 'npm:typescript@^7.0.2',
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

      expect(await validateToolConfig(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('bumps consumer CLI pin to npm latest even when the running CLI is a linked prerelease', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-tool-validation-'));
    try {
      const configuredRange = '^0.10.1';
      const expectedRange = `^${registryLatestTags['@smoothbricks/cli']}`;
      await writeJson(join(root, 'package.json'), {
        name: '@fixture/app',
        version: '0.0.0',
        private: true,
        workspaces: ['packages/*', 'tooling'],
      });
      await writeJson(join(root, 'tooling/package.json'), {
        name: '@fixture/tooling',
        private: true,
        dependencies: { '@smoothbricks/cli': configuredRange },
      });

      const context = await readToolContext(root);
      expect(context.policy.cliDependencyRange).toBe(expectedRange);
      applyToolingPackageDefaults(root, context.policy);

      const toolingPackage = JSON.parse(await readFile(join(root, 'tooling/package.json'), 'utf8'));
      expect(toolingPackage.dependencies['@smoothbricks/cli']).toBe(expectedRange);
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
      applyToolingPackageDefaults(root, (await readToolContext(root)).policy);

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
          '@nx/js': '23.2.0',
          eslint: '^10.0.0',
          nx: '24.0.0',
          prettier: '^3.7.0',
          ttsc: '^0.19.0',
          typescript: '^6.0.0',
        },
      });

      const context = await readToolContext(root);
      await applyRootDevDependencyDefaults(root, context);
      applyToolingPackageDefaults(root, context.policy);

      const rootPackage = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
      const toolingPackage = JSON.parse(await readFile(join(root, 'tooling/package.json'), 'utf8'));
      expect(rootPackage.devDependencies['eslint-stdout']).toBe(await currentEslintStdoutRange());
      expect(rootPackage.devDependencies['@smoothbricks/nx-plugin']).toBe(await currentNxPluginRange());
      expect(rootPackage.devDependencies['@typescript/native']).toBe('npm:typescript@^7.0.2');
      expect(toolingPackage.name).toBe('@fixture/tooling');
      expect(toolingPackage.dependencies['@smoothbricks/cli']).toBe(await currentCliRange());
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

async function currentCliRange(): Promise<string> {
  return `^${registryLatestTags['@smoothbricks/cli']}`;
}

async function currentEslintStdoutRange(): Promise<string> {
  const pkg = JSON.parse(await readFile(new URL('../../../eslint-stdout/package.json', import.meta.url), 'utf8'));
  return `^${latestPatchVersion('eslint-stdout', pkg.version) ?? pkg.version}`;
}

const mockRegistryFetch = Object.assign(
  (input: string | URL | Request): Promise<Response> => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url;
    const packageName = decodeURIComponent(new URL(url).pathname.slice(1));
    const versions = registryVersions[packageName];
    if (!versions) {
      return Promise.resolve(new Response('{}', { status: 404 }));
    }
    const body: Record<string, unknown> = {
      versions: Object.fromEntries(versions.map((version) => [version, {}])),
    };
    const latest = registryLatestTags[packageName];
    if (latest) {
      body['dist-tags'] = { latest };
    }
    return Promise.resolve(
      new Response(JSON.stringify(body), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    );
  },
  { preconnect: realFetch.preconnect },
) satisfies typeof fetch;

function latestPatchVersion(packageName: string, version: string): string | null {
  const [major, minor] = version.split('.');
  if (!major || !minor) {
    return null;
  }
  return (
    (registryVersions[packageName] ?? [])
      .filter((candidate) => candidate.startsWith(`${major}.${minor}.`))
      .sort((left, right) => compareDotVersions(right, left))[0] ?? null
  );
}

function compareDotVersions(left: string, right: string): number {
  const leftParts = left.split('.').map(Number);
  const rightParts = right.split('.').map(Number);
  for (let index = 0; index < Math.max(leftParts.length, rightParts.length); index += 1) {
    const diff = (leftParts[index] ?? 0) - (rightParts[index] ?? 0);
    if (diff !== 0) {
      return diff;
    }
  }
  return 0;
}

async function currentNxPluginRange(): Promise<string> {
  return `^${latestRegistryVersion('@smoothbricks/nx-plugin')}`;
}

function latestRegistryVersion(packageName: string): string {
  const versions = registryVersions[packageName];
  if (!versions) {
    throw new Error(`missing mocked registry versions for ${packageName}`);
  }
  let latest = versions[0];
  for (const version of versions) {
    if (!latest || compareDotVersions(version, latest) > 0) {
      latest = version;
    }
  }
  if (!latest) {
    throw new Error(`empty mocked registry versions for ${packageName}`);
  }
  return latest;
}

async function writeJson(path: string, value: Record<string, unknown>): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
