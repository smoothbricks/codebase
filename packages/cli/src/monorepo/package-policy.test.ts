import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import {
  applyNxProjectNameDefaults,
  applyWorkspaceDependencyDefaults,
  listValidCommitScopes,
  validateNxProjectNames,
  validateWorkspaceDependencies,
} from './package-policy.js';

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
      expect(validateWorkspaceDependencies(root)).toBe(5);

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

async function writeJson(path: string, value: Record<string, unknown>): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
