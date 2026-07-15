import { describe, expect, it, spyOn } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { applyWranglerDefaults, firstWranglerEnv, validateWrangler } from './wrangler.js';

describe('firstWranglerEnv', () => {
  it('extracts the first [env.<name>] header', () => {
    expect(firstWranglerEnv('name = "svc"\n\n[env.staging]\nvars = {}\n\n[env.production]\n')).toBe('staging');
  });

  it('handles nested env sub-tables', () => {
    expect(firstWranglerEnv('[env.production.vars]\nFOO = "bar"\n')).toBe('production');
  });

  it('returns null when the toml declares no env block', () => {
    expect(firstWranglerEnv('name = "svc"\nmain = "src/index.ts"\n')).toBeNull();
  });
});

describe('applyWranglerDefaults', () => {
  it('injects the wrangler-types target and wires typecheck, idempotently', async () => {
    const root = await createWorkspace([
      {
        dir: 'api',
        name: '@acme/api',
        toml: '[env.production]\n',
        nx: { targets: { typecheck: { dependsOn: ['^build'] } } },
      },
    ]);
    try {
      applyWranglerDefaults(root);
      const pkg = await readJson(join(root, 'packages/api/package.json'));
      expect(nxTargets(pkg)['wrangler-types']).toEqual({
        executor: 'nx:run-commands',
        cache: true,
        inputs: ['{projectRoot}/wrangler.toml', '{projectRoot}/.dev.vars.example'],
        outputs: ['{projectRoot}/worker-configuration.d.ts'],
        options: {
          command: 'wrangler types --env production --env-file .dev.vars.example --include-runtime false',
          cwd: '{projectRoot}',
        },
      });
      const typecheck = nxTargets(pkg).typecheck;
      expect(isRecord(typecheck) ? typecheck.dependsOn : null).toEqual(['^build', 'wrangler-types']);

      const logs = captureConsoleLogs();
      applyWranglerDefaults(root);
      expect(logs.some((line) => line.startsWith('updated'))).toBe(false);
      expect(logs.some((line) => line.startsWith('unchanged'))).toBe(true);
      expect(await readJson(join(root, 'packages/api/package.json'))).toEqual(pkg);
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });

  it('omits --env from the command when the wrangler.toml declares no env block', async () => {
    const root = await createWorkspace([
      { dir: 'api', name: '@acme/api', toml: 'name = "svc"\nmain = "src/index.ts"\n' },
    ]);
    captureConsoleLogs();
    try {
      applyWranglerDefaults(root);
      const target = nxTargets(await readJson(join(root, 'packages/api/package.json')))['wrangler-types'];
      const options = isRecord(target) ? target.options : null;
      expect(isRecord(options) ? options.command : null).toBe(
        'wrangler types --env-file .dev.vars.example --include-runtime false',
      );
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });

  it('creates an empty .dev.vars.example and logs it when the project has none', async () => {
    const root = await createWorkspace([{ dir: 'api', name: '@acme/api', toml: 'name = "svc"\n' }]);
    const logs = captureConsoleLogs();
    try {
      applyWranglerDefaults(root);
      expect(await readFile(join(root, 'packages/api/.dev.vars.example'), 'utf8')).toBe('');
      expect(logs.some((line) => line.startsWith('created') && line.includes('api/.dev.vars.example'))).toBe(true);
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });

  it('preserves a human-authored .dev.vars.example and does not log a creation', async () => {
    const root = await createWorkspace([{ dir: 'api', name: '@acme/api', toml: 'name = "svc"\n' }]);
    const examplePath = join(root, 'packages/api/.dev.vars.example');
    await writeFile(examplePath, 'GITHUB_CLIENT_SECRET=\n');
    const logs = captureConsoleLogs();
    try {
      applyWranglerDefaults(root);
      expect(await readFile(examplePath, 'utf8')).toBe('GITHUB_CLIENT_SECRET=\n');
      expect(logs.some((line) => line.startsWith('created') && line.includes('.dev.vars.example'))).toBe(false);
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });

  it('adds the missing entries to an incomplete root .gitignore', async () => {
    const root = await createWorkspace([{ dir: 'api', name: '@acme/api', toml: 'name = "svc"\n' }]);
    const logs = captureConsoleLogs();
    try {
      await writeFile(join(root, '.gitignore'), 'node_modules\n');
      applyWranglerDefaults(root);
      const lines = (await readFile(join(root, '.gitignore'), 'utf8')).split('\n').map((line) => line.trim());
      expect(lines).toContain('.dev.vars');
      expect(lines).toContain('worker-configuration.d.ts');
      expect(logs.some((line) => line.startsWith('updated') && line.includes('.gitignore'))).toBe(true);
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });

  it('leaves the root .gitignore untouched when both entries are already present', async () => {
    const root = await createWorkspace([{ dir: 'api', name: '@acme/api', toml: 'name = "svc"\n' }]);
    const gitignorePath = join(root, '.gitignore');
    const logs = captureConsoleLogs();
    try {
      await writeFile(gitignorePath, 'node_modules\n.dev.vars\nworker-configuration.d.ts\n');
      const before = await readFile(gitignorePath, 'utf8');
      applyWranglerDefaults(root);
      expect(await readFile(gitignorePath, 'utf8')).toBe(before);
      expect(logs.some((line) => line.startsWith('updated') && line.includes('.gitignore'))).toBe(false);
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('validateWrangler', () => {
  it('flags a project whose .dev.vars.example is missing while target and .gitignore are wired', async () => {
    const root = await createWorkspace([
      {
        dir: 'api',
        name: '@acme/api',
        toml: '[env.staging]\n',
        nx: { targets: { 'wrangler-types': { executor: 'nx:run-commands' } } },
      },
    ]);
    try {
      await writeFile(join(root, '.gitignore'), '.dev.vars\nworker-configuration.d.ts\n');
      expect(validateWrangler(root)).toBe(1);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('passes a fully wired project', async () => {
    const root = await createWorkspace([{ dir: 'api', name: '@acme/api', toml: '[env.staging]\n' }]);
    try {
      applyWranglerDefaults(root);
      await writeFile(join(root, 'packages/api/.dev.vars.example'), 'SECRET=\n');
      await writeFile(join(root, '.gitignore'), '.dev.vars\n!.dev.vars.example\nworker-configuration.d.ts\n');
      expect(validateWrangler(root)).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

async function createWorkspace(
  packages: Array<{ dir: string; name: string; toml: string; nx?: Record<string, unknown> }>,
): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-wrangler-'));
  await writeJson(join(root, 'package.json'), {
    name: '@acme/codebase',
    version: '0.0.0',
    private: true,
    workspaces: ['packages/*'],
  });
  for (const pkg of packages) {
    await writeJson(join(root, `packages/${pkg.dir}/package.json`), {
      name: pkg.name,
      version: '0.0.0',
      private: true,
      ...(pkg.nx ? { nx: pkg.nx } : {}),
    });
    await writeFile(join(root, `packages/${pkg.dir}/wrangler.toml`), pkg.toml);
  }
  return root;
}

function nxTargets(pkg: Record<string, unknown>): Record<string, unknown> {
  const nx = pkg.nx;
  if (!isRecord(nx) || !isRecord(nx.targets)) {
    throw new Error('nx.targets not found');
  }
  return nx.targets;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

async function readJson(path: string): Promise<Record<string, unknown>> {
  const parsed: unknown = JSON.parse(await readFile(path, 'utf8'));
  if (!isRecord(parsed)) {
    throw new Error('expected JSON object');
  }
  return parsed;
}

async function writeJson(path: string, value: Record<string, unknown>): Promise<void> {
  await mkdir(join(path, '..'), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

const originalConsoleLog = console.log;

function captureConsoleLogs(): string[] {
  const logs: string[] = [];
  spyOn(console, 'log').mockImplementation((...args: unknown[]) => {
    logs.push(args.join(' '));
  });
  return logs;
}
