import { describe, expect, it, spyOn } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { scaffold } from './scaffold.js';

describe('scaffold', () => {
  it('writes the manifest-driven starter script and wires the prepare-env nx target', async () => {
    const root = await createWorkspace([{ dir: 'worker', name: '@acme/worker', toml: '[env.production]\n' }]);
    captureConsoleLogs();
    try {
      const scriptPath = scaffold(root, '@acme/worker');

      // Returns the absolute path it wrote.
      expect(scriptPath).toBe(join(root, 'packages/worker/scripts/prepare-env.ts'));

      // The generated script imports the primitives module and derives work from the manifest.
      const script = await readFile(scriptPath, 'utf8');
      expect(script).toContain('@smoothbricks/cli/wrangler/prepare-env');
      expect(script).toContain('readManifest');

      // The nx target is wired to run the script from the project root.
      const pkg = await readJson(join(root, 'packages/worker/package.json'));
      expect(nxTargets(pkg)['prepare-env']).toEqual({
        executor: 'nx:run-commands',
        options: { command: 'bun scripts/prepare-env.ts', cwd: '{projectRoot}' },
      });
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });

  it('resolves the same project by nx name, package name, and relative path', async () => {
    const root = await createWorkspace([
      { dir: 'worker', name: '@acme/worker', toml: '[env.production]\n', nx: { name: 'my-worker' } },
    ]);
    captureConsoleLogs();
    try {
      const expected = join(root, 'packages/worker/scripts/prepare-env.ts');
      const byNxName = scaffold(root, 'my-worker');
      const byPackageName = scaffold(root, '@acme/worker', { force: true });
      const byRelativePath = scaffold(root, 'packages/worker', { force: true });

      expect(byNxName).toBe(expected);
      expect(byPackageName).toBe(expected);
      expect(byRelativePath).toBe(expected);
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });

  it('throws when the project is unknown, naming the known projects', async () => {
    const root = await createWorkspace([
      { dir: 'worker', name: '@acme/worker', toml: '[env.production]\n', nx: { name: 'my-worker' } },
    ]);
    try {
      let message = '';
      try {
        scaffold(root, 'ghost');
      } catch (error) {
        message = error instanceof Error ? error.message : String(error);
      }
      expect(message).toContain('not found');
      expect(message).toContain('my-worker');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('throws when the resolved package has no wrangler.toml', async () => {
    const root = await createWorkspace([{ dir: 'plain', name: '@acme/plain' }]);
    try {
      expect(() => scaffold(root, '@acme/plain')).toThrow('has no wrangler.toml');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('throws on a second scaffold without force, pointing at --force', async () => {
    const root = await createWorkspace([{ dir: 'worker', name: '@acme/worker', toml: '[env.production]\n' }]);
    captureConsoleLogs();
    try {
      scaffold(root, '@acme/worker');
      expect(() => scaffold(root, '@acme/worker')).toThrow('--force');
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });

  it('overwrites an existing script when force is set', async () => {
    const root = await createWorkspace([{ dir: 'worker', name: '@acme/worker', toml: '[env.production]\n' }]);
    const logs = captureConsoleLogs();
    try {
      const scriptPath = scaffold(root, '@acme/worker');
      await writeFile(scriptPath, 'stale hand-edited content\n');

      const again = scaffold(root, '@acme/worker', { force: true });
      expect(again).toBe(scriptPath);

      // The stale content is replaced by a freshly rendered script.
      const script = await readFile(scriptPath, 'utf8');
      expect(script).not.toContain('stale hand-edited content');
      expect(script).toContain('@smoothbricks/cli/wrangler/prepare-env');
      expect(logs.some((line) => line.startsWith('overwrote'))).toBe(true);
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });

  it('preserves a hand-customized prepare-env target and never duplicates it', async () => {
    const custom = {
      executor: 'nx:run-commands',
      options: { command: 'bun scripts/prepare-env.ts --custom', cwd: '{projectRoot}' },
    };
    const root = await createWorkspace([
      {
        dir: 'worker',
        name: '@acme/worker',
        toml: '[env.production]\n',
        nx: { targets: { 'prepare-env': { ...custom } } },
      },
    ]);
    captureConsoleLogs();
    try {
      scaffold(root, '@acme/worker', { force: true });

      // The idempotent guard leaves an already-wired target exactly as authored.
      const target = nxTargets(await readJson(join(root, 'packages/worker/package.json')))['prepare-env'];
      expect(target).toEqual(custom);
      // Still a single target object, not wrapped into an array or duplicated.
      expect(Array.isArray(target)).toBe(false);
    } finally {
      console.log = originalConsoleLog;
      await rm(root, { recursive: true, force: true });
    }
  });
});

async function createWorkspace(
  packages: Array<{ dir: string; name: string; toml?: string; nx?: Record<string, unknown> }>,
): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-scaffold-'));
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
    if (pkg.toml !== undefined) {
      await writeFile(join(root, `packages/${pkg.dir}/wrangler.toml`), pkg.toml);
    }
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
