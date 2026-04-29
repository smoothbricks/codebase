import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { applyNxProjectNameDefaults, listValidCommitScopes, validateNxProjectNames } from './package-policy.js';

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

async function createWorkspace(input: {
  rootName: string;
  packages: Array<{ dir: string; name: string; nx?: Record<string, unknown> }>;
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
      ...(pkg.nx ? { nx: pkg.nx } : {}),
    });
  }
  return root;
}

async function writeJson(path: string, value: Record<string, unknown>): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
