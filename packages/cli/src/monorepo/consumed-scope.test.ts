import { describe, expect, it, spyOn } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { validateNoMachineLocalSpecifiers } from './consumed-scope.js';

describe('machine-local dependency specifiers', () => {
  it('rejects link:, file: and portal: specifiers in any dependency field', async () => {
    const root = await createWorkspace({
      rootName: '@smoothbricks/codebase',
      packages: [
        {
          dir: 'app',
          name: '@smoothbricks/app',
          nx: { name: 'app' },
          dependencies: { '@scope/linked': 'link:@scope/linked', '@scope/tar': 'file:../tar.tgz' },
          devDependencies: { '@scope/portal': 'portal:../portal' },
        },
      ],
    });
    try {
      const errors = captureConsoleErrors();
      expect(validateNoMachineLocalSpecifiers(root)).toBe(3);
      expect(errors.join('\n')).toContain(
        'dependencies.@scope/linked is "link:@scope/linked", a machine-local specifier',
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

it('rejects a link: override at the workspace root', async () => {
  const root = await createWorkspace({
    rootName: '@smoothbricks/codebase',
    packages: [{ dir: 'app', name: '@smoothbricks/app', nx: { name: 'app' } }],
    rootExtra: { overrides: { '@scope/linked': 'link:@scope/linked' } },
  });
  try {
    const errors = captureConsoleErrors();
    expect(validateNoMachineLocalSpecifiers(root)).toBe(1);
    expect(errors.join('\n')).toContain('.: overrides.@scope/linked is "link:@scope/linked"');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

function captureConsoleErrors(): string[] {
  const errors: string[] = [];
  spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    errors.push(args.join(' '));
  });
  return errors;
}

async function createWorkspace(input: {
  rootName: string;
  packages: Array<{
    dir: string;
    name: string;
    dependencies?: Record<string, string>;
    devDependencies?: Record<string, string>;
    nx?: Record<string, unknown>;
  }>;
  rootExtra?: Record<string, unknown>;
}): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-consumed-scope-'));
  await writeJson(join(root, 'package.json'), {
    name: input.rootName,
    version: '0.0.0',
    private: true,
    workspaces: ['packages/*'],
    ...input.rootExtra,
  });
  for (const pkg of input.packages) {
    await writeJson(join(root, `packages/${pkg.dir}/package.json`), {
      name: pkg.name,
      version: '0.0.0',
      ...(pkg.dependencies ? { dependencies: pkg.dependencies } : {}),
      ...(pkg.devDependencies ? { devDependencies: pkg.devDependencies } : {}),
      ...(pkg.nx ? { nx: pkg.nx } : {}),
    });
  }
  return root;
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
