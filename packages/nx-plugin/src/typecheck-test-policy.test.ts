import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { applyTypecheckTestPolicy, checkTypecheckTestPolicy } from './typecheck-test-policy.js';

async function writeJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

async function readJson(path: string): Promise<unknown> {
  return JSON.parse(await readFile(path, 'utf8'));
}

describe('typecheck test policy', () => {
  it('creates tsconfig.test.json for bun test package', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJson(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
      });

      // check should report missing tsconfig.test.json
      const issues = checkTypecheckTestPolicy(root);
      expect(issues.length).toBe(1);
      expect(issues[0]!.message).toContain('bun test');
      expect(issues[0]!.message).toContain('tsconfig.test.json');

      // apply should create it
      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = (await readJson(join(root, 'packages/app/tsconfig.test.json'))) as Record<string, unknown>;
      const compilerOptions = tsconfig.compilerOptions as Record<string, unknown>;
      expect(compilerOptions.noEmit).toBe(true);
      expect(compilerOptions.composite).toBe(false);
      expect(compilerOptions.declaration).toBe(false);
      expect(compilerOptions.declarationMap).toBe(false);
      expect(compilerOptions.emitDeclarationOnly).toBe(false);
      expect(compilerOptions.types).toContain('bun');
      expect(tsconfig.extends).toBe('../../tsconfig.base.json');

      const include = tsconfig.include as string[];
      expect(include).toContain('src/**/*.test.ts');
      expect(include).toContain('src/**/*.spec.ts');
      expect(include).toContain('src/**/__tests__/**/*.ts');
      expect(include).toContain('src/test-suite-tracer.ts');

      // second apply should be idempotent
      expect(applyTypecheckTestPolicy(root)).toBe(false);

      // check should now pass
      expect(checkTypecheckTestPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('creates tsconfig.test.json for vitest package', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJson(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/web/package.json'), {
        name: '@scope/web',
        scripts: { test: 'vitest run' },
      });

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = (await readJson(join(root, 'packages/web/tsconfig.test.json'))) as Record<string, unknown>;
      const compilerOptions = tsconfig.compilerOptions as Record<string, unknown>;
      expect(compilerOptions.noEmit).toBe(true);
      // vitest should NOT add bun types
      expect(compilerOptions.types).toBeUndefined();

      expect(checkTypecheckTestPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('detects bun test in Nx targets', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJson(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
        nx: {
          targets: {
            test: {
              options: { command: 'bun test --coverage' },
            },
          },
        },
      });

      const issues = checkTypecheckTestPolicy(root);
      expect(issues.length).toBe(1);
      expect(issues[0]!.message).toContain('bun test');

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = (await readJson(join(root, 'packages/lib/tsconfig.test.json'))) as Record<string, unknown>;
      const compilerOptions = tsconfig.compilerOptions as Record<string, unknown>;
      expect(compilerOptions.noEmit).toBe(true);
      expect(compilerOptions.types).toContain('bun');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects composite/declaration/dist-test in tsconfig.test.json', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJson(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/bad/package.json'), {
        name: '@scope/bad',
        scripts: { test: 'bun test' },
      });
      await writeJson(join(root, 'packages/bad/tsconfig.test.json'), {
        compilerOptions: {
          composite: true,
          declaration: true,
          declarationMap: true,
          outDir: 'dist-test',
          tsBuildInfoFile: 'dist-test/tsconfig.test.tsbuildinfo',
        },
      });

      const issues = checkTypecheckTestPolicy(root);
      // Should report: noEmit missing, composite, declaration, declarationMap, outDir dist-test, tsBuildInfoFile dist-test
      expect(issues.length).toBe(6);
      const messages = issues.map((issue) => issue.message);
      expect(messages).toContainEqual(expect.stringContaining('noEmit must be true'));
      expect(messages).toContainEqual(expect.stringContaining('composite'));
      expect(messages).toContainEqual(expect.stringContaining('declaration = true'));
      expect(messages).toContainEqual(expect.stringContaining('declarationMap'));
      expect(messages).toContainEqual(expect.stringContaining('dist-test'));
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('removes tsconfig.json reference to ./tsconfig.test.json', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJson(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
      });
      await writeJson(join(root, 'packages/app/tsconfig.test.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {
          noEmit: true,
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
        },
        include: ['src/**/*.test.ts'],
      });
      await writeJson(join(root, 'packages/app/tsconfig.json'), {
        references: [{ path: './tsconfig.lib.json' }, { path: './tsconfig.test.json' }],
      });

      // check should report the bad reference
      const issues = checkTypecheckTestPolicy(root);
      const referenceIssues = issues.filter((i) => i.message.includes('must not reference'));
      expect(referenceIssues.length).toBe(1);

      // apply should remove it
      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = (await readJson(join(root, 'packages/app/tsconfig.json'))) as Record<string, unknown>;
      const references = tsconfig.references as Array<{ path: string }>;
      expect(references).toEqual([{ path: './tsconfig.lib.json' }]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('copies lib compiler options', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJson(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
      });
      await writeJson(join(root, 'packages/app/tsconfig.lib.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {
          baseUrl: '.',
          module: 'esnext',
          moduleResolution: 'bundler',
          jsx: 'react-jsx',
          lib: ['ES2023', 'DOM'],
        },
      });

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = (await readJson(join(root, 'packages/app/tsconfig.test.json'))) as Record<string, unknown>;
      const compilerOptions = tsconfig.compilerOptions as Record<string, unknown>;
      expect(compilerOptions.baseUrl).toBe('.');
      expect(compilerOptions.module).toBe('esnext');
      expect(compilerOptions.moduleResolution).toBe('bundler');
      expect(compilerOptions.jsx).toBe('react-jsx');
      expect(compilerOptions.lib).toEqual(['ES2023', 'DOM']);
      // extends should come from tsconfig.lib.json
      expect(tsconfig.extends).toBe('../../tsconfig.base.json');

      // references should include ./tsconfig.lib.json
      const references = tsconfig.references as Array<{ path: string }>;
      expect(references).toContainEqual({ path: './tsconfig.lib.json' });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('adds workspace dependency references', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJson(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
      });
      await writeJson(join(root, 'packages/lib/tsconfig.lib.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {},
      });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
        dependencies: { '@scope/lib': 'workspace:*' },
      });
      await writeJson(join(root, 'packages/app/tsconfig.lib.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {},
      });

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = (await readJson(join(root, 'packages/app/tsconfig.test.json'))) as Record<string, unknown>;
      const references = tsconfig.references as Array<{ path: string }>;
      expect(references).toContainEqual({ path: './tsconfig.lib.json' });
      expect(references).toContainEqual({ path: '../lib/tsconfig.lib.json' });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('does not require tsconfig.test.json for non-bun/vitest runners', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJson(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'node --test' },
      });

      expect(checkTypecheckTestPolicy(root)).toEqual([]);
      expect(applyTypecheckTestPolicy(root)).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('accepts valid noEmit test tsconfig', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJson(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
      });
      await writeJson(join(root, 'packages/app/tsconfig.test.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
          noEmit: true,
          types: ['bun'],
        },
        include: [
          'src/**/*.test.ts',
          'src/**/*.spec.ts',
          'src/**/__tests__/**/*.ts',
          'src/**/__tests__/**/*.tsx',
          'src/test-suite-tracer.ts',
        ],
      });

      expect(checkTypecheckTestPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('detects bun test behind env prefix', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJson(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'NODE_ENV=test bun test' },
      });

      const issues = checkTypecheckTestPolicy(root);
      expect(issues.length).toBe(1);
      expect(issues[0]!.message).toContain('bun test');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
