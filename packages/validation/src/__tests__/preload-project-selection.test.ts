import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
// Bun resolves a --preload argument as a module specifier, so it needs a path
// it cannot mistake for a package name.
const preload = join(packageRoot, 'src', 'bun', 'preload.ts');
const emitExcludedEntry = join('test-fixtures', 'emit-excluded', 'scripts', 'deploy.ts');

describe('ttsc project router', () => {
  it('transforms an entry point the emit program excludes', () => {
    // The fixture's nearest tsconfig.lib.json lists src/ alone; the governing
    // config owns the entry point. The imported validator is deliberately
    // excluded from both root lists but remains a transitive program member.
    const result = spawnSync(process.execPath, ['--preload', preload, emitExcludedEntry], {
      cwd: packageRoot,
      encoding: 'utf8',
    });

    if (result.status !== 0) console.error(result.stderr);
    expect(result.status).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({
      imported: { good: true, bad: false },
      own: { good: true, bad: false },
    });
  });

  it('reselects source owners after an inherited config changes without losing unchanged modules', () => {
    const root = mkdtempSync(join(packageRoot, 'test-fixtures', 'project-selection-'));
    try {
      const compilerOptions = {
        target: 'esnext',
        module: 'esnext',
        moduleResolution: 'bundler',
        noEmit: true,
        allowImportingTsExtensions: true,
        types: [],
      };
      writeFileSync(
        join(root, 'tsconfig.json'),
        JSON.stringify({
          compilerOptions,
          files: ['entry.ts', 'plain.ts', 'validator.ts'],
        }),
      );
      writeFileSync(join(root, 'roots.json'), JSON.stringify({ files: ['plain.ts', 'validator.ts'] }));
      writeFileSync(
        join(root, 'tsconfig.lib.json'),
        JSON.stringify({
          extends: './roots.json',
          compilerOptions: { ...compilerOptions, plugins: [{ transform: 'typia', enabled: false }] },
        }),
      );
      writeFileSync(join(root, 'plain.ts'), 'export const plain = 7;\n');
      writeFileSync(
        join(root, 'validator.ts'),
        `
import typia from 'typia';
export const isPayload = typia.createIs<{ id: string }>();
`,
      );
      writeFileSync(
        join(root, 'entry.ts'),
        `
import { writeFileSync } from 'node:fs';
import { plain } from './plain.ts';
writeFileSync(new URL('./roots.json', import.meta.url), JSON.stringify({ files: ['plain.ts'] }));
// Static imports run before the config edit; this intentionally exercises a later module load.
const { isPayload } = await import('./validator.ts');
console.log(JSON.stringify({ plain, good: isPayload({ id: 'a' }), bad: isPayload({ id: 1 }) }));
`,
      );
      const result = spawnSync(process.execPath, ['--preload', preload, join(root, 'entry.ts')], {
        cwd: packageRoot,
        encoding: 'utf8',
      });
      if (result.status !== 0) console.error(result.stderr);
      expect(result.status).toBe(0);
      expect(JSON.parse(result.stdout)).toEqual({ plain: 7, good: true, bad: false });
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
