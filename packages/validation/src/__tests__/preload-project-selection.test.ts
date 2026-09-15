import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
// Bun resolves a --preload argument as a module specifier, so it needs a path
// it cannot mistake for a package name.
const preload = join(packageRoot, 'src', 'bun', 'preload.ts');
const emitExcludedEntry = join('test-fixtures', 'emit-excluded', 'scripts', 'deploy.ts');

describe('ttsc project router', () => {
  it('transforms an entry point the emit program excludes', () => {
    // The fixture's nearest tsconfig.lib.json lists src/ alone, so the program
    // this file used to be routed to never held it: the run died with "ttsc
    // transform did not return output". Four discriminated results prove the
    // transform reached both this file and the module it imports.
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
});
