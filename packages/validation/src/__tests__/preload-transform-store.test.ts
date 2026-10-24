import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { generationPath } from '../bun/transform-store.js';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const preload = join(packageRoot, 'src', 'bun', 'preload.ts');

/**
 * A preload that counts the child processes ttsc starts synchronously (plugin
 * key resolution, descriptor evaluation, the native transform) beside the real
 * preload. ttsc reads `spawnSync` off the shared module object at call time, so
 * the patch sees every call. The count goes to stderr on exit; stdout stays the
 * entry's JSON.
 */
function countingPreload(realPreload: string): string {
  return [
    "import childProcess from 'node:child_process';",
    "import fs from 'node:fs';",
    `import ${JSON.stringify(pathToFileURL(realPreload).href)};`,
    'let spawns = 0;',
    'const spawnSync = childProcess.spawnSync;',
    'childProcess.spawnSync = (...args) => {',
    '  spawns += 1;',
    '  return Reflect.apply(spawnSync, childProcess, args);',
    '};',
    // biome-ignore lint/suspicious/noTemplateCurlyInString: the child's source text, interpolated by the child.
    "process.on('exit', () => fs.writeSync(2, `ttsc spawns: ${spawns}\\n`));",
    '',
  ].join('\n');
}

describe('stored ttsc generations', () => {
  it('serves a later process without compiling, and recompiles when a type-only input changes', () => {
    // The managed shell sets it; without it the preload has nowhere to store.
    const ttscCache = process.env.TTSC_CACHE_DIR;
    expect(ttscCache).toBeDefined();
    const store = join(ttscCache ?? '', 'bun-preload');
    const root = mkdtempSync(join(packageRoot, 'test-fixtures', 'transform-store-'));
    const project = join(root, 'tsconfig.json');
    try {
      writeFileSync(
        project,
        JSON.stringify({
          compilerOptions: {
            target: 'esnext',
            module: 'esnext',
            moduleResolution: 'bundler',
            noEmit: true,
            allowImportingTsExtensions: true,
            types: [],
          },
          files: ['entry.ts', 'payload.ts'],
        }),
      );
      writeFileSync(join(root, 'payload.ts'), 'export interface Payload { id: string }\n');
      writeFileSync(
        join(root, 'entry.ts'),
        `
import typia from 'typia';
import type { Payload } from './payload.ts';
const isPayload = typia.createIs<Payload>();
console.log(JSON.stringify({ text: isPayload({ id: 'a' }), number: isPayload({ id: 1 }) }));
`,
      );
      const counter = join(root, 'count-spawns.mjs');
      writeFileSync(counter, countingPreload(preload));
      const run = () => {
        const result = spawnSync(process.execPath, ['--preload', counter, join(root, 'entry.ts')], {
          cwd: packageRoot,
          encoding: 'utf8',
        });
        if (result.status !== 0) console.error(result.stderr);
        expect(result.status).toBe(0);
        // The marker must be present, so a missing line fails rather than reading as zero spawns.
        const spawns = /^ttsc spawns: (\d+)$/m.exec(result.stderr)?.[1];
        expect(spawns).toBeDefined();
        return { output: JSON.parse(result.stdout), spawns: Number(spawns) };
      };

      const cold = run();
      expect(cold.output).toEqual({ text: true, number: false });
      expect(cold.spawns).toBeGreaterThan(0);
      expect(existsSync(generationPath(store, project))).toBe(true);

      // Nothing changed: the stored generation is proven and served, and ttsc
      // starts no process at all.
      const warm = run();
      expect(warm.output).toEqual({ text: true, number: false });
      expect(warm.spawns).toBe(0);

      // entry.ts is byte-identical; only a type it imports changed. The stored
      // validator for `id: string` must not be served.
      writeFileSync(join(root, 'payload.ts'), 'export interface Payload { id: number }\n');
      const changed = run();
      expect(changed.output).toEqual({ text: false, number: true });
      expect(changed.spawns).toBeGreaterThan(0);
    } finally {
      rmSync(root, { recursive: true, force: true });
      rmSync(generationPath(store, project), { force: true });
    }
  });
});
