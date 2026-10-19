import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const preload = join(packageRoot, 'src', 'bun', 'preload.ts');
const multiDirectoryEntry = join('test-fixtures', 'watch-cost', 'scripts', 'deploy.ts');

/**
 * A preload that counts `fs.watch` calls on the shared `node:fs` default export,
 * then loads the real preload. ttsc's `transform.mjs` calls `fs.watch` through
 * that same object at call time, so the patch sees every registration. The total
 * goes to stderr with `writeSync` on exit, so stdout stays the pure JSON the
 * entry point prints.
 */
function countingPreload(realPreload: string): string {
  return [
    "import fs from 'node:fs';",
    `import ${JSON.stringify(pathToFileURL(realPreload).href)};`,
    'let registrations = 0;',
    'const watch = fs.watch;',
    'fs.watch = (...args) => {',
    '  registrations += 1;',
    '  return Reflect.apply(watch, fs, args);',
    '};',
    // biome-ignore lint/suspicious/noTemplateCurlyInString: the child's source text, interpolated by the child.
    "process.on('exit', () => fs.writeSync(2, `fs.watch registrations: ${registrations}\\n`));",
    '',
  ].join('\n');
}

/**
 * The seam under test is `preload.ts`'s polling directory watch, which opens no
 * `fs.watch`. ttsc's fallback opens a watcher per tracked location: one recursive
 * root for project membership, then one per host-input directory (`openDirectoryWatch`
 * in `@ttsc/unplugin`'s `lib/core/transform.mjs`, reached only when the cache has
 * no `watch` seam), so the oracle is the number of `fs.watch` registrations the
 * child makes, not elapsed time. The child carries no timer of the test's own:
 * the seam's interval is `unref`'d, so the run ends when the entry finishes, and
 * the leg's `idleTimeoutMs` is the backstop for a run that never does.
 */
describe('ttsc directory watching', () => {
  it('loads a multi-directory project without paying per-directory watch registration', () => {
    const dir = mkdtempSync(join(tmpdir(), 'watch-cost-'));
    try {
      const counter = join(dir, 'count-fs-watch.mjs');
      writeFileSync(counter, countingPreload(preload));
      const result = spawnSync(process.execPath, ['--preload', counter, multiDirectoryEntry], {
        cwd: packageRoot,
        encoding: 'utf8',
      });

      if (result.status !== 0) console.error(result.stderr);
      expect(result.status).toBe(0);
      // Nine discriminating validators: the transform still reaches every module
      // the entry point imports, and its own callsite.
      expect(JSON.parse(result.stdout)).toEqual({ all: true, count: 9 });
      // The marker must be present, so a missing line fails rather than reading
      // as zero registrations.
      expect(/^fs\.watch registrations: (\d+)$/m.exec(result.stderr)?.[1]).toBe('0');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
