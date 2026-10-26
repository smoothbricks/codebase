import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, symlinkSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

it('keeps the installed scriptc toolchain valid when a warm checkout changes depth', () => {
  const require = createRequire(fileURLToPath(new URL('../../packages/failure-summary/package.json', import.meta.url)));
  const installer = require.resolve('scriptc/scripts/install-native.mjs');
  const root = mkdtempSync(join(tmpdir(), 'scriptc-install-'));
  try {
    mkdirSync(join(root, 'payload', 'bin'), { recursive: true });
    symlinkSync(join(root, 'payload'), join(root, 'cache'), 'dir');
    const run = spawnSync(
      'bun',
      [
        '--config=/dev/null',
        '--no-env-file',
        '--eval',
        `import assert from 'node:assert/strict';
import { realpathSync } from 'node:fs';
import { join } from 'node:path';
const { relocateToolchain } = await import(process.env.SCRIPTC_INSTALLER);
const root = process.env.SCRIPTC_FIXTURE;
const source = join(root, 'cache', 'bin');
const manifest = {
  ts7: '../tools/tsc',
  runtime_packs: [{ target: 'test', path: '../runtime' }],
  linker: 'clang',
  dsymutil: '../tools/dsymutil',
};
const installed = relocateToolchain(manifest, source, join(root, 'main', 'bin'));
const cloned = relocateToolchain(manifest, source, join(root, 'deeper', 'workspace', 'bin'));
assert.deepEqual(cloned, installed);
const payload = realpathSync(join(root, 'cache'));
assert.deepEqual(installed, {
  ts7: join(payload, 'tools', 'tsc'),
  runtime_packs: [{ target: 'test', path: join(payload, 'runtime') }],
  linker: 'clang',
  dsymutil: join(payload, 'tools', 'dsymutil'),
});`,
      ],
      {
        encoding: 'utf8',
        env: { ...process.env, SCRIPTC_INSTALLER: pathToFileURL(installer).href, SCRIPTC_FIXTURE: root },
      },
    );
    expect(run.error).toBeUndefined();
    expect(run.stderr).toBe('');
    expect(run.status).toBe(0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
