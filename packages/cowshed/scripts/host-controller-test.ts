/// <reference types="bun" />

import { linkSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const checkout = realpathSync(fileURLToPath(new URL('../../../', import.meta.url)));
const temporaryRoot = join(checkout, '.cowshed/tmp');
let temporary: string | undefined;
let status = 1;
try {
  mkdirSync(temporaryRoot, { recursive: true });
  temporary = mkdtempSync(join(temporaryRoot, 'host-controller-'));
  const source = join(temporary, 'authority-source');
  writeFileSync(source, 'hard-link authority probe\n');
  // Test the actual operation, not an environment flag: a nested profile cannot
  // regain file-link authority denied by an enclosing workspace sandbox.
  linkSync(source, join(temporary, 'authority-alias'));
  const child = Bun.spawn(
    [
      'cargo',
      '--frozen',
      'nextest',
      'run',
      '-p',
      'cowshed-core',
      '-p',
      'cowshed-cli',
      '-E',
      'test(/host_controller_/)',
      '--run-ignored',
      'only',
      '--user-config-file',
      'none',
      '--config-file',
      'packages/nx-plugin/nextest.toml',
    ],
    {
      cwd: checkout,
      env: { ...process.env, TMPDIR: temporary },
      stdin: 'inherit',
      stdout: 'inherit',
      stderr: 'inherit',
    },
  );
  status = await child.exited;
} catch (error) {
  console.error(`Host-controller proofs could not run: ${error instanceof Error ? error.message : String(error)}`);
  console.error(`From an unsandboxed host-controller shell in ${checkout}, run: nx run cowshed:host-controller-test`);
  console.error(
    'Workspace supervisors and children deliberately deny file-link; no write grant or nested sandbox can restore it.',
  );
} finally {
  if (temporary !== undefined) {
    try {
      rmSync(temporary, { recursive: true });
    } catch (error) {
      console.error(
        `Could not remove host-controller proof directory ${temporary}: ${error instanceof Error ? error.message : String(error)}`,
      );
      status = 1;
    }
  }
}
process.exit(status);
