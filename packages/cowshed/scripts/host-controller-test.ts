/// <reference types="bun" />

import { linkSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  cargoTargetDirectory,
  ensureInPlace,
  NEXTEST_BINARIES_METADATA,
  NEXTEST_CARGO_METADATA,
} from '../../nextest-extract/src/nextest-binaries.js';

const checkout = realpathSync(fileURLToPath(new URL('../../../', import.meta.url)));
const toolConfig = `smoo:${join(checkout, 'packages/nx-plugin/nextest.toml')}`;
const say = (line: string) => console.error(`host-controller-test: ${line}`);
if (process.argv[2] === '--ensure-binaries') {
  // The target's fixture: the binaries run where cargo-test-build left them, and a checkout whose build was a cache hit
  // builds them here, in the fixture's build interval, never under the test guard.
  try {
    await ensureInPlace(checkout, toolConfig, say);
    process.exit(0);
  } catch (error) {
    say(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}
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
  const target = await cargoTargetDirectory(checkout);
  const child = Bun.spawn(
    [
      'cargo',
      '--frozen',
      'nextest',
      'run',
      '--binaries-metadata',
      join(checkout, NEXTEST_BINARIES_METADATA),
      '--cargo-metadata',
      join(checkout, NEXTEST_CARGO_METADATA),
      '--target-dir-remap',
      target,
      '--workspace-remap',
      checkout,
      '-E',
      'test(/host_controller_/) and (package(cowshed-core) or package(cowshed-cli) or package(cowshed-shell) or package(cowshed-gateway))',
      '--run-ignored',
      'only',
      '--no-tests=fail',
      '--user-config-file',
      'none',
      '--tool-config-file',
      toolConfig,
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
