/**
 * Fail when any packaged CLI binary under `dist/bin/` lost its execute bit.
 *
 * The bits survive the build itself (cargo emits 0755 and napi copies it
 * through), but they do not survive the release pipeline's GitHub Actions
 * artifact round-trip: upload/download-artifact strips Unix permission bits,
 * so the publish job applies 0644 binaries into the tree that `bun pm pack`
 * then tars verbatim (pack only forces +x for `bin`-manifest targets, which
 * the platform binaries are not — @smoothbricks/cowshed@0.1.4 shipped them
 * 0644). This guard runs wherever freshly built outputs exist so a stripped
 * state is a red pipeline instead of an EACCES in a user's bunx.
 *
 * An absent `dist/bin` is a failure, not a pass. This used to exit 0 with "no packaged binaries
 * to verify", which meant the guard reported success in exactly the situation where it had
 * verified nothing — including a build that silently produced no binaries at all. The nx target
 * depends on `build`, so by the time this runs the directory has to be there.
 *
 * Run with: `nx run cowshed:verify-packaging`
 */

import { readdirSync, type Stats, statSync } from 'node:fs';
import { join } from 'node:path';

const BIN_ROOT = new URL('../dist/bin', import.meta.url).pathname;
const EXECUTABLE_OWNER_BIT = 0o100;

function listFilesRecursively(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? listFilesRecursively(path) : [path];
  });
}

let stat: Stats;
try {
  stat = statSync(BIN_ROOT);
} catch (error) {
  console.error(`${BIN_ROOT} does not exist, so no packaged binary was checked`);
  console.error(`  ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
}
if (!stat.isDirectory()) {
  console.error(`${BIN_ROOT} is not a directory`);
  process.exit(1);
}

const files = listFilesRecursively(BIN_ROOT);
if (files.length === 0) {
  console.error(`${BIN_ROOT} holds no files, so no packaged binary was checked`);
  process.exit(1);
}

const notExecutable = files.filter((path) => (statSync(path).mode & EXECUTABLE_OWNER_BIT) === 0);

if (notExecutable.length > 0) {
  console.error('packaged binaries without the execute bit:');
  for (const path of notExecutable) {
    console.error(`  ${path}`);
  }
  process.exit(1);
}

const count = files.length;
console.log(`${count} packaged ${count === 1 ? 'binary' : 'binaries'} executable in ${BIN_ROOT}`);
