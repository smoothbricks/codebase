/**
 * Fail when any packaged CLI binary under `dist/bin/` lost its execute bit, when this host's N-API
 * addon is not under `dist/native/`, or when any family's addon there is empty or unreadable.
 *
 * The bits survive the build itself (cargo emits 0755 and napi copies it
 * through), but they do not survive the release pipeline's GitHub Actions
 * artifact round-trip: upload/download-artifact strips Unix permission bits,
 * so the publish job applies 0644 binaries into the tree that `bun pm pack`
 * then tars verbatim (pack only forces +x for `bin`-manifest targets, which
 * the platform binaries are not — @smoothbricks/cowshed@0.1.4 shipped them
 * 0644). This guard is the package's `release-check` target: `smoo release pack` runs it in the
 * publishing job, after every platform's prebuilt outputs merge and before the tarball is packed,
 * so a stripped state is a red pipeline instead of an EACCES in a user's bunx.
 *
 * An absent `dist/bin` is a failure, not a pass. This used to exit 0 with "no packaged binaries
 * to verify", which meant the guard reported success in exactly the situation where it had
 * verified nothing — including a build that silently produced no binaries at all. The target
 * declares no dependencies, so it judges the merged bytes and never rebuilds them: by the time
 * this runs, the publishing job has put the directory there. The development builds under
 * `.cache/bin/` are not shipped, and this never reads them.
 *
 * The addon of this host's family must be in the tree too, and every other family's addon that is
 * there is judged the same way, at `dist/native/<directory>/` for each row of `NATIVE_TARGETS`
 * (src/platform.ts `shippedAddonProblems`): an installed package loads only that tree, so a family
 * packed with a broken addon fails at a user's first import, and a consumer such as bounded-exec's
 * admission observer (`processLineages`) has nothing to call. The publishing job has every family's
 * addon there; the land seam builds this host's. An empty file, or one this process cannot open for
 * reading, is a problem; nothing is loaded. The development addons under `.cache/native-dev/` are
 * not shipped, and this never reads them.
 *
 * Run with: `nx run cowshed:release-check`
 *
 * With `--digest` it prints, instead of judging, a SHA-256 over what it judges: this host's family and every file under
 * `dist/bin` and `dist/native` with its mode and the SHA-256 of its bytes (`absent` for a missing tree). That is
 * the runtime input `release-check` is cached on, so a cached verdict stands for exactly these bytes and bits.
 */

import { createHash } from 'node:crypto';
import { closeSync, openSync, readdirSync, readFileSync, type Stats, statSync } from 'node:fs';
import { basename, join, relative } from 'node:path';
import { addonDirectory, shippedAddonProblems } from '../src/platform.js';

const BIN_ROOT = new URL('../dist/bin', import.meta.url).pathname;
const EXECUTABLE_OWNER_BIT = 0o100;
/**
 * The record of what a build read, which `smoo-nx-build-inputs` writes beside the binaries it
 * describes (@smoothbricks/build-inputs `BUILD_INPUTS_FILE`): data, never a binary to execute.
 */
const BUILD_INPUTS_FILE = '.build-inputs.json';

function listFilesRecursively(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? listFilesRecursively(path) : [path];
  });
}

if (process.argv.slice(2).join(' ') === '--digest') {
  const packageRoot = new URL('..', import.meta.url).pathname;
  const digest = createHash('sha256').update(`${process.platform}-${process.arch}\n`);
  for (const tree of ['dist/bin', 'dist/native']) {
    let files: string[];
    try {
      files = listFilesRecursively(join(packageRoot, tree)).sort();
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
      digest.update(`${tree} absent\n`);
      continue;
    }
    for (const path of files) {
      const bytes = createHash('sha256').update(readFileSync(path)).digest('hex');
      digest.update(`${relative(packageRoot, path)} ${statSync(path).mode.toString(8)} ${bytes}\n`);
    }
  }
  console.log(digest.digest('hex'));
  process.exit(0);
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

const files = listFilesRecursively(BIN_ROOT).filter((path) => basename(path) !== BUILD_INPUTS_FILE);
if (files.length === 0) {
  console.error(`${BIN_ROOT} holds no files, so no packaged binary was checked`);
  process.exit(1);
}

const notExecutable = files.filter((path) => (statSync(path).mode & EXECUTABLE_OWNER_BIT) === 0);
const PACKAGE_ROOT = new URL('..', import.meta.url).pathname;
const host = addonDirectory(process.platform, process.arch);
if (host === null) {
  console.error(`this package ships no N-API addon for ${process.platform}-${process.arch}, so none was checked`);
  process.exit(1);
}
const addonProblems = shippedAddonProblems(host, (relative) => {
  const path = join(PACKAGE_ROOT, relative);
  try {
    const entry = statSync(path);
    if (!entry.isFile()) {
      return { kind: 'other' };
    }
    // Readable means opened for reading: permission bits alone can lie (ACLs, flags, ownership).
    closeSync(openSync(path, 'r'));
    return { kind: 'file', bytes: entry.size };
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return { kind: 'absent' };
    }
    return { kind: 'unread', error: error instanceof Error ? error.message : String(error) };
  }
});

if (notExecutable.length > 0) {
  console.error('packaged binaries without the execute bit:');
  for (const path of notExecutable) {
    console.error(`  ${path}`);
  }
}
if (addonProblems.length > 0) {
  console.error(`N-API addon problems under ${PACKAGE_ROOT}:`);
  for (const problem of addonProblems) {
    console.error(`  ${problem}`);
  }
}
if (notExecutable.length > 0 || addonProblems.length > 0) {
  process.exit(1);
}

const count = files.length;
console.log(
  `${count} packaged ${count === 1 ? 'binary' : 'binaries'} executable in ${BIN_ROOT}; ` +
    `the ${host} N-API addon and every other family's present addon are readable`,
);
