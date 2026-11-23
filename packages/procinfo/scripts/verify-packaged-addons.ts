/**
 * procinfo's `release-check` (package.json): fail when this host's N-API addon is not under `dist/native/`, or when
 * any family's addon there is empty, not a regular file, or unreadable. `smoo release pack` runs it in the publishing
 * job after every platform's prebuilt addon is applied and before the tarball is packed, so it judges the bytes that
 * get packed; the target declares no dependencies and never rebuilds them. The development addons under
 * `.cache/native-dev/` and `.cache/native-debug/` are not shipped, and this never reads them.
 *
 * With `--digest` it prints, instead of judging, a SHA-256 over what it judges: this host's family and every file
 * under `dist/native` with its mode and the SHA-256 of its bytes (`absent` for a missing tree), URL-safe base64. That
 * is the runtime input `release-check` is cached on, so a cached verdict stands for exactly these bytes.
 */

import { createHash } from 'node:crypto';
import { closeSync, openSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import { ADDON_DIRECTORIES, addonFileName } from '../src/addon.js';

const PACKAGE_ROOT = new URL('..', import.meta.url).pathname;
const NATIVE_ROOT = join(PACKAGE_ROOT, 'dist/native');
const host = `${process.platform}-${process.arch}`;

function listFilesRecursively(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? listFilesRecursively(path) : [path];
  });
}

if (process.argv.slice(2).join(' ') === '--digest') {
  const digest = createHash('sha256').update(`${host}\n`);
  let files: string[] = [];
  try {
    files = listFilesRecursively(NATIVE_ROOT).sort();
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
    digest.update('dist/native absent\n');
  }
  for (const path of files) {
    const bytes = createHash('sha256').update(readFileSync(path)).digest('base64url');
    digest.update(`${relative(PACKAGE_ROOT, path)} ${statSync(path).mode.toString(8)} ${bytes}\n`);
  }
  console.log(digest.digest('base64url'));
  process.exit(0);
}

const hostDirectory = ADDON_DIRECTORIES.get(host);
if (hostDirectory === undefined) {
  console.error(`procinfo ships no N-API addon for ${host}, so none was checked`);
  process.exit(1);
}

/** What is wrong with the addon of family `directory`, if anything; `required` says it is this host's. */
function problem(directory: string, required: boolean): string | null {
  const path = join('dist/native', directory, addonFileName(directory));
  try {
    const entry = statSync(join(PACKAGE_ROOT, path));
    if (!entry.isFile()) return `${path}: not a regular file`;
    if (entry.size === 0) return `${path}: empty`;
    // Readable means opened for reading: permission bits alone can lie (ACLs, flags, ownership).
    closeSync(openSync(join(PACKAGE_ROOT, path), 'r'));
    return null;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return required ? `${path}: missing for this host's family` : null;
    }
    return `${path}: ${error instanceof Error ? error.message : String(error)}`;
  }
}

const problems = [...ADDON_DIRECTORIES.values()].flatMap((directory) => {
  const found = problem(directory, directory === hostDirectory);
  return found === null ? [] : [found];
});
if (problems.length > 0) {
  console.error(`N-API addon problems under ${PACKAGE_ROOT}:`);
  for (const found of problems) console.error(`  ${found}`);
  process.exit(1);
}
console.log(`the ${hostDirectory} N-API addon and every other family's present addon are readable`);
