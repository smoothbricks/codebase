import { createHash } from 'node:crypto';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

/**
 * Cowshed owns the sccache patches (`rust-basedir-cwd` lets one cache serve
 * workspaces at different mount paths; `singleflight` stops N clones compiling
 * one crate). The direnv nixpkgs overlay is its own flake and cannot reference
 * paths outside its directory, so it keeps a physical copy — a symlink would
 * not survive `nix flake` source copying. This check makes silent divergence
 * unlandable: same filenames, same bytes.
 */
const AUTHORITATIVE_SCCACHE_PATCH_DIR = 'packages/cowshed/patches';
const OVERLAY_SCCACHE_PATCH_DIR = 'tooling/direnv/nixpkgs-overlay';

function listPatchNames(directory: string): string[] {
  if (!existsSync(directory)) {
    return [];
  }
  return readdirSync(directory)
    .filter((name) => name.endsWith('.patch') && statSync(join(directory, name)).isFile())
    .sort();
}

function report(message: string): number {
  console.error(message);
  return 1;
}

export function validateSccachePatches(root: string): number {
  const authoritativeDir = join(root, AUTHORITATIVE_SCCACHE_PATCH_DIR);
  const overlayDir = join(root, OVERLAY_SCCACHE_PATCH_DIR);
  const authoritative = new Set(listPatchNames(authoritativeDir));
  const overlay = new Set(listPatchNames(overlayDir));
  const names = [...new Set([...authoritative, ...overlay])].sort();

  let failures = 0;
  for (const name of names) {
    const authoritativePath = `${AUTHORITATIVE_SCCACHE_PATCH_DIR}/${name}`;
    const overlayPath = `${OVERLAY_SCCACHE_PATCH_DIR}/${name}`;
    if (!authoritative.has(name)) {
      failures += report(`${overlayPath}: no counterpart at ${authoritativePath}`);
      continue;
    }
    if (!overlay.has(name)) {
      failures += report(`${authoritativePath}: no counterpart at ${overlayPath}`);
      continue;
    }

    const authoritativeBytes = readFileSync(join(authoritativeDir, name));
    const overlayBytes = readFileSync(join(overlayDir, name));
    if (authoritativeBytes.equals(overlayBytes)) {
      continue;
    }
    const authoritativeHash = createHash('md5').update(authoritativeBytes).digest('hex');
    const overlayHash = createHash('md5').update(overlayBytes).digest('hex');
    failures += report(`${authoritativePath}: md5 ${authoritativeHash} != ${overlayPath} md5 ${overlayHash}`);
  }
  return failures;
}
