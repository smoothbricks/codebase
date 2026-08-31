import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

/**
 * The sccache patches live in exactly one place — `packages/cowshed/nix/sccache`, beside the flake
 * that applies them — and this check keeps that directory and the flake's `patches` list naming
 * each other exactly.
 *
 * It replaces a byte-for-byte comparison between two physical copies of the patches, which existed
 * because the direnv nixpkgs overlay was a separate flake that could not reference paths outside
 * its own directory. That second copy is gone, and with it the drift the old check guarded. The
 * failure mode that remains is quieter and worse: nix silently ignores a `.patch` file nobody
 * references, so an orphaned patch reads as applied while the built binary does not carry it —
 * which is precisely how a half-patched sccache came to serve a fleet of fully-patched clients.
 * A reference to a file that is not there fails loudly at build time, but only for whoever next
 * runs `nix build`, which may be weeks after the commit that deleted it.
 *
 * So: every `.patch` in the directory must be referenced by `flake.nix`, and every path
 * `flake.nix` references must exist. Both directions, or the check has no teeth.
 */
const SCCACHE_FLAKE_DIR = 'packages/cowshed/nix/sccache';
const SCCACHE_FLAKE = 'flake.nix';

/**
 * A relative patch path as the flake spells it: `./name.patch`, in nix path syntax.
 *
 * Matched rather than parsed. A nix evaluator would be the rigorous answer and would also make
 * `smoo monorepo check` depend on nix being installed, which no other check does. The pattern is
 * anchored on the `.patch` extension and the `./` prefix, so it cannot match an arbitrary string,
 * and the two-directional comparison below turns any spelling this misses into a reported failure
 * rather than a silent pass.
 */
const FLAKE_PATCH_REFERENCE = /(?<![\w./-])\.\/([\w.-]+\.patch)(?![\w./-])/g;

function report(message: string): number {
  console.error(message);
  return 1;
}

export function validateSccachePatches(root: string): number {
  const directory = join(root, SCCACHE_FLAKE_DIR);
  const flakePath = join(directory, SCCACHE_FLAKE);
  if (!existsSync(flakePath)) {
    return report(`${SCCACHE_FLAKE_DIR}/${SCCACHE_FLAKE}: missing; the sccache flake is not there`);
  }
  const present = new Set(
    readdirSync(directory)
      .filter((name) => name.endsWith('.patch') && statSync(join(directory, name)).isFile())
      .sort(),
  );
  const referenced = new Set(
    [...readFileSync(flakePath, 'utf8').matchAll(FLAKE_PATCH_REFERENCE)].map((match) => match[1]),
  );

  let failures = 0;
  if (referenced.size === 0) {
    // A flake that applies no patches builds plain upstream sccache under a `-cowshed` version, so
    // every client would trust a binary carrying none of the behaviour that name promises.
    failures += report(
      `${SCCACHE_FLAKE_DIR}/${SCCACHE_FLAKE}: references no .patch files; the patched sccache is what this flake is for`,
    );
  }
  for (const name of [...new Set([...present, ...referenced])].sort()) {
    if (!referenced.has(name)) {
      failures += report(
        `${SCCACHE_FLAKE_DIR}/${name}: not referenced by ${SCCACHE_FLAKE}; nix would ignore it and the built sccache would not carry it`,
      );
      continue;
    }
    if (!present.has(name)) {
      failures += report(`${SCCACHE_FLAKE_DIR}/${SCCACHE_FLAKE}: references ./${name}, which is not there`);
    }
  }
  return failures;
}
