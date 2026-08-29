import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { readJsonObject } from '../lib/json.js';
import { runText } from '../lib/run.js';

/**
 * devenv's Go and the Go SDK ttsc vendors inside its native package, as a pair.
 *
 * These are two different toolchains by construction: ttsc exposes no override
 * for the SDK it ships, so the only way to have one Go is for the devenv pin to
 * name the same patch release the vendored SDK reports. Nothing enforces that
 * agreement at install time — npm resolves ttsc, the lock resolves Go, and the
 * two move on unrelated schedules — so it is checked here instead of being
 * discovered as `compile: version does not match go tool version` mid-build.
 */
export interface GoToolchainPair {
  /** Normalised `goX.Y.Z` from the devenv profile's `go version`. */
  devenv: string;
  /** Normalised `goX.Y.Z` from the vendored SDK's own VERSION file. */
  vendored: string;
  /** The ttsc version whose native package supplied `vendored`. */
  ttscVersion: string;
  /** Where `vendored` was read from, so a mismatch report names the file. */
  vendoredPath: string;
}

/**
 * ttsc's native package for the running platform. Its name is the only thing
 * that varies, so the VERSION file's location follows from it.
 */
export function ttscPlatformPackage(platform: string, arch: string): string | null {
  const os = platform === 'darwin' ? 'darwin' : platform === 'linux' ? 'linux' : platform === 'win32' ? 'win32' : null;
  if (os === null) {
    return null;
  }
  // `arm` (32-bit) is published for linux only, matching ttsc's own
  // optionalDependencies set.
  const cpu = arch === 'arm64' ? 'arm64' : arch === 'x64' ? 'x64' : arch === 'arm' && os === 'linux' ? 'arm' : null;
  return cpu === null ? null : `${os}-${cpu}`;
}

/** `go version go1.26.5 darwin/arm64` and a bare `go1.26.5` both reduce to `go1.26.5`. */
export function normaliseGoVersion(text: string): string | null {
  return /\b(go\d+\.\d+(?:\.\d+)?(?:rc\d+|beta\d+)?)\b/.exec(text)?.[1] ?? null;
}

/**
 * Candidate locations for the vendored VERSION file, most conventional first.
 *
 * Bun does not hoist a transitive optional platform package into the top-level
 * node_modules, so the store layout is load-bearing here rather than a fallback:
 * on a normal install only the second form exists.
 */
export function vendoredVersionCandidates(root: string, platformPackage: string, ttscVersion: string): string[] {
  const suffix = join('bin', 'go', 'VERSION');
  return [
    join(root, 'node_modules', '@ttsc', platformPackage, suffix),
    join(
      root,
      'node_modules',
      '.bun',
      `@ttsc+${platformPackage}@${ttscVersion}`,
      'node_modules',
      '@ttsc',
      platformPackage,
      suffix,
    ),
  ];
}

/**
 * Read the pair from disk, or explain why it cannot be read.
 *
 * Returns null only when this repository does not depend on ttsc at all, which
 * is the one case where there is no invariant to check. Every other unreadable
 * state is an error rather than a silent pass: if ttsc is installed and its
 * vendored SDK cannot be located, the check cannot speak to the invariant and
 * must say so.
 */
export function readGoToolchainPair(
  root: string,
  goBinary: string,
  devenvGoVersionText: string,
): GoToolchainPair | Error | null {
  const ttscPackage = readJsonObject(join(root, 'node_modules', 'ttsc', 'package.json'));
  if (!ttscPackage) {
    return null;
  }
  const ttscVersion = typeof ttscPackage.version === 'string' ? ttscPackage.version : null;
  if (ttscVersion === null) {
    return new Error('node_modules/ttsc/package.json has no version field, so its vendored Go SDK cannot be located');
  }
  const platformPackage = ttscPlatformPackage(process.platform, process.arch);
  if (platformPackage === null) {
    return new Error(`ttsc publishes no native package for ${process.platform}-${process.arch}`);
  }
  const candidates = vendoredVersionCandidates(root, platformPackage, ttscVersion);
  const vendoredPath = candidates.find((path) => existsSync(path));
  if (vendoredPath === undefined) {
    return new Error(
      `ttsc ${ttscVersion} is installed but its vendored Go SDK VERSION file was not found; looked in:\n  ${candidates.join('\n  ')}`,
    );
  }
  const vendored = normaliseGoVersion(readFileSync(vendoredPath, 'utf8'));
  if (vendored === null) {
    return new Error(`${vendoredPath} does not contain a recognisable Go version`);
  }
  const devenv = normaliseGoVersion(devenvGoVersionText);
  if (devenv === null) {
    return new Error(`\`${goBinary} version\` printed no recognisable Go version: ${devenvGoVersionText.trim()}`);
  }
  return { devenv, vendored, ttscVersion, vendoredPath };
}

/**
 * Fail loudly when the pinned Go and the vendored SDK disagree.
 *
 * Pure so the comparison and its message are testable without an install tree.
 */
export function validateGoToolchainPair(pair: GoToolchainPair): number {
  if (pair.devenv === pair.vendored) {
    return 0;
  }
  console.error(
    `devenv provides ${pair.devenv} but ttsc ${pair.ttscVersion} vendors ${pair.vendored} (${pair.vendoredPath}).\n` +
      '  Two Go toolchains are two cache keys, and a GOROOT crossing between them fails as\n' +
      '  `compile: version does not match go tool version`. Resolve it by moving one to the other:\n' +
      `  move devenv's Go to ${pair.vendored} (update devenv.lock, or pin a nixpkgs revision that\n` +
      `  packages it), or pin ttsc to a release whose vendored SDK reports ${pair.devenv}.\n` +
      '  Not every pairing is reachable — ttsc 0.28.1 and 0.28.2 vendor go1.26.6, which no nixpkgs\n' +
      '  channel packages — so prefer moving the ttsc pin when no revision offers the vendored patch.',
  );
  return 1;
}

/**
 * Compare the Go devenv provides against the SDK ttsc vendors.
 *
 * Reads the live devenv profile rather than a stored template, for the same
 * reason the runtime pin check does: the shell is the thing that builds, so it
 * is the only authoritative statement of which Go is in play.
 */
export async function validateGoToolchainAgreement(root: string): Promise<number> {
  const goBinary = goCommand(root);
  let devenvGoVersionText: string;
  try {
    devenvGoVersionText = await runText(goBinary, ['version'], root);
  } catch (error) {
    console.error(`Unable to run \`${goBinary} version\`: ${error instanceof Error ? error.message : String(error)}`);
    return 1;
  }
  const pair = readGoToolchainPair(root, goBinary, devenvGoVersionText);
  if (pair === null) {
    return 0;
  }
  if (pair instanceof Error) {
    console.error(pair.message);
    return 1;
  }
  return validateGoToolchainPair(pair);
}

function goCommand(root: string): string {
  const candidates = [
    join(root, 'tooling', 'direnv', '.devenv', 'profile', 'bin', 'go'),
    join(root, 'tooling', 'devenv', '.profile', 'bin', 'go'),
  ];
  return candidates.find((path) => existsSync(path)) ?? 'go';
}
