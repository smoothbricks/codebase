/// <reference types="node" />

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import typia from 'typia';
import { packageRootFromModule } from './launcher.js';

// Every host this package ships a native addon and a CLI binary for, derived at load from the
// one owner of that list: `package.json`'s `napi.targets` triples (the napi-rs convention).
// The Nx plugin generates the `napi-*` and `cli-*` build targets from the same list, and this
// loader derives the same rows from it, so no second statement of the triples or directories
// exists to drift. `bin/cowshed` still restates the CLI directories because the launcher is a
// shell script; `launcher.test.ts` checks it against these derived rows.
//
// The Linux CLI is a static musl executable (no loader, no libraries), so its triple is the
// musl twin of the addon's gnu triple and its directory carries no libc suffix, while the
// addon is glibc and keeps `-gnu`.

/** One shipped host: where its addon and its CLI binary live under `dist`. */
export interface NativeTarget {
  readonly platform: NodeJS.Platform;
  readonly arch: string;
  readonly addon: { readonly triple: string; readonly directory: string };
  readonly cli: { readonly triple: string; readonly directory: string };
}

/**
 * The row one shipped triple names. A triple is its grammar, not a row of a second list:
 * `<cpu>-apple-darwin` or `<cpu>-unknown-linux-gnu`, exactly, with no extra segment and no
 * other vendor or environment. Anything else in `napi.targets` is refused by name here, as
 * the Nx plugin refuses a triple with no convention at graph time.
 */
function targetFromTriple(triple: string): NativeTarget {
  const parts = triple.split('-');
  const cpu: string | undefined = parts[0];
  const vendor: string | undefined = parts[1];
  const os: string | undefined = parts[2];
  const env: string | undefined = parts[3];
  const arch = cpu === 'aarch64' ? 'arm64' : cpu === 'x86_64' ? 'x64' : null;
  const isDarwin = parts.length === 3 && vendor === 'apple' && os === 'darwin';
  const isLinux = parts.length === 4 && vendor === 'unknown' && os === 'linux' && env === 'gnu';
  if (arch === null || (!isDarwin && !isLinux)) {
    throw new Error(
      `package.json napi.targets names ${triple}, a triple this loader cannot place: it ships <aarch64|x86_64>-apple-darwin and <aarch64|x86_64>-unknown-linux-gnu only`,
    );
  }
  const platform: NodeJS.Platform = isDarwin ? 'darwin' : 'linux';
  const addonDirectory = isDarwin ? `${platform}-${arch}` : `${platform}-${arch}-gnu`;
  return {
    platform,
    arch,
    addon: { triple, directory: addonDirectory },
    cli: isLinux
      ? { triple: triple.replace(/-gnu$/, '-musl'), directory: `${platform}-${arch}` }
      : { triple, directory: addonDirectory },
  };
}

/**
 * The one projection of the package manifest this loader reads. Admission is Typia-generated
 * and additive: the manifest's other fields are npm's and Nx's to own, never refused here.
 */
interface NapiManifest {
  readonly napi: { readonly targets: readonly string[] };
}

const assertNapiManifest = typia.createAssert<NapiManifest>();

function deriveNativeTargets(): readonly NativeTarget[] {
  const manifest = assertNapiManifest(
    JSON.parse(readFileSync(join(packageRootFromModule(import.meta.url), 'package.json'), 'utf8')),
  );
  if (manifest.napi.targets.length === 0) {
    throw new Error('package.json napi.targets is empty: this package ships a native addon and CLI for no host');
  }
  return manifest.napi.targets.map(targetFromTriple);
}

export const NATIVE_TARGETS: readonly NativeTarget[] = deriveNativeTargets();

export function addonDirectory(platform: NodeJS.Platform, arch: string): string | null {
  const row = NATIVE_TARGETS.find((target) => target.platform === platform && target.arch === arch);
  return row?.addon.directory ?? null;
}

export function cliDirectory(platform: NodeJS.Platform, arch: string): string | null {
  const row = NATIVE_TARGETS.find((target) => target.platform === platform && target.arch === arch);
  return row?.cli.directory ?? null;
}
