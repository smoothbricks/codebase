import { basename, dirname, join } from 'node:path';

// Every host this package ships a native addon and a CLI binary for. `addonDirectory` and
// `cliDirectory` are derived from this table.
//
// `package.json` restates the addon triples as `napi.targets`, and the CLI triples and directories
// in the per-arch `cli-*` targets: napi `--target` and the `outputDir` its `--output-dir` reads on
// macOS, the `cargo-zigbuild` executor's `target`/`output` on Linux, each in a `.cache/bin` tree for
// the development build and a `dist/bin` tree for the shipped one. `bin/cowshed` restates the CLI
// directories because the launcher is a shell script. `platform.test.ts` checks the `package.json`
// restatements, and `launcher.test.ts` checks `bin/cowshed`.
//
// The Linux CLI is a static musl executable (no loader, no libraries), so its directory carries no
// libc suffix, while the addon is glibc and keeps `-gnu`.
export const NATIVE_TARGETS = [
  {
    platform: 'darwin',
    arch: 'arm64',
    addon: { triple: 'aarch64-apple-darwin', directory: 'darwin-arm64' },
    cli: { triple: 'aarch64-apple-darwin', directory: 'darwin-arm64' },
  },
  {
    platform: 'darwin',
    arch: 'x64',
    addon: { triple: 'x86_64-apple-darwin', directory: 'darwin-x64' },
    cli: { triple: 'x86_64-apple-darwin', directory: 'darwin-x64' },
  },
  {
    platform: 'linux',
    arch: 'arm64',
    addon: { triple: 'aarch64-unknown-linux-gnu', directory: 'linux-arm64-gnu' },
    cli: { triple: 'aarch64-unknown-linux-musl', directory: 'linux-arm64' },
  },
  {
    platform: 'linux',
    arch: 'x64',
    addon: { triple: 'x86_64-unknown-linux-gnu', directory: 'linux-x64-gnu' },
    cli: { triple: 'x86_64-unknown-linux-musl', directory: 'linux-x64' },
  },
] as const;

export function addonDirectory(platform: NodeJS.Platform, arch: string): string | null {
  const row = NATIVE_TARGETS.find((target) => target.platform === platform && target.arch === arch);
  return row?.addon.directory ?? null;
}

/** The addon's file name in a family's directory, as `napi build` writes it and the loader reads it. */
export function addonFileName(directory: string): string {
  return `cowshed.${directory}.node`;
}

/** What the release guard reads at one path of a packed tree. */
export type ShippedEntry =
  | { readonly kind: 'file'; readonly bytes: number }
  | { readonly kind: 'absent' }
  | { readonly kind: 'other' }
  | { readonly kind: 'unread'; readonly error: string };

/**
 * What a packed tree gets wrong about its N-API addons, each problem with its path: the addon of the
 * `host` family (an {@link addonDirectory}) must be there, and every other family's that is there
 * must be the same, a non-empty regular file that can be read, at
 * `dist/native/<directory>/<addonFileName>` for each {@link NATIVE_TARGETS} row, the only tree an
 * installed package loads ({@link addonSearch}). A family whose addon is absent is not this tree's
 * unless it is the host's, as the CLI binaries under `dist/bin` are judged for what is there. `read`
 * answers for a path relative to the package root.
 */
export function shippedAddonProblems(host: string, read: (relative: string) => ShippedEntry): string[] {
  return NATIVE_TARGETS.flatMap(({ addon }) => {
    const relative = join('dist/native', addon.directory, addonFileName(addon.directory));
    const entry = read(relative);
    switch (entry.kind) {
      case 'file':
        return entry.bytes > 0 ? [] : [`${relative}: empty`];
      case 'absent':
        return addon.directory === host ? [`${relative}: missing for this host's family`] : [];
      case 'other':
        return [`${relative}: not a regular file`];
      case 'unread':
        return [`${relative}: ${entry.error}`];
    }
  });
}

export function cliDirectory(platform: NodeJS.Platform, arch: string): string | null {
  const row = NATIVE_TARGETS.find((target) => target.platform === platform && target.arch === arch);
  return row?.cli.directory ?? null;
}

/**
 * The package root of a module at `src/<file>` (run from source) or `dist/ts/<file>` (built, in a checkout or
 * installed).
 */
function packageRoot(modulePath: string): string {
  const directory = dirname(modulePath);
  return basename(directory) === 'src' ? dirname(directory) : dirname(dirname(directory));
}

/** Where the loader looks for the addon, and the Nx target that writes it where something in the checkout does. */
export interface AddonSearch {
  /** The files to try, in order; the first that loads wins. */
  readonly paths: readonly string[];
  /** The Nx target that writes them in a checkout, or `null` for an installed package, which ships its addon. */
  readonly producer: string | null;
}

/**
 * The addon's own source entry: tracked in a checkout, and in no published package, whose `files` list no crate. Judging
 * a checkout by it is unambiguous even for compiled JavaScript; a generic `src/` entry is not, since a pack policy can
 * add `src` for development exports. The CLI's launcher reads `crates/cowshed-cli/src/main.rs` the same way.
 */
const ADDON_SOURCE_ENTRY = 'crates/cowshed-napi/src/lib.rs';

/**
 * The package root says which tree is authoritative, and the loader never falls from one tree to another. A checkout
 * holds the addon's source entry, so it loads the dev build `cowshed:napi-native` writes under `.cache/native-dev`,
 * even running compiled from `dist/ts/`: a missing dev build is an error, never a reason to load a release that an
 * earlier production build left in `dist/native`. An installed package has no source and loads `dist/native`, the only
 * tree it ships.
 *
 * Two names are decided by their caller. The napi-rs loaders' own `NAPI_RS_NATIVE_LIBRARY_PATH` override comes first.
 * `NAPI_DEBUG_ADDON=1`, which only the napi-test targets set, replaces the tree with the debug addon of their own graph.
 */
export function addonSearch(search: {
  /** This module's own file: `src/<name>.ts` run from source, `dist/ts/<name>.js` built. */
  readonly modulePath: string;
  readonly exists: (path: string) => boolean;
  readonly directory: string;
  readonly fileName: string;
  readonly override: string | undefined;
  readonly debug: boolean;
}): AddonSearch {
  const root = packageRoot(search.modulePath);
  const override = search.override ? [search.override] : [];
  if (search.debug) {
    return { paths: [...override, join(root, '.cache/native-debug', search.fileName)], producer: 'cowshed:napi-debug' };
  }
  const checkout = search.exists(join(root, ADDON_SOURCE_ENTRY));
  const tree = checkout ? '.cache/native-dev' : 'dist/native';
  return {
    paths: [...override, ...['host', search.directory].map((name) => join(root, tree, name, search.fileName))],
    producer: checkout ? 'cowshed:napi-native' : null,
  };
}
