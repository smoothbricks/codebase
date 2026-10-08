import { fileURLToPath } from 'node:url';

/** Packaged assets, independent of the consumer's cwd and node_modules layout. */
export const managedAssetsRoot = fileURLToPath(new URL('../managed/', import.meta.url));

/** Explicit package-manager relink intent for the packaged bootstrap preservation helper. */
export interface KeepDeveloperLinksOptions {
  readonly relink: readonly string[];
}

/** The managed helper's callable contract; type-only consumers need no installed runtime import. */
export type KeepDeveloperLinks = <T>(
  root: string,
  install: () => Promise<T>,
  options?: KeepDeveloperLinksOptions,
) => Promise<T>;

/** Portable immutable package identities; host paths are resolved only by the activation owner. */
export interface PackageDependencyClosure {
  readonly version: 1;
  readonly packageName: string;
  readonly platform: string;
  readonly arch: string;
  readonly dependencies: readonly {
    readonly name: string;
    readonly version: string;
    readonly storePath: string;
    readonly bindingPaths: readonly string[];
    readonly direct: boolean;
  }[];
}

/**
 * Cached product description; only setup activation publishes its immutable installed generation.
 * The public installed layout is `<product directory>/node_modules/<closure.packageName>`;
 * its alias selects `generations/<identity>/node_modules/<closure.packageName>`.
 * Static typed consumers use that installed namespace, never the producer's cached staging files.
 */
export interface PortablePackageProduct {
  readonly version: 1;
  /** Existing tar release identity, plus the declared producer's existing Nx task hash when present. */
  readonly identity: string;
  readonly asset: string;
  readonly integrity: string;
  readonly closure: PackageDependencyClosure;
}
