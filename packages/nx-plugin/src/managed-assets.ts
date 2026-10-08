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
