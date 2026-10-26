import { fileURLToPath } from 'node:url';

/** Provenance used by the copied developer-link helper; its schema remains owned by build-inputs. */
export type { BuildInputProducer } from '@smoothbricks/build-inputs';

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
