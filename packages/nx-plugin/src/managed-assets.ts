import { fileURLToPath } from 'node:url';

/** Packaged assets, independent of the consumer's cwd and node_modules layout. */
export const managedAssetsRoot = fileURLToPath(new URL('../managed/', import.meta.url));
