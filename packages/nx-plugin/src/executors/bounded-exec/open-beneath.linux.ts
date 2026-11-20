import type { CLibrary } from './open-beneath.js';

/** The musl loader's name on this architecture. */
const MUSL_ARCH: Partial<Record<NodeJS.Architecture, string>> = { x64: 'x86_64', arm64: 'aarch64' };

/** Linux's C library, glibc then musl by the name each is loaded under, and the function that returns `errno`'s address. */
export const linuxCLibrary: CLibrary = {
  names: ['libc.so.6', `libc.musl-${MUSL_ARCH[process.arch] ?? process.arch}.so.1`],
  errnoAddress: '__errno_location',
};
