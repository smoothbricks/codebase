import type { CLibrary } from './open-beneath.js';

/** macOS's C library, as every process has it loaded, and the function that returns the address of its `errno`. */
export const darwinCLibrary: CLibrary = { names: ['libSystem.B.dylib'], errnoAddress: '__error' };
