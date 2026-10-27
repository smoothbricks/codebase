/**
 * The compiled CLI's open(2) and flock(2), bound by ffi.json to native/flock.c. openLockFd returns the descriptor,
 * or -1 on failure. tryLockFd returns 0 when held, 1 on contention, -1 on any other failure. Failures write errno and
 * the OS cause, NUL-terminated, into reason. The open path includes a trailing NUL. A hand-written declaration, because scriptc
 * binds a call to a declaration it can resolve from the JavaScript it compiles, and the native build puts this file
 * beside the emitted `locks-ffi.js`.
 */
declare function openLockFd(path: Uint8Array, create: number, reason: Uint8Array): number;
declare function tryLockFd(fd: number, reason: Uint8Array): number;
