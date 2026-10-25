/**
 * The compiled CLI's one flock(2), bound to `cargo_retain_try_lock` in `native/flock.c` by the native build's FFI
 * manifest (`ffi.json`): 0 when the exclusive lock is taken, 1 when another process holds it, -1 for any other failure,
 * whose `strerror` text is written, NUL-terminated, into `reason`. A hand-written declaration file, because scriptc
 * binds a call to a declaration it can resolve from the JavaScript it compiles, and the native build puts this file
 * beside the emitted `locks-ffi.js`.
 */
declare function tryLockFd(fd: number, reason: Uint8Array): number;
