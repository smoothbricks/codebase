import { runCli } from '../cli.js';
import { ffiLocks } from '../locks-ffi.js';

// The compiled CLI: scriptc builds this entry, and the package's `ffi.json` binds `tryLockFd` (native/flock.c) for locks-ffi.ts.
process.exitCode = await runCli(process.argv.slice(2), ffiLocks, Date.now());
