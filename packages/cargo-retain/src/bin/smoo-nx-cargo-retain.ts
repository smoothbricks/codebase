import { runCli } from '../cli.js';
import { ffiLocks } from '../locks-ffi.js';

// The compiled CLI: package.json#scriptc.entries names this module for the public bin's native executable, scriptc
// builds it, and the package's `ffi.json` binds `tryLockFd` (native/flock.c) for locks-ffi.ts. Node cannot run it.
process.exitCode = await runCli(process.argv.slice(2), ffiLocks, Date.now());
