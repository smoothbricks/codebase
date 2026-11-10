// The Bun shards and the `ttsc-plugin-hosts` target (package.json) name that target's output, the ttsc plugin hosts the
// suite loads, as a TTSC_CACHE_DIR relative to this package: an Nx option cannot name an absolute path (Nx resolves
// `{workspaceRoot}/…` to a workspace-relative one). ttsc resolves a relative TTSC_CACHE_DIR against its own working
// directory, and the suite runs ttsc in fixture directories, so this process names it against the directory it started
// in before any ttsc it runs reads it. The checkout's shell exports an absolute one, which this leaves as it is.
import { resolve } from 'node:path';

const cache = process.env.TTSC_CACHE_DIR;
if (cache !== undefined && cache !== '') process.env.TTSC_CACHE_DIR = resolve(cache);
