import { join } from 'node:path';
import { autoSetupBunTestTracing } from './packages/lmao/src/lib/testing/bun-harness.js';

// import.meta.dir is the directory containing this preload file (monorepo root)
await autoSetupBunTestTracing({ packagesDir: join(import.meta.dir, 'packages') });
