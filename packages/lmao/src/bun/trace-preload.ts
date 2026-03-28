/**
 * Bun test tracing preload.
 *
 * Registers LMAO SQLite-backed test tracing after the Typia preload has already
 * been registered in bunfig.
 */

import { join } from 'node:path';
import { autoSetupBunTestTracing } from '../lib/testing/bun-harness.js';

// WHY: Find the monorepo packages dir relative to this file's location.
// This file lives at packages/lmao/src/bun/trace-preload.ts (4 levels up to monorepo root).
const monorepoRoot = join(import.meta.dir, '..', '..', '..', '..');
await autoSetupBunTestTracing({ packagesDir: join(monorepoRoot, 'packages') });
