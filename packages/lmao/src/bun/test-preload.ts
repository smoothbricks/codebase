/**
 * Bun test preload — Typia validation codegen + LMAO test tracing.
 *
 * Combines:
 * 1. Typia Bun plugin (validation codegen at import time)
 * 2. LMAO autoSetupBunTestTracing (flush spans to .trace-results.db)
 *
 * Usage in bunfig.toml:
 *   [test]
 *   preload = ["@smoothbricks/lmao/bun/test-preload"]
 *
 * This is the ONLY preload packages need under [test]. It handles both
 * validation codegen and test tracing in one import.
 */

// 1. Register Typia plugin (must come before any typia imports)
import './preload.js';

// 2. Setup LMAO test tracing
import { join } from 'node:path';
import { autoSetupBunTestTracing } from '../lib/testing/bun-harness.js';

// WHY: Find the monorepo packages dir relative to this file's location.
// This file lives at packages/lmao/src/bun/test-preload.ts (4 levels up to monorepo root).
const monorepoRoot = join(import.meta.dir, '..', '..', '..', '..');
await autoSetupBunTestTracing({ packagesDir: join(monorepoRoot, 'packages') });
