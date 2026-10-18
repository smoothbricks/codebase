/**
 * Build prerequisite for the `test` target (see `test-warmup` in
 * `packages/nx-cache-server/package.json`), following the `wire-warmup`
 * pattern in `packages/cowshed`.
 *
 * Each TypeScript project the test loads through the compiler preload keys its
 * own native plugin build (`ttsc: building source plugin ... can take several
 * minutes on a cold Go cache`). That compilation must never run inside the
 * 180s-idle-bounded `test`, so this script imports a module from every project
 * the test's preloads and sources load, paying each key in a 600s build step.
 * The test then reuses the warm keys. A preload added to `bunfig.toml` that
 * loads another project needs its modules imported here too.
 *
 * Plain `.mjs` on purpose: the ttsc router only claims TypeScript files, so
 * this entry loads through Bun natively while every TypeScript module it
 * imports is still transformed through the real router.
 *
 * Run under the compiler preload ONLY — never add the trace preload itself:
 * its setup calls `afterAll()`, which throws outside `bun test`. Its module,
 * LMAO's Bun test harness, is imported instead: loading it does no setup, and
 * it routes through LMAO's own project, whose plugin key this project's never
 * covers.
 *
 * Never import test files from this script: warming must not execute tests.
 */

import '../src/index.js';
import '@smoothbricks/lmao/testing/bun';

console.log('[nx-cache-server] ttsc runtime transform warm');
