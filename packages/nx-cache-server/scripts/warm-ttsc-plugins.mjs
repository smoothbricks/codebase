/**
 * Build prerequisite for the `test` target (see `test-warmup` in
 * `packages/nx-cache-server/package.json`), following the `wire-warmup`
 * pattern in `packages/cowshed`.
 *
 * The first load of this project through the compiler preload compiles the
 * typia native plugin (`ttsc: building source plugin ... can take several
 * minutes on a cold Go cache`). That compilation must never run inside the
 * 180s-idle-bounded `test`: importing the same production module graph here
 * pays it in a 600s build step, and the test then reuses the warm keys.
 *
 * Plain `.mjs` on purpose: the ttsc router only claims TypeScript files, so
 * this entry loads through Bun natively while every TypeScript module it
 * imports is still transformed through the real router.
 *
 * Run under the compiler preload ONLY — never add the trace preload here:
 * trace setup calls `afterAll()`, which throws outside `bun test`.
 *
 * Production modules only. Never import test files from this script:
 * warming must not execute tests.
 */

import '../src/index.js';

console.log('[nx-cache-server] ttsc runtime transform warm');
