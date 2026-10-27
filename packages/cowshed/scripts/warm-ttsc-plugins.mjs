/**
 * The compiler-fixture producer for the `wire-test` and `declaration-model-test`
 * targets: their `fixtureCommand` (see `packages/cowshed/package.json`) runs it
 * as the bounded-exec fixture segment, a build interval ahead of the tests.
 *
 * The first load of each project through the compiler preload compiles the
 * typia native plugin (`ttsc: building source plugin ... can take several
 * minutes on a cold Go cache`). That compilation must never spend the tests'
 * progress bound: importing the same non-test module graph here pays it in
 * the fixture interval, and the test then reuses the warm keys.
 *
 * Plain `.mjs` on purpose: the ttsc router only claims TypeScript files, so
 * this entry loads through Bun natively while every TypeScript module it
 * imports is still transformed through the real router. A `.ts` entry under
 * `scripts/` would instead crash the load: it belongs to no tsconfig program
 * (`tsconfig.lib.json` only includes the `src` tree), and the transform
 * reports "did not return output" for files outside the program.
 *
 * Run under the compiler preload ONLY — never add the trace preload here:
 * trace setup calls `afterAll()`, which throws outside `bun test`.
 *
 * Production modules only. Never import test files from this script:
 * warming must not execute tests.
 */

import '../src/types.js';

console.log('[cowshed] ttsc runtime transform warm');
