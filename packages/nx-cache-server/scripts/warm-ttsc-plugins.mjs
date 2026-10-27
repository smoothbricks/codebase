/**
 * The compiler-fixture producer for the `test` target: its `fixtureCommand`
 * (see `packages/nx-cache-server/package.json`) runs it as the bounded-exec
 * fixture segment, a build interval ahead of the tests, following the
 * `wire-test` fixture in `packages/cowshed`.
 *
 * Each TypeScript project the test loads through the compiler preload keys its
 * own native plugin build (`ttsc: building source plugin ... can take several
 * minutes on a cold Go cache`). That compilation must never spend the test's
 * progress bound, so this script imports a module from every project the
 * test's preloads and sources load, paying each key in the fixture interval.
 * The test then reuses the warm keys. A preload added to `bunfig.toml` that
 * loads another project needs its modules imported here too.
 *
 * Plain `.mjs` on purpose: the ttsc router only claims TypeScript files, so
 * this entry loads through Bun natively while every TypeScript module it
 * imports is still transformed through the real router.
 *
 * Run under the compiler preload ONLY — never add the trace preload itself:
 * its setup calls `afterAll()`, which throws outside `bun test`. That preload
 * and every other workspace library load from their built `dist`, which the
 * router never claims, so only this project's own key needs warming.
 *
 * Never import test files from this script: warming must not execute tests.
 */

import '../src/index.js';

console.log('[nx-cache-server] ttsc runtime transform warm');
