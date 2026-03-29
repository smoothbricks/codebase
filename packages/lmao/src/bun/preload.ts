/**
 * Bun runtime preload — registers Typia validation codegen plugin.
 *
 * Re-exports from @smoothbricks/validation which owns the Typia integration.
 * Packages that cannot depend on lmao should use @smoothbricks/validation/bun/preload directly.
 *
 * Usage in bunfig.toml:
 *   preload = ["@smoothbricks/lmao/bun/preload"]
 */

import '@smoothbricks/validation/bun/preload';
