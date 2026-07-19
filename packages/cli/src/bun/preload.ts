/**
 * Bun runtime preload for consumers of `@smoothbricks/cli`.
 *
 * Re-exports the Typia/ttsc transform preload from `@smoothbricks/validation`.
 * External repos depend on the CLI package only — they must not need a direct
 * `@smoothbricks/validation` dependency just to run `smoo` against source.
 *
 * Usage:
 *   import '@smoothbricks/cli/bun/preload';
 *   // or bunfig.toml: preload = ["@smoothbricks/cli/bun/preload"]
 */
import '@smoothbricks/validation/bun/preload';
