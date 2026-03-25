/**
 * Bun runtime preload — registers Typia validation codegen plugin.
 *
 * This MUST run before any code that calls typia.createValidate() etc.
 * It transforms those calls into actual validation code at import time.
 *
 * Usage in bunfig.toml (top-level, applies to all bun execution):
 *   preload = ["@smoothbricks/lmao/bun/preload"]
 *
 * For bun build (bundler), pass UnpluginTypia() in plugins instead — preload
 * does not apply to `bun build`.
 */

import UnpluginTypia from '@typia/unplugin/bun';
import { plugin } from 'bun';

plugin(UnpluginTypia());
