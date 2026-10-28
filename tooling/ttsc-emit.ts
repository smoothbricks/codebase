#!/usr/bin/env bun
/**
 * Emit the package in the working directory as nx-plugin's typescript-emit executor does (EMIT_OVERRIDES), for a
 * bootstrap that must build a dist before Nx can run: Nx's project graph loads the plugin this builds. Exits as ttsc did.
 */
import { EMIT_OVERRIDES } from '../packages/nx-plugin/src/executors/typescript-emit/emit-overrides.js';

const ttsc = Bun.spawn(['ttsc', '-p', 'tsconfig.lib.json', '--emit', ...EMIT_OVERRIDES], {
  stdio: ['inherit', 'inherit', 'inherit'],
});
process.exit(await ttsc.exited);
