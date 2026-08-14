#!/usr/bin/env node
/// <reference types="node" />

import { loadNativeModule } from './native.js';

// The CLI lives in the Rust `cowshed-cli` crate and is reached through the same
// Node-API addon the library uses, so `bunx @smoothbricks/cowshed` and the
// standalone `cowshed` binary run identical dispatch code with no second
// per-platform artifact to publish.
process.exitCode = await loadNativeModule().runCli(process.argv.slice(2));
