#!/usr/bin/env node
/// <reference types="node" />

import { packageRootFromModule, runCli } from './cli-trampoline.js';

process.exitCode = await runCli(process.argv.slice(2), {
  packageRoot: packageRootFromModule(import.meta.url),
});
