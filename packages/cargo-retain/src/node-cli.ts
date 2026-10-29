#!/usr/bin/env node
import { runCli } from './cli.js';
import { nodeLocks } from './locks-node.js';

// The published CLI: package.json#bin declares this entry, so npm links it and runs it under Node through the shebang.
// It is also the process the Nx hook's automatic pass runs in (launch-node.ts), and the oracle the native-vs-Node
// differential and the in-process CLI test compare against.
process.exitCode = await runCli(process.argv.slice(2), nodeLocks, Date.now());
