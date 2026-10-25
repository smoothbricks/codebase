#!/usr/bin/env node
import { runCli } from './cli.js';
import { nodeLocks } from './locks-node.js';

// The same CLI under Node, for the Nx target and for checkouts that have not built the native binary.
process.exitCode = await runCli(process.argv.slice(2), nodeLocks, Date.now());
