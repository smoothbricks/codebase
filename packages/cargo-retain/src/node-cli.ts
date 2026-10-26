#!/usr/bin/env node
import { runCli } from './cli.js';
import { nodeLocks } from './locks-node.js';

// The same CLI under Node: the oracle the native-vs-Node differential and the in-process CLI test compare against.
process.exitCode = await runCli(process.argv.slice(2), nodeLocks, Date.now());
