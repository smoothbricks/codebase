#!/usr/bin/env node
import { runNxPublish } from '../nx-publish.js';

process.exitCode = await runNxPublish(process.argv.slice(2));
