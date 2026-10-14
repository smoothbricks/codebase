#!/usr/bin/env node
import { runFailureSummary } from '../failure-summary.js';

process.exitCode = runFailureSummary(process.argv.slice(2));
