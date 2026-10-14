#!/usr/bin/env bun
import { runFailureSummary } from '@smoothbricks/nx-plugin/failure-summary';

process.exitCode = runFailureSummary(process.argv.slice(2));
