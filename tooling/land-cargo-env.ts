#!/usr/bin/env bun
import { runBuildScriptEnvironmentDrift } from '@smoothbricks/nx-plugin/cargo-build-script-env';

process.exitCode = runBuildScriptEnvironmentDrift(process.argv.slice(2));
