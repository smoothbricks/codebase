#!/usr/bin/env node
import { cargoToolchainPaths, NIX_STORE_ROOT, resolveProgram } from '../cargo-toolchain-paths.js';

/** The runtime input of a cached cargo target: stdout is the value Nx hashes (cargo-toolchain-paths.ts says what it names). */
process.stdout.write(
  cargoToolchainPaths({
    environment: process.env,
    storeRoot: NIX_STORE_ROOT,
    resolve: (program) => resolveProgram(program, process.env.PATH),
  }),
);
