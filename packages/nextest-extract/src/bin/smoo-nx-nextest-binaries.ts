#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import {
  cargoTargetDirectory,
  describeReadiness,
  ensureNextestBinaries,
  materializeBinaries,
  recordBuildKey,
} from '../nextest-binaries.js';

// smoo-nx-nextest-binaries target-dir
//     Prints the Cargo target directory of the workspace in cwd, the directory a runner remaps the build's to.
// smoo-nx-nextest-binaries record <build.key>
//     The test build's last step: records that this checkout's target directory holds the build the key names.
// smoo-nx-nextest-binaries ensure <binaries-metadata.json> <build.key> -- <build command...>
//     A runner's fixture: exits 0 when the target directory holds that build with every binary the metadata names,
//     and otherwise runs the build command (the test build's own `cargo nextest list`) once for the checkout's
//     runners, saying how long it took and what mbx served, and loudly when mbx compiled anything.
// stdout carries only target-dir's answer; everything said to a person goes to stderr.
const say = (line: string) => process.stderr.write(`smoo-nx-nextest-binaries: ${line}\n`);
const usage =
  'usage: smoo-nx-nextest-binaries target-dir | record <build.key> | ' +
  'ensure <binaries-metadata.json> <build.key> -- <build command...>';
const [mode, ...rest] = process.argv.slice(2);
try {
  if (mode === 'target-dir' && rest.length === 0) {
    process.stdout.write(`${await cargoTargetDirectory(process.cwd())}\n`);
  } else if (mode === 'record' && rest.length === 1 && rest[0] !== undefined) {
    const key = (await readFile(rest[0], 'utf8')).trim();
    if (key === '') throw new Error(`${rest[0]} names no build`);
    await recordBuildKey(await cargoTargetDirectory(process.cwd()), key);
  } else if (
    mode === 'ensure' &&
    rest.length >= 4 &&
    rest[2] === '--' &&
    rest[0] !== undefined &&
    rest[1] !== undefined
  ) {
    const command = rest.slice(3);
    const started = performance.now();
    const readiness = await ensureNextestBinaries(rest[0], rest[1], process.cwd(), () =>
      materializeBinaries(command, process.cwd()),
    );
    for (const line of describeReadiness(readiness, performance.now() - started)) say(line);
  } else {
    process.stderr.write(`${usage}\n`);
    process.exit(2);
  }
} catch (error) {
  say(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
