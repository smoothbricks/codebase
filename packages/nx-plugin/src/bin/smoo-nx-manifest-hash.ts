#!/usr/bin/env node
import { hashVersionlessCrateManifests } from '../manifest-hash.js';

const args = process.argv.slice(2);
const [projectRoot = '.'] = args;
// An unrecognised flag must not be read as a project root: hashing a directory
// that does not exist would produce a constant digest, and Nx discards the
// failure, so the mistake would surface as a permanent cache hit rather than
// an error.
if (args.length > 1 || projectRoot.startsWith('-')) {
  process.stderr.write('usage: smoo-nx-manifest-hash [project-root]\n');
  process.exit(2);
}
try {
  process.stdout.write(`${await hashVersionlessCrateManifests(projectRoot, process.cwd())}\n`);
} catch (error) {
  process.stderr.write(`smoo-nx-manifest-hash: ${error instanceof Error ? error.message : String(error)}\n`);
  process.stderr.write(
    'Nx discards a failing hash command without failing the task, so the targets that exclude the raw crate manifests cannot see a manifest change until this succeeds. Restore the project crate manifests, then rerun the Nx target.\n',
  );
  process.exitCode = 1;
}
