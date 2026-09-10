#!/usr/bin/env node
import { hashVersionlessManifests, hashVersionlessWorkspaceManifests } from '../manifest-hash.js';

const WORKSPACE_FLAG = '--workspace';

const args = process.argv.slice(2);
const [target = '.'] = args;
// An unrecognised flag must not be read as a project root: hashing a directory
// that does not exist would produce a constant digest, and Nx discards the
// failure, so the mistake would surface as a permanent cache hit rather than
// an error.
if (args.length > 1 || (target.startsWith('-') && target !== WORKSPACE_FLAG)) {
  process.stderr.write(`usage: smoo-nx-manifest-hash [project-root | ${WORKSPACE_FLAG}]\n`);
  process.exit(2);
}
try {
  const digest =
    target === WORKSPACE_FLAG
      ? await hashVersionlessWorkspaceManifests(process.cwd())
      : await hashVersionlessManifests(target, process.cwd());
  process.stdout.write(`${digest}\n`);
} catch (error) {
  process.stderr.write(`smoo-nx-manifest-hash: ${error instanceof Error ? error.message : String(error)}\n`);
  process.stderr.write(
    'Nx discards a failing hash command without failing the task, so the targets that exclude the raw manifests cannot see a manifest change until this succeeds. Restore the project and workspace manifests, then rerun the Nx target.\n',
  );
  process.exitCode = 1;
}
