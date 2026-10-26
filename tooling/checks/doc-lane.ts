/// <reference types="bun" />
// The doc lane's check: `[doc] check` in main's .cowshed.toml. cowshed doc-commit runs it in main with the edit's paths
// appended, before it stages them, and refuses the edit when it exits nonzero, showing what it printed.
//
//   spec-links.ts         the edit leaves no code reference in a spec naming nothing in the tree the commit holds
//   compiled-in-docs.ts   the edit carries no file a crate compiles in, which a code task hashes
//
// Both run, in this order, so one refusal shows every reason.
import { join } from 'node:path';

const CHECKS = ['spec-links.ts', 'compiled-in-docs.ts'];
const paths = process.argv.slice(2);
const statuses = CHECKS.map(
  (check) =>
    Bun.spawnSync([process.execPath, join(import.meta.dir, check), ...paths], { stdout: 'inherit', stderr: 'inherit' })
      .exitCode,
);
process.exit(statuses.every((status) => status === 0) ? 0 : 1);
