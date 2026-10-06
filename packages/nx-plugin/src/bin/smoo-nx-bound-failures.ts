#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { cacheDirectoryForWorkspace, workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory.js';
import { workspaceRoot } from 'nx/src/utils/workspace-root.js';
import { describeVerdict, type FailedTask, failedTasksOfRun } from '../executors/bounded-exec/verdict.js';

// Judges the workspace's last Nx run (its cache directory's run.json) by the verdicts
// bounded-exec recorded: one line per failed task on stdout, `bound` or `failed` first.
// Exit 0 when the run failed and every failed task failed only on a bound; 1 when any failed
// task did not, or the run recorded no failed task; 2 when the run cannot be read.
if (process.argv.length !== 2) {
  process.stderr.write('usage: smoo-nx-bound-failures\n');
  process.exit(2);
}
let failed: FailedTask[];
try {
  const runJson: unknown = JSON.parse(
    await readFile(join(cacheDirectoryForWorkspace(workspaceRoot), 'run.json'), 'utf8'),
  );
  failed = await failedTasksOfRun(runJson, workspaceDataDirectoryForWorkspace(workspaceRoot));
} catch (error) {
  process.stderr.write(`smoo-nx-bound-failures: ${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(2);
}
for (const task of failed) {
  process.stdout.write(
    task.bound ? `bound  ${task.task}: ${describeVerdict(task.verdict)}\n` : `failed ${task.task}: ${task.reason}\n`,
  );
}
if (failed.length === 0) {
  process.stdout.write('failed: the last Nx run recorded no failed task\n');
}
process.exit(failed.length > 0 && failed.every((task) => task.bound) ? 0 : 1);
