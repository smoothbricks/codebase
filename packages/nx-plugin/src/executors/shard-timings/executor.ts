import { posix } from 'node:path';

import { timingsCoverage, untimedTestFilesReport } from '../../test-timings.js';
import type { ShardTimingsOptions } from './schema.js';

interface ShardTimingsContext {
  root: string;
  projectName?: string;
}

/**
 * Replays Bun's test discovery rules for the suite, running no test, and fails listing each discovered file the
 * `--timings` manifests do not name, with the command that refreshes them.
 */
export default async function shardTimingsExecutor(
  options: ShardTimingsOptions,
  context: ShardTimingsContext,
): Promise<{ success: boolean }> {
  const projectName = context.projectName;
  if (!projectName) {
    throw new Error('@smoothbricks/nx-plugin:shard-timings requires a project context.');
  }
  const result = await timingsCoverage(context.root, options);
  if (!result.ok) {
    console.error(`shard-timings: ${result.error}`);
    return { success: false };
  }
  const { coverage } = result;
  if (coverage.untimed.length > 0) {
    console.error(untimedTestFilesReport(projectName, options.cwd, coverage));
    return { success: false };
  }
  const manifest = posix.join(options.cwd, coverage.manifests[0] ?? '');
  console.log(`shard-timings: ${manifest} names all ${coverage.files.length} test files the shards run`);
  return { success: true };
}
