import type { TimingsShardedSuite } from '../../test-timings.js';

/** Options of `@smoothbricks/nx-plugin:shard-timings`: the suite whose `--timings` manifests must name every file. */
export type ShardTimingsOptions = Pick<TimingsShardedSuite, 'cwd' | 'args'>;
