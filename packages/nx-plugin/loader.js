import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';

const realPluginPath = new URL('./dist/index.js', import.meta.url);

if (!existsSync(realPluginPath)) {
  throw new Error(
    `The Nx plugin bootstrap product is missing: ${realPluginPath.pathname}. ` +
      'Enter the managed shell or run `ttsc -p packages/nx-plugin/tsconfig.lib.json --emit` before Nx.',
  );
}

// Nx's real loader uses require(), so this boundary must stay synchronous in Node and Bun.
const plugin = createRequire(import.meta.url)('./dist/index.js');
export const createNodesV2 = plugin.createNodesV2;
export const createDependencies = plugin.createDependencies;
export const createMetadata = plugin.createMetadata;
export const preTasksExecution = plugin.preTasksExecution;
export const postTasksExecution = plugin.postTasksExecution;
export default plugin.default;
