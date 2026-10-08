import { existsSync } from 'node:fs';

const realPluginPath = new URL('./dist/index.js', import.meta.url);

if (!existsSync(realPluginPath)) {
  throw new Error(
    `The Nx plugin bootstrap product is missing: ${realPluginPath.pathname}. ` +
      'Enter the managed shell or run `ttsc -p packages/nx-plugin/tsconfig.lib.json --emit` before Nx.',
  );
}

const plugin = await import(realPluginPath.href);
export const createNodesV2 = plugin.createNodesV2;
export default plugin.default;
