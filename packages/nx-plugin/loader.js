import { existsSync } from 'node:fs';

const realPluginPath = new URL('./dist/index.js', import.meta.url);

export const createNodesV2 = [
  '**/package.json',
  async (...args) => {
    if (!existsSync(realPluginPath)) {
      return [];
    }
    const plugin = await import(realPluginPath.href);
    return plugin.createNodesV2[1](...args);
  },
];

export default { createNodesV2 };
