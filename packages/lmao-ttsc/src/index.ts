import type { TtscUnpluginOptions } from '@ttsc/unplugin';
import type { BunLikePlugin } from '@ttsc/unplugin/bun';
import * as bunAdapterModule from '@ttsc/unplugin/bun';

const bunAdapter: unknown = bunAdapterModule.default;

export function createBunTtscPlugin(options?: TtscUnpluginOptions): BunLikePlugin {
  if (typeof bunAdapter === 'function') {
    return bunAdapter(options);
  }
  if (
    typeof bunAdapter === 'object' &&
    bunAdapter !== null &&
    'default' in bunAdapter &&
    typeof bunAdapter.default === 'function'
  ) {
    return bunAdapter.default(options);
  }
  throw new TypeError('@ttsc/unplugin/bun did not export a Bun adapter factory');
}

export default createBunTtscPlugin;
export type { BunLikeBuild, BunLoader } from '@ttsc/unplugin/bun';
export type { BunLikePlugin, TtscUnpluginOptions };
