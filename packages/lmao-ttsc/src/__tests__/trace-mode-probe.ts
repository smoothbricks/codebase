/**
 * Child process of trace-mode-cache.integration.test.ts. Each argument is `<module>=<NODE_ENV>`: the probe sets
 * NODE_ENV, delivers that trace-mode fixture module through one @ttsc/unplugin transform cache, and prints whether each
 * output kept its trace statement. A cache compiles the whole project once and delivers its other modules from that
 * generation, so a second module is where reuse shows.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import {
  beginTtscTransformBuild,
  createTtscTransformCache,
  resolveOptions,
  type TtscTransformHooks,
  transformTtsc,
} from '@ttsc/unplugin/api';

const HOOKS: TtscTransformHooks = {
  addWatchFile: () => undefined,
  exactPath: true,
  watching: false,
  markVolatile: () => undefined,
};
const fixtures = fileURLToPath(new URL('./fixtures/trace-mode/', import.meta.url));
const project = fileURLToPath(new URL('./fixtures/trace-mode/tsconfig.json', import.meta.url));
const options = resolveOptions({ project, plugins: [{ transform: '@smoothbricks/lmao-ttsc/ttsc-plugin' }] });
const cache = createTtscTransformCache();
beginTtscTransformBuild(cache);
const kept: boolean[] = [];
for (const delivery of process.argv.slice(2)) {
  const [module, nodeEnv] = delivery.split('=');
  if (module === undefined || nodeEnv === undefined) throw new Error(`expected <module>=<NODE_ENV>, got ${delivery}`);
  process.env.NODE_ENV = nodeEnv;
  const file = `${fixtures}${module}.ts`;
  const result = await transformTtsc(file, await readFile(file, 'utf8'), options, undefined, cache, HOOKS);
  if (result === undefined) throw new Error(`no transform of ${file} under NODE_ENV=${nodeEnv}`);
  // The argument call survives only where the trace statement does.
  kept.push(result.code.includes('traceProbeMessage())'));
}
process.stdout.write(`${JSON.stringify(kept)}\n`);
