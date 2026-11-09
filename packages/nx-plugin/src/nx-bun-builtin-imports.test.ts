import { expect, test } from 'bun:test';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import typia from 'typia';

/**
 * Guards the Bun-builtin hunk of `patches/nx@24.0.3.patch`. Nx 24.0.3 tells a
 * runtime module from a package with `node:module`'s `isBuiltin`. Under Node,
 * the runtime of an Nx daemon an editor's Nx language server starts, that is
 * false for Bun's `bun` and `bun:*` modules (under Bun it is true), so every
 * `bun:test` import in a test file went through the npm lookup, TypeScript
 * resolution and Node's `require.resolve` from its directory: in one graph
 * recompute of such a daemon serving a large Bun workspace, 3,738 lookups of each of `bun:test`,
 * `bun:test.js`, `bun:test.json` and `bun:test.node` and 41,382 missing
 * `node_modules` directories in 11 s. The patched `isBuiltinModuleImport`
 * knows them in every runtime.
 *
 * The installed Nx is loaded in Node, as that daemon runs it.
 *
 * Drop the hunk, and keep this test, once the Nx in use knows Bun's modules.
 */

const repoRoot = dirname(dirname(dirname(dirname(fileURLToPath(import.meta.url)))));
const BUN_MODULES = ['bun', 'bun:test', 'bun:sqlite', 'bun:ffi', 'bun:jsc'];
const PACKAGES = ['bunyan', 'bun-types', '@types/bun', 'bun/x'];

interface NodeAnswer {
  readonly builtin: Record<string, boolean>;
  readonly projects: Record<string, string | null>;
  readonly npmLookups: number;
}

/** Asks the installed Nx, in Node, how it classifies and resolves each import. */
function askNode(): NodeAnswer {
  const script = `
const { isBuiltinModuleImport, TargetProjectLocator } = require('nx/src/plugins/js/project-graph/build-dependencies/target-project-locator.js');
const names = ${JSON.stringify([...BUN_MODULES, ...PACKAGES])};
const bun = ${JSON.stringify(BUN_MODULES)};
const locator = new TargetProjectLocator({ consumer: { name: 'consumer', type: 'lib', data: { root: 'packages/consumer' } } });
// A non-builtin import takes the npm lookup first, then TypeScript resolution and require.resolve.
let npmLookups = 0;
const lookup = TargetProjectLocator.prototype.findNpmProjectFromImport;
TargetProjectLocator.prototype.findNpmProjectFromImport = function (...args) { npmLookups += 1; return lookup.apply(this, args); };
const projects = Object.fromEntries(bun.map((name) => [name, locator.findProjectFromImport(name, 'packages/consumer/src/a.test.ts') ?? null]));
process.stdout.write(JSON.stringify({ builtin: Object.fromEntries(names.map((name) => [name, isBuiltinModuleImport(name)])), projects, npmLookups }));
`;
  const run = Bun.spawnSync(['node', '-e', script], { cwd: repoRoot, stdout: 'pipe', stderr: 'pipe' });
  expect(run.exitCode, run.stderr.toString()).toBe(0);
  return typia.assert<NodeAnswer>(JSON.parse(run.stdout.toString()));
}

test('under Node, Bun runtime modules are builtins and resolve without a lookup; bun-named packages are not', () => {
  const answer = askNode();
  for (const name of BUN_MODULES) {
    expect(answer.builtin[name], name).toBe(true);
    expect(answer.projects[name], name).toBeNull();
  }
  for (const name of PACKAGES) expect(answer.builtin[name], name).toBe(false);
  expect(answer.npmLookups, 'npm lookups for bun:* imports').toBe(0);
});
