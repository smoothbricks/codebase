/**
 * The value-import graph a bundler evaluates from one `src/` module.
 *
 * It follows value imports only: `import type` edges are erased before a
 * bundler ever sees them, so they cannot pull a module into a bundle.
 */

const SRC = new URL('../../', import.meta.url);

/** Value imports and side-effect imports; `import type` / `export type` are excluded. */
const VALUE_IMPORT = /^(?:import|export)(?!\s+type\b)[^'"]*?from\s*['"]([^'"]+)['"]/gm;
const SIDE_EFFECT_IMPORT = /^import\s*['"]([^'"]+)['"]/gm;

export interface ModuleGraph {
  /** Source-relative paths of every module the entry point evaluates. */
  readonly modules: ReadonlySet<string>;
  /** Bare specifiers reached from the entry point, keyed by the module naming each one. */
  readonly bare: ReadonlyMap<string, string>;
}

export async function runtimeModuleGraph(entry: string): Promise<ModuleGraph> {
  const modules = new Set<string>();
  const bare = new Map<string, string>();
  const queue: string[] = [entry];

  while (queue.length > 0) {
    const current = queue.pop();
    if (current === undefined || modules.has(current)) continue;
    modules.add(current);

    const url = new URL(current, SRC);
    const file = Bun.file(url);
    if (!(await file.exists())) throw new Error(`Import graph references a missing module: ${current}`);
    const source = await file.text();

    for (const match of [...source.matchAll(VALUE_IMPORT), ...source.matchAll(SIDE_EFFECT_IMPORT)]) {
      const specifier = match[1];
      if (!specifier.startsWith('.')) {
        if (!bare.has(specifier)) bare.set(specifier, current);
        continue;
      }
      // Published specifiers carry the emitted `.js`; the sources are `.ts`.
      queue.push(new URL(specifier.replace(/\.js$/, '.ts'), url).href.slice(SRC.href.length));
    }
  }

  return { modules, bare };
}
