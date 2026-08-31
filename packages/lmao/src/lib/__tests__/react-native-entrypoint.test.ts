/**
 * React Native entry point contract.
 *
 * The React Native lane is the ES lane: `./react-native` must hand back the
 * pure-TypedArray `traceRoot.es.ts` factory, and the TraceRoot it selects must
 * stay clear of `lib/wasm/` — Hermes has no `WebAssembly` global, so a single
 * value import from a shared module would make the lane unloadable on device.
 * Nothing in the bundle may name a `node:` or `bun:` builtin statically either;
 * those specifiers are unresolvable to Metro.
 *
 * The graph walk models what Hermes evaluates, so it follows value imports only:
 * `import type` edges are erased before a bundler ever sees them.
 */

import { describe, expect, it } from 'bun:test';
import {
  convertSpanTreeToArrowTable,
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  JsBufferStrategy,
  type LogSchema,
  S,
  TestTracer,
  type TracerOptions,
} from '../../react-native.js';
import { createTraceRoot as esCreateTraceRoot } from '../traceRoot.es.js';

const SRC = new URL('../../', import.meta.url);

/** Value imports and side-effect imports; `import type` / `export type` are excluded. */
const VALUE_IMPORT = /^(?:import|export)(?!\s+type\b)[^'"]*?from\s*['"]([^'"]+)['"]/gm;
const SIDE_EFFECT_IMPORT = /^import\s*['"]([^'"]+)['"]/gm;

interface ModuleGraph {
  /** Source-relative paths of every module the entry point evaluates. */
  readonly modules: ReadonlySet<string>;
  /** Bare specifiers reached from the entry point, keyed by the module naming each one. */
  readonly bare: ReadonlyMap<string, string>;
}

async function runtimeModuleGraph(entry: string): Promise<ModuleGraph> {
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

/** Threads the schema type through both option fields the way a React Native app would. */
function reactNativeTracerOptions<T extends LogSchema>(): TracerOptions<T> {
  return { bufferStrategy: new JsBufferStrategy<T>(), createTraceRoot };
}

describe('react-native entry point', () => {
  it('selects the ES TraceRoot lane', () => {
    expect(createTraceRoot).toBe(esCreateTraceRoot);
  });

  it('writes span rows through a tracer built on its factory', async () => {
    const opContext = defineOpContext({ logSchema: defineLogSchema({ screen: S.category() }) });
    const tracer = new TestTracer(opContext, { ...reactNativeTracerOptions() });

    const result = await tracer.trace('app-launch', async (ctx) => {
      ctx.tag.screen('home');
      return 'launched';
    });

    expect(result).toBe('launched');
    expect(tracer.rootBuffers.length).toBe(1);

    const table = convertSpanTreeToArrowTable(tracer.rootBuffers[0]);
    expect(table.numRows).toBeGreaterThan(0);
  });

  it('evaluates no WASM module on the TraceRoot lane it selects', async () => {
    const lane = await runtimeModuleGraph('lib/traceRoot.es.ts');

    expect([...lane.modules].filter((module) => module.startsWith('lib/wasm/'))).toEqual([]);
  });

  it('names no node: or bun: builtin in the bundle Metro resolves', async () => {
    const graph = await runtimeModuleGraph('react-native.ts');

    expect([...graph.bare.keys()].filter((specifier) => /^(?:node|bun):/.test(specifier)).sort()).toEqual([]);
  });

  it('evaluates exactly the ES entry point graph', async () => {
    const reactNative = await runtimeModuleGraph('react-native.ts');
    const es = await runtimeModuleGraph('es.ts');

    expect([...reactNative.modules].filter((module) => module !== 'react-native.ts').sort()).toEqual(
      [...es.modules].filter((module) => module !== 'es.ts').sort(),
    );
  });
});
