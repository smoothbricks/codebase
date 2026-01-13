/**
 * Benchmark comparing JS (JsBufferStrategy) vs WASM (WasmBufferStrategy) performance.
 *
 * This benchmark measures:
 * 1. Simple trace throughput - baseline trace creation
 * 2. Trace with tags - impact of column writes
 * 3. Nested spans - child span overhead
 * 4. Multiple log entries - log write throughput
 * 5. Memory reuse - freelist efficiency over many traces
 *
 * Run with: bun run benchmarks/js-vs-wasm.bench.ts
 */

import { bench, group, run, summary } from 'mitata';

import { defineLogSchema, defineOpContext, S, TestTracer } from '../src/index.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { WasmBufferStrategy } from '../src/lib/wasm/WasmBufferStrategy.js';
import { createWasmTraceRootFactory } from '../src/lib/wasm/wasmTraceRoot.js';

// =============================================================================
// Schema definition with various column types
// =============================================================================

const schema = defineLogSchema({
  userId: S.category(),
  requestId: S.category(),
  latency: S.number(),
  statusCode: S.number(),
  success: S.boolean(),
  operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
});

const opContext = defineOpContext({
  logSchema: schema,
});

// Extract schema type for strategy generics
type SchemaType = typeof opContext.logBinding.logSchema;

// =============================================================================
// Tracer setup (async initialization for WASM)
// =============================================================================

let jsTracer: TestTracer<typeof opContext>;
let wasmTracer: TestTracer<typeof opContext> | null = null;
let wasmStrategy: WasmBufferStrategy<SchemaType> | null = null;

async function setup() {
  // JS tracer with JsBufferStrategy
  jsTracer = new TestTracer(opContext, {
    bufferStrategy: new JsBufferStrategy<SchemaType>({ capacity: 64 }),
    createTraceRoot,
  });

  // WASM tracer with WasmBufferStrategy
  try {
    wasmStrategy = (await WasmBufferStrategy.create({ capacity: 64 })) as WasmBufferStrategy<SchemaType>;
    wasmTracer = new TestTracer(opContext, {
      bufferStrategy: wasmStrategy,
      createTraceRoot: createWasmTraceRootFactory(wasmStrategy.allocator),
    });
    console.log('WASM benchmarks enabled');
  } catch (err) {
    console.warn('Failed to initialize WASM strategy:', err);
    wasmTracer = null;
    wasmStrategy = null;
  }
}

// =============================================================================
// Benchmark cases - JS only for now
// =============================================================================

summary(() => {
  group('Simple trace', () => {
    bench('JS', async () => {
      await jsTracer.trace('test', async (ctx) => ctx.ok('done'));
      jsTracer.clear();
    });

    if (wasmTracer) {
      bench('WASM', async () => {
        await wasmTracer!.trace('test', async (ctx) => ctx.ok('done'));
        wasmTracer!.clear();
      });
    }
  });
});

summary(() => {
  group('Trace with tags (6 columns)', () => {
    bench('JS', async () => {
      await jsTracer.trace('test', async (ctx) => {
        ctx.tag.userId('user-123');
        ctx.tag.requestId('req-456');
        ctx.tag.latency(42.5);
        ctx.tag.statusCode(200);
        ctx.tag.success(true);
        ctx.tag.operation('READ');
        return ctx.ok('done');
      });
      jsTracer.clear();
    });

    if (wasmTracer) {
      bench('WASM', async () => {
        await wasmTracer!.trace('test', async (ctx) => {
          ctx.tag.userId('user-123');
          ctx.tag.requestId('req-456');
          ctx.tag.latency(42.5);
          ctx.tag.statusCode(200);
          ctx.tag.success(true);
          ctx.tag.operation('READ');
          return ctx.ok('done');
        });
        wasmTracer!.clear();
      });
    }
  });
});

summary(() => {
  group('Nested spans (3 levels)', () => {
    bench('JS', async () => {
      await jsTracer.trace('level1', async (ctx) => {
        await ctx.span('level2', async (ctx2) => {
          await ctx2.span('level3', async (ctx3) => {
            return ctx3.ok('done');
          });
          return ctx2.ok('done');
        });
        return ctx.ok('done');
      });
      jsTracer.clear();
    });

    if (wasmTracer) {
      bench('WASM', async () => {
        await wasmTracer!.trace('level1', async (ctx) => {
          await ctx.span('level2', async (ctx2) => {
            await ctx2.span('level3', async (ctx3) => {
              return ctx3.ok('done');
            });
            return ctx2.ok('done');
          });
          return ctx.ok('done');
        });
        wasmTracer!.clear();
      });
    }
  });
});

summary(() => {
  group('Multiple log entries (50)', () => {
    bench('JS', async () => {
      await jsTracer.trace('test', async (ctx) => {
        for (let i = 0; i < 50; i++) {
          ctx.log.info(`message ${i}`);
        }
        return ctx.ok('done');
      });
      jsTracer.clear();
    });

    if (wasmTracer) {
      bench('WASM', async () => {
        await wasmTracer!.trace('test', async (ctx) => {
          for (let i = 0; i < 50; i++) {
            ctx.log.info(`message ${i}`);
          }
          return ctx.ok('done');
        });
        wasmTracer!.clear();
      });
    }
  });
});

summary(() => {
  group('Memory reuse (100 traces)', () => {
    bench('JS', async () => {
      for (let i = 0; i < 100; i++) {
        await jsTracer.trace('test', async (ctx) => ctx.ok('done'));
      }
      jsTracer.clear();
    });

    if (wasmTracer && wasmStrategy) {
      bench('WASM', async () => {
        for (let i = 0; i < 100; i++) {
          await wasmTracer!.trace('test', async (ctx) => ctx.ok('done'));
        }
        wasmTracer!.clear();
        wasmStrategy!.reset();
      });
    }
  });
});

summary(() => {
  group('Trace with tags + nested spans', () => {
    bench('JS', async () => {
      await jsTracer.trace('parent', async (ctx) => {
        ctx.tag.userId('user-123');
        ctx.tag.operation('CREATE');

        await ctx.span('child', async (ctx2) => {
          ctx2.tag.latency(15.5);
          ctx2.tag.statusCode(201);
          return ctx2.ok('created');
        });

        return ctx.ok('done');
      });
      jsTracer.clear();
    });

    if (wasmTracer) {
      bench('WASM', async () => {
        await wasmTracer!.trace('parent', async (ctx) => {
          ctx.tag.userId('user-123');
          ctx.tag.operation('CREATE');

          await ctx.span('child', async (ctx2) => {
            ctx2.tag.latency(15.5);
            ctx2.tag.statusCode(201);
            return ctx2.ok('created');
          });

          return ctx.ok('done');
        });
        wasmTracer!.clear();
      });
    }
  });
});

// =============================================================================
// Run benchmarks
// =============================================================================

await setup();

console.log('JS vs WASM Buffer Strategy Benchmark\n');
console.log('Setup:');
console.log('  - JS: JsBufferStrategy (GC-managed TypedArrays, capacity=64)');
if (wasmStrategy) {
  console.log('  - WASM: WasmBufferStrategy (freelist allocation, capacity=64)');
} else {
  console.log('  - WASM: DISABLED (initialization failed - see warning above)');
}
console.log('  - Schema: 6 columns (2 category, 2 number, 1 boolean, 1 enum)');
console.log('');

await run({
  colors: true,
});
