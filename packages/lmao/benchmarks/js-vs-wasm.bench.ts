/**
 * Benchmark comparing JS (JsBufferStrategy) vs WASM (WasmBufferStrategy) performance.
 *
 * This benchmark measures two scenarios:
 *
 * **Cold start** - Initialization cost (realistic for Lambda, short-lived processes, first trace):
 *   - Simple trace throughput
 *   - Trace with tags - impact of column writes
 *   - Multiple log entries - log write throughput
 *
 * **Warm/steady-state** - Reuse performance (realistic for long-running services, high-throughput APIs):
 *   - Simple trace throughput
 *   - Trace with tags - impact of column writes
 *   - Nested spans - child span overhead
 *   - Multiple log entries - log write throughput
 *   - Memory reuse - freelist efficiency over many traces
 *   - Trace with tags + nested spans - combined overhead
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
let wasmTracer: TestTracer<typeof opContext>;
let wasmStrategy: WasmBufferStrategy<SchemaType>;

async function setup() {
  // JS tracer with JsBufferStrategy
  jsTracer = new TestTracer(opContext, {
    bufferStrategy: new JsBufferStrategy<SchemaType>({ capacity: 8 }),
    createTraceRoot,
  });

  // WASM tracer with WasmBufferStrategy
  wasmStrategy = (await WasmBufferStrategy.create({
    capacity: 8,
  })) as WasmBufferStrategy<SchemaType>;
  wasmTracer = new TestTracer(opContext, {
    bufferStrategy: wasmStrategy,
    createTraceRoot: createWasmTraceRootFactory(wasmStrategy.allocator),
  });
}

// =============================================================================
// Benchmark cases - JS only for now
// =============================================================================

summary(() => {
  group('Warm: Simple trace', () => {
    bench('JS', async () => {
      await jsTracer.trace('test', async (ctx) => ctx.ok('done'));
      jsTracer.clear();
    });

    bench('WASM', async () => {
      await wasmTracer.trace('test', async (ctx) => ctx.ok('done'));
      wasmTracer.clear();
    });
  });
});

summary(() => {
  group('Warm: Trace with tags (6 columns)', () => {
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

    bench('WASM', async () => {
      await wasmTracer.trace('test', async (ctx) => {
        ctx.tag.userId('user-123');
        ctx.tag.requestId('req-456');
        ctx.tag.latency(42.5);
        ctx.tag.statusCode(200);
        ctx.tag.success(true);
        ctx.tag.operation('READ');
        return ctx.ok('done');
      });
      wasmTracer.clear();
    });
  });
});

summary(() => {
  group('Warm: Nested spans (3 levels)', () => {
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

    bench('WASM', async () => {
      await wasmTracer.trace('level1', async (ctx) => {
        await ctx.span('level2', async (ctx2) => {
          await ctx2.span('level3', async (ctx3) => {
            return ctx3.ok('done');
          });
          return ctx2.ok('done');
        });
        return ctx.ok('done');
      });
      wasmTracer.clear();
    });
  });
});

summary(() => {
  group('Warm: Multiple log entries (50)', () => {
    bench('JS', async () => {
      await jsTracer.trace('test', async (ctx) => {
        for (let i = 0; i < 50; i++) {
          ctx.log.info(`message ${i}`);
        }
        return ctx.ok('done');
      });
      jsTracer.clear();
    });

    bench('WASM', async () => {
      await wasmTracer.trace('test', async (ctx) => {
        for (let i = 0; i < 50; i++) {
          ctx.log.info(`message ${i}`);
        }
        return ctx.ok('done');
      });
      wasmTracer.clear();
    });
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

    bench('WASM', async () => {
      for (let i = 0; i < 100; i++) {
        await wasmTracer.trace('test', async (ctx) => ctx.ok('done'));
      }
      wasmTracer.clear();
    });
  });
});

summary(() => {
  group('Warm: Trace with tags + nested spans', () => {
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

    bench('WASM', async () => {
      await wasmTracer.trace('parent', async (ctx) => {
        ctx.tag.userId('user-123');
        ctx.tag.operation('CREATE');

        await ctx.span('child', async (ctx2) => {
          ctx2.tag.latency(15.5);
          ctx2.tag.statusCode(201);
          return ctx2.ok('created');
        });

        return ctx.ok('done');
      });
      wasmTracer.clear();
    });
  });
});

summary(() => {
  group('Cold start: Simple trace', () => {
    bench('JS', async () => {
      // Create fresh tracer each iteration
      const tracer = new TestTracer(opContext, {
        bufferStrategy: new JsBufferStrategy<SchemaType>({ capacity: 8 }),
        createTraceRoot,
      });
      await tracer.trace('test', async (ctx) => ctx.ok('done'));
    });

    bench('WASM', async () => {
      // Create fresh strategy + tracer each iteration
      const strategy = (await WasmBufferStrategy.create({ capacity: 8 })) as WasmBufferStrategy<SchemaType>;
      const tracer = new TestTracer(opContext, {
        bufferStrategy: strategy,
        createTraceRoot: createWasmTraceRootFactory(strategy.allocator),
      });
      await tracer.trace('test', async (ctx) => ctx.ok('done'));
    });
  });
});

summary(() => {
  group('Cold start: Trace with tags (6 columns)', () => {
    bench('JS', async () => {
      const tracer = new TestTracer(opContext, {
        bufferStrategy: new JsBufferStrategy<SchemaType>({ capacity: 8 }),
        createTraceRoot,
      });
      await tracer.trace('test', async (ctx) => {
        ctx.tag.userId('user-123');
        ctx.tag.requestId('req-456');
        ctx.tag.latency(42.5);
        ctx.tag.statusCode(200);
        ctx.tag.success(true);
        ctx.tag.operation('READ');
        return ctx.ok('done');
      });
    });

    bench('WASM', async () => {
      const strategy = (await WasmBufferStrategy.create({ capacity: 8 })) as WasmBufferStrategy<SchemaType>;
      const tracer = new TestTracer(opContext, {
        bufferStrategy: strategy,
        createTraceRoot: createWasmTraceRootFactory(strategy.allocator),
      });
      await tracer.trace('test', async (ctx) => {
        ctx.tag.userId('user-123');
        ctx.tag.requestId('req-456');
        ctx.tag.latency(42.5);
        ctx.tag.statusCode(200);
        ctx.tag.success(true);
        ctx.tag.operation('READ');
        return ctx.ok('done');
      });
    });
  });
});

summary(() => {
  group('Cold start: Multiple log entries (50)', () => {
    bench('JS', async () => {
      const tracer = new TestTracer(opContext, {
        bufferStrategy: new JsBufferStrategy<SchemaType>({ capacity: 8 }),
        createTraceRoot,
      });
      await tracer.trace('test', async (ctx) => {
        for (let i = 0; i < 50; i++) {
          ctx.log.info(`message ${i}`);
        }
        return ctx.ok('done');
      });
    });

    bench('WASM', async () => {
      const strategy = (await WasmBufferStrategy.create({ capacity: 8 })) as WasmBufferStrategy<SchemaType>;
      const tracer = new TestTracer(opContext, {
        bufferStrategy: strategy,
        createTraceRoot: createWasmTraceRootFactory(strategy.allocator),
      });
      await tracer.trace('test', async (ctx) => {
        for (let i = 0; i < 50; i++) {
          ctx.log.info(`message ${i}`);
        }
        return ctx.ok('done');
      });
    });
  });
});

// =============================================================================
// Run benchmarks
// =============================================================================

await setup();

console.log('JS vs WASM Buffer Strategy Benchmark\n');
console.log('Setup:');
console.log('  - JS: JsBufferStrategy (GC-managed TypedArrays, capacity=8)');
console.log('  - WASM: WasmBufferStrategy (freelist allocation, capacity=8)');
console.log('  - Schema: 6 columns (2 category, 2 number, 1 boolean, 1 enum)');
console.log('\nScenarios:');
console.log('  - Cold start: Fresh tracer/strategy each iteration (initialization cost)');
console.log('  - Warm/steady-state: Reused tracer/strategy (reuse performance)');
console.log('');

await run({
  colors: true,
});
