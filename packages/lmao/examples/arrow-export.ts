#!/usr/bin/env bun
/**
 * Example: One trace, two outputs — stdout and an Apache Arrow table
 *
 * Demonstrates:
 * - Fanning a single trace run out to several tracers with `CompositeTracer`
 * - Printing the span tree to stdout with `StdioTracer`
 * - Collecting completed root buffers with `ArrayQueueTracer` and draining them
 *   into columnar Arrow tables via `convertSpanTreeToArrowTable`
 * - Reading columns back by their clean schema names (`userId`, `operation`, ...)
 *   plus system columns (`entry_type`, `message`)
 *
 * The tracer owns the buffer. Never stash `ctx.buffer` in a variable outside the
 * op: an op is a reusable definition that may run concurrently, so a shared
 * variable races, and the buffer strategy may recycle the buffer once the trace
 * completes.
 *
 * Run it:
 *   bun run examples/arrow-export.ts
 */

import {
  ArrayQueueTracer,
  CompositeTracer,
  convertSpanTreeToArrowTable,
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  JsBufferStrategy,
  S,
  StdioTracer,
} from '../src/node.js';

const schema = defineLogSchema({
  userId: S.category(),
  operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
  itemCount: S.number(),
});

const opContext = defineOpContext({ logSchema: schema });
const { defineOp } = opContext;

const processItems = defineOp('process-items', async (ctx, userId: string, items: string[]) => {
  ctx.tag.userId(userId).operation('READ').itemCount(items.length);
  ctx.log.info('processing {{itemCount}} items').itemCount(items.length);

  await ctx.span('validate-items', async (childCtx) => {
    childCtx.tag.operation('READ').itemCount(items.length);
    return childCtx.ok({ valid: true });
  });

  return ctx.ok({ processed: items.length });
});

// `CompositeTracerOptions` carries `delegates: Tracer<B>[]`, which pins B to this op
// context — so the buffer strategy must be built for the same concrete log schema
// rather than the loose default.
type ScenarioLogSchema = (typeof opContext)['logBinding']['logSchema'];

const tracerOptions = { bufferStrategy: new JsBufferStrategy<ScenarioLogSchema>(), createTraceRoot };

// StdioTracer prints each span as it completes; ArrayQueueTracer keeps the
// completed root buffers so they can be converted afterwards. CompositeTracer
// drives both from one trace run.
const stdio = new StdioTracer(opContext, tracerOptions);
const queued = new ArrayQueueTracer(opContext, tracerOptions);
const tracer = new CompositeTracer(opContext, { ...tracerOptions, delegates: [stdio, queued] });

async function main(): Promise<void> {
  await tracer.trace('process-items', processItems, 'user-123', ['alpha', 'beta', 'gamma']);

  // `drain()` hands over the queued root buffers and empties the queue, so the
  // next batch starts clean. Each root buffer converts with its children.
  const tables = queued.drain().map((rootBuffer) => convertSpanTreeToArrowTable(rootBuffer));

  for (const table of tables) {
    console.log(`\nArrow table: ${table.numRows} rows`);
    console.log(`Columns: ${table.names.join(', ')}`);

    // Read columns back by their clean schema names + system columns.
    const entryType = table.getChild('entry_type');
    const message = table.getChild('message');
    const userId = table.getChild('userId');

    for (let row = 0; row < table.numRows; row++) {
      console.log(
        `  row ${row}: entry_type=${entryType?.get(row)} message=${message?.get(row)} userId=${userId?.get(row)}`,
      );
    }
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
