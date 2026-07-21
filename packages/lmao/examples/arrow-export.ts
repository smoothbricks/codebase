#!/usr/bin/env bun
/**
 * Example: Exporting a span tree to an Apache Arrow table
 *
 * Demonstrates:
 * - Reading a completed trace's root buffer from a retaining tracer (`TestTracer.rootBuffers`)
 * - Converting the whole span tree (root + children) to a columnar Arrow table
 *   with `convertSpanTreeToArrowTable`
 * - Reading columns back by their clean schema names (`userId`, `operation`, ...)
 *   plus system columns (`entry_type`, `message`)
 *
 * (This replaces the old `transformer-demo.ts`: the build-time transformer it relied on
 * is not part of the shipped package, so this example focuses on the Arrow export that is.)
 *
 * Run it:
 *   bun run examples/arrow-export.ts
 */

import {
  convertSpanTreeToArrowTable,
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  JsBufferStrategy,
  S,
  TestTracer,
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

// `TestTracer` retains each completed root buffer in `rootBuffers` — that is how you
// get at a finished span tree. Don't stash `ctx.buffer` in a variable outside the op:
// an op is a reusable definition (potentially running concurrently), and the buffer
// strategy may recycle the buffer once the trace completes.
const tracer = new TestTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});

async function main(): Promise<void> {
  await tracer.trace('process-items', processItems, 'user-123', ['alpha', 'beta', 'gamma']);

  const rootBuffer = tracer.rootBuffers[0];
  if (!rootBuffer) {
    throw new Error('expected the tracer to retain a root buffer');
  }

  // Zero-copy conversion of the span tree into an Apache Arrow table.
  const table = convertSpanTreeToArrowTable(rootBuffer);

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

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
