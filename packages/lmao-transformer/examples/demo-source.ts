/**
 * LMAO Transformer Demo - Source File
 *
 * This file is compiled with the LMAO transformer which:
 * 1. Injects moduleMetadata into createModuleContext()
 * 2. Injects .line(N) after ctx.log.info/debug/warn/error()
 * 3. Injects .line(N) after ctx.ok() and ctx.err()
 * 4. Injects line number as 3rd argument to ctx.span()
 *
 * Run with: ./run-demo.sh
 */

import {
  convertToArrowTable,
  createModuleContext,
  createRequestContext,
  defineFeatureFlags,
  defineTagAttributes,
  InMemoryFlagEvaluator,
  labelInterner,
  moduleIdInterner,
  S,
} from '@smoothbricks/lmao';

// Define schema
const schema = defineTagAttributes({
  userId: S.category(),
  operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
  duration: S.number(),
  itemCount: S.number(),
});

// Define feature flags
const featureFlags = defineFeatureFlags({
  enableFeature: S.boolean().default(true).sync(),
});

// NO moduleMetadata here - the transformer will inject it!
// After transformation this will have:
//   moduleMetadata: {
//     gitSha: '<actual git SHA>',
//     packageName: '@smoothbricks/lmao-transformer',
//     packagePath: 'examples/demo-source.ts'
//   }
const { task } = createModuleContext({
  tagAttributes: schema,
});

// Task that demonstrates all the transformer injections
const processData = task('process-data', async (ctx, userId: string, items: string[]) => {
  // These log calls will have .line(N) injected by the transformer
  ctx.log.info('Starting data processing');
  ctx.log.debug('Received items to process');

  // Tag calls (no line injection, but shows in Arrow table)
  ctx.tag.userId(userId).operation('READ').itemCount(items.length);

  // Child span - transformer injects line number as 3rd argument
  const validationResult = await ctx.span('validate-items', async (childCtx) => {
    // These also get .line(N) injected
    childCtx.log.debug('Running validation');

    if (items.length === 0) {
      childCtx.log.warn('No items to validate');
      // ctx.err() gets .line(N) injected
      return childCtx.err('NO_ITEMS', { reason: 'empty array' });
    }

    childCtx.tag.duration(15.5);
    childCtx.log.info('Validation passed');

    // ctx.ok() gets .line(N) injected
    return childCtx.ok({ valid: true, count: items.length });
  });

  if (!validationResult.success) {
    ctx.log.error('Validation failed');
    return ctx.err('VALIDATION_FAILED', validationResult.error);
  }

  // More logging to show line numbers
  ctx.log.info('Processing items');
  ctx.tag.duration(42.0).operation('UPDATE');
  ctx.log.info('Processing complete');

  // Return both the result value AND the buffer for Arrow conversion
  // ctx.buffer is a getter that returns the SpanBuffer
  return ctx.ok({ processed: items.length, userId, _buffer: ctx.buffer });
});

// Main execution
async function main() {
  console.log('='.repeat(70));
  console.log('EXECUTING TRANSFORMED CODE');
  console.log('='.repeat(70));

  const requestCtx = createRequestContext(
    { requestId: 'req-demo-001', userId: 'user-abc' },
    featureFlags,
    new InMemoryFlagEvaluator({ enableFeature: true }),
    { environment: 'demo' },
  );

  console.log('\nRequest ID:', requestCtx.requestId);
  console.log('Trace ID:', requestCtx.traceId);
  console.log('\nExecuting task...\n');

  const result = await processData(requestCtx, 'user-123', ['item-1', 'item-2', 'item-3']);

  console.log('Task result:', result.success ? 'SUCCESS' : 'FAILED');
  if (result.success) {
    const { _buffer, ...value } = result.value;
    console.log('Value:', JSON.stringify(value));

    // Convert to Arrow and print full table
    const table = convertToArrowTable(_buffer, moduleIdInterner, labelInterner);

    console.log('\n' + '='.repeat(70));
    console.log('ARROW TABLE');
    console.log('='.repeat(70));

    console.log('\nSchema:');
    for (const field of table.schema.fields) {
      console.log(`  ${field.name}: ${field.type}`);
    }

    console.log(`\nTotal Rows: ${table.numRows}`);

    // Print all rows with all columns
    for (let i = 0; i < table.numRows; i++) {
      console.log(`\n--- Row ${i} ---`);
      for (const field of table.schema.fields) {
        const column = table.getChild(field.name);
        if (column) {
          const value = column.get(i);
          if (value !== null && value !== undefined) {
            const displayValue = typeof value === 'bigint' ? value.toString() : JSON.stringify(value);
            console.log(`  ${field.name}: ${displayValue}`);
          }
        }
      }
    }

    console.log('\n' + '='.repeat(70));
  } else {
    console.log('Error:', JSON.stringify(result.error));
  }
}

main().catch(console.error);
