#!/usr/bin/env bun
/**
 * LMAO Transformer Demo
 *
 * This example demonstrates:
 * 1. Module context with moduleMetadata (as transformer would inject)
 * 2. Op execution with logging
 * 3. Conversion to Arrow table
 * 4. Printing the table contents
 *
 * Usage: bun run examples/transformer-demo.ts
 */

import {
  convertToArrowTable,
  defineFeatureFlags,
  defineLogSchema,
  defineModule,
  InMemoryFlagEvaluator,
  S,
  type SpanBuffer,
} from '../src/index.js';

// Define schema
const schema = defineLogSchema({
  userId: S.category(),
  operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
  duration: S.number(),
  itemCount: S.number(),
});

// Define feature flags
const featureFlags = defineFeatureFlags({
  enableCaching: S.boolean().default(true).sync(),
});

// Create module with defineModule - in production the transformer injects moduleMetadata
const demoModule = defineModule({
  moduleMetadata: {
    gitSha: 'abc123def456',
    packageName: '@smoothbricks/lmao',
    packagePath: 'examples/transformer-demo.ts',
  },
  logSchema: schema,
});

// Store reference to the root buffer for Arrow conversion
let rootBuffer: SpanBuffer | null = null;

// Op that logs things - in production the transformer injects .line(N) calls
const processItems = demoModule.task('process-items', async (ctx, userId: string, items: string[]) => {
  // Capture the buffer (accessing internal for demo purposes)
  rootBuffer = (ctx.log as unknown as { _buffer: SpanBuffer })._buffer;

  // These would have .line(N) injected by transformer
  ctx.log.info('Starting item processing');
  ctx.tag.userId(userId).operation('READ');

  // Child span - transformer would add line number as 3rd arg
  const result = await ctx.span('validate-items', async (childCtx) => {
    childCtx.log.debug('Validating items');
    childCtx.tag.itemCount(items.length);

    if (items.length === 0) {
      return childCtx.err('NO_ITEMS', { userId });
    }

    childCtx.tag.duration(15.5);
    return childCtx.ok({ valid: true, count: items.length });
  });

  if (!result.success) {
    ctx.log.error('Validation failed');
    return ctx.err('VALIDATION_FAILED', result.error);
  }

  ctx.log.info('Processing complete');
  ctx.tag.duration(42.0);
  return ctx.ok({ processed: items.length });
});

// Main
async function main() {
  console.log('='.repeat(70));
  console.log('LMAO TRANSFORMER DEMO - Arrow Table Output');
  console.log('='.repeat(70));

  // Setup
  const flagEvaluator = new InMemoryFlagEvaluator({ enableCaching: true });

  // Create trace context via module
  const traceCtx = demoModule.traceContext({ requestId: 'req-001', userId: 'user-123' }, featureFlags, flagEvaluator, {
    environment: 'demo',
  });

  console.log('\n--- EXECUTING OP ---\n');
  console.log('Request ID:', traceCtx.requestId);
  console.log('Trace ID:', traceCtx.traceId);

  // Execute op
  const result = await processItems(traceCtx, 'user-456', ['item-1', 'item-2', 'item-3']);

  console.log('\nOp result:', result.success ? 'SUCCESS' : 'FAILED');
  if (result.success) {
    console.log('Value:', JSON.stringify(result.value));
  }

  // Convert to Arrow and print
  console.log('\n--- ARROW TABLE ---\n');

  if (rootBuffer) {
    // convertToArrowTable now uses direct string access - no interners needed
    const table = convertToArrowTable(rootBuffer);

    console.log('Schema:');
    for (const field of table.schema.fields) {
      console.log(`  ${field.name}: ${field.type}`);
    }

    console.log(`\nRows: ${table.numRows}`);
    console.log('\nData:');

    // Print each row
    for (let i = 0; i < table.numRows; i++) {
      console.log(`\n  Row ${i}:`);
      for (const field of table.schema.fields) {
        const column = table.getChild(field.name);
        if (column) {
          const value = column.get(i);
          // Skip null values for cleaner output
          if (value !== null && value !== undefined) {
            // Handle BigInt serialization
            const displayValue = typeof value === 'bigint' ? value.toString() : JSON.stringify(value);
            console.log(`    ${field.name}: ${displayValue}`);
          }
        }
      }
    }
  } else {
    console.log('No buffer captured');
  }

  console.log(`\n${'='.repeat(70)}`);
}

main().catch(console.error);
