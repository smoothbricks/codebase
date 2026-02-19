/**
 * LMAO Transformer Demo - Source File
 *
 * This file demonstrates the LMAO transformer injecting metadata into defineModule().
 *
 * Before transformation:
 *   defineModule({ logSchema: schema })
 *
 * After transformation:
 *   defineModule({
 *     metadata: { gitSha: '...', packageName: '...', packagePath: '...' },
 *     logSchema: schema
 *   })
 *
 * Run with: ./run-demo.sh
 */

import { defineLogSchema, defineModule, S } from '@smoothbricks/lmao';

// Define schema
const schema = defineLogSchema({
  userId: S.category(),
  operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
  duration: S.number(),
  itemCount: S.number(),
});

// NO metadata here - the transformer will inject it!
const module = defineModule({
  logSchema: schema,
});

// Op that demonstrates transformer line number injections
const _processData = module.op('process-data', async (ctx, userId: string, items: string[]) => {
  // These log calls will have .line(N) injected by the transformer
  ctx.log.info('Starting data processing');
  ctx.log.debug('Received items to process');

  // Tag calls (no line injection, but shows in Arrow table)
  ctx.tag.userId(userId).operation('READ').itemCount(items.length);

  // Child span - transformer injects line number as 3rd argument
  const result = await ctx.span('validate-items', async (childCtx) => {
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

  if (!result.success) {
    ctx.log.error('Validation failed');
    return ctx.err('VALIDATION_FAILED', result.error);
  }

  // More logging to show line numbers
  ctx.log.info('Processing items');
  ctx.tag.duration(42.0).operation('UPDATE');
  ctx.log.info('Processing complete');

  return ctx.ok({ processed: items.length, userId });
});
