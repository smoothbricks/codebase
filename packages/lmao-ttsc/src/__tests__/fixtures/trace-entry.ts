import {
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  JsBufferStrategy,
  S,
  TestTracer,
} from '@smoothbricks/lmao/node';
import { extractFacts } from '@smoothbricks/lmao/testing';

const opContext = defineOpContext({ logSchema: defineLogSchema({ reason: S.category() }) });
const { defineOp } = opContext;

// An idempotent warm: a repeat is a no-op that production takes silently and a test counts.
const warmCache = defineOp('warm-cache', async (ctx, alreadyWarm: boolean) => {
  if (alreadyWarm) {
    ctx.log.trace('cache warm no-op').reason('already warm');
    return ctx.ok('unchanged');
  }
  return ctx.ok('warmed');
});

async function main(): Promise<void> {
  const tracer = new TestTracer(opContext, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  });
  await tracer.trace('first-warm', warmCache, false);
  await tracer.trace('repeat-warm', warmCache, true);
  await tracer.trace('second-repeat-warm', warmCache, true);
  const logFacts = tracer.rootBuffers.flatMap((root) => extractFacts(root).byNamespace('log'));
  process.stdout.write(`${JSON.stringify({ logFacts })}\n`);
}

await main();
