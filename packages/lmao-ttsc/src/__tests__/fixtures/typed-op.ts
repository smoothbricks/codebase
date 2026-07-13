import { defineLogSchema, defineOpContext } from '@smoothbricks/lmao';

const { defineOp } = defineOpContext({
  logSchema: defineLogSchema({}),
});

export const typedOp = defineOp('native-fixture', async (ctx, value: number) => {
  ctx.log.info('native fixture log');
  ctx.log.debug('json-sensitive <>& \u2028\u2029');
  return ctx.ok(value + 1);
});
