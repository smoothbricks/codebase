import { defineLogSchema, defineOpContext } from '@smoothbricks/lmao';

const { defineOp } = defineOpContext({
  logSchema: defineLogSchema({}),
});

export const typedOp = defineOp('native-fixture', async (ctx, value: number) => {
  ctx.log.info('native fixture log');
  return ctx.ok(value + 1);
});
