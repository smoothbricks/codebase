import { defineLogSchema, defineOpContext } from '@smoothbricks/lmao';

const { defineOp } = defineOpContext({ logSchema: defineLogSchema({}) });

function traceProbeMessage(): string {
  return 'trace-mode probe, second module';
}

export const secondProbeOp = defineOp('trace-mode-probe-second', async (ctx) => {
  ctx.log.trace(traceProbeMessage());
  return ctx.ok(null);
});
