import { defineLogSchema, defineOpContext } from '@smoothbricks/lmao';

const { defineOp } = defineOpContext({ logSchema: defineLogSchema({}) });

function traceProbeMessage(): string {
  return 'trace-mode probe';
}

export const probeOp = defineOp('trace-mode-probe', async (ctx) => {
  ctx.log.trace(traceProbeMessage());
  return ctx.ok(null);
});
