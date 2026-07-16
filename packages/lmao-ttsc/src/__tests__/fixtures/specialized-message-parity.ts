import { createHash } from 'node:crypto';
import {
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  JsBufferStrategy,
  TestTracer,
} from '@smoothbricks/lmao/node';
import { extractFacts } from '@smoothbricks/lmao/testing';

const opContext = defineOpContext({ logSchema: defineLogSchema({}) });
const effects: number[] = [];

function observeDynamic(index: number): string {
  effects.push(index);
  return `dynamic-${index.toString().padStart(2, '0')}`;
}

const specializedParityOp = opContext.defineOp('specialized-message-parity', (ctx) => {
  ctx.log.info('static-00');
  ctx.log.info('static-01');
  ctx.log.info('static-02');
  ctx.log.info('static-03');
  ctx.log.info('static-04');
  ctx.log.info('static-05');
  ctx.log.info('static-06');
  ctx.log.info('static-07');
  ctx.log.info('static-08');
  ctx.log.info('static-09');
  ctx.log.info('static-10');
  ctx.log.info('static-11');
  ctx.log.info('static-12');
  ctx.log.info('static-13');
  ctx.log.info('static-14');
  ctx.log.info('static-15');
  ctx.log.info('static-16');
  ctx.log.info('static-17');
  ctx.log.info('static-18');
  ctx.log.info('static-19');
  ctx.log.info('static-20');
  ctx.log.info('static-21');
  ctx.log.info('static-22');
  ctx.log.info('static-23');
  ctx.log.info('static-24');
  ctx.log.info('static-25');
  ctx.log.info('static-26');
  ctx.log.info('static-27');
  ctx.log.info('static-28');
  ctx.log.info('static-29');
  ctx.log.info('static-30');
  ctx.log.debug(observeDynamic(0));
  ctx.log.debug(observeDynamic(1));
  ctx.log.debug(observeDynamic(2));
  ctx.log.debug(observeDynamic(3));
  ctx.log.debug(observeDynamic(4));
  ctx.log.debug(observeDynamic(5));
  ctx.log.debug(observeDynamic(6));
  ctx.log.debug(observeDynamic(7));
  ctx.log.debug(observeDynamic(8));
  ctx.log.debug(observeDynamic(9));
  ctx.log.debug(observeDynamic(10));
  ctx.log.debug(observeDynamic(11));
  ctx.log.debug(observeDynamic(12));
  ctx.log.debug(observeDynamic(13));
  ctx.log.debug(observeDynamic(14));
  ctx.log.debug(observeDynamic(15));
  ctx.log.debug(observeDynamic(16));
  ctx.log.debug(observeDynamic(17));
  ctx.log.debug(observeDynamic(18));
  ctx.log.debug(observeDynamic(19));
  ctx.log.debug(observeDynamic(20));
  ctx.log.debug(observeDynamic(21));
  ctx.log.debug(observeDynamic(22));
  ctx.log.debug(observeDynamic(23));
  ctx.log.debug(observeDynamic(24));
  ctx.log.debug(observeDynamic(25));
  ctx.log.debug(observeDynamic(26));
  ctx.log.debug(observeDynamic(27));
  ctx.log.debug(observeDynamic(28));
  ctx.log.debug(observeDynamic(29));
  ctx.log.debug(observeDynamic(30));
  return ctx.ok('complete');
});
specializedParityOp.callsitePlan.SpanBufferClass.stats.capacity = 64;

const tracer = new TestTracer(opContext, { bufferStrategy: new JsBufferStrategy(), createTraceRoot });
const result = await tracer.trace('specialized-message-parity-root', specializedParityOp);
const root = tracer.rootBuffers[0];
if (!root) throw new Error('Specialized message parity fixture produced no root');
if (root._messageLayoutFamily !== 'mixed' || root.message_values === undefined) {
  throw new Error('Specialized parity requires mixed-family raw message storage');
}
const plan = specializedParityOp.callsitePlan;
const messageIdentityStorage = plan.arrowExposure.messageIdentityStorage;
const rawMessageSentinels = [root.message_values[2] ?? null, root.message_values[33] ?? null];
const logFacts = extractFacts(root).byNamespace('log');
const segments = [
  {
    capacity: root._capacity,
    writeIndex: root._writeIndex,
    physicalLayout: plan.messagePhysicalLayout,
  },
];
const checksum = createHash('sha256').update(JSON.stringify(logFacts)).digest('hex');
process.stdout.write(
  `${JSON.stringify({
    result,
    effects,
    logFacts,
    checksum,
    segments,
    physicalLayout: plan.messagePhysicalLayout,
    messageIdentityStorage,
    hasRowHeaders: '_rowHeaders' in root,
    hasEntryType: 'entry_type' in root,
    hasLogHeaders: '_logHeaders' in root,
    hasMessageIds: '_messageIds' in root,
    hasMessageValidity: 'message_nulls' in root,
    hasRawMessages: 'message_values' in root,
    rawMessageSentinels,
  })}\n`,
);
