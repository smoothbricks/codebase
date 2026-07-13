import {
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  JsBufferStrategy,
  S,
  TestTracer,
} from '@smoothbricks/lmao/node';
import { extractFacts } from '@smoothbricks/lmao/testing';

const schema = defineLogSchema({
  userId: S.category(),
  elapsedMs: S.number(),
  region: S.category(),
});

const opContext = defineOpContext({ logSchema: schema });
const { defineOp } = opContext;
const evaluationEffects: string[] = [];

function observeString(label: string, value: string): string {
  evaluationEffects.push(label);
  return value;
}

function observeNumber(label: string, value: number): number {
  evaluationEffects.push(label);
  return value;
}

const structuredLogOp = defineOp('structured-log-parity', async (ctx) => {
  ctx.log.info('loaded {userId} in {elapsedMs}ms', {
    userId: observeString('info.userId', 'user-42'),
    elapsedMs: observeNumber('info.elapsedMs', 17),
  });
  ctx.log.warn('literal braces: {{ok}} for {region}', {
    region: observeString('warn.region', 'iad'),
  });
  ctx.log.error('loaded {userId} in {elapsedMs}ms', {
    userId: observeString('error.userId', 'user-99'),
    elapsedMs: observeNumber('error.elapsedMs', 29),
  });

  const debugMessage = observeString('debug.message', 'debug-state-ready');
  ctx.log.debug(debugMessage);
  const traceMessage = observeString('trace.message', 'trace-state-ready');
  ctx.log.trace(traceMessage);

  return ctx.ok('complete');
});

async function main(): Promise<void> {
  evaluationEffects.length = 0;
  const tracer = new TestTracer(opContext, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  });
  const result = await tracer.trace('structured-log-parity-root', structuredLogOp);
  const root = tracer.rootBuffers[0];
  if (!root) throw new Error('Structured logging fixture produced no trace');
  const messageIdentityLane = root._messageIds ?? root._logHeaders;
  if (root._messageLayoutFamily !== 'mixed' || messageIdentityLane === undefined || root.message_values === undefined) {
    throw new Error(`Structured logging fixture requires mixed message storage, received ${root._messageLayoutFamily}`);
  }

  const facts = extractFacts(root, { tagFields: ['userId', 'elapsedMs', 'region'] });
  const logHeaders = Array.from(messageIdentityLane.subarray(0, root._writeIndex));
  const rawMessages = root.message_values.slice(0, root._writeIndex);
  process.stdout.write(
    `${JSON.stringify({
      result,
      effects: [...evaluationEffects],
      decodedFacts: [...facts.byNamespace('log'), ...facts.byNamespace('tag')],
      logHeaders,
      rawMessages,
    })}\n`,
  );
}

await main();
