/**
 * workerd's codegen policy in one process. workerd allows `new Function` while a
 * worker's modules evaluate and refuses it inside a request with
 * "EvalError: Code generation from strings disallowed for this context".
 *
 * 1. navigator reports workerd's identity before any lmao/arrow-builder module evaluates;
 * 2. the modules evaluate with codegen still allowed (startup);
 * 3. `Function` then refuses to construct (request), and every class is first built after that.
 *
 * Prints one JSON line describing what the request observed.
 */
Object.defineProperty(globalThis, 'navigator', {
  value: { userAgent: 'Cloudflare-Workers' },
  configurable: true,
});

// Dynamic on purpose: the modules must evaluate after navigator reports workerd
// and before codegen is refused, which static imports would defeat.
const {
  activeMaterializerMode,
  ColumnSchema,
  createColumnWriter,
  createGeneratedColumnBuffer,
  S: ArrowS,
} = await import('@smoothbricks/arrow-builder');
const { defineOpContext } = await import('../../defineOpContext.js');
const { S } = await import('../../schema/builder.js');
const { defineLogSchema } = await import('../../schema/defineLogSchema.js');
const { TestTracer } = await import('../../tracers/TestTracer.js');
const { createTestTracerOptions } = await import('../test-helpers.js');

function refuseCodegen(): never {
  throw new EvalError('Code generation from strings disallowed for this context');
}
globalThis.Function = new Proxy(globalThis.Function, { apply: refuseCodegen, construct: refuseCodegen });

let codegenRefused = false;
try {
  new Function('return 1;');
} catch (error) {
  codegenRefused = error instanceof EvalError;
}

// arrow-builder: ColumnBuffer and ColumnWriter classes built inside the request.
const columns = new ColumnSchema({ count: ArrowS.number(), phase: ArrowS.enum(['start', 'done']) });
const columnBuffer = createGeneratedColumnBuffer(columns, 8);
columnBuffer.count(0, 1.5).phase(0, 1);
const writerBuffer = createGeneratedColumnBuffer(columns, 8);
createColumnWriter(columns, writerBuffer).nextRow().count(2.5).phase('done');

// lmao: SpanBuffer, SpanLogger, TagWriter and ResultWriter classes built inside the request.
const logSchema = defineLogSchema({ status: S.number(), phase: S.enum(['start', 'done']) });
const context = defineOpContext({ logSchema });
const tracer = new TestTracer(context, createTestTracerOptions());
const result = tracer.trace('request', (ctx) => {
  ctx.tag.phase('start');
  ctx.log.info('handled').status(202);
  return ctx.ok(7).status(200).phase('done');
});
const span = tracer.rootBuffers[0];

process.stdout.write(
  JSON.stringify({
    codegenRefused,
    mode: activeMaterializerMode(),
    columnBuffer: { count: columnBuffer.count_values[0], phase: columnBuffer.phase_values[0] },
    columnWriter: { count: writerBuffer.count_values[0], phase: writerBuffer.phase_values[0] },
    span: {
      value: result.success ? result.value : null,
      tagPhase: span.phase_values[0],
      resultStatus: span.status_values[1],
      resultPhase: span.phase_values[1],
      logMessage: span.message_values?.[2],
      logStatus: span.status_values[2],
    },
  }),
);
