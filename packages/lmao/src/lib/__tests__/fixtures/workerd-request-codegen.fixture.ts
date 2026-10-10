/**
 * workerd's codegen policy in one process. workerd allows `new Function` while a
 * worker's modules evaluate and refuses it inside a request with
 * "EvalError: Code generation from strings disallowed for this context".
 *
 * 1. the workerd identity named by argv[2] is installed before any lmao/arrow-builder module evaluates:
 *    - `navigator`: navigator.userAgent reports 'Cloudflare-Workers' (global_navigator on);
 *    - `websocketpair`: no navigator and a `WebSocketPair` global (compatibility date before
 *      2022-03-21, or no_global_navigator);
 *    - `none`: the host's own globals, so nothing identifies workerd;
 * 2. the modules evaluate with codegen still allowed (startup);
 * 3. `Function` then refuses to construct (request), and every class is first built after that.
 *    The WASM allocator and trace root are created at startup, as a worker instantiates its module.
 *
 * Prints one JSON line describing what the request observed.
 */
const identity = process.argv[2];
if (identity === 'navigator') {
  Object.defineProperty(globalThis, 'navigator', {
    value: { userAgent: 'Cloudflare-Workers' },
    configurable: true,
  });
} else if (identity === 'websocketpair') {
  if (!Reflect.deleteProperty(globalThis, 'navigator')) throw new Error('the host navigator is not configurable');
  Object.defineProperty(globalThis, 'WebSocketPair', {
    value: class WebSocketPair {},
    configurable: true,
  });
} else if (identity !== 'none') {
  throw new Error(`unknown workerd identity: ${String(identity)}`);
}

// Dynamic on purpose: the modules must evaluate after the identity is installed
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
const { EMPTY_SCOPE } = await import('../../spanBuffer.js');
const { createTraceId } = await import('../../traceId.js');
const { createWasmAllocator } = await import('../../wasm/wasmAllocator.js');
const { createWasmSpanBuffer } = await import('../../wasm/wasmSpanBuffer.js');
const { WasmTraceRoot } = await import('../../wasm/wasmTraceRoot.js');
const { createTestOpMetadata, createTestTracerOptions, TEST_TRACER } = await import('../test-helpers.js');

const allocator = await createWasmAllocator({ capacity: 8 });
const wasmTraceRoot = new WasmTraceRoot(allocator, createTraceId('workerd-request'), TEST_TRACER);

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

// lmao WASM: a WasmSpanBuffer class built inside the request.
const wasmSchema = defineLogSchema({ route: S.category(), status: S.number(), phase: S.enum(['start', 'done']) });
const wasmBuffer = createWasmSpanBuffer(
  wasmSchema,
  { allocator, capacity: 8 },
  wasmTraceRoot,
  EMPTY_SCOPE,
  createTestOpMetadata(),
  createTestOpMetadata({ name: 'span', line: 0 }),
);
// The WASM trace root is schema-agnostic, so the buffer's columns are reached by name.
for (const [column, value] of [
  ['route', '/orders'],
  ['status', 404],
  ['phase', 1],
] as const) {
  const setter: unknown = Reflect.get(wasmBuffer, column);
  if (typeof setter !== 'function') throw new TypeError(`Missing WASM column setter: ${column}`);
  Reflect.apply(setter, wasmBuffer, [2, value]);
}

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
    wasmSpan: {
      route: wasmBuffer.getColumnIfAllocated('route')?.[2],
      status: wasmBuffer.getColumnIfAllocated('status')?.[2],
      phase: wasmBuffer.getColumnIfAllocated('phase')?.[2],
    },
  }),
);
