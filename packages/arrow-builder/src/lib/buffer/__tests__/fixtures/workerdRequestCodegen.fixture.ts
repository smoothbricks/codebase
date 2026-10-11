/**
 * workerd's codegen policy in one process. workerd allows `new Function` while a
 * worker's modules evaluate and refuses it inside a request with
 * "EvalError: Code generation from strings disallowed for this context".
 *
 * 1. the workerd identity named by argv[2] is installed before arrow-builder evaluates:
 *    - `navigator`: navigator.userAgent reports 'Cloudflare-Workers' (global_navigator on);
 *    - `websocketpair`: no navigator and a `WebSocketPair` global (compatibility date before
 *      2022-03-21, or no_global_navigator);
 *    - `none`: the host's own globals, so nothing identifies workerd;
 * 2. arrow-builder evaluates with codegen still allowed (startup), so its module-init probe succeeds;
 * 3. `Function` then refuses to construct (request), and the ColumnBuffer and ColumnWriter classes
 *    are first built after that.
 *
 * Prints one JSON line describing what the request observed; a refused class build exits non-zero.
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

// Dynamic on purpose: arrow-builder must evaluate after the identity is installed
// and before codegen is refused, which a static import would defeat.
const arrow = await import('../../../../index.js');

function refuseCodegen(): never {
  throw new EvalError('Code generation from strings disallowed for this context');
}
globalThis.Function = new Proxy(globalThis.Function, { apply: refuseCodegen, construct: refuseCodegen });

const schema = new arrow.ColumnSchema({
  status: arrow.S.enum(['ok', 'error']),
  count: arrow.S.number(),
  label: arrow.S.category(),
  done: arrow.S.boolean(),
});
const buffer = arrow.createGeneratedColumnBuffer(schema, 8);
const writer = arrow.createColumnWriter(schema, buffer);
writer.nextRow().status('ok').count(1).label('a').done(true);
writer.nextRow().status('error').count(2).label('b').done(false);

process.stdout.write(
  JSON.stringify({
    codegenSupported: arrow.isStringCodegenSupported(),
    mode: arrow.activeMaterializerMode(),
    status: Array.from(buffer.status_values.subarray(0, 2)),
    count: Array.from(buffer.count_values.subarray(0, 2)),
    label: buffer.label_values.slice(0, 2),
    done: Array.from(buffer.done_values.subarray(0, 1)),
  }),
);
