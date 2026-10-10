/**
 * Materializer selection: whether generated ColumnBuffer/ColumnWriter (and lmao's
 * SpanLogger/ResultWriter) classes are built with string codegen (`new Function`)
 * or composed from closures.
 *
 * The question is not "may this module call `new Function` now" but "may every
 * context that builds a class call it". Classes are built lazily, per schema, on
 * first use — on a server that is usually inside a request. workerd allows code
 * generation while a worker's top-level module evaluates and refuses it inside a
 * request (EvalError: Code generation from strings disallowed for this context),
 * so a module-init probe answers yes there and every per-request build then throws.
 *
 * The decision is made once, at module init, so a process never mixes the two
 * class kinds for one schema:
 *   - workerd (Cloudflare's documented `navigator.userAgent === 'Cloudflare-Workers'`,
 *     or the `WebSocketPair` global) → 'closure', whatever the startup probe says;
 *   - otherwise a `new Function` probe decides: it fails where code generation is
 *     forbidden outright (strict CSP), and runtimes that allow it at init allow it
 *     everywhere (bun, node, browsers without CSP).
 *
 * Spec: specs/lmao/01b6_buffer_codegen_extension.md#smoo/lmao!n/buffer-codegen.materializer-selection
 */
//#region smoo/lmao!n/buffer-codegen.materializer-selection

/** Which class materializer builds generated ColumnBuffer/ColumnWriter classes. */
export type MaterializerMode = 'compiled' | 'closure';

/**
 * workerd refuses code generation inside requests even when startup allowed it.
 * Either of two documented Workers runtime signals identifies it:
 *   - `navigator.userAgent === 'Cloudflare-Workers'` (global_navigator, on by default
 *     since compatibility date 2022-03-21; miniflare runs workerd and reports the same);
 *   - the `WebSocketPair` global (Workers WebSockets API), defined under every
 *     compatibility date and flag. It identifies the isolates that have no `navigator`
 *     (compatibility date before 2022-03-21, or no_global_navigator).
 * `typeof navigator` guards the read because an isolate without global_navigator does not
 * declare the identifier at all, so even `navigator?.userAgent` would throw ReferenceError.
 */
const isWorkerd =
  (typeof navigator !== 'undefined' && navigator.userAgent === 'Cloudflare-Workers') ||
  typeof Reflect.get(globalThis, 'WebSocketPair') === 'function';

/** Whether `new Function` runs in the module-init context. */
function probeStringCodegen(): boolean {
  try {
    const probe: unknown = new Function('return true;');
    if (typeof probe !== 'function') return false;
    const result: unknown = probe();
    return result === true;
  } catch {
    // The refusal IS the answer: EvalError (or a CSP violation) means no codegen here.
    return false;
  }
}

const stringCodegenSupported = !isWorkerd && probeStringCodegen();

let modeOverride: MaterializerMode | undefined;

/**
 * Whether `new Function` may run in every context that builds a class: false in
 * workerd (refused inside requests) and wherever the module-init probe fails.
 */
export function isStringCodegenSupported(): boolean {
  return stringCodegenSupported;
}

/**
 * The materializer that getColumnBufferClass/getColumnWriterClass will use:
 * the override when set, otherwise 'compiled' where string codegen is
 * supported and 'closure' where it is not (workerd, CSP-restricted hosts).
 */
export function activeMaterializerMode(): MaterializerMode {
  return modeOverride ?? (stringCodegenSupported ? 'compiled' : 'closure');
}

/**
 * Force a materializer, overriding runtime selection. Pass `undefined` to restore
 * it. Used by the parity suites to exercise the closure path under runtimes that
 * allow string codegen; consumers may also force 'closure' to reproduce workerd
 * behavior locally.
 *
 * Forcing 'compiled' where string codegen is unsupported (including workerd) throws.
 */
export function setMaterializerModeOverride(mode: MaterializerMode | undefined): void {
  if (mode === 'compiled' && !stringCodegenSupported) {
    throw new Error(
      'Cannot force the compiled materializer: this runtime disallows code generation from strings in some contexts.',
    );
  }
  modeOverride = mode;
}
//#endregion smoo/lmao!n/buffer-codegen.materializer-selection
