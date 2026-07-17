/**
 * Runtime capability probe for string codegen (`new Function`).
 *
 * Production workerd forbids code generation from strings as platform policy
 * (EvalError: Code generation from strings disallowed), while bun, node, and
 * miniflare allow it. The probe runs ONCE at module init and caches the
 * result; getColumnBufferClass/getColumnWriterClass use it to select between
 * the compiled (string codegen) and closure-composed (no-eval) materializers.
 */

/** Which class materializer builds generated ColumnBuffer/ColumnWriter classes. */
export type MaterializerMode = 'compiled' | 'closure';

function detectStringCodegen(): boolean {
  try {
    const probe: unknown = new Function('return true;');
    if (typeof probe !== 'function') return false;
    const result: unknown = probe();
    return result === true;
  } catch {
    return false;
  }
}

const stringCodegenSupported = detectStringCodegen();

let modeOverride: MaterializerMode | undefined;

/**
 * Whether this runtime allows `new Function` (probed once at module init).
 */
export function isStringCodegenSupported(): boolean {
  return stringCodegenSupported;
}

/**
 * The materializer that getColumnBufferClass/getColumnWriterClass will use:
 * the override when set, otherwise 'compiled' where string codegen is
 * supported and 'closure' where it is not (e.g. workerd).
 */
export function activeMaterializerMode(): MaterializerMode {
  return modeOverride ?? (stringCodegenSupported ? 'compiled' : 'closure');
}

/**
 * Force a materializer, overriding the probe. Pass `undefined` to restore
 * probe-based selection. Used by the parity suite to exercise the closure
 * path under runtimes that allow string codegen; consumers may also force
 * 'closure' to reproduce workerd behavior locally.
 *
 * Forcing 'compiled' in a runtime without string codegen support throws.
 */
export function setMaterializerModeOverride(mode: MaterializerMode | undefined): void {
  if (mode === 'compiled' && !stringCodegenSupported) {
    throw new Error('Cannot force the compiled materializer: this runtime disallows code generation from strings.');
  }
  modeOverride = mode;
}
