import type { ComposedRuntime, EventHandle, RuntimeDiagnostic } from './composition.js';

/** Sanitized typed facts only. Raw Error objects, stack traces, ports, headers and credentials never enter this bridge. */
export function bindRuntimeDiagnostics<T>(
  runtime: ComposedRuntime,
  event: EventHandle<T>,
  decode: (diagnostic: RuntimeDiagnostic) => T | undefined,
): () => void {
  runtime.assertOwner(event.ownerToken);
  if (runtime.mode === 'replay') return () => {};
  let lastWave = -1;
  return runtime.observeDiagnostics((diagnostic) => {
    // A diagnostic reducer failing in its own successor wave cannot create an unbounded failure loop.
    if (lastWave === diagnostic.wave) return;
    lastWave = diagnostic.wave;
    const payload = decode(diagnostic);
    if (payload !== undefined && !runtime.disposed) runtime.publish(event, payload);
  });
}
