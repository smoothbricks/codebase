import { watch } from 'node:fs';

/**
 * Resolves once `ready` holds: checked when the watch is armed and again on every change in `directory`, so a change
 * made before the watch exists is still seen. The wait ends on a filesystem event, never on a timer; one that never
 * comes is the leg's no-progress bound to report.
 */
export function whenChanged(directory: string, ready: () => boolean): Promise<void> {
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  const watcher = watch(directory, () => check());
  function check(): void {
    try {
      if (!ready()) return;
      watcher.close();
      resolve();
    } catch (error) {
      watcher.close();
      reject(error);
    }
  }
  watcher.once('error', (error) => {
    watcher.close();
    reject(error);
  });
  check();
  return promise;
}
