import type { ByteSample } from './model.js';
import type { TimerPort } from './ports.js';

/** Throttles actual transport measurements. It never fabricates bytes from query status or elapsed time. */
export function createByteProgressReporter(options: {
  readonly timer: TimerPort;
  readonly intervalMs: number;
  readonly emit: (sample: ByteSample) => void;
}): { report(sample: ByteSample): void; flush(): void; dispose(): void } {
  if (!Number.isFinite(options.intervalMs) || options.intervalMs <= 0) {
    throw new RangeError('A progress interval must be a finite positive number.');
  }
  let upload: ByteSample | undefined;
  let download: ByteSample | undefined;
  let cancelTimer: (() => void) | undefined;
  let disposed = false;
  function flush(): void {
    cancelTimer?.();
    cancelTimer = undefined;
    if (disposed) return;
    const pendingUpload = upload;
    const pendingDownload = download;
    upload = undefined;
    download = undefined;
    if (pendingUpload) options.emit(pendingUpload);
    if (pendingDownload) options.emit(pendingDownload);
  }
  return {
    report(sample) {
      if (disposed) return;
      if (sample.direction === 'upload') upload = { ...sample };
      else download = { ...sample };
      cancelTimer ??= options.timer.after(options.intervalMs, flush);
    },
    flush,
    dispose() {
      disposed = true;
      cancelTimer?.();
      cancelTimer = undefined;
      upload = undefined;
      download = undefined;
    },
  };
}
