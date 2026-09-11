import { type ByteSample, comparableByteTotal } from './model.js';
import type { TimerPort } from './ports.js';

export type ByteProgressSink = (direction: ByteSample['direction'], transferred: number, total?: number) => void;
export interface ByteProgressReporter {
  /** Adapt an existing transport sample without cloning it. */
  report(sample: ByteSample): void;
  /** Numeric producer path: no sample object is constructed for a suppressed chunk. */
  reportBytes: ByteProgressSink;
  flush(): void;
  dispose(): void;
}

/** Accumulate primitives per attempt; create owned samples only at the notification boundary. */
export function createByteProgressReporter(options: {
  readonly timer: TimerPort;
  readonly intervalMs: number;
  readonly emit: (sample: ByteSample) => void;
}): ByteProgressReporter {
  if (!Number.isFinite(options.intervalMs) || options.intervalMs <= 0) {
    throw new RangeError('A progress interval must be a finite positive number.');
  }
  let uploadTransferred = -1;
  let downloadTransferred = -1;
  let uploadTotal: number | undefined;
  let downloadTotal: number | undefined;
  let uploadDirty = false;
  let downloadDirty = false;
  let cancelTimer: (() => void) | undefined;
  let disposed = false;
  let flushing = false;

  function flush(): void {
    if (flushing || disposed) return;
    cancelTimer?.();
    cancelTimer = undefined;
    // Capture both directions before callbacks: reentrant reports belong to the next flush.
    const sendUpload = uploadDirty;
    const sendDownload = downloadDirty;
    const uploaded = uploadTransferred;
    const downloaded = downloadTransferred;
    const uploadSize = uploadTotal;
    const downloadSize = downloadTotal;
    uploadDirty = false;
    downloadDirty = false;
    flushing = true;
    try {
      if (sendUpload) options.emit({ direction: 'upload', transferred: uploaded, total: uploadSize });
      if (sendDownload && !disposed)
        options.emit({ direction: 'download', transferred: downloaded, total: downloadSize });
    } finally {
      flushing = false;
    }
  }

  const reportBytes: ByteProgressSink = (direction, transferred, total) => {
    if (disposed || !Number.isSafeInteger(transferred) || transferred < 0) return;
    const comparableTotal = comparableByteTotal(transferred, total);
    if (direction === 'upload') {
      if (transferred < uploadTransferred || (transferred === uploadTransferred && comparableTotal === uploadTotal))
        return;
      uploadTransferred = transferred;
      uploadTotal = comparableTotal;
      uploadDirty = true;
    } else {
      if (
        transferred < downloadTransferred ||
        (transferred === downloadTransferred && comparableTotal === downloadTotal)
      )
        return;
      downloadTransferred = transferred;
      downloadTotal = comparableTotal;
      downloadDirty = true;
    }
    cancelTimer ??= options.timer.after(options.intervalMs, flush);
  };

  return {
    report(sample) {
      reportBytes(sample.direction, sample.transferred, sample.total);
    },
    reportBytes,
    flush,
    dispose() {
      disposed = true;
      cancelTimer?.();
      cancelTimer = undefined;
      uploadDirty = false;
      downloadDirty = false;
    },
  };
}
