import type { ByteSample } from './model.js';

/**
 * A mechanical streaming adapter. Forward these measurements to the periodic reporter.
 * total must describe this stream's bytes; omit it when wire compression makes Content-Length incomparable.
 * Returning/throwing from the consumer closes the source iterator through for-await cleanup.
 */
export async function* measureByteStream(
  source: AsyncIterable<Uint8Array>,
  report: (sample: ByteSample) => void,
  options: { readonly direction: 'upload' | 'download'; readonly total?: number },
): AsyncGenerator<Uint8Array, void, unknown> {
  let transferred = 0;
  for await (const chunk of source) {
    transferred += chunk.byteLength;
    report(
      options.total === undefined
        ? { direction: options.direction, transferred }
        : { direction: options.direction, transferred, total: options.total },
    );
    yield chunk;
  }
}
