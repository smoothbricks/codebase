import type { ByteSample } from './model.js';
import type { ByteProgressSink } from './progress.js';

/**
 * Pass through the original chunks and report cumulative byte counts, not per-chunk objects.
 * total must describe this stream's bytes; omit incomparable compressed Content-Lengths.
 * Early return/throw closes the source iterator. The transport owns aborting a blocked read.
 */
export async function* measureByteStream(
  source: AsyncIterable<Uint8Array>,
  reportBytes: ByteProgressSink,
  options: { readonly direction: ByteSample['direction']; readonly total?: number },
): AsyncGenerator<Uint8Array, void, unknown> {
  let transferred = 0;
  for await (const chunk of source) {
    transferred += chunk.byteLength;
    reportBytes(options.direction, transferred, options.total);
    yield chunk;
  }
  if (transferred === 0) reportBytes(options.direction, 0, options.total);
}
