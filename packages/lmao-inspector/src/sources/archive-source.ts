/**
 * Archive source fetches historical Arrow IPC data via HTTP for offline trace analysis.
 * Returns async iterables of Arrow IPC buffers for progressive loading.
 *
 * The archive is a remote peer that streams its answer, so every wait on it is an `idle` wait
 * (specs/lmao/01r_transient_errors_and_retry.md, "TypeScript: deadlines"): the answer's head, and each read of its body,
 * must arrive within the remote-http bound (`REMOTE_HTTP_MS`; smoothbricks specs/cowshed/05_gateway.md, "Deadlines").
 * An archive silent for longer ends the read with `SilentError`, naming the bytes it had sent; the time the consumer
 * spends between reads is never counted.
 */

import { Ok } from '@smoothbricks/lmao';
import {
  idle,
  NOTHING,
  type Progress,
  REMOTE_HTTP_MS,
  received,
  SYSTEM_TIMERS,
  type Timers,
} from '@smoothbricks/lmao/errors/Deadline';

export interface ArchiveSource {
  /** Fetch Arrow IPC buffers for a given time range */
  fetchRange(startTime: number, endTime: number): AsyncIterable<Uint8Array>;
}

/**
 * Create an archive source that fetches historical Arrow chunks via HTTP GET.
 * The server returns Arrow IPC files for the requested time range.
 */
//#region smoo/lmao!n/inspector-archive-source.create
export function createArchiveSource(baseUrl: string, timers: Timers = SYSTEM_TIMERS): ArchiveSource {
  const holder = `archive ${baseUrl}`;
  return {
    async *fetchRange(startTime: number, endTime: number): AsyncIterable<Uint8Array> {
      const url = `${baseUrl}?start=${startTime}&end=${endTime}`;
      // Ending our side of the request ends the head's wait and every later read of the body.
      const request = new AbortController();
      let last: Progress = NOTHING;
      /** One wait on the archive, ended `SILENT` once it has sent nothing for the remote-http bound. */
      const heard = async <T>(op: string, wait: () => Promise<T>): Promise<T> => {
        const answer = await idle(
          op,
          holder,
          REMOTE_HTTP_MS,
          async (silence) => {
            silence.progress(last);
            const end = () => request.abort(silence.signal.reason);
            silence.signal.addEventListener('abort', end, { once: true });
            try {
              return new Ok(await wait());
            } catch (error) {
              // Past the bound, the request's abort is this wait's own end: the failure is the answer.
              throw silence.signal.aborted ? silence.signal.reason : error;
            } finally {
              silence.signal.removeEventListener('abort', end);
            }
          },
          timers,
        );
        if (!answer.isOk()) throw answer.error;
        return answer.value;
      };

      const response = await heard('archive fetch', () => fetch(url, { signal: request.signal }));

      if (!response.ok) {
        throw new Error(`Archive fetch failed: ${response.status} ${response.statusText}`);
      }

      const reader = response.body?.getReader();
      if (!reader) {
        throw new Error('Archive response has no readable body');
      }

      // Stream response body as chunks — each chunk is a complete Arrow IPC buffer
      // separated by the Arrow IPC continuation marker (0xFFFFFFFF)
      let buffer = new Uint8Array(0);

      try {
        while (true) {
          const { done, value } = await heard('archive read', () => reader.read());
          if (done) break;
          last = received(last, value.length);

          // Accumulate into buffer
          const combined = new Uint8Array(buffer.length + value.length);
          combined.set(buffer);
          combined.set(value, buffer.length);
          buffer = combined;

          // Yield complete Arrow IPC messages as we find them
          // Arrow IPC stream format: each message starts with continuation (4 bytes 0xFF)
          // followed by metadata length (4 bytes), metadata, then body
          while (buffer.length >= 8) {
            const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);

            // Check for Arrow IPC continuation marker
            if (view.getInt32(0, true) !== -1) {
              // Not a valid Arrow IPC stream — yield entire buffer as single batch
              yield buffer;
              buffer = new Uint8Array(0);
              break;
            }

            const metadataLength = view.getInt32(4, true);
            if (metadataLength === 0) {
              // End-of-stream marker (EOS)
              buffer = buffer.slice(8);
              continue;
            }

            // Pad metadata to 8-byte alignment
            const paddedMetadataLength = (metadataLength + 7) & ~7;
            const headerSize = 8 + paddedMetadataLength;

            if (buffer.length < headerSize) break; // Need more data

            // Read body length from flatbuffer metadata (offset 4 in Message table)
            // For simplicity, yield the whole accumulated buffer when we have a complete message
            // The consumer (DuckDB-WASM) handles Arrow IPC parsing
            yield buffer.slice(0, headerSize);
            buffer = buffer.slice(headerSize);
          }
        }

        // Yield any remaining data
        if (buffer.length > 0) {
          yield buffer;
        }
      } finally {
        reader.releaseLock();
      }
    },
  };
}
//#endregion smoo/lmao!n/inspector-archive-source.create
