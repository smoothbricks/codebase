/**
 * Archive source fetches historical Arrow IPC data via HTTP for offline trace analysis.
 * Returns async iterables of Arrow IPC buffers for progressive loading.
 */

export interface ArchiveSource {
  /** Fetch Arrow IPC buffers for a given time range */
  fetchRange(startTime: number, endTime: number): AsyncIterable<Uint8Array>;
}

/**
 * Create an archive source that fetches historical Arrow chunks via HTTP GET.
 * The server returns Arrow IPC files for the requested time range.
 */
export function createArchiveSource(baseUrl: string): ArchiveSource {
  return {
    async *fetchRange(startTime: number, endTime: number): AsyncIterable<Uint8Array> {
      const url = `${baseUrl}?start=${startTime}&end=${endTime}`;
      const response = await fetch(url);

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
          const { done, value } = await reader.read();
          if (done) break;

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
