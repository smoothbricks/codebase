import { describe, expect, it } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  nativeArtifactPath,
  THREAD_SPAN_BUFFER_FFI_BUILD,
  threadSpanBufferFfiDylibPath,
} from '../threadSpanBufferFfiPath.js';

describe('the thread FFI library resolution', () => {
  it('takes LMAO_THREAD_FFI_DYLIB when a consumer names one', () => {
    const directory = mkdtempSync(join(tmpdir(), 'lmao-ffi-path-'));
    try {
      const library = join(directory, 'library');
      writeFileSync(library, 'native');
      expect(nativeArtifactPath({ LMAO_THREAD_FFI_DYLIB: library })).toBe(library);
      rmSync(library);
      expect(() => nativeArtifactPath({ LMAO_THREAD_FFI_DYLIB: library })).toThrow(
        `LMAO_THREAD_FFI_DYLIB names ${library}, which does not exist`,
      );
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("otherwise names this checkout's own build, and how to make it", () => {
    const path = threadSpanBufferFfiDylibPath({});
    expect(path).toMatch(/\/\.cache\/lmao-thread-ffi\/(lib)?lmao_ffi_dylib\.(dylib|so|dll)$/);
    expect(threadSpanBufferFfiDylibPath({ LMAO_THREAD_FFI_DYLIB: '' })).toBe(path);
    const missing = { LMAO_THREAD_FFI_DYLIB: '' };
    try {
      expect(nativeArtifactPath(missing)).toBe(path);
    } catch (error) {
      expect(String(error)).toContain(`run \`${THREAD_SPAN_BUFFER_FFI_BUILD}\``);
    }
  });
});
