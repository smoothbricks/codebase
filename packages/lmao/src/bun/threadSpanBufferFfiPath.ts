/**
 * Where the thread FFI shared library lives, for everything that reads it: the
 * Bun provider that dlopens it (threadSpanBufferFfi.ts) and a consumer that
 * keys its cache on the bytes a run will load. One resolution, so the key names
 * exactly the library the provider opens.
 *
 * `LMAO_THREAD_FFI_DYLIB` names it for a published consumer, because npm
 * packages ship no platform-native build output. Otherwise it is this
 * checkout's own build: `nx run lmao:cargo-thread-ffi` publishes it into
 * `.cache/lmao-thread-ffi` at the checkout root, which is also where a consumer
 * that links this checkout finds it.
 *
 * Free of `bun:ffi`, so a reader that only needs the path loads no library.
 */
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

/** The Nx target that builds the library, run in this checkout. */
export const THREAD_SPAN_BUFFER_FFI_BUILD = 'nx run lmao:cargo-thread-ffi';

const dylibName =
  process.platform === 'win32'
    ? 'lmao_ffi_dylib.dll'
    : process.platform === 'darwin'
      ? 'liblmao_ffi_dylib.dylib'
      : 'liblmao_ffi_dylib.so';

/** The library a Bun process here loads, whether or not it exists. */
export function threadSpanBufferFfiDylibPath(env: NodeJS.ProcessEnv = process.env): string {
  const configured = env.LMAO_THREAD_FFI_DYLIB;
  if (configured !== undefined && configured.length > 0) return configured;
  return fileURLToPath(new URL(`../../../../.cache/lmao-thread-ffi/${dylibName}`, import.meta.url));
}

/**
 * The library's path when it exists; otherwise an error naming how to supply
 * it. A consumer's cache key calls this: a key over a library nobody built
 * would name no bytes.
 */
export function nativeArtifactPath(env: NodeJS.ProcessEnv = process.env): string {
  const path = threadSpanBufferFfiDylibPath(env);
  if (existsSync(path)) return path;
  const configured = env.LMAO_THREAD_FFI_DYLIB;
  throw new Error(
    configured !== undefined && configured.length > 0
      ? `LMAO_THREAD_FFI_DYLIB names ${path}, which does not exist`
      : `the LMAO thread FFI library ${path} is not built: run \`${THREAD_SPAN_BUFFER_FFI_BUILD}\` in ${fileURLToPath(new URL('../../../../', import.meta.url))}`,
  );
}
