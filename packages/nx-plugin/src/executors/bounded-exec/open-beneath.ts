import { constants as osConstants } from 'node:os';
import { darwinCLibrary } from './open-beneath.darwin.js';
import { linuxCLibrary } from './open-beneath.linux.js';

/**
 * openat(2), as Bun reaches it in the C library the process already has loaded (`bun:ffi`): a name opened relative to
 * a directory descriptor, never resolved from a root or a working directory. Node has no openat, so only a Bun program
 * (the capture reader) loads this. Each platform names its C library in its own file (`open-beneath.darwin.ts`,
 * `open-beneath.linux.ts`); callers open beneath a descriptor here and never ask which platform they run on.
 */

/** A C library by the names it is loaded under, and the function that returns the address of its `errno`. */
export interface CLibrary {
  readonly names: readonly string[];
  readonly errnoAddress: '__error' | '__errno_location';
}

/** What opening a name beneath a directory descriptor answers. */
export type Beneath =
  | { readonly kind: 'opened'; readonly fd: number }
  | { readonly kind: 'absent' }
  /** A link, or not the kind `flags` asks for (`ELOOP`, `ENOTDIR`). */
  | { readonly kind: 'not-it' }
  | { readonly kind: 'failed'; readonly cause: string };

export type OpenBeneath = (directory: number, name: string, flags: number) => Beneath;

const PLATFORMS: Partial<Record<NodeJS.Platform, CLibrary>> = { darwin: darwinCLibrary, linux: linuxCLibrary };

/** The Bun FFI specifier, which the compiler does not resolve against Node's types. */
const BUN_FFI = 'bun:ffi';

interface BunFfi {
  dlopen(path: string, symbols: Record<string, { args: string[]; returns: string }>): { symbols: unknown };
  ptr(view: ArrayBufferView): number;
  readonly read: { i32(address: number, offset: number): number };
}

/** openat(2) without `O_CREAT`, so its variadic mode is never read: a descriptor, or -1 with the cause in errno. */
type OpenAt = (directory: number, path: number, flags: number) => number;
/** The function that returns the address of this thread's `errno`. */
type ErrnoAddress = () => number | null;
type CSymbols =
  | { readonly openat: OpenAt; readonly __error: ErrnoAddress }
  | { readonly openat: OpenAt; readonly __errno_location: ErrnoAddress };

/*
 * The two guards below check module shapes, not data, and are written out because the reader runs on Bun with an empty
 * config (`bun --config=/dev/null`): from source, as under `bun test`, no typia transform would apply in it, as in the
 * peak waiter's `host-memory.darwin.ts`.
 */
function isBunFfi(module: unknown): module is BunFfi {
  return (
    typeof module === 'object' &&
    module !== null &&
    'dlopen' in module &&
    typeof module.dlopen === 'function' &&
    'ptr' in module &&
    typeof module.ptr === 'function' &&
    'read' in module &&
    typeof module.read === 'object' &&
    module.read !== null &&
    'i32' in module.read &&
    typeof module.read.i32 === 'function'
  );
}

function isCSymbols(symbols: unknown): symbols is CSymbols {
  if (
    typeof symbols !== 'object' ||
    symbols === null ||
    !('openat' in symbols) ||
    typeof symbols.openat !== 'function'
  ) {
    return false;
  }
  return (
    ('__error' in symbols && typeof symbols.__error === 'function') ||
    ('__errno_location' in symbols && typeof symbols.__errno_location === 'function')
  );
}

function errnoName(code: number): string {
  return Object.entries(osConstants.errno).find(([, value]) => value === code)?.[0] ?? `errno ${code}`;
}

/** openat(2) beneath a directory descriptor, loaded from this platform's C library. */
export async function loadOpenBeneath(): Promise<OpenBeneath> {
  const library = PLATFORMS[process.platform];
  if (library === undefined)
    throw new Error(`${process.platform} names no C library to open beneath a descriptor with`);
  // Bun's FFI exists only on Bun: Node, which loads the executor that names this module's reader, has no `bun:ffi`.
  const ffi: unknown = await import(BUN_FFI);
  if (!isBunFfi(ffi)) throw new Error(`${BUN_FFI} names no dlopen, ptr and read: open beneath a descriptor on Bun`);
  const failures: unknown[] = [];
  for (const name of library.names) {
    let symbols: unknown;
    try {
      ({ symbols } = ffi.dlopen(name, {
        openat: { args: ['i32', 'ptr', 'i32'], returns: 'i32' },
        [library.errnoAddress]: { args: [], returns: 'ptr' },
      }));
    } catch (error) {
      failures.push(error);
      continue;
    }
    if (!isCSymbols(symbols)) throw new Error(`${name} opened without openat and ${library.errnoAddress}`);
    const c = symbols;
    const errnoAddress = '__error' in c ? c.__error : c.__errno_location;
    const errno = (): number => {
      const address = errnoAddress();
      if (address === null) throw new Error(`${library.errnoAddress}() returned no address`);
      return ffi.read.i32(address, 0);
    };
    return (directory, entry, flags) => {
      const path = new TextEncoder().encode(`${entry}\0`);
      const fd = c.openat(directory, ffi.ptr(path), flags);
      if (fd >= 0) return { kind: 'opened', fd };
      const cause = errno();
      if (cause === osConstants.errno.ENOENT) return { kind: 'absent' };
      if (cause === osConstants.errno.ELOOP || cause === osConstants.errno.ENOTDIR) return { kind: 'not-it' };
      return { kind: 'failed', cause: errnoName(cause) };
    };
  }
  throw new AggregateError(failures, `no C library among ${library.names.join(', ')} gives openat`);
}
