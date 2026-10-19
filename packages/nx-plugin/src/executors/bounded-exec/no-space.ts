import type { OutputStream } from './progress.js';

/**
 * The operating system's own words for ENOSPC: the errno name Node and Bun print (`ENOSPC: no space left on device,
 * write`), and `strerror(28)`, which Rust (`No space left on device (os error 28)`), Go and C print. Nothing else in a
 * command's output says the disk was full, and a write that failed on it is otherwise reported as whatever its caller
 * was doing: a failed test, an extraction error.
 */
const NO_SPACE = /\bENOSPC\b|No space left on device/;
const ANSI = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*[A-Za-z]`, 'g');

/** Watches a command's output for the first line that says a write found no space. */
export interface NoSpaceWatch {
  /** One chunk of one output stream: the first matching line, once, as printed; `null` before and after it. */
  scan(stream: OutputStream, chunk: string): string | null;
}

export function createNoSpaceWatch(): NoSpaceWatch {
  const partial: Record<OutputStream, string> = { stdout: '', stderr: '' };
  let seen = false;
  return {
    scan(stream, chunk) {
      if (seen) {
        return null;
      }
      const lines = (partial[stream] + chunk).split('\n');
      // The unfinished last line is kept for the next chunk, and read now too: a command that dies mid-line still
      // printed the words.
      partial[stream] = lines.at(-1) ?? '';
      for (const line of lines) {
        const text = line.replace(ANSI, '').trim();
        if (NO_SPACE.test(text)) {
          seen = true;
          return text;
        }
      }
      return null;
    },
  };
}
