/**
 * Index operands — the encoding of every column, slot and match-count operand
 * of a program (`columine_types::operand`): an unsigned LEB128 `u32`, seven
 * value bits per byte, low group first, the high bit set on every byte but the
 * last. Only the shortest encoding is an index, so a program has one byte
 * image per meaning.
 */

/** The longest index encoding: five groups cover a `u32`. */
export const MAX_INDEX_LEN = 5;

/** Append `value` to `out` as an index. */
export function appendIndex(out: number[], value: number): void {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
    throw new RangeError(`index operand ${value} is not a u32`);
  }
  let rest = value;
  while (rest >= 0x80) {
    out.push((rest % 0x80) | 0x80);
    rest = Math.floor(rest / 0x80);
  }
  out.push(rest);
}

/** The index at `code[at]` and the position after it; throws on a truncated, overlong or out-of-range encoding. */
export function readIndex(code: Uint8Array, at: number): { readonly value: number; readonly next: number } {
  let value = 0;
  for (let i = 0; i < MAX_INDEX_LEN; i++) {
    const byte = code[at + i];
    if (byte === undefined) {
      throw new Error('Invalid program: truncated index operand');
    }
    const group = byte & 0x7f;
    if (i === MAX_INDEX_LEN - 1 && group > 0x0f) {
      throw new Error('Invalid program: index operand exceeds u32');
    }
    value += group * 2 ** (7 * i);
    if ((byte & 0x80) === 0) {
      if (i > 0 && byte === 0) {
        throw new Error('Invalid program: overlong index operand');
      }
      return { value, next: at + i + 1 };
    }
  }
  throw new Error('Invalid program: index operand exceeds u32');
}
