/** Tokens shared by Rust input inference and statement comparison. */
export interface RustToken {
  readonly text: string;
  readonly literal: boolean;
  readonly local: boolean;
  readonly kind: 'ident' | 'literal' | 'punct' | 'lifetime';
  readonly offset: number;
}

const RUST_IDENT = /[A-Za-z_][A-Za-z0-9_]*/uy;
const RUST_NUMBER =
  /(?:0[xob][0-9A-Fa-f_]+|[0-9][0-9_]*(?:\.[0-9][0-9_]*)?(?:[eE][+-]?[0-9_]+)?)(?:[iuf](?:8|16|32|64|128|size))?/uy;
const RUST_RAW_STRING = /b?r(#*)"/uy;
const RUST_CHAR = /b?'(?:\\(?:u\{[0-9A-Fa-f]+\}|x[0-9A-Fa-f]{2}|.)|[^\\'\n])'/uy;
/** Longest first: `..` is one token, so a range end (`0..5`) is never read as a tuple index after `.`. */
const RUST_PUNCT = ['..=', '::', '->', '=>', '==', '!=', '<=', '>=', '&&', '||', '..'];

/** A Rust lexer that keeps exactly what statement comparison needs: idents, literals, lifetimes, punctuation. */
export function rustTokens(text: string): RustToken[] {
  const tokens: RustToken[] = [];
  let index = 0;
  const at = (pattern: RegExp): string | undefined => {
    pattern.lastIndex = index;
    return pattern.exec(text)?.[0];
  };
  const push = (kind: RustToken['kind'], start: number): void => {
    tokens.push({ text: text.slice(start, index), literal: kind === 'literal', local: false, kind, offset: start });
  };
  while (index < text.length) {
    const char = text[index] ?? '';
    const start = index;
    if (/\s/u.test(char)) index += 1;
    else if (text.startsWith('//', index)) {
      const end = text.indexOf('\n', index);
      index = end === -1 ? text.length : end;
    } else if (text.startsWith('/*', index)) {
      let depth = 0;
      do {
        if (text.startsWith('/*', index)) depth += 1;
        else if (text.startsWith('*/', index)) depth -= 1;
        index += text.startsWith('/*', index) || text.startsWith('*/', index) ? 2 : 1;
      } while (depth > 0 && index < text.length);
    } else if (at(RUST_RAW_STRING) !== undefined) {
      const raw = at(RUST_RAW_STRING) ?? '';
      const close = `"${raw.slice(raw.indexOf('r') + 1, -1)}`;
      const end = text.indexOf(close, index + raw.length);
      index = end === -1 ? text.length : end + close.length;
      push('literal', start);
    } else if (char === '"' || (char === 'b' && text[index + 1] === '"')) {
      index += char === 'b' ? 2 : 1;
      while (index < text.length && text[index] !== '"') index += text[index] === '\\' ? 2 : 1;
      index += 1;
      push('literal', start);
    } else if (at(RUST_CHAR) !== undefined) {
      index += at(RUST_CHAR)?.length ?? 0;
      push('literal', start);
    } else if (char === "'") {
      index += 1;
      index += at(RUST_IDENT)?.length ?? 0;
      push('lifetime', start);
    } else if (at(RUST_IDENT) !== undefined) {
      const ident = at(RUST_IDENT) ?? '';
      index += ident.length;
      push(ident === 'true' || ident === 'false' ? 'literal' : 'ident', start);
    } else if (tokens.at(-1)?.text === '.' && /[0-9]/u.test(char)) {
      // A tuple index (`pair.0.1`) names a field; lexed as a number it would read `0.1` as one float literal.
      while (/[0-9]/u.test(text[index] ?? '')) index += 1;
      push('ident', start);
    } else if ((at(RUST_NUMBER) ?? '') !== '') {
      index += at(RUST_NUMBER)?.length ?? 0;
      push('literal', start);
    } else {
      index += RUST_PUNCT.find((punct) => text.startsWith(punct, index))?.length ?? 1;
      push('punct', start);
    }
  }
  return tokens;
}
