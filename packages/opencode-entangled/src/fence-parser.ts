// WHY: fenceparser's default export is lex+parse composed, but nodenext can't call it.
// Using the named exports directly avoids the TS2349 issue.
import { lex, parse } from 'fenceparser';

const fenceparser = (input: string) => parse(lex(input));

export interface ParsedBlock {
  /** Block identifier from #name */
  id?: string;
  /** Tangle target from file="path" */
  file?: string;
  /** Language from the fence info string (before the {) */
  language?: string;
  /** Block body content (without fence lines) */
  content: string;
  /** 1-indexed line number of the fence opening */
  line: number;
  /** <<ref>> references found in the body */
  refs: string[];
  /** Raw attributes parsed from {} */
  attrs: Record<string, unknown>;
}

// WHY: fenceparser doesn't handle CSS-like #id or .class tokens — it throws on '#' and '.'
// We extract those ourselves, then pass the remainder to fenceparser for key=value parsing.
// We must skip over quoted strings so that e.g. file="foo.ts" doesn't match .ts as a class.
function parseAttrString(raw: string): { id?: string; classes: string[]; rest: Record<string, unknown> } {
  let id: string | undefined;
  const classes: string[] = [];
  const remaining: string[] = [];
  let i = 0;

  while (i < raw.length) {
    const ch = raw.charAt(i);

    // Skip quoted strings verbatim — they may contain dots/hashes
    if (ch === '"' || ch === "'") {
      const quote = ch;
      let j = i + 1;
      while (j < raw.length && raw[j] !== quote) j++;
      remaining.push(raw.slice(i, j + 1));
      i = j + 1;
      continue;
    }

    // Match #id or .class token at current position
    if (ch === '#' || ch === '.') {
      const m = raw.slice(i).match(/^[#.][\w-]+/);
      if (m) {
        // biome-ignore lint/style/noNonNullAssertion: m[0] always exists when match() succeeds
        const token = m[0]!;
        if (token.startsWith('#')) {
          id = token.slice(1);
        } else {
          classes.push(token.slice(1));
        }
        i += token.length;
        continue;
      }
    }

    remaining.push(ch);
    i++;
  }

  let rest: Record<string, unknown> = {};
  const trimmed = remaining.join('').trim();
  if (trimmed.length > 0) {
    rest = fenceparser(trimmed);
  }

  return { id, classes, rest };
}

const FENCE_OPEN_RE = /^(`{3,})(\S+)?\s*(\{[^}]*\})?\s*$/;
const NOWEB_REF_RE = /^\s*<<([^>]+)>>\s*$/;

export function parseFences(markdown: string): ParsedBlock[] {
  const lines = markdown.split('\n');
  const blocks: ParsedBlock[] = [];

  let i = 0;
  while (i < lines.length) {
    const line = lines[i] ?? '';
    const openMatch = line.match(FENCE_OPEN_RE);

    if (!openMatch) {
      i++;
      continue;
    }

    const backticks = openMatch[1] ?? '```';
    const language = openMatch[2];
    const attrRaw = openMatch[3]; // includes the { }

    // Skip blocks without {…} attributes — Entangled ignores them
    if (!attrRaw) {
      i++;
      continue;
    }

    // Strip outer braces
    const innerAttrs = attrRaw.slice(1, -1).trim();
    const { id, rest } = parseAttrString(innerAttrs);
    const file = typeof rest.file === 'string' ? rest.file : undefined;

    // Skip blocks that have attributes but no #id and no file= — not Entangled blocks
    if (!id && !file) {
      i++;
      continue;
    }

    // Collect body lines until fence close
    const openLine = i + 1; // 1-indexed
    const bodyLines: string[] = [];
    const refs: string[] = [];
    i++;

    while (i < lines.length) {
      const bodyLine = lines[i] ?? '';
      // WHY: fence close must have at least as many backticks as the opener
      if (
        bodyLine.startsWith(backticks) &&
        bodyLine.trim() === backticks.charAt(0).repeat(bodyLine.trim().length) &&
        bodyLine.trim().length >= backticks.length
      ) {
        i++; // skip close fence
        break;
      }

      const refMatch = bodyLine.match(NOWEB_REF_RE);
      if (refMatch) {
        // biome-ignore lint/style/noNonNullAssertion: capture group 1 always exists when the regex matches
        refs.push(refMatch[1]!);
      }

      bodyLines.push(bodyLine);
      i++;
    }

    // Build merged attrs record for the raw output
    const attrs: Record<string, unknown> = { ...rest };
    if (id) attrs.id = id;

    blocks.push({
      ...(id != null && { id }),
      ...(file != null && { file }),
      ...(language != null && { language }),
      content: bodyLines.join('\n'),
      line: openLine,
      refs,
      attrs,
    });
  }

  return blocks;
}
