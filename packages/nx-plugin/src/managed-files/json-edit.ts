import { isDeepStrictEqual } from 'node:util';
import { applyEdits, type Edit, type JSONPath, type Node, type ParseError, parse, parseTree } from 'jsonc-parser';
import { isRecord } from '../type-guards.js';

/**
 * One structural difference between two documents. `add-key` and `append` name the object or array they add to;
 * `replace` and `remove` name the value they change.
 */
type Change =
  | { readonly kind: 'replace'; readonly path: JSONPath; readonly value: unknown }
  | { readonly kind: 'add-key'; readonly path: JSONPath; readonly key: string; readonly value: unknown }
  | { readonly kind: 'append'; readonly path: JSONPath; readonly value: unknown }
  | { readonly kind: 'remove'; readonly path: JSONPath };

/**
 * `text` edited to parse as `after`, with every byte the difference does not touch left alone.
 *
 * A managed file that people also write (a tsconfig explaining why it extends what it extends) cannot be
 * regenerated from its parsed value: serializing drops every comment. The generator owns a handful of fields, so it
 * edits those, and the rest of the file is the author's.
 *
 * - A scalar or an object that changed is replaced where it stands; an array that only grew gains its new elements
 *   at the end, one that only shrank loses those elements, and any other array is replaced whole (comments inside
 *   it go with it: no generator step reorders an array it owns).
 * - A comment that trails an element on its own line belongs to that element: removing the element removes the
 *   comment with it, and an element added after it goes after the comment, never between the two. A comment on a
 *   line of its own stays wherever it is, including above a key that was removed.
 * - Inserted values are written compactly on a line of their own; their layout is the caller's formatter's to
 *   decide, not this function's.
 *
 * The result is checked: text that does not parse to exactly `after` is a fault in this function, not something to
 * write.
 */
export function editJsonText(text: string, after: unknown): string {
  // What the document means, not how a caller built it: a key set to undefined is no key.
  const target: unknown = JSON.parse(JSON.stringify(after));
  let edited = text;
  for (const change of changesBetween(parseDocument(text), target, [])) {
    edited = applyEdits(edited, editsFor(edited, change));
    // A repeated key is every one of its properties: one removal leaves the other still in the document.
    while (change.kind === 'remove' && typeof change.path.at(-1) === 'string' && isPresent(edited, change.path)) {
      edited = applyEdits(edited, editsFor(edited, change));
    }
  }
  if (!isDeepStrictEqual(parseDocument(edited), target)) {
    throw new Error('the edited JSON text does not parse to the document the policy produced, so it is not written');
  }
  return edited;
}

/** Whether `text` still has a member at `path`. */
function isPresent(text: string, path: JSONPath): boolean {
  const root = parseTree(text, undefined, { allowTrailingComma: true });
  return root !== undefined && memberAt(root, path) !== undefined;
}

function parseDocument(text: string): unknown {
  const errors: ParseError[] = [];
  const document: unknown = parse(text, errors, { allowTrailingComma: true });
  const [first] = errors;
  if (first !== undefined) throw new Error(`not valid JSON/JSONC (error ${first.error} at offset ${first.offset})`);
  return document;
}

function* changesBetween(before: unknown, after: unknown, path: JSONPath): Generator<Change> {
  if (isRecord(before) && isRecord(after)) {
    for (const key of Object.keys(before)) {
      if (!Object.hasOwn(after, key)) yield { kind: 'remove', path: [...path, key] };
    }
    for (const [key, value] of Object.entries(after)) {
      if (Object.hasOwn(before, key)) yield* changesBetween(before[key], value, [...path, key]);
      else yield { kind: 'add-key', path, key, value };
    }
  } else if (Array.isArray(before) && Array.isArray(after)) {
    yield* arrayChanges(before, after, path);
  } else if (!isDeepStrictEqual(before, after)) {
    yield { kind: 'replace', path, value: after };
  }
}

function* arrayChanges(before: readonly unknown[], after: readonly unknown[], path: JSONPath): Generator<Change> {
  if (isDeepStrictEqual(before, after)) return;
  if (before.every((element, index) => isDeepStrictEqual(element, after[index]))) {
    for (const element of after.slice(before.length)) yield { kind: 'append', path, value: element };
    return;
  }
  const removed = removedIndexes(before, after);
  if (removed !== null) {
    // Last first, so an index still names the element it did when the changes were derived.
    for (const index of [...removed].reverse()) yield { kind: 'remove', path: [...path, index] };
    return;
  }
  yield { kind: 'replace', path, value: after };
}

/** The indexes of `before` that `after` leaves out, when `after` is `before` with only removals; else null. */
function removedIndexes(before: readonly unknown[], after: readonly unknown[]): number[] | null {
  const removed: number[] = [];
  let kept = 0;
  before.forEach((element, index) => {
    if (kept < after.length && isDeepStrictEqual(element, after[kept])) kept += 1;
    else removed.push(index);
  });
  return kept === after.length ? removed : null;
}

/** The edits that make `change` to `text`, found by walking the text's own syntax tree. */
function editsFor(text: string, change: Change): Edit[] {
  const root = parseTree(text, undefined, { allowTrailingComma: true });
  const member = root === undefined ? undefined : memberAt(root, change.path);
  if (member === undefined) throw new Error(`no ${JSON.stringify(change.path)} in the JSON text to edit`);
  switch (change.kind) {
    case 'replace': {
      const value = valueNodeOf(member);
      return [{ offset: value.offset, length: value.length, content: JSON.stringify(change.value) }];
    }
    case 'remove':
      return removal(text, member);
    case 'add-key':
      return insertion(text, valueNodeOf(member), `${JSON.stringify(change.key)}: ${JSON.stringify(change.value)}`);
    case 'append':
      return insertion(text, valueNodeOf(member), JSON.stringify(change.value));
  }
}

/** The node a path names: the property for a key, the element for an index, the root for no path. */
function memberAt(root: Node, path: JSONPath): Node | undefined {
  let member: Node | undefined = root;
  for (const segment of path) {
    if (member === undefined) return undefined;
    const container = valueNodeOf(member);
    member = typeof segment === 'number' ? container.children?.[segment] : lastProperty(container, segment);
  }
  return member;
}

/** The value a member holds: a property's value, or an element itself. */
function valueNodeOf(member: Node): Node {
  return member.type === 'property' ? (member.children?.[1] ?? member) : member;
}

/** The last property of an object with this key: a parser keeps the last of duplicate keys, so that is the one that means something. */
function lastProperty(object: Node, key: string): Node | undefined {
  if (object.type !== 'object') return undefined;
  const properties = object.children ?? [];
  for (let index = properties.length - 1; index >= 0; index--) {
    const property = properties[index];
    if (property?.children?.[0]?.value === key) return property;
  }
  return undefined;
}

/** The edits that add `entry` after the last member of `container`, or inside it when it is empty. */
function insertion(text: string, container: Node, entry: string): Edit[] {
  if (container.type !== 'object' && container.type !== 'array')
    throw new Error('only an object or an array can gain a member');
  const last = container.children?.at(-1);
  if (last === undefined) return [{ offset: container.offset + 1, length: 0, content: entry }];
  const lastEnd = last.offset + last.length;
  const closer = container.offset + container.length - 1;
  const separator = separatorAfter(text, lastEnd, closer);
  // After the comments on the last member's line, and after its separator when that is on a later line.
  const at = Math.max(sameLineEnd(text, lastEnd, closer), separator === null ? 0 : separator + 1);
  // A trailing comma the author wrote stays, one comma further along.
  const added = `\n${entry}${separator === null ? '' : ','}`;
  if (separator !== null) return [{ offset: at, length: 0, content: added }];
  // No separator yet: it goes straight after the member, ahead of its comments.
  return at === lastEnd
    ? [{ offset: lastEnd, length: 0, content: `,${added}` }]
    : [
        { offset: lastEnd, length: 0, content: ',' },
        { offset: at, length: 0, content: added },
      ];
}

/**
 * The edits that delete a member of an object or array: its text, its separator, and the comments that trail it on
 * its line, plus the whole line when it has the line to itself. The member before a last one loses its separator so
 * no comma is left dangling.
 */
function removal(text: string, member: Node): Edit[] {
  const container = member.parent;
  const siblings = container?.children;
  if (container === undefined || siblings === undefined)
    throw new Error('a JSON value with no container cannot be removed');
  const index = siblings.indexOf(member);
  const previous = siblings[index - 1];
  const memberEnd = member.offset + member.length;
  const limit = siblings[index + 1]?.offset ?? container.offset + container.length - 1;
  const separator = separatorAfter(text, memberEnd, limit);
  const { from, to } = wholeLines(text, member.offset, sameLineEnd(text, memberEnd, limit));
  const edits: Edit[] = [{ offset: from, length: to - from, content: '' }];
  // A separator on a later line (a leading-comma style) is not part of the member's line.
  if (separator !== null && separator >= to) edits.push({ offset: separator, length: 1, content: '' });
  if (separator === null && previous !== undefined) {
    const own = separatorAfter(text, previous.offset + previous.length, member.offset);
    if (own !== null) edits.push({ offset: own, length: 1, content: '' });
  }
  return edits;
}

/** The span `start`..`end` widened to its whole lines, when nothing else is on them. */
function wholeLines(text: string, start: number, end: number): { from: number; to: number } {
  let from = start;
  while (from > 0 && isHorizontalSpace(text[from - 1])) from -= 1;
  let to = end;
  while (to < text.length && isHorizontalSpace(text[to])) to += 1;
  const alone = from === 0 || isLineBreak(text[from - 1]);
  const lineBreak = text.startsWith('\r\n', to) ? 2 : isLineBreak(text[to]) ? 1 : 0;
  return alone && lineBreak > 0 ? { from, to: to + lineBreak } : { from: start, to: end };
}

/** The offset of the separating comma in `text[from, limit)`, skipping comments; null when there is none. */
function separatorAfter(text: string, from: number, limit: number): number | null {
  let index = from;
  while (index < limit) {
    const char = text[index];
    if (char === ',') return index;
    if (text.startsWith('//', index)) index = lineEnd(text, index, limit);
    else if (text.startsWith('/*', index)) index = blockEnd(text, index, limit);
    else if (isHorizontalSpace(char) || isLineBreak(char)) index += 1;
    else return null;
  }
  return null;
}

/**
 * The end of what trails a member on its line: its comma if that is on the line, and the comments after it. A comment
 * after the comma with a value behind it on the line leads that value, so it is not the member's to take.
 */
function sameLineEnd(text: string, from: number, limit: number): number {
  let end = from;
  let index = from;
  let sawComma = false;
  while (index < limit) {
    const char = text[index];
    if (isHorizontalSpace(char)) {
      index += 1;
    } else if (char === ',' && !sawComma) {
      sawComma = true;
      index += 1;
      end = index;
    } else if (text.startsWith('/*', index)) {
      const close = blockEnd(text, index, limit);
      if (sawComma && !onlyTriviaToLineEnd(text, close, limit)) break;
      index = end = close;
    } else if (text.startsWith('//', index)) {
      return lineEnd(text, index, limit);
    } else {
      break;
    }
  }
  return end;
}

/**
 * Whether nothing but whitespace and comments is left on the line from `from`. `limit` is where the next member
 * starts, or the container's closer: reaching a member on the line is not "nothing left".
 */
function onlyTriviaToLineEnd(text: string, from: number, limit: number): boolean {
  let index = from;
  while (index < limit) {
    const char = text[index];
    if (isHorizontalSpace(char)) index += 1;
    else if (text.startsWith('/*', index)) index = blockEnd(text, index, limit);
    else return text.startsWith('//', index) || isLineBreak(char);
  }
  const atLimit = text[limit];
  return atLimit === '}' || atLimit === ']' || atLimit === undefined;
}

/** Where a `//` comment ends: at the line break, which jsonc-parser takes as `\n` or `\r`. */
function lineEnd(text: string, from: number, limit: number): number {
  let index = from;
  while (index < limit && !isLineBreak(text[index])) index += 1;
  return index;
}

function blockEnd(text: string, from: number, limit: number): number {
  const close = text.indexOf('*/', from + 2);
  return close === -1 ? limit : Math.min(close + 2, limit);
}

function isHorizontalSpace(char: string | undefined): boolean {
  return char === ' ' || char === '\t';
}

function isLineBreak(char: string | undefined): boolean {
  return char === '\n' || char === '\r';
}
