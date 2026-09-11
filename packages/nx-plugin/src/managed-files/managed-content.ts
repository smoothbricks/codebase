/** Content ownership policy. No filesystem, process, environment or Nx dependencies. */
export class ManagedContentConflict extends Error {}

/**
 * Repos may append their own content to a managed file below this marker —
 * e.g. extra merge drivers in .gitattributes. Everything from the marker
 * line onward is preserved verbatim across updates and ignored by the
 * drift check; the managed section above it stays byte-exact.
 */
export const LOCAL_SECTION_MARKER = '# smoo-local: everything below this line is repo-owned and preserved';

/** Split a managed target's content into the managed part and the repo-owned tail. */
export function splitLocalSection(current: string): { managed: string; localTail: string } {
  // A marker mentioned inside a value or comment is not an ownership boundary.
  let offset = 0;
  for (const line of current.split('\n')) {
    if (line.replace(/\r$/, '') === LOCAL_SECTION_MARKER) {
      return { managed: current.slice(0, offset), localTail: current.slice(offset) };
    }
    offset += line.length + 1;
  }
  return { managed: current, localTail: '' };
}

/**
 * A repo-owned block INSIDE the managed section — e.g. one extra pattern
 * spliced into a formatter's list, where a trailing marker (LOCAL_SECTION_MARKER)
 * can't express it because it isn't at the end of the file. Wrap it in
 * `# smoo-local-begin` / `# smoo-local-end`; the block is anchored to the line
 * immediately before `# smoo-local-begin`. On update, the block is re-spliced
 * right after that same anchor line in the freshly rendered template — if the
 * anchor no longer appears there (the template reworked that section), the
 * update refuses rather than silently dropping the repo's customization.
 */
export const INLINE_LOCAL_BEGIN = '# smoo-local-begin';
export const INLINE_LOCAL_END = '# smoo-local-end';

export interface InlineLocalBlock {
  anchor: string;
  lines: string;
  markerIndent?: string;
  /** Distinguishes no lines from one deliberately blank line. */
  empty?: true;
}

/** Pull inline local blocks out of a managed section, returning the section
 * with each block (and its markers) removed, plus the extracted blocks in
 * the order they appeared. */
export function extractInlineLocalBlocks(managed: string): { withoutInline: string; blocks: InlineLocalBlock[] } {
  const lines = managed.split('\n');
  const kept: string[] = [];
  const blocks: InlineLocalBlock[] = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (line !== undefined && line.trim() === INLINE_LOCAL_BEGIN) {
      const anchor = kept.at(-1);
      if (anchor === undefined) {
        throw new ManagedContentConflict(`${INLINE_LOCAL_BEGIN} on line ${i + 1} has no preceding anchor line`);
      }
      const blockLines: string[] = [];
      i += 1;
      while (i < lines.length && lines[i]?.trim() !== INLINE_LOCAL_END) {
        const blockLine = lines[i];
        if (blockLine.trim() === INLINE_LOCAL_BEGIN) {
          throw new ManagedContentConflict(`${INLINE_LOCAL_BEGIN} on line ${i + 1} is nested`);
        }
        blockLines.push(blockLine);
        i += 1;
      }
      if (i >= lines.length) {
        throw new ManagedContentConflict(
          `${INLINE_LOCAL_BEGIN} anchored on "${anchor}" has no matching ${INLINE_LOCAL_END}`,
        );
      }
      const markerIndent = line.slice(0, line.length - line.trimStart().length);
      blocks.push({
        anchor,
        lines: blockLines.join('\n'),
        ...(blockLines.length === 0 ? { empty: true as const } : {}),
        ...(markerIndent === '' ? {} : { markerIndent }),
      });
      i += 1; // skip the END marker line itself
      continue;
    }
    if (line.trim() === INLINE_LOCAL_END) {
      throw new ManagedContentConflict(`${INLINE_LOCAL_END} on line ${i + 1} has no matching ${INLINE_LOCAL_BEGIN}`);
    }
    kept.push(line);
    i += 1;
  }
  return { withoutInline: kept.join('\n'), blocks };
}

/** Re-splice extracted inline blocks into freshly rendered managed content,
 * each immediately after its anchor line. A no-op when there are no blocks. */
export function reinsertInlineLocalBlocks(content: string, blocks: InlineLocalBlock[]): string {
  if (blocks.length === 0) return content;
  const lines = content.split('\n');
  const additions = new Map<number, string[]>();
  for (const block of blocks) {
    const matches = lines.flatMap((line, index) => (line === block.anchor ? [index] : []));
    if (matches.length !== 1) {
      const reason = matches.length === 0 ? 'no line' : `${matches.length} lines`;
      throw new ManagedContentConflict(
        `${INLINE_LOCAL_BEGIN} block anchored on "${block.anchor}" matches ${reason} in the updated ` +
          'template — reconcile the repo-owned block manually',
      );
    }
    const index = matches[0];
    const markerIndent = block.markerIndent ?? '';
    const inserted = additions.get(index) ?? [];
    inserted.push(
      `${markerIndent}${INLINE_LOCAL_BEGIN}`,
      ...(block.empty ? [] : block.lines.split('\n')),
      `${markerIndent}${INLINE_LOCAL_END}`,
    );
    additions.set(index, inserted);
  }
  return lines.flatMap((line, index) => [line, ...(additions.get(index) ?? [])]).join('\n');
}
