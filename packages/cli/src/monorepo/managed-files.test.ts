/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are asserted literally. */
import { describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { PLATFORM_TARGET_GLOBS } from '@smoothbricks/nx-plugin/workspace-config-policy';
import fc from 'fast-check';
import {
  extractInlineLocalBlocksForTest,
  INLINE_LOCAL_BEGIN,
  INLINE_LOCAL_END,
  LOCAL_SECTION_MARKER,
  platformTargetGlobsForTest,
  reinsertInlineLocalBlocksForTest,
  splitLocalSectionForTest,
} from './managed-files.js';

const MANAGED = '# managed content\npath merge=driver\n';

const REPO_ROOT = join(import.meta.dir, '..', '..', '..', '..');
const ARCHITECTURE_SCOPED_PREFIX = '${{ runner.os }}-${{ runner.arch }}-';
const NODE_MODULES_CACHE_KEY = "${{ hashFiles('bun.lock', 'package.json', 'packages/*/package.json') }}";
const CACHE_ACTIONS = [
  { name: 'cache-nix-devenv', osKeyLines: 6 },
  { name: 'cache-node-modules', osKeyLines: 2 },
  { name: 'cache-nx', osKeyLines: 2 },
] as const;

describe('managed-file local sections', () => {
  it('content without a marker is entirely managed', () => {
    const { managed, localTail } = splitLocalSectionForTest(MANAGED);
    expect(managed).toBe(MANAGED);
    expect(localTail).toBe('');
  });

  it('everything from the marker onward is the repo-owned tail', () => {
    const tail = `${LOCAL_SECTION_MARKER}\ncustom/*.jsonl merge=custom-log\n`;
    const { managed, localTail } = splitLocalSectionForTest(`${MANAGED}\n${tail}`);
    expect(managed).toBe(`${MANAGED}\n`);
    expect(localTail).toBe(tail);
  });

  it('a tail directly after the managed content tolerates the separating newline', () => {
    // The compare rule accepts `managed === content + '\n'` when a tail exists,
    // so update → check round-trips as unchanged.
    const written = `${MANAGED}\n${LOCAL_SECTION_MARKER}\nextra\n`;
    const { managed, localTail } = splitLocalSectionForTest(written);
    expect(managed).toBe(`${MANAGED}\n`);
    expect(localTail.startsWith(LOCAL_SECTION_MARKER)).toBe(true);
  });
});

describe('managed-file inline local blocks', () => {
  it('content with no inline markers extracts as fully managed, no blocks', () => {
    const { withoutInline, blocks } = extractInlineLocalBlocksForTest(MANAGED);
    expect(withoutInline).toBe(MANAGED);
    expect(blocks).toEqual([]);
  });

  it('a wrapped block is extracted and removed, anchored on the preceding line', () => {
    const current = ['a:', '  - one', '  - two', INLINE_LOCAL_BEGIN, '  - repo-owned', INLINE_LOCAL_END, 'b:'].join(
      '\n',
    );
    const { withoutInline, blocks } = extractInlineLocalBlocksForTest(current);
    expect(withoutInline).toBe(['a:', '  - one', '  - two', 'b:'].join('\n'));
    expect(blocks).toEqual([{ anchor: '  - two', lines: '  - repo-owned' }]);
  });

  it('multiple blocks each anchor to their own preceding line', () => {
    const current = [
      'x',
      INLINE_LOCAL_BEGIN,
      'first',
      INLINE_LOCAL_END,
      'y',
      INLINE_LOCAL_BEGIN,
      'second',
      INLINE_LOCAL_END,
      'z',
    ].join('\n');
    const { withoutInline, blocks } = extractInlineLocalBlocksForTest(current);
    expect(withoutInline).toBe(['x', 'y', 'z'].join('\n'));
    expect(blocks).toEqual([
      { anchor: 'x', lines: 'first' },
      { anchor: 'y', lines: 'second' },
    ]);
  });

  it('a begin marker with no preceding line throws rather than silently dropping content', () => {
    const current = [INLINE_LOCAL_BEGIN, 'orphan', INLINE_LOCAL_END].join('\n');
    expect(() => extractInlineLocalBlocksForTest(current)).toThrow(/no preceding anchor/);
  });

  it('an unterminated begin marker throws rather than silently dropping content', () => {
    const current = ['a', INLINE_LOCAL_BEGIN, 'unterminated'].join('\n');
    expect(() => extractInlineLocalBlocksForTest(current)).toThrow(/no matching/);
  });

  it('reinserting into fresh content with no blocks is a no-op', () => {
    expect(reinsertInlineLocalBlocksForTest(MANAGED, [])).toBe(MANAGED);
  });

  it('reinserting splices the block back after its anchor in freshly rendered content', () => {
    const fresh = ['a:', '  - one', '  - two', 'b:'].join('\n');
    const result = reinsertInlineLocalBlocksForTest(fresh, [{ anchor: '  - two', lines: '  - repo-owned' }]);
    expect(result).toBe(
      ['a:', '  - one', '  - two', INLINE_LOCAL_BEGIN, '  - repo-owned', INLINE_LOCAL_END, 'b:'].join('\n'),
    );
  });

  it('preserves marker indentation when refreshing nested configuration', () => {
    const markerIndent = '      ';
    const current = [
      'patterns:',
      "      - '*.html'",
      `${markerIndent}${INLINE_LOCAL_BEGIN}`,
      "      - '!**/templates/*.html'",
      `${markerIndent}${INLINE_LOCAL_END}`,
    ].join('\n');
    const { withoutInline, blocks } = extractInlineLocalBlocksForTest(current);
    expect(reinsertInlineLocalBlocksForTest(withoutInline, blocks)).toBe(current);
  });

  it("reinserting refuses when the anchor no longer appears — never silently drops the repo's customization", () => {
    const fresh = ['a:', '  - one', 'b:'].join('\n'); // '  - two' is gone
    expect(() => reinsertInlineLocalBlocksForTest(fresh, [{ anchor: '  - two', lines: '  - repo-owned' }])).toThrow(
      /no longer matches/,
    );
  });

  it('property: extract then reinsert round-trips to the original for unique, single-occurrence anchors', () => {
    // Lines that can serve as anchors: non-empty, not a marker, and each drawn
    // from a small alphabet so uniqueness is checkable — the round-trip only
    // holds when an anchor line occurs exactly once in the managed section
    // (reinsert finds the FIRST occurrence by design, same as the real file
    // shape: comments/patterns are unique in practice).
    const linePool = fc.constantFrom('alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta');
    fc.assert(
      fc.property(
        fc.uniqueArray(linePool, { minLength: 1, maxLength: 6 }),
        fc.array(fc.string(), { maxLength: 3 }),
        (anchors, blockLinesFlat) => {
          const blockLines = blockLinesFlat.length > 0 ? [blockLinesFlat.join('|') || 'x'] : ['x'];
          const fresh = anchors.join('\n');
          const withBlocks = anchors
            .map((anchor) => [anchor, INLINE_LOCAL_BEGIN, ...blockLines, INLINE_LOCAL_END].join('\n'))
            .join('\n');
          const { withoutInline, blocks } = extractInlineLocalBlocksForTest(withBlocks);
          expect(withoutInline).toBe(fresh);
          const reinserted = reinsertInlineLocalBlocksForTest(fresh, blocks);
          expect(reinserted).toBe(withBlocks);
        },
      ),
    );
  });
});

describe('managed publish platform discovery', () => {
  it('returns canonical target families from resolved target names without leaking project names', () => {
    const discovered = platformTargetGlobsForTest(['build', 'bundle-linux', 'package-macos', 'simulator-ios', 'test']);

    expect(discovered).toEqual([...PLATFORM_TARGET_GLOBS]);
    expect(discovered).not.toContain('native-app');
  });

  it('returns no platform families for ordinary Nx targets', () => {
    expect(platformTargetGlobsForTest(['build', 'lint', 'test', 'typecheck'])).toEqual([]);
  });
});

describe('managed cache actions', () => {
  it('renders the checked-in action copies from their managed templates', async () => {
    for (const action of CACHE_ACTIONS) {
      const [template, generated] = await Promise.all([
        readFile(
          join(REPO_ROOT, 'packages', 'cli', 'managed', 'templates', 'github', 'actions', action.name, 'action.yml'),
          'utf8',
        ),
        readFile(join(REPO_ROOT, '.github', 'actions', action.name, 'action.yml'), 'utf8'),
      ]);

      expect(generated).toBe(template.replace('{{NODE_MODULES_CACHE_KEY}}', NODE_MODULES_CACHE_KEY));
    }
  });

  it('scopes every primary, restore, and save key to the runner OS and architecture', async () => {
    for (const action of CACHE_ACTIONS) {
      for (const actionRoot of [
        join(REPO_ROOT, 'packages', 'cli', 'managed', 'templates', 'github', 'actions'),
        join(REPO_ROOT, '.github', 'actions'),
      ]) {
        const content = await readFile(join(actionRoot, action.name, 'action.yml'), 'utf8');
        const osKeyLines = content.split('\n').filter((line) => line.includes('${{ runner.os }}'));

        expect(osKeyLines).toHaveLength(action.osKeyLines);
        expect(osKeyLines.every((line) => line.includes(ARCHITECTURE_SCOPED_PREFIX))).toBe(true);
      }
    }
  });
});
