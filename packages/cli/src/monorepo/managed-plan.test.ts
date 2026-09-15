import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import {
  extractInlineLocalBlocks,
  INLINE_LOCAL_BEGIN,
  INLINE_LOCAL_END,
  LOCAL_SECTION_MARKER,
  reinsertInlineLocalBlocks,
  splitLocalSection,
} from './managed-content.js';
import { type ManagedFileInput, planManagedFiles } from './managed-plan.js';

const desired = { content: 'managed\n', executable: false };
const missing: ManagedFileInput = { target: 'tooling/config', desired, current: { kind: 'missing' } };
const regular: ManagedFileInput = { ...missing, current: { kind: 'file', ...desired } };

function withContent(content: string): ManagedFileInput {
  return { ...regular, current: { kind: 'file', content, executable: false } };
}

describe('managed desired-state planning', () => {
  it('creates missing files, and emits no operations for an already synchronized file', () => {
    expect(planManagedFiles([missing]).writes).toEqual([{ target: missing.target, ...desired }]);
    expect(planManagedFiles([regular])).toEqual({
      results: [{ target: regular.target, action: 'unchanged' }],
      writes: [],
      conflicts: [],
    });
  });

  it('leaves disabled descriptors alone, including existing files', () => {
    expect(planManagedFiles([{ ...regular, desired: null }])).toEqual({
      results: [{ target: regular.target, action: 'skipped' }],
      writes: [],
      conflicts: [],
    });
  });

  it('repairs executable drift even when content is identical', () => {
    expect(planManagedFiles([{ ...regular, desired: { ...desired, executable: true } }]).writes).toEqual([
      { target: regular.target, ...desired, executable: true },
    ]);
  });

  it('retains existing local-tail separator bytes on a mode-only repair', () => {
    const content = `${desired.content}${LOCAL_SECTION_MARKER}\nlocal\n`;
    const input = { ...withContent(content), desired: { ...desired, executable: true } };
    expect(planManagedFiles([input]).writes[0]?.content).toBe(content);
  });

  it('preserves a repo-owned tail when replacing the managed section', () => {
    const tail = `${LOCAL_SECTION_MARKER}\ncustom value\n`;
    expect(planManagedFiles([withContent(`old\n\n${tail}`)]).writes[0]?.content).toBe(`${desired.content}\n${tail}`);
  });

  it('checks a valid symlink by content and never plans a write through it', () => {
    const input: ManagedFileInput = { ...regular, current: { kind: 'symlink', destination: 'source', ...desired } };
    expect(planManagedFiles([input])).toEqual({
      results: [{ target: input.target, action: 'ok-symlink' }],
      writes: [],
      conflicts: [],
    });
  });

  it('refuses drifted symlinks instead of reporting them as healthy', () => {
    const input: ManagedFileInput = {
      ...regular,
      current: { kind: 'symlink', destination: 'source', content: 'old', executable: false },
    };
    expect(planManagedFiles([input]).conflicts).toHaveLength(1);
    expect(planManagedFiles([input]).writes).toEqual([]);
  });

  it('does not keep an executable prefix of a plan with a later conflict', () => {
    const input: ManagedFileInput = { ...regular, target: 'later', current: { kind: 'blocked', reason: 'directory' } };
    const plan = planManagedFiles([missing, input]);
    expect(plan.conflicts).toEqual([{ target: 'later', reason: 'directory' }]);
    expect(plan.writes).toEqual([]);
  });

  it.each(['../escape', '/absolute', 'a/../b', 'a/./b', './x', 'a//b', 'C:/x', 'a\\b', 'a\0b', ''])(
    'rejects noncanonical target %j without a write',
    (target) => {
      const plan = planManagedFiles([{ ...missing, target }]);
      expect(plan.conflicts).toHaveLength(1);
      expect(plan.writes).toEqual([]);
    },
  );

  it('refuses duplicate paths and parent/file collisions', () => {
    expect(planManagedFiles([missing, missing]).conflicts).toHaveLength(1);
    expect(planManagedFiles([missing, { ...missing, target: 'tooling' }]).writes).toEqual([]);
    expect(planManagedFiles([missing, { ...missing, target: 'tooling' }]).conflicts).toHaveLength(1);
  });

  it('validates inline ownership before offering an update', () => {
    const current = ['old-anchor', INLINE_LOCAL_BEGIN, 'custom', INLINE_LOCAL_END, ''].join('\n');
    const plan = planManagedFiles([withContent(current)]);
    expect(plan.conflicts[0]?.reason).toContain('matches no line');
    expect(plan.writes).toEqual([]);
  });

  it('does not mutate inputs', () => {
    const input = Object.freeze({ ...regular, current: Object.freeze(regular.current) });
    const files = Object.freeze([input]);
    expect(() => planManagedFiles(files)).not.toThrow();
  });

  it('applying a successful plan reaches a fixed point', () => {
    fc.assert(
      fc.property(fc.string(), fc.string(), fc.boolean(), (before, after, executable) => {
        // Arbitrary text here is data, not local ownership syntax.
        const current = `value=${JSON.stringify(before)}\n`;
        const content = `value=${JSON.stringify(after)}\n`;
        const input = { ...withContent(current), desired: { content, executable } };
        const first = planManagedFiles([input]);
        expect(first.conflicts).toEqual([]);
        const write = first.writes[0];
        const next: ManagedFileInput = write ? { ...input, current: { kind: 'file', ...write } } : input;
        expect(planManagedFiles([next]).writes).toEqual([]);
        expect(planManagedFiles([next]).conflicts).toEqual([]);
      }),
      { numRuns: 200 },
    );
  });
});

describe('managed ownership grammar', () => {
  it('a marker mentioned inside another line is not an ownership boundary', () => {
    const content = `example: "${LOCAL_SECTION_MARKER}"\n`;
    expect(splitLocalSection(content)).toEqual({ managed: content, localTail: '' });
  });

  it('adjacent local blocks with one anchor retain their declared order', () => {
    const current = [
      'anchor',
      INLINE_LOCAL_BEGIN,
      'first',
      INLINE_LOCAL_END,
      INLINE_LOCAL_BEGIN,
      'second',
      INLINE_LOCAL_END,
      '',
    ].join('\n');
    const extracted = extractInlineLocalBlocks(current);
    expect(reinsertInlineLocalBlocks(extracted.withoutInline, extracted.blocks)).toBe(current);
  });

  it('inserted local text cannot become an anchor or make another anchor ambiguous', () => {
    expect(
      reinsertInlineLocalBlocks('a\nb\n', [
        { anchor: 'a', lines: 'b' },
        { anchor: 'b', lines: 'local' },
      ]),
    ).toBe(
      ['a', INLINE_LOCAL_BEGIN, 'b', INLINE_LOCAL_END, 'b', INLINE_LOCAL_BEGIN, 'local', INLINE_LOCAL_END, ''].join(
        '\n',
      ),
    );
  });

  it.each([[], ['']])('round-trips an empty block or blank line: %j', (...body) => {
    const current = ['anchor', INLINE_LOCAL_BEGIN, ...body, INLINE_LOCAL_END].join('\n');
    const extracted = extractInlineLocalBlocks(current);
    expect(reinsertInlineLocalBlocks(extracted.withoutInline, extracted.blocks)).toBe(current);
  });

  it('rejects nested and orphaned ownership markers', () => {
    expect(() =>
      extractInlineLocalBlocks(['a', INLINE_LOCAL_BEGIN, INLINE_LOCAL_BEGIN, INLINE_LOCAL_END].join('\n')),
    ).toThrow('nested');
    expect(() => extractInlineLocalBlocks(['a', INLINE_LOCAL_END].join('\n'))).toThrow('no matching');
  });
});
