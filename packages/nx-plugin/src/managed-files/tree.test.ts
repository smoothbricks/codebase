import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { createTree } from 'nx/src/devkit-testing-exports.js';
import { INLINE_LOCAL_BEGIN, INLINE_LOCAL_END, LOCAL_SECTION_MARKER } from './managed-content.js';
import { assertNoManagedConflicts, mergeManagedContent, stageManagedFiles } from './tree.js';

const file = { target: 'tooling/config', content: 'managed\n' };

describe('managed files in Nx Tree', () => {
  it('stages a missing template with explicit permissions', () => {
    const tree = createTree();
    const initial = tree.listChanges();
    expect(stageManagedFiles(tree, [file])).toEqual([{ target: file.target, action: 'created' }]);
    expect(tree.listChanges()).toEqual([
      ...initial,
      { path: file.target, type: 'CREATE', content: Buffer.from(file.content), options: { mode: 0o644 } },
    ]);
  });

  it('distinguishes an empty file from a missing file', () => {
    const tree = createTree();
    tree.write(file.target, '');
    expect(stageManagedFiles(tree, [file])[0].action).toBe('updated');
  });

  it('does not delete disabled descriptors or their customizations', () => {
    const tree = createTree();
    tree.write(file.target, 'owned locally');
    expect(stageManagedFiles(tree, [{ ...file, content: null }])[0].action).toBe('skipped');
    expect(tree.read(file.target, 'utf8')).toBe('owned locally');
  });

  it('preserves tails, adjacent inline blocks, indentation, and their order', () => {
    const tree = createTree();
    const local = [
      'anchor',
      `  ${INLINE_LOCAL_BEGIN}`,
      'one',
      `  ${INLINE_LOCAL_END}`,
      INLINE_LOCAL_BEGIN,
      'two',
      INLINE_LOCAL_END,
      'old',
      LOCAL_SECTION_MARKER,
      'tail',
    ].join('\n');
    tree.write(file.target, local);
    assertNoManagedConflicts(stageManagedFiles(tree, [{ ...file, content: 'anchor\nnew\n' }]));
    const result = tree.read(file.target, 'utf8');
    expect(result).toBe(local.replace('old\n', 'new\n\n'));
    expect(stageManagedFiles(tree, [{ ...file, content: 'anchor\nnew\n' }])[0].action).toBe('unchanged');
    expect(tree.read(file.target, 'utf8')).toBe(result);
  });

  it('uses Tree for an existing pending edit instead of rereading the disk', () => {
    const tree = createTree();
    tree.write(file.target, `before\n${LOCAL_SECTION_MARKER}\nfirst`);
    tree.write(file.target, `before\n${LOCAL_SECTION_MARKER}\nsecond`);
    stageManagedFiles(tree, [file]);
    expect(tree.read(file.target, 'utf8')).toBe(`${file.content}\n${LOCAL_SECTION_MARKER}\nsecond`);
  });

  it.each(['../outside', '/outside', './config', 'a//b', 'C:/config', 'a\\b', 'a\0b'])(
    'rejects non-relative target %s',
    (target) => {
      const tree = createTree();
      const initial = tree.listChanges();
      const results = stageManagedFiles(tree, [{ ...file, target }]);
      expect(() => assertNoManagedConflicts(results)).toThrow('workspace-relative');
      expect(tree.listChanges()).toEqual(initial);
    },
  );

  it('reports duplicate descriptors and overlapping file/directory targets', () => {
    expect(() => assertNoManagedConflicts(stageManagedFiles(createTree(), [file, file]))).toThrow('duplicate');
    expect(() =>
      assertNoManagedConflicts(stageManagedFiles(createTree(), [{ ...file, target: 'tooling' }, file])),
    ).toThrow('parent directory');
  });

  it('refuses lost or ambiguous anchors without changing that file', () => {
    for (const content of ['removed\n', 'anchor\nanchor\n']) {
      const tree = createTree();
      const current = ['anchor', INLINE_LOCAL_BEGIN, 'local', INLINE_LOCAL_END, ''].join('\n');
      tree.write(file.target, current);
      expect(() => assertNoManagedConflicts(stageManagedFiles(tree, [{ ...file, content }]))).toThrow('matches');
      expect(tree.read(file.target, 'utf8')).toBe(current);
    }
  });

  it('keeps matching links and refuses drifted links', () => {
    const tree = createTree();
    tree.write(file.target, file.content);
    const paths = new Map([[file.target, { symlink: true, executable: false }]]);
    expect(stageManagedFiles(tree, [file], paths)[0].action).toBe('ok-symlink');
    expect(() => assertNoManagedConflicts(stageManagedFiles(tree, [{ ...file, content: 'new' }], paths))).toThrow(
      'symlink content',
    );
    expect(tree.read(file.target, 'utf8')).toBe(file.content);
  });

  it('uses Tree.changePermissions for a permission-only update and preserves the content', () => {
    const tree = createTree();
    const content = `${file.content}${LOCAL_SECTION_MARKER}\nlocal`;
    tree.write(file.target, content);
    const paths = new Map([[file.target, { executable: false }]]);
    expect(stageManagedFiles(tree, [{ ...file, executable: true }], paths)[0].action).toBe('updated');
    expect(tree.listChanges().find((change) => change.path === file.target)?.options?.mode).toBe(0o755);
    expect(tree.read(file.target, 'utf8')).toBe(content);
  });

  it('preserves arbitrary local tails and converges under repeated rendering', () => {
    fc.assert(
      fc.property(fc.string(), fc.string(), (old, tail) => {
        const current = `${old.replaceAll('# smoo-local', '# ordinary')}\n${LOCAL_SECTION_MARKER}\n${tail}`;
        const next = mergeManagedContent(current, file.content);
        expect(next.endsWith(`${LOCAL_SECTION_MARKER}\n${tail}`)).toBe(true);
        expect(mergeManagedContent(next, file.content)).toBe(next);
      }),
      { numRuns: 100 },
    );
  });
});
