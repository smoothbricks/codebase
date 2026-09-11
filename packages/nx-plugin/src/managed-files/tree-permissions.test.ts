import { afterEach, describe, expect, it } from 'bun:test';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { createTree } from 'nx/src/devkit-testing-exports.js';
import { FsTree, flushChanges } from 'nx/src/generators/tree.js';
import { LOCAL_SECTION_MARKER } from './managed-content.js';
import { type ManagedFile, stageManagedFiles } from './tree.js';

const script = {
  target: 'tooling/run',
  content: '#!/bin/sh\necho ready\n',
  executable: true,
} satisfies ManagedFile;
const roots: string[] = [];

function diskTree(content: string, mode: number): FsTree {
  const root = mkdtempSync(join(tmpdir(), 'smoo-tree-mode-'));
  roots.push(root);
  const target = join(root, script.target);
  mkdirSync(dirname(target), { recursive: true });
  writeFileSync(target, content);
  chmodSync(target, mode);
  return new FsTree(root, false);
}

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

describe('managed permissions compose with other Nx generators', () => {
  it('makes an already-staged matching script executable', () => {
    const tree = createTree();
    tree.write(script.target, script.content);
    expect(stageManagedFiles(tree, [script])[0].action).toBe('updated');
    expect(tree.listChanges().find(({ path }) => path === script.target)?.options?.mode).toBe(0o755);
  });

  it('uses staged permissions rather than stale filesystem metadata', () => {
    const tree = createTree();
    tree.write(script.target, script.content, { mode: 0o644 });
    const paths = new Map([[script.target, { executable: true }]]);
    expect(stageManagedFiles(tree, [script], paths)[0].action).toBe('updated');
    expect(tree.listChanges().find(({ path }) => path === script.target)?.options?.mode).toBe(0o755);
  });

  it('is unchanged on a second pass even when the disk snapshot predates the repair', () => {
    const tree = createTree();
    tree.write(script.target, script.content);
    const paths = new Map([[script.target, { executable: false }]]);
    stageManagedFiles(tree, [script], paths);
    const before = tree.listChanges();
    expect(stageManagedFiles(tree, [script], paths)[0].action).toBe('unchanged');
    expect(tree.listChanges()).toEqual(before);
  });

  it('recognizes octal-string modes supported by Nx Tree', () => {
    const tree = createTree();
    tree.write(script.target, script.content, { mode: '755' });
    const before = tree.listChanges();
    expect(stageManagedFiles(tree, [script], new Map([[script.target, { executable: false }]]))[0].action).toBe(
      'unchanged',
    );
    expect(tree.listChanges()).toEqual(before);
  });

  it('removes a staged executable bit from a non-executable managed file', () => {
    const tree = createTree();
    tree.write(script.target, script.content, { mode: 0o755 });
    expect(stageManagedFiles(tree, [{ ...script, executable: false }])[0].action).toBe('updated');
    expect(tree.listChanges().find(({ path }) => path === script.target)?.options?.mode).toBe(0o644);
  });

  it('preserves local content during a staged permission-only repair', () => {
    const tree = createTree();
    const current = `${script.content}\n${LOCAL_SECTION_MARKER}\n# local\n`;
    tree.write(script.target, current);
    stageManagedFiles(tree, [script]);
    expect(tree.read(script.target, 'utf8')).toBe(current);
    expect(tree.listChanges().find(({ path }) => path === script.target)?.options?.mode).toBe(0o755);
  });

  it('retains the required mode when write() elides a reversion to the disk contents', () => {
    const tree = diskTree(script.content, 0o644);
    tree.write(script.target, '#!/bin/sh\necho temporary\n', { mode: 0o755 });
    stageManagedFiles(tree, [script], new Map([[script.target, { executable: false }]]));
    flushChanges(tree.root, tree.listChanges());
    expect(readFileSync(join(tree.root, script.target), 'utf8')).toBe(script.content);
    expect(statSync(join(tree.root, script.target)).mode & 0o777).toBe(0o755);

    const fresh = new FsTree(tree.root, false);
    expect(stageManagedFiles(fresh, [script], new Map([[script.target, { executable: true }]]))[0].action).toBe(
      'unchanged',
    );
    expect(fresh.listChanges()).toEqual([]);
  });

  it('inherits the disk mode for a pending update with no explicit mode', () => {
    const tree = diskTree('#!/bin/sh\necho old\n', 0o755);
    tree.write(script.target, script.content);
    const before = tree.listChanges();
    expect(stageManagedFiles(tree, [script], new Map([[script.target, { executable: true }]]))[0].action).toBe(
      'unchanged',
    );
    expect(tree.listChanges()).toEqual(before);
    flushChanges(tree.root, tree.listChanges());
    expect(statSync(join(tree.root, script.target)).mode & 0o777).toBe(0o755);
  });
});
