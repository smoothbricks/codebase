/**
 * Tests for consumer-package resolution.
 *
 * The resolver must identify "the package whose tests are running" by standard
 * resolution semantics — nearest package.json above cwd — identically in every
 * layout: a monorepo checkout, a consumer of the Bun isolated store, or plain
 * node_modules. The regression this guards: resolution anchored on the
 * library's own file location left consumer repos' .trace-results.db silently
 * stale.
 */

import { afterEach, describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { resolveConsumerTarget } from '../consumer-package.js';

const roots: string[] = [];

function makeTree(dirs: readonly string[], files: Readonly<Record<string, string>>): string {
  const root = mkdtempSync(join(tmpdir(), 'lmao-consumer-'));
  roots.push(root);
  for (const dir of dirs) mkdirSync(join(root, dir), { recursive: true });
  for (const [path, content] of Object.entries(files)) writeFileSync(join(root, path), content);
  return root;
}

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

describe('resolveConsumerTarget', () => {
  it('resolves the nearest package.json as the consumer package root', () => {
    const root = makeTree(['repo/packages/some-app/src/deep'], {
      'repo/package.json': '{"workspaces":["packages/*"]}',
      'repo/packages/some-app/package.json': '{}',
    });
    const target = resolveConsumerTarget(join(root, 'repo', 'packages', 'some-app', 'src', 'deep'));
    expect(target.kind).toBe('package');
    if (target.kind === 'package') expect(target.packageRoot).toBe(join(root, 'repo', 'packages', 'some-app'));
  });

  it('resolves in a foreign consumer workspace regardless of where the library is installed', () => {
    // Consumer repo whose lmao copy lives under the Bun isolated store — the
    // resolver must anchor on cwd, never on any path related to the store.
    const root = makeTree(
      [
        'repo/packages/consumer-pkg',
        'repo/node_modules/.bun/@smoothbricks+lmao@0.0.0+cafebabe/node_modules/@smoothbricks/lmao',
      ],
      {
        'repo/package.json': '{"workspaces":["packages/*"]}',
        'repo/packages/consumer-pkg/package.json': '{}',
      },
    );
    const target = resolveConsumerTarget(join(root, 'repo', 'packages', 'consumer-pkg'));
    expect(target.kind).toBe('package');
    if (target.kind === 'package') expect(target.packageRoot).toBe(join(root, 'repo', 'packages', 'consumer-pkg'));
  });

  it('treats a nearest package.json with workspaces as a workspace-root run and expands members', () => {
    const root = makeTree(['repo/packages/a', 'repo/packages/b', 'repo/packages/not-a-pkg', 'repo/tools/standalone'], {
      'repo/package.json': '{"workspaces":["packages/*","tools/standalone"]}',
      'repo/packages/a/package.json': '{}',
      'repo/packages/b/package.json': '{}',
      'repo/tools/standalone/package.json': '{}',
    });
    const target = resolveConsumerTarget(join(root, 'repo'));
    expect(target.kind).toBe('workspace-root');
    if (target.kind === 'workspace-root') {
      expect(target.workspaceRoot).toBe(join(root, 'repo'));
      expect([...target.packageRoots].sort()).toEqual([
        join(root, 'repo', 'packages', 'a'),
        join(root, 'repo', 'packages', 'b'),
        join(root, 'repo', 'tools', 'standalone'),
      ]);
    }
  });

  it('prefers a member package over the workspace root when cwd is inside the member', () => {
    const root = makeTree(['repo/packages/member'], {
      'repo/package.json': '{"workspaces":["packages/*"]}',
      'repo/packages/member/package.json': '{}',
    });
    const target = resolveConsumerTarget(join(root, 'repo', 'packages', 'member'));
    expect(target.kind).toBe('package');
  });

  it('classifies a workspace-root run from a non-member subdirectory', () => {
    const root = makeTree(['repo/scripts', 'repo/packages/a'], {
      'repo/package.json': '{"workspaces":["packages/*"]}',
      'repo/packages/a/package.json': '{}',
    });
    const target = resolveConsumerTarget(join(root, 'repo', 'scripts'));
    expect(target.kind).toBe('workspace-root');
  });

  it('degrades malformed package.json to a plain package root instead of crashing', () => {
    const root = makeTree(['repo/pkg'], {
      'repo/pkg/package.json': '{not json',
    });
    const target = resolveConsumerTarget(join(root, 'repo', 'pkg'));
    expect(target.kind).toBe('package');
    if (target.kind === 'package') expect(target.packageRoot).toBe(join(root, 'repo', 'pkg'));
  });
});
