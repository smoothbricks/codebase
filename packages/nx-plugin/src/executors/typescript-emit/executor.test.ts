import { describe, expect, it } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { type CompilerInvocation, runTypeScriptEmit } from './executor.js';

describe('@smoothbricks/nx-plugin TypeScript emit executor', () => {
  it('runs transformed JavaScript and native declaration lanes with project references preserved', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-'));
    const projectRoot = join(root, 'packages/example');
    await mkdir(projectRoot, { recursive: true });
    await writeFile(
      join(projectRoot, 'tsconfig.lib.json'),
      '{\n  // Project references are intentionally not inherited through extends.\n  "references": [{ "path": "../dependency/tsconfig.lib.json" }]\n}\n',
    );

    const invocations: CompilerInvocation[] = [];
    let overlayContents = '';
    let overlayPath = '';
    try {
      const result = await runTypeScriptEmit(
        { cwd: 'packages/example', tsConfig: 'tsconfig.lib.json' },
        { root },
        async (invocation) => {
          invocations.push(invocation);
          if (invocation.command === 'ttsc') {
            overlayPath = invocation.args[1] ?? '';
            overlayContents = await readFile(overlayPath, 'utf8');
          }
          return true;
        },
      );

      expect(result).toEqual({ success: true });
      expect(invocations).toHaveLength(2);
      expect(invocations[0]).toEqual({
        command: 'ttsc',
        args: ['-p', overlayPath, '--emit'],
        cwd: projectRoot,
      });
      expect(overlayPath).toMatch(/\/\.ttsc-js-\d+-[0-9a-f-]+\.json$/);
      expect(overlayContents).toBe(
        `${JSON.stringify(
          {
            extends: './tsconfig.lib.json',
            compilerOptions: {
              composite: false,
              declaration: false,
              declarationMap: false,
              emitDeclarationOnly: false,
              incremental: false,
            },
            references: [{ path: '../dependency/tsconfig.lib.json' }],
          },
          null,
          2,
        )}\n`,
      );
      expect(invocations[1]).toEqual({
        command: 'tsc',
        args: ['-p', overlayPath, '--emitDeclarationOnly', '--declaration', '--declarationMap'],
        cwd: projectRoot,
      });
      expect(existsSync(overlayPath)).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('does not emit declarations when transformed JavaScript fails and still removes its overlay', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-'));
    const projectRoot = join(root, 'packages/example');
    await mkdir(projectRoot, { recursive: true });
    await writeFile(join(projectRoot, 'tsconfig.lib.json'), '{}\n');

    const invocations: CompilerInvocation[] = [];
    let overlayPath = '';
    try {
      const result = await runTypeScriptEmit(
        { cwd: projectRoot, tsConfig: 'tsconfig.lib.json' },
        { root },
        async (invocation) => {
          invocations.push(invocation);
          overlayPath = invocation.args[1] ?? '';
          return false;
        },
      );

      expect(result).toEqual({ success: false });
      expect(invocations.map(({ command }) => command)).toEqual(['ttsc']);
      expect(existsSync(overlayPath)).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
