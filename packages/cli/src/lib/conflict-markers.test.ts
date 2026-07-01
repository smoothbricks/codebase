import { describe, expect, it } from 'bun:test';
import {
  assertNoConflictMarkers,
  type ConflictMarkerHit,
  ConflictMarkersError,
  findMarkerLines,
  formatMarkerHits,
  type MarkerScanShell,
  parseGitGrep,
  scanRefChangedForMarkers,
  scanTrackedForMarkers,
} from './conflict-markers.js';

describe('findMarkerLines', () => {
  it('detects ours/base/theirs markers with their 1-based line numbers', () => {
    const text = [
      '{',
      '<<<<<<< HEAD',
      '  "a": 1',
      '||||||| base',
      '  "a": 0',
      '=======',
      '  "a": 2',
      '>>>>>>> feat',
      '}',
    ].join('\n');
    expect(findMarkerLines(text)).toEqual([2, 4, 8]);
  });

  it('does not flag a bare ======= separator (Markdown h1 / diff fill)', () => {
    expect(findMarkerLines('Title\n=======\nbody')).toEqual([]);
  });

  it('does not flag quoted/indented marker strings in source', () => {
    const source = ["const OPEN = '<<<<<<< ';", "  const CLOSE = '>>>>>>> ';", 'const P = /^<<<<<<< /;'].join('\n');
    expect(findMarkerLines(source)).toEqual([]);
  });

  it('returns empty for clean text', () => {
    expect(findMarkerLines('{\n  "a": 1\n}\n')).toEqual([]);
  });
});

describe('parseGitGrep', () => {
  it('groups path:line:content rows by file', () => {
    const stdout = ['pkg/a.json:12:<<<<<<< HEAD', 'pkg/a.json:16:>>>>>>> x', 'b.toml:36:<<<<<<< HEAD', ''].join('\n');
    expect(parseGitGrep(stdout)).toEqual([
      { file: 'pkg/a.json', lines: [12, 16] },
      { file: 'b.toml', lines: [36] },
    ]);
  });

  it('tolerates paths containing colons in content and skips malformed rows', () => {
    const stdout = ['a.ts:3:foo: bar', 'garbage-without-colon', 'a.ts:9:>>>>>>> y'].join('\n');
    expect(parseGitGrep(stdout)).toEqual([{ file: 'a.ts', lines: [3, 9] }]);
  });

  it('returns empty for empty output', () => {
    expect(parseGitGrep('')).toEqual([]);
  });
});

describe('scanTrackedForMarkers', () => {
  it('returns hits when git grep finds markers (exit 0)', async () => {
    const shell = scriptedShell([{ exitCode: 0, stdout: 'x.json:2:<<<<<<< HEAD\n', stderr: '' }]);
    const hits = await scanTrackedForMarkers(shell.shell, '/repo');
    expect(hits).toEqual([{ file: 'x.json', lines: [2] }]);
    expect(shell.calls[0].args).toEqual(['grep', '-nI', '-E', '^(<<<<<<< |\\|\\|\\|\\|\\|\\|\\| |>>>>>>> )']);
  });

  it('passes pathspecs through after --', async () => {
    const shell = scriptedShell([{ exitCode: 1, stdout: '', stderr: '' }]);
    await scanTrackedForMarkers(shell.shell, '/repo', ['packages/a', 'packages/b']);
    expect(shell.calls[0].args.slice(-3)).toEqual(['--', 'packages/a', 'packages/b']);
  });

  it('treats exit 1 as no matches', async () => {
    const shell = scriptedShell([{ exitCode: 1, stdout: '', stderr: '' }]);
    expect(await scanTrackedForMarkers(shell.shell, '/repo')).toEqual([]);
  });

  it('throws on git grep error (exit > 1)', async () => {
    const shell = scriptedShell([{ exitCode: 128, stdout: '', stderr: 'not a git repo' }]);
    await expect(scanTrackedForMarkers(shell.shell, '/repo')).rejects.toThrow('not a git repo');
  });
});

describe('scanRefChangedForMarkers', () => {
  it('reads changed-file blobs at head and reports only markered files', async () => {
    const shell = scriptedShell([
      { exitCode: 0, stdout: 'pkg/a.json\npkg/clean.ts\n', stderr: '' }, // git diff --name-only
      { exitCode: 0, stdout: '{\n<<<<<<< HEAD\n=======\n>>>>>>> feat\n}\n', stderr: '' }, // show a.json
      { exitCode: 0, stdout: 'export const ok = 1;\n', stderr: '' }, // show clean.ts
    ]);
    const hits = await scanRefChangedForMarkers(shell.shell, '/repo', 'base', 'head');
    expect(hits).toEqual([{ file: 'pkg/a.json', lines: [2, 4] }]);
    expect(shell.calls[0].args).toEqual(['diff', '--name-only', 'base...head']);
    expect(shell.calls[1].args).toEqual(['show', 'head:pkg/a.json', '--textconv']);
  });

  it('skips files deleted/unreadable at head', async () => {
    const shell = scriptedShell([
      { exitCode: 0, stdout: 'gone.txt\n', stderr: '' },
      { exitCode: 128, stdout: '', stderr: 'exists on disk, but not in head' },
    ]);
    expect(await scanRefChangedForMarkers(shell.shell, '/repo', 'base', 'head')).toEqual([]);
  });
});

describe('formatMarkerHits', () => {
  it('formats file + line list per hit', () => {
    const hits: ConflictMarkerHit[] = [
      { file: 'a.json', lines: [12, 16] },
      { file: 'b.toml', lines: [3] },
    ];
    expect(formatMarkerHits(hits)).toBe('  a.json (lines 12, 16)\n  b.toml (lines 3)');
  });
});

interface ScriptedCall {
  command: string;
  args: string[];
  cwd: string;
}

function scriptedShell(responses: { exitCode: number; stdout: string; stderr: string }[]): {
  shell: MarkerScanShell;
  calls: ScriptedCall[];
} {
  const calls: ScriptedCall[] = [];
  let index = 0;
  const shell: MarkerScanShell = {
    async runResult(command, args, cwd) {
      calls.push({ command, args, cwd });
      const response = responses[index++];
      if (!response) {
        throw new Error(`unexpected shell call: ${command} ${args.join(' ')}`);
      }
      return response;
    },
  };
  return { shell, calls };
}

describe('assertNoConflictMarkers', () => {
  it('throws ConflictMarkersError listing hits when markers exist', async () => {
    const shell = scriptedShell([{ exitCode: 0, stdout: 'a.json:2:<<<<<<< HEAD\n', stderr: '' }]);
    let caught: unknown;
    try {
      await assertNoConflictMarkers(shell.shell, '/repo', 'publish');
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(ConflictMarkersError);
    if (caught instanceof ConflictMarkersError) {
      expect(caught.hits).toEqual([{ file: 'a.json', lines: [2] }]);
      expect(caught.message).toContain('Refusing to publish');
    }
  });

  it('resolves when the tree is clean', async () => {
    const shell = scriptedShell([{ exitCode: 1, stdout: '', stderr: '' }]);
    await expect(assertNoConflictMarkers(shell.shell, '/repo', 'publish')).resolves.toBeUndefined();
  });
});
