import { beforeEach, describe, expect, it } from 'bun:test';
import { FileDB, type FileDBData } from '../src/filedb.js';

// WHY: mock readFile so tests are pure and don't touch the filesystem
function mockReader(contents: Record<string, string>): (path: string) => Promise<string> {
  return async (path: string) => {
    // biome-ignore lint/style/noNonNullAssertion: `path in contents` guarantees the key exists
    if (path in contents) return contents[path]!;
    throw new Error(`ENOENT: ${path}`);
  };
}

const VALID_DB: FileDBData = {
  version: '2.2.0',
  files: {
    'agents/review.md': { modified: '2025-01-15T10:30:00', hexdigest: 'abc123' },
    'src/signals.ts': { modified: '2025-01-15T10:30:01', hexdigest: 'def456' },
    'src/agent.ts': { modified: '2025-01-15T10:30:02', hexdigest: 'ghi789' },
  },
  targets: ['src/signals.ts', 'src/agent.ts'],
};

describe('FileDB', () => {
  let db: FileDB;

  beforeEach(() => {
    db = new FileDB('/project');
  });

  // --- load from valid filedb.json ---

  it('loads and parses a valid filedb.json', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify(VALID_DB),
    });

    const data = await db.load(reader);

    expect(data.version).toBe('2.2.0');
    expect(data.targets).toEqual(['src/signals.ts', 'src/agent.ts']);
    expect(Object.keys(data.files)).toHaveLength(3);
  });

  // --- isManaged ---

  it('isManaged returns true for managed tangle targets', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify(VALID_DB),
    });
    await db.load(reader);

    expect(db.isManaged('src/signals.ts')).toBe(true);
    expect(db.isManaged('src/agent.ts')).toBe(true);
  });

  it('isManaged returns false for non-managed files', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify(VALID_DB),
    });
    await db.load(reader);

    // Source .md is tracked but not a target
    expect(db.isManaged('agents/review.md')).toBe(false);
    expect(db.isManaged('src/unknown.ts')).toBe(false);
  });

  it('isManaged normalizes absolute paths against project root', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify(VALID_DB),
    });
    await db.load(reader);

    expect(db.isManaged('/project/src/signals.ts')).toBe(true);
    expect(db.isManaged('/project/src/unknown.ts')).toBe(false);
  });

  // --- listTargets ---

  it('listTargets returns all managed target file paths', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify(VALID_DB),
    });
    await db.load(reader);

    const targets = db.listTargets().sort();
    expect(targets).toEqual(['src/agent.ts', 'src/signals.ts']);
  });

  it('listTargets returns empty array before load()', () => {
    expect(db.listTargets()).toEqual([]);
  });

  // --- getStat ---

  it('getStat returns stat info for tracked files', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify(VALID_DB),
    });
    await db.load(reader);

    const stat = db.getStat('src/signals.ts');
    expect(stat).toBeDefined();
    expect(stat?.hexdigest).toBe('def456');
  });

  it('getStat returns undefined for untracked files', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify(VALID_DB),
    });
    await db.load(reader);

    expect(db.getStat('src/unknown.ts')).toBeUndefined();
  });

  // --- listFiles ---

  it('listFiles returns all tracked paths (sources + targets)', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify(VALID_DB),
    });
    await db.load(reader);

    const files = db.listFiles().sort();
    expect(files).toEqual(['agents/review.md', 'src/agent.ts', 'src/signals.ts']);
  });

  // --- missing filedb.json ---

  it('handles missing .entangled/filedb.json gracefully', async () => {
    const reader = mockReader({}); // no files at all

    const data = await db.load(reader);

    expect(data.version).toBe('');
    expect(data.targets).toEqual([]);
    expect(Object.keys(data.files)).toHaveLength(0);
    expect(db.isManaged('anything')).toBe(false);
    expect(db.listTargets()).toEqual([]);
  });

  // --- malformed JSON ---

  it('handles malformed JSON gracefully', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': '{not valid json!!!',
    });

    const data = await db.load(reader);

    expect(data.version).toBe('');
    expect(data.targets).toEqual([]);
    expect(db.isManaged('anything')).toBe(false);
  });

  // --- partial / weird data ---

  it('handles JSON with missing fields gracefully', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify({ version: '1.0.0' }),
    });

    const data = await db.load(reader);

    expect(data.version).toBe('1.0.0');
    expect(data.targets).toEqual([]);
    expect(Object.keys(data.files)).toHaveLength(0);
  });

  it('handles JSON with wrong types gracefully', async () => {
    const reader = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify({
        version: 42,
        files: 'not an object',
        targets: 'not an array',
      }),
    });

    const data = await db.load(reader);

    expect(data.version).toBe('');
    expect(data.targets).toEqual([]);
    expect(Object.keys(data.files)).toHaveLength(0);
  });

  // --- reload clears previous state ---

  it('reloading clears previous state', async () => {
    const reader1 = mockReader({
      '/project/.entangled/filedb.json': JSON.stringify(VALID_DB),
    });
    await db.load(reader1);
    expect(db.listTargets()).toHaveLength(2);

    // WHY: reload with empty db should clear all state from first load
    const reader2 = mockReader({});
    await db.load(reader2);
    expect(db.listTargets()).toHaveLength(0);
    expect(db.isManaged('src/signals.ts')).toBe(false);
  });
});
