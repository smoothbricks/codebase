import { describe, expect, it } from 'bun:test';
import { posix } from 'node:path';
import { DEFAULT_TRACE_DB_PATH, TRACE_DB_DIRECTORY, TRACE_DB_FILENAME } from '../trace-db-path.js';

describe('DEFAULT_TRACE_DB_PATH', () => {
  const directories = posix.dirname(DEFAULT_TRACE_DB_PATH).split('/');

  it('is the one directory and filename callers join onto a root', () => {
    expect(DEFAULT_TRACE_DB_PATH).toBe(posix.join(TRACE_DB_DIRECTORY, TRACE_DB_FILENAME));
    expect(posix.isAbsolute(DEFAULT_TRACE_DB_PATH)).toBe(false);
  });

  it('sits where project walkers and file watchers never descend, so SQLite sidecars cannot perturb a compile', () => {
    expect(directories[0]).toBe('node_modules');
  });

  it("is per-checkout tool state in the package's node_modules/.cache, never beside a package's declared outputs", () => {
    // `node_modules/.cache/<tool>` is the convention a workspace manager keeps on its build volume; a
    // package-level `.cache/` holds build outputs, which stay with the source.
    expect(directories.slice(0, 3)).toEqual(['node_modules', '.cache', 'lmao']);
    expect(directories).toHaveLength(3);
  });
});
