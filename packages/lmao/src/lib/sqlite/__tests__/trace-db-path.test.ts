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
    expect(directories[0]).toBe('.cache');
  });

  it('is per-checkout tool state in a directory of its own, never beside a package root or inside node_modules', () => {
    // A workspace manager holds exactly `.cache/lmao` as build state; `.cache/` itself holds
    // declared build outputs, which stay with the source.
    expect(directories).toEqual(['.cache', 'lmao']);
  });
});
